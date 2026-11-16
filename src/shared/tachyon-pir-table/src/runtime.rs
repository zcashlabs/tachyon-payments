//! The P8 table runtime (spec 6.6): one table's rows prepared to answer
//! queries, over the free primitives of `zakura-pir-server-core` and
//! `zakura-pir-query-server`, as Receiver's `Prepared` and txid's
//! `TableRuntime` are (`docs/shared/wallet-pir-integration.md` section 1).
//!
//! Building transposes the rows to column-major bytes, computes the public
//! hint under the query masks, runs two-mask preprocessing under the packing
//! setup, interleaves the bytes for the scan kernel and publishes the public
//! setup. Answering parses an upload of the geometry's single length, scans
//! the database and packs the reply.

use rayon::ThreadPool;
use rayon::prelude::*;
use reinspiring::native_kernel::PreparedU16Query;
use tachyon_pir_protocol::{Geometry, PirParams, Product, ROW_BYTES};
use zakura_pir_protocol::native::{D, P8, PREPARED_HEADER_BYTES, Q};
use zakura_pir_query_server::native::ProfileQueryExt as _;
use zakura_pir_server_core::native::{
    NativePreprocessed, ProfilePublicationExt as _, hint, preprocess,
};

use crate::digest::public_sha256;
use crate::error::Error;

/// Columns per interleaved band of the scan kernel.
const BAND_COLUMNS: usize = 16;

/// One table prepared to answer P8 queries. `Send + Sync`: one shared
/// runtime answers concurrent queries.
pub struct TableRuntime {
    product: Product,
    geometry: Geometry,
    preprocessed: Vec<NativePreprocessed>,
    /// Column-major, interleaved database bytes.
    db: Vec<u8>,
    public: Vec<u8>,
    public_sha256: [u8; 32],
}

impl TableRuntime {
    /// Builds the runtime for `rows` (`rows x 8192` bytes, row-major, already
    /// validated) on the current rayon pool. CPU-heavy: about 1 s on 32
    /// threads, 4-5 s on one, with a transient peak of a few hundred MiB.
    /// The rows are borrowed, not kept.
    pub fn build(params: &PirParams, rows: &[u8]) -> Result<TableRuntime, Error> {
        let n = params.rows();
        let expected = n * ROW_BYTES;
        if rows.len() != expected {
            return Err(Error::RowsLength {
                expected,
                actual: rows.len(),
            });
        }
        let mut db = vec![0u8; expected];
        db.par_chunks_mut(n).enumerate().for_each(|(col, column)| {
            for (row, cell) in column.iter_mut().enumerate() {
                *cell = rows[row * ROW_BYTES + col];
            }
        });
        let hint = hint(params.masks(), n, params.cols(), |col| {
            &db[col * n..(col + 1) * n]
        })
        .map_err(Error::Engine)?;
        let preprocessed = preprocess(params.setup(), &hint).map_err(Error::Engine)?;
        drop(hint);
        db.par_chunks_mut(n * BAND_COLUMNS)
            .for_each(|band| PreparedU16Query::interleave_u8_columns(band, n));
        let public = P8.publish(&preprocessed).map_err(Error::Engine)?;
        if public.len() != params.public_len() {
            return Err(Error::Engine(format!(
                "public setup is {} bytes, expected {}",
                public.len(),
                params.public_len()
            )));
        }
        let public_sha256 = public_sha256(&public);
        Ok(TableRuntime {
            product: params.product(),
            geometry: params.geometry(),
            preprocessed,
            db,
            public,
            public_sha256,
        })
    }

    /// [`TableRuntime::build`] inside `pool`, so a host can keep builds off
    /// the pool that answers queries.
    pub fn build_in(
        pool: &ThreadPool,
        params: &PirParams,
        rows: &[u8],
    ) -> Result<TableRuntime, Error> {
        pool.install(|| TableRuntime::build(params, rows))
    }

    /// The product whose parameters built this runtime.
    pub fn product(&self) -> Product {
        self.product
    }

    /// The geometry of this runtime's table.
    pub fn geometry(&self) -> Geometry {
        self.geometry
    }

    /// The public setup bytes, `P8.public_len(8192)` = 45,056.
    pub fn public(&self) -> &[u8] {
        &self.public
    }

    /// SHA-256 of [`TableRuntime::public`].
    pub fn public_sha256(&self) -> [u8; 32] {
        self.public_sha256
    }

    /// Answers one query. `payload` is the body after the 52-byte header and
    /// must be exactly the geometry's payload length (28,160 bytes at
    /// `tachyon-4k8`); any other length is [`Error::Query`]. Returns the
    /// packed reply, 10,240 bytes. Runs on the current rayon pool. The row
    /// selected is never known here.
    pub fn answer(&self, params: &PirParams, payload: &[u8]) -> Result<Vec<u8>, Error> {
        if params.product() != self.product || params.geometry() != self.geometry {
            return Err(Error::Params {
                product: self.product,
                geometry: self.geometry,
                params_product: params.product(),
                params_geometry: params.geometry(),
            });
        }
        let rows = params.rows();
        let (keys, query) = P8
            .parse_accepted(params.setup(), payload, rows)
            .map_err(Error::Query)?;
        let prepared =
            PreparedU16Query::new(&query, Q).map_err(|e| Error::Engine(e.to_string()))?;
        let mut intermediate = vec![0u64; self.preprocessed.len() * D];
        if self.db.len() != rows * intermediate.len() {
            return Err(Error::Engine("query rows do not match the database".into()));
        }
        intermediate
            .par_chunks_mut(BAND_COLUMNS)
            .zip(self.db.par_chunks(rows * BAND_COLUMNS))
            .for_each(|(out, band)| prepared.multiply_interleaved_u8(band, out));
        let reply = P8
            .pack(&self.preprocessed, &keys, &intermediate)
            .map_err(Error::Engine)?;
        if reply.len() != params.reply_len() {
            return Err(Error::Engine(format!(
                "reply is {} bytes, expected {}",
                reply.len(),
                params.reply_len()
            )));
        }
        Ok(reply)
    }

    /// Bytes this runtime holds, as txid's `TableRuntime::held_bytes`
    /// computes them: the database, the public setup, the prepared header
    /// and, per block, its id and width word plus the retained coefficient
    /// storage.
    pub fn held_bytes(&self) -> u64 {
        const BLOCK_ID_AND_WIDTH_BYTES: u64 = 32 + 8;
        self.db.len() as u64
            + self.public.len() as u64
            + PREPARED_HEADER_BYTES
            + self
                .preprocessed
                .iter()
                .map(|block| BLOCK_ID_AND_WIDTH_BYTES + block.coefficient_bytes() as u64)
                .sum::<u64>()
    }
}

impl std::fmt::Debug for TableRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableRuntime")
            .field("product", &self.product)
            .field("geometry", &self.geometry)
            .field("public_sha256", &crate::build::hex(&self.public_sha256))
            .field("held_bytes", &self.held_bytes())
            .finish_non_exhaustive()
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TableRuntime>();
};
