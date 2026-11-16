//! Preparing built or published tables: the runtime, the public setup, and
//! the map `Table` entry with its session id (spec 6.6, 10.3, 10.4).

use rayon::ThreadPool;
use tachyon_pir_protocol::{Bytes32, PirParams, Product, SessionParts, Table, session_id};

use crate::build::BuiltTable;
use crate::digest::rows_sha256;
use crate::error::Error;
use crate::runtime::TableRuntime;

/// A table ready to serve: its runtime and everything the map says of it.
#[derive(Debug)]
pub struct PreparedTable {
    pub runtime: TableRuntime,
    /// The public setup bytes, 45,056; published as an object.
    pub public: Vec<u8>,
    pub public_sha256: [u8; 32],
    pub session_id: [u8; 32],
    /// The map entry for this table.
    pub table: Table,
}

/// What the publisher needs of a table: its map entry and public setup. The
/// runtime is dropped after the build to free its memory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribedTable {
    pub table: Table,
    pub public: Vec<u8>,
}

impl BuiltTable {
    /// The map `Table` for this table of scope `scope_id`, given its public
    /// setup digest; the session id is computed from them (spec 10.4).
    pub fn map_table(&self, product: Product, scope_id: u64, public_sha256: [u8; 32]) -> Table {
        let parts = SessionParts {
            scope_id,
            bucket: self.bucket,
            segment: self.segment,
            rows: self.geometry.rows(),
            seed: self.seed,
            rows_sha256: self.rows_sha256,
            public_sha256,
        };
        Table {
            bucket: self.bucket,
            segment: self.segment,
            geometry: self.geometry,
            seed: self.seed,
            entries: u64::from(self.entries),
            rows_sha256: Bytes32(self.rows_sha256),
            public_sha256: Bytes32(public_sha256),
            session_id: Bytes32(session_id(product, &parts)),
        }
    }
}

fn run<T: Send>(pool: Option<&ThreadPool>, f: impl FnOnce() -> T + Send) -> T {
    match pool {
        Some(pool) => pool.install(f),
        None => f(),
    }
}

fn prepare_with(
    pool: Option<&ThreadPool>,
    product: Product,
    scope_id: u64,
    built: &BuiltTable,
) -> Result<PreparedTable, Error> {
    if rows_sha256(&built.rows) != built.rows_sha256 {
        return Err(Error::RowsDigest);
    }
    let params = PirParams::new(product, built.geometry);
    let runtime = run(pool, || TableRuntime::build(&params, &built.rows))?;
    let public_sha256 = runtime.public_sha256();
    let table = built.map_table(product, scope_id, public_sha256);
    Ok(PreparedTable {
        public: runtime.public().to_vec(),
        public_sha256,
        session_id: table.session_id.0,
        table,
        runtime,
    })
}

/// Builds the runtime for `built`, table of scope `scope_id`, on the current
/// rayon pool, and returns it with the public setup and the map entry.
/// Refuses rows that do not hash to `built.rows_sha256`.
pub fn prepare(
    product: Product,
    scope_id: u64,
    built: &BuiltTable,
) -> Result<PreparedTable, Error> {
    prepare_with(None, product, scope_id, built)
}

/// [`prepare`] inside `pool`.
pub fn prepare_in(
    pool: &ThreadPool,
    product: Product,
    scope_id: u64,
    built: &BuiltTable,
) -> Result<PreparedTable, Error> {
    prepare_with(Some(pool), product, scope_id, built)
}

/// The map entry and public setup of `built`, for the publisher: a full
/// preparation whose runtime is dropped as soon as the public setup is known
/// (spec 6.6: `public_sha256` enters the session id).
pub fn describe(
    product: Product,
    scope_id: u64,
    built: &BuiltTable,
) -> Result<DescribedTable, Error> {
    let prepared = prepare_with(None, product, scope_id, built)?;
    Ok(DescribedTable {
        table: prepared.table,
        public: prepared.public,
    })
}

/// [`describe`] inside `pool`.
pub fn describe_in(
    pool: &ThreadPool,
    product: Product,
    scope_id: u64,
    built: &BuiltTable,
) -> Result<DescribedTable, Error> {
    let prepared = prepare_with(Some(pool), product, scope_id, built)?;
    Ok(DescribedTable {
        table: prepared.table,
        public: prepared.public,
    })
}

fn prepare_published_with(
    pool: Option<&ThreadPool>,
    product: Product,
    scope_id: u64,
    table: &Table,
    rows: &[u8],
) -> Result<PreparedTable, Error> {
    let expected = table.geometry.table_bytes();
    if rows.len() != expected {
        return Err(Error::RowsLength {
            expected,
            actual: rows.len(),
        });
    }
    if table.session_id != table.expected_session_id(product, scope_id) {
        return Err(Error::SessionId);
    }
    if rows_sha256(rows) != table.rows_sha256.0 {
        return Err(Error::RowsDigest);
    }
    let params = PirParams::new(product, table.geometry);
    let runtime = run(pool, || TableRuntime::build(&params, rows))?;
    if runtime.public_sha256() != table.public_sha256.0 {
        return Err(Error::PublicDigest {
            expected: table.public_sha256.0,
            actual: runtime.public_sha256(),
        });
    }
    Ok(PreparedTable {
        public: runtime.public().to_vec(),
        public_sha256: runtime.public_sha256(),
        session_id: table.session_id.0,
        table: table.clone(),
        runtime,
    })
}

/// A query host's preparation of published rows for map `table` of scope
/// `scope_id`: checks the session id recomputes and the rows hash to
/// `rows_sha256`, builds the runtime on the current rayon pool, and checks
/// the prepared public setup equals `public_sha256` (spec 6.6). Row
/// validation is separate ([`crate::validate_published`]).
pub fn prepare_published(
    product: Product,
    scope_id: u64,
    table: &Table,
    rows: &[u8],
) -> Result<PreparedTable, Error> {
    prepare_published_with(None, product, scope_id, table, rows)
}

/// [`prepare_published`] inside `pool`.
pub fn prepare_published_in(
    pool: &ThreadPool,
    product: Product,
    scope_id: u64,
    table: &Table,
    rows: &[u8],
) -> Result<PreparedTable, Error> {
    prepare_published_with(Some(pool), product, scope_id, table, rows)
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PreparedTable>();
};
