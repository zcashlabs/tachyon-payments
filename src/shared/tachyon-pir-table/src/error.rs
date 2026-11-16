//! The one error type of this crate.

use tachyon_pir_protocol::{Geometry, PlacementError, Product, RowError};

/// Why a scope could not be built, a table did not validate, or a runtime
/// could not be built or answer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// More entries than a `u32` index can name.
    #[error("{0} entries is more than one scope can hold")]
    TooManyEntries(usize),
    /// An input entry of another product.
    #[error("entry {index} is a {actual} entry in a {expected} scope")]
    EntryProduct {
        index: usize,
        expected: Product,
        actual: Product,
    },
    /// An input entry whose key is all zero (spec 6.2: rejected at ingestion).
    #[error("entry {index} has an all-zero key")]
    ZeroKey { index: usize },
    /// Two tachygram entries with the same tachygram (spec 8.3: resolved at
    /// ingestion, before the table).
    #[error("tachygram entries {first} and {second} have the same key")]
    DuplicateKey { first: usize, second: usize },
    /// A bucket whose spill does not fit in `MAX_SEGMENTS` segments (spec
    /// 6.4). The publication fails.
    #[error("bucket {bucket} still spills {spilled} entries after {max} segments")]
    TooManySegments {
        bucket: u32,
        spilled: usize,
        max: u32,
    },
    /// Placement refused a key or shape.
    #[error(transparent)]
    Placement(#[from] PlacementError),
    /// Row encoding refused a row's entries while building.
    #[error("bucket {bucket} segment {segment} row {row}: {source}")]
    Encode {
        bucket: u32,
        segment: u32,
        row: u32,
        source: RowError,
    },
    /// A row of a table is not canonical for its context.
    #[error("row {row}: {source}")]
    InvalidRow { row: u32, source: RowError },
    /// A table-level rule failed; `source` names the rule.
    #[error("bucket {bucket} segment {segment}: {source}")]
    InTable {
        bucket: u32,
        segment: u32,
        source: Box<Error>,
    },
    /// Table bytes that are not `rows x 8192`.
    #[error("table is {actual} bytes, expected {expected}")]
    RowsLength { expected: usize, actual: usize },
    /// Table bytes that do not hash to the recorded `rows_sha256`.
    #[error("rows do not hash to rows_sha256")]
    RowsDigest,
    /// A table whose rows hold another entry count than recorded.
    #[error("rows hold {actual} entries, the table records {expected}")]
    EntryCount { expected: u64, actual: u64 },
    /// A prepared public setup that differs from the published one.
    #[error("prepared public setup differs from public_sha256")]
    PublicDigest {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    /// A map table whose session id does not recompute.
    #[error("session id does not recompute")]
    SessionId,
    /// A scope's tables break a rule of spec 6.3, 6.4 or 10.3.
    #[error("scope layout: {0}")]
    Layout(String),
    /// PIR parameters of another product or geometry than the runtime's.
    #[error(
        "parameters are {params_product}/{params_geometry}, the runtime is {product}/{geometry}"
    )]
    Params {
        product: Product,
        geometry: Geometry,
        params_product: Product,
        params_geometry: Geometry,
    },
    /// A query payload the profile refuses, including any length other than
    /// the geometry's single payload length. A host answers `400`.
    #[error("query payload: {0}")]
    Query(String),
    /// The PIR engine failed while building or answering.
    #[error("PIR engine: {0}")]
    Engine(String),
}
