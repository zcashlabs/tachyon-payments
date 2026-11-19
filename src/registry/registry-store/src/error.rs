//! Why the log refused an operation.
use crate::Binding;

/// A refused operation. A refused [`crate::Store::register`] or
/// [`crate::Store::register_many`] writes nothing.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The log is bound to another network identity (spec 9.2 Identity).
    #[error("the registry log is bound to {stored:?}, not {requested:?}")]
    BindingMismatch {
        stored: Box<Binding>,
        requested: Box<Binding>,
    },
    /// The log's chunk membership was decided under another
    /// `REGISTRY_CHUNK_ENTRIES` than this build's. Chunk ids never change
    /// (spec 9.2), so such a log cannot be served by this build.
    #[error("the registry log uses {stored} entries per chunk; this build uses {build}")]
    ChunkRuleMismatch { stored: u64, build: u64 },
    /// The file holds a newer or unknown schema.
    #[error("registry store schema version {0} is not supported")]
    UnsupportedSchema(u32),
    /// The file is a SQLite database of something else.
    #[error("the database holds tables but is not a registry store")]
    NotAStore,
    /// SQLite did not switch to write-ahead logging.
    #[error("the database journal mode is {0:?}, not wal")]
    JournalMode(String),
    /// The address's `key16` is all zero, which no row entry can carry (spec
    /// 6.2, 9.3). The publisher refuses such a registration before it reaches
    /// the store; the store refuses it as well.
    #[error("address digest {} has an all-zero key16", hex(digest))]
    ZeroKey { digest: [u8; 32] },
    /// A WAL checkpoint could not complete because readers held the log.
    #[error("the WAL checkpoint did not complete; readers are active")]
    CheckpointBusy,
    /// A stored value that breaks the log's invariants: a wrong length, an
    /// address that does not hash to its digest, a gap in sequences.
    #[error("corrupt registry log: {0}")]
    Corrupt(&'static str),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
