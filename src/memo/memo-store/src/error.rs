//! Why the journal refused an operation.
use crate::Binding;
use crate::block::Ordinal;
use tachyon_chain::{BlockId, EpochError, Hash};

/// What is wrong with a hand-built [`crate::IndexedBlock`]'s entries.
/// [`crate::IndexedBlock::new`] never produces any of these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EntryFault {
    /// The ordinal names another block's height.
    #[error("the ordinal names another block")]
    ForeignHeight,
    /// The entry does not follow the previous one of its kind in ordinal
    /// order, or repeats an ordinal used by another entry of the block.
    #[error("out of ordinal order or repeated")]
    Order,
    /// The entry's `key16` is all zero, which no valid envelope carries
    /// (spec 6.2).
    #[error("all-zero key16")]
    ZeroKey,
    /// The entry has an epoch but the block has none (below activation).
    #[error("entry below the activation height")]
    BelowActivation,
}

/// A refused operation. A refused [`crate::Store::append`] or
/// [`crate::Store::rewind`] changes nothing.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The database is bound to another network or epoch schedule.
    #[error("the store is bound to {stored:?}, not {requested:?}")]
    BindingMismatch {
        stored: Box<Binding>,
        requested: Box<Binding>,
    },
    /// The database's spans were decided under other span constants
    /// (`FC_SPAN_TARGET`, `FC_SPAN_MAX_EPOCHS`) than this build's.
    #[error(
        "the store's spans were decided with target {stored_target} and at most \
         {stored_max_epochs} epochs; this build uses {target} and {max_epochs}"
    )]
    SpanRuleMismatch {
        stored_target: u64,
        stored_max_epochs: u32,
        target: u64,
        max_epochs: u32,
    },
    /// The binding describes no epoch schedule.
    #[error("invalid binding: {0}")]
    InvalidBinding(#[from] EpochError),
    /// The file holds a newer or unknown schema.
    #[error("memo store schema version {0} is not supported")]
    UnsupportedSchema(u32),
    /// The file is a SQLite database of something else.
    #[error("the database holds tables but is not a memo store")]
    NotAStore,
    /// SQLite did not switch to write-ahead logging.
    #[error("the database journal mode is {0:?}, not wal")]
    JournalMode(String),
    /// The first block of an empty journal is not the activation block
    /// (spec 7.3: every journal starts at the activation height).
    #[error("the journal starts at the activation height {activation}, not {height}")]
    NotAtActivation { height: u32, activation: u32 },
    /// The block does not follow the stored tip by height.
    #[error("block {height} does not follow the stored tip {tip:?}")]
    HeightGap { tip: BlockId, height: u32 },
    /// The block's parent is not the stored tip.
    #[error("block {height}'s parent {prev} is not the stored tip {tip:?}")]
    ParentMismatch {
        tip: BlockId,
        height: u32,
        prev: Hash,
    },
    /// The block's epoch is not the one the store's schedule gives its height.
    #[error("block {height} claims epoch {claimed:?}; the store's schedule says {expected:?}")]
    EpochMismatch {
        height: u32,
        claimed: Option<u32>,
        expected: Option<u32>,
    },
    /// A hand-built block's entry is malformed.
    #[error("block {height}: {kind:?} entry {ordinal}: {fault}")]
    Entry {
        height: u32,
        kind: crate::Kind,
        ordinal: Ordinal,
        fault: EntryFault,
    },
    /// A rewind target that is not a stored block.
    #[error("{0:?} is not a stored block")]
    UnknownCheckpoint(BlockId),
    /// A first-contact scope id that is neither a closed span's first epoch
    /// nor the open span's.
    #[error("no first-contact span starts at epoch {0}")]
    UnknownSpan(u32),
    /// The stored data contradicts itself.
    #[error("corrupt store: {0}")]
    Corrupt(&'static str),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
}
