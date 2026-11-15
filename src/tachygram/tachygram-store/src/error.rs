//! Why the journal refused an operation.
use crate::Binding;
use crate::block::StampOrdinal;
use tachyon_chain::{BlockId, EpochError, Hash};

/// A block whose tachygrams cannot be canonical chain content. Spec 8.3 makes
/// each of these a decoder or node fault: ingestion MUST stop and alert, and
/// MUST NOT publish. The block is not written.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IntegrityFault {
    /// The tachygram is already stored in the same epoch (spec 8.3).
    #[error(
        "block {height}: tachygram {} already appears in epoch {epoch} \
         (block {first_height}, stamp {first_stamp_ordinal})",
        hex(tachygram)
    )]
    DuplicateInEpoch {
        epoch: u32,
        height: u32,
        tachygram: [u8; 32],
        first_height: u32,
        first_stamp_ordinal: StampOrdinal,
    },
    /// The tachygram is listed twice in the block, by one stamp or two
    /// (spec 8.3).
    #[error(
        "block {height}: tachygram {} is listed twice in the block",
        hex(tachygram)
    )]
    DuplicateInBlock { height: u32, tachygram: [u8; 32] },
    /// An all-zero tachygram, which no valid row entry can hold (spec 6.2).
    #[error("block {height}, transaction {tx_index}: all-zero tachygram")]
    ZeroTachygram { height: u32, tx_index: u32 },
    /// A proof stamp in a block below the activation height, which is in no
    /// epoch.
    #[error("block {height}, transaction {tx_index}: proof stamp below the activation height")]
    StampBelowActivation { height: u32, tx_index: u32 },
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
    /// The binding describes no epoch schedule.
    #[error("invalid binding: {0}")]
    InvalidBinding(#[from] EpochError),
    /// The file holds a newer or unknown schema.
    #[error("tachygram store schema version {0} is not supported")]
    UnsupportedSchema(u32),
    /// The file is a SQLite database of something else.
    #[error("the database holds tables but is not a tachygram store")]
    NotAStore,
    /// SQLite did not switch to write-ahead logging.
    #[error("the database journal mode is {0:?}, not wal")]
    JournalMode(String),
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
    /// The block's stamps are not in strictly ascending transaction order.
    #[error("block {height}: stamp of transaction {tx_index} is out of transaction order")]
    StampOrder { height: u32, tx_index: u32 },
    /// The block's tachygrams fail spec 8.3 or 6.2.
    #[error("integrity fault: {0}")]
    Integrity(#[from] IntegrityFault),
    /// A rewind target that is not a stored block.
    #[error("{0:?} is not a stored block")]
    UnknownCheckpoint(BlockId),
    /// A stored value outside its type's range.
    #[error("corrupt store: {0}")]
    Corrupt(&'static str),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
}

impl Error {
    /// The integrity fault, if this is one: spec 8.3 says ingestion MUST stop
    /// and alert on it rather than retry.
    pub fn integrity_fault(&self) -> Option<&IntegrityFault> {
        match self {
            Error::Integrity(fault) => Some(fault),
            _ => None,
        }
    }
}
