//! A block's tachygram content, ready to append (spec sections 8.1 and 8.2).
use std::fmt;
use tachyon_chain::{BlockId, EpochSchedule, Hash, Stamp, TachyonBlock};

/// The zero-based ordinal of a proof stamp among the proof stamps of its epoch,
/// in block order then transaction order (spec 8.1). The store assigns it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StampOrdinal(pub u32);

impl StampOrdinal {
    /// The ordinal as an integer, the `stamp_ordinal` of the record.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for StampOrdinal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// One proof stamp of a block and the tachygrams it lists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProofStamp {
    /// The carrying transaction's index among all transactions of the block,
    /// coinbase included (spec 11.1).
    pub tx_index: u32,
    /// The carrying transaction's id, protocol order.
    pub txid: Hash,
    /// The stamp's tachygrams in the node's order. The count is whatever the
    /// data says; nothing assumes two per action (spec 3).
    pub tachygrams: Vec<[u8; 32]>,
}

/// A block as the journal stores it: its id, its parent, its epoch and its
/// proof stamps.
///
/// Build it with [`IndexedBlock::new`]. The fields are public so that a caller
/// (or a test) can hand-build one, so [`crate::Store::append`] checks every
/// field again: the epoch against the store's own schedule, the stamp order and
/// every tachygram.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedBlock {
    pub id: BlockId,
    /// The parent block's hash, protocol order.
    pub prev: Hash,
    /// The block's epoch, `None` below the activation height.
    pub epoch: Option<u32>,
    /// The block's proof stamps in block order, then transaction order (each
    /// transaction carries one stamp). Pointer stamps contribute nothing: their
    /// actions' tachygrams appear in the covering aggregate's stamp (spec 8.2).
    pub stamps: Vec<ProofStamp>,
}

impl IndexedBlock {
    /// The tachygram content of `block` on a network with `schedule`.
    pub fn new(block: &TachyonBlock, schedule: &EpochSchedule) -> Self {
        let stamps = block
            .txs
            .iter()
            .filter_map(|tx| match &tx.stamp {
                Stamp::Proof { tachygrams, .. } => Some(ProofStamp {
                    tx_index: tx.tx_index,
                    txid: tx.txid,
                    tachygrams: tachygrams.clone(),
                }),
                Stamp::Pointer { .. } => None,
            })
            .collect();
        Self {
            id: block.id,
            prev: block.prev,
            epoch: schedule.epoch_of(block.id.height),
            stamps,
        }
    }

    /// The number of tachygrams over all of the block's proof stamps.
    pub fn tachygram_count(&self) -> usize {
        self.stamps.iter().map(|stamp| stamp.tachygrams.len()).sum()
    }
}
