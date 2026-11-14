//! The decoded Tachyon view of one block (spec section 11.1).
use crate::{BlockId, Hash};

/// Bytes in a tachygram, an anchor and a tachygram set.
pub const TACHYGRAM_BYTES: usize = 32;

/// Bytes in a pointer stamp's `aggregateId`, as the alpha testnet serves it.
pub const AGGREGATE_ID_BYTES: usize = 64;

/// One block's Tachyon content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TachyonBlock {
    pub id: BlockId,
    /// The parent block's hash; [`Hash::ZERO`] for the genesis block.
    pub prev: Hash,
    /// Every transaction that carries a `tachyon` bundle, in block order.
    pub txs: Vec<TachyonTx>,
}

/// One transaction's Tachyon bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TachyonTx {
    /// Zero-based index of this transaction among **all** transactions of the
    /// block, the coinbase included, so it is the "tx index in block" of the
    /// spec's entry ordinal (section 5.2). Transactions without a bundle are
    /// skipped, so indices of consecutive [`TachyonTx`]s can have gaps.
    pub tx_index: u32,
    pub txid: Hash,
    /// The bundle's `vMemoTachyon`, empty when the node prints `""`.
    pub memo: Vec<u8>,
    /// The number of actions in this transaction's own bundle.
    pub own_actions: u32,
    pub stamp: Stamp,
}

/// A bundle's stamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stamp {
    /// A stamp carrying a proof. Its tachygrams cover every action it proves,
    /// including actions of other transactions in the block.
    Proof {
        anchor: [u8; 32],
        tachygram_set: [u8; 32],
        /// In the node's order; the count is read from the data and need not be
        /// two per covered action on the alpha testnet.
        tachygrams: Vec<[u8; 32]>,
    },
    /// A stamp naming the aggregate that covers this transaction's actions.
    ///
    /// On the alpha testnet every sampled `aggregateId` is the covering proof
    /// transaction's txid followed by its authorizing-data digest, both in
    /// protocol order; this crate does not rely on that.
    Pointer { aggregate_id: [u8; 64] },
}

impl Stamp {
    /// The tachygrams this stamp lists: a proof stamp's, or none.
    pub fn tachygrams(&self) -> &[[u8; 32]] {
        match self {
            Stamp::Proof { tachygrams, .. } => tachygrams,
            Stamp::Pointer { .. } => &[],
        }
    }

    /// Whether this is a proof stamp.
    pub fn is_proof(&self) -> bool {
        matches!(self, Stamp::Proof { .. })
    }
}
