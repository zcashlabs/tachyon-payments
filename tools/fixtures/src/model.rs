//! The synthetic chain: authored blocks and transactions, the ground truth that
//! end-to-end tests check the services against.
//!
//! Everything here is held as authored. The JSON the node double serves is
//! rendered from these values (see [`crate::json`]) and never parsed back, so a
//! test comparing a decoder's output with these accessors compares against an
//! independent oracle.
use crate::hash::{Hash32, display, merkle_root, sha256d};
use crate::rng::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// The NuTachyon activation height of the alpha testnet (spec section 3).
pub const DEFAULT_ACTIVATION_HEIGHT: u64 = 8;
/// The alpha testnet's `getblockchaininfo.chain`.
pub const DEFAULT_CHAIN: &str = "test";
/// The stamp fee every sampled testnet transaction pays.
pub const DEFAULT_FEE_ZAT: u64 = 50_000;
/// The block subsidy paid by synthetic coinbases, before fees.
pub const BLOCK_SUBSIDY_ZAT: u64 = 625_000_000;
/// Synthetic transactions expire this many blocks after the block they are
/// authored for, as the sample's do.
pub const EXPIRY_DELTA: u64 = 19;
/// The sampled testnet block time.
pub const DEFAULT_GENESIS_TIME: u64 = 1_512_758_833;
/// About 9 s between testnet blocks (spec section 3).
pub const DEFAULT_BLOCK_INTERVAL: u64 = 9;
/// The transaction version the testnet's Tachyon transactions report.
pub const TX_VERSION: u32 = 7;
/// The block version the testnet reports.
pub const BLOCK_VERSION: u32 = 4;

/// Magic of a synthetic transaction's bytes (the `hex` field).
const TX_MAGIC: &[u8; 4] = b"TFX1";
/// Magic of a synthetic block header.
const BLOCK_MAGIC: &[u8; 4] = b"TFB1";

/// One Tachyon action as `getblock` prints it. The fixtures treat its values as
/// opaque filler: they are not curve points or signatures.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Action {
    pub cv: [u8; 32],
    pub rk: [u8; 32],
    pub spend_auth_sig: [u8; 64],
}

/// A proof stamp. `tachygrams` are held exactly as authored: a test may author
/// them out of order or repeated to exercise the decoder's checks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProofStamp {
    pub anchor: [u8; 32],
    pub tachygram_set: [u8; 32],
    pub tachygrams: Vec<[u8; 32]>,
    pub proof: Vec<u8>,
    pub coverage: [u8; 32],
    pub fee_zat: u64,
}

/// A pointer stamp: it names an aggregate and carries no tachygrams.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PointerStamp {
    pub aggregate_id: [u8; 64],
    pub fee_zat: u64,
}

/// A transaction's stamp.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Stamp {
    Proof(ProofStamp),
    Pointer(PointerStamp),
}

impl Stamp {
    /// The stamp's tachygrams; none for a pointer stamp.
    pub fn tachygrams(&self) -> &[[u8; 32]] {
        match self {
            Stamp::Proof(proof) => &proof.tachygrams,
            Stamp::Pointer(_) => &[],
        }
    }

    /// The stamp's fee.
    pub fn fee_zat(&self) -> u64 {
        match self {
            Stamp::Proof(proof) => proof.fee_zat,
            Stamp::Pointer(pointer) => pointer.fee_zat,
        }
    }

    /// Whether this is a proof stamp.
    pub fn is_proof(&self) -> bool {
        matches!(self, Stamp::Proof(_))
    }
}

/// A transaction's Tachyon bundle, the `tx[].tachyon` object.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TachyonBundle {
    pub actions: Vec<Action>,
    pub value_balance_zat: i64,
    pub cv_fee_max: [u8; 32],
    pub binding_sig: [u8; 64],
    /// `vMemoTachyon`, arbitrary bytes; printed as hex, `""` when empty.
    pub memo: Vec<u8>,
    pub stamp: Stamp,
}

/// A transparent coinbase. It has no Tachyon bundle.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Coinbase {
    pub height: u64,
    pub value_zat: u64,
    /// Miner data, which also keeps coinbases of competing blocks distinct.
    pub extra: [u8; 8],
    /// The pay-to-public-key-hash recipient.
    pub pubkey_hash: [u8; 20],
}

/// What a transaction carries.
// Fixture data: a few hundred bytes per transaction is immaterial, and an unboxed
// bundle keeps pattern matching in tests plain.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TxBody {
    Coinbase(Coinbase),
    Tachyon(TachyonBundle),
}

/// A synthetic transaction. Its bytes are a fixture encoding, not the node's wire
/// format; its txid is the double SHA-256 of those bytes, so the same content has
/// the same txid wherever it is mined, as on a real chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tx {
    body: TxBody,
    expiry_height: u64,
    bytes: Vec<u8>,
    txid: Hash32,
    auth_digest: Hash32,
}

impl Tx {
    /// The transaction with `body` expiring at `expiry_height`.
    pub fn new(body: TxBody, expiry_height: u64) -> Self {
        let bytes = encode_tx(&body, expiry_height);
        let txid = sha256d(&[b"txid", &bytes]);
        let auth_digest = sha256d(&[b"auth", &bytes]);
        Self {
            body,
            expiry_height,
            bytes,
            txid,
            auth_digest,
        }
    }

    /// The transaction id in protocol order.
    pub fn txid(&self) -> Hash32 {
        self.txid
    }

    /// The transaction id in display order, as the node prints it.
    pub fn txid_display(&self) -> String {
        display(&self.txid)
    }

    /// The `authdigest` in protocol order.
    pub fn auth_digest(&self) -> Hash32 {
        self.auth_digest
    }

    pub fn body(&self) -> &TxBody {
        &self.body
    }

    pub fn expiry_height(&self) -> u64 {
        self.expiry_height
    }

    /// The fixture encoding printed as the transaction's `hex`.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The transaction's `size`: the length of [`Self::bytes`].
    pub fn size(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_coinbase(&self) -> bool {
        matches!(self.body, TxBody::Coinbase(_))
    }

    /// The Tachyon bundle, if the transaction has one.
    pub fn tachyon(&self) -> Option<&TachyonBundle> {
        match &self.body {
            TxBody::Tachyon(bundle) => Some(bundle),
            TxBody::Coinbase(_) => None,
        }
    }

    /// The coinbase, if this is one.
    pub fn coinbase(&self) -> Option<&Coinbase> {
        match &self.body {
            TxBody::Coinbase(coinbase) => Some(coinbase),
            TxBody::Tachyon(_) => None,
        }
    }

    /// The number of the transaction's own Tachyon actions (zero for a coinbase).
    pub fn own_actions(&self) -> u32 {
        self.tachyon()
            .map_or(0, |bundle| bundle.actions.len() as u32)
    }

    /// The Tachyon memo bytes, if the transaction has a bundle.
    pub fn memo(&self) -> Option<&[u8]> {
        self.tachyon().map(|bundle| bundle.memo.as_slice())
    }

    /// The stamp, if the transaction has a bundle.
    pub fn stamp(&self) -> Option<&Stamp> {
        self.tachyon().map(|bundle| &bundle.stamp)
    }

    /// The fee the transaction pays (its stamp fee; zero for a coinbase).
    pub fn fee_zat(&self) -> u64 {
        self.stamp().map_or(0, Stamp::fee_zat)
    }
}

fn encode_tx(body: &TxBody, expiry_height: u64) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(TX_MAGIC);
    out.extend_from_slice(&TX_VERSION.to_le_bytes());
    out.extend_from_slice(&expiry_height.to_le_bytes());
    match body {
        TxBody::Coinbase(coinbase) => {
            out.push(0);
            out.extend_from_slice(&coinbase.height.to_le_bytes());
            out.extend_from_slice(&coinbase.value_zat.to_le_bytes());
            out.extend_from_slice(&coinbase.extra);
            out.extend_from_slice(&coinbase.pubkey_hash);
        }
        TxBody::Tachyon(bundle) => {
            out.push(1);
            out.extend_from_slice(&(bundle.actions.len() as u32).to_le_bytes());
            for action in &bundle.actions {
                out.extend_from_slice(&action.cv);
                out.extend_from_slice(&action.rk);
                out.extend_from_slice(&action.spend_auth_sig);
            }
            out.extend_from_slice(&bundle.value_balance_zat.to_le_bytes());
            out.extend_from_slice(&bundle.cv_fee_max);
            out.extend_from_slice(&bundle.binding_sig);
            out.extend_from_slice(&(bundle.memo.len() as u32).to_le_bytes());
            out.extend_from_slice(&bundle.memo);
            match &bundle.stamp {
                Stamp::Proof(proof) => {
                    out.push(0);
                    out.extend_from_slice(&proof.fee_zat.to_le_bytes());
                    out.extend_from_slice(&proof.anchor);
                    out.extend_from_slice(&proof.tachygram_set);
                    out.extend_from_slice(&(proof.tachygrams.len() as u32).to_le_bytes());
                    for tachygram in &proof.tachygrams {
                        out.extend_from_slice(tachygram);
                    }
                    out.extend_from_slice(&proof.coverage);
                    out.extend_from_slice(&(proof.proof.len() as u32).to_le_bytes());
                    out.extend_from_slice(&proof.proof);
                }
                Stamp::Pointer(pointer) => {
                    out.push(1);
                    out.extend_from_slice(&pointer.fee_zat.to_le_bytes());
                    out.extend_from_slice(&pointer.aggregate_id);
                }
            }
        }
    }
    out
}

/// The ground truth of one Tachyon transaction, as authored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TachyonTxTruth<'a> {
    /// Position in the block's `tx` list (the coinbase is 0).
    pub index: usize,
    /// Protocol order.
    pub txid: Hash32,
    pub own_actions: u32,
    pub memo: &'a [u8],
    pub stamp: &'a Stamp,
}

/// A synthetic block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    height: u64,
    hash: Hash32,
    prev: Hash32,
    time: u64,
    salt: u64,
    merkle_root: Hash32,
    txs: Vec<Tx>,
    transparent_delta_zat: i64,
    tachyon_delta_zat: i64,
    transparent_supply_zat: i64,
    tachyon_supply_zat: i64,
}

impl Block {
    pub fn height(&self) -> u64 {
        self.height
    }

    /// The block hash in protocol order.
    pub fn hash(&self) -> Hash32 {
        self.hash
    }

    /// The block hash in display order, as the node prints it.
    pub fn hash_display(&self) -> String {
        display(&self.hash)
    }

    /// The previous block's hash in protocol order; all zeros for genesis.
    pub fn prev(&self) -> Hash32 {
        self.prev
    }

    pub fn time(&self) -> u64 {
        self.time
    }

    /// The fork salt the hash commits to: the chain's reorg generation when the
    /// block was authored.
    pub fn salt(&self) -> u64 {
        self.salt
    }

    /// The Merkle root of the txids, protocol order.
    pub fn merkle_root(&self) -> Hash32 {
        self.merkle_root
    }

    /// The transactions, coinbase first.
    pub fn txs(&self) -> &[Tx] {
        &self.txs
    }

    /// The txids in protocol order, in block order.
    pub fn txids(&self) -> Vec<Hash32> {
        self.txs.iter().map(Tx::txid).collect()
    }

    /// The block's Tachyon transactions as authored, in block order.
    pub fn tachyon_txs(&self) -> impl Iterator<Item = TachyonTxTruth<'_>> {
        self.txs.iter().enumerate().filter_map(|(index, tx)| {
            tx.tachyon().map(|bundle| TachyonTxTruth {
                index,
                txid: tx.txid(),
                own_actions: bundle.actions.len() as u32,
                memo: &bundle.memo,
                stamp: &bundle.stamp,
            })
        })
    }

    /// Every tachygram of the block's proof stamps, in block and stamp order.
    pub fn tachygrams(&self) -> Vec<[u8; 32]> {
        self.txs
            .iter()
            .filter_map(Tx::stamp)
            .flat_map(|stamp| stamp.tachygrams().iter().copied())
            .collect()
    }

    /// The synthetic header bytes the hash commits to.
    pub fn header_bytes(&self) -> Vec<u8> {
        header_bytes(
            &self.prev,
            &self.merkle_root,
            self.height,
            self.time,
            self.salt,
        )
    }

    /// The fixture encoding of the whole block: header, transaction count and each
    /// transaction's bytes. `getblock` at verbosity 0 prints it.
    pub fn bytes(&self) -> Vec<u8> {
        let mut out = self.header_bytes();
        out.extend_from_slice(&(self.txs.len() as u32).to_le_bytes());
        for tx in &self.txs {
            out.extend_from_slice(tx.bytes());
        }
        out
    }

    /// The block's `size`.
    pub fn size(&self) -> usize {
        self.header_bytes().len() + 4 + self.txs.iter().map(Tx::size).sum::<usize>()
    }

    /// Change in the transparent pool's value from this block (coinbase outputs).
    pub fn transparent_delta_zat(&self) -> i64 {
        self.transparent_delta_zat
    }

    /// Change in the Tachyon pool's value from this block: minus the sum of the
    /// bundles' value balances.
    pub fn tachyon_delta_zat(&self) -> i64 {
        self.tachyon_delta_zat
    }

    /// The transparent pool's value after this block, along its own branch.
    pub fn transparent_supply_zat(&self) -> i64 {
        self.transparent_supply_zat
    }

    /// The Tachyon pool's value after this block, along its own branch.
    pub fn tachyon_supply_zat(&self) -> i64 {
        self.tachyon_supply_zat
    }
}

fn header_bytes(prev: &Hash32, merkle: &Hash32, height: u64, time: u64, salt: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 4 + 32 + 32 + 24);
    out.extend_from_slice(BLOCK_MAGIC);
    out.extend_from_slice(&BLOCK_VERSION.to_le_bytes());
    out.extend_from_slice(prev);
    out.extend_from_slice(merkle);
    out.extend_from_slice(&height.to_le_bytes());
    out.extend_from_slice(&time.to_le_bytes());
    out.extend_from_slice(&salt.to_le_bytes());
    out
}

/// How a transaction's actions are chosen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionsSpec {
    /// This many actions with random filler values.
    Random(u32),
    /// Exactly these actions.
    Explicit(Vec<Action>),
}

/// How a transaction's stamp is chosen. Filler (anchor, set, coverage, proof,
/// aggregate id) is random unless the stamp is [`StampSpec::Exact`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StampSpec {
    /// A pointer stamp with a random aggregate id.
    Pointer,
    /// A proof stamp with this many fresh random tachygrams in strictly ascending
    /// order, distinct from every tachygram the chain has used.
    RandomProof(u32),
    /// A proof stamp with exactly these tachygrams, in this order (duplicates and
    /// disorder allowed, for negative tests).
    Proof(Vec<[u8; 32]>),
    /// Exactly this stamp, fee included.
    Exact(Stamp),
}

/// A Tachyon transaction to author.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxSpec {
    pub actions: ActionsSpec,
    pub memo: Vec<u8>,
    pub stamp: StampSpec,
    /// The stamp fee, unless the stamp is [`StampSpec::Exact`].
    pub fee_zat: u64,
    pub value_balance_zat: i64,
    pub cv_fee_max: Option<[u8; 32]>,
    pub binding_sig: Option<[u8; 64]>,
    /// Defaults to the block height plus [`EXPIRY_DELTA`].
    pub expiry_height: Option<u64>,
}

impl TxSpec {
    /// A Tachyon transaction with `actions` random actions, an empty memo and a
    /// pointer stamp.
    pub fn tachyon(actions: u32) -> Self {
        Self {
            actions: ActionsSpec::Random(actions),
            memo: Vec::new(),
            stamp: StampSpec::Pointer,
            fee_zat: DEFAULT_FEE_ZAT,
            value_balance_zat: 0,
            cv_fee_max: None,
            binding_sig: None,
            expiry_height: None,
        }
    }

    /// With this memo.
    pub fn memo(mut self, memo: impl Into<Vec<u8>>) -> Self {
        self.memo = memo.into();
        self
    }

    /// With a pointer stamp.
    pub fn pointer(mut self) -> Self {
        self.stamp = StampSpec::Pointer;
        self
    }

    /// With a proof stamp holding exactly `tachygrams`.
    pub fn proof(mut self, tachygrams: Vec<[u8; 32]>) -> Self {
        self.stamp = StampSpec::Proof(tachygrams);
        self
    }

    /// With a proof stamp holding `count` fresh ascending tachygrams.
    pub fn random_proof(mut self, count: u32) -> Self {
        self.stamp = StampSpec::RandomProof(count);
        self
    }

    /// With exactly this stamp.
    pub fn stamp(mut self, stamp: Stamp) -> Self {
        self.stamp = StampSpec::Exact(stamp);
        self
    }

    /// With exactly these actions.
    pub fn with_actions(mut self, actions: Vec<Action>) -> Self {
        self.actions = ActionsSpec::Explicit(actions);
        self
    }

    pub fn fee(mut self, fee_zat: u64) -> Self {
        self.fee_zat = fee_zat;
        self
    }

    pub fn value_balance(mut self, value_balance_zat: i64) -> Self {
        self.value_balance_zat = value_balance_zat;
        self
    }

    pub fn expiry(mut self, expiry_height: u64) -> Self {
        self.expiry_height = Some(expiry_height);
        self
    }

    /// The spec reproducing `tx` exactly, so it keeps its txid when mined again
    /// (for example on the other side of a reorg). `None` for a coinbase.
    pub fn from_tx(tx: &Tx) -> Option<Self> {
        let bundle = tx.tachyon()?;
        Some(Self {
            actions: ActionsSpec::Explicit(bundle.actions.clone()),
            memo: bundle.memo.clone(),
            stamp: StampSpec::Exact(bundle.stamp.clone()),
            fee_zat: bundle.stamp.fee_zat(),
            value_balance_zat: bundle.value_balance_zat,
            cv_fee_max: Some(bundle.cv_fee_max),
            binding_sig: Some(bundle.binding_sig),
            expiry_height: Some(tx.expiry_height()),
        })
    }
}

/// A block to author: a coinbase is always added first, then `txs`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockSpec {
    pub txs: Vec<TxSpec>,
    /// Defaults to the genesis time plus the height times the block interval.
    pub time: Option<u64>,
}

impl BlockSpec {
    /// A block with only its coinbase.
    pub fn new() -> Self {
        Self::default()
    }

    /// With `tx` appended.
    pub fn tx(mut self, tx: TxSpec) -> Self {
        self.txs.push(tx);
        self
    }

    /// The spec reproducing `block`'s Tachyon transactions (a new coinbase is
    /// authored).
    pub fn from_block(block: &Block) -> Self {
        Self {
            txs: block.txs().iter().filter_map(TxSpec::from_tx).collect(),
            time: Some(block.time()),
        }
    }
}

/// The random content of generated blocks. Ranges are inclusive `[min, max]`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TxMix {
    /// Tachyon transactions per block at or above activation.
    pub tachyon_txs: [u32; 2],
    /// Own actions per Tachyon transaction.
    pub actions: [u32; 2],
    /// Chance, in percent, that a Tachyon transaction has a proof stamp rather
    /// than a pointer stamp.
    pub proof_percent: u8,
    /// Tachygrams per proof stamp; odd counts occur.
    pub tachygrams: [u32; 2],
    /// Chance, in percent, that a Tachyon transaction has a non-empty memo.
    pub memo_percent: u8,
    /// Length of a non-empty memo.
    pub memo_len: [u32; 2],
}

impl Default for TxMix {
    fn default() -> Self {
        Self {
            tachyon_txs: [0, 4],
            actions: [1, 3],
            proof_percent: 30,
            tachygrams: [5, 21],
            memo_percent: 50,
            memo_len: [1, 120],
        }
    }
}

/// A synthetic network and its generator settings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChainConfig {
    /// Seeds every random value, the genesis block included.
    pub seed: u64,
    /// `getblockchaininfo.chain`.
    pub chain: String,
    /// The NuTachyon activation height; `None` (JSON `null`) leaves the upgrade
    /// out of `getblockchaininfo` and generates no Tachyon transactions.
    pub activation_height: Option<u64>,
    pub genesis_time: u64,
    pub block_interval: u64,
    pub mix: TxMix,
}

impl Default for ChainConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            chain: DEFAULT_CHAIN.to_owned(),
            activation_height: Some(DEFAULT_ACTIVATION_HEIGHT),
            genesis_time: DEFAULT_GENESIS_TIME,
            block_interval: DEFAULT_BLOCK_INTERVAL,
            mix: TxMix::default(),
        }
    }
}

impl ChainConfig {
    /// The default network with `seed`.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            seed,
            ..Self::default()
        }
    }
}

/// A refused chain edit.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FixtureError {
    #[error("fork height {fork} is above the tip {tip}")]
    ForkAboveTip { fork: u64, tip: u64 },
    #[error("invalid chain spec: {0}")]
    Spec(String),
}

/// A block found by hash, and whether it is on the active chain.
#[derive(Clone, Copy, Debug)]
pub struct Found<'a> {
    pub block: &'a Block,
    pub active: bool,
}

/// A deterministic synthetic chain: the same config and the same sequence of
/// edits always produce the same blocks.
#[derive(Clone, Debug)]
pub struct SyntheticChain {
    config: ChainConfig,
    rng: Rng,
    /// The active chain; index is height.
    blocks: Vec<Block>,
    /// Height of each active block by protocol-order hash.
    index: HashMap<Hash32, u64>,
    /// Blocks reorged away, by hash.
    stale: HashMap<Hash32, Block>,
    /// Reorgs so far; the fork salt of newly authored blocks.
    generation: u64,
    /// Every tachygram authored so far, on any branch.
    used_tachygrams: HashSet<[u8; 32]>,
}

impl SyntheticChain {
    /// A chain holding only its genesis block.
    pub fn new(config: ChainConfig) -> Self {
        let rng = Rng::new("chain", config.seed);
        let mut chain = Self {
            config,
            rng,
            blocks: Vec::new(),
            index: HashMap::new(),
            stale: HashMap::new(),
            generation: 0,
            used_tachygrams: HashSet::new(),
        };
        chain.push(BlockSpec::new());
        chain
    }

    /// A chain with `config` and `random_blocks` generated blocks after genesis.
    pub fn generate(config: ChainConfig, random_blocks: u64) -> Self {
        let mut chain = Self::new(config);
        chain.append_random(random_blocks);
        chain
    }

    pub fn config(&self) -> &ChainConfig {
        &self.config
    }

    /// `getblockchaininfo.chain`.
    pub fn chain_name(&self) -> &str {
        &self.config.chain
    }

    /// The genesis block hash, protocol order.
    pub fn genesis_hash(&self) -> Hash32 {
        self.blocks[0].hash
    }

    pub fn activation_height(&self) -> Option<u64> {
        self.config.activation_height
    }

    pub fn tip_height(&self) -> u64 {
        self.blocks.len() as u64 - 1
    }

    /// The tip block.
    pub fn tip(&self) -> &Block {
        self.blocks.last().expect("genesis always exists")
    }

    /// The tip hash, protocol order.
    pub fn tip_hash(&self) -> Hash32 {
        self.tip().hash
    }

    /// The active block at `height`.
    pub fn block(&self, height: u64) -> Option<&Block> {
        self.blocks.get(usize::try_from(height).ok()?)
    }

    /// The active chain, genesis first.
    pub fn blocks(&self) -> &[Block] {
        &self.blocks
    }

    /// The block with protocol-order `hash`, active or reorged away.
    pub fn lookup(&self, hash: &Hash32) -> Option<Found<'_>> {
        if let Some(height) = self.index.get(hash) {
            return Some(Found {
                block: &self.blocks[*height as usize],
                active: true,
            });
        }
        self.stale.get(hash).map(|block| Found {
            block,
            active: false,
        })
    }

    /// Blocks reorged away, in no particular order.
    pub fn stale_blocks(&self) -> impl Iterator<Item = &Block> {
        self.stale.values()
    }

    /// The number of reorgs so far.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Every tachygram on the active chain, in chain order.
    pub fn active_tachygrams(&self) -> Vec<[u8; 32]> {
        self.blocks.iter().flat_map(Block::tachygrams).collect()
    }

    /// Appends a block authored from `spec`; returns its hash (protocol order).
    pub fn append(&mut self, spec: BlockSpec) -> Hash32 {
        self.push(spec)
    }

    /// Appends `count` blocks of random content drawn from the config's mix.
    pub fn append_random(&mut self, count: u64) {
        for _ in 0..count {
            let spec = self.random_block_spec(self.tip_height() + 1);
            self.push(spec);
        }
    }

    /// Replaces every block above `fork_height` with blocks authored from
    /// `new_blocks`. Replacement blocks commit to a new fork salt, so their hashes
    /// differ from the blocks they replace even for identical content. The old
    /// blocks stay retrievable by hash through [`Self::lookup`].
    pub fn reorg(
        &mut self,
        fork_height: u64,
        new_blocks: Vec<BlockSpec>,
    ) -> Result<(), FixtureError> {
        self.truncate(fork_height)?;
        for spec in new_blocks {
            self.push(spec);
        }
        Ok(())
    }

    /// [`Self::reorg`] with `count` random replacement blocks.
    pub fn reorg_random(&mut self, fork_height: u64, count: u64) -> Result<(), FixtureError> {
        self.truncate(fork_height)?;
        self.append_random(count);
        Ok(())
    }

    /// `count` fresh random tachygrams in strictly ascending byte order, distinct
    /// from every tachygram the chain has authored or handed out, and reserved so
    /// later random ones avoid them too.
    pub fn fresh_tachygrams(&mut self, count: u32) -> Vec<[u8; 32]> {
        let mut out = Vec::with_capacity(count as usize);
        while out.len() < count as usize {
            let tachygram: [u8; 32] = self.rng.array();
            if self.used_tachygrams.insert(tachygram) {
                out.push(tachygram);
            }
        }
        out.sort();
        out
    }

    /// `len` random bytes from the chain's stream, for test memos and filler.
    pub fn random_bytes(&mut self, len: usize) -> Vec<u8> {
        self.rng.bytes(len)
    }

    /// The random block content the mix would author at `height`; it draws from
    /// the chain's stream.
    pub fn random_block_spec(&mut self, height: u64) -> BlockSpec {
        let active = self
            .config
            .activation_height
            .is_some_and(|activation| height >= activation);
        if !active {
            return BlockSpec::new();
        }
        let mix = self.config.mix.clone();
        let count = self.range(mix.tachyon_txs);
        let mut spec = BlockSpec::new();
        for _ in 0..count {
            let actions = self.range(mix.actions);
            let mut tx = TxSpec::tachyon(actions);
            if self.rng.percent(mix.memo_percent) {
                let len = self.range(mix.memo_len);
                tx = tx.memo(self.rng.bytes(len as usize));
            }
            if self.rng.percent(mix.proof_percent) {
                tx = tx.random_proof(self.range(mix.tachygrams));
            }
            spec = spec.tx(tx);
        }
        spec
    }

    fn range(&mut self, [low, high]: [u32; 2]) -> u32 {
        self.rng.range(u64::from(low), u64::from(high)) as u32
    }

    fn truncate(&mut self, fork_height: u64) -> Result<(), FixtureError> {
        let tip = self.tip_height();
        if fork_height > tip {
            return Err(FixtureError::ForkAboveTip {
                fork: fork_height,
                tip,
            });
        }
        for block in self.blocks.drain(fork_height as usize + 1..) {
            self.index.remove(&block.hash);
            self.stale.insert(block.hash, block);
        }
        self.generation += 1;
        Ok(())
    }

    fn push(&mut self, spec: BlockSpec) -> Hash32 {
        let height = self.blocks.len() as u64;
        let (prev, transparent_before, tachyon_before) = match self.blocks.last() {
            Some(parent) => (
                parent.hash,
                parent.transparent_supply_zat,
                parent.tachyon_supply_zat,
            ),
            None => ([0; 32], 0, 0),
        };
        let mut txs: Vec<Tx> = spec
            .txs
            .into_iter()
            .map(|tx| self.build_tx(tx, height))
            .collect();
        let fees: u64 = txs.iter().map(Tx::fee_zat).sum();
        let coinbase = Coinbase {
            height,
            value_zat: BLOCK_SUBSIDY_ZAT + fees,
            extra: self.rng.array(),
            pubkey_hash: self.rng.array(),
        };
        txs.insert(0, Tx::new(TxBody::Coinbase(coinbase), height));
        let transparent_delta_zat = (BLOCK_SUBSIDY_ZAT + fees) as i64;
        let tachyon_delta_zat: i64 = -txs
            .iter()
            .filter_map(Tx::tachyon)
            .map(|bundle| bundle.value_balance_zat)
            .sum::<i64>();
        let time = spec
            .time
            .unwrap_or(self.config.genesis_time + height * self.config.block_interval);
        let merkle = merkle_root(&txs.iter().map(Tx::txid).collect::<Vec<_>>());
        let salt = self.generation;
        let hash = sha256d(&[&header_bytes(&prev, &merkle, height, time, salt)]);
        let block = Block {
            height,
            hash,
            prev,
            time,
            salt,
            merkle_root: merkle,
            txs,
            transparent_delta_zat,
            tachyon_delta_zat,
            transparent_supply_zat: transparent_before + transparent_delta_zat,
            tachyon_supply_zat: tachyon_before + tachyon_delta_zat,
        };
        self.index.insert(hash, height);
        self.blocks.push(block);
        hash
    }

    fn build_tx(&mut self, spec: TxSpec, height: u64) -> Tx {
        let actions = match spec.actions {
            ActionsSpec::Explicit(actions) => actions,
            ActionsSpec::Random(count) => (0..count)
                .map(|_| Action {
                    cv: self.rng.array(),
                    rk: self.rng.array(),
                    spend_auth_sig: self.rng.array(),
                })
                .collect(),
        };
        let stamp = match spec.stamp {
            StampSpec::Exact(stamp) => stamp,
            StampSpec::Pointer => Stamp::Pointer(PointerStamp {
                aggregate_id: self.rng.array(),
                fee_zat: spec.fee_zat,
            }),
            StampSpec::RandomProof(count) => {
                let tachygrams = self.fresh_tachygrams(count);
                self.proof_stamp(tachygrams, spec.fee_zat)
            }
            StampSpec::Proof(tachygrams) => self.proof_stamp(tachygrams, spec.fee_zat),
        };
        self.used_tachygrams
            .extend(stamp.tachygrams().iter().copied());
        let bundle = TachyonBundle {
            actions,
            value_balance_zat: spec.value_balance_zat,
            cv_fee_max: spec.cv_fee_max.unwrap_or_else(|| self.rng.array()),
            binding_sig: spec.binding_sig.unwrap_or_else(|| self.rng.array()),
            memo: spec.memo,
            stamp,
        };
        Tx::new(
            TxBody::Tachyon(bundle),
            spec.expiry_height.unwrap_or(height + EXPIRY_DELTA),
        )
    }

    fn proof_stamp(&mut self, tachygrams: Vec<[u8; 32]>, fee_zat: u64) -> Stamp {
        let proof_len = self.rng.range(200, 300) as usize;
        Stamp::Proof(ProofStamp {
            anchor: self.rng.array(),
            tachygram_set: self.rng.array(),
            tachygrams,
            proof: self.rng.bytes(proof_len),
            coverage: self.rng.array(),
            fee_zat,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_chain_other_seed_other_chain() {
        let a = SyntheticChain::generate(ChainConfig::with_seed(1), 30);
        let b = SyntheticChain::generate(ChainConfig::with_seed(1), 30);
        let c = SyntheticChain::generate(ChainConfig::with_seed(2), 30);
        assert_eq!(a.blocks(), b.blocks());
        assert_ne!(a.genesis_hash(), c.genesis_hash());
    }

    #[test]
    fn blocks_link_and_respect_activation() {
        let chain = SyntheticChain::generate(ChainConfig::with_seed(3), 40);
        assert_eq!(chain.tip_height(), 40);
        for (height, block) in chain.blocks().iter().enumerate() {
            assert_eq!(block.height(), height as u64);
            assert!(block.txs()[0].is_coinbase());
            if height > 0 {
                assert_eq!(block.prev(), chain.blocks()[height - 1].hash());
            }
            if height < DEFAULT_ACTIVATION_HEIGHT as usize {
                assert_eq!(block.txs().len(), 1);
            }
        }
        assert!(chain.blocks().iter().any(|b| b.txs().len() > 1));
    }

    #[test]
    fn random_tachygrams_ascend_and_never_repeat() {
        let chain = SyntheticChain::generate(ChainConfig::with_seed(4), 120);
        let mut seen = HashSet::new();
        let mut odd = false;
        for block in chain.blocks() {
            for tx in block.tachyon_txs() {
                let tachygrams = tx.stamp.tachygrams();
                odd |= tachygrams.len() % 2 == 1;
                assert!(tachygrams.windows(2).all(|w| w[0] < w[1]));
                for tachygram in tachygrams {
                    assert!(seen.insert(*tachygram));
                }
            }
        }
        assert!(odd && !seen.is_empty());
    }

    #[test]
    fn a_tx_mined_again_keeps_its_txid() {
        let mut chain = SyntheticChain::new(ChainConfig::with_seed(5));
        chain.append_random(8);
        chain.append(BlockSpec::new().tx(TxSpec::tachyon(2).random_proof(3).memo(*b"hi")));
        let original = chain.tip().clone();
        chain
            .reorg(
                original.height() - 1,
                vec![BlockSpec::from_block(&original)],
            )
            .unwrap();
        let again = chain.tip();
        assert_ne!(again.hash(), original.hash());
        assert_eq!(again.txs()[1], original.txs()[1]);
        assert_ne!(again.txs()[0].txid(), original.txs()[0].txid());
    }

    #[test]
    fn reorg_above_tip_is_refused() {
        let mut chain = SyntheticChain::new(ChainConfig::default());
        assert_eq!(
            chain.reorg(3, Vec::new()),
            Err(FixtureError::ForkAboveTip { fork: 3, tip: 0 })
        );
    }
}
