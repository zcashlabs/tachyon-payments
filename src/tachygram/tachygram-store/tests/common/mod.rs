//! Shared test helpers: fixture chains decoded through `tachyon-chain`,
//! hand-built blocks, an independent oracle and whole-store snapshots.
#![allow(dead_code)]
use std::collections::BTreeMap;
use std::path::Path;
use tachygram_store::{
    Binding, EpochSummary, IndexedBlock, StampOrdinal, StampRecord, Store, TachygramRecord,
};
use tachyon_chain::{
    BlockId, DecodeOptions, EpochSchedule, Hash, Stamp, TachyonBlock, TachyonTx, decode_block,
};
use tachyon_fixtures::json::{self, Placement};
use tachyon_fixtures::{ChainConfig, SyntheticChain, TxMix};

/// The fixtures' default activation height, as on the alpha testnet.
pub const ACTIVATION: u32 = 8;
/// A small epoch length, so a short chain spans several epochs.
pub const EPOCH_LENGTH: u32 = 4;

/// A fixture chain of `blocks` random blocks after genesis, with enough proof
/// stamps that most epochs hold several.
pub fn fixture_chain(seed: u64, blocks: u64) -> SyntheticChain {
    let config = ChainConfig {
        mix: TxMix {
            tachyon_txs: [0, 4],
            proof_percent: 60,
            tachygrams: [1, 9],
            ..TxMix::default()
        },
        ..ChainConfig::with_seed(seed)
    };
    SyntheticChain::generate(config, blocks)
}

/// The binding of a fixture chain with `epoch_length`-block epochs.
pub fn fixture_binding(chain: &SyntheticChain, epoch_length: u32) -> Binding {
    Binding {
        chain: chain.chain_name().to_owned(),
        genesis_hash: Hash::from_protocol_bytes(chain.genesis_hash()),
        activation_height: u32::try_from(chain.activation_height().unwrap()).unwrap(),
        epoch_length,
    }
}

/// A binding for hand-built chains.
pub fn hand_binding() -> Binding {
    Binding {
        chain: "test".to_owned(),
        genesis_hash: Hash::from_protocol_bytes([0x5a; 32]),
        activation_height: ACTIVATION,
        epoch_length: EPOCH_LENGTH,
    }
}

pub fn schedule(binding: &Binding) -> EpochSchedule {
    binding.schedule().unwrap()
}

/// The fixture block at `height`, served as `getblock <hash> 2` JSON and
/// decoded by `tachyon-chain`.
pub fn decoded(chain: &SyntheticChain, height: u32) -> TachyonBlock {
    let block = chain.block(u64::from(height)).unwrap();
    let value = json::block(block, Placement::of(chain, block, true), 2);
    let hash = Hash::from_protocol_bytes(block.hash());
    decode_block(&value, &hash, Some(height), DecodeOptions::default()).unwrap()
}

/// The fixture block at `height` as the store takes it.
pub fn indexed(chain: &SyntheticChain, height: u32, schedule: &EpochSchedule) -> IndexedBlock {
    IndexedBlock::new(&decoded(chain, height), schedule)
}

/// Appends fixture blocks `from..=through`.
pub fn append_range(store: &mut Store, chain: &SyntheticChain, from: u32, through: u32) {
    let schedule = store.schedule();
    for height in from..=through {
        store.append(&indexed(chain, height, &schedule)).unwrap();
    }
}

/// One expected stamp: (ordinal, height, tx_index, txid, tachygram count).
pub type StampRow = (u32, u32, u32, [u8; 32], u32);

/// What the store should hold for fixture blocks `from..=through`, computed
/// from the fixture's authored ground truth, never from decoded JSON or the
/// store.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Oracle {
    /// Per epoch: (ordinal, height, tx_index, txid, count) in stamp order.
    pub stamps: BTreeMap<u32, Vec<StampRow>>,
    /// Per epoch: (ordinal, tachygram) in stamp order.
    pub tachygrams: BTreeMap<u32, Vec<(u32, [u8; 32])>>,
    /// Per epoch: (tachygram, height, ordinal) in tachygram order.
    pub records: BTreeMap<u32, Vec<([u8; 32], u32, u32)>>,
    /// Per epoch: (first height, last height).
    pub heights: BTreeMap<u32, (u32, u32)>,
}

impl Oracle {
    pub fn of(chain: &SyntheticChain, from: u32, through: u32, epoch_length: u32) -> Self {
        let activation = u32::try_from(chain.activation_height().unwrap()).unwrap();
        let mut oracle = Oracle::default();
        for height in from..=through {
            let Some(pool) = height.checked_sub(activation) else {
                continue;
            };
            let epoch = pool / epoch_length;
            let span = oracle.heights.entry(epoch).or_insert((height, height));
            span.1 = height;
            let block = chain.block(u64::from(height)).unwrap();
            for tx in block.tachyon_txs() {
                if !tx.stamp.is_proof() {
                    continue;
                }
                let stamps = oracle.stamps.entry(epoch).or_default();
                let ordinal = stamps.len() as u32;
                let tachygrams = tx.stamp.tachygrams();
                stamps.push((
                    ordinal,
                    height,
                    tx.index as u32,
                    tx.txid,
                    tachygrams.len() as u32,
                ));
                for tachygram in tachygrams {
                    oracle
                        .tachygrams
                        .entry(epoch)
                        .or_default()
                        .push((ordinal, *tachygram));
                    oracle
                        .records
                        .entry(epoch)
                        .or_default()
                        .push((*tachygram, height, ordinal));
                }
            }
        }
        for records in oracle.records.values_mut() {
            records.sort();
        }
        oracle
    }

    /// Asserts that `store` holds exactly this.
    pub fn check(&self, store: &Store) {
        let epochs: Vec<u32> = self.heights.keys().copied().collect();
        assert_eq!(store.epochs().unwrap(), epochs);
        for &epoch in &epochs {
            let stamps: Vec<_> = store
                .stamps(epoch)
                .unwrap()
                .into_iter()
                .map(|s| {
                    (
                        s.ordinal.get(),
                        s.height,
                        s.tx_index,
                        *s.txid.protocol_bytes(),
                        s.tachygram_count,
                    )
                })
                .collect();
            let want_stamps = self.stamps.get(&epoch).cloned().unwrap_or_default();
            assert_eq!(stamps, want_stamps, "epoch {epoch} stamps");
            let tachygrams: Vec<_> = store
                .epoch_tachygrams(epoch)
                .unwrap()
                .map(|(o, t)| (o.get(), t))
                .collect();
            let want_tachygrams = self.tachygrams.get(&epoch).cloned().unwrap_or_default();
            assert_eq!(tachygrams, want_tachygrams, "epoch {epoch} tachygrams");
            let records: Vec<_> = store
                .epoch_records(epoch)
                .unwrap()
                .into_iter()
                .map(|r| (r.tachygram, r.blk_height, r.stamp_ordinal.get()))
                .collect();
            let want_records = self.records.get(&epoch).cloned().unwrap_or_default();
            assert_eq!(records, want_records, "epoch {epoch} records");
            let (first_height, last_height) = self.heights[&epoch];
            assert_eq!(
                store.epoch_summary(epoch).unwrap(),
                Some(EpochSummary {
                    epoch,
                    entries: want_records.len() as u64,
                    stamps: want_stamps.len() as u32,
                    first_height,
                    last_height,
                }),
                "epoch {epoch} summary"
            );
        }
    }
}

/// Everything a store answers, for "unchanged" and "identical" comparisons.
#[derive(Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub committed: Option<BlockId>,
    pub hashes: Vec<(u32, Option<Hash>)>,
    pub epochs: Vec<u32>,
    pub stamps: Vec<Vec<StampRecord>>,
    pub records: Vec<Vec<TachygramRecord>>,
    pub tachygrams: Vec<Vec<(StampOrdinal, [u8; 32])>>,
    pub summaries: Vec<Option<EpochSummary>>,
}

impl Snapshot {
    /// The store's answers for heights up to `max_height` and every stored
    /// epoch.
    pub fn of(store: &Store, max_height: u32) -> Self {
        let epochs = store.epochs().unwrap();
        Snapshot {
            committed: store.committed(),
            hashes: (0..=max_height)
                .map(|h| (h, store.block_hash(h).unwrap()))
                .collect(),
            stamps: epochs.iter().map(|&e| store.stamps(e).unwrap()).collect(),
            records: epochs
                .iter()
                .map(|&e| store.epoch_records(e).unwrap())
                .collect(),
            tachygrams: epochs
                .iter()
                .map(|&e| store.epoch_tachygrams(e).unwrap().collect())
                .collect(),
            summaries: epochs
                .iter()
                .map(|&e| store.epoch_summary(e).unwrap())
                .collect(),
            epochs,
        }
    }
}

/// A distinct, nonzero tachygram for `n`.
pub fn tg(n: u32) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[0] = 0x7e;
    bytes[1..5].copy_from_slice(&n.to_be_bytes());
    bytes
}

/// A deterministic hand-built block hash for `height` on branch `branch`.
pub fn hand_hash(height: u32, branch: u8) -> Hash {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&height.to_le_bytes());
    bytes[4] = branch;
    bytes[31] = 0xb1;
    Hash::from_protocol_bytes(bytes)
}

/// A proof stamp listing `tachygrams`.
pub fn proof(tachygrams: Vec<[u8; 32]>) -> Stamp {
    Stamp::Proof {
        anchor: [0xa1; 32],
        tachygram_set: [0xa2; 32],
        tachygrams,
    }
}

/// A pointer stamp.
pub fn pointer() -> Stamp {
    Stamp::Pointer {
        aggregate_id: [0xa3; 64],
    }
}

/// A hand-built block at `height` on branch 0 whose transactions carry
/// `stamps`, at transaction indices 1, 2, ... (the coinbase is 0).
pub fn hand_block(height: u32, prev: Hash, stamps: Vec<Stamp>) -> TachyonBlock {
    let txs = stamps
        .into_iter()
        .enumerate()
        .map(|(i, stamp)| {
            let mut txid = [0u8; 32];
            txid[..4].copy_from_slice(&height.to_le_bytes());
            txid[4] = i as u8;
            txid[31] = 0x71;
            TachyonTx {
                tx_index: i as u32 + 1,
                txid: Hash::from_protocol_bytes(txid),
                memo: Vec::new(),
                own_actions: 1,
                stamp,
            }
        })
        .collect();
    TachyonBlock {
        id: BlockId {
            height,
            hash: hand_hash(height, 0),
        },
        prev,
        txs,
    }
}

/// Appends a hand-built chain starting at `first`, one block per entry of
/// `blocks`, each the stamps of that block. Returns the tip.
pub fn append_hand(store: &mut Store, first: u32, blocks: Vec<Vec<Stamp>>) -> BlockId {
    let schedule = store.schedule();
    let mut prev = store
        .committed()
        .map_or(hand_hash(first.wrapping_sub(1), 0), |tip| tip.hash);
    let mut tip = None;
    for (offset, stamps) in blocks.into_iter().enumerate() {
        let block = hand_block(first + offset as u32, prev, stamps);
        store.append(&IndexedBlock::new(&block, &schedule)).unwrap();
        prev = block.id.hash;
        tip = Some(block.id);
    }
    tip.unwrap()
}

/// Opens a store at `path` with `binding`.
pub fn open(path: &Path, binding: &Binding) -> Store {
    Store::open(path, binding.clone()).unwrap()
}
