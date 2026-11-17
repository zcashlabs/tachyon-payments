//! The independent oracle of the chain products' end-to-end tests (spec 13.1:
//! "oracles are independent").
//!
//! Every expected answer is computed here from the chain the test authored,
//! read through `tachyon-fixtures`' ground-truth accessors (blocks, their
//! Tachyon transactions and stamps as authored, never the served JSON). This
//! module shares no code with the table builder, the journals or the
//! publishers: it does not depend on `tachyon-chain`, `tachyon-pir-protocol`,
//! `tachyon-pir-table` or any product crate. Epoch arithmetic (spec 3), the
//! stamp ordinal (spec 8.1), the scope listing (spec 10.3), sealing (spec 6.5)
//! and the meaning of absence (spec 8.4) are restated from the spec in this
//! file.
use std::collections::{BTreeMap, HashMap};
use tachyon_fixtures::{Stamp, SyntheticChain};

/// `SEAL_DEPTH` (spec 6.5, 15): a scope is sealed once its last block is this
/// many blocks below the publication's anchor.
pub const SEAL_DEPTH: u64 = 100;
/// `CONFIRMATION_DEPTH` (spec 11.2, 15): the publisher ingests to
/// `tip - CONFIRMATION_DEPTH`, which is the anchor of a caught-up map.
pub const CONFIRMATION_DEPTH: u64 = 2;
/// The tachygram product's target load per row times the rows of a
/// `tachyon-4k8` table: the most entries one bucket holds before a second
/// bucket is needed (spec 6.2, 6.3).
pub const TACHYGRAM_BUCKET_ENTRIES: u64 = 124 * 4096;

/// Epochs of the chain products (spec 3): `pool_height = height -
/// activation_height`, `epoch = pool_height / epoch_length`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochSchedule {
    pub activation_height: u64,
    pub epoch_length: u64,
}

impl EpochSchedule {
    pub fn new(activation_height: u64, epoch_length: u64) -> Self {
        assert!(epoch_length > 0, "an epoch has at least one block");
        Self {
            activation_height,
            epoch_length,
        }
    }

    /// The epoch of a block at `height`; `None` below activation.
    pub fn epoch_of(&self, height: u64) -> Option<u64> {
        let pool_height = height.checked_sub(self.activation_height)?;
        Some(pool_height / self.epoch_length)
    }

    /// The first block of `epoch`.
    pub fn first_height(&self, epoch: u64) -> u64 {
        self.activation_height + epoch * self.epoch_length
    }

    /// The last block of `epoch`.
    pub fn last_height(&self, epoch: u64) -> u64 {
        self.first_height(epoch) + self.epoch_length - 1
    }
}

/// One expected tachygram record (spec 8.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TachygramRecord {
    pub tachygram: [u8; 32],
    /// Height of the block holding the proof stamp that lists the tachygram.
    pub blk_height: u32,
    /// Zero-based position of that stamp among the proof stamps of its epoch,
    /// in block order then transaction order.
    pub stamp_ordinal: u32,
}

/// A scope state, as the oracle expects it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Open,
    Sealed,
}

/// One scope of the map a publication at some anchor must list (spec 10.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpectedScope {
    /// The scope index, which for an epoched product is the epoch and the
    /// `scope_id`.
    pub index: u64,
    pub state: State,
    pub first_height: u64,
    /// The epoch's last block, or the anchor for the newest scope.
    pub last_height: u64,
    /// Records of the epoch at heights up to `last_height`.
    pub entries: u64,
}

/// The tachygram product's ground truth for one authored chain.
#[derive(Clone, Debug)]
pub struct TachygramOracle {
    schedule: EpochSchedule,
    tip: u64,
    /// Active block hashes by height, protocol order.
    hashes: Vec<[u8; 32]>,
    /// Every epoch's records in ordinal order: block order, then transaction
    /// order, then the order the stamp lists its tachygrams (spec 8.1, 8.6).
    epochs: BTreeMap<u64, Vec<TachygramRecord>>,
    /// `(epoch, tachygram)` to its position in `epochs[epoch]`.
    index: HashMap<(u64, [u8; 32]), usize>,
}

impl TachygramOracle {
    /// The oracle of the active chain of `chain`, under `epoch_length`-block
    /// epochs from the chain's NuTachyon activation height.
    ///
    /// Panics if an epoch lists a tachygram twice: consensus forbids it, so an
    /// authored chain that does is a test bug (spec 8.3).
    pub fn new(chain: &SyntheticChain, epoch_length: u64) -> Self {
        let activation = chain
            .activation_height()
            .expect("the tachygram product needs NuTachyon activated");
        let schedule = EpochSchedule::new(activation, epoch_length);
        let mut epochs: BTreeMap<u64, Vec<TachygramRecord>> = BTreeMap::new();
        let mut ordinals: BTreeMap<u64, u32> = BTreeMap::new();
        let mut index = HashMap::new();
        let mut hashes = Vec::with_capacity(chain.blocks().len());
        for block in chain.blocks() {
            hashes.push(block.hash());
            // Journals start at activation (spec 7.3); nothing below it is a scope.
            let Some(epoch) = schedule.epoch_of(block.height()) else {
                continue;
            };
            let height = u32::try_from(block.height()).expect("heights fit u32");
            for tx in block.tachyon_txs() {
                // Pointer stamps contribute nothing and are not counted (spec 8.2).
                let Stamp::Proof(proof) = tx.stamp else {
                    continue;
                };
                let ordinal = ordinals.entry(epoch).or_insert(0);
                let stamp_ordinal = *ordinal;
                *ordinal += 1;
                let records = epochs.entry(epoch).or_default();
                for tachygram in &proof.tachygrams {
                    let previous = index.insert((epoch, *tachygram), records.len());
                    assert!(
                        previous.is_none(),
                        "the authored chain repeats tachygram {} in epoch {epoch}",
                        hex::encode(tachygram)
                    );
                    records.push(TachygramRecord {
                        tachygram: *tachygram,
                        blk_height: height,
                        stamp_ordinal,
                    });
                }
            }
        }
        Self {
            schedule,
            tip: chain.tip_height(),
            hashes,
            epochs,
            index,
        }
    }

    pub fn schedule(&self) -> EpochSchedule {
        self.schedule
    }

    pub fn tip(&self) -> u64 {
        self.tip
    }

    /// The anchor of a publication caught up with this chain.
    pub fn anchor(&self) -> u64 {
        self.tip - CONFIRMATION_DEPTH
    }

    /// The active block hash at `height`, protocol order.
    pub fn block_hash(&self, height: u64) -> [u8; 32] {
        self.hashes[usize::try_from(height).expect("height fits usize")]
    }

    /// The scope index of the newest scope a map anchored at `anchor` lists.
    pub fn newest_scope(&self, anchor: u64) -> u64 {
        self.schedule
            .epoch_of(anchor)
            .expect("the anchor is at or above activation")
    }

    /// Whether `epoch` is sealed under `anchor` (spec 6.5).
    pub fn sealed(&self, epoch: u64, anchor: u64) -> bool {
        self.schedule.last_height(epoch) + SEAL_DEPTH <= anchor
    }

    /// The heights scope `epoch` covers in a map anchored at `anchor`: its
    /// whole epoch, or up to the anchor for the newest scope (spec 8.4,
    /// 10.3). `None` for an epoch the map does not list yet.
    pub fn coverage(&self, epoch: u64, anchor: u64) -> Option<(u64, u64)> {
        let newest = self.newest_scope(anchor);
        if epoch > newest {
            return None;
        }
        let last = if epoch == newest {
            anchor
        } else {
            self.schedule.last_height(epoch)
        };
        Some((self.schedule.first_height(epoch), last))
    }

    /// Every scope a map anchored at `anchor` lists, from index 0 (spec 10.3).
    pub fn expected_scopes(&self, anchor: u64) -> Vec<ExpectedScope> {
        (0..=self.newest_scope(anchor))
            .map(|epoch| {
                let (first_height, last_height) =
                    self.coverage(epoch, anchor).expect("listed epoch");
                let entries = self
                    .records(epoch)
                    .iter()
                    .filter(|record| u64::from(record.blk_height) <= last_height)
                    .count() as u64;
                ExpectedScope {
                    index: epoch,
                    state: if self.sealed(epoch, anchor) {
                        State::Sealed
                    } else {
                        State::Open
                    },
                    first_height,
                    last_height,
                    entries,
                }
            })
            .collect()
    }

    /// `n_buckets` of a tachygram scope of `entries` entries (spec 6.3).
    pub fn n_buckets(entries: u64) -> u32 {
        let mut n = 1u32;
        while n < 64 && entries > TACHYGRAM_BUCKET_ENTRIES * u64::from(n) {
            n *= 2;
        }
        n
    }

    /// The expected answer for `tachygram` in scope `epoch` of a map anchored at
    /// `anchor`: its record when the epoch's stamps list it at a covered height,
    /// otherwise `None`, meaning absent within the scope's coverage (spec 8.4).
    pub fn expected(
        &self,
        epoch: u64,
        tachygram: &[u8; 32],
        anchor: u64,
    ) -> Option<TachygramRecord> {
        let (_, last) = self.coverage(epoch, anchor)?;
        let position = self.index.get(&(epoch, *tachygram))?;
        let record = self.epochs[&epoch][*position];
        (u64::from(record.blk_height) <= last).then_some(record)
    }

    /// The records of `epoch` in ordinal order (empty for an epoch without
    /// proof stamps).
    pub fn records(&self, epoch: u64) -> &[TachygramRecord] {
        self.epochs.get(&epoch).map_or(&[], Vec::as_slice)
    }

    /// The epochs whose proof stamps list at least one tachygram.
    pub fn epochs_with_records(&self) -> impl Iterator<Item = u64> + '_ {
        self.epochs
            .iter()
            .filter(|(_, records)| !records.is_empty())
            .map(|(epoch, _)| *epoch)
    }

    /// The epochs in which the authored chain lists `tachygram`.
    pub fn epochs_of(&self, tachygram: &[u8; 32]) -> Vec<u64> {
        self.epochs
            .keys()
            .copied()
            .filter(|epoch| self.index.contains_key(&(*epoch, *tachygram)))
            .collect()
    }
}

/// A small deterministic generator for sampling in tests (xorshift64*), so a
/// failing sample is reproducible from its seed.
#[derive(Clone, Debug)]
pub struct Sampler(u64);

impl Sampler {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// A uniform index below `n`.
    pub fn below(&mut self, n: usize) -> usize {
        assert!(n > 0, "sample from an empty range");
        (self.next_u64() % n as u64) as usize
    }

    /// `k` distinct items of `items` (all of them if `k >= items.len()`), in
    /// their original order.
    pub fn sample<T: Clone>(&mut self, items: &[T], k: usize) -> Vec<T> {
        if k >= items.len() {
            return items.to_vec();
        }
        let mut positions: Vec<usize> = (0..items.len()).collect();
        for i in 0..k {
            let j = i + self.below(items.len() - i);
            positions.swap(i, j);
        }
        let mut chosen = positions[..k].to_vec();
        chosen.sort_unstable();
        chosen.into_iter().map(|i| items[i].clone()).collect()
    }
}
