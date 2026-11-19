//! The independent oracle of the memo and first-contact end-to-end tests
//! (spec 13.1: "oracles are independent").
//!
//! Every expected answer is computed here from the chain the test authored,
//! read through `tachyon-fixtures`' envelope ground truth: the entries of
//! valid envelopes as authored ([`SyntheticChain::follow_ups`],
//! [`SyntheticChain::first_contacts`], in ordinal order) and each block's
//! [`EnvelopeCounts`]. Nothing here parses a memo, and the module shares no
//! code with the envelope parser, the journal, the table builder or the
//! publishers: it depends on none of `tachyon-chain`, `tachyon-pir-protocol`,
//! `tachyon-pir-table`, `memo-store` or `memo-publisher`.
//!
//! Restated from the spec in this file:
//!
//! - epochs (spec 3) through [`EpochSchedule`];
//! - the memo scope listing: one scope per epoch, `scope_id = epoch`, the
//!   newest ending at the anchor (spec 7.1, 10.3);
//! - the first-contact span rule (spec 7.2): spans of whole epochs, decided
//!   when an epoch's last block is stored, closing at the first boundary
//!   where the span holds at least [`FC_SPAN_TARGET`] entries or covers
//!   [`FC_SPAN_MAX_EPOCHS`] epochs; the open span exists once a block of its
//!   first epoch is stored; `scope_id` is the first epoch, the scope index
//!   the span's position;
//! - sealing, `last_height + SEAL_DEPTH <= anchor` (spec 6.5);
//! - `n_buckets`, the smallest power of two keeping the expected load per row
//!   at or below the target (spec 6.2, 6.3);
//! - lookup results: every entry whose `key16` matches, at a covered height,
//!   in ordinal order (spec 5.3, 7.1).
//!
//! The journal stores blocks up to the publication's anchor
//! (`tip - CONFIRMATION_DEPTH`, spec 11.2), so "stored" below means "at or
//! below the anchor".
use crate::oracle::{CONFIRMATION_DEPTH, EpochSchedule, SEAL_DEPTH, State};
use std::collections::BTreeMap;
use tachyon_fixtures::{EnvelopeCounts, FirstContactTruth, FollowUpTruth, SyntheticChain};

/// `FC_SPAN_TARGET` (spec 7.2, 15).
pub const FC_SPAN_TARGET: u64 = 3072;
/// `FC_SPAN_MAX_EPOCHS` (spec 7.2, 15).
pub const FC_SPAN_MAX_EPOCHS: u64 = 64;
/// Rows of a `tachyon-4k8` table (spec 6.1).
pub const ROWS: u64 = 4096;
/// `MAX_BUCKETS` (spec 6.3, 15).
pub const MAX_BUCKETS: u32 = 64;

/// Which of the two envelope products a truth list feeds (spec 5.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    /// Kind `0x01`, scoped by epoch.
    Memo,
    /// Kind `0x02`, scoped by span.
    FirstContact,
}

impl Family {
    /// Target load per row (spec 6.2): 34 of 56 memo slots, 1 of 6
    /// first-contact slots.
    pub const fn target_load(self) -> u64 {
        match self {
            Family::Memo => 34,
            Family::FirstContact => 1,
        }
    }

    /// Entries per row a row holds (spec 6.2: `floor(8192 / ENTRY_BYTES)`
    /// with 144- and 1,232-byte entries).
    pub const fn slots(self) -> usize {
        match self {
            Family::Memo => 8192 / 144,
            Family::FirstContact => 8192 / 1232,
        }
    }

    /// `n_buckets` of a scope of `entries` entries (spec 6.3).
    pub fn n_buckets(self, entries: u64) -> u32 {
        let per_bucket = self.target_load() * ROWS;
        let mut n = 1u32;
        while n < MAX_BUCKETS && entries > per_bucket * u64::from(n) {
            n *= 2;
        }
        n
    }
}

/// An entry of a valid envelope, as the fixtures authored it.
pub trait EnvelopeTruth: Clone + std::fmt::Debug + Send + Sync + 'static {
    const FAMILY: Family;
    /// Every entry of this kind on the active chain, in ordinal order.
    fn all(chain: &SyntheticChain) -> Vec<Self>;
    fn height(&self) -> u64;
    /// `(height, tx index in block, entry index)` (spec 5.2).
    fn ordinal(&self) -> (u64, usize, usize);
    fn tag(&self) -> &[u8; 32];
    /// The table key: the tag's first 16 bytes (spec 6.3).
    fn key16(&self) -> [u8; 16] {
        let mut key = [0; 16];
        key.copy_from_slice(&self.tag()[..16]);
        key
    }
}

impl EnvelopeTruth for FollowUpTruth {
    const FAMILY: Family = Family::Memo;
    fn all(chain: &SyntheticChain) -> Vec<Self> {
        chain.follow_ups(..)
    }
    fn height(&self) -> u64 {
        self.height
    }
    fn ordinal(&self) -> (u64, usize, usize) {
        FollowUpTruth::ordinal(self)
    }
    fn tag(&self) -> &[u8; 32] {
        &self.tag
    }
}

impl EnvelopeTruth for FirstContactTruth {
    const FAMILY: Family = Family::FirstContact;
    fn all(chain: &SyntheticChain) -> Vec<Self> {
        chain.first_contacts(..)
    }
    fn height(&self) -> u64 {
        self.height
    }
    fn ordinal(&self) -> (u64, usize, usize) {
        FirstContactTruth::ordinal(self)
    }
    fn tag(&self) -> &[u8; 32] {
        &self.tag
    }
}

/// A first-contact span as the spec 7.2 rule decides it at some anchor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanTruth {
    /// Position among spans, the scope index.
    pub index: u64,
    /// The span's first epoch, its `scope_id`.
    pub first_epoch: u64,
    /// The epoch whose boundary closed the span; `None` while open.
    pub last_epoch: Option<u64>,
    /// Entries stored in the span (at heights up to the anchor).
    pub entries: u64,
}

/// One scope a publication at some anchor must list (spec 10.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScopeTruth {
    pub index: u64,
    pub scope_id: u64,
    pub state: State,
    pub first_height: u64,
    /// The scope's last block, or the anchor for a scope still extending.
    pub last_height: u64,
    pub entries: u64,
    pub n_buckets: u32,
    /// For a span, the epoch whose boundary closed it.
    pub closed_at_epoch: Option<u64>,
}

/// The ground truth of one envelope product over one authored chain.
#[derive(Clone, Debug)]
pub struct EnvelopeOracle<T> {
    schedule: EpochSchedule,
    tip: u64,
    /// Active block hashes by height, protocol order.
    hashes: Vec<[u8; 32]>,
    /// Every entry of the product on the active chain, ordinal order.
    entries: Vec<T>,
    /// Each active block's envelope counts, by height.
    counts: Vec<EnvelopeCounts>,
}

/// The memo product's oracle.
pub type MemoOracle = EnvelopeOracle<FollowUpTruth>;
/// The first-contact product's oracle.
pub type FirstContactOracle = EnvelopeOracle<FirstContactTruth>;

impl<T: EnvelopeTruth> EnvelopeOracle<T> {
    /// The oracle of the active chain of `chain`, under `epoch_length`-block
    /// epochs from the chain's NuTachyon activation height.
    pub fn new(chain: &SyntheticChain, epoch_length: u64) -> Self {
        let activation = chain
            .activation_height()
            .expect("the memo products need NuTachyon activated");
        let schedule = EpochSchedule::new(activation, epoch_length);
        let entries = T::all(chain);
        assert!(
            entries.windows(2).all(|w| w[0].ordinal() < w[1].ordinal()),
            "the fixtures' ground truth is in strictly ascending ordinal order"
        );
        Self {
            schedule,
            tip: chain.tip_height(),
            hashes: chain.blocks().iter().map(|b| b.hash()).collect(),
            entries,
            counts: chain.blocks().iter().map(|b| b.envelope_counts()).collect(),
        }
    }

    pub fn family(&self) -> Family {
        T::FAMILY
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

    /// Every entry of the product on the active chain, ordinal order.
    pub fn entries(&self) -> &[T] {
        &self.entries
    }

    /// Envelope counts summed over the active blocks at heights up to
    /// `through`: what a journal holding those blocks has seen.
    pub fn envelope_counts(&self, through: u64) -> EnvelopeCounts {
        let mut total = EnvelopeCounts::default();
        for counts in self.counts.iter().take(through as usize + 1) {
            total += *counts;
        }
        total
    }

    /// Entries stored at `anchor`, by epoch.
    fn per_epoch(&self, anchor: u64) -> BTreeMap<u64, u64> {
        let mut out = BTreeMap::new();
        for entry in self.entries.iter().filter(|e| e.height() <= anchor) {
            let epoch = self
                .schedule
                .epoch_of(entry.height())
                .expect("no Tachyon entry below activation");
            *out.entry(epoch).or_insert(0) += 1;
        }
        out
    }

    /// The first-contact spans a journal holding the blocks up to `anchor`
    /// has decided (spec 7.2), recomputed from per-epoch counts.
    pub fn spans(&self, anchor: u64) -> Vec<SpanTruth> {
        assert_eq!(
            T::FAMILY,
            Family::FirstContact,
            "only first-contact has spans"
        );
        let Some(newest_epoch) = self.schedule.epoch_of(anchor) else {
            return Vec::new();
        };
        let per_epoch = self.per_epoch(anchor);
        let mut spans = Vec::new();
        let (mut first, mut entries) = (0u64, 0u64);
        for epoch in 0..=newest_epoch {
            entries += per_epoch.get(&epoch).copied().unwrap_or(0);
            // A boundary is decided when the epoch's last block is stored.
            if self.schedule.last_height(epoch) > anchor {
                break;
            }
            let covered = epoch - first + 1;
            if entries >= FC_SPAN_TARGET || covered >= FC_SPAN_MAX_EPOCHS {
                spans.push(SpanTruth {
                    index: spans.len() as u64,
                    first_epoch: first,
                    last_epoch: Some(epoch),
                    entries,
                });
                (first, entries) = (epoch + 1, 0);
            }
        }
        // The open span exists once a block of its first epoch is stored.
        if self.schedule.first_height(first) <= anchor {
            spans.push(SpanTruth {
                index: spans.len() as u64,
                first_epoch: first,
                last_epoch: None,
                entries,
            });
        }
        spans
    }

    /// Every scope a map anchored at `anchor` lists, from index 0
    /// (spec 10.3).
    pub fn scopes(&self, anchor: u64) -> Vec<ScopeTruth> {
        let schedule = self.schedule;
        let raw: Vec<(u64, u64, u64, u64, Option<u64>)> = match T::FAMILY {
            Family::Memo => {
                let newest = schedule
                    .epoch_of(anchor)
                    .expect("the anchor is at or above activation");
                (0..=newest)
                    .map(|epoch| {
                        let last = if epoch == newest {
                            anchor
                        } else {
                            schedule.last_height(epoch)
                        };
                        (epoch, epoch, schedule.first_height(epoch), last, None)
                    })
                    .collect()
            }
            Family::FirstContact => self
                .spans(anchor)
                .into_iter()
                .map(|span| {
                    let last = span.last_epoch.map_or(anchor, |e| schedule.last_height(e));
                    (
                        span.index,
                        span.first_epoch,
                        schedule.first_height(span.first_epoch),
                        last,
                        span.last_epoch,
                    )
                })
                .collect(),
        };
        raw.into_iter()
            .map(|(index, scope_id, first_height, last_height, closed)| {
                let entries = self
                    .entries
                    .iter()
                    .filter(|e| (first_height..=last_height).contains(&e.height()))
                    .count() as u64;
                let state = if last_height + SEAL_DEPTH <= anchor {
                    State::Sealed
                } else {
                    State::Open
                };
                ScopeTruth {
                    index,
                    scope_id,
                    state,
                    first_height,
                    last_height,
                    entries,
                    n_buckets: T::FAMILY.n_buckets(entries),
                    closed_at_epoch: closed,
                }
            })
            .collect()
    }

    /// Scope `index` of a map anchored at `anchor`, if listed.
    pub fn scope(&self, index: u64, anchor: u64) -> Option<ScopeTruth> {
        self.scopes(anchor).into_iter().find(|s| s.index == index)
    }

    /// The entries of scope `index` a map anchored at `anchor` covers, in
    /// ordinal order.
    pub fn scope_entries(&self, index: u64, anchor: u64) -> Vec<T> {
        let Some(scope) = self.scope(index, anchor) else {
            return Vec::new();
        };
        self.entries
            .iter()
            .filter(|e| (scope.first_height..=scope.last_height).contains(&e.height()))
            .cloned()
            .collect()
    }

    /// The expected answer for `key16` in scope `index` of a map anchored at
    /// `anchor`: every entry with that key at a covered height, in ordinal
    /// order (spec 5.3, 7.1). Empty means absent within the scope's coverage.
    pub fn expected(&self, index: u64, key16: &[u8; 16], anchor: u64) -> Vec<T> {
        self.scope_entries(index, anchor)
            .into_iter()
            .filter(|e| &e.key16() == key16)
            .collect()
    }

    /// The distinct keys of scope `index` at `anchor`, in order of first
    /// ordinal.
    pub fn keys(&self, index: u64, anchor: u64) -> Vec<[u8; 16]> {
        let mut seen = std::collections::HashSet::new();
        self.scope_entries(index, anchor)
            .into_iter()
            .map(|e| e.key16())
            .filter(|k| seen.insert(*k))
            .collect()
    }

    /// The entries of the active chain at heights in `heights`, ordinal
    /// order (covered or not).
    pub fn entries_at(&self, heights: impl std::ops::RangeBounds<u64>) -> Vec<T> {
        self.entries
            .iter()
            .filter(|e| heights.contains(&e.height()))
            .cloned()
            .collect()
    }
}
