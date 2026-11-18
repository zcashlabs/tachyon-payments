//! Shared test helpers: authored envelopes, hand-built and fixture chains, an
//! oracle computed from what the test authored, and whole-store snapshots.
#![allow(dead_code)]
use memo_store::{
    Binding, Counts, EpochSummary, FC_SPAN_MAX_EPOCHS, FC_SPAN_TARGET, FirstContactRecord,
    IndexedBlock, MemoRecord, Ordinal, Span, Store,
};
use std::collections::BTreeMap;
use std::path::Path;
use tachyon_chain::{BlockId, EpochSchedule, Hash, Stamp, TachyonBlock, TachyonTx};
use tachyon_pir_protocol::envelope::{Envelope, EnvelopeEntry};

/// The fixtures' default activation height, as on the alpha testnet.
pub const ACTIVATION: u32 = 8;
/// A small epoch length, so a short chain spans several epochs.
pub const EPOCH_LENGTH: u32 = 4;

/// A binding for hand-built chains with `epoch_length`-block epochs.
pub fn binding_with(epoch_length: u32) -> Binding {
    Binding {
        chain: "test".to_owned(),
        genesis_hash: Hash::from_protocol_bytes([0x5a; 32]),
        activation_height: ACTIVATION,
        epoch_length,
    }
}

/// A binding for hand-built chains with [`EPOCH_LENGTH`]-block epochs.
pub fn hand_binding() -> Binding {
    binding_with(EPOCH_LENGTH)
}

/// Opens a store at `path` with `binding`.
pub fn open(path: &Path, binding: &Binding) -> Store {
    Store::open(path, binding.clone()).unwrap()
}

// ---- Authored envelope content ----

/// A distinct tag for `n`, whose `key16` is nonzero.
pub fn tag(n: u32) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[0] = 0x7e;
    bytes[1..5].copy_from_slice(&n.to_be_bytes());
    bytes[31] = 0x0f;
    bytes
}

/// `N` deterministic bytes for `seed` and `salt`.
pub fn filler<const N: usize>(seed: u32, salt: u8) -> [u8; N] {
    let mut out = [0u8; N];
    let mut state = u64::from(seed) << 8 | u64::from(salt);
    for chunk in out.chunks_mut(8) {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        chunk.copy_from_slice(&z.to_le_bytes()[..chunk.len()]);
    }
    out
}

/// A follow-up entry with tag `tag(t)` and a ciphertext distinct per `c`.
pub fn follow_up(t: u32, c: u32) -> EnvelopeEntry {
    EnvelopeEntry::FollowUp {
        tag: tag(t),
        ciphertext: Box::new(filler(c, 1)),
    }
}

/// A first-contact entry with tag `tag(t)` and ciphertexts distinct per `c`.
pub fn first_contact(t: u32, c: u32) -> EnvelopeEntry {
    EnvelopeEntry::FirstContact {
        tag: tag(t),
        kem_ciphertext: Box::new(filler(c, 2)),
        ciphertext: Box::new(filler(c, 3)),
    }
}

/// A wallet-state entry of `len` ciphertext bytes.
pub fn wallet_state(t: u32, len: usize) -> EnvelopeEntry {
    EnvelopeEntry::WalletState {
        tag: tag(t),
        ciphertext: vec![0x5e; len],
    }
}

/// What a test authors in one transaction's memo.
#[derive(Clone, Debug)]
pub enum Memo {
    /// `""`: skipped, counted as nothing.
    Empty,
    /// A valid envelope of these entries.
    Valid(Vec<EnvelopeEntry>),
    /// Bytes the test authored to be invalid, with why.
    Invalid(Vec<u8>, &'static str),
}

impl Memo {
    /// The memo's bytes.
    pub fn bytes(&self) -> Vec<u8> {
        match self {
            Memo::Empty => Vec::new(),
            Memo::Valid(entries) => Envelope {
                entries: entries.clone(),
            }
            .encode()
            .unwrap(),
            Memo::Invalid(bytes, _) => bytes.clone(),
        }
    }
}

/// The encoding of `entries` built by hand, byte by byte, for malformed
/// variants that `Envelope::encode` refuses to produce.
pub fn raw_envelope(count: u8, entries: &[(u8, [u8; 32], Vec<u8>)]) -> Vec<u8> {
    let mut out = b"TPM1".to_vec();
    out.push(count);
    for (kind, tag, body) in entries {
        out.push(*kind);
        out.extend_from_slice(tag);
        out.extend_from_slice(body);
    }
    out
}

/// A catalogue of invalid memos, each authored to break one rule of spec 5.1,
/// 5.2 or 6.2.
pub fn malformed_memos() -> Vec<Memo> {
    let valid = Memo::Valid(vec![follow_up(900, 900), first_contact(901, 901)]).bytes();
    let mut zero_tag = tag(5);
    zero_tag[..16].fill(0);
    let mut trailing = valid.clone();
    trailing.push(0);
    let mut bad_magic = valid.clone();
    bad_magic[3] = b'2';
    let mut wrong_count = valid.clone();
    wrong_count[4] = 3;
    vec![
        Memo::Invalid(vec![0x01], "one byte"),
        Memo::Invalid(b"TPM1".to_vec(), "magic only"),
        Memo::Invalid(bad_magic, "bad magic"),
        Memo::Invalid(valid[..valid.len() - 1].to_vec(), "truncated"),
        Memo::Invalid(trailing, "trailing byte"),
        Memo::Invalid(wrong_count, "count larger than entries"),
        Memo::Invalid(raw_envelope(0, &[]), "zero entries"),
        Memo::Invalid(
            raw_envelope(17, &vec![(1, tag(1), vec![7; 128]); 17]),
            "17 entries",
        ),
        Memo::Invalid(
            raw_envelope(1, &[(4, tag(1), vec![7; 128])]),
            "unknown kind",
        ),
        Memo::Invalid(
            raw_envelope(1, &[(1, zero_tag, vec![7; 128])]),
            "zero follow-up key",
        ),
        Memo::Invalid(
            raw_envelope(
                2,
                &[(1, tag(1), vec![7; 128]), (2, zero_tag, vec![7; 1216])],
            ),
            "zero first-contact key after a good entry",
        ),
        Memo::Invalid(
            raw_envelope(
                1,
                &[(
                    3,
                    tag(1),
                    [1025u16.to_le_bytes().to_vec(), vec![0; 1025]].concat(),
                )],
            ),
            "wallet state over 1024 bytes",
        ),
    ]
}

// ---- Hand-built chains ----

/// A deterministic hand-built block hash for `height` on `branch`.
pub fn hand_hash(height: u32, branch: u32) -> Hash {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&height.to_le_bytes());
    bytes[4..8].copy_from_slice(&branch.to_le_bytes());
    bytes[31] = 0xb1;
    Hash::from_protocol_bytes(bytes)
}

/// One authored block: the decoded block and what each transaction's memo
/// was authored to be.
#[derive(Clone, Debug)]
pub struct Authored {
    pub block: TachyonBlock,
    /// `(tx_index, memo)` in block order.
    pub txs: Vec<(u32, Memo)>,
}

/// A hand-built chain starting at the activation height, with authored memos.
#[derive(Clone, Debug)]
pub struct HandChain {
    pub schedule: EpochSchedule,
    /// `blocks[i]` is at height `ACTIVATION + i`.
    pub blocks: Vec<Authored>,
    /// Bumped by each reorg, so replaced blocks get new hashes.
    pub branch: u32,
}

impl HandChain {
    pub fn new(binding: &Binding) -> Self {
        Self {
            schedule: binding.schedule().unwrap(),
            blocks: Vec::new(),
            branch: 0,
        }
    }

    pub fn tip_height(&self) -> u32 {
        ACTIVATION + self.blocks.len() as u32 - 1
    }

    pub fn next_height(&self) -> u32 {
        ACTIVATION + self.blocks.len() as u32
    }

    /// Appends a block whose memos are `memos` at transaction indices 1, 2,
    /// ... (the coinbase, index 0, has no bundle).
    pub fn push(&mut self, memos: Vec<Memo>) -> BlockId {
        let txs = memos
            .into_iter()
            .enumerate()
            .map(|(i, memo)| (i as u32 + 1, memo))
            .collect();
        self.push_at(txs)
    }

    /// Appends a block whose memos are at the given transaction indices.
    pub fn push_at(&mut self, txs: Vec<(u32, Memo)>) -> BlockId {
        let height = self.next_height();
        let prev = self
            .blocks
            .last()
            .map_or(hand_hash(ACTIVATION - 1, 0), |a| a.block.id.hash);
        let id = BlockId {
            height,
            hash: hand_hash(height, self.branch),
        };
        let block = TachyonBlock {
            id,
            prev,
            txs: txs
                .iter()
                .map(|(tx_index, memo)| {
                    let mut txid = [0u8; 32];
                    txid[..4].copy_from_slice(&height.to_le_bytes());
                    txid[4..8].copy_from_slice(&tx_index.to_le_bytes());
                    txid[8..12].copy_from_slice(&self.branch.to_le_bytes());
                    txid[31] = 0x71;
                    TachyonTx {
                        tx_index: *tx_index,
                        txid: Hash::from_protocol_bytes(txid),
                        memo: memo.bytes(),
                        own_actions: 1,
                        stamp: Stamp::Pointer {
                            aggregate_id: [0xa3; 64],
                        },
                    }
                })
                .collect(),
        };
        self.blocks.push(Authored { block, txs });
        id
    }

    /// Appends `n` blocks without memos.
    pub fn push_empty(&mut self, n: u32) {
        for _ in 0..n {
            self.push(Vec::new());
        }
    }

    /// Appends empty blocks through the last block of `epoch`.
    pub fn fill_epoch(&mut self, epoch: u32) {
        let last = self.schedule.last_height(epoch).unwrap();
        while self.next_height() <= last {
            self.push(Vec::new());
        }
    }

    /// Drops every block above `height` and starts a new branch.
    pub fn truncate(&mut self, height: u32) {
        self.blocks.truncate((height + 1 - ACTIVATION) as usize);
        self.branch += 1;
    }

    pub fn id(&self, height: u32) -> BlockId {
        self.blocks[(height - ACTIVATION) as usize].block.id
    }

    pub fn indexed(&self, height: u32) -> IndexedBlock {
        IndexedBlock::new(
            &self.blocks[(height - ACTIVATION) as usize].block,
            &self.schedule,
        )
    }

    /// Appends blocks `from..=through` to `store`.
    pub fn append(&self, store: &mut Store, from: u32, through: u32) {
        for height in from..=through {
            store.append(&self.indexed(height)).unwrap();
        }
    }

    /// Appends every block of the chain the store does not hold yet.
    pub fn sync(&self, store: &mut Store) {
        let from = store.committed().map_or(ACTIVATION, |tip| tip.height + 1);
        self.append(store, from, self.tip_height());
    }

    /// The oracle for blocks `ACTIVATION..=through`.
    pub fn oracle(&self, through: u32) -> Oracle {
        let mut oracle = Oracle::new(self.schedule);
        for authored in &self.blocks[..=(through - ACTIVATION) as usize] {
            oracle.add(authored.block.id.height, &authored.txs);
        }
        oracle
    }
}

// ---- The oracle ----

/// An expected memo entry: (ordinal, tag, ciphertext).
pub type MemoRow = (Ordinal, [u8; 32], [u8; 128]);
/// An expected first-contact entry: (ordinal, tag, kem ciphertext, ciphertext).
pub type FirstContactRow = (Ordinal, [u8; 32], Vec<u8>, [u8; 128]);

/// What the store should hold, computed from authored memos only: valid
/// envelopes are taken entry by entry as authored, never by parsing bytes.
#[derive(Debug, Clone)]
pub struct Oracle {
    pub schedule: EpochSchedule,
    pub memo: BTreeMap<u32, Vec<MemoRow>>,
    pub first_contact: BTreeMap<u32, Vec<FirstContactRow>>,
    pub counts: BTreeMap<u32, Counts>,
    /// Per epoch: (first height, last height).
    pub heights: BTreeMap<u32, (u32, u32)>,
    pub tip: Option<u32>,
}

impl Oracle {
    pub fn new(schedule: EpochSchedule) -> Self {
        Self {
            schedule,
            memo: BTreeMap::new(),
            first_contact: BTreeMap::new(),
            counts: BTreeMap::new(),
            heights: BTreeMap::new(),
            tip: None,
        }
    }

    /// Adds the block at `height` whose memos were authored as `txs`.
    pub fn add(&mut self, height: u32, txs: &[(u32, Memo)]) {
        let epoch = (height - self.schedule.activation_height()) / self.schedule.epoch_length();
        self.tip = Some(height);
        let span = self.heights.entry(epoch).or_insert((height, height));
        span.1 = height;
        let counts = self.counts.entry(epoch).or_default();
        for (tx_index, memo) in txs {
            match memo {
                Memo::Empty => {}
                Memo::Invalid(..) => counts.envelopes_invalid += 1,
                Memo::Valid(entries) => {
                    counts.envelopes_valid += 1;
                    for (i, entry) in entries.iter().enumerate() {
                        let ordinal = Ordinal {
                            height,
                            tx_index: *tx_index,
                            entry_index: i as u8,
                        };
                        match entry {
                            EnvelopeEntry::FollowUp { tag, ciphertext } => {
                                counts.memo_entries += 1;
                                self.memo.entry(epoch).or_default().push((
                                    ordinal,
                                    *tag,
                                    **ciphertext,
                                ));
                            }
                            EnvelopeEntry::FirstContact {
                                tag,
                                kem_ciphertext,
                                ciphertext,
                            } => {
                                counts.first_contact_entries += 1;
                                self.first_contact.entry(epoch).or_default().push((
                                    ordinal,
                                    *tag,
                                    kem_ciphertext.to_vec(),
                                    **ciphertext,
                                ));
                            }
                            EnvelopeEntry::WalletState { .. } => counts.state_entries += 1,
                        }
                    }
                }
            }
        }
    }

    pub fn totals(&self) -> Counts {
        let mut total = Counts::default();
        for c in self.counts.values() {
            total.envelopes_valid += c.envelopes_valid;
            total.envelopes_invalid += c.envelopes_invalid;
            total.memo_entries += c.memo_entries;
            total.first_contact_entries += c.first_contact_entries;
            total.state_entries += c.state_entries;
        }
        total
    }

    /// The spans of spec 7.2, recomputed here from the authored counts:
    /// `(first epoch, last epoch if closed, entries)`.
    pub fn spans(&self) -> Vec<(u32, Option<u32>, u64)> {
        let Some(tip) = self.tip else {
            return Vec::new();
        };
        let tip_epoch = self.schedule.epoch_of(tip).unwrap();
        let mut spans = Vec::new();
        let mut first = 0;
        let mut entries = 0u64;
        for epoch in 0..=tip_epoch {
            entries += self
                .counts
                .get(&epoch)
                .map_or(0, |c| c.first_contact_entries);
            let complete = self.schedule.last_height(epoch).unwrap() <= tip;
            let covered = epoch - first + 1;
            if complete && (entries >= FC_SPAN_TARGET || covered >= FC_SPAN_MAX_EPOCHS) {
                spans.push((first, Some(epoch), entries));
                first = epoch + 1;
                entries = 0;
            }
        }
        if first <= tip_epoch {
            spans.push((first, None, entries));
        }
        spans
    }

    /// The first-contact entries of epochs `first..=last`.
    pub fn span_entries(&self, first: u32, last: Option<u32>) -> Vec<FirstContactRow> {
        self.first_contact
            .range(first..=last.unwrap_or(u32::MAX))
            .flat_map(|(_, rows)| rows.iter().cloned())
            .collect()
    }

    /// Asserts that `store` holds exactly this.
    pub fn check(&self, store: &Store) {
        let epochs: Vec<u32> = self.heights.keys().copied().collect();
        assert_eq!(store.epochs().unwrap(), epochs);
        for &epoch in &epochs {
            let want = self.memo.get(&epoch).cloned().unwrap_or_default();
            assert_eq!(
                memo_rows(&store.memo_entries(epoch).unwrap()),
                want,
                "epoch {epoch} memo"
            );
            let (first_height, last_height) = self.heights[&epoch];
            assert_eq!(
                store.epoch_summary(epoch).unwrap(),
                Some(EpochSummary {
                    epoch,
                    first_height,
                    last_height,
                    counts: self.counts[&epoch],
                }),
                "epoch {epoch} summary"
            );
        }
        assert_eq!(store.totals().unwrap(), self.totals());
        let spans = store.spans().unwrap();
        let want = self.spans();
        assert_eq!(
            spans
                .iter()
                .map(|s| (s.first_epoch, s.last_epoch, s.entries))
                .collect::<Vec<_>>(),
            want,
            "spans"
        );
        for (index, span) in spans.iter().enumerate() {
            assert_eq!(span.index as usize, index);
            match span.last_epoch {
                Some(last) => {
                    let height = self.schedule.last_height(last).unwrap();
                    assert_eq!(span.closed_by.unwrap().height, height);
                    assert_eq!(
                        span.closed_by.unwrap().hash,
                        store.block_hash(height).unwrap().unwrap()
                    );
                    assert_eq!(span.last_height, height);
                }
                None => {
                    assert_eq!(span.closed_by, None);
                    assert_eq!(Some(span.last_height), self.tip);
                }
            }
            assert_eq!(
                fc_rows(&store.first_contact_entries(span.first_epoch).unwrap()),
                self.span_entries(span.first_epoch, span.last_epoch),
                "span {index} entries"
            );
        }
    }
}

pub fn memo_rows(records: &[MemoRecord]) -> Vec<MemoRow> {
    records
        .iter()
        .map(|r| (r.ordinal, r.tag, r.ciphertext))
        .collect()
}

pub fn fc_rows(records: &[FirstContactRecord]) -> Vec<FirstContactRow> {
    records
        .iter()
        .map(|r| (r.ordinal, r.tag, r.kem_ciphertext.to_vec(), r.ciphertext))
        .collect()
}

/// Everything a store answers, for "unchanged" and "identical" comparisons.
#[derive(Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub committed: Option<BlockId>,
    pub hashes: Vec<(u32, Option<Hash>)>,
    pub epochs: Vec<u32>,
    pub summaries: Vec<Option<EpochSummary>>,
    pub memo: Vec<Vec<MemoRecord>>,
    pub spans: Vec<Span>,
    pub first_contact: Vec<Vec<FirstContactRecord>>,
    pub totals: Counts,
}

impl Snapshot {
    /// The store's answers for heights up to `max_height`, every stored epoch
    /// and every span.
    pub fn of(store: &Store, max_height: u32) -> Self {
        let epochs = store.epochs().unwrap();
        let spans = store.spans().unwrap();
        Snapshot {
            committed: store.committed(),
            hashes: (0..=max_height)
                .map(|h| (h, store.block_hash(h).unwrap()))
                .collect(),
            summaries: epochs
                .iter()
                .map(|&e| store.epoch_summary(e).unwrap())
                .collect(),
            memo: epochs
                .iter()
                .map(|&e| store.memo_entries(e).unwrap())
                .collect(),
            first_contact: spans
                .iter()
                .map(|s| store.first_contact_entries(s.first_epoch).unwrap())
                .collect(),
            spans,
            epochs,
            totals: store.totals().unwrap(),
        }
    }
}

/// `count` first-contact entries packed 16 to an envelope, tags and
/// ciphertexts numbered from `from`.
pub fn first_contact_memos(from: u32, count: u32) -> Vec<Memo> {
    let entries: Vec<EnvelopeEntry> = (from..from + count).map(|n| first_contact(n, n)).collect();
    entries
        .chunks(16)
        .map(|chunk| Memo::Valid(chunk.to_vec()))
        .collect()
}
