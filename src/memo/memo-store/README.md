# memo-store

The memo and first-contact products' journal and source of truth,
`memo-v1.sqlite` (spec sections 5, 7.1 to 7.3 and 11.8,
`docs/spec/spec-tachyon-payment-pir.md`). It follows `wallet-pir`'s
`receiver-store` and mirrors `tachygram-store`: one SQLite file in WAL mode
with `synchronous=FULL`, every write in one `IMMEDIATE` transaction, bound to
its network at creation. Envelopes are parsed by
`tachyon_pir_protocol::envelope`; this crate has no parser of its own.

## API

```rust
pub struct Ordinal { pub height: u32, pub tx_index: u32, pub entry_index: u8 }   // derives Ord
pub enum Kind { Memo, FirstContact }
pub struct MemoRecord { pub ordinal: Ordinal, pub tag: [u8; 32], pub ciphertext: [u8; 128] }
pub struct FirstContactRecord {
    pub ordinal: Ordinal, pub tag: [u8; 32],
    pub kem_ciphertext: Box<[u8; 1088]>, pub ciphertext: [u8; 128],
}
// MemoRecord::entry() -> MemoEntry, FirstContactRecord::entry() -> FirstContactEntry (key16 = tag[..16])
pub struct InvalidEnvelope { pub tx_index: u32, pub txid: Hash, pub error: EnvelopeError }
pub struct Counts {
    pub envelopes_valid: u64, pub envelopes_invalid: u64,
    pub memo_entries: u64, pub first_contact_entries: u64, pub state_entries: u64,
}
pub struct IndexedBlock {
    pub id: BlockId, pub prev: Hash, pub epoch: Option<u32>,
    pub memo: Vec<MemoRecord>, pub first_contact: Vec<FirstContactRecord>,
    pub envelopes_valid: u32, pub invalid: Vec<InvalidEnvelope>, pub state_entries: u32,
}
impl IndexedBlock {
    pub fn new(block: &TachyonBlock, schedule: &EpochSchedule) -> IndexedBlock;
    pub fn counts(&self) -> Counts;
}

pub struct Binding { pub chain: String, pub genesis_hash: Hash, pub activation_height: u32, pub epoch_length: u32 }
impl Binding {
    pub fn new(identity: &NetworkIdentity, epoch_length: u32) -> Binding;
    pub fn schedule(&self) -> Result<EpochSchedule, Error>;
}

impl Store {
    pub fn open(path: impl AsRef<Path>, binding: Binding) -> Result<Store, Error>;
    pub fn binding(&self) -> &Binding;
    pub fn schedule(&self) -> EpochSchedule;
    // Journal (the publisher's `Ingestion` sits on these)
    pub fn committed(&self) -> Option<BlockId>;
    pub fn append(&mut self, block: &IndexedBlock) -> Result<(), Error>;
    pub fn rewind(&mut self, keep: Option<&BlockId>) -> Result<(), Error>;
    pub fn checkpoint_at_or_below(&self, height: u32) -> Result<Option<BlockId>, Error>;
    pub fn block_hash(&self, height: u32) -> Result<Option<Hash>, Error>;
    // Reads
    pub fn epochs(&self) -> Result<Vec<u32>, Error>;
    pub fn epoch_summary(&self, epoch: u32) -> Result<Option<EpochSummary>, Error>;
    pub fn memo_entries(&self, epoch: u32) -> Result<Vec<MemoRecord>, Error>;
    pub fn first_contact_entries(&self, scope_id: u32) -> Result<Vec<FirstContactRecord>, Error>;
    pub fn entries(&self, kind: Kind, scope_id: u32)
        -> Result<impl ExactSizeIterator<Item = Record>, Error>;
    pub fn spans(&self) -> Result<Vec<Span>, Error>;
    // Metrics (spec 11.8)
    pub fn totals(&self) -> Result<Counts, Error>;
    pub fn envelopes_invalid_total(&self) -> Result<u64, Error>;
    pub fn state_entries_total(&self) -> Result<u64, Error>;
}

pub struct EpochSummary { pub epoch: u32, pub first_height: u32, pub last_height: u32, pub counts: Counts }
pub struct Span {
    pub index: u32,
    pub first_epoch: u32,            // the span's scope_id
    pub last_epoch: Option<u32>,     // None while open
    pub entries: u64,
    pub closed_by: Option<BlockId>,  // the block whose append decided the boundary
    pub last_height: u32,            // closed_by's height, or the tip while open
}
pub enum Record { Memo(MemoRecord), FirstContact(FirstContactRecord) }  // ordinal(), tag(), entry() -> Entry
```

`STORE_FILE` is `"memo-v1.sqlite"`; `SCHEMA_VERSION` (1) is kept in
`PRAGMA user_version`. `FC_SPAN_TARGET` and `FC_SPAN_MAX_EPOCHS` are
re-exported from `tachyon-pir-protocol`.

## Rules

- **Extraction** (spec 5.2). `IndexedBlock::new` walks the Tachyon
  transactions in block order. An empty memo is skipped and counted as
  nothing. A memo that `envelope::parse` refuses is skipped whole and recorded
  in `invalid` (the store keeps the count); it never fails ingestion. A valid
  envelope counts once in `envelopes_valid`; its follow-up (`0x01`) and
  first-contact (`0x02`) entries are kept with ordinal
  `(height, tx_index, entry_index)`, where `tx_index` counts every transaction
  of the block (coinbase included) and `entry_index` every entry of the
  envelope (wallet-state included); wallet-state (`0x03`) entries are only
  counted. An all-zero `key16` makes the envelope invalid (spec 6.2); the
  parser refuses it.
- **Duplicates** (spec 5.3). Every entry is stored, whatever its tag: the same
  tag in one envelope, in two transactions, in two blocks or in both kinds.
  Reads return entries in ordinal order.
- **Binding.** A new file records chain, genesis hash, activation height and
  epoch length, and the span constants its boundaries are decided under;
  reopening with any binding field different is `Error::BindingMismatch`, a
  build with other span constants is `Error::SpanRuleMismatch`, and a zero
  epoch length is `Error::InvalidBinding`. A non-empty SQLite file that is not
  a store, or another schema version, is refused.
- **Append.** Every journal starts at the activation height (spec 7.3): the
  first block of an empty store must be the activation block
  (`NotAtActivation`), so every memo epoch is stored from its first block and
  spans from epoch 0. Later blocks must extend the tip:
  `height == tip.height + 1` (`HeightGap`) and `prev == tip.hash`
  (`ParentMismatch`). `block.epoch` must match the store's schedule
  (`EpochMismatch`). A hand-built block's entries are checked again: ordinals
  name the block, each kind is strictly ascending, no ordinal is shared, no key
  is zero (`Error::Entry`).
- **Spans** (spec 7.2). Spans are consecutive whole epochs; span 0 starts at
  epoch 0. When the appended block is the last block of its epoch, the open
  span (the one after the last boundary) is closed at that epoch if it holds at
  least `FC_SPAN_TARGET` (3,072) first-contact entries or covers
  `FC_SPAN_MAX_EPOCHS` (64) epochs; the next span starts at the following
  epoch. The boundary is a `span_boundaries` row written in the same
  transaction as the block, so it is as durable as the block and depends only
  on the stored chain. Reaching the target in the middle of an epoch closes
  nothing until that epoch's last block. `spans()` lists the closed spans and,
  once a block of its first epoch is stored, the open span;
  `first_contact_entries` accepts a closed span's first epoch or the open
  span's (empty before its first block), and refuses any other
  (`UnknownSpan`). `spans()` checks every stored boundary against the rule and
  the stored counts (`Error::Corrupt` otherwise).
- **Rewind.** `rewind(Some(keep))` removes every block above `keep`, which
  must be a stored block by height and hash (`UnknownCheckpoint`), with its
  entries and every span boundary those blocks decided; `rewind(None)` removes
  everything. Every stored block is a checkpoint. Re-appending the same blocks
  re-decides the same boundaries.
- **Metrics** (spec 11.8). `totals()` sums the stored blocks' counts:
  `envelopes_invalid` is `tachyon_memo_envelopes_invalid_total` and
  `state_entries` is `tachyon_memo_state_entries_total`. They describe the
  stored chain, so a rewind lowers them.
- **Atomicity and durability.** Each append and rewind is one transaction; a
  refused or failed one leaves the file unchanged. Committed writes survive a
  crash (WAL, `synchronous=FULL`). `committed()` is cached in memory, so one
  `Store` should own the file; `append` re-reads the tip inside its
  transaction regardless.

## Schema

```sql
binding               (id = 1, chain, genesis_hash, activation_height, epoch_length,
                       fc_span_target, fc_span_max_epochs)
blocks                (height PRIMARY KEY, hash, epoch, envelopes_valid, envelopes_invalid,
                       memo_entries, first_contact_entries, state_entries)
                      INDEX blocks_epoch (epoch, height)
memo_entries          (epoch, height, tx_index, entry_index, tag, ciphertext)
                      PRIMARY KEY (epoch, height, tx_index, entry_index)          WITHOUT ROWID
first_contact_entries (epoch, height, tx_index, entry_index, tag, kem_ciphertext, ciphertext)
                      UNIQUE INDEX first_contact_order (epoch, height, tx_index, entry_index)
span_boundaries       (span_index PRIMARY KEY, first_epoch UNIQUE, last_epoch UNIQUE, entries,
                       decided_height UNIQUE -> blocks ON DELETE CASCADE)
```

Entries keep their full 32-byte tag; the table key is `tag[..16]`. The memo
primary key is the ordinal order within an epoch, so `memo_entries` is one
clustered range scan. First-contact rows (1.2 KB) are too large to cluster
well, so they live in a rowid table with a unique ordinal index; rows are only
appended in ordinal order and rewinds remove a tail, so rowid order follows
the ordinal too. A span's entries are one index range over its epochs. Entry
tables carry no foreign key (SQLite would look up the parent per deleted row);
`rewind` deletes them by range from `keep`'s epoch, and `epoch_summary`
checks them against their blocks' counts (`Error::Corrupt` otherwise).

## Performance

`an_epoch_of_100k_memo_entries_is_cheap_to_read` parses and appends 100
blocks of 1,000 follow-up entries (63 envelopes each) into one epoch and reads
them back. Under `--profile release-fast` on a development machine with the
file on an NVMe SSD: parse and append about 0.1 s in total, `memo_entries`
about 15 ms, summary and totals about 3 ms, and a rewind of half the epoch
about 10 ms. The test asserts the reads stay under 10 s.

## Tests

`cargo test --locked -p memo-store --profile release-fast`

- `tests/store.rs`: hand-built chains whose memos are authored as entry lists
  (encoded with `Envelope::encode`) or as malformed bytes, checked against an
  oracle built from what was authored: entries in ordinal order with gaps in
  transaction indices, invalid envelopes (each malformation) skipped and
  counted while the block's other entries are stored (scenario 7), empty memos
  not counted, duplicate tags all kept in ordinal order (scenario 5),
  wallet-state entries counted only, the generic `entries(kind, scope)`
  read, reopen durability, rewind and re-append determinism across sessions,
  reorgs, rewind to `None`, checkpoints, the activation start, refused
  heights, parents, epochs and hand-built entries, binding and span-rule
  mismatches, foreign files and schema versions, an injected SQL failure
  rolled back, a tampered store reported corrupt, and the 100,000-entry epoch.
- `tests/spans.rs`: spans closing by count at exactly the first boundary with
  3,072 entries (not at 3,071, not mid-epoch), a one-epoch span of exactly
  3,072, spans closing after 64 epochs (4-block epochs, 520 blocks), the two
  rules interleaved (one-block epochs), rewinds to, below and across
  boundaries re-decided identically across reopen and cadence, a reorg
  moving a boundary, a failed boundary write failing the append, and tampered
  boundaries reported corrupt.
- `tests/fixture.rs`: `tachyon-fixtures` chains with generated envelopes
  (valid, malformed, repeated tags, wallet state), served as
  `getblock <hash> 2` JSON and decoded by `tachyon-chain`, against the
  generator's authored truth, including a reorg, first-contact-heavy chains
  whose spans close by count, and random memo bytes counted invalid.
