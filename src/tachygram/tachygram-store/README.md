# tachygram-store

The tachygram product's journal and source of truth, `tachygram-v1.sqlite`
(spec sections 8.1 to 8.6, `docs/spec/spec-tachyon-payment-pir.md`). It follows
`wallet-pir`'s `receiver-store`: one SQLite file in WAL mode with
`synchronous=FULL`, every write in one `IMMEDIATE` transaction, bound to its
network at creation.

## API

```rust
pub struct StampOrdinal(pub u32);               // zero-based within an epoch
pub struct ProofStamp { pub tx_index: u32, pub txid: Hash, pub tachygrams: Vec<[u8; 32]> }
pub struct IndexedBlock { pub id: BlockId, pub prev: Hash, pub epoch: Option<u32>, pub stamps: Vec<ProofStamp> }
impl IndexedBlock {
    pub fn new(block: &TachyonBlock, schedule: &EpochSchedule) -> IndexedBlock;
    pub fn tachygram_count(&self) -> usize;
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
    pub fn stamps(&self, epoch: u32) -> Result<Vec<StampRecord>, Error>;
    pub fn epoch_records(&self, epoch: u32) -> Result<Vec<TachygramRecord>, Error>;
    pub fn epoch_tachygrams(&self, epoch: u32)
        -> Result<impl ExactSizeIterator<Item = (StampOrdinal, [u8; 32])>, Error>;
}

pub struct StampRecord { pub ordinal: StampOrdinal, pub height: u32, pub tx_index: u32, pub txid: Hash, pub tachygram_count: u32 }
pub struct TachygramRecord { pub tachygram: [u8; 32], pub blk_height: u32, pub stamp_ordinal: StampOrdinal }
pub struct EpochSummary { pub epoch: u32, pub entries: u64, pub stamps: u32, pub first_height: u32, pub last_height: u32 }
```

`STORE_FILE` is `"tachygram-v1.sqlite"`; `SCHEMA_VERSION` (1) is kept in
`PRAGMA user_version`.

## Rules

- **Extraction** (spec 8.2). `IndexedBlock::new` keeps every proof stamp, in
  transaction order, with its tachygrams in the node's order and whatever count
  the data gives (odd and zero counts included). Pointer stamps contribute
  nothing. `epoch` is `None` below the activation height.
- **Binding.** A new file records chain, genesis hash, activation height and
  epoch length; reopening with any of them different is
  `Error::BindingMismatch`, and a zero epoch length is `Error::InvalidBinding`.
  A non-empty SQLite file that is not a store, or another schema version, is
  refused.
- **Append.** A block must extend the tip: `height == tip.height + 1`
  (`HeightGap`) and `prev == tip.hash` (`ParentMismatch`). The first block of
  an empty store may have any height and parent. The publisher starts at the
  activation height, so every epoch is stored from its first block; a store
  that starts inside an epoch numbers that epoch's stamps from its first stored
  block, which is not the canonical numbering (`EpochSummary::first_height`
  shows it). `block.epoch` must match the store's schedule (`EpochMismatch`);
  stamps must be in strictly ascending `tx_index` (`StampOrder`).
- **Ordinals** (spec 8.1). Each proof stamp gets the next ordinal of its
  epoch, from zero, in block order then transaction order. Ordinals depend only
  on the stored blocks below, so a rewind and re-append reproduces them.
- **Integrity faults** (spec 8.3, 6.2). A tachygram already stored in the same
  epoch, listed twice in the block, all zero, or a proof stamp below
  activation, is `Error::Integrity(IntegrityFault::..)` and nothing is
  written. Spec 8.3 requires ingestion to stop and alert on these;
  `Error::integrity_fault()` picks them out. The same tachygram in two
  different epochs is accepted: consensus forbids reuse across the current and
  previous epoch, but each epoch is its own scope and spec 8.3 names only a
  duplicate within one epoch as the journal's fault.
- **Rewind.** `rewind(Some(keep))` removes every block above `keep`, which
  must be a stored block by height and hash (`UnknownCheckpoint`);
  `rewind(None)` removes everything. Every stored block is a checkpoint.
- **Atomicity and durability.** Each append and rewind is one transaction;
  a refused or failed one leaves the file unchanged. Committed writes survive
  a crash (WAL, `synchronous=FULL`). `committed()` is cached in memory, so one
  `Store` should own the file; `append` re-reads the tip inside its
  transaction regardless.

## Schema

```sql
binding    (id = 1, chain, genesis_hash, activation_height, epoch_length)
blocks     (height PRIMARY KEY, hash, epoch NULL below activation)
           INDEX blocks_epoch (epoch, height)
stamps     (epoch, ordinal, height -> blocks ON DELETE CASCADE, tx_index, txid, tachygram_count)
           PRIMARY KEY (epoch, ordinal), UNIQUE (height, tx_index)        WITHOUT ROWID
tachygrams (epoch, tg, height, stamp_ordinal, position)
           PRIMARY KEY (epoch, tg)                                         WITHOUT ROWID
           UNIQUE INDEX tachygrams_stamp_order (epoch, stamp_ordinal, position)
```

The `tachygrams` primary key is both the record order of a row (ascending
tachygram bytes, spec 6.2) and the epoch uniqueness of spec 8.3, so
`epoch_records` is one clustered range scan. `epoch_tachygrams` (stamp order,
spec 8.6) is a covering scan of the stamp-order index. `tachygrams` has no
foreign key: SQLite would look up the parent stamp per deleted row, which made
a rewind over 100,000 tachygrams about ten times slower. `rewind` deletes them
by stamp-order range, and `epoch_summary` checks the epoch's row count against
its stamps' counts (`Error::Corrupt` otherwise).

## Performance

`an_epoch_of_200k_tachygrams_is_cheap_to_read` appends 100 blocks of 2,000
tachygrams (one 200,000-tachygram epoch, about the testnet's rate) and reads
them back. Under `--profile release-fast` on a development machine (file on
an SSD, so every commit is a real fsync): append about 0.8 s in total,
`epoch_records` about 16 ms, `epoch_tachygrams` about 12 ms, summary and stamps
about 3 ms, and a rewind of half the epoch about 0.17 s. The test asserts the
reads stay under 10 s.

## Tests

`cargo test --locked -p tachygram-store --profile release-fast`

- `tests/store.rs`: fixture chains from `tachyon-fixtures`, served as
  `getblock <hash> 2` JSON and decoded by `tachyon-chain`, checked against an
  oracle computed from the fixtures' authored ground truth (ordinals
  restarting at each 4-block epoch, records, stamp order, summaries); reopen
  durability; rewind and re-append determinism across sessions; reorgs;
  rewind to `None`; checkpoints; refused heights, parents, epochs and stamp
  order; each integrity fault leaving the store unchanged (also across
  reopen); cross-epoch reuse; pointer stamps; odd and empty counts; binding
  mismatch; foreign files and schema versions; an injected SQL failure rolled
  back; a tampered store reported corrupt; the 200,000-tachygram epoch.
- `tests/golden.rs`: the frozen alpha-testnet blocks of
  `src/shared/tachyon-chain/tests/golden/` against that directory's
  Python-computed `expectations.json` (proof stamps, tx indices, txids,
  counts, first and last tachygram, SHA-256 of each stamp's tachygrams, epoch),
  each block then appended to a store of its own.
