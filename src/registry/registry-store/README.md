# registry-store

The registry product's registration log and source of truth,
`registry-v1.sqlite` (spec sections 9.2 to 9.4, 10.3 and scenario 14 of 13.2,
`docs/spec/spec-tachyon-payment-pir.md`). It mirrors `tachygram-store` and
`memo-store`: one SQLite file in WAL mode with `synchronous=FULL`, every write
in one `IMMEDIATE` transaction, bound to its network at creation. Addresses,
digests and `REGISTRY_CHUNK_ENTRIES` come from `tachyon-pir-protocol`.

## API

```rust
pub struct Binding { pub chain: String, pub genesis_hash: BlockHash }
pub struct Registration { pub sequence: u64, pub chunk_id: u64, pub digest: [u8; 32], pub created: bool }
impl Registration {
    pub fn response(&self) -> tachyon_pir_protocol::Registration;   // the 200 body of spec 9.3
}
pub const fn chunk_of(sequence: u64) -> u64;      // sequence / REGISTRY_CHUNK_ENTRIES
pub const fn chunk_start(chunk_id: u64) -> u64;   // chunk_id * REGISTRY_CHUNK_ENTRIES, saturating

impl Store {                                       // Send + Sync; share behind an Arc
    pub fn open(path: impl AsRef<Path>, binding: Binding) -> Result<Store, Error>;
    pub fn binding(&self) -> &Binding;
    // Writes (the publisher's POST /registry/v1/register)
    pub fn register(&self, address: &Address) -> Result<Registration, Error>;
    pub fn register_many(&self, addresses: &[Address]) -> Result<Vec<Registration>, Error>;
    // Reads (the publisher's table builds and map)
    pub fn log_sequence(&self) -> Result<u64, Error>;
    pub fn chunk_count(&self) -> Result<u64, Error>;
    pub fn chunk(&self, chunk_id: u64) -> Result<Vec<(u64, Address)>, Error>;
    pub fn chunk_before(&self, chunk_id: u64, log_sequence: u64) -> Result<Vec<(u64, Address)>, Error>;
    pub fn lookup(&self, digest: &[u8; 32]) -> Result<Option<Registration>, Error>;
    // Backup
    pub fn checkpoint(&self) -> Result<(), Error>;
}
```

`STORE_FILE` is `"registry-v1.sqlite"`; `SCHEMA_VERSION` (1) is kept in
`PRAGMA user_version`; `REGISTRY_CHUNK_ENTRIES` is re-exported. There is no
rewind, update or delete.

## Rules

- **Sequences and chunks** (spec 9.4, 10.3). Entries take sequences 0, 1, 2,
  ... with no gaps; chunk membership is `sequence / REGISTRY_CHUNK_ENTRIES`.
  `log_sequence()` is the number of entries, which is the map's
  `log_sequence` and the next registration's sequence. `chunk_count()` is
  `log_sequence / 3072 + 1`: every sealed chunk plus the one open chunk, which
  is empty right after a chunk fills and for an empty log. `chunk(c)` returns
  the chunk's entries in sequence order, which is the entry ordinal of the
  table; a chunk beyond the open one is empty. `chunk_before(c, s)` returns
  only the entries below sequence `s`: the publisher reads `log_sequence()`
  once per publication and builds every chunk with it, so registrations that
  land meanwhile cannot make a chunk disagree with its map.
- **Registration** (spec 9.3). `register` is idempotent: an address already
  in the log (found by digest) returns its existing sequence and chunk with
  `created: false` and writes nothing. Otherwise it is appended at the next
  sequence, read inside the same `IMMEDIATE` transaction, and `register`
  returns only after the commit is durable (WAL fsync under
  `synchronous=FULL`); the caller sends the HTTP response after it returns.
  `register_many` is a group commit of the same thing, all or nothing, with
  one fsync: one result per input, a repeat within the batch `created: false`.
- **Validation.** The store stores what it is given and checks only its own
  integrity: an all-zero `key16` is `Error::ZeroKey`. The spec 9.1 `pk`
  canonical and `ek` modulus checks, and the rest of spec 9.3's validation, are
  `registry-publisher`'s and run before `register`.
- **Integrity.** Every read checks each entry: lengths, that the address
  hashes to its stored digest, and no gap in sequences (`Error::Corrupt`). A
  repeat registration whose stored address differs from the given one under
  the same digest is `Error::Corrupt`.
- **Binding** (spec 9.2 Identity). A new file records chain, genesis hash and
  `REGISTRY_CHUNK_ENTRIES`. Reopening under another identity is
  `Error::BindingMismatch`; a file whose chunk size differs from this build's
  is `Error::ChunkRuleMismatch` (chunk ids never change). A non-empty SQLite
  file that is not a store is `Error::NotAStore`, another schema version
  `Error::UnsupportedSchema`, a file that is not SQLite `Error::Sql`. A
  refused open changes nothing.
- **Backup** (spec 9.4). The file holds the whole registry. Committed entries
  may still sit in `registry-v1.sqlite-wal`, so either copy the file after
  `checkpoint()` (`wal_checkpoint(TRUNCATE)`, which empties the WAL) with no
  registration running in between, or copy the main file and its `-wal`
  together while the store is stopped. The `challenge-secret` of spec 9.3 is
  not registry content: restoring without it only invalidates challenges that
  have not yet expired.

## Concurrency

`Store` holds two connections, each behind a `std::sync::Mutex`:

- the **writer**, locked by `register`, `register_many` and `checkpoint` for
  their whole transaction. Registrations are serialised in the process, and
  because the next sequence is read inside the `IMMEDIATE` transaction that
  inserts it, a second process on the same file waits on SQLite's write lock
  (5 s busy timeout) and still cannot produce a duplicate or a gap;
- a **reader** (`PRAGMA query_only=ON`) for every read. Under WAL it sees the
  last committed state without blocking or waiting for the writer, so the
  publisher's chunk reads do not delay registrations.

A poisoned mutex is taken over: a panic mid-transaction drops the transaction,
which rolls back. All methods block on SQLite and fsync; from async code call
them in `spawn_blocking`. Throughput is bounded by one fsync per registration
(or per `register_many` batch).

## Schema

```sql
binding        (id = 1, chain, genesis_hash BLOB(32), chunk_entries)
registrations  (sequence INTEGER PRIMARY KEY >= 0,
                digest BLOB(32) with digest[0..16] != 0,
                addr BLOB(1216))
               UNIQUE INDEX registrations_digest (digest)
TRIGGER registrations_gap_free       BEFORE INSERT: NEW.sequence must be MAX(sequence) + 1, or 0
TRIGGER registrations_never_changed  BEFORE UPDATE: abort
TRIGGER registrations_never_deleted  BEFORE DELETE: abort
```

`sequence` is the rowid, so a chunk is one clustered range scan and
`log_sequence` is `MAX(sequence) + 1`. The `CHECK` constraints and triggers
make the file itself append-only and gap-free whichever connection writes.

## Performance

`one_chunk_of_a_100k_log_is_cheap_to_read` group-commits 100,000 addresses
and reads chunks back. Under `--profile release-fast` on a development machine
(NVMe SSD, ext4): writing about 0.55 s, a sealed chunk of 3,072 entries with
digest verification about 3 ms, the open chunk about 1.5 ms, a lookup about
40 us. The test asserts each chunk read under 1 s and the lookup under 0.1 s.
On the same disk, a single `register` costs one fsync; 670 concurrent
registrations take about 0.2 s.

## Tests

`cargo test --locked -p registry-store --profile release-fast`

- `src/store.rs`: the writer runs with `synchronous=FULL` in WAL mode and the
  reader is `query_only`.
- `tests/store.rs`: idempotency (single and in batches, `created`, an
  unchanged `log_sequence`, the `200` body); sequences and chunks across the
  first chunk boundary with 3,081 authored addresses, including the empty
  open chunk right after chunk 0 fills, chunks beyond the open one, and
  `chunk_before`; durability across reopen; an unclosed store (connections
  leaked) and a copy of its files taken while open both reopen complete; a
  child process killed with SIGKILL mid-registration loses no acknowledged
  registration, and holds at most the one in flight beyond them; 16 threads
  racing over their own and a shared set of addresses (some in group commits)
  get distinct gap-free sequences, one entry and one `created: true` per
  address; binding and chunk-size mismatches; foreign, non-SQLite and
  future-schema files; the file refusing gaps, reused sequences, wrong
  lengths, zero keys, repeated digests, updates and deletes from another
  connection; tampered entries reported corrupt; an injected failure in a
  group commit writing nothing; a checkpointed copy of the main file alone
  restoring identical contents and continuing at the same next sequence; and
  the 100,000-entry log.
