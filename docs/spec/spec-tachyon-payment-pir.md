# Tachyon payment-protocol PIR services: implementation specification

Status: draft for review, 2026-11-14. Nothing in this document is implemented.

This specification defines the server side of the Tachyon payment protocol's
private-retrieval layer: four PIR databases, the publisher that builds them and
the query host that serves them. It is written to be implemented directly. It
mirrors the designs in `wallet-pir` (`../wallet-pir`, revision `40b0ba6a`)
wherever they fit, and says so where it departs from them.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## 1. Sources and what each one fixes

| Source | What this spec takes from it |
|---|---|
| The Tachyon Book, "A Deep Dive on Tachyon", section "Payment Protocol" (`../revisit.html#payment`) | The four databases, tag derivation, address shape, the wallet's sync and recovery procedures |
| `tachyon-zcash/tachyon` book, `bundle.md`, `tachygrams.md`, `anchor.md` | Bundle wire format, the `vMemoTachyon` field, stamps, tachygrams, epochs |
| Draft Tachyon ZIPs (`tachyon-zcash/zips` pull request 5, open, last updated 2026-10-21): bundle format, shielded protocol, NuTachyon upgrade | Consensus limits and encodings: action and tachygram counts, tachygram ordering and uniqueness, anchor acceptance, epoch numbering |
| Tachyon alpha testnet (`https://tachyscan.cnode.phd`, JSON-RPC `http://tachyscan.cnode.phd:18232`) | The chain the services ingest; observed data shapes and volumes (section 3) |
| `wallet-pir` at `40b0ba6a` | Publication model, serving contract, PIR profile, table layouts, conventions |

Two facts found during research shape the design and are worth knowing first.

1. **No payment-protocol data exists on chain yet.** Every `memo` field sampled
   on the testnet is empty (290 Tachyon transactions across 51 blocks). Notes
   are handed over out of band as JSON files. The on-chain envelope that carries
   tags and ciphertexts is therefore defined here, provisionally (section 5).
2. **`wallet-pir`'s hosts cannot be extended from outside.** `pir-query-server`
   owns each product's serving code in-crate, and `zakura-server-chain` exposes
   no generic block API. This workspace reuses the shared libraries and the
   publisher host library, but writes its own query host and its own chain
   decoder (section 4).

## 2. Scope

### In scope

- Four PIR products, each with a publisher library and a serving module:
  - **memo**: the epoched memo database, `tag => memo`.
  - **first-contact**: the first-contact memo database, `tag => KEM.c || memo`.
  - **tachygram**: the epoched tachygram database.
  - **registry**: the off-chain address registry (PKI), `H(addr) => addr`,
    including its write API and abuse controls.
- One publisher binary and one query-server binary.
- A chain decoder for the Tachyon testnet node.
- The wallet-facing wire protocol, to the byte.
- A diagnostic probe client and fixture tooling, used only for testing.

### Out of scope

- The wallet client library: key derivation, handshake, tag derivation,
  decryption, the sync state machine, Faerie-gold checks, mnemonic recovery.
  Section 12 states what the servers assume about wallet behaviour.
- Integration with any specific wallet.
- Proof construction of any kind.

### Interfaces only

- **Wallet-state recovery blobs.** The envelope reserves an entry kind for them
  (section 5.2). Ingestion recognises and counts them and builds no index.
- **Oblivious sync service (OSS).** The tachygram store exposes the per-epoch
  tachygram stream an OSS needs (section 8.6). No OSS is built.

## 3. Chain facts this spec relies on

Observed on the Tachyon alpha testnet on 2026-11-14. Parameters marked
"observed" are read from the node at start-up and MUST NOT be hard-coded.

| Fact | Value | Source |
|---|---|---|
| Node | Zakura 1.8.2, `chain: "test"` | `getinfo`, `getblockchaininfo` |
| NuTachyon activation height | 8 (observed) | `getblockchaininfo.upgrades` |
| Epoch length | 4096 blocks | `zcash_tachyon::constants::EPOCH_SIZE` |
| Block interval | about 9 s | explorer dashboard |
| Tip at time of writing | 39,586 | `getblockcount` |
| Retained tachygrams (current and previous epoch) | 321,736 | explorer dashboard |
| Tachygram size | 32 bytes | bundle wire format |
| Memo field | one `vMemoTachyon` per bundle, variable length, covered by the sighash | bundle wire format |

Derived quantities:

- `pool_height = height - activation_height`
- `epoch = pool_height / 4096`
- An epoch lasts about 10.2 hours on the testnet. At a 75 s block interval it
  would last about 3.6 days.
- The testnet produces roughly 190,000 tachygrams per epoch (321,736 retained
  over about 6,790 blocks, scaled to 4,096).

Stamp shapes, as returned by `getblock <hash> 2` under `tx[].tachyon`:

- A **proof** stamp has `anchor`, `tachygramSet`, `tachygrams[]`, `proof`,
  `coverage` and `feeZat`. Its `tachygrams` cover every action the stamp
  proves, including actions of other transactions in the block.
- A **pointer** stamp has `aggregateId` and `feeZat`, and no tachygrams. The
  transaction's own `actions` and `memo` are still present.
- In the sample, 42 of 290 Tachyon transactions carried proof stamps. Tachygram
  counts per proof stamp ranged from 5 to 108 and were often odd. The draft
  ZIPs require exactly two tachygrams per covered action, so the testnet build
  does not yet follow them. Ingestion MUST read counts from the data and MUST
  NOT assume that rule.

Limits from the draft ZIPs, which the decoder enforces as integrity checks once
the node follows them (configuration flag `strict_zip_rules`, off for the alpha
testnet):

- A bundle has 0 to 4,095 actions. A proof stamp has 2 to 8,190 tachygrams,
  so one stamp covers at most 4,095 actions.
- Tachygrams within a stamp are in strictly ascending byte order. Their order
  therefore says nothing about which action produced which tachygram.
- All tachygrams in a block are distinct, and distinct from those of earlier
  blocks in the current and preceding epoch.
- A stamp's anchor is an end-of-block pool state in the block's epoch or the
  one before.
- The activation block has pool height zero and begins epoch zero.
- The memo is opaque to consensus and has no structure. The ZIPs prescribe no
  payment protocol, so the envelope of section 5 remains this spec's to define.
- The epoch length is not final (marked TODO in the upgrade ZIP).

The node does not serve `gettachyoninfo` on the public RPC port. The services
depend only on `getblockchaininfo`, `getblockcount`, `getblockhash` and
`getblock` at verbosity 2.

Byte order of 32-byte values:

- **Block hashes and transaction ids** (including `genesis_hash`, `anchor_hash`
  and `prev`) are held in **protocol order**, the reverse of the hex the node's
  RPC prints, as `wallet-pir`'s chain reader (`pir-chain/src/hash.rs`) does.
  Every hash input in this document (placement, section 6.3) uses protocol
  order. JSON documents (the map, configuration) write them in **display
  order**, the same hex the node prints, so an operator can compare them with
  the node and the explorer directly.
- **Tachygrams, anchors, tags and digests** are not reversed: their bytes are
  the RPC hex decoded left to right. The testnet confirms this for tachygrams:
  every sampled proof stamp lists them in ascending order of that hex, which is
  the ascending byte order the draft ZIPs require.

## 4. Architecture

### 4.1 Processes

```
Zakura node ──JSON-RPC──▶ tachyon-pir-publisher ──HTTPS pull──▶ tachyon-pir-query-server ◀── wallets
                              ▲
                              └── wallets (registry writes only)
```

- **`tachyon-pir-publisher`**: one process, one chain follower, four product
  publishers. It never contacts query servers. Exactly one publisher runs per
  network, because the registry has a single writer.
- **`tachyon-pir-query-server`**: serves any compiled subset of the four
  products. Version 1 supports only the `combined` role (routing, evaluation
  and packing in one process), as `wallet-pir` does for transaction details.
  Replicas are independent and pull from the publisher.

### 4.2 Workspace layout

A new Cargo workspace at `tachyon-payments/`. Package layout follows
`wallet-pir`: products under `src/<product>/`, shared libraries under
`src/shared/`, hosts at `src/`, tools under `tools/`.

| Package | Path | Purpose |
|---|---|---|
| `tachyon-chain` | `src/shared/tachyon-chain` | Decodes verbose blocks into `TachyonBlock`; epoch arithmetic; network identity |
| `tachyon-pir-protocol` | `src/shared/tachyon-pir-protocol` | Wallet-facing contract: constants, row layouts, placement hashing, map and header types. No server dependencies |
| `tachyon-pir-table` | `src/shared/tachyon-pir-table` | The shared bucket-row table: build, validate, segment, digest |
| `memo-store`, `memo-publisher` | `src/memo/` | Journal and publication for memo and first-contact |
| `tachygram-store`, `tachygram-publisher` | `src/tachygram/` | Journal and publication for tachygrams |
| `registry-store`, `registry-publisher` | `src/registry/` | Registration log, write API, publication |
| `tachyon-pir-publisher` | `src/tachyon-pir-publisher` | Publisher binary |
| `tachyon-pir-query-server` | `src/tachyon-pir-query-server` | Query host binary; product serving modules live in-crate |
| `tachyon-pir-cli` | `tools/client` | Diagnostic probe client |
| `tachyon-pir-e2e-tests` | `tools/e2e-tests` | End-to-end tests across real HTTP boundaries |
| `tachyon-fixtures` | `tools/fixtures` | Node double and synthetic envelope generator |

Cargo features select products independently on both hosts: `memo`,
`first-contact`, `tachygram`, `registry`. Default is none. A configuration
section for a product that was not compiled in MUST be rejected at start-up.

Dependency rules, enforced by a boundary check in CI as in
`wallet-pir/tools/ci/boundary.py`:

- Serving code never depends on chain or ingestion code.
- Product crates never depend on each other. `first-contact` shares the
  `memo-*` crates by design and counts as the same product family.

### 4.3 What is reused from `wallet-pir`

Taken as git dependencies pinned to `40b0ba6a`:

| Crate | Used for |
|---|---|
| `pir-publication` | Objects, publication envelope, authority fence, consumer floor |
| `pir-pipeline` | `Ingestion`, `Publication`, `Publisher` traits; `PublisherEngine`; `ConsumerEngine` |
| `pir-sync-manager` | HTTPS source, object fetcher, envelope consumer |
| `pir-service-discovery` | Publisher/query-server version contract |
| `pir-control` | Process identity, admission queue, per-client slots, `PublicMount`, durable files |
| `pir-observability` | HTTP metrics |
| `pir-node-rpc` | Bounded JSON-RPC transport |
| `pir-publisher` (library) | `run_registered` and `PublisherDefinition` |

Taken from crates.io at the versions `wallet-pir` pins: `zakura-pir-protocol`,
`zakura-pir-publisher`, `zakura-pir-query-server` (`=0.0.1-rc5`) for the native
8-bit PIR profile. The root manifest MUST carry the same `[patch.crates-io]`
block as `wallet-pir` for `ipir-sp`, `inspiring`, `reinspiring` and
`simplepir-kernel` (git `valargroup/ipir-sp`, rev `5ea080a3`).

Written new, because the originals cannot be extended:

- The query host (`pir-query-server` is not a plugin host).
- The chain decoder (`zakura-server-chain` exposes only per-product modules).

### 4.4 Conventions copied

- Rust `1.97.1` in `rust-toolchain.toml`.
- Profiles `release` (fat LTO, deploy) and `release-fast` (tests).
- `make check-fast`, `check-package`, `check-correctness`, `check-full`,
  `doctor`.
- Each CPU test under 30 s; the warm correctness run under 5 minutes. Slow
  cases sit behind a `qualification-tests` feature.
- Docs under `docs/<product>/` with `README.md`, `protocol.md`,
  `architecture.md`, `deployment.md`, `qualification.md`, `CHANGELOG.md`.
- Evidence under `evidence/<product>/<topic>-<date>/README.md`.
- No compatibility windows: a format change is a new protocol identifier and a
  direct cutover.

## 5. On-chain envelope (provisional)

The Tachyon pool treats `vMemoTachyon` as an opaque blob. The payment protocol
needs it to carry, per output note, a tag and a ciphertext. One bundle can
create several notes, so the blob is a list of entries.

This format is defined here because none is deployed. It is owned by the
payment-protocol authors and is expected to change; see decision D1.

### 5.1 Envelope

```
envelope   = magic entry_count entry*
magic      = "TPM1"                      4 bytes
entry_count= u8                          1..=16
entry      = kind tag body
kind       = u8
tag        = 32 bytes
```

| `kind` | Name | `body` | Body length |
|---|---|---|---|
| `0x01` | follow-up | `ciphertext` | 128 |
| `0x02` | first contact | `kem_ciphertext \|\| ciphertext` | 1088 + 128 = 1216 |
| `0x03` | wallet state | `len (u16 LE) \|\| ciphertext` | 2 + `len`, `len` ≤ 1024 |

- `MEMO_CT_BYTES = 128`. Provisional: a 72-byte note opening (`v`, `ψ`, `rcm`),
  40 bytes of payload and a 16-byte AEAD tag.
- `KEM_CT_BYTES = 1088`. Provisional: ML-KEM-768.
- The envelope MUST be consumed exactly; trailing bytes make it invalid.

### 5.2 Ingestion rules

The blob is attacker-controlled. Ingestion MUST treat it as data.

- An empty memo is skipped.
- A memo that does not parse as a valid envelope is skipped whole and counted
  (`tachyon_memo_envelopes_invalid_total`). It MUST NOT fail ingestion.
- Kind `0x01` entries go to the memo database; kind `0x02` to first-contact.
- Kind `0x03` entries are counted and otherwise ignored.
- Unknown kinds make the envelope invalid.
- Entries are ordered by `(height, tx index in block, entry index)`. This
  order is the entry's **ordinal** and is used wherever entries must be sorted
  deterministically.

### 5.3 Duplicate tags

A tag is visible in the mempool before it is mined, so anyone can publish a
second entry with the same tag, and anyone who holds an address can publish
entries at its first-contact tag. The databases therefore store **every** entry
for a tag, in ordinal order, and the wallet authenticates by decryption. A
first-entry-wins rule would let a front-runner censor a payment and MUST NOT be
used.

## 6. The bucket-row table

All four products use one table design. It is the single-table layout of
`wallet-pir`'s transaction-detail product, chosen over the Receiver
directory-plus-companion design because it needs one query per lookup and
gives an identical transcript for a hit and a miss. The Receiver design
remains the fallback if large-value fill becomes a cost problem (decision D4).

### 6.1 Geometry

| Name | Rows | Row bytes | Raw size |
|---|---:|---:|---:|
| `tachyon-4k8` | 4,096 | 8,192 | 32 MiB |
| `tachyon-2k8` | 2,048 | 8,192 | 16 MiB |

These are the two geometries `wallet-pir` already runs for transaction details
on the native 8-bit profile. No other geometry is valid in version 1.

### 6.2 Row layout

A row holds `SLOTS = floor(8192 / ENTRY_BYTES)` fixed-size entries.

- Occupied entries come first, sorted ascending by `(key, ordinal)`.
- Remaining slots and the row tail are zero. An all-zero entry is empty; no
  valid entry is all zero, because every entry begins with a nonzero key.
  A key of all zero bytes is rejected at ingestion: in memo and first-contact
  an entry whose `key16` is all zero makes its envelope invalid (section 5.2);
  in tachygram an all-zero tachygram is an integrity fault handled like a
  duplicate (section 8.3); in registry the registration is refused
  (section 9.3).
- A row that violates any of this is non-canonical. Publisher, query host and
  client each MUST validate rows they produce or decode, as Status
  `decode_row` does.

| Product | Entry | `ENTRY_BYTES` | `SLOTS` | Target load per row | Entries per `4k8` table |
|---|---|---:|---:|---:|---:|
| memo | `key16 \|\| ciphertext(128)` | 144 | 56 | 34 | 139,264 |
| first-contact | `key16 \|\| kem_ct(1088) \|\| ciphertext(128)` | 1,232 | 6 | 1 | 4,096 |
| tachygram | `tg(32) \|\| blk_height(u32) \|\| stamp_ordinal(u32)` | 40 | 204 | 124 | 507,904 |
| registry | `key16 \|\| pk(32) \|\| ek(1184)` | 1,232 | 6 | 1 | 4,096 |

Target load is about 61% of `SLOTS` for the small-entry tables, matching the
transaction-detail product. The two 1,232-byte tables hold only six entries per
row, so they target a mean of one entry per row and rely on seed retry
(section 6.4) to avoid overflow.

Integers are little-endian throughout.

### 6.3 Keys and placement

`key16` is the first 16 bytes of the 32-byte tag or address digest. Truncation
is safe: a colliding prefix is equivalent to a duplicate tag, which the design
already tolerates (section 5.3), and the wallet authenticates by decryption.
The tachygram table stores the full 32-byte tachygram and uses it as the key.

A **scope** is the unit a table set covers: an epoch (memo, tachygram), a span
of epochs (first-contact) or a chunk (registry). Each scope has a `scope_id`
(u64) and is split into `n_buckets` tables, `n_buckets ∈ {1, 2, 4, …, 64}`.

```
h(s)   = SHA-256(domain || genesis_hash(32) || scope_id(u64) || s(u32) || key)
bucket = u32_le(h(scope_seed)[4..8]) mod n_buckets
row_k  = u32_le(h(seed_k)[0..4]) mod rows        for segment k of that bucket
```

- `key` is `key16` for memo, first-contact and registry, and the full 32-byte
  tachygram for tachygram. `genesis_hash` is in protocol order (section 3).
- `scope_seed` is the seed of segment 0. It is the same in every bucket of the
  scope, so a client computes the bucket once, from any segment-0 table of the
  scope. `seed_k` is the `seed` of segment `k` of that bucket, so the client
  computes a row separately for each segment it queries. A key's bucket never
  depends on a segment seed.

| Product | `domain` |
|---|---|
| memo | `"tachyon-pir/memo/v1/place\0"` |
| first-contact | `"tachyon-pir/first-contact/v1/place\0"` |
| tachygram | `"tachyon-pir/tachygram/v1/place\0"` |
| registry | `"tachyon-pir/registry/v1/place\0"` |

- `u32_le(h[0..4])` is the `H(tg)[:4]` key of the Tachyon Book's tachygram
  database; the other products use the same construction for uniformity.
- `scope_id` is hashed in so that the same key lands in unrelated buckets in
  different scopes. A wallet asks for the same next tag in several epochs;
  without this the bucket index would link those queries.
- `n_buckets` is the smallest power of two for which the expected load per row
  is at or below the target. It costs the wallet `log2(n_buckets)` bits of
  privacy per query, at most 6.

### 6.4 Seeds, overflow and segments

- `scope_seed` is the smallest `s` in `0..MAX_SEEDS` for which, with bucket
  and row both computed under `s`, no row in any bucket exceeds `SLOTS`. Each
  bucket then has exactly one segment, with `seed = scope_seed`.
- If no seed works, `scope_seed = 0` and buckets are assigned under seed 0.
  Each bucket's segment 0 is filled under seed 0 by taking the bucket's entries
  in ordinal order and placing each in its row if that row has a free slot. An
  entry whose row is full **spills**, in ordinal order, to the next
  **segment** of the same bucket.
- Segment `k ≥ 1` of a bucket takes the entries spilled from segment `k - 1`.
  Its seed is the smallest `s` in `0..MAX_SEEDS` under which all of them fit
  (rows computed with `h(s)`). If none fits, it is filled under seed 0 the same
  way as segment 0, and its own spill goes to segment `k + 1`.
- Entries are taken in ordinal order, so every entry of a key in segment `k`
  has a lower ordinal than every entry of that key in segment `k + 1`. A client
  that concatenates its per-segment results in segment order therefore returns
  them in ordinal order.
- At most `MAX_SEGMENTS = 8` segments per bucket; beyond that the publication
  fails, the previous publication keeps serving and an alert is raised.
- A client MUST query every segment of the bucket it selects. Segment counts
  are public in the map.
- Entries are never evicted. Status's eviction rule does not apply, because
  these are membership data.

Overflow can be forced by grinding keys against a public seed. The cost to the
attacker is one fee-paying action per entry; the effect is extra segments, which
is an availability cost and not a correctness or privacy failure.

### 6.5 Open and sealed tables

- A scope is **open** while it can still change. Its tables are rebuilt in full
  at every publication, as the Receiver open shard and the transaction-detail
  recent shard are.
- A scope is **sealed** once its last block is `SEAL_DEPTH` blocks below the
  tip. Its tables are built once and are then immutable and content-addressed.
- `SEAL_DEPTH = 100` blocks (provisional; Enhance uses 1,000 on mainnet).
- Table bytes are a pure function of the canonical chain and the constants in
  this document. Two publishers on the same chain MUST produce identical
  sealed tables. Tests MUST assert this across restart and across different
  publication cadences.

### 6.6 PIR profile

The native ReinspiRING two-mask 8-bit profile, exactly as Receiver and
transaction details use it:
`reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1` (d = 2048, q = 2^54, p = 2^8).
CPU only.

- No per-client keys are stored. Key material rides in each query.
- Public setup material is deterministic per table and published as an object.
- Query hosts prepare runtimes from published rows before switching to a new
  publication, as Receiver hosts do.
- Reference sizes from `wallet-pir` for `4k8`: upload 28,168 bytes, reply
  10,256 bytes, public setup about 60 KB, resident runtime about 160 MiB per
  table. These MUST be re-measured for this workspace before they are quoted
  as its own.

## 7. Product: memo and first-contact

### 7.1 Memo (epoched)

- **Scope**: one Tachyon epoch. `scope_id = epoch`.
- **Source**: kind `0x01` entries from every Tachyon bundle in the epoch's
  blocks, whatever the stamp type.
- **Lookup result**: all entries whose `key16` matches, in ordinal order.
- **Absence**: a canonical row with no matching entry means "no memo with this
  tag in this scope up to the map's anchor".

### 7.2 First-contact

- **Scope**: a **span** of consecutive whole epochs. `scope_id` is the span's
  first epoch.
- **Source**: kind `0x02` entries.
- **Span rule**: spans are decided at epoch boundaries and depend only on the
  chain. The open span closes at the first epoch boundary where either holds:
  - it contains at least `FC_SPAN_TARGET = 3,072` entries (75% of one `4k8`
    table at target load), or
  - it covers `FC_SPAN_MAX_EPOCHS = 64` epochs.
- A span that closes by count has `n_buckets = 1`. A single epoch that alone
  exceeds one table's target uses more buckets under the rule in section 6.3.
- The span seals `SEAL_DEPTH` blocks after its last block.

Long spans are the point: a wallet checks one first-contact tag per address it
has handed out, per span, and first contacts are rare.

### 7.3 Journal

`memo-store` keeps one SQLite database, `memo-v1.sqlite`, as the source of
truth for both tables, following `receiver-store`.

```
Store::open(path) -> Store
Store::append(block: &IndexedBlock) -> Result<()>
Store::rewind(to: BlockId) -> Result<()>
Store::checkpoint_at_or_below(height) -> Option<BlockId>
Store::entries(kind, scope) -> impl Iterator<Item = Entry>
Store::spans() -> Vec<Span>
```

`IndexedBlock` holds the block id, epoch and the parsed entries with their
ordinals. `append` is atomic per block. `rewind` removes every block above the
target and any span boundary decided by them.

## 8. Product: tachygram

### 8.1 Record

```
tg            32 bytes   the tachygram
blk_height    u32 LE     height of the block containing the stamp
stamp_ordinal u32 LE     zero-based ordinal of the stamp among proof stamps in
                         its epoch, in block order then transaction order
```

The record is 40 bytes. `blk_height` and `stamp_ordinal` together locate the
creation stamp, which is what a wallet needs in order to fetch it.

This departs from the Tachyon Book's 41-byte sketch
(`tg || blk_height || anchor_height || action_count`) in two ways:

- **`action_count` is removed.** The book's layout is carried over from the
  Orchard spendability-PIR nullifier entry, where a one-byte count of the
  spending transaction's actions sized a follow-up query. In Tachyon the
  per-transaction count cannot be computed from an aggregate stamp, the stamp's
  own count can reach 4,095 and does not fit in a byte, and the stamp states
  its tachygram count itself. No wallet exists that reads the field. It is
  added back, at the width a real consumer needs, only when one does.
- **`anchor_height` is named `stamp_ordinal` and given one meaning.** The
  anchor chain advances once per proof stamp, so the ordinal identifies the
  post-stamp anchor the book describes. The publisher does not recompute
  anchors.

Changing the record later is a new `protocol` string and a rebuild of the
tachygram tables, which the no-compatibility-window rule of section 4.4
permits.

### 8.2 Scope and source

- **Scope**: one epoch. `scope_id = epoch`.
- **Source**: `tachygrams[]` of every **proof** stamp. Pointer stamps
  contribute nothing; their actions' tachygrams appear in the covering
  aggregate's stamp.

### 8.3 Uniqueness

Consensus rejects a tachygram that already appears in the current or previous
epoch. A duplicate tachygram within one epoch is therefore a decoder or node
fault. Ingestion MUST stop and alert; it MUST NOT publish.

### 8.4 Lookup semantics

A wallet uses this table for two things: to locate its note commitment
(inclusion) and to check that a per-epoch nullifier is absent (exclusion).

- **Found**: exactly one entry with the full 32-byte tachygram.
- **Absent**: a canonical row with no such entry. Absence is meaningful only
  for the heights the map says the scope covers. For a sealed scope that is the
  whole epoch; for an open scope it is the epoch's blocks up to the anchor.
- The client MUST validate the whole row before trusting either answer:
  sorted order, zero padding, every `blk_height` inside the scope's range, and
  every entry's placement hash equal to this row and bucket.

### 8.5 Journal

`tachygram-store` keeps `tachygram-v1.sqlite` with the same `open`, `append`,
`rewind` and `checkpoint_at_or_below` operations as `memo-store`, plus
`stamps(epoch)`.

### 8.6 OSS interface (not built)

`tachygram-store` exposes

```
Store::epoch_tachygrams(epoch) -> impl Iterator<Item = (StampOrdinal, [u8; 32])>
```

in stamp order. This is the input an OSS routes into buckets. The function is
public API so that a later OSS crate can depend on it without changing the
journal.

## 9. Product: registry

The registry is the only product that takes writes and the only one with no
chain source. It follows the same publication and serving design, with a
registration log in place of the chain journal.

### 9.1 Address and digest

```
addr   = pk(32) || ek(1184)                     1,216 bytes, ML-KEM-768 (provisional)
digest = SHA-256("tachyon-pir/registry/v1/addr\0" || addr)
key16  = digest[0..16]
```

### 9.2 Chunks

- **Scope**: a **chunk**, a run of registrations in log order.
  `scope_id = chunk_id`, counting from 0.
- A chunk closes when it holds `REGISTRY_CHUNK_ENTRIES = 3,072` entries. It is
  sealed immediately; there is no reorg to wait for.
- Only the last, open chunk is rebuilt at each publication. This is the
  Tachyon Book's size-capped chunking.
- `n_buckets = 1` always.

A sender must know which chunk to query. The registration response returns the
chunk id, and the short address a recipient shares is `(chunk_id, digest)`.
Chunk ids never change. The lookup therefore reveals the chunk, giving an
anonymity set of at most 3,072 addresses (decision D3).

### 9.3 Write API

Served by the publisher process on its own public listener, mounted at
`/registry/v1` with legacy alias `/v1/registry`.

`GET /registry/v1/challenge`

```json
{ "challenge": "<32 bytes hex>", "difficulty_bits": 20, "expires_unix": 1794670000 }
```

`POST /registry/v1/register`, body:

```
challenge(32) || nonce(8) || addr(1216)
```

- Proof of work: `SHA-256("tachyon-pir/registry/v1/pow\0" || challenge || nonce || digest)`
  MUST have at least `difficulty_bits` leading zero bits. Binding the digest
  stops one solution being reused for many addresses.
- Challenges are stateless: an HMAC over the expiry under a publisher secret.
  Lifetime `REGISTRY_CHALLENGE_SECONDS = 600`.
- Validation: body length exact; `pk` a canonical field element; `ek` passes
  the ML-KEM-768 encapsulation-key modulus check; `key16` not all zero.
- Success response, `200`:

```json
{ "chunk_id": 12, "digest": "<64 hex>", "created": true, "visible_after_sequence": 4821 }
```

- Registration is idempotent. A repeated address returns its existing
  `chunk_id` with `"created": false`.
- The response is sent only after the log entry is durable (fsync).
  `visible_after_sequence` is the first publication sequence that will contain
  the entry.
- Entries are never deleted or changed.

Abuse controls:

| Control | Value |
|---|---|
| Proof of work | `difficulty_bits` configurable, default 20 |
| Per-client concurrent registrations | 2 (`pir_control::ClientSlots`) |
| Per-client rate | 6 per minute, configurable |
| Body limit | 1,256 bytes exactly; otherwise `400` or `413` |
| Upload deadline | 15 s, then `408` |
| Global rate | configurable cap per minute; above it `429` with `Retry-After` |

Errors use the common body of section 10.5.

### 9.4 Log

`registry-store` keeps `registry-v1.sqlite`: an append-only table of
`(sequence, digest, addr)` with a unique index on `digest`. Chunk membership is
`sequence / REGISTRY_CHUNK_ENTRIES`. A backup of this file is a complete backup
of the registry.

## 10. Wallet-facing protocol

### 10.1 Mounts

Each product is mounted twice with `pir_control::PublicMount`, as in
`wallet-pir`:

| Product | Canonical | Legacy alias |
|---|---|---|
| memo | `/memo/v1` | `/v1/memo` |
| first-contact | `/first-contact/v1` | `/v1/first-contact` |
| tachygram | `/tachygram/v1` | `/v1/tachygram` |
| registry | `/registry/v1` | `/v1/registry` |

Every response carries `Cache-Control: no-store` except immutable objects
addressed by digest.

### 10.2 Routes

| Route | Returns |
|---|---|
| `GET map` | The map head (JSON, at most 16 KiB), with header `X-Map-Sha256` |
| `GET map/{base}/{sha256}` | An immutable chunk describing 32 scopes starting at scope index `base` |
| `GET public/{session_id}` | Public setup material for one table (octet-stream, immutable) |
| `POST query` | One encrypted row query |

### 10.3 Map

The map tells a wallet which tables exist and how to address them. All structs
refuse unknown fields, so adding a field requires a new `protocol` string.

Head:

```json
{
  "protocol": "tachyon-memo-pir-v1",
  "profile": "reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1",
  "network": "test",
  "genesis_hash": "<64 hex>",
  "activation_height": 8,
  "epoch_length": 4096,
  "generation": 9123,
  "recovery_epoch": "3",
  "anchor_height": 39584,
  "anchor_hash": "<64 hex>",
  "floor_scope": 0,
  "sealed_chunks": [ { "base": 0, "sha256": "<64 hex>" } ],
  "open_scopes": [ /* Scope objects */ ]
}
```

- `protocol` is `tachyon-memo-pir-v1`, `tachyon-first-contact-pir-v1`,
  `tachyon-tachygram-pir-v1` or `tachyon-registry-pir-v1`.
- The registry map omits `activation_height`, `epoch_length`, `anchor_height`
  and `anchor_hash`, and carries `log_sequence` instead.
- `floor_scope` is the oldest scope this provider serves (section 11.5).

Scope:

```json
{
  "scope_id": 9,
  "state": "open",
  "first_height": 36872,
  "last_height": 39584,
  "entries": 51230,
  "n_buckets": 1,
  "tables": [ /* one Table per (bucket, segment) */ ]
}
```

Table:

```json
{
  "bucket": 0,
  "segment": 0,
  "geometry": "tachyon-4k8",
  "seed": 0,
  "entries": 51230,
  "rows_sha256": "<64 hex>",
  "public_sha256": "<64 hex>",
  "session_id": "<64 hex>"
}
```

A sealed scope's `last_height` is the last block of its epoch, span or chunk.
For an open scope it is the anchor height.

### 10.4 Sessions and queries

```
session_id = SHA-256("tachyon-pir/v1/session\0" || protocol_len(u8) || protocol
                     || scope_id(u64) || bucket(u32) || segment(u32)
                     || rows(u32) || seed(u32) || rows_sha256 || public_sha256)
```

The session id is content-addressed and excludes generation and anchor, so the
session for an unchanged sealed table survives every new publication.

Query body:

```
magic(4) || session_id(32) || nonce(16) || payload
```

| Product | `magic` |
|---|---|
| memo | `TMQ1` |
| first-contact | `TFQ1` |
| tachygram | `TGQ1` |
| registry | `TRQ1` |

- The 52-byte header is echoed at the start of the response, followed by the
  packed answer. This is the Receiver `RPQ1` framing.
- The payload length MUST equal the single length the profile defines for the
  table's geometry. Any other length is `400`.
- Body limit 512 KiB; upload deadline 15 s.
- The server MUST NOT log or label metrics with any client-supplied byte.

### 10.5 Errors

JSON body `{"code": "...", "message": "..."}`, as Enhance returns.

| HTTP | `code` | Meaning | Client action |
|---|---|---|---|
| 400 | `invalid_request` | Wrong length, bad magic, malformed body | Do not retry |
| 408 | `upload_timeout` | Body not received in time | Retry |
| 409 | `stale_routing` | Session belongs to a map this server no longer holds; body includes `map_sha256` | Refetch map, retry once |
| 410 | `session_unavailable` | Session revoked by a reorg | Refetch map; re-accept the anchor |
| 413 | `invalid_request` | Body over the limit | Do not retry |
| 429 | `overloaded` | Admission refused; `Retry-After: 1` | Retry later |
| 503 | `temporarily_unavailable` | Not ready or no serving authority | Retry later |

### 10.6 Admission

Per product, using `pir_control::admission::Queue` with Receiver's values as
the starting point: 2 executing, 8 waiting, 2 s wait, 2 in flight per client.
These are configuration and MUST be calibrated in qualification, not assumed.

## 11. Publisher, distribution and consistency

### 11.1 Chain decoding

`tachyon-chain` wraps `pir_node_rpc::RpcClient` and exposes:

```
NodeClient::network_identity() -> NetworkIdentity { chain, genesis_hash, activation_height }
NodeClient::tip() -> BlockId
NodeClient::block_hash(height) -> Hash
NodeClient::block(hash) -> TachyonBlock

TachyonBlock { id: BlockId, prev: Hash, txs: Vec<TachyonTx> }
TachyonTx    { txid, memo: Vec<u8>, own_actions: u32, stamp: Stamp }
Stamp        = Proof { anchor, tachygram_set, tachygrams: Vec<[u8; 32]> }
             | Pointer { aggregate_id }
```

- Blocks are always fetched by hash after resolving the height once, so a
  reorg cannot change the block under a read.
- The decoder MUST reject a block whose hash, height or transaction ids do not
  match what was requested, as `tachyscan` does.
- `activation_height` is read from `getblockchaininfo.upgrades` by the name
  `NuTachyon`. If the upgrade is absent the publisher refuses to start.
- The decoder reads the JSON form. A raw-transaction decoder using the
  `zcash_tachyon` crate is a later hardening step (milestone M4), needed
  because the JSON shape is not a stable contract.

### 11.2 Ingestion

Each chain product implements `pir_pipeline::ingestion::Ingestion`.

- `target_height(tip) = tip - CONFIRMATION_DEPTH`, with
  `CONFIRMATION_DEPTH = 2` (Receiver's default).
- On every pass the follower walks durable checkpoints newest-first against
  the node's canonical hashes. `withdraw_above` is always called before
  `rewind`.
- A batch is accepted only if its newest block is still canonical afterwards.

### 11.3 Publication

Each product implements `pir_pipeline::Publication` and `Publisher` and uses
`pir_publication` unchanged:

- Table rows, public setup material and map chunks are **objects**: immutable,
  addressed by SHA-256 and byte length, installed atomically.
- The **current publication** is a `PublicationEnvelope` whose descriptor names
  the map head. Advancement is atomic and never waits for consumers.
- Distribution prefixes: `/provider/memo/v1/`, `/provider/first-contact/v1/`,
  `/provider/tachygram/v1/`, `/provider/registry/v1/`, each with `current`,
  `objects/<sha256>` and `authority/check`.
- Distribution schema strings: `tachyon-<product>-prepared-distribution-v1`.
- The five normative rules of `wallet-pir/docs/spec/spec-publisher-ingest-model.md`
  apply as written.

Publication cadence: one publication per ingestion pass that changed
something, coalesced to one active and one pending, as Enhance does.

### 11.4 Reorgs

- A rewind that stays inside open scopes produces a new generation. Sessions of
  the rebuilt open tables change because their content changes.
- A rewind that reaches a sealed scope increments `recovery_epoch`, revokes the
  sessions of every affected table (`410`) and rebuilds them. Withdrawal is
  durable before the journal rewinds.
- A publisher restart always starts a new recovery epoch, as
  `PublicationAuthority` requires.

### 11.5 Retention

Every sealed table costs resident memory on each query host (about 160 MiB by
`wallet-pir`'s measurement), and the testnet seals an epoch about every ten
hours. Unbounded retention is not affordable on one host.

- Each product has `retain_scopes` in query-server configuration. The host
  serves the newest `retain_scopes` scopes and reports the oldest as
  `floor_scope`.
- A query for a scope below the floor has no session and cannot be formed; the
  wallet learns the floor from the map and reports "below this provider's
  floor" to its caller.
- The publisher keeps every sealed object. Retention is a serving decision.
- Default `retain_scopes`: memo 64, tachygram 64, first-contact unlimited,
  registry unlimited.

An archive tier that serves old scopes from wider, merged tables is future
work, as it is for Receiver.

### 11.6 Query host

`tachyon-pir-query-server` runs one `pir_pipeline::consumer::ConsumerEngine`
per product over `pir_sync_manager::HttpsSource`.

- It checks `/.well-known/pir-service-discovery` before start and before each
  sync pass, and exits on a confirmed major-version mismatch.
- It fetches objects, verifies digests, prepares runtimes and only then
  switches to the new publication. A partial publication is never served.
- It keeps the displaced publication for 600 s so in-flight queries finish.
- It answers `503` for a product's namespace until that product is ready.

### 11.7 Configuration

JSON with unknown fields refused, shaped like `wallet-pir`'s examples.

Publisher:

```json
{
  "management_listen": "127.0.0.1:9400",
  "chain": { "rpc_url": "http://tachyscan.cnode.phd:18232", "no_auth": true, "poll_ms": 1000 },
  "memo":          { "enabled": true, "settings": { "state_dir": "/var/lib/tachyon-pir/memo" } },
  "first_contact": { "enabled": true, "settings": {} },
  "tachygram":     { "enabled": true, "settings": { "state_dir": "/var/lib/tachyon-pir/tachygram" } },
  "registry":      { "enabled": true, "settings": { "state_dir": "/var/lib/tachyon-pir/registry",
                     "public_listen": "127.0.0.1:9410", "difficulty_bits": 20 } }
}
```

`first_contact` shares the memo state directory and requires `memo` to be
enabled.

Query server:

```json
{
  "management_listen": "127.0.0.1:9500",
  "public_listen": "127.0.0.1:9510",
  "memo": { "enabled": true, "settings": {
    "publisher": "https://publisher.example", "state_dir": "/var/lib/tachyon-pir-query/memo",
    "role": "combined", "publisher_network": "test", "publisher_genesis": "<64 hex>",
    "retain_scopes": 64 } }
}
```

Both binaries accept `--config FILE` and `--check` (validate offline).

### 11.8 Observability

- Management listener: `GET /health`, `GET /ready`, `GET /metrics`, with the
  response shapes of `wallet-pir`.
- HTTP metrics from `pir-observability`, with fixed endpoint labels.
- Product metrics, all prefixed `tachyon_<product>_`:
  `ingested_height`, `anchor_height`, `entries_total`, `open_scope_entries`,
  `segments_overflow_total`, `publication_seconds`, `sessions_revoked_total`.
- Memo adds `envelopes_invalid_total` and `state_entries_total`.
- Registry adds `registrations_total{result}` and `pow_rejected_total`.

## 12. Trust and privacy

### 12.1 What the wallet must do

These are assumptions the servers make. They are requirements on a wallet, and
are listed so that the wire protocol can be judged against them.

- Accept `anchor_hash` independently, against its own view of the chain,
  before using any table.
- Authenticate memo and first-contact results by decryption, and registry
  results by recomputing the digest.
- Validate every decoded row (section 8.4).
- Treat tachygram results as hints. Spendability is finally established by the
  proof the wallet builds and consensus checks.

### 12.2 What the server is trusted for

Completeness. No product carries an inclusion or absence proof, as none does in
`wallet-pir`. A dishonest server can withhold a memo or wrongly report a
tachygram absent. It cannot forge a memo, an address or a spendable note. The
consequence of withholding is a missed payment until the wallet syncs against
an honest provider.

### 12.3 What each query hides and reveals

Hidden: the row within the table, and therefore the key, among all keys in that
table.

Revealed to the server:

- the product, scope, bucket and segment queried;
- the number and timing of queries;
- the client's network identity.

Consequences worth stating plainly:

- A hit and a miss send identical transcripts.
- The anonymity set of a query is the entry count of its bucket. An open epoch
  that has just started is small. It is public in the map, so a wallet can see
  it and decide.
- Bucketing costs up to 6 bits. Registry chunking limits the set to 3,072.
- The number of memo queries a wallet makes per epoch tracks its number of
  channels unless it pads. Padding policy belongs to the wallet. The server
  side provides what padding needs: a dummy query is indistinguishable from a
  real one.

### 12.4 Registry-specific

- Registration reveals the address and the registrant's network identity to
  the publisher. A recipient who wants to hide the link registers through an
  anonymising transport; the service does nothing to prevent that.
- The registry is public data. Anyone can download every address by fetching
  the objects.

## 13. Testing and acceptance

### 13.1 Test layers

| Layer | What it proves | Budget |
|---|---|---|
| Unit | Row build and decode, placement, envelope parsing, span and chunk rules | each under 30 s |
| Golden | Decoder output for frozen real testnet blocks, checked in as fixtures | each under 30 s |
| Determinism | Identical sealed tables across restart, cadence and rebuild | each under 30 s, small fixtures |
| End to end | Publisher behind TLS, separately spawned query host, probe client | each under 30 s |
| Qualification | Full-geometry soak and live-testnet oracle runs | behind `qualification-tests` |

Rules carried over from `wallet-pir`:

- Oracles are independent. The expected answer for a lookup is computed from
  raw blocks by code that does not share the table builder.
- Negative cases and restart and recovery paths are tested, across real HTTP
  boundaries.
- Fast fixtures use small synthetic chains from the node double. Deployed-size
  parameters are exercised only in qualification.

### 13.2 Acceptance scenarios

Each product must pass every row that applies to it.

| # | Scenario | Pass condition |
|---|---|---|
| 1 | Publisher with no consumers | Publications advance; nothing blocks |
| 2 | Cold bootstrap over TLS | Query host reaches ready from empty state |
| 3 | Incremental publication | Only changed objects are fetched; sealed sessions survive |
| 4 | Exact answers | n of n lookups match the independent oracle, hits and misses |
| 5 | Duplicate keys | All entries for a repeated tag are returned in ordinal order |
| 6 | Forced overflow | Ground keys produce extra segments; all entries remain retrievable |
| 7 | Invalid envelopes | Malformed memos are skipped and counted; ingestion continues |
| 8 | Shallow reorg | New generation; answers match the new chain |
| 9 | Reorg into a sealed scope | Recovery epoch increments; old sessions return `410`; new answers are correct |
| 10 | Corrupt or truncated object | Query host refuses it and keeps serving the previous publication |
| 11 | Publisher outage | Query host keeps serving within its authority rules and then answers `503` |
| 12 | Restart of each process | State recovers; no wrong answer is served |
| 13 | Overload | `429` with `Retry-After`; admitted queries complete |
| 14 | Registry writes | Idempotent; durable before acknowledgement; visible at the promised sequence; proof of work and rate limits enforced |
| 15 | Live testnet | The tachygram product follows the alpha testnet for 24 hours with exact answers |

### 13.3 Qualification gate

Modelled on `wallet-pir/docs/status/status_qualification.md`. Per product:

- 20 open-loop lookups per second for 6 hours at full geometry;
- every answer checked against an independent oracle;
- no wrong answer, no failed arrival, no out-of-memory event, no swap growth;
- p99 latency under 1 s;
- results recorded under `evidence/<product>/` with build identity, command,
  configuration, host, and an explicit "not established" list.

Passing automated checks is not a qualification certificate.

## 14. Milestones

| Milestone | Delivers | Exit criterion |
|---|---|---|
| M0 | Workspace, toolchain, CI gates, `tachyon-chain`, node double | Golden decoder tests pass on frozen testnet blocks |
| M1 | `tachyon-pir-table`, `tachyon-pir-protocol`, tachygram product end to end, probe client | Scenarios 1–4, 6, 8–13 for tachygram; scenario 15 |
| M2 | Envelope parser, memo and first-contact products, synthetic envelope generator | Scenarios 1–13 for both |
| M3 | Registry product with write API | Scenarios 1–4, 6, 10–14 for registry |
| M4 | Raw-transaction decoder, retention, deployment packaging, qualification runs | Section 13.3 for all four products |

Tachygram comes first because it is the only product with real data on the
testnet today. Memo and first-contact can be proven only on synthetic chains
until wallets publish envelopes.

## 15. Constants

| Constant | Value | Status |
|---|---:|---|
| `EPOCH_LENGTH` | 4,096 | From the Tachyon crate; not final in the draft ZIPs. Read from configuration per network and published in the map |
| `ROW_BYTES` | 8,192 | From `wallet-pir` |
| `MEMO_CT_BYTES` | 128 | Provisional (D1) |
| `KEM_CT_BYTES` | 1,088 | Provisional (D1) |
| `KEM_EK_BYTES` | 1,184 | Provisional (D1) |
| `MAX_ENVELOPE_ENTRIES` | 16 | Provisional (D1) |
| `MAX_STATE_BYTES` | 1,024 | Provisional (D1) |
| `MAX_BUCKETS` | 64 | From `wallet-pir` txid |
| `MAX_SEEDS` | 64 | From `wallet-pir` txid |
| `MAX_SEGMENTS` | 8 | From `wallet-pir` Receiver |
| `CONFIRMATION_DEPTH` | 2 | From `wallet-pir` Receiver |
| `SEAL_DEPTH` | 100 | Provisional |
| `FC_SPAN_TARGET` | 3,072 | Provisional |
| `FC_SPAN_MAX_EPOCHS` | 64 | Provisional |
| `REGISTRY_CHUNK_ENTRIES` | 3,072 | Provisional (D3) |
| `REGISTRY_CHALLENGE_SECONDS` | 600 | Provisional |
| `MAP_CHUNK_SCOPES` | 32 | From `wallet-pir` txid |

## 16. Open decisions

Each has a provisional answer in this document so that work can start.

| ID | Decision | Provisional answer | Who decides |
|---|---|---|---|
| D1 | Envelope format, ciphertext size, ML-KEM level | Section 5; 128-byte ciphertext; ML-KEM-768 | Payment-protocol authors |
| D2 | Tachygram record contents | Decided: 40 bytes, `action_count` removed, `anchor_height` defined as `stamp_ordinal` (section 8.1). Revisit when a wallet design exists | This project |
| D3 | Registry chunk size and how a sender learns the chunk | 3,072 entries; chunk id in the short address | Payment-protocol authors |
| D4 | Table design for the 1,232-byte products | Single table at one entry per row; Receiver's directory-plus-companion if the 17% fill is too costly | Implementers, after M3 measurements |
| D5 | Publisher discovery identity | Inherit `pir-publisher`'s service name and version through `run_registered` | `wallet-pir` maintainers |
| D6 | How the wallet obtains a stamp's full tachygram set for witness construction | Not served by these products; the wallet fetches the block | Payment-protocol authors |

## 17. Risks

- **Dependency access.** Whether `valargroup/wallet-pir` and
  `valargroup/ipir-sp` can be fetched as git dependencies by every builder was
  not checked. M0 must confirm it.
- **Testnet drift.** The alpha testnet's JSON shape and tachygram counts
  already differ from the Tachyon book and the draft ZIPs. The decoder is the
  only component exposed to that, and golden fixtures will show when it moves.
- **Transaction format is unsettled.** The draft ZIPs leave the transaction
  version and the bundle's placement open (ZIP 248 registration is pending).
  This affects only the raw-transaction decoder planned for M4.
- **Profile sizes are borrowed.** Query, reply and memory figures are
  `wallet-pir` measurements for a different entry layout. They are planning
  numbers until re-measured here.
- **Memory growth.** At about 160 MiB per table and two chain tables per epoch,
  64 retained epochs is about 20 GiB per host on testnet volumes. Higher
  throughput multiplies buckets and with them memory.
