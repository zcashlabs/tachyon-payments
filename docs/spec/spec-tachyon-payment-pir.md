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
- In the sample, 42 of 290 Tachyon transactions carried proof stamps. A wider
  read-only scan for the golden fixtures (17,739 blocks) found proof stamps
  with 3 to 112 tachygrams, often odd: always twice the covered actions plus 1
  to 16. The draft ZIPs require exactly two tachygrams per covered action, so
  the testnet build does not yet follow them. Ingestion MUST read counts from
  the data and MUST NOT assume that rule. `strict_zip_rules` does not check it
  either, because covered actions cannot be counted from the JSON without
  linking pointer stamps to their aggregate.
- `aggregateId` is 64 bytes. On the alpha testnet it is the covering proof
  transaction's txid followed by its `authdigest`, each in protocol order, and
  that proof is always in the same block. Ingestion MUST NOT rely on this.
- Every Tachyon bundle carries a stamp. A bundle without one, or a stamp that
  carries the other kind's fields, is a decode error.
- `tx[].tachyon.memo` is the hex of `vMemoTachyon`, `""` when empty.
  Transactions without a Tachyon bundle (for example the coinbase) have no
  `tachyon` key.
- `getblockchaininfo.upgrades` is an object keyed by branch id (NuTachyon is
  `fffffffc` on the testnet), each entry carrying `name`, `activationheight`
  and `status`. Lookup is by `name`.
- JSON-RPC errors arrive with HTTP 200 and `{result: null, error: {code,
  message}}`: `-8` for a height above the tip, `-5` for an unknown block hash.
  A block reorged away may be answered either with `-5` or with
  `confirmations: -1`; the follower MUST treat both as not canonical, and the
  decoder rejects a block whose `confirmations` is negative.

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
| `tachyon-pir-table` | `src/shared/tachyon-pir-table` | The shared bucket-row table: build, validate, segment, digest; the P8 table runtime |
| `tachyon-pir-distribution` | `src/shared/tachyon-pir-distribution` | Publisher-to-query-host contract shared by all products: descriptor, inventory, the publisher's object store and authority with its `current`, `objects` and `authority/check` routes, and the consumer-side inventory reader. No chain dependencies |
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
section for a product that was not compiled in MUST be rejected at start-up,
even when its `enabled` is false.

Dependency rules, enforced by a boundary check in CI as in
`wallet-pir/tools/ci/boundary.py`:

- Serving code never depends on chain or ingestion code.
- Product crates never depend on each other. `first-contact` shares the
  `memo-*` crates by design and counts as the same product family.
- Shared libraries never depend on product crates. Machinery more than one
  product needs (the table, the distribution contract) therefore lives in
  `src/shared/` rather than being repeated per product.

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

Taken from crates.io at the versions `wallet-pir` pins, for the native 8-bit
PIR profile (section 6.6): `zakura-pir-protocol` (feature `native`; masks,
sizes), `zakura-pir-server-core` (hint, preprocessing, public setup),
`zakura-pir-query-server` (upload parsing, reply packing), `zakura-pir-client`
(feature `native`; query construction and decoding), all `=0.0.1-rc5`, and
`reinspiring` `=0.1.3-rc.1` (scan kernel). None of `wallet-pir`'s product
wrappers (`transparent-shared`, `receiver-pir-server`, `pir-query-server`) is
used: they are product-specific, and txid's `TableProfile` selects the 16-bit
profile for any table name it does not know. The root manifest MUST carry the same `[patch.crates-io]`
block as `wallet-pir` for `ipir-sp`, `inspiring`, `reinspiring` and
`simplepir-kernel` (git `valargroup/ipir-sp`, rev `5ea080a3`).

Written new, because the originals cannot be extended:

- The query host (`pir-query-server` is not a plugin host).
- The chain decoder (`zakura-server-chain` exposes only per-product modules).
- The table runtime: about 120 lines over the free P8 primitives, as
  Receiver's `Prepared` is.
- Each product's distribution routes (`current`, `objects/<sha256>`,
  `authority/check`). `pir_publication` defines the types but serves no
  routes; Receiver writes its own, and so does each product here.
- A TLS edge for tests. `pir-publisher` serves plain HTTP only and
  `pir_sync_manager::HttpsSource` accepts only `https://` origins, so a TLS
  terminator sits between them, as in `wallet-pir`'s tests and deployments.

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
- A wallet-state `len` may be 0.
- One envelope may carry the same tag more than once; every entry is kept
  (section 5.3).
- The longest valid envelope is 16 first-contact entries,
  `MAX_ENVELOPE_BYTES = 19,989` bytes.

### 5.2 Ingestion rules

The blob is attacker-controlled. Ingestion MUST treat it as data.

- An empty memo is skipped; it is not an envelope and is not counted as
  invalid. A nonempty memo shorter than 5 bytes is invalid.
- A memo that does not parse as a valid envelope is skipped whole and counted
  (`tachyon_memo_envelopes_invalid_total`). It MUST NOT fail ingestion.
- Kind `0x01` entries go to the memo database; kind `0x02` to first-contact.
- Kind `0x03` entries are counted and otherwise ignored.
- Unknown kinds make the envelope invalid.
- Entries are ordered by `(height, tx index in block, entry index)`. This
  order is the entry's **ordinal** and is used wherever entries must be sorted
  deterministically. The tx index is the transaction's position in the block's
  `tx` list, the coinbase being 0 (section 11.1); the entry index counts every
  entry of the envelope, of all kinds.

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

Version 1 publishes every table at `tachyon-4k8`. `tachyon-2k8` is defined
(its name, sizes and seeds are fixed here) but not published: it saves 7,168
bytes of upload and about 10% of a table's memory, at the cost of half the
anonymity set per table and a second code path. txid likewise runs one
geometry per deployment and keeps the other for rollback only. Clients MUST
reject a map that names any geometry other than `tachyon-4k8`.

### 6.2 Row layout

A row holds `SLOTS = floor(8192 / ENTRY_BYTES)` fixed-size entries.

- Occupied entries come first, sorted ascending by `(key, ordinal)`.
- Remaining slots and the row tail are zero. An all-zero entry is empty; no
  valid entry is all zero, because every entry begins with a nonzero key.
  A key of all zero bytes is rejected at ingestion: in memo and first-contact
  an entry of kind `0x01` or `0x02` whose `key16` (the first 16 bytes of its
  tag) is all zero makes its envelope invalid (section 5.2); the tag of a kind
  `0x03` entry, which builds no index, is not checked;
  in tachygram an all-zero tachygram is an integrity fault handled like a
  duplicate (section 8.3); in registry the registration is refused
  (section 9.3).
- A row that violates any of this is non-canonical. Publisher, query host and
  client each MUST validate rows they produce or decode, as Status
  `decode_row` does.
- The ordinal is not stored in memo, first-contact or registry entries, so a
  reader cannot check the ordinal part of the order. Readers check that keys
  are non-decreasing (strictly increasing for tachygram, whose keys are
  unique); placing equal keys in ordinal order is the publisher's obligation,
  and tests check it against the oracle.

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
  fails, the previous publication keeps serving and an alert is raised. (The
  value matches Receiver's shard-segment limit but is this spec's own
  constant; txid has no overflow-segment cap.)
- A client MUST query every segment of the bucket it selects. Segment counts
  are public in the map.
- Entries are never evicted. Status's eviction rule does not apply, because
  these are membership data.

Overflow can be forced by grinding keys against a public seed. The cost to the
attacker is one fee-paying action per entry; the effect is extra segments, which
is an availability cost and not a correctness or privacy failure.

### 6.5 Open and sealed tables

- A scope is **open** while it can still change. Its entries are recomputed
  from the journal at every publication, as the Receiver open shard and the
  transaction-detail recent shard are. A publisher MAY reuse tables built from
  an identical entry list, since table bytes are a pure function of it.
- A scope is **sealed** once its last block is at least `SEAL_DEPTH` blocks
  below the publication's anchor (`last_height + SEAL_DEPTH ≤ anchor_height`).
  Its tables are built once and are then immutable and content-addressed.
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
- The query masks and the packing setup are derived from two public 32-byte
  seeds per (product, geometry), never from data, as Receiver and txid derive
  theirs:

  ```
  seed(purpose) = SHA-256("tachyon-pir/v1/native-p8/" || product || "\0"
                          || geometry || "\0" || purpose)
  masks = P8.public_query_masks(seed("query-masks"), rows, 8192)
  setup = NativeSetup::new(P8.params(), seed("packing-setup"))
  ```

  `product` is `memo`, `first-contact`, `tachygram` or `registry`; `geometry`
  is the geometry name; `purpose` is `query-masks` or `packing-setup`. A client
  derives both once per product and geometry.
- Public setup material is a deterministic function of the seeds and the row
  bytes, and is published as an object. The publisher therefore prepares every
  table it publishes in full (about 1 s per `4k8` table on a 32-thread host,
  about 5 s on one thread), because `public_sha256` enters the session id.
  Coalescing (section 11.3) bounds that cost.
- Query hosts prepare runtimes from published rows before switching to a new
  publication, as Receiver hosts do, and check that each prepared public setup
  equals the published object.
- Sizes, measured for this workspace on 2026-11-14
  (`docs/shared/wallet-pir-integration.md` section 1):

  | Quantity | `tachyon-4k8` | `tachyon-2k8` |
  |---|---:|---:|
  | Query payload, `P8.request_len_bits(rows, 28)` | 28,160 | 20,992 |
  | Request body, 52-byte header + payload | 28,212 | 21,044 |
  | Reply body, `P8.response_len(8192)` | 10,240 | 10,240 |
  | Response, 52-byte header + reply | 10,292 | 10,292 |
  | Public setup, `P8.public_len(8192)` | 45,056 | 45,056 |
  | Runtime memory, planning bound | 160.17 MiB | 144.17 MiB |

  `wallet-pir`'s published txid figures (28,168, 10,256, about 60 KB) include
  txid's own framing and base64. Building a runtime adds a transient peak of a
  few hundred MiB. The compiled matrices, not the rows, dominate memory, so
  `2k8` costs about 90% of `4k8`.

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
- `n_buckets` follows section 6.3 for every span. A span that closes by count
  holds at least 3,072 entries and fewer than 3,072 plus its last epoch's
  entries, so it usually has one bucket; a span whose last epoch is large has
  more.
- The span seals `SEAL_DEPTH` blocks after its last block.
- Readers (query hosts, clients) can check only a span's shape: whole
  consecutive epochs, at most `FC_SPAN_MAX_EPOCHS`. The closing rule depends on
  per-epoch counts the map does not carry; it is the publisher's obligation,
  checked by tests against the oracle.

Long spans are the point: a wallet checks one first-contact tag per address it
has handed out, per span, and first contacts are rare.

### 7.3 Journal

`memo-store` keeps one SQLite database, `memo-v1.sqlite`, as the source of
truth for both tables, following `receiver-store`.

```
Store::open(path, binding) -> Result<Store>
Store::append(block: &IndexedBlock) -> Result<()>
Store::rewind(keep: Option<&BlockId>) -> Result<()>
Store::checkpoint_at_or_below(height) -> Result<Option<BlockId>>
Store::entries(kind, scope) -> Result<impl Iterator<Item = Entry>>
Store::spans() -> Result<Vec<Span>>
```

`IndexedBlock` holds the block id, epoch and the parsed entries with their
ordinals. `append` is atomic per block. `rewind` removes every block above
`keep`, or every block when `keep` is `None`, matching the `Ingestion` trait's
`rewind`, and any span boundary decided by them. Reads return `Result`: an
SQL error read as "no checkpoint" would rewind everything. The `binding`
records the network identity and epoch length; reopening a journal under a
different binding is refused.

Every journal starts at the activation height. Ordinals, and the stamp
ordinals of section 8.1, are canonical only for a scope stored from its first
block; a publisher MUST NOT publish a scope whose first stored height is not
the scope's first height.

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
- **Ordinal**: a tachygram entry's ordinal (section 5.2) is
  `(blk_height, tx index in block of its stamp's transaction, position in the
  stamp's tachygrams[])`, which is the order of `epoch_tachygrams`
  (section 8.6). It decides which entries spill to overflow segments
  (section 6.4).

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
  every entry's placement hash equal to this row and bucket. Registry rows add
  one check: every entry's `key16` equals the first 16 bytes of the digest of
  its `addr` (section 9.1).

### 8.5 Journal

`tachygram-store` keeps `tachygram-v1.sqlite` with the same `open`, `append`,
`rewind` and `checkpoint_at_or_below` operations as `memo-store`, plus
`stamps(epoch)`. A tachygram repeated in a later epoch is not a journal fault:
each epoch's table is its own scope, and consensus, not this journal, rules on
reuse across the epoch boundary.

### 8.6 OSS interface (not built)

`tachygram-store` exposes

```
Store::epoch_tachygrams(epoch) -> Result<impl Iterator<Item = (StampOrdinal, [u8; 32])>>
```

in stamp order, and within a stamp in the order the stamp lists them. This is the input an OSS routes into buckets. The function is
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

Served by the publisher process on the registry product's listener, mounted
at `/registry/v1` with legacy alias `/v1/registry`. That listener is private
(section 11.7), so a reverse proxy exposes the write API publicly. The proxy
MUST overwrite `X-Forwarded-For` with the connecting client's address; the
per-client controls below key on that header (through `pir_control`'s client
key) and fall back to the peer address when it is absent.

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
  `visible_after_sequence` is the entry's log sequence `s` (section 9.4,
  counting from 0). The entry is contained in every registry publication
  whose map has `log_sequence > s`; the publisher publishes such a map at its
  next publication.
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
| `GET map` | The map head (JSON, at most 1 MiB), with header `X-Map-Sha256` |
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
  and `anchor_hash`, and carries `log_sequence` instead: the number of log
  entries the publication contains, which is also the sequence the next
  registration will receive. Registry Scope objects omit `first_height` and
  `last_height`; chunk `c` holds the entries with sequences
  `c × REGISTRY_CHUNK_ENTRIES` up to that plus `entries`. An empty log is
  published with one open chunk 0 holding no entries. Field order of the
  registry head and scope:

  ```json
  { "protocol": "tachyon-registry-pir-v1", "profile": "…", "network": "test",
    "genesis_hash": "<64 hex>", "generation": 41, "recovery_epoch": "2",
    "log_sequence": 4821, "floor_scope": 0, "sealed_chunks": [ … ],
    "open_scopes": [ { "scope_id": 1, "state": "open", "entries": 1749,
                       "n_buckets": 1, "tables": [ … ] } ] }
  ```
- `floor_scope` is the scope index (below) of the oldest scope this provider
  serves (section 11.5).
- The head is at most `MAX_MAP_HEAD_BYTES = 1 MiB`. Open scopes list their
  tables in the head, and one open scope can reach 64 buckets × 8 segments,
  about 512 tables of about 330 bytes each; two such scopes must fit.
- Encoding: hex is lowercase; integers are little-endian wherever this
  document hashes or stores them; the map head and map chunk objects are
  compact JSON (no whitespace) with fields in the order shown in this section,
  so two publishers on the same chain produce identical chunk digests
  (section 6.5). `recovery_epoch` is a JSON string holding the decimal of a
  positive u64, as in `wallet-pir`'s maps; `generation` is a JSON number.

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

A scope's `last_height` is the last block of its epoch or span. For the newest
scope, which is open, it is the anchor height. An older scope that is still
open (its last block is less than `SEAL_DEPTH` below the tip) ends at its own
last block.

`rows_sha256` is the SHA-256 of the table's `rows × 8192` row bytes in
row-major order; `public_sha256` is the SHA-256 of its public setup bytes.
Clients MUST refuse a map head or chunk whose bytes differ from the canonical
encoding of the parsed value.

Tables are listed in ascending `(bucket, segment)` order. Every bucket of a
scope that has entries has a segment-0 table, possibly with zero entries.

Scope listing:

- Every scope has a **scope index**: the epoch for memo and tachygram (equal to
  `scope_id`), the span's position in the sequence of spans for first-contact
  (0, 1, 2, …), and `chunk_id` for registry.
- Every scope from index 0 up to the newest is listed exactly once: sealed
  scopes in map chunks, open scopes in `open_scopes`, in ascending index.
- The registry always has exactly one open chunk, at index
  `log_sequence / REGISTRY_CHUNK_ENTRIES` holding
  `log_sequence % REGISTRY_CHUNK_ENTRIES` entries; every sealed chunk holds
  exactly `REGISTRY_CHUNK_ENTRIES`. A chunk that has just filled is therefore
  sealed, and the next, empty chunk is open.
- Map chunk `k` describes the sealed scopes with index in
  `32k .. 32k + 32`. It is the JSON object
  `{"protocol": "<protocol>", "base": 32k, "scopes": [Scope, …]}`, refusing
  unknown fields, served at `map/{base}/{sha256}` and stored as an object. A
  chunk that does not yet hold 32 sealed scopes is reissued under a new digest
  as further scopes seal; `sealed_chunks` lists every chunk by ascending
  `base`.
- A scope with no entries has `n_buckets = 1` and no tables (`"tables": []`).
  Every lookup in it is absent; a client that wants an identical transcript
  sends a query to another table of the same product instead. This keeps empty
  epochs from costing query-host memory.

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
  table's geometry (`P8.request_len_bits(rows, 28)`, section 6.6). Any other
  length is `400`.
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

Any other refusal (unknown route, wrong method, a chunk `base` that does not
match its digest) carries an `invalid_request` body with its status; an
internal failure answers `503`. Revocations are held in memory, so after a
host restart a revoked session answers `409`, not `410`; the client's action
(refetch the map) is the same.

`GET map/{base}/{sha256}` for a chunk the host does not hold, and
`GET public/{session_id}` for a session it does not serve, answer as
`POST query` does. `503` MAY carry `Retry-After`.

A query host tells `409` from `410` as follows. A session in the current
publication, or in a displaced publication still within its 600 s retention,
is served. A session that was present in a publication the host displaced
because the publisher's `recovery_epoch` increased, and is absent from the
current publication, is **revoked**: the host remembers it for the retention
window and answers `410`. Any other unknown session is `409` with the current
`map_sha256`.

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
TachyonTx    { tx_index: u32, txid, memo: Vec<u8>, own_actions: u32, stamp: Stamp }
Stamp        = Proof { anchor, tachygram_set, tachygrams: Vec<[u8; 32]> }
             | Pointer { aggregate_id: [u8; 64] }
```

- `txs` holds only transactions that carry a Tachyon bundle. `tx_index` is the
  transaction's zero-based position among **all** transactions of the block,
  coinbase included, so indices may have gaps. It is the "tx index in block" of
  section 5.2.

- Blocks are always fetched by hash after resolving the height once, so a
  reorg cannot change the block under a read.
- The decoder MUST reject a block whose `hash` differs from the requested
  hash, whose `height` differs from the height the hash was resolved from, any
  of whose transactions reports a `blockhash` or `height` other than the
  block's, whose `nTx` differs from the number of listed transactions, or
  whose txids are malformed or repeated.
- `activation_height` is read from `getblockchaininfo.upgrades` by the name
  `NuTachyon`. If the upgrade is absent the publisher refuses to start.
- The decoder reads the JSON form. A raw-transaction decoder using the
  `zcash_tachyon` crate is a later hardening step (milestone M4), needed
  because the JSON shape is not a stable contract.

### 11.2 Ingestion

Each chain product implements `pir_pipeline::ingestion::Ingestion`, whose
methods are `target_height`, `start_height`, `observed`, `committed`,
`checkpoint_at_or_below`, `withdraw_above`, `rewind`, `ingest(from, through)`,
`caught_up` and `failed`.

- `target_height(tip) = tip - CONFIRMATION_DEPTH`, with
  `CONFIRMATION_DEPTH = 2` (Receiver's default).
- On every pass the follower walks durable checkpoints newest-first against
  the node's canonical hashes. `withdraw_above` is always called before
  `rewind`.
- `ingest` fetches and decodes its blocks itself through `tachyon-chain`; the
  feed carries only tips and canonical hashes.
- A batch is accepted only if its newest block is still canonical afterwards.
- Publication runs from `caught_up`, through
  `PublisherExecutor::publish` and the phases `check`, `build`, `store`,
  `verify`, `assemble`, `recheck`, `commit`, as Receiver's `Export` does.
- `pir_pipeline::ingestion::BlockId` and `ChainObservation` carry hashes as
  display-order strings; they are converted to protocol order (section 3) at
  this boundary.

### 11.3 Publication

Each product implements `pir_pipeline::Publication` and `Publisher` and uses
`pir_publication` unchanged:

- Table rows, public setup material and map chunks are **objects**: immutable,
  addressed by SHA-256 and byte length, installed atomically.
- The **current publication** is a `PublicationEnvelope` whose descriptor names
  the map head. Advancement is atomic and never waits for consumers.
- Distribution prefixes: `/provider/memo/v1/`, `/provider/first-contact/v1/`,
  `/provider/tachygram/v1/`, `/provider/registry/v1/`, each with `current`,
  `objects/<sha256>` and `authority/check`. Each product serves these routes
  itself over the `pir_publication` types, as Receiver's distribution does.
- The descriptor carries the map head's object reference, `generation` and
  `recovery_epoch`. `generation` is this product's own durable counter,
  strictly increasing across publications; an attempt that fails may leave a
  gap. `recovery_epoch` is the authority fence's.
- `PublicationScope` is `{ product: <schema string>, chain: genesis_hash,
  source }`, where for the chain products
  `source = SHA-256("tachyon-pir/v1/distribution-source\0" || "chain\0" ||
  activation_height(u64 LE) || epoch_length(u64 LE))` and for the registry
  `source = SHA-256("tachyon-pir/v1/distribution-source\0" || "registry\0" ||
  REGISTRY_CHUNK_ENTRIES(u64 LE))`. A query host refuses a publication from
  any other scope.
- The **inventory** (itself an object, named by the envelope) lists the map
  head, every map chunk, `newest_scope`, and per table
  `{scope_index, session_id, rows, public}` (rows exactly 4,096 × 8,192 bytes,
  public exactly 45,056 bytes), and the authority subject: for the chain
  products the anchor and the last block of each open scope, for the registry
  `log_sequence`. It is at most `MAX_INVENTORY_BYTES = 16 MiB`, which holds
  about 60,000 tables; an archive tier (section 11.5) is the answer when
  history outgrows it. A map chunk is at most `MAX_MAP_CHUNK_BYTES = 8 MiB`.
- An authority check carries at most 256 subjects and
  `MAX_CHECK_BLOCKS = 4,096` blocks in total.
- The publisher keeps the objects of a displaced publication for
  `PUBLISHER_RETENTION_SECONDS = 600` and collects objects nothing retains
  (normative rule 5).
- Distribution schema strings: `tachyon-<product>-prepared-distribution-v1`.
- The five normative rules of `wallet-pir/docs/spec/spec-publisher-ingest-model.md`
  apply as written.

Publication cadence: one publication per ingestion pass that changed
something, coalesced to one active and one pending, as Enhance does.

### 11.4 Reorgs

- A rewind that stays inside open scopes produces a new generation and keeps
  `recovery_epoch`. Sessions of the rebuilt open tables change because their
  content changes; sealed sessions are unchanged.
- A rewind that reaches a sealed scope increments `recovery_epoch`, revokes the
  sessions of every affected table (`410`) and rebuilds them. `withdraw_above`
  does this by applying `AuthorityTransition::Recover` to the
  `PublicationAuthority` (which also withdraws), as Status does after a
  withdrawal. Whether a rewind reaches a sealed scope is judged against the
  last publication in the envelope, eligible or not, so a withdrawal made
  earlier (for example by the canonical guard) still leads to `Recover`. A rewind that stays inside open scopes applies
  `AuthorityTransition::Withdraw` if the current publication's anchor is above
  the kept block. Either transition is durable before the journal rewinds.
- A publisher restart always starts a new recovery epoch, as
  `PublicationAuthority` requires. After the restart the publisher republishes
  its previous publication under the new epoch and a new generation, provided
  its anchor is still canonical. Only the map head is reissued (it carries
  `generation` and `recovery_epoch`); table objects and session ids are
  unchanged, so query hosts prepare nothing.

### 11.5 Retention

Every sealed table costs resident memory on each query host (about 160 MiB by
`wallet-pir`'s measurement), and the testnet seals an epoch about every ten
hours. Unbounded retention is not affordable on one host.

- Each product has `retain_scopes` in query-server configuration. The host
  serves the newest `retain_scopes` scopes. The map head is a content-addressed
  object built by the publisher, whose `floor_scope` is always 0, so a host
  cannot rewrite it. A host that retains fewer scopes reports its floor, as a
  scope index, in an `X-Floor-Scope` header on `GET map`, and answers a session
  below its floor with `409`. The host always sends `X-Floor-Scope`, `0` when
  it retains every scope. A client takes the larger of the two floors.
- A query for a scope below the floor has no session and cannot be formed; the
  wallet learns the floor from the map and reports "below this provider's
  floor" to its caller.
- The publisher keeps every sealed object. Retention is a serving decision.
- Default `retain_scopes`: memo 64, tachygram 64, first-contact unlimited,
  registry unlimited. An absent `retain_scopes` means the product default;
  there is no explicit "unlimited" value, and a configured value is at
  least 1.

An archive tier that serves old scopes from wider, merged tables is future
work, as it is for Receiver.

### 11.6 Query host

`tachyon-pir-query-server` runs one `pir_pipeline::consumer::ConsumerEngine`
per product over `pir_sync_manager::HttpsSource`.

- It checks `/.well-known/pir-service-discovery` before start and before each
  sync pass. Before start, any discovery failure, including an unreachable
  publisher, stops start-up, as in `wallet-pir`; afterwards it exits on a
  confirmed major-version mismatch. The publisher
  serves that document only on its management listener, so the TLS edge in
  front of the publisher routes this path there and every other path to the
  product listener.
- It trusts the publisher's certificate through `publisher_ca` (a PEM file) in
  its configuration, as `HttpsSource` requires.
- It fetches objects, verifies digests, prepares runtimes and only then
  switches to the new publication. A partial publication is never served.
- It keeps the displaced publication for 600 s so in-flight queries finish.
- A restarted host serves the objects already verified in its state directory
  without fetching them again (scenario 12).
- After a failed pass, including a refused object, it retries with
  `pir_pipeline`'s back-off (10 s, doubling, at most 120 s). A refused object
  therefore delays the switch to the next publication by at least 10 s.
- It answers `503` for a product's namespace until that product is ready.
- While the publisher is unreachable it keeps serving what the last successful
  authority check confirmed, for `max_unconfirmed_seconds` (configuration,
  default 600) after that check started, and then answers `503` until a check
  succeeds again. A withdrawal, a new recovery epoch or a refuted subject
  stops serving at once. This is "within its authority rules" in scenario 11.

### 11.7 Configuration

JSON with unknown fields refused, shaped like `wallet-pir`'s examples.

Publisher:

```json
{
  "management_listen": "127.0.0.1:9400",
  "chain": { "rpc_url": "http://tachyscan.cnode.phd:18232", "no_auth": true, "poll_ms": 1000,
             "epoch_length": 4096 },
  "memo":          { "enabled": true, "settings": { "listen": "127.0.0.1:9401",
                     "state_dir": "/var/lib/tachyon-pir/memo" } },
  "first_contact": { "enabled": true, "settings": {} },
  "tachygram":     { "enabled": true, "settings": { "listen": "127.0.0.1:9402",
                     "state_dir": "/var/lib/tachyon-pir/tachygram" } },
  "registry":      { "enabled": true, "settings": { "listen": "127.0.0.1:9403",
                     "state_dir": "/var/lib/tachyon-pir/registry", "difficulty_bits": 20 } }
}
```

- `pir_publisher::run_registered` hosts the products. It requires
  `management_listen` to be a loopback address with a fixed port, every
  product listener to be a private address with a fixed port, one listener per
  product definition, and disjoint state directories between definitions.
- `chain` is required whenever any product is enabled; the registry reads only
  the network identity from it (the genesis hash of its distribution scope,
  section 11.3). `chain.epoch_length` is required and has no default.
- A product that stops is reported `failed` on `/ready` and is not restarted
  in-process (`run_registered`'s policy); the process supervisor restarts the
  publisher.
- `first_contact` shares the memo state directory (`memo-v1.sqlite`) and
  requires `memo` to be enabled. Memo and first-contact therefore run as **one**
  publisher definition on the memo listener, serving both distribution
  prefixes.
- `epoch_length` is read per network (section 15). Tests use a small value so
  that synthetic chains cross epoch boundaries in a few hundred blocks.
- The registry listener serves both the registry distribution prefix and the
  write API (section 9.3).

Query server:

```json
{
  "management_listen": "127.0.0.1:9500",
  "public_listen": "127.0.0.1:9510",
  "memo": { "enabled": true, "settings": {
    "publisher": "https://publisher.example", "state_dir": "/var/lib/tachyon-pir-query/memo",
    "publisher_ca": "/etc/tachyon-pir-query/publisher-ca.pem",
    "role": "combined", "publisher_network": "test", "publisher_genesis": "<64 hex>",
    "retain_scopes": 64 } }
}
```

`publisher_ca` is optional; without it the system roots are used.

- Chain products also set `activation_height` and `epoch_length`. With
  `publisher_genesis` they fix the distribution scope (section 11.3), and every
  map the host accepts must state the same values.
- `admission` (optional) sets the section 10.6 values: `executing` (2),
  `waiting` (8), `wait_ms` (2,000), `per_client` (2) and `upload_ms` (the
  section 10.4 upload deadline, 15,000). `poll_ms` (1,000) sets the sync
  cadence and `max_unconfirmed_seconds` (600) the window of section 11.6.

Both binaries accept `--config FILE` and `--check` (validate offline).

### 11.8 Observability

- Query server management listener: `GET /health`, `GET /ready`,
  `GET /metrics`, with the response shapes of `wallet-pir`.
- Publisher: `run_registered`'s management listener serves `/health`,
  `/ready` and the discovery document, and no metrics. Each product therefore
  serves `GET /metrics` on its own listener.
- HTTP metrics from `pir-observability`, with fixed endpoint labels.
- Product metrics, all prefixed `tachyon_<product>_`:
  `ingested_height`, `anchor_height`, `entries_total`, `open_scope_entries`,
  `segments_overflow_total`, `publication_seconds`, `sessions_revoked_total`.
  `entries_total`, `open_scope_entries` and `segments_overflow_total` are
  gauges describing the current publication (the `_total` names count entries
  and segments in it, not events); `publication_seconds` is a histogram from
  build to commit; `sessions_revoked_total` is a counter of sessions in scopes
  reached by a sealed-scope rewind.
- Memo adds `envelopes_invalid_total` and `state_entries_total`.
- Registry adds `registrations_total{result}` and `pow_rejected_total`.

## 12. Trust and privacy

### 12.1 What the wallet must do

These are assumptions the servers make. They are requirements on a wallet, and
are listed so that the wire protocol can be judged against them.

- Accept `anchor_hash` independently, against its own view of the chain,
  before using any table: for example by comparing `anchor_hash` and
  `genesis_hash` with its node's `getblockhash anchor_height` and
  `getblockhash 0`.
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
  real one. A dummy lookup selects a bucket uniformly and queries every
  segment of it at a uniformly random row, so its requests match a real
  lookup's in number, sessions and sizes.

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
- End-to-end harness: the node double, a TLS edge written in
  `tools/e2e-tests`, the publisher binary and the query-server binary each run
  as separate processes or listeners joined only by real HTTP(S). Tables keep
  the real `tachyon-4k8` geometry (no smaller valid table exists), so a test
  keeps its table count small: each table costs about 1 s to prepare on each
  side. Chains use a small `epoch_length`. `wallet-pir` runs no
  process-spawning test inside a 30 s budget; this workspace does, and its
  tests MUST stay inside it.

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
| `MAX_ENVELOPE_BYTES` | 19,989 | Follows from section 5.1 |
| `MAX_BUCKETS` | 64 | From `wallet-pir` txid |
| `MAX_SEEDS` | 64 | From `wallet-pir` txid |
| `MAX_SEGMENTS` | 8 | This spec's own (matches Receiver's shard-segment limit) |
| `CONFIRMATION_DEPTH` | 2 | From `wallet-pir` Receiver |
| `SEAL_DEPTH` | 100 | Provisional |
| `FC_SPAN_TARGET` | 3,072 | Provisional |
| `FC_SPAN_MAX_EPOCHS` | 64 | Provisional |
| `REGISTRY_CHUNK_ENTRIES` | 3,072 | Provisional (D3) |
| `REGISTRY_CHALLENGE_SECONDS` | 600 | Provisional |
| `MAP_CHUNK_SCOPES` | 32 | From `wallet-pir` txid |
| `MAX_MAP_HEAD_BYTES` | 1 MiB | This spec (section 10.3) |
| `MAX_MAP_CHUNK_BYTES` | 8 MiB | This spec (section 11.3) |
| `MAX_INVENTORY_BYTES` | 16 MiB | This spec (section 11.3) |
| `MAX_CHECK_BLOCKS` | 4,096 | From `wallet-pir` Receiver |
| `PUBLISHER_RETENTION_SECONDS` | 600 | This spec (section 11.3) |
| `max_unconfirmed_seconds` (default) | 600 | Configuration (section 11.6) |
| `GEOMETRY` (published) | `tachyon-4k8` | Version 1 (section 6.1) |

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
- **Memory growth.** At about 160 MiB per table (planning bound) and two chain
  tables per epoch, 64 retained epochs is about 20 GiB per host on testnet
  volumes. Higher throughput multiplies buckets and with them memory.
- **Publisher preparation cost.** The publisher prepares every table it
  publishes (section 6.6), about 1 s per table on a large host. An open epoch
  with many buckets and segments is rebuilt at every publication.
