# tachyon-pir-table

The shared bucket-row table of the Tachyon payment PIR services, for all four
products: building a scope's tables (bucket count, seed search, overflow
segments, canonical rows), validating them, their digests, and the P8 table
runtime that answers queries. Spec sections 6, 10.3, 10.4 and 13.1
(`docs/spec/spec-tachyon-payment-pir.md`); the runtime follows
`docs/shared/wallet-pir-integration.md` section 1.

A serving library (`tools/ci/boundary.json`): no chain, ingestion, HTTP or
async dependencies. Placement hashing, row encoding, row validation, PIR
parameters, session ids and map types come from `tachyon-pir-protocol`; this
crate never reimplements them. The PIR engine is `zakura-pir-server-core`
(hint, preprocessing, publication), `zakura-pir-query-server` (upload parsing,
reply packing) and `reinspiring` (scan kernel). `zakura-pir-client` is a
dev-dependency only, for round-trip tests.

## API

```rust
// build: spec 6.2-6.4
pub fn build_scope(product: Product, genesis: &BlockHash, scope_id: u64, entries: &[Entry])
    -> Result<ScopeTables, Error>;                 // tachyon-4k8, current rayon pool
pub struct ScopeTables { pub n_buckets: u32, pub scope_seed: u32, pub tables: Vec<BuiltTable> }
impl ScopeTables {
    pub fn entries(&self) -> u64;
    pub fn segments(&self, bucket: u32) -> &[BuiltTable];
    pub fn row_context(&self, product, genesis, scope_id, table: &BuiltTable,
                       heights: Option<(u32, u32)>) -> RowContext;   // row 0: a template
}
pub struct BuiltTable { pub bucket: u32, pub segment: u32, pub geometry: Geometry, pub seed: u32,
                        pub entries: u32, pub rows: Vec<u8> /* rows x 8192 */, pub rows_sha256: [u8; 32] }
impl BuiltTable {
    pub fn row(&self, row: u32) -> &[u8];
    pub fn map_table(&self, product, scope_id, public_sha256: [u8; 32]) -> Table;  // map entry + session id
}

// validate: every row through tachyon_pir_protocol::validate_row
pub fn validate_table(product, template: &RowContext, rows: &[u8]) -> Result<u64, Error>;
pub fn validate_scope(product, genesis, scope_id, heights: Option<(u32, u32)>, &ScopeTables)
    -> Result<u64, Error>;                        // publisher verify phase
pub fn validate_published<S: map::ScopeTables>(product, genesis, scope: &S, table: &Table, rows: &[u8])
    -> Result<u64, Error>;                        // query host, against the map

// digests
pub fn rows_sha256(rows: &[u8]) -> [u8; 32];
pub fn public_sha256(public: &[u8]) -> [u8; 32];

// runtime: spec 6.6
impl TableRuntime {   // Send + Sync
    pub fn build(params: &PirParams, rows: &[u8]) -> Result<TableRuntime, Error>;
    pub fn build_in(pool: &rayon::ThreadPool, params: &PirParams, rows: &[u8]) -> Result<TableRuntime, Error>;
    pub fn public(&self) -> &[u8];               // 45,056 bytes
    pub fn public_sha256(&self) -> [u8; 32];
    pub fn answer(&self, params: &PirParams, payload: &[u8]) -> Result<Vec<u8>, Error>;  // 28,160 in, 10,240 out
    pub fn held_bytes(&self) -> u64;             // txid's TableRuntime::held_bytes formula
    pub fn product(&self) -> Product; pub fn geometry(&self) -> Geometry;
}

// preparation
pub struct PreparedTable { pub runtime: TableRuntime, pub public: Vec<u8>, pub public_sha256: [u8; 32],
                           pub session_id: [u8; 32], pub table: Table }
pub struct DescribedTable { pub table: Table, pub public: Vec<u8> }
pub fn prepare(product, scope_id, &BuiltTable) -> Result<PreparedTable, Error>;
pub fn describe(product, scope_id, &BuiltTable) -> Result<DescribedTable, Error>;   // runtime dropped
pub fn prepare_published(product, scope_id, &Table, rows: &[u8]) -> Result<PreparedTable, Error>;
// ..._in(pool, ...) variants build inside a given rayon pool.
```

`Error` is one enum; `Error::Query` is a payload the profile refuses (a host
answers `400`), `Error::TooManySegments` fails the publication (spec 6.4).

Note the name: this crate's `ScopeTables` struct is the built tables of a
scope; `tachyon_pir_protocol::ScopeTables` is the map trait. Import one of
them under another name when both are needed.

## Build rules (spec 6.3, 6.4)

- `n_buckets = n_buckets_for(product, tachyon-4k8, entries)`.
- Scope seed: the smallest `s < MAX_SEEDS` under which, with bucket and row
  both from `h(s)`, no row of any bucket holds more than `SLOTS` entries.
  Every bucket then has one segment with that seed.
- Otherwise the scope seed is 0, buckets come from `h(0)`, and each bucket's
  segment 0 is filled under seed 0 in ordinal order; an entry whose row is
  full spills to the next segment. Segment `k >= 1` takes the smallest seed
  under which all its entries fit, or is filled under seed 0 and spills again.
  More than `MAX_SEGMENTS = 8` segments is `Error::TooManySegments`.
- Within a row, entries are sorted stably by key, so `(key, ordinal)`.
- An empty scope has `n_buckets = 1`, `scope_seed = 0` and no tables; in a
  non-empty scope every bucket has a segment-0 table, possibly empty.
- Inputs are refused if they are another product's, have an all-zero key, or
  (tachygram) repeat a key.

Each entry's placement hash is computed once per seed tried, in parallel on
the current rayon pool (wrap a call in `ThreadPool::install` to choose one).

## Performance (Ryzen 9 9950X3D, 32 threads, `release-fast`)

| | 32 threads | 1 thread |
|---|---:|---:|
| `build_scope`, 507,904 tachygrams (one 4k8 table) | 0.07-0.11 s | 0.25 s |
| `validate_scope` of that table | 0.02 s | 0.07 s |
| `TableRuntime::build` (4k8) | 0.82 s | 4.6 s |
| `answer` | 3.1 ms | 4.3 ms |

`held_bytes` of a built 4k8 runtime: 167,948,464 bytes (160.17 MiB).

## Tests

`cargo test -p tachyon-pir-table --profile release-fast`.

- `tests/build.rs`: empty scope; a light load under seed 0; a four-bucket
  scope with duplicate keys; a ground overflow that picks a later scope seed;
  forced overflow into segments with a nonzero segment seed and the spill
  order property; many duplicates through four segments; `MAX_SEGMENTS`
  reached and exceeded; an empty bucket's zero-entry table; determinism across
  rebuilds, batching and pool size, and dependence on genesis and scope id;
  `(key, ordinal)` order in a row; tampered tables; a full 507,904-entry
  tachygram scope. Each layout is compared with a reference that applies spec
  6.4 literally, one key at a time, and every inserted key is looked up
  through the map's `query_plan` and `validate_row`, against the expected
  entries taken from the input list.
- `tests/runtime.rs`: four runtime builds in all, serialised by a lock. Client
  queries from `zakura-pir-client` for a full row, an empty row and others
  decode to the exact row bytes; sizes (payload 28,160, reply 10,240, public
  45,056); wrong payload lengths and parameters are refused; the public setup
  is identical across two builds of the same rows and changes when one row
  byte changes; `prepare_published` refuses wrong digests and session ids.
