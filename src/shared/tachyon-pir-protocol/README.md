# tachyon-pir-protocol

The wallet-facing contract of the Tachyon payment PIR services, for all four
products (memo, first-contact, tachygram, registry): constants, geometries and
exact message sizes, PIR seeds and parameters, placement hashing, session ids,
query framing, error bodies, entry layouts and the canonical row codec, map
types with their client-side checks, and the registry wire types. Spec
sections 3, 6, 7, 8.1, 8.4, 9.1-9.3, 10, 12.1 and 15
(`docs/spec/spec-tachyon-payment-pir.md`).

A serving library (`tools/ci/boundary.json`): no chain, ingestion, HTTP, async
or server dependencies. It depends only on `zakura-pir-protocol` (feature
`native`, for P8), `serde`, `serde_json`, `sha2`, `hex` and `thiserror`, and
defines its own 32-byte types instead of using `tachyon-chain`.

Not here: the on-chain envelope parser (milestone M2), table building, seed
search and segmenting (`tachyon-pir-table`), and the registry's `pk`/`ek`
checks and HMAC challenges (publisher side, M3).

## API

```rust
// product: per-product wire constants
pub enum Product { Memo, FirstContact, Tachygram, Registry }
impl Product {
    pub const ALL: [Product; 4];
    pub const fn name(self) -> &'static str;                 // "memo", "first-contact", ...
    pub const fn protocol(self) -> &'static str;             // "tachyon-memo-pir-v1", ...
    pub const fn placement_domain(self) -> &'static [u8];    // b"tachyon-pir/memo/v1/place\0", ...
    pub const fn query_magic(self) -> [u8; 4];               // TMQ1 TFQ1 TGQ1 TRQ1
    pub const fn mount(self) -> &'static str;                // "/memo/v1"
    pub const fn legacy_mount(self) -> &'static str;         // "/v1/memo"
    pub const fn distribution_prefix(self) -> &'static str;  // "/provider/memo/v1/"
    pub const fn distribution_schema(self) -> &'static str;  // "tachyon-memo-prepared-distribution-v1"
    pub const fn key_len(self) -> usize;                     // 16, or 32 for tachygram
    pub const fn entry_bytes(self) -> usize;                 // 144, 1232, 40, 1232
    pub const fn slots(self) -> usize;                       // 56, 6, 204, 6
    pub const fn target_load(self) -> u64;                   // 34, 1, 124, 1
    pub const fn is_chain(self) -> bool;
    pub const fn index_is_scope_id(self) -> bool;            // false for first-contact
    pub fn from_protocol(protocol: &str) -> Option<Product>;
    pub fn key_of(self, value: &[u8; 32]) -> &[u8];          // key16 or the full tachygram
}   // Display / FromStr by name

// geometry: tachyon-4k8 (published) and tachyon-2k8 (defined only)
pub enum Geometry { Tachyon4k8, Tachyon2k8 }
impl Geometry {
    pub const fn name(self) -> &'static str;
    pub const fn rows(self) -> u32;
    pub const fn table_bytes(self) -> usize;
    pub const fn payload_len(self) -> usize;   // 28,160 / 20,992
    pub const fn request_len(self) -> usize;   // 28,212 / 21,044
    pub const fn reply_len(self) -> usize;     // 10,240
    pub const fn response_len(self) -> usize;  // 10,292
    pub const fn public_len(self) -> usize;    // 45,056
    pub fn from_name(name: &str) -> Option<Geometry>;
}   // Display / FromStr / serde as the name; any other name is refused

// params: spec 6.6
pub enum SeedPurpose { QueryMasks, PackingSetup }
pub fn seed(product: Product, geometry: Geometry, purpose: SeedPurpose) -> [u8; 32];
impl PirParams {
    pub fn new(product: Product, geometry: Geometry) -> PirParams;
    pub fn masks(&self) -> &[Vec<u64>];
    pub fn setup(&self) -> &NativeSetup;
    pub fn rows(&self) -> usize; pub fn cols(&self) -> usize;
    pub fn payload_len(&self) -> usize; pub fn reply_len(&self) -> usize; pub fn public_len(&self) -> usize;
}

// placement: spec 6.3
pub fn placement_hash(product, genesis: &BlockHash, scope_id: u64, seed: u32, key: &[u8]) -> Result<[u8; 32], PlacementError>;
pub fn bucket(product, genesis, scope_id, scope_seed: u32, n_buckets: u32, key) -> Result<u32, PlacementError>;
pub fn row(product, genesis, scope_id, seed: u32, rows: u32, key) -> Result<u32, PlacementError>;
pub const fn n_buckets_for(product: Product, geometry: Geometry, entries: u64) -> u32;
pub const fn valid_n_buckets(n: u32) -> bool;

// session: spec 10.4
pub struct SessionParts { scope_id, bucket, segment, rows, seed, rows_sha256, public_sha256 }
pub fn session_id(product: Product, parts: &SessionParts) -> [u8; 32];

// query: spec 10.4 framing
pub struct QueryHeader { pub magic: [u8; 4], pub session_id: [u8; 32], pub nonce: [u8; 16] }
impl QueryHeader { new(product, session_id, nonce); encode() -> [u8; 52]; parse(&[u8]) -> Result<Self, FrameError> }
pub fn encode_request(header, payload) -> Vec<u8>;
pub fn parse_request(product, body) -> Result<(QueryHeader, &[u8]), FrameError>;  // limit, header, magic
pub fn check_request_len(geometry, body_len) -> Result<(), FrameError>;           // exactly 52 + payload_len
pub fn validate_request(product, geometry, body) -> Result<(QueryHeader, &[u8]), FrameError>;
pub fn encode_response(header, reply) -> Vec<u8>;
pub fn validate_response(request: &QueryHeader, geometry, body) -> Result<&[u8], FrameError>;
impl FrameError { pub fn status(&self) -> ErrorStatus }  // 413 or 400

// error_body: spec 10.5
pub enum ErrorCode { InvalidRequest, UploadTimeout, StaleRouting, SessionUnavailable, Overloaded, TemporarilyUnavailable }
pub enum ErrorStatus { BadRequest, UploadTimeout, StaleRouting, SessionUnavailable, PayloadTooLarge, Overloaded, Unavailable }
impl ErrorStatus { http_status(); code(); retry_after_seconds(); from_http_status(u16) }
pub struct ErrorBody { pub code, pub message, pub map_sha256: Option<Bytes32> }  // validate(): map_sha256 iff 409

// entry and row: spec 6.2, 8.1, 8.4
pub enum Entry { Memo(MemoEntry), FirstContact(Box<FirstContactEntry>), Tachygram(TachygramEntry), Registry(Box<RegistryEntry>) }
impl Entry { product(); key(); encode_into(&mut [u8]); to_bytes(); decode(product, &[u8]) -> Option<Entry> }
pub struct RowContext { product, genesis, scope_id, n_buckets, scope_seed, bucket, seed, rows, row, heights: Option<(u32, u32)> }
pub fn encode_row(product, entries: &[Entry]) -> Result<Vec<u8>, RowError>;
pub fn validate_row(product, row: &[u8], context: &RowContext) -> Result<Vec<Entry>, RowError>;
pub fn lookup<'a>(entries: &'a [Entry], key: &[u8]) -> Vec<&'a Entry>;

// map: spec 10.3
pub struct ChainMapHead { protocol, profile, network, genesis_hash, activation_height, epoch_length, generation,
                          recovery_epoch, anchor_height, anchor_hash, floor_scope, sealed_chunks, open_scopes: Vec<Scope> }
pub struct RegistryMapHead { protocol, profile, network, genesis_hash, generation, recovery_epoch, log_sequence,
                             floor_scope, sealed_chunks, open_scopes: Vec<RegistryScope> }
pub struct Scope { scope_id, state, first_height, last_height, entries, n_buckets, tables: Vec<Table> }
pub struct RegistryScope { scope_id, state, entries, n_buckets, tables: Vec<Table> }
pub struct Table { bucket, segment, geometry, seed, entries, rows_sha256, public_sha256, session_id }
pub struct SealedChunkRef { base, sha256 }
pub struct MapChunk<S> { protocol, base, scopes: Vec<S> }   // ChainMapChunk, RegistryMapChunk
impl ChainMapHead {
    parse(&[u8]) -> Result<Self, MapError>;  to_bytes() -> Result<Vec<u8>, MapError>;  // 1 MiB limit
    validate(product);  check_chunk(product, position, &chunk);  parse_chunk(product, position, bytes);
    check_listing(product, &chunks);  product();  open_scope(scope_id);
}
impl RegistryMapHead { parse; to_bytes; validate(); check_chunk; parse_chunk; check_listing; open_chunk() }
pub trait ScopeTables {   // Scope and RegistryScope
    scope_seed(); segments(bucket); check_tables(product);
    row_context(product, genesis, table, row) -> RowContext;
    query_plan(product, genesis, key) -> Result<Vec<QueryTarget>, PlacementError>;
}
pub struct QueryTarget<'a> { pub table: &'a Table, pub row: u32, pub context: RowContext }
pub fn map_sha256(bytes: &[u8]) -> Bytes32;

// registry: spec 9.1-9.3
pub struct Address { pub pk: [u8; 32], pub ek: [u8; 1184] }   // from_bytes, to_bytes, digest, key16
pub struct Challenge { challenge: Bytes32, difficulty_bits: u32, expires_unix: u64 }
pub struct Registration { chunk_id: u64, digest: Bytes32, created: bool, visible_after_sequence: u64 }
pub fn pow_hash(challenge: &[u8; 32], nonce: &[u8; 8], digest: &[u8; 32]) -> [u8; 32];
pub fn pow_meets(hash: &[u8; 32], difficulty_bits: u32) -> bool;
pub fn leading_zero_bits(hash: &[u8; 32]) -> u32;
pub fn solve_pow(challenge, digest, difficulty_bits) -> Option<[u8; 8]>;
pub struct RegisterRequest { challenge, nonce, address }       // parse (exactly 1,256 bytes), encode, pow_hash

// bytes: spec 3
pub struct BlockHash;      // protocol order inside; display-order hex in JSON and Display
pub struct Bytes32;        // plain hex
pub struct RecoveryEpoch;  // positive u64 written as a canonical decimal string
```

All constants of spec section 15 are in `constants`, with `ROW_BYTES`,
`HEADER_BYTES = 52`, `MAX_QUERY_BODY_BYTES = 512 KiB`, `MAX_MAP_HEAD_BYTES =
1 MiB`, `PROFILE`, `ADDR_BYTES = 1216` and `REGISTER_BODY_BYTES = 1256`.

## How a client uses it

1. Fetch `map`, check `X-Map-Sha256` against `map_sha256(bytes)`, parse it
   (`ChainMapHead::parse`), compare `network` and `genesis_hash` with its own,
   accept `anchor_hash` independently (spec 12.1), and `validate(product)`.
2. For a sealed scope, fetch its chunk and `parse_chunk(product, position, bytes)`.
3. `scope.query_plan(product, &head.genesis_hash, key)` gives every
   (table, row) to query: the bucket from the scope seed, then each segment of
   that bucket with its own row. Empty means the scope has no tables and the
   key is absent.
4. Derive `PirParams::new(product, geometry)` once, build each query with
   `zakura-pir-client`, frame it with `QueryHeader` and `encode_request`, and
   check the answer with `validate_response`.
5. `validate_row(product, &row, &target.context)` on every decoded row, then
   `lookup(&entries, key)`; concatenating per-segment results in segment order
   gives ordinal order.

## Rules the checks enforce

- Rows (spec 6.2, 8.4): exactly 8,192 bytes; occupied entries first, then
  all-zero slots and a zero tail; nonzero keys; keys non-decreasing (strictly
  increasing for tachygram); every entry's bucket under the scope seed and row
  under the segment seed equal to the table's bucket and the queried row;
  tachygram heights inside the scope; registry keys equal to the address
  digest prefix.
- Tables (spec 6.3, 6.4): `n_buckets` a power of two up to 64 (1 for
  registry); listed in ascending `(bucket, segment)` order; every bucket has
  segments `0..k`, `1 <= k <= 8`; segment-0 seeds equal (the scope seed); a
  nonzero scope seed means one segment per bucket; seeds below 64; overflow
  segments nonempty; entries within slot capacity and summing to the scope's;
  geometry `tachyon-4k8`; session ids recompute.
- Listing (spec 10.3): every scope from index 0 to the newest listed once,
  sealed in chunks of 32 at bases `32k`, open in `open_scopes`, ascending;
  heights contiguous from the activation height, each scope starting on its
  epoch boundary, sealed and older open scopes ending on one, the newest
  ending at the anchor; memo and tachygram scopes one epoch, first-contact
  spans 1 to 64 epochs; an empty scope has one bucket and no tables; the
  registry has exactly one open chunk, `log_sequence / 3072` with
  `log_sequence % 3072` entries, and full sealed chunks.

## Tests

`cargo test -p tachyon-pir-protocol --profile release-fast`. Unit tests sit
beside each module; `tests/map.rs` covers the map rules and the query plan;
`tests/vectors.rs` checks seeds, placement, `n_buckets`, session ids, one
canonical row per product and the registry digest, proof of work and body
against `tests/vectors/vectors.json`, which `tests/vectors/generate.py`
computes independently with Python's hashlib (`python3 -I
tests/vectors/generate.py` regenerates it; the output is deterministic).
