# registry-publisher

The registry product's publisher (spec sections 6, 9, 10.3, 11.3-11.8,
`docs/spec/spec-tachyon-payment-pir.md`): the write API that admits
registrations into `registry-store`'s append-only log, and the publication of
that log as one scope of tables per 3,072-entry chunk through
`tachyon-pir-distribution`, under `pir-publisher`'s host. It follows
`tachygram-publisher` and `memo-publisher` in structure, but the registry is
not a chain follower: it runs `pir_pipeline`'s `Publisher` loop with
`PublisherEngine::run` (as Status does) instead of `Ingestion`.

Chain and ingestion code (`tools/ci/boundary.json`, registry family): it
depends on `registry-store` and the shared crates, never on another product.

## Registering it (publisher binary)

```rust
// Once at start-up, from the node the chain section names (spec 9.2 Identity).
let identity = RegistryIdentity::new(network.chain, BlockHash::from_protocol_bytes(genesis_protocol_bytes));
let settings: RegistrySettings = /* config.registry.settings */;
let config = RegistryConfig::new(settings, identity);
config.validate()?;
let definition = registry_publisher::configure(config, pir_pipeline::engine::PublisherEngine)?;
pir_publisher::run_registered(management_listen, definitions /* incl. this one */, chain, shutdown).await
```

The definition is named `registry` and does **not** call `following_chain`:
the host gives it no chain feed and it needs none. `configure` validates the
settings, declares the one listener, the state directory and a readiness probe
on `/provider/registry/v1/current` (ready when the envelope is eligible and
names a publication), and opens nothing until the host starts it.

`RegistrySettings` is the `registry.settings` object (spec 11.7, 9.2, 9.3;
unknown fields refused):

| Field | Default | Meaning |
|---|---:|---|
| `listen` | required | private listener: distribution, write API, `/metrics` |
| `state_dir` | required | absolute; disjoint from other products' |
| `difficulty_bits` | 20 | proof-of-work leading zero bits (at most 256) |
| `per_client_concurrent` | 2 | registrations in flight per client |
| `per_client_per_minute` | 6 | registrations per client per sliding minute |
| `global_per_minute` | 600 | write-API requests per sliding minute in total |
| `upload_ms` | 15,000 | registration body deadline |
| `publish_ms` | 1,000 | least time between publication starts |

State under `state_dir`: `registry-v1.sqlite` (the log, `registry-store`),
`challenge-secret` (32 random bytes, mode 0600, created at first start),
`distribution/registry/` (authority, generation counter, objects) and
`publisher.lock`.

## Public API

```rust
pub struct RegistrySettings { pub listen, pub state_dir, pub difficulty_bits: u32, pub per_client_concurrent: usize,
                              pub per_client_per_minute: u32, pub global_per_minute: u32, pub upload_ms: u64, pub publish_ms: u64 }
pub struct RegistryIdentity { pub chain: String, pub genesis_hash: BlockHash /* protocol order */ }
pub struct RegistryConfig { pub settings, pub identity, pub retention: Duration }
impl RegistryConfig { pub fn new(settings, identity) -> Self; pub fn validate(&self) -> Result<(), String> }

pub fn configure<E: PublisherExecutor>(config, executor: E) -> pir_pipeline::publisher::Result<PublisherDefinition<axum::Router>>;
pub async fn run<E: PublisherExecutor>(config, http: Http<Router>, stop: Shutdown, executor: E) -> pir_pipeline::publisher::Result<()>;
pub fn distribution_ready(body: &[u8]) -> bool;
pub const READINESS_PATH: &str = "/provider/registry/v1/current";
pub const MOUNT: PublicMount;   // /registry/v1 and /v1/registry

pub struct Registry;
impl Registry {
    pub fn open(config: &RegistryConfig, shutdown: Shutdown) -> Result<Self>;
    pub fn router(&self) -> axum::Router;      // serve with into_make_service_with_connect_info for the peer fallback
    pub fn publisher(&mut self) -> &mut PublicationTask;
    pub fn take_publisher(&mut self) -> Option<PublicationTask>;
    pub fn distribution(&self) -> &Distribution;
    pub fn store(&self) -> &registry_store::Store;
    pub fn metrics(&self) -> Arc<Metrics>;
    pub fn tables_built(&self) -> u64;
}
pub struct PublicationTask;    // impl pir_pipeline::publisher::Publisher
impl PublicationTask { pub async fn publish_pending<E: PublisherExecutor>(&mut self, &E) -> Result<Option<Report>> }

pub mod validate { pk_canonical, ek_modulus_check, ek_coefficients, check_address, AddressRefusal, PALLAS_BASE_MODULUS_LE }
pub mod challenge { ChallengeKey { load_or_create, from_secret, challenge, issue, verify }, ChallengeRefusal, now_unix }
pub mod limits { RateLimits, Limited, WINDOW }
```

## Write API (spec 9.3)

`GET /registry/v1/challenge` (and `/v1/registry/challenge`) returns
`{"challenge", "difficulty_bits", "expires_unix"}`, the challenge being
`expires_unix (u64 LE) || HMAC-SHA256(secret, "tachyon-pir/registry/v1/challenge\0" || expires_unix LE)[0..24]`
with `expires_unix = now + 600`. It is stateless: verification recomputes the
tag from the body and compares it in constant time (`hmac`'s
`verify_truncated_left`); one challenge serves any number of registrations
until it expires, and survives a restart because the secret does.

`POST /registry/v1/register` takes `challenge(32) || nonce(8) || addr(1216)`:

| Step | Refusal | Status | `result` label |
|---|---|---|---|
| client already has `per_client_concurrent` in flight | `overloaded`, `Retry-After: 1` | 429 | `overloaded` |
| global window full | `overloaded`, `Retry-After: n` | 429 | `rate_limited` |
| client window full | `overloaded`, `Retry-After: n` | 429 | `rate_limited` |
| body not complete within `upload_ms` | `upload_timeout` | 408 | `upload_timeout` |
| body longer than 1,256 bytes | `invalid_request` | 413 | `too_large` |
| body shorter (or unreadable) | `invalid_request` "registration body must be exactly 1256 bytes" | 400 | `invalid` |
| challenge tag wrong | "challenge is not authentic" | 400 | `invalid` |
| challenge past `expires_unix` | "challenge has expired" | 400 | `invalid` |
| proof of work below `difficulty_bits` | "proof of work does not meet N bits" (+ `pow_rejected_total`) | 400 | `pow_rejected` |
| `pk` ≥ p | "pk is not a canonical Pallas base field element" | 400 | `invalid` |
| an `ek` coefficient ≥ 3,329 | "ek fails the ML-KEM modulus check" | 400 | `invalid` |
| `key16` all zero | "key16 is all zero" | 400 | `invalid` |
| the log cannot be written | `temporarily_unavailable`, `Retry-After: 1` | 503 | `unavailable` |
| appended or already present | `{chunk_id, digest, created, visible_after_sequence}` | 200 | `created` / `existing` |

- The concurrency slot is taken first and held through the body upload and
  the durable append (it moves into the blocking task, so a client that
  disconnects keeps it until the append ends). Only then is the request
  admitted against the global and per-client windows; an admitted request
  counts even if its body is later refused, and a refused one never counts.
  `Retry-After` is the ceiling of the seconds until the window admits again,
  at least 1.
- The challenge route is admitted against (and counted in) the global window
  only.
- The client key is `pir_control::admission::client_key`: the first
  `X-Forwarded-For` address, else `X-Real-IP`, else the peer address when the
  server provides `ConnectInfo`, else the single key `unknown`.
- The `200` is sent only after `Store::register` returned (WAL commit with
  `synchronous=FULL`). The registration then wakes the publication loop.
- Every response carries `Cache-Control: no-store`; unknown routes answer 404
  and wrong methods 405, both with an `invalid_request` body. Nothing a client
  sends is logged or used as a metric label.

## Publication (spec 9.2, 10.3, 11.3, 11.4)

- **Cadence.** `PublicationTask` implements `Publisher`. `acquire` (after
  restoring the previous process's publication once) reads `log_sequence` and
  offers new material when it differs from the last publication's, the
  envelope is not eligible, or the last attempt failed. `wait` sleeps until a
  registration grows the log past the last publication, then until
  `publish_ms` after the previous start (at least 1 s after a failure).
  Registrations during a publication only advance the sequence the next
  `acquire` reads: one active, one pending. Nothing waits for a consumer.
- **Snapshot.** A publication pins `log_sequence` first and reads every chunk
  with `Store::chunk_before(chunk, log_sequence)`. Chunks `0 .. log_sequence /
  3072` are sealed with exactly 3,072 entries; the open chunk `log_sequence /
  3072` holds the rest and may be empty (no tables). `scope_id` = scope index
  = `chunk_id`; `n_buckets = 1`; entries in sequence order.
- **Reuse.** Sealed chunks are built once and kept by chunk id; the open chunk
  is kept by `(chunk_id, entries, entry digest)`. After a restart the
  previous publication's chunks are taken back, each checked by rebuilding
  its rows from the log (cheap; no PIR preparation) against the published
  `rows_sha256`, so neither the restored publication nor the next one
  prepares them again.
- **Phases.** `build`: the snapshot, `build_scope` and `describe` for each
  chunk to build. `store`: rows and public setup objects. `verify`:
  `validate_scope` (every row through `validate_row`), entry counts, one
  bucket, digests and session ids against the bytes, installed objects
  against the map, and `RegistryMapHead::check_listing`. `assemble`:
  generation, map chunks of 32 sealed chunks, the head (fields in spec order:
  protocol, profile, network, genesis_hash, generation, recovery_epoch,
  log_sequence, floor_scope 0, sealed_chunks, open_scopes), the inventory with
  `Subject::Registry { log_sequence }`. `recheck`: the log still holds at
  least `log_sequence` entries. `commit`: `Distribution::commit`.
- **Hooks.** `check_subjects` answers a `{log_sequence}` subject true while
  the log holds at least that many entries (it is append-only);
  `reissue_head` rewrites `generation` and `recovery_epoch` only.
- **Restart.** `Distribution::open` starts a new recovery epoch; the first
  `acquire` restores the previous publication under it and a new generation.
  The open chunk is rebuilt only if the log grew.

## Metrics (spec 11.8)

`GET /metrics`: `tachyon_registry_entries_total`, `open_scope_entries`,
`segments_overflow_total` (gauges of the current publication),
`publication_seconds` (histogram), `sessions_revoked_total` (counter, always
0: the registry never rewinds), `registrations_total{result}` with the fixed
labels `created`, `existing`, `invalid`, `pow_rejected`, `too_large`,
`upload_timeout`, `rate_limited`, `overloaded`, `unavailable`, and
`pow_rejected_total`; plus `pir-observability` HTTP metrics with the
endpoint labels `current`, `objects`, `authority_check`, `metrics`,
`challenge`, `register`.

## Tests

`cargo test --locked -p registry-publisher --profile release-fast`. A low
difficulty (6 bits) keeps proof of work instant; bulk logs are seeded with
`Store::register_many` and only boundary entries go over HTTP.

- Unit: settings and validation; `pk` boundaries around `p`; `ByteDecode12`
  layout and `q - 1`/`q` in both halves of a 3-byte group; the check order;
  challenge bytes against an RFC 2104 HMAC built from `sha2`, expiry, every
  altered byte refused, the secret's mode and reuse; sliding windows and
  `Retry-After`; metrics rendering.
- `tests/address_vectors.rs`: known answers from
  `tests/vectors/address_checks.py` (independent: integer comparison with `p`;
  FIPS 203 `ByteDecode12` bit by bit and the 7.2 re-encoding check), checked in
  as `address_checks.json`.
- `tests/write_api.rs`: challenge format, legacy mount, restart; every
  refusal with status, code and message, and the validation order; 408; 
  idempotence; per-client concurrency (two held uploads) and rate with
  `Retry-After`, other clients unaffected; accepted-then-invalid requests
  count; the global cap including the challenge route; the peer-address
  fallback. A zero `key16` cannot be ground (it needs a 128-bit digest
  prefix), so that check is covered through `validate::check_address`.
- `tests/publication.rs`: the empty log's exact head bytes; publications
  advancing with no consumer; entries found through `query_plan` and
  `validate_row` in the first map past their sequence; coalescing and
  `publish_ms`; sealing at exactly 3,072 with a new empty open chunk; the
  sealed chunk built once; restart (new recovery epoch, restored with no
  table built, sessions unchanged, then only the open chunk rebuilt);
  identical sealed tables from a fresh state directory.
- `tests/overflow.rs`: 448 valid addresses ground through `ρ` so every seed
  overflows row 0; seed 0 with overflow segments; every address found once.
- `tests/durability.rs`: the registry in a child process killed with SIGKILL
  right after the last `200`; every acknowledged entry is in the log and the
  next publication.
- `tests/host.rs`: `configure` under `run_registered` with no chain:
  readiness, writes at both mounts, publications advancing,
  `authority/check` confirming and refuting, `/metrics`, clean shutdown.
