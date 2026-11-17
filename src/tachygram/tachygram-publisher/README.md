# tachygram-publisher

The tachygram product's publisher (spec sections 6, 8, 10.3 and 11,
`docs/spec/spec-tachyon-payment-pir.md`): it follows the chain into
`tachygram-store`, builds one scope of tables per epoch with
`tachyon-pir-table`, and publishes them through `tachyon-pir-distribution`
under `pir-publisher`'s host. It mirrors `wallet-pir`'s Receiver indexer
(`receiver-indexer/src/managed.rs`, `distribution.rs`) and Enhance's coalescing
publication task (`enhance-publisher/src/application.rs`).

Chain and ingestion code (`tools/ci/boundary.json`, tachygram family): it
depends on `tachyon-chain` and `tachygram-store`, never on another product.

## Registering it (publisher binary)

```rust
let settings: TachygramSettings = /* config.tachygram.settings */;
let config = TachygramConfig::new(settings, config.chain.epoch_length);
let chain = ChainSettings { rpc_url, rpc_cookie, poll };
let definitions = vec![tachygram_publisher::configure(config, Some(&chain), PublisherEngine)?];
pir_publisher::run_registered(management_listen, definitions, Some(chain), shutdown).await
```

`TachygramSettings` is the `tachygram.settings` object of spec 11.7
(`listen`, `state_dir`; unknown fields refused). `configure` validates an
absolute `state_dir`, a fixed port and a nonzero epoch length, declares the
listener, the state directory and a readiness probe on
`/provider/tachygram/v1/current` (ready when the envelope is eligible and
names a publication), and refuses to register without chain settings. It
opens nothing until the host starts it.

## Public API

```rust
pub struct TachygramSettings { pub listen: SocketAddr, pub state_dir: PathBuf }
pub struct TachygramConfig { pub listen, pub state_dir, pub epoch_length: u32,
                             pub decode: DecodeOptions, pub retention: Duration }
impl TachygramConfig { pub fn new(settings, epoch_length) -> Self; pub fn validate(&self) -> Result<(), String> }

pub fn configure<E: PublisherExecutor + IngestionExecutor>(config, chain: Option<&ChainSettings>, executor: E)
    -> pir_pipeline::publisher::Result<PublisherDefinition<axum::Router>>;
pub async fn run<E>(config, chain: ChainSettings, feed: ChainFeed, http: Http<Router>, stop: Shutdown, executor: E)
    -> pir_pipeline::publisher::Result<()>;
pub fn distribution_ready(body: &[u8]) -> bool;
pub const READINESS_PATH: &str = "/provider/tachygram/v1/current";

pub struct Tachygram { pub writer: Writer, pub publisher: PublicationTask, .. }
impl Tachygram {
    pub async fn open(config: &TachygramConfig, node: NodeClient, shutdown: Shutdown) -> Result<Self>;
    pub fn open_with(config, node, identity: NetworkIdentity, shutdown) -> Result<Self>;
    pub fn router(&self) -> axum::Router;          // distribution prefix + GET /metrics
    pub fn guard(&self, feed: ChainFeed, stop: Shutdown) -> impl Future<Output = Result<()>>;
    pub fn distribution(&self) -> &Distribution;
    pub fn metrics(&self) -> Arc<Metrics>;
    pub fn tables_built(&self) -> u64;
}
pub struct Writer;            // impl pir_pipeline::ingestion::Ingestion
pub struct PublicationTask;   // impl pir_pipeline::publisher::Publisher
impl PublicationTask { pub async fn publish_pending<E: PublisherExecutor>(&mut self, &E) -> Result<Option<Report>> }
pub enum Error { Integrity(IntegrityFault), Network(String), Other(BoxError) }  // policy(): Stop, Stop, NextPoll
```

State under `state_dir`: `tachygram-v1.sqlite` (the journal),
`distribution/tachygram/` (authority, generation counter, objects) and
`publisher.lock`.

## Ingestion (spec 11.1, 11.2)

`Writer` implements `Ingestion` over the journal and a `tachyon-chain`
`NodeClient` built from the root chain settings (cookie file reread per
request):

| Method | Does |
|---|---|
| `target_height(tip)` | `tip - CONFIRMATION_DEPTH` (2) |
| `start_height` | the `NuTachyon` activation height: every journal starts there (spec 7.3) |
| `observed` | the observation's genesis and the node's `chain`, genesis and activation must equal the journal's binding; otherwise withdraw and stop. The first pass restores the previous process's publication |
| `committed`, `checkpoint_at_or_below` | the journal's; a failed checkpoint read never empties the journal (`withdraw_above` reads again and refuses) |
| `withdraw_above(keep)` | under the rewind gate: `Recover` if the rewind reaches a sealed scope of the last publication (counting its sessions in those scopes as revoked), else `Withdraw` if that publication's anchor is above `keep`; durable before `rewind`. Drops the pending snapshot |
| `rewind(keep)` | the journal's |
| `ingest(from, through)` | resolves each height to a hash once, fetches the block by hash (8 in flight), decodes, extracts with `IndexedBlock::new` and appends in order |
| `caught_up` | offers the journal at its tip for publication (below) |
| `failed` | integrity and network faults `Stop` (logged as `ALERT`), everything else `NextPoll` |

`pir_pipeline`'s display-order string hashes and `u64` heights become
protocol-order `Hash`es and `u32` heights at this boundary.

A duplicate or all-zero tachygram (spec 8.3, 6.2) is refused by the journal,
so the faulty block is never written; ingestion stops and nothing containing
the block is ever offered for publication.

## Publication (spec 6.4, 6.5, 10.3, 11.3, 11.4)

`caught_up` takes a **snapshot** of the journal (one writer operation) and puts
it in a one-slot watch channel, then returns: ingestion never waits for
preparation, and nothing waits for a consumer. `PublicationTask` (run with
`PublisherExecutor::run`) publishes the latest snapshot; snapshots offered
while one is being published replace each other. That is one active and one
pending publication. A pass that changed nothing offers nothing; a failed
attempt is offered again on the next pass.

- **Scopes.** One per epoch, `scope_id` = scope index = epoch, from 0 to the
  anchor's epoch, each listed once. A scope is sealed when its last block is
  at least `SEAL_DEPTH` (100) blocks below the anchor. The newest scope ends at
  the anchor, every older one at its epoch's last block. Every epoch must be
  stored from its first height, or nothing is published.
- **Entries.** `TachygramEntry { tg, blk_height, stamp_ordinal }` in **ordinal
  order**: stamp order (block, then transaction) and, within a stamp, the
  stamp's own order (`Store::epoch_tachygrams`). This is spec 5.2's
  `(height, tx index, entry index)` applied to tachygrams. Tachygrams are
  unique in an epoch, so within a row the `(key, ordinal)` order is key order;
  the ordinal order matters only for which entries spill into overflow
  segments when no seed fits (spec 6.4).
- **Reuse.** Table bytes are a pure function of `(product, genesis, scope_id,
  entries)`. Built scopes are kept by `(epoch, digest of the entry list)`; a
  sealed scope is found by `(epoch, hash of its last block)` without reading the
  journal. Open scopes are recomputed from the journal at every publication
  and rebuilt whenever their entry list changed. After a restart the previous
  publication's scopes are taken back (checked against the journal) so
  neither the restored publication nor the next one rebuilds them.
- **Phases.** `check`: the fence and the rewind count are those of the
  snapshot, no shutdown. `build`: `build_scope` and `describe` (P8 runtime,
  public setup) for each scope to build. `store`: rows and public setup as
  objects. `verify`: `validate_scope` (every row through `validate_row`, the
  table rules), entry counts against the journal, digests and session ids
  against the bytes, each installed object against its map entry, and the
  whole listing through `ChainMapHead::check_listing`. `assemble`: reserve the
  generation, install map chunks (32 sealed scopes each, compact canonical
  JSON) and the head, store the inventory (subject: the anchor and each open
  scope's last block). `recheck`: the node's identity and the anchor still
  canonical. `commit`: under the rewind gate, `Distribution::commit`.
- **Head.** `protocol`, `profile`, `network`, `genesis_hash` (display order),
  `activation_height`, `epoch_length`, `generation`, `recovery_epoch`,
  `anchor_height`, `anchor_hash`, `floor_scope: 0`, `sealed_chunks`,
  `open_scopes`.
- **Hooks.** `check_subjects` asks the node for every named block (a block
  above the node's tip is an error, `503`); `reissue_head` rewrites
  `generation` and `recovery_epoch` only.
- **Canonical guard.** A task on the root feed withdraws the current
  publication as soon as a block its subject names leaves the chain, even
  while the writer is preparing.

## Metrics (spec 11.8)

`GET /metrics` on the product listener, Prometheus text:
`tachyon_tachygram_ingested_height`, `anchor_height`, `entries_total`,
`open_scope_entries`, `segments_overflow_total` (gauges of the current
publication), `publication_seconds` (histogram, build to commit),
`sessions_revoked_total` (counter), and `pir-observability` HTTP metrics with
the fixed endpoint labels `current`, `objects`, `authority_check`, `metrics`.

## Tests

`cargo test --locked -p tachygram-publisher --profile release-fast`. Every
test runs against `tachyon-fixtures`' node double with 16-block epochs; the
oracle is the authored chain.

- `tests/publisher.rs`: map and tables against the authored records (every
  epoch listed once, sealed and open states, heights, subject, every row
  validated, every tachygram found once through the query plan, metrics);
  determinism across publishing after every block and once from an empty
  state directory; restart (new recovery epoch, previous publication restored
  without building, continued without rebuilding, identical to a fresh
  publisher); shallow reorg (withdraw, new generation, same epoch, new
  content); reorg into a sealed scope (recovery, revoked sessions gone, new
  content); duplicate tachygram (stop policy, block not written, nothing
  published, the scheduler stops); a node without `NuTachyon` refused;
  coalescing; the canonical guard.
- `tests/overflow.rs`: 13,120 tachygrams ground so that under every seed some
  row of epoch 0 overflows; seed 0 with overflow segments, every entry found
  once through every segment of its bucket.
- `tests/host.rs`: `configure` under `pir_publisher::run_registered` with the
  host's own follower and no consumer: readiness, publications advancing,
  tables served over HTTP decoding to the authored records,
  `authority/check` confirming and refuting, `/metrics`, clean shutdown.
