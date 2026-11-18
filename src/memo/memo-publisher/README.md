# memo-publisher

The memo and first-contact products' publisher (spec sections 5, 6, 7, 10.3
and 11, `docs/spec/spec-tachyon-payment-pir.md`). It follows the chain into
`memo-store`, builds one scope of tables per memo epoch and per first-contact
span with `tachyon-pir-table`, and publishes both products through
`tachyon-pir-distribution` under `pir-publisher`'s host. It mirrors
`tachygram-publisher` (itself after `wallet-pir`'s Receiver indexer and
Enhance's coalescing publication task), with one journal feeding two
publications.

Chain and ingestion code (`tools/ci/boundary.json`, memo family):
first-contact is part of the memo family, so both products live here. It
depends on `tachyon-chain` and `memo-store`, never on another product.

## Registering it (publisher binary)

Memo and first-contact are **one** publisher definition, named `memo`, on the
memo listener (spec 11.7). The binary reads both sections and makes one call:

```rust
let settings: MemoSettings = /* config.memo.settings */;
let mut config = MemoConfig::new(settings, config.chain.epoch_length);
if let Some(first_contact) = /* config.first_contact.settings, when enabled */ {
    let first_contact: FirstContactSettings = first_contact; // `{}`; unknown fields refused
    config = config.with_first_contact(first_contact);
}
config.validate()?;
let chain = ChainSettings { rpc_url, rpc_cookie, poll };
let definitions = vec![memo_publisher::configure(config, Some(&chain), PublisherEngine)?];
pir_publisher::run_registered(management_listen, definitions, Some(chain), shutdown).await
```

`first_contact` enabled without `memo` enabled is the binary's to refuse
(spec 11.7). `configure` validates an absolute `state_dir`, a fixed port and a
nonzero epoch length, declares the listener, the state directory and a
readiness probe on `/provider/memo/v1/current` and, with first-contact, on
`/provider/first-contact/v1/current` (each ready when its envelope is eligible
and names a publication), and refuses to register without chain settings. It
opens nothing until the host starts it.

## Public API

```rust
pub struct MemoSettings { pub listen: SocketAddr, pub state_dir: PathBuf }   // memo.settings
pub struct FirstContactSettings {}                                           // first_contact.settings
pub struct MemoConfig { pub listen, pub state_dir, pub epoch_length: u32, pub first_contact: bool,
                        pub decode: DecodeOptions, pub retention: Duration }
impl MemoConfig { pub fn new(settings, epoch_length) -> Self;
                  pub fn with_first_contact(self, FirstContactSettings) -> Self;
                  pub fn validate(&self) -> Result<(), String> }

pub fn configure<E: PublisherExecutor + IngestionExecutor>(config, chain: Option<&ChainSettings>, executor: E)
    -> pir_pipeline::publisher::Result<PublisherDefinition<axum::Router>>;
pub async fn run<E>(config, chain: ChainSettings, feed: ChainFeed, http: Http<Router>, stop: Shutdown, executor: E)
    -> pir_pipeline::publisher::Result<()>;
pub fn distribution_ready(body: &[u8]) -> bool;
pub const DEFINITION_NAME: &str = "memo";
pub const MEMO_READINESS_PATH: &str = "/provider/memo/v1/current";
pub const FIRST_CONTACT_READINESS_PATH: &str = "/provider/first-contact/v1/current";

pub struct MemoPublisher { pub writer: Writer, pub memo: PublicationTask,
                           pub first_contact: Option<PublicationTask>, .. }
impl MemoPublisher {
    pub async fn open(config: &MemoConfig, node: NodeClient, shutdown: Shutdown) -> Result<Self>;
    pub fn open_with(config, node, identity: NetworkIdentity, shutdown) -> Result<Self>;
    pub fn router(&self) -> axum::Router;       // both distribution prefixes + GET /metrics
    pub fn guard(&self, feed: ChainFeed, stop: Shutdown) -> impl Future<Output = Result<()>>;
    pub fn memo_distribution(&self) -> &Distribution;
    pub fn first_contact_distribution(&self) -> Option<&Distribution>;
    pub fn distribution(&self, product: Product) -> Option<&Distribution>;
    pub fn metrics(&self) -> Arc<Metrics>;
    pub fn tables_built(&self, product: Product) -> u64;
}
pub struct Writer;            // impl pir_pipeline::ingestion::Ingestion
pub struct PublicationTask;   // impl pir_pipeline::publisher::Publisher, one per product
impl PublicationTask { pub fn product(&self) -> Product;
                       pub async fn publish_pending<E: PublisherExecutor>(&mut self, &E) -> Result<Option<Report>> }
pub struct Report { pub product, pub committed, pub anchor, pub tables_built }
pub enum Error { Network(String), Journal(memo_store::Error), Other(BoxError) }
```

`Error::policy()`: a network mismatch and a journal that cannot be what it
claims (another binding or span rule, a foreign or newer schema, corruption)
`Stop`; everything else `NextPoll`. No chain content is an error: invalid
envelopes are skipped and counted (spec 5.2).

State under `state_dir`: `memo-v1.sqlite` (the journal, shared by both
products), `distribution/memo/` and `distribution/first-contact/` (authority,
generation counter, objects; each product has its own recovery epoch and
generation) and `publisher.lock`.

## Ingestion (spec 11.1, 11.2)

One `Writer` and one chain follower feed both products:

| Method | Does |
|---|---|
| `target_height(tip)` | `tip - CONFIRMATION_DEPTH` (2) |
| `start_height` | the `NuTachyon` activation height: every journal starts there (spec 7.3) |
| `observed` | the observation's genesis and the node's `chain`, genesis and activation must equal the journal's binding; otherwise withdraw both products and stop. The first pass restores each product's previous publication |
| `committed`, `checkpoint_at_or_below` | the journal's; a failed checkpoint read never empties the journal (`withdraw_above` reads again and refuses) |
| `withdraw_above(keep)` | under the rewind gate, **per product against that product's last publication** (eligible or not): `Recover` if the rewind reaches one of its sealed scopes (counting that publication's sessions in reached scopes as revoked), else `Withdraw` if its anchor is above `keep`; durable before `rewind`. Drops both pending snapshots |
| `rewind(keep)` | the journal's; the envelope gauges are re-read |
| `ingest(from, through)` | resolves each height to a hash once, fetches the block by hash (8 in flight), decodes, extracts with `IndexedBlock::new` (envelopes parsed; invalid ones skipped and counted) and appends in order |
| `caught_up` | offers the journal at its tip to each product for publication (below) |
| `failed` | `Error::policy()`, `Stop` logged as `ALERT` |

The per-product decision matters because the two products seal differently:
a rewind can reach a sealed memo epoch while the first-contact span it touches
is still open (scenario 9 test). With both publications at one anchor the
converse cannot happen (a sealed span's last epoch is a sealed memo epoch),
but the publications are independent and may sit at different anchors.

Invalid envelopes are logged at `debug` by height and count only, never their
bytes or parse errors (which quote them).

## Publication (spec 6.4, 6.5, 7, 10.3, 11.3, 11.4)

`caught_up` takes a **snapshot** per product (one writer operation) and puts
it in that product's one-slot watch channel, then returns: ingestion never
waits for preparation, and nothing waits for a consumer. Each product's
`PublicationTask` (run with `PublisherExecutor::run`; `run` starts one per
product) publishes the latest snapshot; snapshots offered while one is being
published replace each other. That is one active and one pending publication
per product. A pass that changed nothing offers nothing; a failed attempt is
offered again on the next pass.

- **Memo scopes.** One per epoch, `scope_id` = scope index = epoch, from 0 to
  the anchor's epoch. Entries: `memo_entry`s (`key16 || ciphertext`) of the
  epoch's follow-up entries in ordinal order (`Store::memo_entries`).
- **First-contact scopes.** The journal's spans (`Store::spans`): span `i` at
  scope index `i`, `scope_id` its first epoch. Entries: the span's
  first-contact entries in ordinal order (`Store::first_contact_entries`). A
  span is open until a boundary closes it (spec 7.2, decided by the journal
  when an epoch's last block is appended), and seals `SEAL_DEPTH` blocks after
  its last block.
- **Heights and states** (spec 10.3). A scope is sealed when it has its final
  last block (a whole epoch; a closed span) and that block is at least
  `SEAL_DEPTH` (100) below the anchor. The newest scope ends at the anchor;
  every older scope at its own last block (an epoch's last block; a closed
  span's closing block). Every scope must be stored from its first block, or
  nothing of that product is published (spec 7.3).
- **Duplicates.** Every entry of a repeated tag is kept and published in
  ordinal order (spec 5.3); the table builder places equal keys in the order
  given and spills in ordinal order.
- **Reuse.** Table bytes are a pure function of `(product, genesis, scope_id,
  entries)`. Built scopes are kept per product by `(scope_id, digest of the
  entry list)`; a sealed scope is found by `(scope_id, hash of its last
  block)` without reading the journal. Open scopes are recomputed at every
  publication and rebuilt whenever their entry list changed. After a restart
  each previous publication's scopes are taken back (checked against the
  journal) so neither the restored publication nor the next one rebuilds them.
- **Phases.** `check`: the product's fence and the rewind count are those of
  the snapshot, no shutdown. `build`: `build_scope` and `describe` (P8
  runtime, public setup). `store`: rows and public setup as objects.
  `verify`: `validate_scope` (every row through `validate_row`, the table
  rules), entry counts against the journal, digests and session ids against
  the bytes, each installed object against its map entry, and the whole
  listing through `ChainMapHead::check_listing`. `assemble`: reserve the
  generation, install map chunks (32 sealed scopes each by scope index, compact
  canonical JSON) and the head, store the inventory (tables by scope index;
  subject: the anchor and each open scope's last block). `recheck`: the
  node's identity and the anchor still canonical. `commit`: under the rewind
  gate, `Distribution::commit`.
- **Hooks.** `check_subjects` asks the node for every named block (a block
  above the node's tip is an error, `503`); `reissue_head` rewrites
  `generation` and `recovery_epoch` only.
- **Canonical guard.** One task per product on the root feed withdraws that
  product's current publication as soon as a block its subject names leaves
  the chain, even while the writer is preparing.

## Metrics (spec 11.8)

`GET /metrics` on the memo listener, Prometheus text. For each product
(`tachyon_memo_` and, when enabled, `tachyon_first_contact_`):
`ingested_height` (the shared journal's), `anchor_height`, `entries_total`,
`open_scope_entries`, `segments_overflow_total` (gauges of that product's
current publication), `publication_seconds` (histogram, build to commit),
`sessions_revoked_total` (counter). Memo adds `envelopes_invalid_total` and
`state_entries_total`, gauges over the journal's stored blocks (they fall on a
rewind and survive a restart). HTTP metrics from `pir-observability` with the
fixed endpoint labels `memo_current`, `memo_objects`, `memo_authority_check`,
`first_contact_current`, `first_contact_objects`,
`first_contact_authority_check`, `metrics`.

## Tests

`cargo test --locked -p memo-publisher --profile release-fast`. Every test runs
against `tachyon-fixtures`' node double with 4-block epochs; the oracle is the
fixture's authored envelopes (`follow_ups`, `first_contacts`,
`envelope_counts`) and an independent restatement of the span rule, never the
journal or the tables. `check_against_chain` checks, per product, every scope
from index 0 listed once with its id, state and heights, the subject, every
row of every table through `validate_row` decoding to exactly the scope's
authored entries, and every key's query-plan lookup returning all its entries
in ordinal order.

- `tests/publisher.rs`: both maps and tables against the authored envelopes
  with repeated tags (scenario 5), all twelve malformations beside valid
  envelopes (scenario 7) and wallet-state entries, and the metrics; sealed
  determinism across a 15-block cadence and a rebuild from empty state, with a
  span closing by the 64-epoch rule (spec 6.5); restart (new recovery epochs,
  previous publications restored without building, continued without
  rebuilding, identical to a fresh publisher; scenario 12); coalescing; memo
  without first-contact; a node without `NuTachyon` refused.
- `tests/spans.rs`: a span closing by count (3,074 entries, closing at its
  epoch boundary), then a rewind into it: both products recover, old sessions
  gone, the replacement closes it again.
- `tests/reorg.rs`: shallow reorg (scenario 8, both withdraw and publish new
  generations; the envelope gauges fall); reorg into a sealed memo epoch with
  first-contact open (scenario 9: memo recovers, first-contact only
  withdraws); the canonical guards.
- `tests/overflow.rs`: 3,648 memo tags ground so that under every seed some
  row of epoch 0 overflows (scenario 6); seed 0 with overflow segments, every
  entry retrievable through the query plan.
- `tests/host.rs`: `configure` under `pir_publisher::run_registered` with the
  host's own follower and no consumer (scenario 1): readiness on both
  prefixes, both publications advancing, tables served over HTTP decoding to
  the authored entries, `authority/check` confirming and refuting on both
  prefixes, `/metrics`, clean shutdown.
