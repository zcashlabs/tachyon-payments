# tachyon-pir-distribution

The publisher-to-query-host contract shared by the four products (memo,
first-contact, tachygram, registry), so no product repeats it (spec 4.2,
11.3-11.6). It is built on `wallet-pir`'s `pir-publication`,
`pir-sync-manager` and `pir-pipeline`, used unchanged, and mirrors Receiver's
distribution (`receiver-pir-server/src/distribution.rs`,
`receiver-indexer/src/distribution.rs`, `pir-query-server/src/receiver/sync.rs`).

It is a serving crate (`tools/ci/boundary.json`): no chain, store, publisher
host or fixture dependency. Chain facts reach it only as the `Subject` a
product supplies and checks.

## Contract (`contract`)

| Item | Value |
|---|---|
| Product | `Product::{Memo, FirstContact, Tachygram, Registry}`, named `memo`, `first-contact`, `tachygram`, `registry` |
| Prefix | `/provider/<product>/v1/` (`Product::prefix`; `mount_path` without the slash) |
| Schema | `tachyon-<product>-prepared-distribution-v1`, the envelope scope's `product` |
| Scope | `PublicationScope { product: schema, chain: genesis (protocol order), source }` |
| Routes | `GET current`, `GET objects/<sha256>`, `POST authority/check` |

`source` binds the parameters that shape every scope, so a host configured for
another chain shape refuses the envelope:

- chain products: `SHA-256("tachyon-pir/v1/distribution-source\0" || "chain\0" || activation_height u64 LE || epoch_length u64 LE)` (`ScopeBinding::chain`);
- registry: `SHA-256("tachyon-pir/v1/distribution-source\0" || "registry\0" || REGISTRY_CHUNK_ENTRIES u64 LE)` (`ScopeBinding::registry`).

The envelope is `pir_publication::PublicationEnvelope<Descriptor>`:

```rust
Descriptor { map_head: ObjectReference, generation: u64, recovery_epoch: u64 }
```

`generation` is the product's durable counter, advanced by every publication;
`recovery_epoch` is the authority fence's epoch the publication was made
under. An eligible envelope's descriptor always carries the fence's current
epoch; hosts refuse one that does not.

The envelope names an inventory object (material identity = SHA-256 of its
JSON, as Receiver's):

```rust
Inventory {
    generation, recovery_epoch,
    map_head: ObjectReference,          // at most 1 MiB (spec 10.3)
    map_chunks: Vec<ObjectReference>,   // every sealed chunk, ascending base
    newest_scope: u64,                  // index of the newest scope the map lists
    tables: Vec<TableObjects>,          // ascending scope index
    subject: Subject,
}
TableObjects { scope_index: u64, session_id: [u8; 32] /* hex */, rows: ObjectReference, public: ObjectReference }
Subject = Chain { anchor: BlockRef, open_scopes: Vec<BlockRef> }   // memo, first-contact, tachygram
        | Registry { log_sequence: u64 }                          // registry
BlockRef { height: u64, hash: [u8; 32] /* display-order hex in JSON */ }
```

Every struct refuses unknown fields. `Inventory::validate(product, limits)`
binds rows objects to exactly `4096 × 8192` bytes and public objects to exactly
45,056 bytes (`ObjectLimits::SPEC`, spec 6.1 and 6.6; tests pass smaller
lengths), bounds the head (1 MiB) and chunks (8 MiB), refuses repeated
sessions or map objects, more chunks than the scopes fill, tables out of scope
order or beyond `newest_scope`, zero generation or epoch, and a subject of the
wrong kind or with an open-scope block above the anchor. Two tables may share
identical objects (empty segment-0 tables of different scopes). The product's
own map-level checks (session ids, `rows_sha256`, `public_sha256` against the
map) stay with the product, which can parse its map.

## Publisher side (`publisher::Distribution`)

```rust
let d = Distribution::open(&state_dir, Product::Tachygram, binding, hooks, DistributionOptions::default())?;
let router = d.mount(axum::Router::new());          // nests the three routes at the prefix
d.restore().await?;                                 // once, right after open
let draft = d.begin()?;                             // reserves the next generation durably
let rows = d.install(&rows_bytes)?;                 // bytes -> ObjectReference (not yet served)
let candidate = d.assemble(draft, Contents { map_head, map_chunks, newest_scope, tables, subject })?;
d.commit(candidate)?;                               // retain, then Publish
d.withdraw()?;  d.withdraw_above(Some(kept))?;  d.recover()?;
```

State lives in `state_dir/distribution/<product>/`: `authority.json`
(`PublicationAuthority`), `generation.json`, `objects/` (`PublishedObjectStore`).
Memo and first-contact can share a state directory.

- **Open** starts a new, withdrawn recovery epoch (the authority's rule,
  spec 11.4). The generation resumes from `max(generation.json, current descriptor)`.
- **Publication** maps onto `pir_pipeline::Publication`: `build` installs
  objects, `verify` is `assemble`'s `Inventory::validate`, `assemble` stores the
  inventory object, `commit` is `Distribution::commit`. A commit is refused
  (`CommitRefusal`) if a withdrawal or recovery happened since `begin`, if a
  publication of the same or a later generation is current, or if a named
  object is not installed. It then retains the candidate's objects (the
  displaced publication's stay for `retention`, default 600 s; everything else
  is deleted, normative rule 5) and applies `Publish`. If the authority is
  uncertain after a failed write, it applies `Recover` and publishes the
  candidate again under the new epoch, with the head re-issued through
  `PublisherHooks::reissue_head`.
- **Withdraw** (open-scope rewind whose anchor is above the kept block) and
  **Recover** (rewind into a sealed scope) are durable on return, before the
  product rewinds its journal (spec 11.4).
- **Restore** republishes the previous process's publication under the new
  epoch and a new generation, with the head re-issued, if every object is
  still retained and `PublisherHooks::check_subjects` still confirms its
  subject. Its tables keep their session ids, so hosts reuse their runtimes.
- **Routes**: `GET current` returns the envelope with `ETag:
  conditional_token`, `304` on a matching `If-None-Match`, `Cache-Control:
  no-store`. `GET objects/:id` streams a retained object with `Cache-Control:
  public, max-age=31536000, immutable`, `404` otherwise. `POST authority/check`
  admits the request against the current eligible envelope (`400` malformed,
  `409` advanced), bounds it (256 subjects, 4,096 blocks, subjects of the
  product's kind), asks `check_subjects` (`503` on error), admits again and
  answers in request order.

`PublisherHooks` is what the product supplies: `check_subjects` (chain: every
named block canonical; registry: the log still begins with `log_sequence`
entries) and `reissue_head(head, generation, recovery_epoch)`.

## Query-host side (`consumer`)

```rust
let publications = Publications::<MyRuntime>::new(ServingPolicy::default());
let mut config = FollowerConfig::new(Product::Tachygram, binding, publisher_origin, state_dir);
config.publisher_ca = Some(pem_bytes);
config.retain_scopes = Some(64);
let engine = follow(config, MyPreparer, publications.clone())?;   // ConsumerEngine<EnvelopeConsumer<Follower<_>>>
SyncManager::new(engine).with_success_interval(poll).run_until(stop).await;
match publications.session(&session_id) { SessionLookup::Found { table, publication } => .., _ => .. }
```

`follow` locks `state_dir`, opens `authority-floor.json` (`AuthorityFloor`
bound to origin, prefix and genesis), the `objects/` cache, and an
`HttpsSource` for the product prefix. `Follower` is the `PublicationAdapter`:

- `plan` reads the inventory, validates it, checks that it is exactly the
  publication the envelope authorizes and was made under the fence's epoch,
  and lists the objects to fetch: the head, every chunk, and the tables of
  scopes at or above the host's floor (`newest_scope + 1 - retain_scopes`).
  The engine fetches each into the cache, verified by SHA-256 and length.
- `load` hands the verified set to `Preparer::check` (the whole publication:
  head and chunk bytes, tables, inventory) and `Preparer::prepare` for each
  table no held runtime covers (a runtime is reused only for identical rows
  and public objects). Any error refuses the publication; the previous one
  keeps serving.
- Commit records `active.json`, then installs into `Publications`.

`Publications::session` answers spec 10.5:

| Lookup | HTTP | When |
|---|---|---|
| `Found` | serve | in the current publication, or in one displaced by a newer publication of the same epoch within `retention` (default 600 s) |
| `Revoked` | 410 | in a publication superseded because the recovery epoch increased, and absent from the current one; remembered for `retention` |
| `Unknown { map_sha256 }` | 409 | anything else while serving |
| `Unavailable` | 503 | nothing serves: before the first commit, after a fence, or when unconfirmed too long |

Authority and outage rules, following `pir-publication`'s floor:

- Negative evidence fences at once: a withdrawn envelope, a new recovery
  epoch, a subject the publisher no longer confirms, or a failed floor write
  stops serving (503) until the next publication is installed. Fenced
  runtimes are kept only to be reused by the next load.
- Every pass re-confirms every serving view through `authority/check` before
  anything new is planned. If the publisher is unreachable, the floor gains
  no evidence and nothing is fenced: the host keeps serving what it last
  confirmed for `max_unconfirmed` (default 600 s) after that confirmation
  started, then answers 503 until a pass confirms again. `SyncManager` retries
  an unreachable publisher every second (it is `transient`) for 90 attempts,
  then backs off.
- A host restart serves nothing from local state; it follows the publisher
  again (cached objects are re-verified, not re-fetched). Revocations are
  in-memory, so after a host restart a revoked session answers 409.
- In-flight queries: look the session up again after evaluation and discard
  the answer if it is no longer `Found`.

## Tests

`tests/distribution.rs` runs the publisher side on axum at 127.0.0.1 behind an
in-test TLS terminator (tokio-rustls, certificate from `rcgen`), and the
follower over `HttpsSource`: publications without consumers, routes and
bounded checks, cold bootstrap (and a narrower `retain_scopes` host),
incremental fetch counted at the server, corrupt/truncated/refused
publications, withdrawal, outage, publisher restart and restore, sealed-reorg
revocation (410) versus shallow rewind (409), a refuted subject, retention
and collection on both sides, and recovery after an uncertain authority write.
Table objects are small blobs (`ObjectLimits` with 64-byte rows and 16-byte
public setups).
