# tachyon-pir-query-server

The query host of the Tachyon payment PIR services (spec
`docs/spec/spec-tachyon-payment-pir.md`, sections 4.1, 10 and 11.5-11.8). One
process serves every compiled product in the `combined` role: it follows the
publisher over HTTPS, prepares a PIR runtime for every table it serves and
answers wallet queries.

A serving package (`tools/ci/boundary.json`): it depends on
`tachyon-pir-protocol`, `tachyon-pir-table`, `tachyon-pir-distribution` and
`wallet-pir`'s `pir-pipeline`, `pir-publication`, `pir-sync-manager`,
`pir-control` (feature `admission`) and `pir-observability` (feature `http`),
and on no chain, store, publisher or fixture crate.

## Products

Cargo features `memo`, `first-contact`, `tachygram`, `registry`; default none
(spec 4.2). Serving modules exist for `memo`, `first-contact` and `tachygram`;
each feature compiles independently of the others (first-contact does not
need memo: separate mounts, separate publications under
`/provider/memo/v1/` and `/provider/first-contact/v1/`). A configuration
section for a product without a compiled serving module is refused at
start-up and by `--check`, enabled or not; the `registry` feature reserves
the name and compiles no module yet.

Every product shares one serving core (`serving`, `prepare`, `sync`, `host`).
A product module (`src/memo.rs`, `src/first_contact.rs`,
`src/tachygram.rs`) is constants only: its configuration
section, Cargo feature, metric prefix, the four fixed endpoint labels, its
default `retain_scopes` and its map family. The mounts, query magic, protocol
string, entry layout and placement come from `tachyon_pir_protocol::Product`.
All three are `MapFamily::Chain`. What differs between them is handled by the
protocol crate from the product constant: entry layout (memo 144 bytes, 56
slots; first-contact 1,232 bytes, 6 slots; tachygram 40 bytes), key order
(non-decreasing for memo and first-contact, whose tags may repeat, spec 5.3;
strictly increasing for tachygram) and the height check (tachygram only).
First-contact scopes are spans: the shared core uses the scope index (the
span's position) for the listing, the inventory, the floor and
`retain_scopes`, and the span's `scope_id` (its first epoch) for placement
and session ids. The registry adds `MapFamily::Registry` (its `RegistryMapHead` in `map.rs`, its scope
binding in `config.rs`) and a module.

## Usage

```
tachyon-pir-query-server --config FILE           # serve until SIGTERM or SIGINT
tachyon-pir-query-server --config FILE --check   # validate offline; exit 0 or 1
```

`--check` parses and validates the file without opening state, binding a
socket, reading the CA file or contacting a publisher. Logging follows
`RUST_LOG` (default `info`); no client-supplied byte is ever logged.

## Configuration (spec 11.7)

JSON; every object refuses unknown fields.

```json
{
  "management_listen": "127.0.0.1:9500",
  "public_listen": "127.0.0.1:9510",
  "tachygram": { "enabled": true, "settings": {
    "publisher": "https://publisher.example/",
    "state_dir": "/var/lib/tachyon-pir-query/tachygram",
    "publisher_ca": "/etc/tachyon-pir-query/publisher-ca.pem",
    "role": "combined",
    "publisher_network": "test",
    "publisher_genesis": "<64 hex, display order>",
    "activation_height": 8,
    "epoch_length": 4096,
    "retain_scopes": 64,
    "max_unconfirmed_seconds": 600,
    "admission": { "executing": 2, "waiting": 8, "wait_ms": 2000,
                   "per_client": 2, "upload_ms": 15000 },
    "poll_ms": 1000 } }
}
```

| Field | Rule |
|---|---|
| `management_listen` | Loopback, fixed port |
| `public_listen` | Loopback or private, fixed port, distinct from management; behind the deployment's HTTPS edge |
| `memo`, `first_contact`, `tachygram`, `registry` | `{enabled, settings}`; settings required when enabled; a section of a product not compiled in is refused |
| `publisher` | `https://host[:port]/` (checked by `HttpsSource`) |
| `state_dir` | Absolute; disjoint between products; holds the lock, `authority-floor.json`, `active.json` and the object cache |
| `publisher_ca` | Optional absolute PEM path; the system roots without it |
| `role` | `combined` only (spec 4.1) |
| `publisher_network`, `publisher_genesis` | Every map must state exactly these; the genesis also binds the distribution envelope's scope |
| `activation_height`, `epoch_length` | Required for chain products; with the genesis they form the distribution scope (`ScopeBinding::chain`, spec 11.3) and every map must state them |
| `retain_scopes` | At least 1, counted in scope indices; default 64 for memo and tachygram, every span for first-contact (spec 11.5) |
| `max_unconfirmed_seconds` | 1..=86400, default 600 (spec 11.6) |
| `admission` | Defaults 2 executing, 8 waiting, 2,000 ms wait, 2 per client, 15,000 ms upload (spec 10.4, 10.6) |
| `poll_ms` | Interval between successful synchronization passes, 10..=3,600,000, default 1,000 |

## Behaviour

Start-up: validate; check every product's publisher discovery document
(`/.well-known/pir-service-discovery`), refusing to start on any failure; bind
both listeners; open each product's follower; serve.

Synchronization (spec 11.6), per product: `tachyon_pir_distribution::follow`
over `HttpsSource`, driven by `pir_sync_manager::SyncManager`. Each pass
re-checks discovery (`EnvelopeConsumer::settle`; a confirmed major-version
mismatch is the engine's only terminal error, and the host then exits
non-zero), confirms every serving view with the publisher, fetches and
verifies new objects, prepares, and only then switches. Preparation of a
publication:

1. The map head and every chunk are parsed and checked with the protocol
   crate's client checks (`ChainMapHead::validate`, `parse_chunk`,
   `check_listing`) and must be canonically encoded (spec 10.3); the head's `network`, `genesis_hash`,
   `activation_height` and `epoch_length` must be the configured ones; its
   `generation` and `recovery_epoch` the inventory's; the authority subject
   must be the map's anchor and the last block of every open scope; the
   inventory's newest scope, chunks and tables (session ids, `rows_sha256`,
   `public_sha256`) must be exactly the map's.
2. For every table at or above the host floor that no held runtime covers:
   the rows must hash to `rows_sha256`, every row must pass
   `tachyon_pir_table::validate_table` under the context the map implies
   (tachygram rows with the scope's heights), the entry count must match, the
   runtime is built (`prepare_published_in`) and its public setup must equal
   the published object.

Builds and validation run on a dedicated rayon pool of half the cores. Any
failure refuses the publication and the previous one keeps serving. A
displaced publication of the same recovery epoch keeps serving for 600 s;
sessions dropped by a recovery answer 410 for 600 s; while the publisher is
unreachable the host serves what it last confirmed for
`max_unconfirmed_seconds` (all from `tachyon_pir_distribution::Publications`).

Stop (SIGTERM or SIGINT): products are marked stopping and refuse new wallet
requests (503), the public listener drains accepted requests (at most 60 s),
synchronization stops at its next pass boundary, then management closes.
Exit status 0.

## Public routes (spec 10.1, 10.2)

Each product at its canonical and legacy mount (`/memo/v1` and `/v1/memo`,
`/first-contact/v1` and `/v1/first-contact`, `/tachygram/v1` and
`/v1/tachygram`), through `pir_control::public_routes::PublicMount`. Query
magics `TMQ1`, `TFQ1`, `TGQ1`.

| Route | Answer |
|---|---|
| `GET map` | The current map head (`application/json`), `X-Map-Sha256`, `X-Floor-Scope` (the host floor, `newest + 1 - retain_scopes`, or the head's if larger), `no-store` |
| `GET map/{base}/{sha256}` | A sealed chunk of the current map, `public, max-age=31536000, immutable`; a chunk the map does not hold is `409` with `map_sha256`; a `base` that is not the chunk's is `400` |
| `GET public/{session_id}` | The table's public setup (`application/octet-stream`, immutable); unknown, revoked or unavailable sessions answer as `query` does |
| `POST query` | `magic || session_id || nonce || payload`; answer: the echoed 52-byte header and the 10,240-byte reply, `no-store` |

`query` checks, in order: product ready (503); the client's slot (429); a
declared length over 512 KiB (413); the upload within the deadline (408) and
the limit (413); framing: header present and the product's magic (400); the
session (`409 stale_routing` with `map_sha256`, `410 session_unavailable`,
`503`); the exact length for the table's geometry (400); an evaluation permit
(429). The answer runs on the blocking pool holding every guard; the session
is looked up again afterwards and the answer is discarded if it is no longer
served.

Errors are `tachyon_pir_protocol::ErrorBody` exactly (spec 10.5), with
`Cache-Control: no-store`; 429 and 503 carry `Retry-After: 1`. Unknown paths
(404) and wrong methods (405) also carry an `invalid_request` body. Every
response is `no-store` except chunks and public setups. Before a product's
first publication every route of its namespace answers 503.

Admission (spec 10.6): per product a `pir_control::admission::Queue` and
`ClientSlots`, keyed by `pir_control::admission::client_key` (the first
`X-Forwarded-For` address, else `X-Real-IP`, else the peer). The edge must
overwrite those headers.

## Management routes (spec 11.8)

| Route | Answer |
|---|---|
| `GET /health` | `{"ready", "identity", "products": {"<product>": {"phase", "ready"}}}` keyed `memo`, `first-contact`, `tachygram`; phases `running`, `stopping`, `stopped`, `failed` |
| `GET /ready` | The same body; 200 only when every enabled product is ready |
| `GET /metrics` | Prometheus text: `pir_http_*{endpoint}` from `pir-observability` for the public routes, and the product metrics below |

Product metrics, prefix `tachyon_memo_`, `tachyon_first_contact_` or
`tachyon_tachygram_`: `ready`, `anchor_height`,
`generation`, `recovery_epoch`, `floor_scope`, `newest_scope`,
`entries_total`, `open_scope_entries` (of the served map), `tables_served`,
`runtime_held_bytes`, `sessions_revoked_total`,
`queries_total{result}` (`answered`, `invalid_request`, `too_large`,
`upload_timeout`, `stale_routing`, `session_unavailable`,
`temporarily_unavailable`, `overloaded`) and
`sync_passes_total{result}` (`committed`, `deferred`, `failed`). Endpoint
labels are fixed (`<product>_map`, `<product>_map_chunk`, `<product>_public`,
`<product>_query`, with `first_contact` for first-contact); no path segment, session id or client byte is a label.

## Tests

`cargo test -p tachyon-pir-query-server --profile release-fast --features memo,first-contact,tachygram`.
`tests/support` publishes synthetic publications of any chain product with real
`tachyon-4k8` tables (`tachyon-pir-table`'s scope builder) through a
publisher-side `Distribution` behind an in-test TLS terminator; the host runs
in-process on its own listeners, or as the built binary. Wallet requests come
from the probe client (`tachyon-pir-cli`) or are built with
`zakura-pir-client`; the oracle is the input entry list.

- `tachygram`: cold bootstrap to ready and exact answers for hits and misses on
  both mounts; incremental publication with the displaced view still served;
  every refusal (400 magic, short, long, 413 declared and streamed, 408, 409
  with `map_sha256` on all three session routes, 404, 405); recovery-epoch
  revocation (410); a corrupt row and a wrong public setup each refused with
  the previous publication still serving; overload (429 per client and per
  queue, admitted answers correct); 503 before the first publication;
  `X-Floor-Scope` and 409 below the floor; refusal to start on a discovery
  major mismatch.
- `memo`: cold bootstrap with a tag repeated past a row's 56 slots, all its
  entries returned in ordinal order across the overflow segment (both
  mounts); recovery revocation (410), unknown session (409), wrong magic
  (400); a corrupt row refused.
- `first_contact`: a sealed three-epoch span and an open span with their
  heights, lookups by scope index and `scope_id`, a tag repeated past a
  row's 6 slots across the overflow segment; `retain_scopes` and
  `X-Floor-Scope` as scope indices; revocation (410 on `query` and
  `public`); a corrupt row refused; with `memo` also compiled, both products
  side by side in one host.
- `binary`: `--check` exit codes; the binary bootstraps, answers and exits 0
  on SIGTERM with its ports released.
- `config`: unknown fields, products not compiled in, role, listeners, paths
  and limits.
