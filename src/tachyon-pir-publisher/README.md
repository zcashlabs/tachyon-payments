# tachyon-pir-publisher

The publisher binary (spec sections 4.1, 11.7, 11.8): one process, one chain
follower, one publisher definition per compiled and enabled product, hosted by
`wallet-pir`'s `pir_publisher::run_registered`. It never contacts query
servers.

## Building

Products are Cargo features; the default build hosts none.

| Feature | Section | Product library |
|---|---|---|
| `tachygram` | `tachygram` | `tachygram-publisher` |
| `memo` | `memo` | `memo-publisher` |
| `first-contact` (implies `memo`) | `first_contact` | `memo-publisher` |
| `registry` | `registry` | `registry-publisher` |

```
cargo build --release -p tachyon-pir-publisher --features memo,first-contact,tachygram,registry
```

## Command line

```
tachyon-pir-publisher --config FILE           # run until SIGTERM or SIGINT
tachyon-pir-publisher --config FILE --check   # validate offline and exit
tachyon-pir-publisher --contract-info         # discovery contract and compiled products, as JSON
```

`--check` parses the file and applies every static rule below. It opens no
store, socket or credential, creates nothing, and never contacts the node.

Exit status: `0` after a valid `--check` or a requested shutdown, `1` when the
configuration is invalid, the node refuses start-up or the host fails, `2` for
usage errors. Logs go to stderr; `RUST_LOG` sets the filter (default `info`).

## Configuration

JSON, unknown fields refused at every level. Example with all four products
([`publisher.example.json`](publisher.example.json), checked by the tests in a
build with all four features):

```json
{
  "management_listen": "127.0.0.1:9400",
  "chain": {
    "rpc_url": "http://127.0.0.1:18232",
    "no_auth": true,
    "poll_ms": 1000,
    "epoch_length": 4096
  },
  "memo": {
    "enabled": true,
    "settings": {
      "listen": "127.0.0.1:9401",
      "state_dir": "/var/lib/tachyon-pir/memo"
    }
  },
  "first_contact": { "enabled": true, "settings": {} },
  "tachygram": {
    "enabled": true,
    "settings": {
      "listen": "127.0.0.1:9402",
      "state_dir": "/var/lib/tachyon-pir/tachygram"
    }
  },
  "registry": {
    "enabled": true,
    "settings": {
      "listen": "127.0.0.1:9403",
      "state_dir": "/var/lib/tachyon-pir/registry",
      "difficulty_bits": 20
    }
  }
}
```

| Field | Rule |
|---|---|
| `management_listen` | Required. Loopback address, fixed port |
| `chain` | Required when any product is enabled: the chain products follow it, and every product's distribution scope names its genesis hash (spec 11.3). When present, the node's identity is checked at start-up |
| `chain.rpc_url` | `http` or `https` with a host and no credentials |
| `chain.no_auth` / `chain.rpc_cookie` | Exactly one: `"no_auth": true`, or an absolute cookie path |
| `chain.poll_ms` | Tip poll interval, nonzero, default 1000 |
| `chain.epoch_length` | Blocks per epoch on this network (spec 15), at least 2 (spec 11.3: an epoch stays open `SEAL_DEPTH` blocks after it ends and a subject names at most `MAX_OPEN_SCOPES` = 64 open-scope blocks), no default |
| `memo`, `first_contact`, `tachygram`, `registry` | Optional `{ "enabled": bool, "settings": {...} }`. A section for a product this build does not include is refused, enabled or not, naming the feature. Settings are checked against the product's schema whenever present; an enabled product needs them |
| `first_contact.enabled` | Requires `memo.enabled`: first-contact runs in the memo publisher's definition (named `memo`), on its listener and state directory |
| `<product>.settings.listen` | Loopback or private (RFC 1918, ULA) address with a fixed port, not shared with any other listener |
| `<product>.settings.state_dir` | Absolute; disjoint from every other product's (existing prefixes and symlinks are resolved) |

- `memo.settings` is `{ "listen", "state_dir" }` (`memo_publisher::MemoSettings`); the
  state directory holds `memo-v1.sqlite`, shared by both products, and
  `distribution/memo/`, `distribution/first-contact/`.
- `first_contact.settings` is `{}` (`memo_publisher::FirstContactSettings`); an
  enabled `first_contact` adds the first-contact product to the `memo`
  definition.
- `tachygram.settings` is `{ "listen", "state_dir" }` (`tachygram_publisher::TachygramSettings`).
- `registry.settings` is `{ "listen", "state_dir", "difficulty_bits" (20),
  "per_client_concurrent" (2), "per_client_per_minute" (6),
  "global_per_minute" (600), "upload_ms" (15000), "publish_ms" (1000) }`
  (`registry_publisher::RegistrySettings`; defaults in parentheses). The
  registry follows no chain, but the `chain` section is still required: its
  log and map are bound to the node's `chain` name and genesis hash (spec
  9.2), read at start-up. `--check` validates the settings and the registry's
  listener and state directory offline; only the identity waits for the node.

## Start-up

1. Parse and apply the static rules (as `--check`).
2. When `chain` is present, read the node's network identity once
   (`getblockchaininfo`, `getblockhash 0`) and log `chain`, `genesis` and
   `activation_height`. A node without the `NuTachyon` upgrade, or one that
   cannot be read, stops start-up with status 1 before anything is bound
   (spec 11.1).
3. Build the registry definition (if enabled) bound to that identity.
4. Hand the definitions to `run_registered`, which re-checks the layout, binds
   every listener up front (a bind failure names the owner and address and
   exits 1), starts the root chain follower and the products.
5. SIGTERM or SIGINT requests shutdown; products finish their current
   operation, listeners are released, and the process exits 0.

A product whose task fails is reported as `failed` on `/health` and `/ready`
(503) and is not restarted; the management listener keeps running
(`run_registered`'s policy). The supervisor should restart the process.

## Listeners

All listeners are plain HTTP on private addresses; a TLS edge or reverse proxy
fronts them (spec 4.3, 11.6).

| Listener | Routes |
|---|---|
| `management_listen` | `GET /health` (always 200), `GET /ready` (200 or 503), both `{"ready": bool, "products": {"<name>": {"phase": "running"\|"failed"\|"stopped", "ready": bool}}}`; `GET /.well-known/pir-service-discovery` (`{"service":"pir-publisher","version":"1.0.0"}`, `cache-control: no-store`). No `/metrics` |
| `memo.settings.listen` | `GET /provider/memo/v1/current`, `GET /provider/memo/v1/objects/<sha256>`, `POST /provider/memo/v1/authority/check`; with first-contact enabled the same three under `/provider/first-contact/v1/`; `GET /metrics` (`tachyon_memo_*`, `tachyon_first_contact_*`) |
| `tachygram.settings.listen` | `GET /provider/tachygram/v1/current`, `GET /provider/tachygram/v1/objects/<sha256>`, `POST /provider/tachygram/v1/authority/check`, `GET /metrics` |
| `registry.settings.listen` | `GET /provider/registry/v1/current`, `GET /provider/registry/v1/objects/<sha256>`, `POST /provider/registry/v1/authority/check`; the write API `GET /registry/v1/challenge` and `POST /registry/v1/register` (legacy alias `/v1/registry/...`; a reverse proxy exposes it and MUST overwrite `X-Forwarded-For`, spec 9.3); `GET /metrics` (`tachyon_registry_*`) |

A definition is ready when its task runs and each of its `current` envelopes
is eligible and names a publication (the `memo` definition probes both
prefixes when first-contact is enabled). `/health` and `/ready` list
definitions: `memo` (covering first-contact), `tachygram` and `registry`. The discovery document is served only on the
management listener, so the edge in front of the publisher routes
`/.well-known/pir-service-discovery` there and every other path to the product
listener (spec 11.6).

Restarting over the same state directories starts a new recovery epoch for
every product (memo and first-contact each have their own): the previous
publication is reissued under it with a new generation, without
rebuilding tables (spec 11.4).
