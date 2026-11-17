# tachyon-pir-publisher

The publisher binary (spec sections 4.1, 11.7, 11.8): one process, one chain
follower, one publisher definition per compiled and enabled product, hosted by
`wallet-pir`'s `pir_publisher::run_registered`. It never contacts query
servers.

## Building

Products are Cargo features; the default build hosts none.

| Feature | Section | Product library | Status |
|---|---|---|---|
| `tachygram` | `tachygram` | `tachygram-publisher` | M1 |
| `memo` | `memo` | `memo-publisher` | M2: enabling the feature is a compile error until the crate exists |
| `first-contact` (implies `memo`) | `first_contact` | `memo-publisher` | M2, as above |
| `registry` | `registry` | `registry-publisher` | M3, as above |

```
cargo build --release -p tachyon-pir-publisher --features tachygram
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

JSON, unknown fields refused at every level. Example with tachygram only
([`publisher.example.json`](publisher.example.json), checked by the tests):

```json
{
  "management_listen": "127.0.0.1:9400",
  "chain": {
    "rpc_url": "http://127.0.0.1:18232",
    "no_auth": true,
    "poll_ms": 1000,
    "epoch_length": 4096
  },
  "tachygram": {
    "enabled": true,
    "settings": {
      "listen": "127.0.0.1:9402",
      "state_dir": "/var/lib/tachyon-pir/tachygram"
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
| `chain.epoch_length` | Blocks per epoch on this network (spec 15), nonzero, no default |
| `memo`, `first_contact`, `tachygram`, `registry` | Optional `{ "enabled": bool, "settings": {...} }`. A section for a product this build does not include is refused, enabled or not, naming the feature. Settings are checked against the product's schema whenever present; an enabled product needs them |
| `first_contact.enabled` | Requires `memo.enabled`: first-contact runs in the memo publisher's definition, on its listener and state directory |
| `<product>.settings.listen` | Loopback or private (RFC 1918, ULA) address with a fixed port, not shared with any other listener |
| `<product>.settings.state_dir` | Absolute; disjoint from every other product's (existing prefixes and symlinks are resolved) |

`tachygram.settings` is `{ "listen", "state_dir" }` (`tachygram_publisher::TachygramSettings`).

## Start-up

1. Parse and apply the static rules (as `--check`).
2. When `chain` is present, read the node's network identity once
   (`getblockchaininfo`, `getblockhash 0`) and log `chain`, `genesis` and
   `activation_height`. A node without the `NuTachyon` upgrade, or one that
   cannot be read, stops start-up with status 1 before anything is bound
   (spec 11.1).
3. Hand the definitions to `run_registered`, which re-checks the layout, binds
   every listener up front (a bind failure names the owner and address and
   exits 1), starts the root chain follower and the products.
4. SIGTERM or SIGINT requests shutdown; products finish their current
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
| `tachygram.settings.listen` | `GET /provider/tachygram/v1/current`, `GET /provider/tachygram/v1/objects/<sha256>`, `POST /provider/tachygram/v1/authority/check`, `GET /metrics` |

A product is ready when its task runs and its `current` envelope is eligible
and names a publication. The discovery document is served only on the
management listener, so the edge in front of the publisher routes
`/.well-known/pir-service-discovery` there and every other path to the product
listener (spec 11.6).

Restarting over the same state directories starts a new recovery epoch: the
previous publication is reissued under it with a new generation, without
rebuilding tables (spec 11.4).
