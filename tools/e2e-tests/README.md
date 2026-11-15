# tachyon-pir-e2e-tests

End-to-end tests across real HTTP boundaries (spec sections 4.1 and 13.1), and
the harness they are written with. Today the package holds the harness library
(`src/`) and its own tests (`tests/harness.rs`). Product end-to-end tests come
once the publisher and the query server exist.

## Topology a product test builds

```text
node double ──JSON-RPC──▶ tachyon-pir-publisher ──plain HTTP──▶ TLS edge ──HTTPS──▶ tachyon-pir-query-server ◀── test client
(in process or           (spawned; management +                (in process)         (spawned; trusts the edge
 spawned binary)          one listener per product)                                   through publisher_ca)
```

Each piece is a separate process or listener, joined only by real HTTP(S).
The oracle is computed from the chain the test authored (the `SyntheticChain`
or `ChainSpec` the node double serves), never from the publisher's tables.

## Modules

| Module | What it gives a test |
|---|---|
| `tls_edge` | `TlsEdge`: HTTPS reverse proxy with a generated certificate, discovery routing, faults, outage, counting |
| `process` | `Process`: spawned binary, JSON config in a temp dir, logs, process-group kill, restart, readiness; `free_port`, `free_addr`, `free_addrs` |
| `node` | `start_node` (in-process `NodeDouble`), `SpawnedNode` (the binary from a `ChainSpec`), `rpc` |
| `client` | `trusting(pem)`, `plain()`, `get`, `get_json`, `get_bytes`, `post_bytes`, `post_octets`, `post_json`, `Reply` |
| `budget` | `budget::e2e(async { .. })`, `Budget`: the 25 s wall-clock guard |

### TLS edge

```rust
let edge = TlsEdge::builder()
    .management(publisher_management)          // "127.0.0.1:<port>"
    .product("memo", memo_listen)              // memo and first-contact share a listener
    .product("first-contact", memo_listen)
    .product("tachygram", tachygram_listen)
    .prefix("/registry/v1/", registry_listen)  // the write API, if a test needs it
    .start().await?;
edge.origin();   // "https://localhost:<port>"  -> query host `publisher`
edge.ca_path();  // PEM file                   -> query host `publisher_ca`
```

Routing: `/.well-known/pir-service-discovery` goes to the management upstream
(the publisher serves discovery only there, spec 11.6);
`/provider/<product>/v1/...` goes to that product's upstream, and a product
without one is `404`; other paths go to the longest matching `prefix` route;
everything else is `404` from the edge. An upstream that does not answer is
`502`. Bodies stream in both directions; client connections are HTTP/1.1
keep-alive (`edge.connections()` counts TLS handshakes). The edge overwrites
`X-Forwarded-For` with the peer address, as spec 9.3 requires of the proxy in
front of the registry write API.

**Certificate and trust.** `EdgeCertificate::generate()` makes an ECDSA P-256
self-signed leaf with rcgen (ring), subject `CN=localhost`, SANs `localhost`
and `127.0.0.1`, `basicConstraints CA:FALSE`: the shape of the openssl
certificate `wallet-pir`'s tests use. The leaf is its own trust anchor. The
edge serves it with rustls 0.23 (ring provider, ALPN `http/1.1`) and writes the
PEM to a file (a temp dir it owns, or `.cert_path(..)`). `edge.client()` and
`client::trusting(pem)` trust that certificate and no built-in root.
`pir_sync_manager::HttpsSource::new(&edge.origin(), "/provider/<p>/v1/", budget,
&[reqwest::Certificate::from_pem(&read(edge.ca_path()))])`, which is how the
query host builds its source, accepts it (`tests/harness.rs` proves discovery
and object fetches through it, and that it fails without the root or with
another edge's).

**Faults**, switched at run time. `edge.inject(target, fault)` lasts until
`edge.remove(id)` or `edge.clear_faults()`. `edge.inject_times(target, fault, n)`
applies to the next `n` matching requests. Faults apply in injection order;
`Delay` and `Hold` let later faults of the same request apply too.

| `Target` | Matches |
|---|---|
| `Any` | every request |
| `Discovery` | the discovery path |
| `Objects` / `ProductObjects(p)` | `/provider/*/v1/objects/...` / of product `p` |
| `Product(p)` | everything under `/provider/p/v1/` |
| `Path(x)` / `Suffix(s)` | exactly `x` / paths ending in `s` (`/current`, `/authority/check`) |

| `Fault` | Effect |
|---|---|
| `CorruptBody` | forward, flipping the low bit of the body's first byte (same length) |
| `TruncateBody` | forward the head and the first half of the body, then abort the connection |
| `Status(404)` (`Fault::not_found()`) | answer 404 without contacting the upstream (an object goes missing) |
| `Status(502)` (`Fault::bad_gateway()`), `Status(503)` | refuse |
| `Reset` | reset the TCP connection (`SO_LINGER` 0) before any response |
| `Delay(d)` | wait `d`, then continue |
| `Hold` | wait until the fault is removed; `edge.held()` counts waiting requests |
| `DiscoveryWrongMajor` | `200` with a discovery document of the next major version (the query host must exit) |
| `DiscoveryMalformed` | `200` with a body that is not a discovery document |

**Outage.** `edge.outage().await` stops accepting and drops every open
connection, held requests included. `edge.resume().await` listens again on the
same port, so the query host's configured origin keeps working. Counts and
faults survive the outage.

**Counting.** Every request is recorded on arrival, faulted and unrouted ones
included, by path without the query: `count(path)`, `count_matching(&Target)`,
`counts()`, `requests()`, `reset_counts()`. For "only changed objects are
fetched" (scenario 3), call `reset_counts()` after the first sync, publish, wait
for the query host to switch, then assert on `count_matching(&Target::Objects)`
and the per-path counts.

### Processes

```rust
let mut publisher = Process::new("publisher", binary!("tachyon-pir-publisher-test"));
let state = publisher.state_dir("tachygram");        // absolute, survives restarts
publisher.config("--config", "publisher.json", &json!({ .. }));
publisher.start();
publisher.wait_ready(&management, budget.remaining()).await;
publisher.restart();                                 // same args, config and state dirs
publisher.write_json("publisher.json", &changed);    // then restart(): config mutation
publisher.terminate(Duration::from_secs(10)).await;  // SIGTERM to the group, wait
```

- Each `Process` owns a fresh temp dir (`dir()`, `path(..)`, `state_dir(..)`).
  The config is written there and passed as `<flag> <path>`.
- Each run's stdout and stderr go to `<name>-<run>.stdout.log` and
  `.stderr.log`. All runs' logs are printed to stderr if the test panics while
  the `Process` is in scope, and are included in every harness panic message.
- The child leads its own process group. `kill()`, `restart()` and drop send
  `SIGKILL` to the whole group and reap the child.
- `wait_ready(addr, timeout)` polls `GET http://<addr>/ready` every 50 ms until
  `200`. It fails at once with the logs if the process exits.
  `wait_stdout_line`, `wait_exit` and `assert_running` cover the rest.
- `free_addrs(n)` reserves `n` distinct `127.0.0.1` ports at once (bind `:0`,
  read, release). `run_registered` refuses port 0, so the publisher's
  `management_listen` and product `listen` addresses come from here. Another
  process can take a port between release and use; tests tolerate that race,
  as `wallet-pir`'s do.

**Locating binaries.** `CARGO_BIN_EXE_<name>` is set only for binaries of the
test's own package. This package therefore re-declares foreign entrypoints as
its own `[[bin]]` targets (see `Cargo.toml`), as `wallet-pir/tools/pir-e2e-tests`
does. Cargo builds them before the tests and `binary!("<name>")` reads the
variable at the test's compile time. A run-time `TACHYON_E2E_BIN_<NAME>`
(upper case, `-` as `_`) overrides the path. The `-test` suffix keeps each
output file apart from the owning package's binary in workspace builds.
Present: `tachyon-node-double-test` (`../fixtures/src/bin/tachyon-node-double.rs`).

Adding a host: declare `[[bin]] name = "tachyon-pir-publisher-test"`,
`path = "../../src/tachyon-pir-publisher/src/main.rs"`, `test = false`. Add the
host crate's dependencies its `main.rs` uses. Mirror its product features
(`memo`, `first-contact`, `tachygram`, `registry`) here, because the
`#[cfg(feature = ..)]` in that `main.rs` resolve against this package's
features. Use `required-features` if a host needs a feature to build.

### Node double

- `start_node(chain).await` serves a `SyntheticChain` in process. `node.chain()`
  is the ground truth, and test edits through it (append, reorg) are served next.
  `node.outage()` and the other fault hooks are in `tools/fixtures/README.md`.
- `SpawnedNode::spawn(binary!("tachyon-node-double-test"), &spec, timeout)`
  runs the binary on a reserved fixed port, so `url()` survives `restart()`.
  `chain()` is the same spec built in this process. `control("append" |
  "reorg" | "outage" | .., body)` and `state()` drive its control endpoints;
  apply the same edits to the local chain to keep the oracle in step. A
  restart rebuilds the chain from the spec, so control edits are lost.
- `rpc(url, method, params)` makes one JSON-RPC call and returns `result`.

### Budget

Every end-to-end test body runs inside `budget::e2e(async { .. }).await`
(25 s, `DEFAULT_BUDGET`). Use `Budget::start()` when a test needs
`remaining()` as the deadline of its waits. The guard fails the test with its
name and elapsed time, before `tools/ci/test_budget.py` kills it at 30 s.

## How a product end-to-end test is written

```rust
#[tokio::test(flavor = "multi_thread")]
async fn tachygram_cold_bootstrap() {
    let budget = Budget::start();
    budget.run(async {
        // 1. Chain and node double: the ground truth.
        let node = start_node(chain_with_tachygrams()).await;
        // 2. Publisher on reserved fixed ports.
        let ports = free_addrs(2);
        let (management, tachygram) = (ports[0], ports[1]);
        let mut publisher = Process::new("publisher", binary!("tachyon-pir-publisher-test"));
        let state = publisher.state_dir("tachygram");
        publisher.config("--config", "publisher.json", &json!({
            "management_listen": management.to_string(),
            "chain": {"rpc_url": node.url(), "no_auth": true, "poll_ms": 100, "epoch_length": 16},
            "tachygram": {"enabled": true, "settings": {"listen": tachygram.to_string(), "state_dir": state}},
        }));
        publisher.start();
        publisher.wait_ready(&management.to_string(), budget.remaining()).await;
        // 3. TLS edge in front of it.
        let edge = TlsEdge::builder()
            .management(management.to_string())
            .product("tachygram", tachygram.to_string())
            .start().await.unwrap();
        // 4. Query server trusting the edge.
        let ports = free_addrs(2);
        let (qmanagement, public) = (ports[0], ports[1]);
        let mut query = Process::new("query", binary!("tachyon-pir-query-server-test"));
        let qstate = query.state_dir("tachygram");
        query.config("--config", "query.json", &json!({
            "management_listen": qmanagement.to_string(), "public_listen": public.to_string(),
            "tachygram": {"enabled": true, "settings": {
                "publisher": edge.origin(), "publisher_ca": edge.ca_path(), "state_dir": qstate,
                "role": "combined", "publisher_network": "test", "publisher_genesis": genesis_hex }},
        }));
        query.start();
        query.wait_ready(&qmanagement.to_string(), budget.remaining()).await;
        // 5. Look up through the public listener; compare with the oracle
        //    computed from node.chain().
    }).await
}
```

## What the harness assumes of the hosts

- Both binaries take `--config <FILE>` (JSON, unknown fields refused) and log to
  stdout or stderr.
- Readiness is `GET /ready` on the `management_listen` address, plain HTTP,
  `200` when ready. For the query host, that means every enabled product
  serves. For the publisher, it is `run_registered`'s management listener.
- The publisher serves `/.well-known/pir-service-discovery` on its management
  listener and `/provider/<product>/v1/{current,objects/<sha256>,authority/check}`
  on each product's `listen` address (memo and first-contact on the memo
  listener). Product listeners and `management_listen` are fixed `127.0.0.1`
  ports from `free_addrs`.
- The query host's `publisher` is `edge.origin()` (`https://localhost:<port>`,
  no path) and `publisher_ca` is `edge.ca_path()`.
- `SIGTERM` starts a graceful stop, used by `terminate`. `SIGKILL` of the
  process group is the crash used by `restart` and drop.

## Tests

`cargo test -p tachyon-pir-e2e-tests --profile release-fast`. `tests/harness.rs`
covers: GET and POST through the edge with a client that trusts only the
generated certificate; discovery routing and `404`s; streaming and keep-alive;
every fault; outage and resume on the same port; per-path counting;
`HttpsSource` with the edge as `publisher_ca` (discovery, wrong major,
malformed, untrusted); the `tachyon-node-double-test` binary spawned, queried,
restarted and gone after drop; exit and readiness waits; port reservation;
the budget guard.
