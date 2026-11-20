# tachyon-pir-e2e-tests

End-to-end tests across real HTTP boundaries (spec sections 4.1 and 13.1), and
the harness they are written with: the harness library (`src/`), its own tests
(`tests/harness.rs`) and the product acceptance suites (`tests/tachygram.rs`,
`tests/memo.rs`, `tests/first_contact.rs`, `tests/registry.rs`; spec 13.2).

## Features

| Feature | Default | Effect |
|---|---|---|
| `tachygram` | on | builds the re-declared hosts with `tachyon-pir-publisher/tachygram` and `tachyon-pir-query-server/tachygram`, and enables `tests/tachygram.rs` (`required-features`) |
| `memo` | on | the same with `tachyon-pir-publisher/memo` and `tachyon-pir-query-server/memo`; `tests/memo.rs` needs `memo` and `first-contact` |
| `first-contact` | on | implies `memo` (first-contact runs inside the memo publisher, spec 11.7); adds both hosts' `first-contact` and enables `tests/first_contact.rs` |
| `registry` | on | the same with `tachyon-pir-publisher/registry` and `tachyon-pir-query-server/registry`; enables `tests/registry.rs` |

Every product feature is a default, so `cargo test --workspace` runs every
product's acceptance scenarios.

## Topology a product test builds

```text
node double ──JSON-RPC──▶ tachyon-pir-publisher-test ──plain HTTP──▶ TLS edge ──HTTPS──▶ tachyon-pir-query-server-test ◀── probe client
(in process or           (spawned; management +                (in process)         (spawned; trusts the edge
 spawned binary)          one listener per product)                                   through publisher_ca)
```

Each piece is a separate process or listener, joined only by real HTTP(S).
The oracle is computed from the chain the test authored (the `SyntheticChain`
or `ChainSpec` the node double serves), never from the publisher's tables.

For memo and first-contact the publisher runs **one** definition with both
products enabled: one listener serves `/provider/memo/v1/`,
`/provider/first-contact/v1/` and `/metrics`, and the edge maps both prefixes
to it (`TopologyOptions::new` with either product sets `publish` to both). The query host serves the product under test, or several
(`also_serve`), each with its own state directory.

## Modules

| Module | What it gives a test |
|---|---|
| `tls_edge` | `TlsEdge`: HTTPS reverse proxy with a generated certificate, discovery routing, faults, outage, counting |
| `process` | `Process`: spawned binary, JSON config in a temp dir, logs, process-group kill, restart, readiness; `free_port`, `free_addr`, `free_addrs` |
| `node` | `start_node` (in-process `NodeDouble`), `SpawnedNode` (the binary from a `ChainSpec`), `rpc` |
| `client` | `trusting(pem)`, `plain()`, `get`, `get_json`, `get_bytes`, `post_bytes`, `post_octets`, `post_json`, `Reply` |
| `budget` | `budget::e2e(async { .. })`, `Budget`: the 25 s wall-clock guard |
| `topology` | `Topology` (node double, publisher, TLS edge and query host of the product under test, from `TopologyOptions`), `PublisherHost` and `QueryHost` (one or more products each), `Distribution` (one product's publisher routes), `ProductNames`, `wait_for`, `wait_map`, `wait_limit`, `metric_value`, `exclusive` |
| `oracle` | `TachygramOracle`, `EpochSchedule`, `ExpectedScope`, `Sampler`: expected answers computed from the authored chain only |
| `memo_oracle` | `MemoOracle`, `FirstContactOracle` (`EnvelopeOracle<T>`), `SpanTruth`, `ScopeTruth`, `Family`: memo and first-contact answers, scope listings and spans from the authored envelopes only |
| `memo_checks` | maps, listings and lookups of those two products judged against `memo_oracle` (`check_map`, `check_listing`, `check_lookups`, `judge`, `probes`, `spawn_prober`), and the scenario 6 key grinder |
| `registry_topology` | `RegistryTopology` (node double for the identity, publisher with the registry, TLS edge, query host, from `RegistryOptions`), `RegistryPublisher`, `RegistryQueryHost`, `RegistrySettings`, the write-API client `WriteApi` (`challenge`, `register`, `register_all`, `post`), `HeldUpload` (a registration held open after `100 Continue`), `register_body`, `solve`, `unsolved`, `parse_ack`, `assert_refusal`, `wait_map`, `wait_sequence` |
| `registry_oracle` | `RegistryOracle`, `AddressGen`, `OracleAddress`, `invalid`, `Ack`, `ExpectedChunk`: the test's record of what it registered, digests from spec 9.1 with `sha2`, `pk` and `ek` checks, chunk structure |
| `registry_checks` | registry maps, chunk listings and lookups judged against `registry_oracle` (`check_map`, `check_listing`, `check_lookups`, `judge`, `hits`, `misses`, `sample`, `spawn_prober`), and the scenario 6 address grinder |

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
- `free_port()`, `free_addr()` and `free_addrs(n)` reserve fixed `127.0.0.1`
  ports for listeners bound later. `run_registered` refuses port 0, so the
  publisher's `management_listen` and product `listen` addresses come from
  here, and so do the query host's.
  - Ports come from outside the kernel's ephemeral range
    (`/proc/sys/net/ipv4/ip_local_port_range`). The pool is the larger of
    `20000..low` and `high+1..=65535`. Nothing that binds `:0` or connects out
    can take a reserved port between reservation and use.
  - Within one test process, an atomic counter walks the pool from a random
    per-process start, so reservations are distinct until the pool (over
    10,000 ports) wraps.
  - Across processes, a reserved port is locked until the reserving process
    exits: a POSIX record lock on byte `port` of
    `<temp dir>/tachyon-pir-e2e-ports.lock`. The kernel drops the locks when
    the process exits. Other test processes skip the port even before anything
    binds it.
  - Each candidate is probed by binding it on `127.0.0.1` with
    `SO_REUSEADDR` and **without listening**; a port anything else holds is
    skipped. A listening probe would leak: `Command::spawn` on another thread
    copies the descriptor table, and until the child's exec closes it, the
    probe still listens and an immediate bind of the port fails.
  - `TACHYON_E2E_PORT_START` (a fixed pool offset) and
    `TACHYON_E2E_PORT_LOCKS` (a lock-file path) exist for the harness's own
    exclusivity test.

**Locating binaries.** `CARGO_BIN_EXE_<name>` is set only for binaries of the
test's own package. This package therefore re-declares foreign entrypoints as
its own `[[bin]]` targets (see `Cargo.toml`), as `wallet-pir/tools/pir-e2e-tests`
does. Cargo builds them before the tests and `binary!("<name>")` reads the
variable at the test's compile time. A run-time `TACHYON_E2E_BIN_<NAME>`
(upper case, `-` as `_`) overrides the path. The `-test` suffix keeps each
output file apart from the owning package's binary in workspace builds.
Present:

| Binary | Source |
|---|---|
| `tachyon-node-double-test` | `../fixtures/src/bin/tachyon-node-double.rs` |
| `tachyon-pir-publisher-test` | `../../src/tachyon-pir-publisher/src/main.rs` |
| `tachyon-pir-query-server-test` | `../../src/tachyon-pir-query-server/src/main.rs` |

The host crates and the dependencies their `main.rs` use are dependencies of
this package. Their product features are mirrored here (see Features),
because the `#[cfg(feature = ..)]` in those `main.rs` files resolve against
this package's features.

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

### Memo and first-contact oracle

`MemoOracle::new(&chain, epoch_length)` and `FirstContactOracle::new(..)` read
only the fixtures' envelope ground truth: `chain.follow_ups(..)` /
`chain.first_contacts(..)` (valid envelopes' entries, ordinal order) and each
block's `envelope_counts()`. They depend on no protocol, table, store or
publisher crate and parse no bytes. From that and their own arithmetic:

- `scopes(anchor)`: every scope a map at `anchor` lists, with `scope_id`,
  heights, entries, `n_buckets` (smallest power of two with
  `entries <= target_load * 4096 * n`, target 34 for memo and 1 for
  first-contact) and state (`last_height + 100 <= anchor`). Memo scopes are
  epochs, the newest ending at the anchor.
- `spans(anchor)` (first-contact): the spec 7.2 rule recomputed from per-epoch
  counts of the blocks a journal holds at `anchor` (heights up to the anchor).
  A boundary is decided once an epoch's last block is held; the open span
  closes there when it holds at least 3,072 entries or covers 64 epochs; the
  next span exists once a block of its first epoch is held. `scope_id` is the
  first epoch, the index the span's position.
- `expected(index, key16, anchor)`: every entry with that key at a covered
  height, in ordinal order; empty means absent.
- `envelope_counts(through)`: malformed envelopes and wallet-state entries of
  the stored blocks, for `tachyon_memo_envelopes_invalid_total` and
  `tachyon_memo_state_entries_total`.

The scenario 6 grinder (`memo_checks::grind_overflow`) uses the protocol
crate's placement to find, for each of the 64 seeds, `SLOTS + 1` keys sharing a
row; the answers expected for them still come from the oracle.

### Registry topology and oracle

The registry has no chain source: its log grows only through the write API
(spec 9.3). `RegistryTopology::launch` serves a genesis-only `SyntheticChain`
from an in-process node double (the publisher reads the network identity
from it once at start-up, spec 9.2), starts the publisher with only the
registry enabled (`RegistrySettings`; `RegistrySettings::bulk()` is a 4-bit
proof of work, limits far above any test's volume and `publish_ms` 100),
fronts it with the edge (discovery to management; `/provider/registry/v1/`
and `/registry/v1/` to the registry listener) and configures the query host
(no `activation_height` or `epoch_length`, spec 11.7).

Write-API traffic goes **directly to the registry listener with an
`X-Forwarded-For` the test sets** (`t.write(ip)`, `WriteApi::as_client`):
the test plays the reverse proxy spec 9.3 requires, so a test chooses which
client a request comes from. `t.write_via_edge()` goes through the edge's
`/registry/v1/` route instead; the edge overwrites `X-Forwarded-For` with
the peer, so every request through it is one client.

`RegistryOracle` is the test's own record of what it registered: each
address it generated (`AddressGen`: `pk` below `2^254 < p`, `ek` with 768
coefficients uniform below 3,329 and a random `ρ`), its digest computed with
`sha2` from spec 9.1, and the acknowledged sequence and chunk.
`record(address, ack)` checks each acknowledgement (digest, `chunk =
sequence / 3072`, a repeat `created: false` at the same sequence, no
sequence twice) and `assert_contiguous` that the sequences are `0 .. n`.
From that alone: `chunks(log_sequence)` (sealed chunks of exactly 3,072,
one open chunk `log_sequence / 3072`) and `expected(chunk, key16,
log_sequence)` (the address, or absent). It depends on no protocol, store or
publisher crate. The scenario 6 grinder (`registry_checks::grind_overflow`)
varies only `ρ` and uses the protocol crate's placement; `solve` uses its
`solve_pow`.

### Budget

Every end-to-end test body runs inside `budget::e2e(async { .. }).await`
(25 s, `DEFAULT_BUDGET`). Use `Budget::start()` when a test needs
`remaining()` as the deadline of its waits. The guard fails the test with its
name and elapsed time, before `tools/ci/test_budget.py` kills it at 30 s.
With `TACHYON_E2E_TIMINGS=<file>` set, each guard that completes appends
`<test binary> <test name> <seconds>` to the file: the in-test time it
measured, for reports.

### Exclusivity

`topology::exclusive().await` is taken by every host-spawning test before its
budget starts, and held for the test: an in-process mutex, because a binary's
tests run in parallel threads. `cargo test` runs test binaries one after
another, so that is all it needs.

With `TACHYON_E2E_EXCLUSIVE_LOCK=<file>` the guard also holds an exclusive
`flock` on that file, so heavy tests run one at a time across every e2e test
binary started with the same path, for a runner that starts binaries in
parallel. It is opt-in because waiting for the lock is part of a test's wall
time as libtest and `tools/ci/test_budget.py` measure it: that script runs
binaries in parallel (`--jobs`) and times each test from its start, so a test
queued behind another binary's would reach the 30 s limit. The kernel drops
the lock when the process exits; the file is opened close-on-exec, so spawned
hosts do not inherit it.

## How a product end-to-end test is written

`topology::Topology` assembles the whole topology of spec 13.1 for one product.
`tests/tachygram.rs` is the worked example.

```rust
#[tokio::test(flavor = "multi_thread")]
async fn cold_bootstrap() {
    let _serial = exclusive().await;          // one host-spawning test at a time
    let budget = Budget::start();
    budget.run(async {
        let chain = authored_chain();          // SyntheticChain: the ground truth
        let oracle = TachygramOracle::new(&chain, 16);
        let t = Topology::start(
            chain,
            TopologyOptions::new(ProductNames::TACHYGRAM, 16),
            binary!("tachyon-pir-publisher-test"),
            binary!("tachyon-pir-query-server-test"),
            &budget,
        ).await;
        // t.node (in-process NodeDouble), t.publisher (PublisherHost),
        // t.edge (TlsEdge), t.query (QueryHost, wallet() is the probe client).
        // Wait with wait_map / wait_for inside wait_limit(&budget); compare
        // every answer with the oracle.
    }).await
}
```

`Topology::launch` does the same without starting the query host. Use it when a
test injects edge faults before the first sync.

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
restarted and gone after drop; exit and readiness waits; port reservation
(8 threads x 64 concurrent reservations, all distinct, bindable and outside
the ephemeral range while other threads bind `:0`; two processes walking the
same ports get disjoint ones, and a dead process's ports are released); the
budget guard.
`tests/tachygram.rs` holds the tachygram acceptance scenarios of spec 13.2.
`tests/memo.rs` (16-block epochs) and `tests/first_contact.rs` (2-block epochs,
so a span closes by the 64-epoch rule after 128 blocks; its standard chain
also closes a span by count with 3,106 entries) hold scenarios 1-13 for memo
and first-contact: no consumers (with the published span listing checked
against the oracle at each anchor through closes and seals), cold bootstrap
with exact answers and repeated tags (scenarios 2, 4, 5), incremental
publication, forced overflow with ground keys, every `Malformation` plus
hand-authored invalid envelopes with valid companions (skipped and counted,
ingestion continues), shallow reorg (for first-contact one that removes an
unsealed span boundary), reorg into a sealed scope (`410`), corrupt and
truncated objects, publisher outage, restart of each process, and overload.
`tests/registry.rs` holds scenarios 1-4, 6 and 10-14 for the registry
(milestone M3): no consumers (the empty log, then each batch published with
the listing checked), cold bootstrap with exact answers (one registration
through the edge), a sealed chunk of 3,072 registrations with the empty open
chunk answering absent without a query, then incremental publication across
both chunks, forced overflow with ground addresses, corrupt and truncated
objects, publisher outage, restart of each process (the publisher killed
right after a `200`), overload, and the writes: validation (challenge
format and HMAC, forged, re-dated and expired challenges, proof of work and
its counter, `pk` and `ek` boundaries, the order of the checks, body length,
routes), idempotence, the abuse controls (per-client concurrency through
held uploads, per-client rate, uploads that time out, the global cap, each
with `Retry-After` and another client unaffected), visibility at
`visible_after_sequence` and durability across `SIGKILL`.
