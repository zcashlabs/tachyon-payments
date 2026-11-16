# tachyon-pir-cli

The diagnostic probe client of the Tachyon payment PIR services (spec
`docs/spec/spec-tachyon-payment-pir.md`, sections 2, 4.2 and 14 M1). A tool for
testing and diagnostics, and the "wallet" of the end-to-end tests. It is not
the wallet client library (spec 2, out of scope): it decrypts nothing and keeps
no sync state.

It speaks the wallet-facing protocol (spec 10) of all four products (memo,
first-contact, tachygram, registry) on one mount, `/<product>/v1` (or the
legacy `/v1/<product>`).

## Library

```rust
let client = Client::new("https://127.0.0.1:8443", Product::Tachygram, Some(&ca_pem))?;
let map: Arc<MapView> = client.fetch_map().await?;
client.accept_anchor(&map, "http://user:pass@127.0.0.1:18232").await?;   // optional, spec 12.1
match client.lookup(&map, ScopeSelector::Index(9), &tachygram).await? {
    LookupOutcome::Found { entries, answer } => { /* answer.coverage, answer.queries */ }
    LookupOutcome::Absent { answer } => { /* absent within answer.coverage only */ }
    LookupOutcome::BelowFloor { scope_index, floor_scope } => { /* spec 11.5 */ }
}
let answer = client.dummy_query(&map, ScopeSelector::Index(9)).await?;   // spec 12.3
```

- `fetch_map`: `GET map`; `X-Map-Sha256` must equal the SHA-256 of the body;
  the head is parsed in its product's form (chain or registry), validated, and
  must be the canonical compact encoding. Every sealed chunk is fetched with
  `GET map/{base}/{sha256}` unless cached by digest, checked against its digest,
  its position and the head, and must be canonical; then the whole listing is
  checked. A map naming another network or genesis than the first one the
  client accepted is refused. The host's `X-Floor-Scope` header is required
  (missing, malformed or above the newest scope is `FloorHeader`); the view's
  `floor_scope()` is the larger of it and the head's `floor_scope` (spec 11.5),
  and `host_floor_scope()` reports the header alone. Returns a `MapView` with every scope by index and
  by `scope_id`.
- `lookup(map, scope, key)`: `key` is the stored key (`key16`, or the full
  tachygram). A scope below the effective floor returns `BelowFloor` without a
  request. A scope with no tables returns `Absent` without a request.
  Otherwise the query plan (`ScopeTables::query_plan`) names the bucket under
  the scope seed and every segment of it with its own row. For each:
  `PirParams` (cached per product and geometry), `GET public/{session_id}`
  (cached by session id; length and `public_sha256` checked), `P8.prepare`,
  `POST query` with `magic || session_id || nonce(16 random) || payload` as
  `application/octet-stream`, `validate_response` (exact length, echoed
  header), `P8.decode_cols`, `validate_row` under the table's `RowContext`
  (tachygram heights included), and `lookup`. Results are concatenated in
  segment order, which is ordinal order (spec 6.4). A tachygram found more than
  once is an error (spec 8.4). Every answer carries the scope's coverage (heights,
  or registry sequences), the map digest and generation, and every table queried.
- `dummy_query(map, scope)`: a uniformly random bucket, then every segment of it
  at a uniformly random row: the same requests, in number, order, sessions and
  sizes, as a real lookup in that bucket. Rows are decoded and validated. A
  scope with no tables is `EmptyScope` (spec 10.3: query another scope).
- `accept_anchor(map, node_rpc_url)`: JSON-RPC `getblockhash 0` and
  `getblockhash anchor_height` must equal the map's `genesis_hash` and
  `anchor_hash`. Chain products only.
- Errors (spec 10.5), each with its parsed `{"code","message"}` body: 409
  `StaleRouting`, after which `lookup` and `dummy_query` refetch the map and
  retry exactly once (a second 409 is returned); 410 `SessionRevoked` (refetch
  the map and accept its anchor again); 429 `Overloaded` and 503 `Unavailable`
  with `Retry-After`; 400 and 413 `InvalidRequest`; 408 `UploadTimeout`. A
  status of the table with a body that is not the matching error body is
  `BadErrorBody`; other statuses are `UnexpectedStatus`. Nothing else is
  retried.
- `Client::stats()` counts requests per route; `current_map()` is the map last
  fetched, including one a 409 retry fetched.

Row validation cannot detect a tampered ciphertext byte inside an occupied
memo, first-contact or registry entry; the wallet authenticates those by
decryption or by recomputing the digest (spec 12.1).

## Binary

```
tachyon-pir-cli --origin URL --product memo|first-contact|tachygram|registry
                [--ca PEM] [--legacy-mount] [--node RPC_URL]
                <map | lookup (--scope N | --scope-id N) --key HEX | dummy (--scope N | --scope-id N)>
```

Prints one JSON document on stdout: the map summary, the lookup result
(`found` with entries in hex, `absent`, or `below_floor`) or the dummy answer,
plus `anchor` when `--node` is given and `requests` (counts per route). On
error it prints `{"error": {"kind", "message", ...}}` and exits 1. `--key`
accepts the stored key or, for the 16-byte products, the full 32-byte tag or
digest. `--scope` is the scope index (epoch for memo and tachygram, span
position for first-contact, chunk id for registry).

## Tests

`cargo test -p tachyon-pir-cli --profile release-fast`.

- `src/error.rs`: the spec 10.5 status and body mapping.
- `tests/offline.rs`: request body layout, head and chunk digest, header and
  canonical-encoding checks, floor refusal and empty scopes with zero requests,
  key and origin parsing.
- `tests/http.rs`: an in-test axum host serving real P8 runtimes built with
  `tachyon-pir-table`: a memo map with a sealed scope (one bucket, three
  segments from one key repeated 130 times) and an open scope, and a tachygram
  map. Every key of the input lists is found with exactly its input entries in
  ordinal order, misses are absent; tampered reply bytes, a tampered echoed
  header and wrong lengths are refused; 409 refetches once and retries once;
  410, 429, 503, 400 and 413 are typed; a public setup of the wrong digest or
  length is refused before any query; a dummy query sends the same sessions and
  sizes as a lookup; the anchor check; the binary's JSON output.
