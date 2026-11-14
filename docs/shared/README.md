# Shared libraries and the common design

Status: specified, not implemented, except where noted. The contract is the
[spec](../spec/spec-tachyon-payment-pir.md); section numbers below refer to it.

All four products use one table design, one wire protocol shape and one
publication model. What differs per product is the entry layout, the scope rule
and the source of entries; those are in each product's docs.

## Crates

| Crate | Path | Purpose | Spec |
|---|---|---|---|
| `tachyon-chain` | `src/shared/tachyon-chain` | Wraps `pir_node_rpc::RpcClient`. Decodes `getblock <hash> 2` into `TachyonBlock`, does epoch arithmetic, reads network identity (`chain`, `genesis_hash`, NuTachyon `activation_height`) | 3, 11.1 |
| `tachyon-pir-protocol` | `src/shared/tachyon-pir-protocol` | The wallet-facing contract: constants, row layouts, placement hashing, map and header types. No server dependencies | 6, 10, 15 |
| `tachyon-pir-table` | `src/shared/tachyon-pir-table` | Builds, validates, segments and digests bucket-row tables | 6 |

`tachyon-pir-protocol` and `tachyon-pir-table` are serving code and may not
depend on `tachyon-chain` or any ingestion crate (`tools/ci/boundary.py`).

## Chain decoding (11.1)

```
NodeClient::network_identity() -> NetworkIdentity { chain, genesis_hash, activation_height }
NodeClient::tip() -> BlockId
NodeClient::block_hash(height) -> Hash
NodeClient::block(hash) -> TachyonBlock
```

- Blocks are fetched by hash after resolving the height once.
- A block whose hash, height or transaction ids differ from the request is
  rejected.
- `activation_height` comes from `getblockchaininfo.upgrades` under the name
  `NuTachyon`; without it the publisher refuses to start.
- `pool_height = height - activation_height`; `epoch = pool_height / 4096`.
- Block hashes and txids are held in protocol order (reversed RPC hex) and
  written in display order in JSON. Tachygrams, anchors, tags and digests are
  never reversed (section 3).

## The bucket-row table (6)

| Geometry | Rows | Row bytes | Raw size |
|---|---:|---:|---:|
| `tachyon-4k8` | 4,096 | 8,192 | 32 MiB |
| `tachyon-2k8` | 2,048 | 8,192 | 16 MiB |

- A row holds `SLOTS = floor(8192 / ENTRY_BYTES)` entries. Occupied entries come
  first, sorted by `(key, ordinal)`; the rest of the row is zero. Any other row is
  non-canonical, and publisher, query host and client each validate it.
- A scope (epoch, span or chunk, per product) is split into `n_buckets` tables,
  `n_buckets ∈ {1, 2, 4, …, 64}`, the smallest power of two that keeps the
  expected load per row at or below the product's target.
- Placement:

  ```
  h      = SHA-256(domain || genesis_hash(32) || scope_id(u64) || seed(u32) || key)
  bucket = u32_le(h[4..8]) mod n_buckets
  row    = u32_le(h[0..4]) mod rows
  ```

- The publisher uses the smallest `seed` in `0..64` with no overflowing row. If
  none works, it places entries in ordinal order under seed 0 and moves the rest
  to further segments, at most `MAX_SEGMENTS = 8` per bucket. Entries are never
  evicted.
- A scope is open while it can change and is rebuilt at every publication. It is
  sealed `SEAL_DEPTH = 100` blocks after its last block (registry chunks seal
  when full) and is then immutable. Sealed table bytes are a pure function of the
  chain and the constants.
- PIR profile: `reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1`, CPU only. No
  per-client keys are stored; public setup material is published per table.

## Wire protocol shape (10)

Each product is mounted at `/<product>/v1` with a legacy alias `/v1/<product>`,
and serves `GET map`, `GET map/{base}/{sha256}`, `GET public/{session_id}` and
`POST query`. The map head names the product's `protocol` string, the network,
the anchor and the scopes; each scope lists one table per `(bucket, segment)`.

```
session_id = SHA-256("tachyon-pir/v1/session\0" || protocol_len(u8) || protocol
                     || scope_id(u64) || bucket(u32) || segment(u32)
                     || rows(u32) || seed(u32) || rows_sha256 || public_sha256)
query body = magic(4) || session_id(32) || nonce(16) || payload
```

The 52-byte header is echoed before the packed answer. Errors use
`{"code": "...", "message": "..."}` with the codes of section 10.5. Admission
(10.6) starts from Receiver's values: 2 executing, 8 waiting, 2 s wait, 2 in
flight per client.

## Publication and serving (11)

- The publisher writes table rows, public setup material and map chunks as
  immutable objects addressed by SHA-256 and length, then advances one
  `PublicationEnvelope` atomically (`pir-publication`, unchanged). It never
  waits for consumers.
- Distribution prefixes are `/provider/<product>/v1/` with `current`,
  `objects/<sha256>` and `authority/check`; the schema string is
  `tachyon-<product>-prepared-distribution-v1`.
- Chain products ingest to `tip - CONFIRMATION_DEPTH` (2). A rewind inside open
  scopes is a new generation; a rewind into a sealed scope increments
  `recovery_epoch` and revokes the affected sessions (`410`). A publisher restart
  always starts a new recovery epoch.
- The query host runs one `ConsumerEngine` per product, verifies every object,
  prepares runtimes before switching publications, keeps the displaced
  publication 600 s, and answers `503` for a product until it is ready. It
  serves the newest `retain_scopes` scopes per product.

## Reused from wallet-pir (4.3)

Pinned to `40b0ba6a`: `pir-publication`, `pir-pipeline`, `pir-sync-manager`,
`pir-service-discovery`, `pir-control`, `pir-observability`, `pir-node-rpc`,
`pir-publisher`. From crates.io at `=0.0.1-rc5`: `zakura-pir-protocol`,
`zakura-pir-publisher`, `zakura-pir-query-server`, with the `ipir-sp` patch
block. See [Integrating with wallet-pir](wallet-pir-integration.md) for how the
crates use them.
