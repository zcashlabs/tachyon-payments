# Registry architecture

Status: specified, not implemented.

```
wallets ──POST /registry/v1/register──▶ tachyon-pir-publisher (registry-publisher)
                                              │  fsync
                                              ▼
                                        registry-store (registry-v1.sqlite)
                                              │
                                              └─▶ tachyon-pir-table ─▶ objects ─▶ /provider/registry/v1/
                                                                                       │
                                     tachyon-pir-query-server (feature registry) ◀─────┘
```

## Crates

| Crate | Path | Role |
|---|---|---|
| `registry-store` | `src/registry/registry-store` | The append-only registration log |
| `registry-publisher` | `src/registry/registry-publisher` | Write API handlers, abuse controls, `Publication` and `Publisher` |
| `tachyon-pir-query-server` | `src/tachyon-pir-query-server` | In-crate `registry` serving module, feature `registry` |

The registry crates may not depend on the memo or tachygram crates; no serving
crate may depend on them. The registry has no chain source and does not use
`tachyon-chain` for ingestion.

## Log (spec 9.4)

`registry-store` keeps `registry-v1.sqlite`: an append-only table of
`(sequence, digest, addr)` with a unique index on `digest`. Chunk membership is
`sequence / REGISTRY_CHUNK_ENTRIES`. A backup of this file is a complete backup
of the registry.

## Single writer

The registry has one writer, so exactly one publisher runs per network
(spec 4.1). The write API runs in the publisher process on a public listener
separate from its management listener.

## Publication

- Full chunks are sealed immediately and built once; the open chunk is rebuilt
  at each publication that changed something.
- Objects under `/provider/registry/v1/`, schema
  `tachyon-registry-prepared-distribution-v1`.
- There are no reorgs. A publisher restart still starts a new recovery epoch, as
  `PublicationAuthority` requires (spec 11.4).

## Abuse controls

Proof of work, per-client concurrency (`pir_control::ClientSlots`), per-client
and global rate limits, an exact body limit and an upload deadline; values in
[deployment](deployment.md).
