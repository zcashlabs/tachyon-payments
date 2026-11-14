# Registry

Status: specified, not implemented (milestone M3). Nothing is deployed.

The registry is the off-chain address registry (PKI): `H(addr) => addr`. A
recipient registers its full address once and shares a short address,
`(chunk_id, digest)`. A sender looks the full address up with one PIR query and
checks it by recomputing the digest. It is the only product that takes writes
and the only one with no chain source.

| | |
|---|---|
| Address | `pk(32) \|\| ek(1184)`, 1,216 bytes (ML-KEM-768, provisional) |
| Digest | `SHA-256("tachyon-pir/registry/v1/addr\0" \|\| addr)`; `key16 = digest[0..16]` |
| Scope | a **chunk** of 3,072 registrations in log order, `scope_id = chunk_id` |
| Entry | `key16 \|\| pk(32) \|\| ek(1184)`, 1,232 bytes, 6 per row, target load 1 |
| Protocol string | `tachyon-registry-pir-v1` |
| Query magic | `TRQ1` |
| Mount | `/registry/v1`, legacy alias `/v1/registry` (query host for lookups; publisher's public listener for writes) |
| Crates | `registry-store`, `registry-publisher` (`src/registry/`); serving module in `tachyon-pir-query-server`, feature `registry` |
| Default retention | unlimited |

A lookup reveals the chunk, so a query's anonymity set is at most 3,072
addresses (decision D3). Registration reveals the address and the registrant's
network identity to the publisher; a recipient who wants to hide that link
registers through an anonymising transport. The registry is public data:
anyone can download every address by fetching the objects.

- [Protocol](protocol.md): address, chunks, write API, map and query.
- [Architecture](architecture.md): the log, the write path and publication.
- [Deployment](deployment.md): configuration, abuse controls and metrics.
- [Qualification](qualification.md): the release gate and its status.
- [Changelog](CHANGELOG.md).

Spec sections: 6 (table), 9 (registry), 10, 11, 12.4, 13; decisions D3 and D4.
