# Documentation

The contract for everything in this workspace is the
[implementation specification](spec/spec-tachyon-payment-pir.md). Where these
documents and the spec disagree, the spec wins and the document is wrong.

## Specification

- [Tachyon payment-protocol PIR services](spec/spec-tachyon-payment-pir.md):
  scope, chain facts, architecture, the on-chain envelope, the bucket-row
  table, the four products, the wallet-facing protocol, publication and
  consistency, trust and privacy, testing, constants and open decisions.

## Products

Each product has the same six documents: `README.md` (what it is),
`protocol.md` (the wallet-facing contract), `architecture.md` (crates and data
flow), `deployment.md` (configuration and operation), `qualification.md` (the
spec 13.3 gate and its status) and `CHANGELOG.md`.

- [Memo](memo/README.md): `tag => memo`, one scope per epoch.
- [First-contact](first-contact/README.md): `tag => KEM.c || memo`, one scope per
  span of epochs; shares the memo crates.
- [Tachygram](tachygram/README.md): tachygram inclusion and exclusion, one scope
  per epoch.
- [Registry](registry/README.md): the address registry, `H(addr) => addr`, one
  scope per chunk of registrations, with a write API.

## Shared

- [Shared libraries and the common design](shared/README.md): `tachyon-chain`,
  `tachyon-pir-protocol`, `tachyon-pir-table`, the bucket-row table, the
  publication model and the query host.

## Elsewhere

- [Development rules and gates](../AGENTS.md).
