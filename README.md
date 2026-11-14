# Tachyon payment PIR

Private-retrieval services for the Tachyon payment protocol on Zcash. A wallet
uses them to fetch the data it needs to receive payments without telling the
server which entry it wants. Four products, each a set of PIR tables:

- **[Memo](docs/memo/README.md)**: the epoched memo database, `tag => memo`.
- **[First-contact](docs/first-contact/README.md)**: the first-contact memo
  database, `tag => KEM.c || memo`, over spans of epochs.
- **[Tachygram](docs/tachygram/README.md)**: the epoched tachygram database a
  wallet uses to locate its note commitment and to check that a nullifier is
  absent.
- **[Registry](docs/registry/README.md)**: the off-chain address registry,
  `H(addr) => addr`, with a rate-limited write API.

One publisher process follows the Tachyon testnet node, builds the tables and
publishes them as content-addressed objects; query servers pull those
publications and answer encrypted row queries.

```
Zakura node ──JSON-RPC──▶ tachyon-pir-publisher ──HTTPS pull──▶ tachyon-pir-query-server ◀── wallets
                              ▲
                              └── wallets (registry writes only)
```

**Status:** draft specification; implementation started at milestone M0 (workspace,
CI gates, chain decoder, node double). Nothing is deployed or qualified.

The contract is the [implementation specification](docs/spec/spec-tachyon-payment-pir.md).
The design mirrors [`wallet-pir`](https://github.com/valargroup/wallet-pir) at
revision `40b0ba6a` and reuses its shared libraries (`pir-publication`,
`pir-pipeline`, `pir-sync-manager`, `pir-control`, `pir-node-rpc` and others) as
pinned git dependencies.

## Layout

| Directory | Contents |
|---|---|
| `src/shared/` | `tachyon-chain` (node reads), `tachyon-pir-protocol` (wallet-facing contract), `tachyon-pir-table` (the shared bucket-row table) |
| `src/memo/`, `src/tachygram/`, `src/registry/` | Each product's store and publisher crates; first-contact uses the memo crates |
| `src/tachyon-pir-publisher`, `src/tachyon-pir-query-server` | The two binaries |
| `tools/` | Probe client (`tools/client`), end-to-end tests (`tools/e2e-tests`), node double and fixtures (`tools/fixtures`), CI gates (`tools/ci`) |
| `docs/` | [Documentation index](docs/README.md), the spec, per-product docs |
| `evidence/` | [Qualification and measurement records](evidence/README.md) |

## Build and test

Prerequisites: rustup with the toolchain pinned in `rust-toolchain.toml`
(1.97.1, with rustfmt and clippy), git, make and Python 3.11 or newer. The git
dependencies on `valargroup/wallet-pir` and `valargroup/ipir-sp` must be
fetchable.

```sh
make doctor                       # prerequisites; NETWORK=1 also probes the testnet RPC
make check-package PACKAGE=tachyon-chain
make check-fast BASE=origin/main  # affected packages since BASE
make check-correctness            # all tests, each under 30 s, the run under 5 min
make check-full                   # fmt, boundary, workspace clippy and tests
```

Tests use the `release-fast` profile (`cargo test --profile release-fast`).
Deploy binaries use `release` (fat LTO): `cargo build --release -p
tachyon-pir-publisher --features memo,first-contact,tachygram,registry`, once
the hosts exist.

See [AGENTS.md](AGENTS.md) for the development rules: the spec is the contract,
dependency boundaries, test budgets and how the gates work.
