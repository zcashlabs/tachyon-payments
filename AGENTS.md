# Tachyon payment PIR development

This workspace builds the server side of the Tachyon payment protocol's private
retrieval layer: four PIR products (memo, first-contact, tachygram, registry),
one publisher binary and one query-server binary. The contract is
[`docs/spec/spec-tachyon-payment-pir.md`](docs/spec/spec-tachyon-payment-pir.md)
("the spec"). It mirrors `../wallet-pir` at revision `40b0ba6a` and reuses its
shared libraries as pinned git dependencies.

## The spec is the contract

- Implement what the spec says, to the byte. When code needs to differ from the
  spec (a wrong constant, a layout that does not fit, a rule the testnet
  breaks), fix the spec first, in its own commit, with the reason. Then change
  the code in a later commit that cites the spec section.
- Never let code and spec disagree silently. A reviewer reads the spec, not the
  code, to decide what is correct.
- Provisional values (marked in spec sections 15 and 16) are still the
  contract until the spec changes them.
- No compatibility windows: a format change is a new `protocol` string and a
  direct cutover (spec 4.4). There are no deployed wallets to keep working.

## Layout (spec 4.2)

| Package | Path | Role |
|---|---|---|
| `tachyon-chain` | `src/shared/tachyon-chain` | Node reads: verbose-block decoding, epoch arithmetic, network identity |
| `tachyon-pir-protocol` | `src/shared/tachyon-pir-protocol` | Wallet-facing contract: constants, row layouts, placement, map and header types; no server dependencies |
| `tachyon-pir-table` | `src/shared/tachyon-pir-table` | The shared bucket-row table: build, validate, segment, digest |
| `memo-store`, `memo-publisher` | `src/memo/` | Journal and publication for memo and first-contact |
| `tachygram-store`, `tachygram-publisher` | `src/tachygram/` | Journal and publication for tachygrams |
| `registry-store`, `registry-publisher` | `src/registry/` | Registration log, write API, publication |
| `tachyon-pir-publisher` | `src/tachyon-pir-publisher` | Publisher binary |
| `tachyon-pir-query-server` | `src/tachyon-pir-query-server` | Query host binary; product serving modules live in this crate |
| `tachyon-pir-cli` | `tools/client` | Diagnostic probe client |
| `tachyon-pir-e2e-tests` | `tools/e2e-tests` | End-to-end tests across real HTTP boundaries |
| `tachyon-fixtures` | `tools/fixtures` | Node double and synthetic envelope generator |

Each product crate sits at `src/<product>/<crate>` (for example
`src/memo/memo-store`). Both hosts select products with the Cargo features
`memo`, `first-contact`, `tachygram` and `registry`; the default is none. Slow
tests sit behind a `qualification-tests` feature. Docs live under
`docs/<product>/`, evidence under `evidence/<product>/`, CI scripts under
`tools/ci/`. Local run output goes under the ignored `.local-pir/`.

## Dependency rules

`tools/ci/boundary.py` checks the resolved graph (`cargo metadata
--all-features`), using the package sets in `tools/ci/boundary.json`:

- **Serving code never depends on chain or ingestion code.** `tachyon-pir-protocol`,
  `tachyon-pir-table` and `tachyon-pir-query-server` may not reach
  `tachyon-chain`, any product store or publisher, `tachyon-pir-publisher`,
  `pir-node-rpc`, `pir-publisher` or `tachyon-fixtures` through normal or build
  dependencies (transitively) or a direct dev-dependency.
- **Product crates never depend on each other.** The memo, tachygram and registry
  families may not reach one another. first-contact is part of the memo family.
- The publisher host and the tools may depend on anything.
- Every workspace crate must be listed in `boundary.json` at its spec path. Add a
  new crate there, in the right sets, in the same change that adds it to the
  workspace; an unlisted crate fails the check.

## Tests

| Layer | Budget |
|---|---|
| Unit, golden, determinism, end to end | each test under 30 s |
| Whole warm correctness run (`make check-correctness`) | under 5 minutes |
| Qualification (full geometry, soak, live-testnet oracle) | behind `qualification-tests`, not in the gates |

- **No stubs, no ignored tests, no weakened assertions.** Do not land
  `todo!()`, `unimplemented!()`, placeholder return values or a "check" that
  checks nothing. Do not mark tests `#[ignore]`: `tools/ci/test_budget.py`
  fails the run on any ignored test. A slow test goes behind
  `qualification-tests`. Never loosen an assertion, widen a tolerance or delete
  a failing case to get green; fix the code, or fix the spec first.
- **Fast tests never touch the network.** They run against the node double and
  synthetic chains in `tools/fixtures` (`tachyon-fixtures`). Golden decoder tests
  read frozen testnet blocks checked into the repository. Only the golden-fixture
  capture reads the live testnet, and only read-only (`getblockchaininfo`,
  `getblockcount`, `getblockhash`, `getblock <hash> 2`); its output is reviewed and
  committed, and no gate runs it. Live-testnet oracle runs are qualification.
- **Oracles are independent.** The expected answer for a lookup is computed from
  raw blocks by code that does not share the table builder (spec 13.1). The
  fixtures crate does not depend on the decoder it checks.
- Test negative cases and restart and recovery paths, across real HTTP
  boundaries in `tools/e2e-tests`.
- Keep fast fixtures small (a few epochs of a short synthetic chain, small
  geometries where the code allows it). Deployed-size parameters belong in
  qualification.
- Determinism is a requirement: sealed tables are a pure function of the chain
  and the constants. Tests assert identical bytes across restart, publication
  cadence and rebuild (spec 6.5).

## Gates

Use the toolchain pinned in `rust-toolchain.toml` (1.97.1) with rustfmt and
clippy. Tests use the `release-fast` profile; deploy binaries use `release`
(fat LTO).

| Command | Runs |
|---|---|
| `make doctor` | Toolchain 1.97.1, cargo, rustfmt, clippy, git, make, Python 3.11+, `cargo fetch --locked`. `NETWORK=1` also calls `getblockcount` on the testnet RPC (read-only) |
| `make check-fast BASE=<commit>` | `cargo fmt --all --check`, the boundary check, CI tool tests if `tools/ci/` changed, then clippy `-D warnings` and release-fast tests for the packages changed since `BASE` and their dependents. Without `BASE`, the whole workspace |
| `make check-package PACKAGE=<name> [TEST=<filter>] [FEATURES=<list>]` | fmt, boundary, clippy and release-fast tests for one package |
| `make check-correctness` | Every workspace test and doctest under release-fast, timed per test (30 s) and in total (300 s, compile included) |
| `make check-full` | CI tool tests, fmt, boundary, `cargo clippy --locked --workspace --all-targets -- -D warnings`, `cargo test --locked --workspace --profile release-fast` |
| `make fmt` | `cargo fmt --all` |

`OFFLINE=1` passes `--offline` to cargo. Iterate with `check-package` or
`check-fast`; run `check-correctness` and `check-full` before handing work back.
Keep Cargo invocations sequential within one target directory.

Passing the gates is not qualification. The per-product qualification gate is
spec 13.3 and is recorded under `evidence/<product>/`.

## Conventions that are easy to get wrong

- Integers are little-endian throughout (spec 6.2).
- Block hashes and txids are held in protocol order (reversed RPC hex) and
  written in display order in JSON. Tachygrams, anchors, tags and digests are not
  reversed (spec 3).
- Read chain parameters (activation height, network) from the node; do not
  hard-code them. Read tachygram counts from the data; the testnet does not
  follow the draft ZIPs' two-per-action rule.
- The on-chain memo is attacker-controlled. Invalid envelopes are skipped and
  counted, never fatal (spec 5.2). Every entry for a tag is kept, in ordinal
  order; never first-entry-wins (spec 5.3).
- The servers never log or label metrics with client-supplied bytes (spec 10.4).

## Documentation and evidence

Start at [`docs/README.md`](docs/README.md). Each product has `README.md`,
`protocol.md`, `architecture.md`, `deployment.md`, `qualification.md` and
`CHANGELOG.md` under `docs/<product>/`. Update them with the code they describe;
the spec stays the source of requirements. Evidence follows
[`evidence/README.md`](evidence/README.md). Never claim a deployment or a
qualification from source code or a passing test run.

Commit code, tests, configuration and concise docs needed for a change. Keep
run output, exploratory scripts and task reports out of git (`.local-pir/` is
ignored).
