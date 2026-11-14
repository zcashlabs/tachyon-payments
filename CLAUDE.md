# Repository guidance

This workspace implements the Tachyon payment-protocol PIR services (memo,
first-contact, tachygram, registry), a publisher and a query host. Follow
[AGENTS.md](AGENTS.md); it is the full guide. The essentials:

- **The spec is the contract**: `docs/spec/spec-tachyon-payment-pir.md`. If the
  code must differ from it, fix the spec first in its own commit, then the code.
- **Layout** (spec 4.2): products under `src/<product>/<crate>`, shared libraries
  under `src/shared/`, hosts at `src/tachyon-pir-publisher` and
  `src/tachyon-pir-query-server`, tools under `tools/`. Every crate is classified
  in `tools/ci/boundary.json` when it is added.
- **Dependency rules** (`tools/ci/boundary.py`): serving code
  (`tachyon-pir-protocol`, `tachyon-pir-table`, `tachyon-pir-query-server`) never
  reaches chain or ingestion code; product families (memo with first-contact,
  tachygram, registry) never reach each other.
- **Tests**: each under 30 s; the warm correctness run under 5 minutes; slow
  cases behind `qualification-tests`. No stubs, no `#[ignore]`, no weakened
  assertions. Fast tests use the node double in `tools/fixtures`; only the
  golden-fixture capture reads the live testnet, read-only.
- **Gates**: `make doctor`, `make check-package PACKAGE=<name> [TEST=<filter>]`,
  `make check-fast BASE=<commit>` while iterating; `make check-correctness` and
  `make check-full` before handing work back. Passing them is not qualification
  (spec 13.3).
- Docs under `docs/<product>/`, evidence under `evidence/<product>/<topic>-<date>/`.
  Local output under the ignored `.local-pir/`.
