# Evidence

Evidence is the dated record that a claim about a running build was checked:
a qualification run, a measurement, an acceptance scenario run outside the
automated gates, or an incident. Source code and a green CI run are not
evidence of deployment or qualification.

No evidence has been recorded yet.

## Layout

```
evidence/<product>/<topic>-<date>/README.md
```

- `<product>` is `memo`, `first-contact`, `tachygram`, `registry`, or `shared`
  for results that cover the common table, publisher or query host.
- `<topic>` is a short kebab-case name (`qualification`, `live-testnet-24h`,
  `profile-sizes`); `<date>` is the run's start date, `YYYY-MM-DD`.
- `README.md` is the note. Raw inputs needed to check the claim sit beside it.

For example: `evidence/tachygram/qualification-2026-12-01/README.md`.

## What a note contains

Spec 13.3 requires each qualification record to state the following, and every
other note should too:

- **Build identity**: the commit (full hash), whether the tree was clean, the
  toolchain, the Cargo profile and features.
- **Command**: the exact commands and flags, in the order run.
- **Configuration**: the publisher and query-server configuration files, with
  secrets removed.
- **Host**: machine, CPU, memory, OS, and where the load driver ran relative to
  the server.
- **Workload and results**: what was asked, what was expected, what happened,
  with the numbers (for qualification: lookup rate, duration, oracle agreement,
  failed arrivals, memory and swap, p99 latency).
- **Failures**: every failed attempt that bears on the claim, not just the
  successful run.
- **Not established**: an explicit list of what this run does not show.

## Rules

- Captured raw files are immutable. A correction is a new note or a new capture
  that links to the old one; never edit a recorded measurement.
- Keep only what supports a decision, an acceptance claim, a correctness or
  security argument, or an open incident. Exploratory runs and generated output
  stay in the ignored `.local-pir/` directory or in CI artifacts.
- Record large raw files by SHA-256 digest in the note when they cannot be
  committed, and say where the bytes are kept.
- Golden test fixtures are not evidence. They live beside the tests that read
  them, with their capture provenance.
- Figures quoted from `wallet-pir` (query, reply and memory sizes in spec 6.6)
  are planning numbers. A note that re-measures them for this workspace states
  the geometry and entry layout it measured.
