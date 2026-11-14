# Tachygram qualification

Status: **not run.** No tachygram build has been qualified, and the 24-hour
live-testnet run (scenario 15) has not been run.

## Gate (spec 13.3)

- 20 open-loop lookups per second for 6 hours at full geometry (`tachyon-4k8`);
- every answer checked against an independent oracle computed from raw blocks
  by code that does not share the table builder;
- no wrong answer, no failed arrival, no out-of-memory event, no swap growth;
- p99 latency under 1 s;
- the result recorded under `evidence/tachygram/<topic>-<date>/README.md` with
  build identity, command, configuration, host and an explicit "not established"
  list.

Passing the automated checks is not a qualification certificate.

## Acceptance scenarios (spec 13.2)

The M1 exit criterion is scenarios 1 to 4, 6 and 8 to 13 for tachygram, plus
scenario 15: the tachygram product follows the alpha testnet for 24 hours with
exact answers. Scenarios 5 (duplicate keys) and 7 (invalid envelopes) do not
apply: tachygrams are unique per epoch and do not come from envelopes.

The live-testnet run reads the node read-only and belongs to qualification,
behind the `qualification-tests` feature or a separate run, never in the fast
gates.

## Results

None recorded.
