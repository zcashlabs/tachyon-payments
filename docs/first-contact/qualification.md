# First-contact qualification

Status: **not run.** No first-contact build has been qualified.

## Gate (spec 13.3)

- 20 open-loop lookups per second for 6 hours at full geometry (`tachyon-4k8`);
- every answer checked against an independent oracle computed from raw blocks;
- no wrong answer, no failed arrival, no out-of-memory event, no swap growth;
- p99 latency under 1 s;
- the result recorded under `evidence/first-contact/<topic>-<date>/README.md`
  with build identity, command, configuration, host and an explicit "not
  established" list.

Passing the automated checks is not a qualification certificate.

## Acceptance scenarios (spec 13.2)

Scenarios 1 to 13 (with memo). Scenario 5 (duplicate
keys) and 6 (forced overflow) matter most here: anyone can publish at a
first-contact tag, and a row holds only six entries.

The testnet carries no first-contact entries yet, so the oracle runs on
synthetic chains from `tools/fixtures`.

## Results

None recorded.
