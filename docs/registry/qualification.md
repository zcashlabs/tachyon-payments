# Registry qualification

Status: **not run.** No registry build has been qualified.

## Gate (spec 13.3)

- 20 open-loop lookups per second for 6 hours at full geometry (`tachyon-4k8`);
- every answer checked against an independent oracle (the registration log, read
  by code that does not share the table builder);
- no wrong answer, no failed arrival, no out-of-memory event, no swap growth;
- p99 latency under 1 s;
- the result recorded under `evidence/registry/<topic>-<date>/README.md` with
  build identity, command, configuration, host and an explicit "not
  established" list.

Passing the automated checks is not a qualification certificate.

## Acceptance scenarios (spec 13.2)

The M3 exit criterion is scenarios 1 to 4, 6, and 10 to 14. Scenario 14 is the
registry's own: writes are idempotent, durable before acknowledgement, visible at
the promised sequence, and proof of work and rate limits are enforced.
Spec 14 does not list scenarios 5, 7, 8, 9 and 15 for the registry; it has no
envelopes and no chain, so 7, 8, 9 and 15 cannot arise.

## Results

None recorded.
