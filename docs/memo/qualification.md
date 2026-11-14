# Memo qualification

Status: **not run.** No memo build has been qualified.

## Gate (spec 13.3)

Memo is qualified for a release only when one run shows all of:

- 20 open-loop lookups per second for 6 hours at full geometry (`tachyon-4k8`);
- every answer checked against an independent oracle, computed from raw blocks
  by code that does not share the table builder;
- no wrong answer, no failed arrival, no out-of-memory event and no swap growth;
- p99 latency under 1 s;
- the result recorded under `evidence/memo/<topic>-<date>/README.md` with build
  identity, command, configuration, host, and an explicit "not established"
  list (see [evidence](../../evidence/README.md)).

Passing the automated checks is not a qualification certificate.

## Acceptance scenarios (spec 13.2)

Memo must pass scenarios 1 to 13: publisher with no consumers, cold bootstrap
over TLS, incremental publication, exact answers, duplicate keys, forced
overflow, invalid envelopes, shallow reorg, reorg into a sealed scope, corrupt
or truncated object, publisher outage, restart of each process, and overload.
This is the M2 exit criterion.

There is no memo traffic on the testnet, so the oracle runs on synthetic chains
from `tools/fixtures` until wallets publish envelopes.

## Results

None recorded.
