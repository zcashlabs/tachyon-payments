# First-contact architecture

Status: implemented.

First-contact has no crates of its own. It is built by the memo crates and
counts as the memo product family for the dependency rules (spec 4.2).

| Crate | Role for first-contact |
|---|---|
| `memo-store` (`src/memo/memo-store`) | Stores kind `0x02` entries in `memo-v1.sqlite` beside memo's, and the span boundaries (`Store::spans()`) |
| `memo-publisher` (`src/memo/memo-publisher`) | Parses envelopes once for both products, decides spans, builds first-contact tables |
| `tachyon-pir-query-server` | In-crate `first-contact` serving module, feature `first-contact` |

## Journal (spec 7.3)

The same `Store` API as memo: `append` per block, `rewind`, `checkpoint_at_or_below`,
`entries(kind, scope)` and `spans()`. `rewind` removes every block above the
target and any span boundary those blocks decided, so a reorg across an epoch
boundary can reopen a span.

## Publication

- The open span's tables are rebuilt at every publication that changed
  something; a span's tables are built once when it seals.
- Objects and the current publication live under `/provider/first-contact/v1/`,
  schema `tachyon-first-contact-prepared-distribution-v1`.
- Reorgs, recovery epochs and session revocation follow spec 11.4, as for memo.

## Serving

One `ConsumerEngine` for first-contact in the query host. Retention defaults to
unlimited: spans are long, so there are few of them.
