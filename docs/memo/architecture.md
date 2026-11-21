# Memo architecture

Status: implemented.

```
Zakura node ─▶ tachyon-chain ─▶ memo-publisher (ingestion) ─▶ memo-store (memo-v1.sqlite)
                                      │
                                      └─▶ tachyon-pir-table ─▶ pir-publication objects ─▶ /provider/memo/v1/
                                                                                      │
                                         tachyon-pir-query-server (feature memo) ◀────┘
```

## Crates

| Crate | Path | Role |
|---|---|---|
| `memo-store` | `src/memo/memo-store` | The SQLite journal for memo and first-contact |
| `memo-publisher` | `src/memo/memo-publisher` | Envelope parsing, `Ingestion`, `Publication` and `Publisher` for memo and first-contact |
| `tachyon-pir-query-server` | `src/tachyon-pir-query-server` | In-crate `memo` serving module, behind the `memo` feature |

The memo crates may depend on shared libraries and on `tachyon-chain`. They may
not depend on the tachygram or registry crates, and no serving crate may depend
on them (`tools/ci/boundary.py`).

## Journal (spec 7.3)

`memo-store` keeps one SQLite database, `memo-v1.sqlite`, as the source of truth
for both memo and first-contact, following wallet-pir's `receiver-store`.

```
Store::open(path) -> Store
Store::append(block: &IndexedBlock) -> Result<()>
Store::rewind(to: BlockId) -> Result<()>
Store::checkpoint_at_or_below(height) -> Option<BlockId>
Store::entries(kind, scope) -> impl Iterator<Item = Entry>
Store::spans() -> Vec<Span>
```

`IndexedBlock` holds the block id, epoch and parsed entries with their ordinals.
`append` is atomic per block. `rewind` removes every block above the target and
any span boundary those blocks decided.

## Ingestion (spec 11.2)

- Implements `pir_pipeline::ingestion::Ingestion`. Target height is
  `tip - CONFIRMATION_DEPTH` (2).
- Each pass walks durable checkpoints newest-first against the node's canonical
  hashes; `withdraw_above` is called before `rewind`.
- Envelopes are parsed as untrusted data; invalid ones are skipped and counted.

## Publication (spec 11.3, 6.5)

- The open epoch's tables are rebuilt in full at every publication that changed
  something. An epoch seals `SEAL_DEPTH = 100` blocks after its last block; its
  tables are then built once and are immutable.
- Rows, public setup material and map chunks are content-addressed objects; the
  current publication is a `PublicationEnvelope` under `/provider/memo/v1/`, schema
  `tachyon-memo-prepared-distribution-v1`.
- A rewind into a sealed epoch increments `recovery_epoch` and revokes the
  affected sessions before the journal rewinds.

## Serving (spec 11.6)

The query host runs a `ConsumerEngine` for memo over `HttpsSource`, verifies
every object, prepares PIR runtimes before switching, and keeps the displaced
publication for 600 s. It serves the newest `retain_scopes` epochs.

## Open decisions

D1 (envelope format, ciphertext size) and D4 do not change memo's design but may
change its constants. See spec section 16.
