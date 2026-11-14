# Tachygram architecture

Status: specified, not implemented.

```
Zakura node ─▶ tachyon-chain ─▶ tachygram-publisher (ingestion) ─▶ tachygram-store (tachygram-v1.sqlite)
                                      │
                                      └─▶ tachyon-pir-table ─▶ pir-publication objects ─▶ /provider/tachygram/v1/
                                                                                      │
                                    tachyon-pir-query-server (feature tachygram) ◀────┘
```

## Crates

| Crate | Path | Role |
|---|---|---|
| `tachygram-store` | `src/tachygram/tachygram-store` | SQLite journal of proof stamps and their tachygrams |
| `tachygram-publisher` | `src/tachygram/tachygram-publisher` | `Ingestion`, `Publication` and `Publisher` |
| `tachyon-pir-query-server` | `src/tachyon-pir-query-server` | In-crate `tachygram` serving module, feature `tachygram` |

The tachygram crates may not depend on the memo or registry crates; no serving
crate may depend on them.

## Source (spec 8.2, 3)

- Each proof stamp in `getblock <hash> 2` under `tx[].tachyon` has `anchor`,
  `tachygramSet`, `tachygrams[]`, `proof`, `coverage` and `feeZat`. Its
  tachygrams cover every action it proves, including actions of other
  transactions in the block.
- Pointer stamps (`aggregateId`, `feeZat`) contribute nothing; their actions'
  tachygrams appear in the covering aggregate's stamp.
- Counts are read from the data. The testnet does not follow the draft ZIPs'
  two-tachygrams-per-action rule; the ZIP checks run only with
  `strict_zip_rules` (off for the alpha testnet).

## Uniqueness (spec 8.3)

Consensus rejects a tachygram that already appears in the current or previous
epoch. A duplicate within one epoch is a decoder or node fault: ingestion stops
and alerts, and nothing is published.

## Journal (spec 8.5, 8.6)

`tachygram-store` keeps `tachygram-v1.sqlite` with `open`, `append`, `rewind`
and `checkpoint_at_or_below` as memo-store does, plus `stamps(epoch)`. It also
exposes, as public API for a later oblivious sync service (OSS, not built):

```
Store::epoch_tachygrams(epoch) -> impl Iterator<Item = (StampOrdinal, [u8; 32])>
```

in stamp order.

## Publication and reorgs

- Target height `tip - 2`. The open epoch is rebuilt at each publication that
  changed something; an epoch seals 100 blocks after its last block.
- Objects under `/provider/tachygram/v1/`, schema
  `tachyon-tachygram-prepared-distribution-v1`.
- A rewind inside the open epoch is a new generation; into a sealed epoch it
  increments `recovery_epoch` and revokes the affected sessions (spec 11.4).
- Sealed tables are a pure function of the chain; two publishers produce
  identical bytes. Tests assert this across restart and cadence (spec 6.5).
