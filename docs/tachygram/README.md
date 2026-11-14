# Tachygram

Status: specified, not implemented (milestone M1). Nothing is deployed.

The tachygram database lets a wallet do two things privately, per epoch:
locate its note commitment (inclusion) and check that a per-epoch nullifier is
absent (exclusion). Both are tachygrams, 32-byte values that proof stamps list
for the actions they cover. A hit returns the block height and stamp ordinal a
wallet needs to fetch the creation stamp.

| | |
|---|---|
| Scope | one Tachyon epoch, `scope_id = epoch` |
| Source | `tachygrams[]` of every **proof** stamp; pointer stamps contribute nothing |
| Entry | `tg(32) \|\| blk_height(u32) \|\| stamp_ordinal(u32)`, 40 bytes, 204 per row, target load 124 |
| Key | the full 32-byte tachygram |
| Lookup result | exactly one entry, or a canonical row proving absence for the heights the map covers |
| Protocol string | `tachyon-tachygram-pir-v1` |
| Query magic | `TGQ1` |
| Mount | `/tachygram/v1`, legacy alias `/v1/tachygram` |
| Crates | `tachygram-store`, `tachygram-publisher` (`src/tachygram/`); serving module in `tachyon-pir-query-server`, feature `tachygram` |
| Default retention | 64 epochs |

Tachygram comes first (M1) because it is the only product with real data on
the testnet: about 190,000 tachygrams per epoch at the observed rate.
Tachygram results are hints; spendability is finally established by the
wallet's proof and consensus.

- [Protocol](protocol.md): record, placement, lookup semantics, map and query.
- [Architecture](architecture.md): crates, journal, uniqueness and the OSS interface.
- [Deployment](deployment.md): configuration, retention and metrics.
- [Qualification](qualification.md): the release gate, including the 24-hour
  live-testnet run.
- [Changelog](CHANGELOG.md).

Spec sections: 3 (chain facts), 6 (table), 8 (tachygram), 10, 11, 12, 13;
decision D2 (record contents), D6 (stamp tachygram sets).
