# Tachygram protocol

Status: specified, not implemented. The contract is the
[spec](../spec/spec-tachyon-payment-pir.md); common rules are in
[shared](../shared/README.md).

## Record (spec 8.1)

```
tg            32 bytes   the tachygram, bytes as the RPC hex reads (not reversed)
blk_height    u32 LE     height of the block containing the stamp
stamp_ordinal u32 LE     zero-based ordinal of the stamp among proof stamps in its
                         epoch, in block order then transaction order
```

40 bytes. `blk_height` and `stamp_ordinal` locate the creation stamp. The anchor
chain advances once per proof stamp, so the ordinal identifies the post-stamp
anchor. This departs from the Tachyon Book's 41-byte sketch: `action_count` is
removed and `anchor_height` is replaced by `stamp_ordinal` (decision D2,
decided). Changing the record is a new `protocol` string and a rebuild.

## Table

| Field | Value |
|---|---|
| Entry | the 40-byte record; the key is the full tachygram |
| Slots per row | 204 (`floor(8192 / 40)`) |
| Target load per row | 124 |
| Entries per `tachyon-4k8` table at target | 507,904 |
| Scope | one epoch, `scope_id = epoch` |
| Placement domain | `"tachyon-pir/tachygram/v1/place\0"` |

```
h      = SHA-256("tachyon-pir/tachygram/v1/place\0" || genesis_hash(32) || scope_id(u64) || seed(u32) || tg)
bucket = u32_le(h[4..8]) mod n_buckets
row    = u32_le(h[0..4]) mod rows
```

`u32_le(h[0..4])` is the `H(tg)[:4]` key of the Tachyon Book's tachygram
database. At the testnet's roughly 190,000 tachygrams per epoch one `4k8` table
holds an epoch at target load (`n_buckets = 1`).

## Lookup semantics (spec 8.4)

- **Found**: exactly one entry with the full 32-byte tachygram.
- **Absent**: a canonical row with no such entry. Absence means something only
  for the heights the map says the scope covers: the whole epoch for a sealed
  scope, the epoch's blocks up to `anchor_height` for an open one.
- Before trusting either answer the client validates the whole row: sorted
  order, zero padding, every `blk_height` inside the scope's
  `[first_height, last_height]`, and every entry's placement hash equal to this
  row and bucket.
- Query every segment of the selected bucket.
- The server can withhold or wrongly report absence (spec 12.2); it cannot forge
  a spendable note. Treat results as hints.

## Wire protocol (spec 10)

- Mount `/tachygram/v1`, legacy alias `/v1/tachygram`.
- Map head `"protocol": "tachyon-tachygram-pir-v1"`, with `network`,
  `genesis_hash`, `activation_height`, `epoch_length` (4096), `generation`,
  `recovery_epoch`, `anchor_height`, `anchor_hash`, `floor_scope`, sealed chunks
  and open scopes.
- `POST query`: `"TGQ1" || session_id(32) || nonce(16) || payload`; 52-byte header
  echoed; body limit 512 KiB; upload deadline 15 s.
- Errors as spec 10.5; `410 session_unavailable` after a reorg into a sealed
  epoch.

The full tachygram set of a stamp, which a wallet needs for witness
construction, is not served here; the wallet fetches the block (decision D6).
