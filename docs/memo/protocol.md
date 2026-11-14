# Memo protocol

Status: specified, not implemented. The contract is the
[spec](../spec/spec-tachyon-payment-pir.md); this page collects what applies to
memo. Common rules are in [shared](../shared/README.md).

## On-chain envelope (spec 5, provisional)

```
envelope    = magic entry_count entry*
magic       = "TPM1"                      4 bytes
entry_count = u8                          1..=16
entry       = kind tag body
kind        = u8
tag         = 32 bytes
```

| `kind` | Name | `body` | Body length | Goes to |
|---|---|---|---|---|
| `0x01` | follow-up | `ciphertext` | 128 | memo |
| `0x02` | first contact | `kem_ciphertext \|\| ciphertext` | 1,216 | first-contact |
| `0x03` | wallet state | `len (u16 LE) \|\| ciphertext` | 2 + `len`, `len` ≤ 1024 | counted only |

- The envelope must be consumed exactly; trailing bytes make it invalid.
- An empty memo is skipped. An envelope that does not parse, or has an unknown
  kind, is skipped whole and counted in `tachyon_memo_envelopes_invalid_total`.
  It never fails ingestion.
- A tag of all zero bytes is rejected (an all-zero entry marks an empty slot).
- Each entry's **ordinal** is `(height, tx index in block, entry index)`.
- Every entry for a tag is stored, in ordinal order. First-entry-wins is
  forbidden: a tag is visible in the mempool and could be front-run (spec 5.3).
  The wallet authenticates by decryption.

## Table

| Field | Value |
|---|---|
| Entry | `key16 \|\| ciphertext(128)` = 144 bytes; `key16` is the first 16 bytes of the tag |
| Slots per row | 56 (`floor(8192 / 144)`) |
| Target load per row | 34 |
| Entries per `tachyon-4k8` table at target | 139,264 |
| Scope | one epoch, `scope_id = epoch` |
| Placement domain | `"tachyon-pir/memo/v1/place\0"` |

```
h      = SHA-256("tachyon-pir/memo/v1/place\0" || genesis_hash(32) || scope_id(u64) || seed(u32) || key16)
bucket = u32_le(h[4..8]) mod n_buckets
row    = u32_le(h[0..4]) mod rows
```

Occupied entries come first in a row, sorted by `(key16, ordinal)`; the rest
of the row is zero. The client validates the whole row before using it.

## Lookup

- **Result**: every entry in the row whose `key16` matches, in ordinal order. A
  wallet tries to decrypt each.
- **Absence**: a canonical row with no matching entry means no memo with this
  tag in this scope up to the map's anchor.
- A wallet queries every segment of the bucket its tag selects.
- A hit and a miss send identical transcripts. The server learns the scope,
  bucket and segment queried, not the row.

## Wire protocol (spec 10)

- Mount `/memo/v1`, legacy alias `/v1/memo`.
- `GET map` returns the head with `"protocol": "tachyon-memo-pir-v1"`, the
  network, `genesis_hash`, `activation_height`, `epoch_length`, `generation`,
  `recovery_epoch`, `anchor_height`, `anchor_hash`, `floor_scope`, sealed map
  chunks and open scopes. Unknown fields are refused.
- `POST query`: `"TMQ1" || session_id(32) || nonce(16) || payload`. The 52-byte
  header is echoed before the packed answer. Body limit 512 KiB, upload deadline
  15 s.
- Errors are those of spec 10.5 (`400`, `408`, `409 stale_routing`,
  `410 session_unavailable`, `413`, `429 overloaded`, `503`).

## Wallet obligations (spec 12.1)

Accept `anchor_hash` independently before using any table; authenticate
results by decryption; validate every row; respect `floor_scope`. Padding of
memo queries per epoch is the wallet's decision; a dummy query is
indistinguishable from a real one.
