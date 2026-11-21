# First-contact protocol

The contract is the [spec](../spec/spec-tachyon-payment-pir.md); common rules
are in [shared](../shared/README.md).

## Source (spec 5)

Kind `0x02` entries of the provisional `TPM1` envelope (see
[memo protocol](../memo/protocol.md#on-chain-envelope-spec-5-provisional)):

```
entry = 0x02 || tag(32) || kem_ciphertext(1088) || ciphertext(128)
```

- `KEM_CT_BYTES = 1088` (ML-KEM-768) and `MEMO_CT_BYTES = 128` are provisional
  (decision D1).
- Invalid envelopes are skipped whole and counted; ingestion continues.
- Every entry for a tag is kept, in ordinal order `(height, tx index, entry index)`.

## Table

| Field | Value |
|---|---|
| Entry | `key16 \|\| kem_ct(1088) \|\| ciphertext(128)` = 1,232 bytes |
| Slots per row | 6 (`floor(8192 / 1232)`) |
| Target load per row | 1 (mean), relying on seed retry to avoid overflow |
| Entries per `tachyon-4k8` table at target | 4,096 |
| Placement domain | `"tachyon-pir/first-contact/v1/place\0"` |

```
h      = SHA-256("tachyon-pir/first-contact/v1/place\0" || genesis_hash(32) || scope_id(u64) || seed(u32) || key16)
bucket = u32_le(h[4..8]) mod n_buckets
row    = u32_le(h[0..4]) mod rows
```

The single-table design at one entry per row fills about 17% of the table. If
that proves too costly, wallet-pir's Receiver directory-plus-companion design is
the fallback (decision D4).

## Span rule (spec 7.2)

- Spans are decided at epoch boundaries and depend only on the chain.
- The open span closes at the first epoch boundary where it holds at least
  `FC_SPAN_TARGET = 3,072` entries (75% of one `4k8` table at target load) or
  covers `FC_SPAN_MAX_EPOCHS = 64` epochs.
- A span that closes by count has `n_buckets = 1`. A single epoch that alone
  exceeds one table's target uses more buckets under the rule of spec 6.3.
- A span seals `SEAL_DEPTH = 100` blocks after its last block.
- `scope_id` is the span's first epoch. The map's scope `first_height` and
  `last_height` give the span's block range.

## Lookup

- **Result**: every entry whose `key16` matches, in ordinal order. The wallet
  decapsulates and decrypts each; entries that fail are someone else's or junk.
- **Absence**: a canonical row with no matching entry means no first contact
  with this tag in this span up to the map's anchor.
- Query every segment of the selected bucket.

## Wire protocol (spec 10)

- Mount `/first-contact/v1`, legacy alias `/v1/first-contact`.
- Map head `"protocol": "tachyon-first-contact-pir-v1"`, otherwise as memo.
- `POST query`: `"TFQ1" || session_id(32) || nonce(16) || payload`; 52-byte header
  echoed; body limit 512 KiB; upload deadline 15 s.
- Errors as spec 10.5.
