# Registry protocol

Status: specified, not implemented. The contract is the
[spec](../spec/spec-tachyon-payment-pir.md); common rules are in
[shared](../shared/README.md).

## Address and digest (spec 9.1)

```
addr   = pk(32) || ek(1184)                     1,216 bytes, ML-KEM-768 (provisional)
digest = SHA-256("tachyon-pir/registry/v1/addr\0" || addr)
key16  = digest[0..16]
```

The short address a recipient shares is `(chunk_id, digest)`.

## Chunks (spec 9.2)

- A chunk is a run of registrations in log order; `scope_id = chunk_id`, from 0.
- A chunk closes when it holds `REGISTRY_CHUNK_ENTRIES = 3,072` entries and is
  sealed at once; there is no reorg to wait for.
- Only the last, open chunk is rebuilt at each publication.
- `n_buckets = 1` always.
- Chunk ids never change. Chunk membership is `sequence / 3072`.

## Table

| Field | Value |
|---|---|
| Entry | `key16 \|\| pk(32) \|\| ek(1184)` = 1,232 bytes |
| Slots per row | 6 |
| Target load per row | 1 (mean), relying on seed retry |
| Entries per `tachyon-4k8` table at target | 4,096 |
| Placement domain | `"tachyon-pir/registry/v1/place\0"` |

```
h   = SHA-256("tachyon-pir/registry/v1/place\0" || genesis_hash(32) || chunk_id(u64) || seed(u32) || key16)
row = u32_le(h[0..4]) mod rows
```

`n_buckets = 1`, so the bucket is always 0.

A sender validates the row, finds the entry whose `key16` matches, and accepts
it only if the digest of `pk || ek` equals the digest in the short address.

## Write API (spec 9.3)

Served by the publisher on its own public listener at `/registry/v1` (alias
`/v1/registry`).

`GET /registry/v1/challenge`

```json
{ "challenge": "<32 bytes hex>", "difficulty_bits": 20, "expires_unix": 1794670000 }
```

`POST /registry/v1/register`, body exactly 1,256 bytes:

```
challenge(32) || nonce(8) || addr(1216)
```

- Proof of work: `SHA-256("tachyon-pir/registry/v1/pow\0" || challenge || nonce || digest)`
  has at least `difficulty_bits` leading zero bits. The digest is bound in so
  one solution cannot register many addresses.
- Challenges are stateless: an HMAC over the expiry under a publisher secret,
  valid for `REGISTRY_CHALLENGE_SECONDS = 600`.
- Validation: exact body length; `pk` a canonical field element; `ek` passes the
  ML-KEM-768 encapsulation-key modulus check; `key16` not all zero.
- Success, `200`:

  ```json
  { "chunk_id": 12, "digest": "<64 hex>", "created": true, "visible_after_sequence": 4821 }
  ```

- Idempotent: a repeated address returns its existing `chunk_id` with
  `"created": false`.
- The response is sent only after the log entry is durable (fsync).
  `visible_after_sequence` is the first publication sequence that will contain
  it. Entries are never deleted or changed.
- Errors use the common body `{"code": "...", "message": "..."}`: `400` or `413`
  for a wrong length, `408` after the 15 s upload deadline, `429` with
  `Retry-After` above the rate limits.

## Lookup wire protocol (spec 10)

- Mount `/registry/v1`, legacy alias `/v1/registry`, on the query host.
- Map head `"protocol": "tachyon-registry-pir-v1"`. It omits
  `activation_height`, `epoch_length`, `anchor_height` and `anchor_hash`, and
  carries `log_sequence` instead.
- `POST query`: `"TRQ1" || session_id(32) || nonce(16) || payload`; 52-byte
  header echoed; body limit 512 KiB.
