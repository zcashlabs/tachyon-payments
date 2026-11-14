# Memo

Status: specified, not implemented (milestone M2). Nothing is deployed.

The memo database answers `tag => memo` for follow-up payments. A sender who
already shares a channel with a recipient puts a 32-byte tag and a 128-byte
ciphertext in the transaction's `vMemoTachyon` field. The recipient's wallet
derives the next tag it expects and fetches every entry stored under that tag
with one PIR query, without revealing the tag to the server.

| | |
|---|---|
| Scope | one Tachyon epoch, `scope_id = epoch` |
| Source | kind `0x01` (follow-up) envelope entries in every Tachyon bundle of the epoch, whatever the stamp type |
| Entry | `key16 \|\| ciphertext(128)`, 144 bytes, 56 per row, target load 34 |
| Lookup result | every entry whose `key16` matches, in ordinal order |
| Protocol string | `tachyon-memo-pir-v1` |
| Query magic | `TMQ1` |
| Mount | `/memo/v1`, legacy alias `/v1/memo` |
| Crates | `memo-store`, `memo-publisher` (`src/memo/`); serving module in `tachyon-pir-query-server`, feature `memo` |
| Default retention | 64 epochs |

The memo crates also carry [first-contact](../first-contact/README.md), which
reads the same envelopes and shares the journal; the two are one product family
for the dependency rules.

No payment-protocol data exists on the testnet yet: every sampled memo field is
empty. The envelope format is defined provisionally in spec section 5 and is
owned by the payment-protocol authors (decision D1). Until wallets publish
envelopes, memo is proven only on synthetic chains from `tools/fixtures`.

- [Protocol](protocol.md): envelope, entry layout, placement, map and query.
- [Architecture](architecture.md): crates, journal and publication.
- [Deployment](deployment.md): configuration and metrics.
- [Qualification](qualification.md): the release gate and its status.
- [Changelog](CHANGELOG.md).

Spec sections: 5 (envelope), 6 (table), 7.1 and 7.3 (memo), 10 (wire protocol),
11 (publication), 12 (trust and privacy), 13 (testing).
