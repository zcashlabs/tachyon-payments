# First-contact

Status: implemented; not yet qualified or deployed.

The first-contact database answers `tag => KEM.c || memo` for the first payment
to an address. A sender with no prior channel encapsulates to the recipient's
ML-KEM key and publishes the KEM ciphertext and a memo ciphertext under the
address's first-contact tag. The recipient checks one tag per address it has
handed out, per span, with one PIR query.

| | |
|---|---|
| Scope | a **span** of consecutive whole epochs, `scope_id` = the span's first epoch |
| Source | kind `0x02` envelope entries |
| Entry | `key16 \|\| kem_ct(1088) \|\| ciphertext(128)`, 1,232 bytes, 6 per row, target load 1 |
| Lookup result | every entry whose `key16` matches, in ordinal order |
| Protocol string | `tachyon-first-contact-pir-v1` |
| Query magic | `TFQ1` |
| Mount | `/first-contact/v1`, legacy alias `/v1/first-contact` |
| Crates | `memo-store`, `memo-publisher` (`src/memo/`), shared with memo; serving module in `tachyon-pir-query-server`, feature `first-contact` |
| Default retention | unlimited |

First contacts are rare, so spans are long: a span closes once it holds 3,072
entries or covers 64 epochs. First-contact belongs to the memo product family
and shares its journal; it has no crates of its own.

Anyone who holds an address can publish entries at its first-contact tag, so
every entry is kept and the wallet authenticates by decryption.

- [Protocol](protocol.md): envelope kind, entry layout, span rule, map and query.
- [Architecture](architecture.md): shared memo crates and span publication.
- [Deployment](deployment.md): configuration and metrics.
- [Qualification](qualification.md): the release gate and its status.
- [Changelog](CHANGELOG.md).

Spec sections: 5 (envelope), 6 (table), 7.2 and 7.3 (first-contact), 10, 11,
12, 13; decisions D1 and D4.
