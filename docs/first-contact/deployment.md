# First-contact deployment

Status: not deployed. Configuration as defined in spec 11.7.

## Publisher

```json
{
  "memo":          { "enabled": true, "settings": { "state_dir": "/var/lib/tachyon-pir/memo" } },
  "first_contact": { "enabled": true, "settings": {} }
}
```

- Build `tachyon-pir-publisher` with the `first-contact` feature (and `memo`).
- The configuration key is `first_contact`. It has no state directory of its
  own: it shares memo's and requires `memo` to be enabled. A configuration that
  enables it without memo is rejected.

## Query server

A `first_contact` section with the same settings shape as memo's
(`publisher`, `state_dir`, `role: "combined"`, `publisher_network`,
`publisher_genesis`, `retain_scopes`). Build with the `first-contact` feature.
Default `retain_scopes` is unlimited.

Both binaries accept `--config FILE` and `--check`.

## Observability

Metrics prefixed `tachyon_first_contact_` (spec 11.8, `tachyon_<product>_` with the hyphen written as an underscore, as metric names require): `ingested_height`,
`anchor_height`, `entries_total`, `open_scope_entries`,
`segments_overflow_total`, `publication_seconds`, `sessions_revoked_total`.
Invalid envelopes are counted once, by memo
(`tachyon_memo_envelopes_invalid_total`).

With six slots per row, overflow is more likely than for memo; watch
`segments_overflow_total`.
