# Memo deployment

Status: not deployed. This page states the configuration the spec defines
(section 11.7); no binary implements it yet.

## Publisher

```json
{
  "management_listen": "127.0.0.1:9400",
  "chain": { "rpc_url": "http://tachyscan.cnode.phd:18232", "no_auth": true, "poll_ms": 1000 },
  "memo": { "enabled": true, "settings": { "state_dir": "/var/lib/tachyon-pir/memo" } }
}
```

- Build `tachyon-pir-publisher` with the `memo` feature. A `memo` section in a
  binary built without it is rejected at start-up.
- `state_dir` holds `memo-v1.sqlite` and the published objects. first-contact
  shares this directory and requires memo to be enabled.
- One publisher runs per network.

## Query server

```json
{
  "management_listen": "127.0.0.1:9500",
  "public_listen": "127.0.0.1:9510",
  "memo": { "enabled": true, "settings": {
    "publisher": "https://publisher.example", "state_dir": "/var/lib/tachyon-pir-query/memo",
    "role": "combined", "publisher_network": "test", "publisher_genesis": "<64 hex>",
    "retain_scopes": 64 } }
}
```

- Build `tachyon-pir-query-server` with the `memo` feature. Only the `combined`
  role exists in version 1.
- `retain_scopes` defaults to 64 epochs. Each retained table costs about 160 MiB
  of resident memory by wallet-pir's measurement (to be re-measured here), so
  64 epochs at one table each is about 10 GiB per host, before buckets and
  segments.

Both binaries accept `--config FILE` and `--check` (validate offline).
Unknown configuration fields are refused.

## Observability (spec 11.8)

Management listener: `GET /health`, `GET /ready`, `GET /metrics`.

Metrics, prefixed `tachyon_memo_`: `ingested_height`, `anchor_height`,
`entries_total`, `open_scope_entries`, `segments_overflow_total`,
`publication_seconds`, `sessions_revoked_total`, `envelopes_invalid_total`,
`state_entries_total` (kind `0x03` entries seen).

Alert on a publication that fails because a bucket needs more than 8 segments;
the previous publication keeps serving.
