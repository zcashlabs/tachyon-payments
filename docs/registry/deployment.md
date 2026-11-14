# Registry deployment

Status: not deployed. Configuration as defined in spec 11.7.

## Publisher

```json
{
  "management_listen": "127.0.0.1:9400",
  "registry": { "enabled": true, "settings": { "state_dir": "/var/lib/tachyon-pir/registry",
                "public_listen": "127.0.0.1:9410", "difficulty_bits": 20 } }
}
```

- Build `tachyon-pir-publisher` with the `registry` feature.
- `public_listen` serves the write API at `/registry/v1` and `/v1/registry`.
- `state_dir` holds `registry-v1.sqlite`; back it up, it is the whole registry.
- Exactly one publisher per network.

### Abuse controls (spec 9.3)

| Control | Value |
|---|---|
| Proof of work | `difficulty_bits`, default 20 |
| Per-client concurrent registrations | 2 (`pir_control::ClientSlots`) |
| Per-client rate | 6 per minute, configurable |
| Body limit | 1,256 bytes exactly; otherwise `400` or `413` |
| Upload deadline | 15 s, then `408` |
| Global rate | configurable cap per minute; above it `429` with `Retry-After` |

## Query server

A `registry` section with `publisher`, `state_dir`, `role: "combined"`,
`publisher_network`, `publisher_genesis` and `retain_scopes` (default
unlimited). Build with the `registry` feature.

Both binaries accept `--config FILE` and `--check`.

## Observability

Metrics prefixed `tachyon_registry_` (spec 11.8): `ingested_height`,
`anchor_height`, `entries_total`, `open_scope_entries`,
`segments_overflow_total`, `publication_seconds`, `sessions_revoked_total`,
plus `registrations_total{result}` and `pow_rejected_total`. The spec lists the
two height metrics for every product; what they report for the registry, which
has no chain source, is not yet defined.
