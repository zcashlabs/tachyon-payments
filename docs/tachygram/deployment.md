# Tachygram deployment

Status: not deployed. Configuration as defined in spec 11.7.

## Publisher

```json
{
  "management_listen": "127.0.0.1:9400",
  "chain": { "rpc_url": "http://tachyscan.cnode.phd:18232", "no_auth": true, "poll_ms": 1000 },
  "tachygram": { "enabled": true, "settings": { "state_dir": "/var/lib/tachyon-pir/tachygram" } }
}
```

- Build `tachyon-pir-publisher` with the `tachygram` feature.
- The publisher needs only `getblockchaininfo`, `getblockcount`, `getblockhash`
  and `getblock` at verbosity 2. It refuses to start if the node reports no
  `NuTachyon` upgrade.
- One publisher per network.

## Query server

A `tachygram` section with `publisher`, `state_dir`, `role: "combined"`,
`publisher_network`, `publisher_genesis` and `retain_scopes` (default 64).
Build with the `tachygram` feature.

Memory: about 160 MiB per resident table (wallet-pir's figure, to be
re-measured). The testnet seals an epoch about every 10.2 hours, so 64 retained
epochs is roughly 27 days of history and about 10 GiB per host at one table per
epoch.

Both binaries accept `--config FILE` and `--check`.

## Observability

Metrics prefixed `tachyon_tachygram_`: `ingested_height`, `anchor_height`,
`entries_total`, `open_scope_entries`, `segments_overflow_total`,
`publication_seconds`, `sessions_revoked_total`.

Alert on: ingestion stopped by a duplicate tachygram (spec 8.3); a publication
that fails at `MAX_SEGMENTS`; `ingested_height` falling behind the node tip.
