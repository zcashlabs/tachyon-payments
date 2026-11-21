# Registry changelog

## Unreleased

- Specified in the draft implementation spec (2026-11-14): protocol
  `tachyon-registry-pir-v1`, query magic `TRQ1`, 1,232-byte entries
  (`key16 || pk(32) || ek(1184)`), chunks of 3,072 registrations, write API with
  proof of work at `/registry/v1`.
