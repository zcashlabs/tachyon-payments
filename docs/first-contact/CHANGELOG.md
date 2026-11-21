# First-contact changelog

## Unreleased

- Specified in the draft implementation spec (2026-11-14): protocol
  `tachyon-first-contact-pir-v1`, query magic `TFQ1`, 1,232-byte entries
  (`key16 || kem_ct(1088) || ciphertext(128)`), scopes are spans of epochs
  closing at 3,072 entries or 64 epochs, envelope kind `0x02`.
