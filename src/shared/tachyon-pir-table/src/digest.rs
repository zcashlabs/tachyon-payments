//! Content digests of tables (spec 10.3, 10.4).

use sha2::{Digest, Sha256};

/// `rows_sha256`: SHA-256 of a table's row bytes, `rows x 8192`, row-major.
pub fn rows_sha256(rows: &[u8]) -> [u8; 32] {
    Sha256::digest(rows).into()
}

/// `public_sha256`: SHA-256 of a table's public setup bytes.
pub fn public_sha256(public: &[u8]) -> [u8; 32] {
    Sha256::digest(public).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digests_are_plain_sha256() {
        // SHA-256 of the empty string.
        assert_eq!(
            rows_sha256(&[]),
            [
                0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
                0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
                0x78, 0x52, 0xb8, 0x55
            ]
        );
        assert_eq!(public_sha256(b"abc"), rows_sha256(b"abc"));
    }
}
