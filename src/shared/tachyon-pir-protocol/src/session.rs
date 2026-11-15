//! Content-addressed session ids (spec 10.4).
//!
//! ```text
//! session_id = SHA-256("tachyon-pir/v1/session\0" || protocol_len(u8) || protocol
//!                      || scope_id(u64) || bucket(u32) || segment(u32)
//!                      || rows(u32) || seed(u32) || rows_sha256 || public_sha256)
//! ```
//!
//! Integers are little-endian. Generation and anchor are excluded, so an
//! unchanged sealed table keeps its session across publications.

use sha2::{Digest, Sha256};

use crate::product::Product;

/// Everything of one table that enters its session id, besides the product.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionParts {
    pub scope_id: u64,
    pub bucket: u32,
    pub segment: u32,
    /// Rows of the table's geometry.
    pub rows: u32,
    pub seed: u32,
    pub rows_sha256: [u8; 32],
    pub public_sha256: [u8; 32],
}

/// The session id of one table of `product`.
pub fn session_id(product: Product, parts: &SessionParts) -> [u8; 32] {
    let protocol = product.protocol().as_bytes();
    let protocol_len = u8::try_from(protocol.len()).expect("protocol strings are short");
    Sha256::new()
        .chain_update(b"tachyon-pir/v1/session\0")
        .chain_update([protocol_len])
        .chain_update(protocol)
        .chain_update(parts.scope_id.to_le_bytes())
        .chain_update(parts.bucket.to_le_bytes())
        .chain_update(parts.segment.to_le_bytes())
        .chain_update(parts.rows.to_le_bytes())
        .chain_update(parts.seed.to_le_bytes())
        .chain_update(parts.rows_sha256)
        .chain_update(parts.public_sha256)
        .finalize()
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_field_enters_the_id() {
        let parts = SessionParts {
            scope_id: 1,
            bucket: 2,
            segment: 3,
            rows: 4096,
            seed: 5,
            rows_sha256: [6; 32],
            public_sha256: [7; 32],
        };
        let base = session_id(Product::Memo, &parts);
        let variants = [
            SessionParts {
                scope_id: 9,
                ..parts
            },
            SessionParts { bucket: 9, ..parts },
            SessionParts {
                segment: 9,
                ..parts
            },
            SessionParts {
                rows: 2048,
                ..parts
            },
            SessionParts { seed: 9, ..parts },
            SessionParts {
                rows_sha256: [9; 32],
                ..parts
            },
            SessionParts {
                public_sha256: [9; 32],
                ..parts
            },
        ];
        for variant in variants {
            assert_ne!(session_id(Product::Memo, &variant), base);
        }
        assert_ne!(session_id(Product::Tachygram, &parts), base);
    }
}
