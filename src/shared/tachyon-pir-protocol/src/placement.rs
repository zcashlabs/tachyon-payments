//! Placement of a key in a scope's buckets and rows (spec 6.3).
//!
//! ```text
//! h(s)   = SHA-256(domain || genesis_hash(32) || scope_id(u64 LE) || s(u32 LE) || key)
//! bucket = u32_le(h(scope_seed)[4..8]) mod n_buckets
//! row_k  = u32_le(h(seed_k)[0..4]) mod rows
//! ```
//!
//! `genesis_hash` is in protocol order. `key` is the stored key: `key16` for
//! memo, first-contact and registry, the full tachygram for tachygram.

use sha2::{Digest, Sha256};

use crate::bytes::BlockHash;
use crate::constants::MAX_BUCKETS;
use crate::geometry::Geometry;
use crate::product::Product;

/// A key or bucket count placement cannot use.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    /// The key is not the product's key length.
    #[error("{product} key is {actual} bytes, expected {expected}")]
    KeyLength {
        product: Product,
        expected: usize,
        actual: usize,
    },
    /// `n_buckets` is not a power of two in `1..=64`.
    #[error("n_buckets {0} is not a power of two in 1..=64")]
    Buckets(u32),
    /// `rows` is zero.
    #[error("rows must be positive")]
    Rows,
}

/// Whether `n` is a valid `n_buckets`: a power of two in `1..=MAX_BUCKETS`.
pub const fn valid_n_buckets(n: u32) -> bool {
    n.is_power_of_two() && n <= MAX_BUCKETS
}

fn check_key(product: Product, key: &[u8]) -> Result<(), PlacementError> {
    if key.len() == product.key_len() {
        Ok(())
    } else {
        Err(PlacementError::KeyLength {
            product,
            expected: product.key_len(),
            actual: key.len(),
        })
    }
}

/// `h(seed)` for `key` in scope `scope_id`.
pub fn placement_hash(
    product: Product,
    genesis: &BlockHash,
    scope_id: u64,
    seed: u32,
    key: &[u8],
) -> Result<[u8; 32], PlacementError> {
    check_key(product, key)?;
    Ok(Sha256::new()
        .chain_update(product.placement_domain())
        .chain_update(genesis.protocol_bytes())
        .chain_update(scope_id.to_le_bytes())
        .chain_update(seed.to_le_bytes())
        .chain_update(key)
        .finalize()
        .into())
}

/// The bucket a placement hash under the scope seed selects.
pub fn bucket_of_hash(hash: &[u8; 32], n_buckets: u32) -> Result<u32, PlacementError> {
    if !valid_n_buckets(n_buckets) {
        return Err(PlacementError::Buckets(n_buckets));
    }
    Ok(u32::from_le_bytes(hash[4..8].try_into().expect("four bytes")) % n_buckets)
}

/// The row a placement hash under a segment seed selects.
pub fn row_of_hash(hash: &[u8; 32], rows: u32) -> Result<u32, PlacementError> {
    if rows == 0 {
        return Err(PlacementError::Rows);
    }
    Ok(u32::from_le_bytes(hash[0..4].try_into().expect("four bytes")) % rows)
}

/// The bucket of `key` in scope `scope_id`, computed under the scope seed
/// (the seed of segment 0, equal in every bucket).
pub fn bucket(
    product: Product,
    genesis: &BlockHash,
    scope_id: u64,
    scope_seed: u32,
    n_buckets: u32,
    key: &[u8],
) -> Result<u32, PlacementError> {
    if !valid_n_buckets(n_buckets) {
        return Err(PlacementError::Buckets(n_buckets));
    }
    bucket_of_hash(
        &placement_hash(product, genesis, scope_id, scope_seed, key)?,
        n_buckets,
    )
}

/// The row of `key` in one segment, computed under that segment's seed.
pub fn row(
    product: Product,
    genesis: &BlockHash,
    scope_id: u64,
    seed: u32,
    rows: u32,
    key: &[u8],
) -> Result<u32, PlacementError> {
    if rows == 0 {
        return Err(PlacementError::Rows);
    }
    row_of_hash(
        &placement_hash(product, genesis, scope_id, seed, key)?,
        rows,
    )
}

/// `n_buckets` for a scope of `entries` entries at `geometry` (spec 6.3).
///
/// The smallest power of two `n` in `1..=64` whose expected load per row,
/// `entries / (n * rows)`, is at or below the product's target load, that is
/// `entries <= target * n * rows` in exact integer arithmetic. At
/// `tachyon-4k8` that is `entries <= target * 4096 * n`: memo 139,264 entries
/// per bucket, tachygram 507,904, first-contact and registry 4,096.
///
/// When even 64 buckets exceed the target the result is still 64: buckets are
/// capped at [`MAX_BUCKETS`] and the excess load is carried by overflow
/// segments (spec 6.4), and beyond `MAX_SEGMENTS` the publication fails.
pub const fn n_buckets_for(product: Product, geometry: Geometry, entries: u64) -> u32 {
    let per_bucket = product.target_load() * geometry.rows() as u64;
    let mut n = 1u32;
    while n < MAX_BUCKETS && entries > per_bucket * n as u64 {
        n *= 2;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    fn genesis() -> BlockHash {
        BlockHash::from_protocol_bytes([7; 32])
    }

    #[test]
    fn key_length_is_checked() {
        let err = placement_hash(Product::Memo, &genesis(), 0, 0, &[1; 32]).unwrap_err();
        assert_eq!(
            err,
            PlacementError::KeyLength {
                product: Product::Memo,
                expected: 16,
                actual: 32
            }
        );
        assert!(placement_hash(Product::Tachygram, &genesis(), 0, 0, &[1; 16]).is_err());
        assert!(placement_hash(Product::Tachygram, &genesis(), 0, 0, &[1; 32]).is_ok());
    }

    #[test]
    fn bucket_counts_must_be_powers_of_two_up_to_64() {
        for n in [0, 3, 5, 96, 128] {
            assert_eq!(
                bucket(Product::Memo, &genesis(), 0, 0, n, &[1; 16]),
                Err(PlacementError::Buckets(n))
            );
        }
        for n in [1, 2, 4, 8, 16, 32, 64] {
            assert!(bucket(Product::Memo, &genesis(), 0, 0, n, &[1; 16]).unwrap() < n);
        }
        assert_eq!(
            row(Product::Memo, &genesis(), 0, 0, 0, &[1; 16]),
            Err(PlacementError::Rows)
        );
    }

    #[test]
    fn scope_and_seed_and_product_separate_hashes() {
        let key = [9u8; 16];
        let base = placement_hash(Product::Memo, &genesis(), 1, 0, &key).unwrap();
        assert_ne!(
            base,
            placement_hash(Product::Memo, &genesis(), 2, 0, &key).unwrap()
        );
        assert_ne!(
            base,
            placement_hash(Product::Memo, &genesis(), 1, 1, &key).unwrap()
        );
        assert_ne!(
            base,
            placement_hash(Product::Registry, &genesis(), 1, 0, &key).unwrap()
        );
        let other = BlockHash::from_protocol_bytes([8; 32]);
        assert_ne!(
            base,
            placement_hash(Product::Memo, &other, 1, 0, &key).unwrap()
        );
    }

    #[test]
    fn n_buckets_rule() {
        let g = Geometry::Tachyon4k8;
        assert_eq!(n_buckets_for(Product::Memo, g, 0), 1);
        assert_eq!(n_buckets_for(Product::Memo, g, 139_264), 1);
        assert_eq!(n_buckets_for(Product::Memo, g, 139_265), 2);
        assert_eq!(n_buckets_for(Product::Tachygram, g, 190_000), 1);
        assert_eq!(n_buckets_for(Product::Tachygram, g, 507_905), 2);
        assert_eq!(n_buckets_for(Product::FirstContact, g, 4_097), 2);
        assert_eq!(n_buckets_for(Product::Registry, g, 3_072), 1);
        assert_eq!(n_buckets_for(Product::Memo, g, 139_264 * 64), 64);
        assert_eq!(n_buckets_for(Product::Memo, g, 139_264 * 64 + 1), 64);
        assert_eq!(n_buckets_for(Product::Memo, g, u64::MAX / 2), 64);
        assert_eq!(
            n_buckets_for(Product::Memo, Geometry::Tachyon2k8, 69_633),
            2
        );
    }
}
