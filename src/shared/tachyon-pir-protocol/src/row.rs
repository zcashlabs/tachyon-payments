//! The canonical row codec (spec 6.2, 8.4), mirroring Status `decode_row` and
//! the txid client's placement recheck.
//!
//! A canonical row holds its occupied entries first, then all-zero slots, then
//! a zero tail up to [`ROW_BYTES`]. Every occupied entry has a nonzero key;
//! keys are non-decreasing (strictly increasing for tachygram, whose keys are
//! unique). The ordinal part of the `(key, ordinal)` order is not stored, so
//! only the publisher can keep equal keys in ordinal order.

use crate::bytes::BlockHash;
use crate::constants::ROW_BYTES;
use crate::entry::Entry;
use crate::placement::{PlacementError, bucket_of_hash, placement_hash, row_of_hash};
use crate::product::Product;

/// Why a row, or entries for one, is not canonical.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RowError {
    #[error("row is {0} bytes, expected 8192")]
    Length(usize),
    #[error("{count} entries exceed the {slots} slots of a {product} row")]
    TooManyEntries {
        product: Product,
        count: usize,
        slots: usize,
    },
    #[error("entry {slot} is a {actual} entry in a {expected} row")]
    ProductMismatch {
        slot: usize,
        expected: Product,
        actual: Product,
    },
    #[error("bytes after the last slot are not zero")]
    NonzeroTail,
    #[error("entry {0} follows an empty slot")]
    EntryAfterEmpty(usize),
    #[error("entry {0} has an all-zero key")]
    ZeroKey(usize),
    #[error("entry {0} is out of key order")]
    Order(usize),
    #[error("entry {0} does not place into this bucket and row")]
    Placement(usize),
    #[error("entry {0} has a block height outside the scope")]
    HeightOutOfRange(usize),
    #[error("registry entry {0} key is not its address digest prefix")]
    RegistryKey(usize),
    #[error("tachygram rows need the scope's height range")]
    MissingHeights,
    #[error("row context is for {context}, not {product}")]
    ContextProduct { product: Product, context: Product },
    #[error(transparent)]
    InvalidPlacement(#[from] PlacementError),
}

/// What a row reader knows about the row it decoded: the table's placement
/// parameters and the queried row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowContext {
    pub product: Product,
    /// Genesis block hash (protocol order inside the type).
    pub genesis: BlockHash,
    pub scope_id: u64,
    pub n_buckets: u32,
    /// The seed of segment 0, which places keys in buckets.
    pub scope_seed: u32,
    /// This table's bucket.
    pub bucket: u32,
    /// This table's (segment's) seed, which places keys in rows.
    pub seed: u32,
    /// Rows of this table's geometry.
    pub rows: u32,
    /// The queried row.
    pub row: u32,
    /// The scope's `[first_height, last_height]`; required for tachygram,
    /// ignored by the other products.
    pub heights: Option<(u32, u32)>,
}

fn order_ok(product: Product, previous: &[u8], key: &[u8]) -> bool {
    match product {
        Product::Tachygram => previous < key,
        _ => previous <= key,
    }
}

/// Packs `entries` into one row and zero-pads it.
///
/// The publisher hands entries already in `(key, ordinal)` order; this checks
/// what can be checked from the entries (product, count, nonzero keys, key
/// order) and does not sort. Placement is the table builder's job and is
/// checked by [`validate_row`].
pub fn encode_row(product: Product, entries: &[Entry]) -> Result<Vec<u8>, RowError> {
    if entries.len() > product.slots() {
        return Err(RowError::TooManyEntries {
            product,
            count: entries.len(),
            slots: product.slots(),
        });
    }
    let size = product.entry_bytes();
    let mut row = vec![0u8; ROW_BYTES];
    for (slot, entry) in entries.iter().enumerate() {
        if entry.product() != product {
            return Err(RowError::ProductMismatch {
                slot,
                expected: product,
                actual: entry.product(),
            });
        }
        if entry.key().iter().all(|b| *b == 0) {
            return Err(RowError::ZeroKey(slot));
        }
        if slot > 0 && !order_ok(product, entries[slot - 1].key(), entry.key()) {
            return Err(RowError::Order(slot));
        }
        entry.encode_into(&mut row[slot * size..(slot + 1) * size]);
    }
    Ok(row)
}

/// Validates a decoded row and returns its occupied entries in row order.
///
/// Checks: exact length; zero tail; occupied entries first and only all-zero
/// slots after them; nonzero keys; key order; every entry's bucket (under the
/// scope seed) equal to this table's bucket and its row (under this segment's
/// seed) equal to the queried row; for tachygram every `blk_height` inside the
/// scope's heights; for registry every key equal to its address digest prefix.
pub fn validate_row(
    product: Product,
    row: &[u8],
    context: &RowContext,
) -> Result<Vec<Entry>, RowError> {
    if context.product != product {
        return Err(RowError::ContextProduct {
            product,
            context: context.product,
        });
    }
    if row.len() != ROW_BYTES {
        return Err(RowError::Length(row.len()));
    }
    let size = product.entry_bytes();
    let used = product.slots() * size;
    if row[used..].iter().any(|b| *b != 0) {
        return Err(RowError::NonzeroTail);
    }
    let heights = match product {
        Product::Tachygram => Some(context.heights.ok_or(RowError::MissingHeights)?),
        _ => None,
    };
    let mut entries: Vec<Entry> = Vec::new();
    let mut empty = false;
    for (slot, bytes) in row[..used].chunks_exact(size).enumerate() {
        if bytes.iter().all(|b| *b == 0) {
            empty = true;
            continue;
        }
        if empty {
            return Err(RowError::EntryAfterEmpty(slot));
        }
        let entry = Entry::decode(product, bytes).expect("slot has the entry length");
        let key = entry.key();
        if key.iter().all(|b| *b == 0) {
            return Err(RowError::ZeroKey(slot));
        }
        if let Some(previous) = entries.last()
            && !order_ok(product, previous.key(), key)
        {
            return Err(RowError::Order(slot));
        }
        let scope_hash = placement_hash(
            product,
            &context.genesis,
            context.scope_id,
            context.scope_seed,
            key,
        )?;
        let row_hash = if context.seed == context.scope_seed {
            scope_hash
        } else {
            placement_hash(
                product,
                &context.genesis,
                context.scope_id,
                context.seed,
                key,
            )?
        };
        if bucket_of_hash(&scope_hash, context.n_buckets)? != context.bucket
            || row_of_hash(&row_hash, context.rows)? != context.row
        {
            return Err(RowError::Placement(slot));
        }
        match &entry {
            Entry::Tachygram(e) => {
                let (first, last) = heights.expect("tachygram heights");
                if e.blk_height < first || e.blk_height > last {
                    return Err(RowError::HeightOutOfRange(slot));
                }
            }
            Entry::Registry(e) if e.key != e.address.key16() => {
                return Err(RowError::RegistryKey(slot));
            }
            _ => {}
        }
        entries.push(entry);
    }
    Ok(entries)
}

/// The entries whose stored key equals `key`, in row order (which is ordinal
/// order within one segment).
pub fn lookup<'a>(entries: &'a [Entry], key: &[u8]) -> Vec<&'a Entry> {
    entries.iter().filter(|e| e.key() == key).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{MemoEntry, RegistryEntry, TachygramEntry};
    use crate::placement::{bucket, row};
    use crate::registry::Address;

    const GENESIS: BlockHash = BlockHash::from_protocol_bytes([3; 32]);

    fn context(product: Product) -> RowContext {
        RowContext {
            product,
            genesis: GENESIS,
            scope_id: 4,
            n_buckets: 2,
            scope_seed: 1,
            bucket: 0,
            seed: 1,
            rows: 4096,
            row: 0,
            heights: Some((100, 200)),
        }
    }

    /// Context for the bucket and row `key` lands in under `seed`.
    fn context_for(product: Product, key: &[u8], scope_seed: u32, seed: u32) -> RowContext {
        let mut c = context(product);
        c.scope_seed = scope_seed;
        c.seed = seed;
        c.bucket = bucket(product, &GENESIS, c.scope_id, scope_seed, c.n_buckets, key).unwrap();
        c.row = row(product, &GENESIS, c.scope_id, seed, c.rows, key).unwrap();
        c
    }

    /// Keys of `product` that land in the same bucket and row as `first` does.
    fn colliding(product: Product, c: &RowContext, count: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut counter = 0u64;
        while out.len() < count {
            let mut key = vec![0u8; product.key_len()];
            key[..8].copy_from_slice(&counter.to_le_bytes());
            key[product.key_len() - 1] = 0x5a;
            counter += 1;
            let b = bucket(
                product,
                &GENESIS,
                c.scope_id,
                c.scope_seed,
                c.n_buckets,
                &key,
            )
            .unwrap();
            let r = row(product, &GENESIS, c.scope_id, c.seed, c.rows, &key).unwrap();
            if b == c.bucket && r == c.row {
                out.push(key);
            }
        }
        out.sort();
        out
    }

    fn memo(key: &[u8], fill: u8) -> Entry {
        Entry::Memo(MemoEntry {
            key: key.try_into().unwrap(),
            ciphertext: [fill; 128],
        })
    }

    fn tg(key: &[u8], height: u32) -> Entry {
        Entry::Tachygram(TachygramEntry {
            tachygram: key.try_into().unwrap(),
            blk_height: height,
            stamp_ordinal: 0,
        })
    }

    #[test]
    fn encode_then_validate_round_trips_with_duplicates() {
        let mut c = context(Product::Memo);
        c.row = 17;
        let keys = colliding(Product::Memo, &c, 2);
        let entries = vec![memo(&keys[0], 1), memo(&keys[1], 2), memo(&keys[1], 3)];
        let bytes = encode_row(Product::Memo, &entries).unwrap();
        assert_eq!(bytes.len(), ROW_BYTES);
        assert!(bytes[3 * 144..].iter().all(|b| *b == 0));
        let decoded = validate_row(Product::Memo, &bytes, &c).unwrap();
        assert_eq!(decoded, entries);
        let hits = lookup(&decoded, &keys[1]);
        assert_eq!(hits, vec![&entries[1], &entries[2]]);
        assert!(lookup(&decoded, &[0x11; 16]).is_empty());
    }

    #[test]
    fn empty_row_is_canonical() {
        let bytes = encode_row(Product::Tachygram, &[]).unwrap();
        assert_eq!(
            validate_row(Product::Tachygram, &bytes, &context(Product::Tachygram)),
            Ok(vec![])
        );
    }

    #[test]
    fn encode_refuses_bad_entry_lists() {
        let key = [1u8; 16];
        let full: Vec<Entry> = (0..57).map(|_| memo(&key, 1)).collect();
        assert!(matches!(
            encode_row(Product::Memo, &full),
            Err(RowError::TooManyEntries {
                count: 57,
                slots: 56,
                ..
            })
        ));
        assert!(encode_row(Product::Memo, &full[..56]).is_ok());
        assert_eq!(
            encode_row(Product::Memo, &[memo(&[2; 16], 0), memo(&[1; 16], 0)]),
            Err(RowError::Order(1))
        );
        assert_eq!(
            encode_row(Product::Memo, &[memo(&[0; 16], 1)]),
            Err(RowError::ZeroKey(0))
        );
        assert_eq!(
            encode_row(Product::Tachygram, &[tg(&[1; 32], 1), tg(&[1; 32], 2)]),
            Err(RowError::Order(1))
        );
        assert!(matches!(
            encode_row(Product::Tachygram, &[memo(&[1; 16], 0)]),
            Err(RowError::ProductMismatch { slot: 0, .. })
        ));
    }

    #[test]
    fn validate_refuses_non_canonical_rows() {
        let mut c = context(Product::Tachygram);
        c.row = 5;
        let keys = colliding(Product::Tachygram, &c, 3);
        let entries = vec![tg(&keys[0], 100), tg(&keys[1], 150), tg(&keys[2], 200)];
        let good = encode_row(Product::Tachygram, &entries).unwrap();
        assert_eq!(
            validate_row(Product::Tachygram, &good, &c).unwrap(),
            entries
        );

        assert_eq!(
            validate_row(Product::Tachygram, &good[..8191], &c),
            Err(RowError::Length(8191))
        );
        let mut long = good.clone();
        long.push(0);
        assert_eq!(
            validate_row(Product::Tachygram, &long, &c),
            Err(RowError::Length(8193))
        );

        let mut tail = good.clone();
        tail[8191] = 1;
        assert_eq!(
            validate_row(Product::Tachygram, &tail, &c),
            Err(RowError::NonzeroTail)
        );

        // A gap: entry 2 moved to slot 3.
        let mut gap = good.clone();
        gap.copy_within(80..120, 120);
        gap[80..120].fill(0);
        assert_eq!(
            validate_row(Product::Tachygram, &gap, &c),
            Err(RowError::EntryAfterEmpty(3))
        );

        // Swapped order.
        let mut swapped = good.clone();
        swapped[..40].copy_from_slice(&good[40..80]);
        swapped[40..80].copy_from_slice(&good[..40]);
        assert_eq!(
            validate_row(Product::Tachygram, &swapped, &c),
            Err(RowError::Order(1))
        );

        // A duplicate tachygram.
        let mut dup = good.clone();
        dup[40..80].copy_from_slice(&good[..40]);
        assert_eq!(
            validate_row(Product::Tachygram, &dup, &c),
            Err(RowError::Order(1))
        );

        // Zero key with nonzero trailer.
        let mut zero = good.clone();
        zero[80..112].fill(0);
        assert_eq!(
            validate_row(Product::Tachygram, &zero, &c),
            Err(RowError::ZeroKey(2))
        );

        // Heights outside the scope.
        for height in [99, 201] {
            let bytes = encode_row(Product::Tachygram, &[tg(&keys[0], height)]).unwrap();
            assert_eq!(
                validate_row(Product::Tachygram, &bytes, &c),
                Err(RowError::HeightOutOfRange(0))
            );
        }
        assert!(matches!(
            validate_row(Product::Memo, &good, &c),
            Err(RowError::ContextProduct { .. })
        ));
        let mut no_heights = c;
        no_heights.heights = None;
        assert_eq!(
            validate_row(Product::Tachygram, &good, &no_heights),
            Err(RowError::MissingHeights)
        );

        // The same row read as another row, bucket, seed or scope.
        for wrong in [
            RowContext { row: 6, ..c },
            RowContext {
                bucket: 1 - c.bucket,
                ..c
            },
            RowContext { scope_id: 5, ..c },
            RowContext { seed: 9, ..c },
            RowContext {
                genesis: BlockHash::from_protocol_bytes([4; 32]),
                ..c
            },
        ] {
            assert_eq!(
                validate_row(Product::Tachygram, &good, &wrong),
                Err(RowError::Placement(0))
            );
        }
    }

    #[test]
    fn overflow_segments_bucket_by_scope_seed_and_row_by_their_own() {
        let key = [0x42u8; 16];
        let c = context_for(Product::Memo, &key, 0, 5);
        let bytes = encode_row(Product::Memo, &[memo(&key, 9)]).unwrap();
        assert_eq!(validate_row(Product::Memo, &bytes, &c).unwrap().len(), 1);
        // Reading it as if the bucket were also chosen under the segment seed
        // fails whenever that would pick another bucket or row.
        let segment_bucket =
            bucket(Product::Memo, &GENESIS, c.scope_id, 5, c.n_buckets, &key).unwrap();
        let scope_row = row(Product::Memo, &GENESIS, c.scope_id, 0, c.rows, &key).unwrap();
        if scope_row != c.row {
            let wrong = RowContext { seed: 0, ..c };
            assert_eq!(
                validate_row(Product::Memo, &bytes, &wrong),
                Err(RowError::Placement(0))
            );
        }
        if segment_bucket != c.bucket {
            let wrong = RowContext { scope_seed: 5, ..c };
            assert_eq!(
                validate_row(Product::Memo, &bytes, &wrong),
                Err(RowError::Placement(0))
            );
        }
    }

    #[test]
    fn registry_keys_must_be_address_digests() {
        let address = Address {
            pk: [1; 32],
            ek: [2; 1184],
        };
        let entry = RegistryEntry::new(address.clone());
        let c = context_for(Product::Registry, &entry.key, 0, 0);
        let c = RowContext {
            n_buckets: 1,
            bucket: 0,
            ..c
        };
        let bytes = encode_row(
            Product::Registry,
            &[Entry::Registry(Box::new(entry.clone()))],
        )
        .unwrap();
        assert_eq!(
            validate_row(Product::Registry, &bytes, &c).unwrap().len(),
            1
        );

        let mut forged = entry.clone();
        forged.address.ek[0] ^= 1;
        let bytes = encode_row(Product::Registry, &[Entry::Registry(Box::new(forged))]).unwrap();
        assert_eq!(
            validate_row(Product::Registry, &bytes, &c),
            Err(RowError::RegistryKey(0))
        );
    }
}
