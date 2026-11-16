//! Building a scope's tables (spec 6.2-6.4).
//!
//! A scope's entries arrive in ordinal order. The builder picks `n_buckets`
//! with `n_buckets_for`, then searches the scope seed: the smallest `s` in
//! `0..MAX_SEEDS` under which, with bucket and row both taken from `h(s)`, no
//! row of any bucket holds more than `SLOTS` entries. When no seed fits, the
//! scope seed is 0 and each bucket is filled under seed 0 in ordinal order,
//! spilling entries whose row is full into further segments; each later
//! segment takes the smallest seed under which all of its entries fit, or is
//! filled under seed 0 and spills again. Within a row, entries are sorted by
//! `(key, ordinal)`.
//!
//! Each entry's placement hash is computed once per seed tried, in parallel
//! on the current rayon pool.

use std::fmt;

use rayon::prelude::*;
use tachyon_pir_protocol::placement::{bucket_of_hash, row_of_hash};
use tachyon_pir_protocol::{
    BlockHash, Entry, Geometry, MAX_SEEDS, MAX_SEGMENTS, PUBLISHED_GEOMETRY, Product, ROW_BYTES,
    RowContext, encode_row, n_buckets_for, placement_hash,
};

use crate::digest::rows_sha256;
use crate::error::Error;

/// One built table: one (bucket, segment) of a scope.
#[derive(Clone, PartialEq, Eq)]
pub struct BuiltTable {
    pub bucket: u32,
    pub segment: u32,
    /// The table's geometry: [`PUBLISHED_GEOMETRY`] for every table the
    /// public builders produce.
    pub geometry: Geometry,
    /// The seed rows are placed under: the scope seed in segment 0.
    pub seed: u32,
    /// Occupied entries in the table.
    pub entries: u32,
    /// `rows x 8192` bytes, row-major.
    pub rows: Vec<u8>,
    /// SHA-256 of `rows`.
    pub rows_sha256: [u8; 32],
}

impl BuiltTable {
    /// Row `row`'s 8,192 bytes.
    pub fn row(&self, row: u32) -> &[u8] {
        let at = row as usize * ROW_BYTES;
        &self.rows[at..at + ROW_BYTES]
    }
}

impl fmt::Debug for BuiltTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the 32 MiB of rows.
        f.debug_struct("BuiltTable")
            .field("bucket", &self.bucket)
            .field("segment", &self.segment)
            .field("geometry", &self.geometry)
            .field("seed", &self.seed)
            .field("entries", &self.entries)
            .field("rows_len", &self.rows.len())
            .field("rows_sha256", &hex(&self.rows_sha256))
            .finish()
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The tables of one scope, in ascending `(bucket, segment)` order.
///
/// A scope with no entries has `n_buckets = 1`, `scope_seed = 0` and no
/// tables (spec 10.3). Otherwise every bucket has a segment-0 table, possibly
/// with zero entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeTables {
    pub n_buckets: u32,
    /// The seed of segment 0, equal in every bucket.
    pub scope_seed: u32,
    pub tables: Vec<BuiltTable>,
}

impl ScopeTables {
    /// Entries over all tables.
    pub fn entries(&self) -> u64 {
        self.tables.iter().map(|t| u64::from(t.entries)).sum()
    }

    /// The tables of `bucket`, in segment order.
    pub fn segments(&self, bucket: u32) -> &[BuiltTable] {
        let start = self.tables.partition_point(|t| t.bucket < bucket);
        let end = self.tables.partition_point(|t| t.bucket <= bucket);
        &self.tables[start..end]
    }

    /// The row context of row 0 of `table`, which [`crate::validate_table`]
    /// takes as its template. `heights` is the scope's
    /// `[first_height, last_height]`, required for tachygram.
    pub fn row_context(
        &self,
        product: Product,
        genesis: &BlockHash,
        scope_id: u64,
        table: &BuiltTable,
        heights: Option<(u32, u32)>,
    ) -> RowContext {
        RowContext {
            product,
            genesis: *genesis,
            scope_id,
            n_buckets: self.n_buckets,
            scope_seed: self.scope_seed,
            bucket: table.bucket,
            seed: table.seed,
            rows: table.geometry.rows(),
            row: 0,
            heights,
        }
    }
}

/// Builds the tables of one scope at [`PUBLISHED_GEOMETRY`].
///
/// `entries` are the scope's entries of `product` in ordinal order; each
/// entry's key is its placement key. Runs on the current rayon pool (call it
/// inside `ThreadPool::install` to choose one).
pub fn build_scope(
    product: Product,
    genesis: &BlockHash,
    scope_id: u64,
    entries: &[Entry],
) -> Result<ScopeTables, Error> {
    build_scope_at(product, PUBLISHED_GEOMETRY, genesis, scope_id, entries)
}

/// [`build_scope`] at any geometry. Version 1 publishes only
/// [`PUBLISHED_GEOMETRY`].
pub(crate) fn build_scope_at(
    product: Product,
    geometry: Geometry,
    genesis: &BlockHash,
    scope_id: u64,
    entries: &[Entry],
) -> Result<ScopeTables, Error> {
    check_entries(product, entries)?;
    if entries.is_empty() {
        return Ok(ScopeTables {
            n_buckets: 1,
            scope_seed: 0,
            tables: Vec::new(),
        });
    }
    let shape = Shape {
        product,
        geometry,
        rows: geometry.rows(),
        slots: product.slots(),
        n_buckets: n_buckets_for(product, geometry, entries.len() as u64),
    };
    let placer = Placer {
        product,
        genesis,
        scope_id,
        entries,
    };

    let mut under_zero = None;
    for seed in 0..MAX_SEEDS {
        let places = placer.place_all(seed, shape.n_buckets, shape.rows)?;
        let cells = places
            .iter()
            .map(|p| p.bucket as usize * shape.rows as usize + p.row as usize);
        if fits(
            cells,
            shape.n_buckets as usize * shape.rows as usize,
            shape.slots,
        ) {
            let tables = by_bucket(&places, shape.n_buckets)
                .into_par_iter()
                .enumerate()
                .map(|(bucket, members)| {
                    encode_table(&shape, entries, bucket as u32, 0, seed, &members)
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(ScopeTables {
                n_buckets: shape.n_buckets,
                scope_seed: seed,
                tables,
            });
        }
        if seed == 0 {
            under_zero = Some(places);
        }
    }

    // No seed fits: buckets under seed 0, each bucket segmented.
    let places = under_zero.expect("seed 0 is always tried");
    let per_bucket = by_bucket(&places, shape.n_buckets)
        .into_par_iter()
        .enumerate()
        .map(|(bucket, members)| segment_bucket(&shape, &placer, bucket as u32, members))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScopeTables {
        n_buckets: shape.n_buckets,
        scope_seed: 0,
        tables: per_bucket.into_iter().flatten().collect(),
    })
}

/// The fixed shape of a scope's tables.
struct Shape {
    product: Product,
    geometry: Geometry,
    rows: u32,
    slots: usize,
    n_buckets: u32,
}

/// An entry's bucket and row under one seed.
#[derive(Clone, Copy)]
struct Place {
    bucket: u32,
    row: u32,
}

/// An entry (by ordinal index) and its row in the table being built.
#[derive(Clone, Copy)]
struct Member {
    index: u32,
    row: u32,
}

struct Placer<'a> {
    product: Product,
    genesis: &'a BlockHash,
    scope_id: u64,
    entries: &'a [Entry],
}

impl Placer<'_> {
    fn hash(&self, entry: &Entry, seed: u32) -> Result<[u8; 32], Error> {
        Ok(placement_hash(
            self.product,
            self.genesis,
            self.scope_id,
            seed,
            entry.key(),
        )?)
    }

    /// Bucket and row of every entry, both under `seed`, in ordinal order.
    fn place_all(&self, seed: u32, n_buckets: u32, rows: u32) -> Result<Vec<Place>, Error> {
        self.entries
            .par_iter()
            .map(|entry| {
                let h = self.hash(entry, seed)?;
                Ok(Place {
                    bucket: bucket_of_hash(&h, n_buckets)?,
                    row: row_of_hash(&h, rows)?,
                })
            })
            .collect()
    }

    /// `members` with their rows recomputed under `seed`.
    fn rows_under(&self, members: &[Member], seed: u32, rows: u32) -> Result<Vec<Member>, Error> {
        members
            .par_iter()
            .map(|m| {
                let h = self.hash(&self.entries[m.index as usize], seed)?;
                Ok(Member {
                    index: m.index,
                    row: row_of_hash(&h, rows)?,
                })
            })
            .collect()
    }
}

/// Refuses entries the table cannot hold: other products, all-zero keys and,
/// for tachygram, repeated keys. Reports the lowest offending index, and for
/// repeated keys the pair with the smallest key.
fn check_entries(product: Product, entries: &[Entry]) -> Result<(), Error> {
    if u32::try_from(entries.len()).is_err() {
        return Err(Error::TooManyEntries(entries.len()));
    }
    let bad = entries
        .par_iter()
        .enumerate()
        .find_first(|(_, e)| e.product() != product || e.key().iter().all(|b| *b == 0));
    if let Some((index, entry)) = bad {
        return Err(if entry.product() != product {
            Error::EntryProduct {
                index,
                expected: product,
                actual: entry.product(),
            }
        } else {
            Error::ZeroKey { index }
        });
    }
    if product == Product::Tachygram {
        let mut order: Vec<u32> = (0..entries.len() as u32).collect();
        order.par_sort_unstable_by(|a, b| {
            entries[*a as usize]
                .key()
                .cmp(entries[*b as usize].key())
                .then(a.cmp(b))
        });
        if let Some(pair) = order
            .par_windows(2)
            .find_first(|w| entries[w[0] as usize].key() == entries[w[1] as usize].key())
        {
            return Err(Error::DuplicateKey {
                first: pair[0] as usize,
                second: pair[1] as usize,
            });
        }
    }
    Ok(())
}

/// Whether no cell receives more than `slots` entries.
fn fits(cells: impl Iterator<Item = usize>, n_cells: usize, slots: usize) -> bool {
    let mut load = vec![0u32; n_cells];
    for cell in cells {
        load[cell] += 1;
        if load[cell] as usize > slots {
            return false;
        }
    }
    true
}

/// Each bucket's members in ordinal order, with their rows.
fn by_bucket(places: &[Place], n_buckets: u32) -> Vec<Vec<Member>> {
    let mut members = vec![Vec::new(); n_buckets as usize];
    for (index, place) in places.iter().enumerate() {
        members[place.bucket as usize].push(Member {
            index: index as u32,
            row: place.row,
        });
    }
    members
}

/// Places `members` in ordinal order, each in its row while the row has a
/// free slot. Returns the placed members and the spill, both in ordinal
/// order.
fn fill(members: &[Member], rows: u32, slots: usize) -> (Vec<Member>, Vec<Member>) {
    let mut load = vec![0usize; rows as usize];
    let mut placed = Vec::with_capacity(members.len());
    let mut spilled = Vec::new();
    for m in members {
        let load = &mut load[m.row as usize];
        if *load < slots {
            *load += 1;
            placed.push(*m);
        } else {
            spilled.push(*m);
        }
    }
    (placed, spilled)
}

/// The segments of one bucket when no scope seed fits. `members` carry their
/// rows under seed 0.
fn segment_bucket(
    shape: &Shape,
    placer: &Placer<'_>,
    bucket: u32,
    members: Vec<Member>,
) -> Result<Vec<BuiltTable>, Error> {
    let (placed, mut spilled) = fill(&members, shape.rows, shape.slots);
    let mut tables = vec![encode_table(shape, placer.entries, bucket, 0, 0, &placed)?];
    let mut segment = 1u32;
    while !spilled.is_empty() {
        if segment >= MAX_SEGMENTS {
            return Err(Error::TooManySegments {
                bucket,
                spilled: spilled.len(),
                max: MAX_SEGMENTS,
            });
        }
        let mut chosen = None;
        for seed in 0..MAX_SEEDS {
            // Spilled members carry their seed-0 rows already.
            let under = if seed == 0 {
                spilled.clone()
            } else {
                placer.rows_under(&spilled, seed, shape.rows)?
            };
            if fits(
                under.iter().map(|m| m.row as usize),
                shape.rows as usize,
                shape.slots,
            ) {
                chosen = Some((seed, under));
                break;
            }
        }
        match chosen {
            Some((seed, under)) => {
                tables.push(encode_table(
                    shape,
                    placer.entries,
                    bucket,
                    segment,
                    seed,
                    &under,
                )?);
                spilled.clear();
            }
            None => {
                let (placed, rest) = fill(&spilled, shape.rows, shape.slots);
                tables.push(encode_table(
                    shape,
                    placer.entries,
                    bucket,
                    segment,
                    0,
                    &placed,
                )?);
                spilled = rest;
            }
        }
        segment += 1;
    }
    Ok(tables)
}

/// Encodes one table from its members (ordinal order, each with its row, no
/// row over capacity).
fn encode_table(
    shape: &Shape,
    entries: &[Entry],
    bucket: u32,
    segment: u32,
    seed: u32,
    members: &[Member],
) -> Result<BuiltTable, Error> {
    let rows = shape.rows as usize;
    // Counting sort by row; stable, so each row's members stay in ordinal
    // order.
    let mut start = vec![0usize; rows + 1];
    for m in members {
        start[m.row as usize + 1] += 1;
    }
    for r in 0..rows {
        start[r + 1] += start[r];
    }
    let mut next = start.clone();
    let mut order = vec![0u32; members.len()];
    for m in members {
        let at = &mut next[m.row as usize];
        order[*at] = m.index;
        *at += 1;
    }

    let mut bytes = vec![0u8; rows * ROW_BYTES];
    bytes
        .par_chunks_mut(ROW_BYTES)
        .enumerate()
        .try_for_each(|(row, out)| {
            let indices = &order[start[row]..start[row + 1]];
            if indices.is_empty() {
                return Ok(());
            }
            let mut in_row: Vec<Entry> = indices
                .iter()
                .map(|i| entries[*i as usize].clone())
                .collect();
            // Stable: equal keys keep ordinal order, giving (key, ordinal).
            in_row.sort_by(|a, b| a.key().cmp(b.key()));
            let encoded = encode_row(shape.product, &in_row).map_err(|source| Error::Encode {
                bucket,
                segment,
                row: row as u32,
                source,
            })?;
            out.copy_from_slice(&encoded);
            Ok::<(), Error>(())
        })?;
    let rows_sha256 = rows_sha256(&bytes);
    Ok(BuiltTable {
        bucket,
        segment,
        geometry: shape.geometry,
        seed,
        entries: members.len() as u32,
        rows: bytes,
        rows_sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_pir_protocol::{MemoEntry, TachygramEntry, validate_row};

    const GENESIS: BlockHash = BlockHash::from_protocol_bytes([0x11; 32]);

    fn tachygram(i: u32) -> Entry {
        let mut tg = [0u8; 32];
        tg[..4].copy_from_slice(&i.to_le_bytes());
        tg[4..8].copy_from_slice(&i.wrapping_mul(0x9e37_79b9).to_le_bytes());
        tg[31] = 1;
        Entry::Tachygram(TachygramEntry {
            tachygram: tg,
            blk_height: 10,
            stamp_ordinal: i,
        })
    }

    #[test]
    fn input_checks_report_the_lowest_index() {
        let mut entries: Vec<Entry> = (0..100).map(tachygram).collect();
        entries[40] = Entry::Memo(MemoEntry {
            key: [1; 16],
            ciphertext: [0; 128],
        });
        entries[70] = Entry::Memo(MemoEntry {
            key: [1; 16],
            ciphertext: [0; 128],
        });
        assert!(matches!(
            build_scope(Product::Tachygram, &GENESIS, 1, &entries),
            Err(Error::EntryProduct {
                index: 40,
                expected: Product::Tachygram,
                actual: Product::Memo
            })
        ));
        let mut entries: Vec<Entry> = (0..100).map(tachygram).collect();
        entries[55] = Entry::Tachygram(TachygramEntry {
            tachygram: [0; 32],
            blk_height: 10,
            stamp_ordinal: 0,
        });
        assert!(matches!(
            build_scope(Product::Tachygram, &GENESIS, 1, &entries),
            Err(Error::ZeroKey { index: 55 })
        ));
        let mut entries: Vec<Entry> = (0..100).map(tachygram).collect();
        entries[90] = tachygram(12);
        entries[80] = tachygram(30);
        assert!(matches!(
            build_scope(Product::Tachygram, &GENESIS, 1, &entries),
            Err(Error::DuplicateKey {
                first: 12,
                second: 90
            })
        ));
    }

    #[test]
    fn geometry_is_a_parameter() {
        let entries: Vec<Entry> = (0..500).map(tachygram).collect();
        let small = build_scope_at(
            Product::Tachygram,
            Geometry::Tachyon2k8,
            &GENESIS,
            3,
            &entries,
        )
        .unwrap();
        let table = &small.tables[0];
        assert_eq!(table.geometry, Geometry::Tachyon2k8);
        assert_eq!(table.rows.len(), 2048 * ROW_BYTES);
        assert_eq!(table.entries, 500);
        let mut context = small.row_context(Product::Tachygram, &GENESIS, 3, table, Some((10, 10)));
        let mut found = 0;
        for row in 0..2048 {
            context.row = row;
            found += validate_row(Product::Tachygram, table.row(row), &context)
                .unwrap()
                .len();
        }
        assert_eq!(found, 500);
        let published = build_scope(Product::Tachygram, &GENESIS, 3, &entries).unwrap();
        assert_eq!(published.tables[0].geometry, PUBLISHED_GEOMETRY);
        assert_eq!(published.tables[0].rows.len(), 4096 * ROW_BYTES);
    }

    #[test]
    fn fill_spills_in_ordinal_order() {
        let members: Vec<Member> = [(0, 1), (1, 1), (2, 0), (3, 1), (4, 1)]
            .iter()
            .map(|&(index, row)| Member { index, row })
            .collect();
        let (placed, spilled) = fill(&members, 2, 2);
        assert_eq!(
            placed.iter().map(|m| m.index).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(spilled.iter().map(|m| m.index).collect::<Vec<_>>(), [3, 4]);
    }

    #[test]
    fn debug_does_not_print_rows() {
        let entries: Vec<Entry> = (0..3).map(tachygram).collect();
        let built = build_scope(Product::Tachygram, &GENESIS, 3, &entries).unwrap();
        let text = format!("{built:?}");
        assert!(text.len() < 1000, "{text}");
        assert!(text.contains("rows_sha256"));
    }
}
