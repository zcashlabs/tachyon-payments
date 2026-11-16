//! Fixtures and the independent oracle shared by the integration tests.
//!
//! The oracle's expected answers come from the input entry list, never from
//! the built tables; its reference placement follows the words of spec 6.3
//! and 6.4 with the protocol crate's per-key `bucket` and `row` functions,
//! independently of the builder's batched code.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};

use sha2::{Digest, Sha256};
use tachyon_pir_protocol::map::ScopeTables as _;
use tachyon_pir_protocol::{
    BlockHash, Entry, FirstContactEntry, MAX_SEEDS, MemoEntry, Product, Scope, ScopeState,
    TachygramEntry, bucket, lookup, row, validate_row,
};
use tachyon_pir_table::{ScopeTables, validate_published};

pub const GENESIS: BlockHash = BlockHash::from_protocol_bytes([0x5a; 32]);
pub const ROWS: u32 = 4096;
/// Heights every tachygram fixture entry lies in.
pub const HEIGHTS: (u32, u32) = (4096, 8191);

/// A pseudo-random nonzero 32-byte value.
pub fn random32(tag: &str, i: u64) -> [u8; 32] {
    let mut out: [u8; 32] = Sha256::new()
        .chain_update(tag.as_bytes())
        .chain_update(i.to_le_bytes())
        .finalize()
        .into();
    out[31] |= 1;
    out
}

pub fn random_key16(tag: &str, i: u64) -> [u8; 16] {
    random32(tag, i)[..16].try_into().unwrap()
}

pub fn tachygram(tg: [u8; 32], ordinal: u32) -> Entry {
    Entry::Tachygram(TachygramEntry {
        tachygram: tg,
        blk_height: HEIGHTS.0 + ordinal % 4096,
        stamp_ordinal: ordinal,
    })
}

pub fn memo(key: [u8; 16], ordinal: u64) -> Entry {
    let mut ciphertext = [0xc3u8; 128];
    ciphertext[..8].copy_from_slice(&ordinal.to_le_bytes());
    Entry::Memo(MemoEntry { key, ciphertext })
}

pub fn first_contact(key: [u8; 16], ordinal: u64) -> Entry {
    let mut ciphertext = [0x3cu8; 128];
    ciphertext[..8].copy_from_slice(&ordinal.to_le_bytes());
    Entry::FirstContact(Box::new(FirstContactEntry {
        key,
        kem_ciphertext: [0x77; 1088],
        ciphertext,
    }))
}

/// The ordinal a fixture entry carries in its payload.
pub fn ordinal_of(entry: &Entry) -> u64 {
    match entry {
        Entry::Tachygram(e) => u64::from(e.stamp_ordinal),
        Entry::Memo(e) => u64::from_le_bytes(e.ciphertext[..8].try_into().unwrap()),
        Entry::FirstContact(e) => u64::from_le_bytes(e.ciphertext[..8].try_into().unwrap()),
        Entry::Registry(_) => panic!("registry fixtures carry no ordinal"),
    }
}

/// Keys of `product` (16 bytes) landing in `(bucket, row)` under `seed`.
pub fn grind_key16(
    tag: &str,
    scope_id: u64,
    seed: u32,
    n_buckets: u32,
    target: (u32, u32),
    count: usize,
    product: Product,
) -> Vec<[u8; 16]> {
    let mut out = Vec::new();
    let mut i = 0u64;
    while out.len() < count {
        let key = random_key16(tag, i);
        i += 1;
        if bucket(product, &GENESIS, scope_id, seed, n_buckets, &key).unwrap() == target.0
            && row(product, &GENESIS, scope_id, seed, ROWS, &key).unwrap() == target.1
        {
            out.push(key);
        }
    }
    out
}

/// Expected layout: the scope seed and, per bucket, each segment's seed and
/// member ordinals (input positions) in ordinal order.
#[derive(Debug, PartialEq, Eq)]
pub struct Layout {
    pub scope_seed: u32,
    pub buckets: Vec<Vec<(u32, Vec<usize>)>>,
}

/// Spec 6.3 and 6.4 applied literally, one key at a time.
pub fn reference_layout(
    product: Product,
    scope_id: u64,
    n_buckets: u32,
    entries: &[Entry],
) -> Layout {
    let slots = product.slots();
    let place = |seed: u32, key: &[u8]| {
        (
            bucket(product, &GENESIS, scope_id, seed, n_buckets, key).unwrap(),
            row(product, &GENESIS, scope_id, seed, ROWS, key).unwrap(),
        )
    };
    for seed in 0..MAX_SEEDS {
        let mut load: HashMap<(u32, u32), usize> = HashMap::new();
        let mut buckets = vec![Vec::new(); n_buckets as usize];
        for (i, e) in entries.iter().enumerate() {
            let p = place(seed, e.key());
            *load.entry(p).or_default() += 1;
            buckets[p.0 as usize].push(i);
        }
        if load.values().all(|l| *l <= slots) {
            return Layout {
                scope_seed: seed,
                buckets: buckets.into_iter().map(|m| vec![(seed, m)]).collect(),
            };
        }
    }
    let fill = |pending: &[usize]| {
        let mut load: HashMap<u32, usize> = HashMap::new();
        let (mut placed, mut spilled) = (Vec::new(), Vec::new());
        for &i in pending {
            let r = place(0, entries[i].key()).1;
            let l = load.entry(r).or_default();
            if *l < slots {
                *l += 1;
                placed.push(i);
            } else {
                spilled.push(i);
            }
        }
        (placed, spilled)
    };
    let mut buckets = Vec::new();
    for b in 0..n_buckets {
        let members: Vec<usize> = (0..entries.len())
            .filter(|&i| place(0, entries[i].key()).0 == b)
            .collect();
        let (placed, mut pending) = fill(&members);
        let mut segments = vec![(0, placed)];
        'segments: while !pending.is_empty() {
            for seed in 0..MAX_SEEDS {
                let mut load: HashMap<u32, usize> = HashMap::new();
                for &i in &pending {
                    *load.entry(place(seed, entries[i].key()).1).or_default() += 1;
                }
                if load.values().all(|l| *l <= slots) {
                    segments.push((seed, std::mem::take(&mut pending)));
                    continue 'segments;
                }
            }
            let (placed, spilled) = fill(&pending);
            segments.push((0, placed));
            pending = spilled;
        }
        buckets.push(segments);
    }
    Layout {
        scope_seed: 0,
        buckets,
    }
}

/// The layout the builder produced, read back from the rows: each table's
/// decoded entries mapped to input positions by their carried ordinal.
pub fn built_layout(product: Product, scope_id: u64, built: &ScopeTables) -> Layout {
    let mut buckets: Vec<Vec<(u32, Vec<usize>)>> = vec![Vec::new(); built.n_buckets as usize];
    for table in &built.tables {
        let mut context = built.row_context(product, &GENESIS, scope_id, table, Some(HEIGHTS));
        let mut members = Vec::new();
        for r in 0..ROWS {
            context.row = r;
            for e in validate_row(product, table.row(r), &context).unwrap() {
                members.push(ordinal_of(&e) as usize);
            }
        }
        members.sort_unstable();
        assert_eq!(members.len(), table.entries as usize);
        buckets[table.bucket as usize].push((table.seed, members));
    }
    Layout {
        scope_seed: built.scope_seed,
        buckets,
    }
}

/// A map scope for `built` with a stand-in public digest: placement and
/// session ids are consistent, which is all `check_tables` and `query_plan`
/// read. The real digest comes from a runtime build (tests/runtime.rs).
pub fn map_scope(product: Product, scope_id: u64, built: &ScopeTables) -> Scope {
    Scope {
        scope_id,
        state: ScopeState::Sealed,
        first_height: HEIGHTS.0,
        last_height: HEIGHTS.1,
        entries: built.entries(),
        n_buckets: built.n_buckets,
        tables: built
            .tables
            .iter()
            .map(|t| t.map_table(product, scope_id, [0xee; 32]))
            .collect(),
    }
}

/// The independent oracle: every table validates under the map, and a
/// lookup of every inserted key through `query_plan` returns exactly the
/// input entries with that key, in ordinal order; absent keys return none.
pub fn check_lookups(product: Product, scope_id: u64, entries: &[Entry], built: &ScopeTables) {
    let scope = map_scope(product, scope_id, built);
    scope.check_tables(product).unwrap();
    for (table, map_table) in built.tables.iter().zip(&scope.tables) {
        let count = validate_published(product, &GENESIS, &scope, map_table, &table.rows).unwrap();
        assert_eq!(count, u64::from(table.entries));
    }
    let mut expected: BTreeMap<Vec<u8>, Vec<&Entry>> = BTreeMap::new();
    for e in entries {
        expected.entry(e.key().to_vec()).or_default().push(e);
    }
    let by_position: HashMap<(u32, u32), &tachyon_pir_table::BuiltTable> = built
        .tables
        .iter()
        .map(|t| ((t.bucket, t.segment), t))
        .collect();
    let find = |key: &[u8]| -> Vec<Entry> {
        let mut found = Vec::new();
        for target in scope.query_plan(product, &GENESIS, key).unwrap() {
            let table = by_position[&(target.table.bucket, target.table.segment)];
            let decoded = validate_row(product, table.row(target.row), &target.context).unwrap();
            found.extend(lookup(&decoded, key).into_iter().cloned());
        }
        found
    };
    for (key, want) in &expected {
        let got = find(key);
        let want: Vec<Entry> = want.iter().map(|e| (*e).clone()).collect();
        assert_eq!(got, want, "lookup of {key:02x?}");
    }
    for i in 0..50 {
        let absent = &random32("absent", i)[..product.key_len()];
        assert!(!expected.contains_key(absent));
        assert!(find(absent).is_empty());
    }
}

/// Spec 6.4: every entry of a key in segment k has a lower ordinal than every
/// entry of that key in segment k + 1.
pub fn check_spill_order(entries: &[Entry], layout: &Layout) {
    for segments in &layout.buckets {
        let mut last: HashMap<&[u8], usize> = HashMap::new();
        for (_, members) in segments {
            let mut this: HashMap<&[u8], (usize, usize)> = HashMap::new();
            for &i in members {
                let key = entries[i].key();
                let span = this.entry(key).or_insert((i, i));
                span.0 = span.0.min(i);
                span.1 = span.1.max(i);
            }
            for (key, (low, _)) in &this {
                if let Some(previous) = last.get(key) {
                    assert!(previous < low, "key spill order");
                }
            }
            for (key, (_, high)) in this {
                last.insert(key, high);
            }
        }
    }
}
