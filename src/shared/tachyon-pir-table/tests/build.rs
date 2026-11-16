//! Scope building: seed search, overflow segments, determinism, and the
//! independent lookup oracle (spec 6.2-6.4, 10.3, 13.1).

mod common;

use std::time::Instant;

use common::*;
use tachyon_pir_protocol::{
    BlockHash, Entry, MAX_SEGMENTS, PUBLISHED_GEOMETRY, Product, ROW_BYTES, RowError,
    n_buckets_for, validate_row,
};
use tachyon_pir_table::{Error, build_scope, rows_sha256, validate_scope, validate_table};

fn tachygrams(tag: &str, count: u32) -> Vec<Entry> {
    (0..count)
        .map(|i| tachygram(random32(tag, u64::from(i)), i))
        .collect()
}

fn build_checked(
    product: Product,
    scope_id: u64,
    entries: &[Entry],
) -> tachyon_pir_table::ScopeTables {
    let built = build_scope(product, &GENESIS, scope_id, entries).unwrap();
    assert_eq!(
        validate_scope(product, &GENESIS, scope_id, Some(HEIGHTS), &built).unwrap(),
        entries.len() as u64
    );
    built
}

#[test]
fn empty_scope_has_no_tables() {
    for product in [Product::Memo, Product::Tachygram, Product::Registry] {
        let built = build_scope(product, &GENESIS, 7, &[]).unwrap();
        assert_eq!(built.n_buckets, 1);
        assert_eq!(built.scope_seed, 0);
        assert!(built.tables.is_empty());
        assert_eq!(
            validate_scope(product, &GENESIS, 7, None, &built).unwrap(),
            0
        );
    }
}

#[test]
fn light_load_fits_seed_zero_and_every_key_is_found() {
    let entries = tachygrams("light", 3000);
    let built = build_checked(Product::Tachygram, 11, &entries);
    // The oracle agrees that seed 0 fits, so this is the seed-0 path.
    let expected = reference_layout(Product::Tachygram, 11, 1, &entries);
    assert_eq!(expected.scope_seed, 0);
    assert_eq!(built.n_buckets, 1);
    assert_eq!(built.scope_seed, 0);
    assert_eq!(built.tables.len(), 1);
    let table = &built.tables[0];
    assert_eq!((table.bucket, table.segment, table.seed), (0, 0, 0));
    assert_eq!(table.entries, 3000);
    assert_eq!(table.geometry, PUBLISHED_GEOMETRY);
    assert_eq!(table.rows.len(), 4096 * ROW_BYTES);
    assert_eq!(table.rows_sha256, rows_sha256(&table.rows));
    assert_eq!(built_layout(Product::Tachygram, 11, &built), expected);
    check_lookups(Product::Tachygram, 11, &entries, &built);
}

#[test]
fn multi_bucket_scope_with_duplicates_is_found_exhaustively() {
    // 9,000 first-contact entries: n_buckets = 4 (4,096 per bucket at the
    // target load of one per row). Every seventh entry repeats an earlier key.
    let entries: Vec<Entry> = (0..9000u64)
        .map(|i| {
            let source = if i % 7 == 6 { i / 2 } else { i };
            first_contact(random_key16("multi", source), i)
        })
        .collect();
    let built = build_checked(Product::FirstContact, 12, &entries);
    assert_eq!(built.n_buckets, 4);
    assert_eq!(
        built.n_buckets,
        n_buckets_for(Product::FirstContact, PUBLISHED_GEOMETRY, 9000)
    );
    let expected = reference_layout(Product::FirstContact, 12, 4, &entries);
    assert_eq!(built_layout(Product::FirstContact, 12, &built), expected);
    check_lookups(Product::FirstContact, 12, &entries, &built);
}

#[test]
fn overflow_under_seed_zero_picks_the_smallest_fitting_seed() {
    // Seven distinct keys ground into one row under seed 0 overflow its six
    // slots; another seed separates them.
    let scope_id = 13;
    let ground = grind_key16("seed", scope_id, 0, 1, (0, 77), 7, Product::FirstContact);
    let mut entries: Vec<Entry> = (0..500u64)
        .map(|i| first_contact(random_key16("seed-filler", i), i))
        .collect();
    for (k, key) in ground.into_iter().enumerate() {
        let ordinal = entries.len() as u64;
        entries.insert(k * 60, first_contact(key, ordinal));
    }
    // Re-number ordinals to input positions.
    let entries: Vec<Entry> = entries
        .iter()
        .enumerate()
        .map(|(i, e)| first_contact(e.key().try_into().unwrap(), i as u64))
        .collect();
    let expected = reference_layout(Product::FirstContact, scope_id, 1, &entries);
    assert!(expected.scope_seed >= 1, "seed 0 must overflow");
    let built = build_checked(Product::FirstContact, scope_id, &entries);
    assert_eq!(built.scope_seed, expected.scope_seed);
    assert_eq!(built.tables.len(), 1);
    assert_eq!(built.tables[0].seed, expected.scope_seed);
    assert_eq!(
        built_layout(Product::FirstContact, scope_id, &built),
        expected
    );
    check_lookups(Product::FirstContact, scope_id, &entries, &built);
}

#[test]
fn forced_overflow_spills_in_ordinal_order_with_segment_seeds() {
    // Key A seven times overflows its row under every seed, so no scope seed
    // fits. Thirteen distinct keys X ground into one row under seed 0: six
    // stay in segment 0, seven spill with one A; together they overflow
    // under seed 0 again, so segment 1 takes a later seed.
    let scope_id = 14;
    let product = Product::FirstContact;
    let xs = grind_key16("spill-x", scope_id, 0, 1, (0, 300), 13, product);
    let a = (0..)
        .map(|i| random_key16("spill-a", i))
        .find(|k| {
            tachyon_pir_protocol::row(product, &GENESIS, scope_id, 0, ROWS, k).unwrap() != 300
        })
        .unwrap();
    let mut keys: Vec<[u8; 16]> = (0..400u64)
        .map(|i| random_key16("spill-filler", i))
        .collect();
    for (k, x) in xs.iter().enumerate() {
        keys.insert(k * 25, *x);
    }
    for k in 0..7 {
        keys.insert(k * 50 + 3, a);
    }
    let entries: Vec<Entry> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| first_contact(*k, i as u64))
        .collect();
    let expected = reference_layout(product, scope_id, 1, &entries);
    assert_eq!(expected.scope_seed, 0);
    assert_eq!(expected.buckets[0].len(), 2);
    assert!(expected.buckets[0][1].0 > 0, "segment 1 takes a later seed");
    let built = build_checked(product, scope_id, &entries);
    assert_eq!(built.scope_seed, 0);
    assert_eq!(built.tables.len(), 2);
    assert_eq!(built.tables[1].segment, 1);
    assert_eq!(built.tables[1].entries, 8);
    let layout = built_layout(product, scope_id, &built);
    assert_eq!(layout, expected);
    check_spill_order(&entries, &layout);
    check_lookups(product, scope_id, &entries, &built);
}

#[test]
fn many_duplicates_spill_through_several_segments() {
    // 178 memo entries of one key: 56 slots per row, so no seed fits and
    // segments 0, 1 and 2 fill under seed 0 (each later seed search fails
    // while more than 56 remain); segment 3 takes the rest under seed 0.
    let scope_id = 15;
    let product = Product::Memo;
    let hot = random_key16("hot", 0);
    let entries: Vec<Entry> = (0..3000u64)
        .map(|i| {
            if i % 20 == 0 || (1000..1030).contains(&i) {
                memo(hot, i)
            } else {
                memo(random_key16("memo-filler", i), i)
            }
        })
        .collect();
    let hot_count = entries.iter().filter(|e| e.key() == hot).count();
    assert_eq!(hot_count, 178);
    let expected = reference_layout(product, scope_id, 1, &entries);
    let built = build_checked(product, scope_id, &entries);
    assert_eq!(built.scope_seed, 0);
    assert_eq!(
        built.tables.len(),
        4,
        "178 = 56 + 56 + 56 + 10, plus any filler sharing the row"
    );
    let layout = built_layout(product, scope_id, &built);
    assert_eq!(layout, expected);
    check_spill_order(&entries, &layout);
    check_lookups(product, scope_id, &entries, &built);
}

#[test]
fn more_than_max_segments_is_an_error() {
    let product = Product::FirstContact;
    let key = random_key16("max", 0);
    let limit = product.slots() * MAX_SEGMENTS as usize;
    let fits: Vec<Entry> = (0..limit as u64).map(|i| first_contact(key, i)).collect();
    let built = build_checked(product, 16, &fits);
    assert_eq!(built.tables.len(), MAX_SEGMENTS as usize);
    assert!(built.tables.iter().all(|t| t.entries == 6));
    check_lookups(product, 16, &fits, &built);
    drop(built);

    let over: Vec<Entry> = (0..=limit as u64).map(|i| first_contact(key, i)).collect();
    match build_scope(product, &GENESIS, 16, &over) {
        Err(Error::TooManySegments {
            bucket: 0,
            spilled: 1,
            max: 8,
        }) => {}
        other => panic!("expected TooManySegments, got {other:?}"),
    }
}

#[test]
fn empty_bucket_gets_a_zero_entry_segment_zero_table() {
    // 4,097 first-contact entries need two buckets; grind them all into
    // bucket 0 under seed 0 while keeping every row within its slots, so
    // seed 0 fits and bucket 1 is empty.
    let product = Product::FirstContact;
    let scope_id = 17;
    let mut load = vec![0usize; ROWS as usize];
    let mut entries = Vec::new();
    let mut i = 0u64;
    while entries.len() < 4097 {
        let key = random_key16("empty-bucket", i);
        i += 1;
        let b = tachyon_pir_protocol::bucket(product, &GENESIS, scope_id, 0, 2, &key).unwrap();
        let r = tachyon_pir_protocol::row(product, &GENESIS, scope_id, 0, ROWS, &key).unwrap();
        if b == 0 && load[r as usize] < product.slots() {
            load[r as usize] += 1;
            entries.push(first_contact(key, entries.len() as u64));
        }
    }
    let built = build_checked(product, scope_id, &entries);
    assert_eq!(built.n_buckets, 2);
    assert_eq!(built.scope_seed, 0);
    assert_eq!(built.tables.len(), 2);
    let empty = &built.tables[1];
    assert_eq!(
        (empty.bucket, empty.segment, empty.seed, empty.entries),
        (1, 0, 0, 0)
    );
    assert!(empty.rows.iter().all(|b| *b == 0));
    assert_eq!(empty.rows_sha256, rows_sha256(&vec![0u8; 4096 * ROW_BYTES]));
    check_lookups(product, scope_id, &entries, &built);
}

#[test]
fn builds_are_deterministic_and_bound_to_genesis_and_scope() {
    let entries = tachygrams("determinism", 20_000);
    let first = build_scope(Product::Tachygram, &GENESIS, 21, &entries).unwrap();
    let second = build_scope(Product::Tachygram, &GENESIS, 21, &entries).unwrap();
    assert_eq!(first, second);

    // The same ordinal order assembled another way: in batches, from the
    // end, into a fresh allocation.
    let mut batched: Vec<Entry> = Vec::new();
    for chunk in entries.chunks(997).rev() {
        let mut part = chunk.to_vec();
        part.extend(batched);
        batched = part;
    }
    assert_eq!(batched, entries);
    // And on another thread pool size.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(3)
        .build()
        .unwrap();
    let third = pool
        .install(|| build_scope(Product::Tachygram, &GENESIS, 21, &batched))
        .unwrap();
    assert_eq!(third, first);

    let other_genesis = BlockHash::from_protocol_bytes([0x5b; 32]);
    let moved = build_scope(Product::Tachygram, &other_genesis, 21, &entries).unwrap();
    assert_ne!(moved.tables[0].rows_sha256, first.tables[0].rows_sha256);
    let moved = build_scope(Product::Tachygram, &GENESIS, 22, &entries).unwrap();
    assert_ne!(moved.tables[0].rows_sha256, first.tables[0].rows_sha256);
    // The session ids derived from identical tables are identical; a
    // different scope id alone changes them.
    let a = first.tables[0].map_table(Product::Tachygram, 21, [1; 32]);
    let b = third.tables[0].map_table(Product::Tachygram, 21, [1; 32]);
    assert_eq!(a, b);
    assert_ne!(
        first.tables[0]
            .map_table(Product::Tachygram, 22, [1; 32])
            .session_id,
        a.session_id
    );
}

#[test]
fn rows_sort_equal_keys_in_ordinal_order() {
    // Interleave two keys in one row: rows hold (key, ordinal) order.
    let product = Product::Memo;
    let scope_id = 23;
    let keys = grind_key16("order", scope_id, 0, 1, (0, 9), 2, product);
    let (low, high) = if keys[0] < keys[1] {
        (keys[0], keys[1])
    } else {
        (keys[1], keys[0])
    };
    let order = [high, low, high, low, low, high];
    let entries: Vec<Entry> = order
        .iter()
        .enumerate()
        .map(|(i, k)| memo(*k, i as u64))
        .collect();
    let built = build_checked(product, scope_id, &entries);
    let table = &built.tables[0];
    let mut context = built.row_context(product, &GENESIS, scope_id, table, None);
    context.row = 9;
    let decoded = validate_row(product, table.row(9), &context).unwrap();
    let got: Vec<([u8; 16], u64)> = decoded
        .iter()
        .map(|e| (e.key().try_into().unwrap(), ordinal_of(e)))
        .collect();
    assert_eq!(
        got,
        [
            (low, 1),
            (low, 3),
            (low, 4),
            (high, 0),
            (high, 2),
            (high, 5)
        ]
    );
}

#[test]
fn validation_refuses_tampered_tables() {
    let product = Product::Tachygram;
    let scope_id = 24;
    let entries = tachygrams("tamper", 2000);
    let built = build_checked(product, scope_id, &entries);
    let table = &built.tables[0];
    let template = built.row_context(product, &GENESIS, scope_id, table, Some(HEIGHTS));
    assert_eq!(
        validate_table(product, &template, &table.rows).unwrap(),
        2000
    );

    // A flipped byte inside an entry's key breaks its placement or order.
    let used = (0..ROWS)
        .find(|r| table.row(*r).iter().any(|b| *b != 0))
        .unwrap();
    let mut rows = table.rows.clone();
    rows[used as usize * ROW_BYTES] ^= 0x80;
    assert!(matches!(
        validate_table(product, &template, &rows),
        Err(Error::InvalidRow { row, .. }) if row == used
    ));
    // A byte in the row tail.
    let mut rows = table.rows.clone();
    rows[(used as usize + 1) * ROW_BYTES - 1] = 1;
    assert!(matches!(
        validate_table(product, &template, &rows),
        Err(Error::InvalidRow { row, source: RowError::NonzeroTail }) if row == used
    ));
    // Truncated rows.
    assert!(matches!(
        validate_table(product, &template, &table.rows[..ROW_BYTES * 4095]),
        Err(Error::RowsLength { .. })
    ));
    // The wrong scope id or seed.
    let wrong = tachyon_pir_protocol::RowContext {
        scope_id: 25,
        ..template
    };
    assert!(matches!(
        validate_table(product, &wrong, &table.rows),
        Err(Error::InvalidRow {
            source: RowError::Placement(0),
            ..
        })
    ));

    // Scope-level: a stale digest, a wrong entry count, a wrong seed.
    let mut stale = built.clone();
    stale.tables[0].rows[used as usize * ROW_BYTES + 35] ^= 1;
    assert!(matches!(
        validate_scope(product, &GENESIS, scope_id, Some(HEIGHTS), &stale),
        Err(Error::InTable { source, .. }) if matches!(*source, Error::RowsDigest)
    ));
    let mut count = built.clone();
    count.tables[0].entries += 1;
    assert!(matches!(
        validate_scope(product, &GENESIS, scope_id, Some(HEIGHTS), &count),
        Err(Error::InTable { source, .. }) if matches!(*source, Error::EntryCount { expected: 2001, actual: 2000 })
    ));
    let mut seed = built.clone();
    seed.scope_seed = 1;
    assert!(matches!(
        validate_scope(product, &GENESIS, scope_id, Some(HEIGHTS), &seed),
        Err(Error::Layout(_))
    ));
    // Tachygram heights outside the scope.
    assert!(matches!(
        validate_scope(product, &GENESIS, scope_id, Some((0, 4095)), &built),
        Err(Error::InTable { source, .. })
            if matches!(*source, Error::InvalidRow { source: RowError::HeightOutOfRange(_), .. })
    ));
}

#[test]
fn full_tachygram_scope_builds_quickly() {
    // The 4k8 target for one bucket: 507,904 tachygrams, 124 per row on
    // average (spec 6.2).
    let count = 507_904u32;
    let entries = tachygrams("full", count);
    let start = Instant::now();
    let built = build_scope(Product::Tachygram, &GENESIS, 31, &entries).unwrap();
    let build = start.elapsed();
    assert_eq!(built.n_buckets, 1);
    assert_eq!(built.tables.len(), 1);
    assert_eq!(built.tables[0].entries, count);
    let start = Instant::now();
    let total = validate_scope(Product::Tachygram, &GENESIS, 31, Some(HEIGHTS), &built).unwrap();
    let validate = start.elapsed();
    assert_eq!(total, u64::from(count));
    eprintln!(
        "{count} tachygrams on {} threads: build {build:?}, validate {validate:?}, scope seed {}",
        rayon::current_num_threads(),
        built.scope_seed
    );

    // One more entry needs a second bucket.
    let mut more = entries;
    more.push(tachygram(random32("full-extra", 0), count));
    let start = Instant::now();
    let built = build_scope(Product::Tachygram, &GENESIS, 31, &more).unwrap();
    let build = start.elapsed();
    assert_eq!(built.n_buckets, 2);
    assert_eq!(built.tables.len(), 2);
    assert_eq!(built.entries(), u64::from(count) + 1);
    eprintln!("{} tachygrams in two buckets: build {build:?}", count + 1);
}
