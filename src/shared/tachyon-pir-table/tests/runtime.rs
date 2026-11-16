//! The P8 table runtime end to end with the real client (spec 6.6): build
//! from built rows, answer client queries, decode exact rows; sizes; the
//! single payload length; determinism of the public setup; preparation for
//! the publisher and the query host.
//!
//! Each runtime build costs about 1 s of the whole machine, so the tests
//! here take a lock and the suite builds four runtimes in all.

mod common;

use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use common::*;
use tachyon_pir_protocol::{
    Entry, Geometry, PUBLISHED_GEOMETRY, PirParams, Product, ROW_BYTES, lookup, validate_row,
};
use tachyon_pir_table::{
    BuiltTable, Error, ScopeTables, TableRuntime, build_scope, describe_in, prepare,
    prepare_published, rows_sha256, validate_scope,
};
use zakura_pir_client::native::ProfileClientExt as _;
use zakura_pir_protocol::native::P8;

const SCOPE_ID: u64 = 40;
const PRODUCT: Product = Product::Memo;

/// Serialises runtime builds across the tests of this binary.
static BUILDS: Mutex<()> = Mutex::new(());

struct Fixture {
    scope: ScopeTables,
    hot: [u8; 16],
    hot_row: u32,
}

/// A memo scope with 2,000 scattered entries and one row filled to all 56
/// slots with one key, every other row of that key's row left to it.
fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let mut entries: Vec<Entry> = (0..2000u64)
            .map(|i| memo(random_key16("rt", i), i))
            .collect();
        let used: std::collections::HashSet<u32> = entries
            .iter()
            .map(|e| {
                tachyon_pir_protocol::row(PRODUCT, &GENESIS, SCOPE_ID, 0, ROWS, e.key()).unwrap()
            })
            .collect();
        let (hot, hot_row) = (0..)
            .map(|i| random_key16("rt-hot", i))
            .map(|k| {
                let r =
                    tachyon_pir_protocol::row(PRODUCT, &GENESIS, SCOPE_ID, 0, ROWS, &k).unwrap();
                (k, r)
            })
            .find(|(_, r)| !used.contains(r))
            .unwrap();
        for _ in 0..PRODUCT.slots() {
            let ordinal = entries.len() as u64;
            entries.push(memo(hot, ordinal));
        }
        let scope = build_scope(PRODUCT, &GENESIS, SCOPE_ID, &entries).unwrap();
        assert_eq!(
            validate_scope(PRODUCT, &GENESIS, SCOPE_ID, None, &scope).unwrap(),
            2056
        );
        assert_eq!(
            (scope.n_buckets, scope.scope_seed, scope.tables.len()),
            (1, 0, 1)
        );
        Fixture {
            scope,
            hot,
            hot_row,
        }
    })
}

fn table() -> &'static BuiltTable {
    &fixture().scope.tables[0]
}

#[test]
fn round_trip_decodes_exact_rows() {
    let _guard = BUILDS.lock().unwrap_or_else(|e| e.into_inner());
    let fx = fixture();
    let table = table();
    let params = PirParams::new(PRODUCT, PUBLISHED_GEOMETRY);
    assert_eq!(params.payload_len(), 28_160);
    assert_eq!(params.reply_len(), 10_240);
    assert_eq!(params.public_len(), 45_056);

    let start = Instant::now();
    let prepared = prepare(PRODUCT, SCOPE_ID, table).unwrap();
    let built_in = start.elapsed();
    let runtime = &prepared.runtime;
    assert_eq!(runtime.public().len(), 45_056);
    assert_eq!(prepared.public, runtime.public());
    assert_eq!(
        prepared.public_sha256,
        tachyon_pir_table::public_sha256(&prepared.public)
    );
    assert_eq!(prepared.public_sha256, runtime.public_sha256());
    assert_eq!(prepared.table.geometry, Geometry::Tachyon4k8);
    assert_eq!(
        (
            prepared.table.bucket,
            prepared.table.segment,
            prepared.table.seed
        ),
        (0, 0, 0)
    );
    assert_eq!(prepared.table.entries, 2056);
    assert_eq!(prepared.table.rows_sha256.0, table.rows_sha256);
    assert_eq!(prepared.table.public_sha256.0, prepared.public_sha256);
    assert_eq!(
        prepared.table.session_id,
        prepared.table.expected_session_id(PRODUCT, SCOPE_ID)
    );
    assert_eq!(prepared.session_id, prepared.table.session_id.0);
    eprintln!(
        "runtime build {built_in:?} on {} threads, held {} bytes",
        rayon::current_num_threads(),
        runtime.held_bytes()
    );
    assert!(runtime.held_bytes() > 32 * 1024 * 1024);

    let empty_row = (0..ROWS)
        .find(|r| table.row(*r).iter().all(|b| *b == 0))
        .unwrap();
    let busy_row = (0..ROWS)
        .filter(|r| *r != fx.hot_row)
        .max_by_key(|r| table.row(*r).iter().filter(|b| **b != 0).count())
        .unwrap();
    // The full row: 56 entries of 144 bytes, then the 128-byte zero tail.
    let full = table.row(fx.hot_row);
    assert!(
        full[..56 * 144]
            .chunks(144)
            .all(|e| e.iter().any(|b| *b != 0))
    );
    let mut answer_time = std::time::Duration::ZERO;
    for target in [fx.hot_row, empty_row, busy_row, 0, ROWS - 1] {
        let (secret, upload) = P8
            .prepare(
                params.setup(),
                params.masks(),
                params.rows(),
                target as usize,
            )
            .unwrap();
        assert_eq!(upload.len(), 28_160);
        let start = Instant::now();
        let reply = runtime.answer(&params, &upload).unwrap();
        answer_time += start.elapsed();
        assert_eq!(reply.len(), 10_240);
        let decoded = P8
            .decode_cols(&secret, &prepared.public, &reply, params.cols())
            .unwrap();
        assert_eq!(decoded.len(), ROW_BYTES);
        assert!(decoded == table.row(target), "row {target} decodes exactly");
        let mut context = fx
            .scope
            .row_context(PRODUCT, &GENESIS, SCOPE_ID, table, None);
        context.row = target;
        let entries = validate_row(PRODUCT, &decoded, &context).unwrap();
        if target == fx.hot_row {
            let hits = lookup(&entries, &fx.hot);
            assert_eq!(hits.len(), 56);
            let ordinals: Vec<u64> = hits.iter().map(|e| ordinal_of(e)).collect();
            assert_eq!(ordinals, (2000..2056).collect::<Vec<_>>());
        }
    }
    eprintln!("answer: {:?} per query", answer_time / 5);

    // The single payload length.
    let (_, upload) = P8
        .prepare(params.setup(), params.masks(), params.rows(), 1)
        .unwrap();
    for bad in [&upload[..upload.len() - 1], &[][..], &upload[..14_336]] {
        assert!(matches!(runtime.answer(&params, bad), Err(Error::Query(_))));
    }
    let mut long = upload.clone();
    long.push(0);
    assert!(matches!(
        runtime.answer(&params, &long),
        Err(Error::Query(_))
    ));
    // Another product's parameters are refused.
    let other = PirParams::new(Product::Tachygram, PUBLISHED_GEOMETRY);
    assert!(matches!(
        runtime.answer(&other, &upload),
        Err(Error::Params { .. })
    ));
    // Rows of the wrong length are refused before any work.
    assert!(matches!(
        TableRuntime::build(&params, &table.rows[..ROW_BYTES]),
        Err(Error::RowsLength { .. })
    ));
}

#[test]
fn public_setup_is_deterministic_and_bound_to_the_rows() {
    let _guard = BUILDS.lock().unwrap_or_else(|e| e.into_inner());
    let table = table();

    // The publisher's description, built in its own pool.
    let pool = rayon::ThreadPoolBuilder::new().build().unwrap();
    let described = describe_in(&pool, PRODUCT, SCOPE_ID, table).unwrap();
    assert_eq!(described.public.len(), 45_056);
    assert_eq!(
        described.table,
        table.map_table(
            PRODUCT,
            SCOPE_ID,
            tachyon_pir_table::public_sha256(&described.public)
        )
    );

    // The query host's preparation of the same published rows: a second,
    // independent build gives the identical public setup and session id.
    let hosted = prepare_published(PRODUCT, SCOPE_ID, &described.table, &table.rows).unwrap();
    assert_eq!(hosted.public, described.public);
    assert_eq!(hosted.table, described.table);
    assert_eq!(hosted.session_id, described.table.session_id.0);

    // Refusals that need no build.
    let mut tampered = table.rows.clone();
    let at = tampered.iter().position(|b| *b != 0).unwrap();
    tampered[at] ^= 1;
    assert!(matches!(
        prepare_published(PRODUCT, SCOPE_ID, &described.table, &tampered),
        Err(Error::RowsDigest)
    ));
    let mut forged_session = described.table.clone();
    forged_session.session_id.0[0] ^= 1;
    assert!(matches!(
        prepare_published(PRODUCT, SCOPE_ID, &forged_session, &table.rows),
        Err(Error::SessionId)
    ));
    assert!(matches!(
        prepare_published(PRODUCT, SCOPE_ID + 1, &described.table, &table.rows),
        Err(Error::SessionId)
    ));
    let mut stale = table.clone();
    stale.rows = tampered.clone();
    assert!(matches!(
        prepare(PRODUCT, SCOPE_ID, &stale),
        Err(Error::RowsDigest)
    ));

    // One changed row byte changes the public setup: a table claiming the
    // old public digest for the new rows is refused after its build.
    let mut claim = described.table.clone();
    claim.rows_sha256.0 = rows_sha256(&tampered);
    claim.session_id = claim.expected_session_id(PRODUCT, SCOPE_ID);
    match prepare_published(PRODUCT, SCOPE_ID, &claim, &tampered) {
        Err(Error::PublicDigest { expected, actual }) => {
            assert_eq!(expected, described.table.public_sha256.0);
            assert_ne!(actual, expected);
        }
        other => panic!("expected PublicDigest, got {other:?}"),
    }
}
