//! The memo serving module across real HTTP(S) (spec 7.1, 10): repeated tags
//! returned whole and in ordinal order across overflow segments, both
//! mounts, revocation and refusal of a corrupt row.
mod support;

use std::sync::Arc;
use support::*;
use tachyon_pir_cli::{Client, LookupOutcome, Mount, ScopeSelector};
use tachyon_pir_protocol::{ErrorCode, Product};

const PRODUCT: Product = Product::Memo;

/// Epoch 0: one tag repeated 70 times, more than a row's 56 slots, so the
/// bucket spills into an overflow segment.
fn spilling() -> Spec {
    Spec::new(PRODUCT, 0, 1, 300).repeating(70)
}
/// Epoch 1: a tag repeated three times, in one row.
fn open() -> Spec {
    Spec::new(PRODUCT, 1, 2, 300).repeating(3)
}
fn open_rebuilt() -> Spec {
    Spec::new(PRODUCT, 1, 3, 300).repeating(3)
}

async fn started(scopes: &[Option<Arc<Fixture>>]) -> (tempfile::TempDir, Publisher, Edge, Host) {
    let dir = tempdir();
    let publisher = Publisher::start(PRODUCT, &dir.path().join("publisher")).await;
    publisher.publish(scopes, 1);
    let edge = Edge::start(publisher.addr).await;
    let host = Host::start(PRODUCT, settings(&edge, &dir.path().join("host"))).await;
    host.wait_ready().await;
    (dir, publisher, edge, host)
}

/// The repeated key of `spec` answers every one of its entries in ordinal
/// order, querying every segment of its bucket.
async fn check_repeated(
    client: &Client,
    index: u64,
    fixture: &Fixture,
    spec: Spec,
    segments: usize,
) {
    let map = client.fetch_map().await.unwrap();
    let key = spec.repeated_key();
    let expected = fixture.expected(&key);
    assert_eq!(expected.len(), spec.repeats as usize);
    match client
        .lookup(&map, ScopeSelector::Index(index), &key)
        .await
        .unwrap()
    {
        LookupOutcome::Found { entries, answer } => {
            assert_eq!(entries, expected, "every entry, in ordinal order");
            assert_eq!(answer.queries.len(), segments);
            let found: usize = answer.queries.iter().map(|q| q.matches).sum();
            assert_eq!(found, expected.len());
            if segments > 1 {
                assert!(answer.queries.iter().all(|q| q.matches > 0), "{answer:?}");
            }
        }
        other => panic!("repeated tag answered {other:?}"),
    }
}

/// Scenarios 2, 4 and 5: cold bootstrap over TLS, exact answers for hits
/// and misses on both mounts, and a repeated tag's entries all returned in
/// ordinal order across its bucket's overflow segment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_bootstrap_answers_repeated_tags_across_overflow_segments() {
    let (s0, s1) = (fixture(spilling()), fixture(open()));
    assert_eq!(
        s0.tables.len(),
        2,
        "the spilling scope has an overflow segment"
    );
    assert_eq!(
        (s0.tables[1].table.bucket, s0.tables[1].table.segment),
        (0, 1)
    );
    assert_eq!(s1.tables.len(), 1);
    let (_dir, _publisher, _edge, host) = started(&[Some(s0.clone()), Some(s1.clone())]).await;

    let client = host.client();
    check_repeated(&client, 0, &s0, spilling(), 2).await;
    check_repeated(&client, 1, &s1, open(), 1).await;
    check_answers(&client, &[(0, &s0), (1, &s1)], 10).await;
    let legacy = Client::with_mount(&host.origin(), PRODUCT, None, Mount::Legacy).unwrap();
    check_repeated(&legacy, 0, &s0, spilling(), 2).await;

    let metrics = host.metrics().await;
    for line in [
        "tachyon_memo_ready 1",
        "tachyon_memo_tables_served 3",
        &format!("tachyon_memo_entries_total {}", 300 + 70 + 300 + 3),
        "tachyon_memo_newest_scope 1",
        // Every lookup in epoch 0 queries both segments of its bucket:
        // 2 + 1 repeated, 2 x 20 + 20 checked, 2 on the legacy mount.
        "pir_http_responses_total{endpoint=\"memo_query\",code=\"2xx\"} 65",
    ] {
        assert!(metrics.lines().any(|l| l == line), "{line} in\n{metrics}");
    }
    host.stop().await;
}

/// Scenario 9: a recovery epoch revokes the dropped session (410); an
/// unknown one is 409 with the current map.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_epoch_revokes_dropped_sessions() {
    let (s1, s1b) = (fixture(open()), fixture(open_rebuilt()));
    let (_dir, publisher, _edge, host) = started(&[None, Some(s1.clone())]).await;
    publisher.distribution.recover().unwrap();
    publisher.publish(&[None, Some(s1b.clone())], 2);
    host.wait_map(&publisher.head().sha256).await;
    let revoked = error_body(post_query(&host, fixture_query(&s1), "10.0.0.1").await, 410).await;
    assert_eq!(revoked.code, ErrorCode::SessionUnavailable);
    let mut unknown = fixture_query(&s1b);
    unknown[4..36].copy_from_slice(&[0xee; 32]);
    let stale = error_body(post_query(&host, unknown, "10.0.0.1").await, 409).await;
    assert_eq!(stale.map_sha256.unwrap().hex(), publisher.head().sha256);
    let mut magic = fixture_query(&s1b);
    magic[..4].copy_from_slice(b"TFQ1");
    error_body(post_query(&host, magic, "10.0.0.1").await, 400).await;
    check_answers(&host.client(), &[(1, &s1b)], 6).await;
    check_repeated(&host.client(), 1, &s1b, open_rebuilt(), 1).await;
    assert_eq!(
        host.metric("tachyon_memo_sessions_revoked_total").await,
        Some(1)
    );
    host.stop().await;
}

/// Scenario 10: a non-canonical row is refused; the previous publication
/// keeps serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_row_refuses_the_publication() {
    let (s1, bad) = (fixture(open()), corrupt_fixture(open()));
    let (_dir, publisher, _edge, host) = started(&[None, Some(s1.clone())]).await;
    let served = publisher.head().sha256;
    publisher.publish(&[None, Some(bad.clone())], 1);
    host.wait_failed_passes(1).await;
    host.wait_map(&served).await;
    check_answers(&host.client(), &[(1, &s1)], 6).await;
    error_body(
        post_query(&host, fixture_query(&bad), "10.0.0.1").await,
        409,
    )
    .await;
    assert_eq!(host.ready_status().await, 200);
    host.stop().await;
}
