//! The first-contact serving module across real HTTP(S) (spec 7.2, 10):
//! multi-epoch spans whose scope index is not their `scope_id`, repeated
//! first-contact tags across overflow segments, the host floor as a scope
//! index, revocation and refusal of a corrupt row.
mod support;

use std::sync::Arc;
use support::*;
use tachyon_pir_cli::{LookupOutcome, ScopeSelector};
use tachyon_pir_protocol::{ChainMapHead, ErrorCode, Product, ScopeState};

const PRODUCT: Product = Product::FirstContact;

/// Span 0: epochs 0..3, sealed; one tag repeated 8 times, more than a row's
/// 6 slots, so its bucket has an overflow segment.
fn span0() -> Spec {
    Spec::new(PRODUCT, 0, 1, 100).repeating(8)
}
/// Span 1 starts at epoch 3 (its `scope_id`) and is open.
fn span1() -> Spec {
    Spec::new(PRODUCT, 3, 2, 60).repeating(2)
}
fn span1_rebuilt() -> Spec {
    Spec::new(PRODUCT, 3, 3, 60).repeating(2)
}

fn slots(first: Option<Arc<Fixture>>, second: Arc<Fixture>) -> Vec<Slot> {
    vec![Slot::span(0, 3, first), Slot::span(3, 1, Some(second))]
}

async fn started(
    slots: &[Slot],
    retain: Option<u64>,
) -> (tempfile::TempDir, Publisher, Edge, Host) {
    let dir = tempdir();
    let publisher = Publisher::start(PRODUCT, &dir.path().join("publisher")).await;
    publisher.publish_slots(slots, 1);
    let edge = Edge::start(publisher.addr).await;
    let mut settings = settings(&edge, &dir.path().join("host"));
    if let Some(retain) = retain {
        settings["retain_scopes"] = retain.into();
    }
    let host = Host::start(PRODUCT, settings).await;
    host.wait_ready().await;
    (dir, publisher, edge, host)
}

/// Scenarios 2, 4 and 5 over spans: cold bootstrap, a three-epoch sealed
/// span and an open span listed with their heights, lookups by scope index
/// and by `scope_id`, and a repeated tag across the overflow segment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_bootstrap_serves_multi_epoch_spans() {
    let (s0, s1) = (fixture(span0()), fixture(span1()));
    assert_eq!(s0.tables.len(), 2, "span 0 has an overflow segment");
    let (_dir, publisher, _edge, host) = started(&slots(Some(s0.clone()), s1.clone()), None).await;

    let map = reqwest::get(host.url("/map")).await.unwrap();
    assert_eq!(map.headers()["x-floor-scope"], "0");
    let head = ChainMapHead::parse(&map.bytes().await.unwrap()).unwrap();
    let open = &head.open_scopes[0];
    assert_eq!((open.scope_id, open.state), (3, ScopeState::Open));
    assert_eq!(open.first_height, ACTIVATION + 3 * EPOCH);
    assert_eq!(open.last_height, head.anchor_height);
    let chunk = reqwest::get(host.url(&format!("/map/0/{}", head.sealed_chunks[0].sha256)))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let chunk = head.parse_chunk(PRODUCT, 0, &chunk).unwrap();
    let sealed = &chunk.scopes[0];
    assert_eq!((sealed.scope_id, sealed.first_height), (0, ACTIVATION));
    assert_eq!(sealed.last_height, ACTIVATION + 3 * EPOCH - 1);

    let client = host.client();
    check_answers(&client, &[(0, &s0), (1, &s1)], 8).await;
    let view = client.fetch_map().await.unwrap();
    for (spec, fixture, selector, segments) in [
        (span0(), &s0, ScopeSelector::Index(0), 2),
        (span1(), &s1, ScopeSelector::Id(3), 1),
        (span1(), &s1, ScopeSelector::Index(1), 1),
    ] {
        let key = spec.repeated_key();
        match client.lookup(&view, selector, &key).await.unwrap() {
            LookupOutcome::Found { entries, answer } => {
                assert_eq!(entries, fixture.expected(&key));
                assert_eq!(entries.len(), spec.repeats as usize);
                assert_eq!(answer.queries.len(), segments);
                assert_eq!(answer.scope_id, fixture.scope_id);
            }
            other => panic!("repeated tag answered {other:?}"),
        }
    }
    assert_eq!(
        host.metric("tachyon_first_contact_tables_served").await,
        Some(3)
    );
    assert_eq!(
        host.metric("tachyon_first_contact_newest_scope").await,
        Some(1)
    );
    host.stop().await;
    drop(publisher);
}

/// Spec 11.5: the floor and `retain_scopes` count spans by scope index:
/// retaining one span serves span 1 (`scope_id` 3) and nothing below it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_floor_is_a_scope_index() {
    let (s0, s1) = (fixture(span0()), fixture(span1()));
    let (_dir, publisher, _edge, host) =
        started(&slots(Some(s0.clone()), s1.clone()), Some(1)).await;
    let map = reqwest::get(host.url("/map")).await.unwrap();
    assert_eq!(map.headers()["x-floor-scope"], "1");
    assert_eq!(
        host.metric("tachyon_first_contact_tables_served").await,
        Some(1)
    );
    let stale = error_body(post_query(&host, fixture_query(&s0), "10.0.0.1").await, 409).await;
    assert_eq!(stale.map_sha256.unwrap().hex(), publisher.head().sha256);
    check_answers(&host.client(), &[(1, &s1)], 6).await;
    host.stop().await;
}

/// Scenario 9: a recovery epoch revokes the dropped span table (410 on
/// `query` and `public`); an unknown session is 409.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_epoch_revokes_dropped_sessions() {
    let (s1, s1b) = (fixture(span1()), fixture(span1_rebuilt()));
    let (_dir, publisher, _edge, host) = started(&slots(None, s1.clone()), None).await;
    publisher.distribution.recover().unwrap();
    publisher.publish_slots(&slots(None, s1b.clone()), 2);
    host.wait_map(&publisher.head().sha256).await;
    let revoked = error_body(post_query(&host, fixture_query(&s1), "10.0.0.1").await, 410).await;
    assert_eq!(revoked.code, ErrorCode::SessionUnavailable);
    error_body(
        reqwest::get(host.url(&format!("/public/{}", hex::encode(s1.session()))))
            .await
            .unwrap(),
        410,
    )
    .await;
    let mut unknown = fixture_query(&s1b);
    unknown[4..36].copy_from_slice(&[0xee; 32]);
    let stale = error_body(post_query(&host, unknown, "10.0.0.1").await, 409).await;
    assert_eq!(stale.map_sha256.unwrap().hex(), publisher.head().sha256);
    check_answers(&host.client(), &[(1, &s1b)], 6).await;
    host.stop().await;
}

/// Scenario 10: a non-canonical row is refused; the previous publication
/// keeps serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_row_refuses_the_publication() {
    let (s1, bad) = (fixture(span1()), corrupt_fixture(span1()));
    let (_dir, publisher, _edge, host) = started(&slots(None, s1.clone()), None).await;
    let served = publisher.head().sha256;
    publisher.publish_slots(&slots(None, bad.clone()), 1);
    host.wait_failed_passes(1).await;
    host.wait_map(&served).await;
    check_answers(&host.client(), &[(1, &s1)], 6).await;
    error_body(
        post_query(&host, fixture_query(&bad), "10.0.0.1").await,
        409,
    )
    .await;
    host.stop().await;
}

/// Memo and first-contact are independent products of one host: separate
/// publications, mounts, state and readiness.
#[cfg(feature = "memo")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memo_and_first_contact_serve_side_by_side() {
    let fc = fixture(span1());
    let memo = fixture(Spec::new(Product::Memo, 1, 2, 300).repeating(3));
    let dir = tempdir();
    let fc_publisher = Publisher::start(PRODUCT, &dir.path().join("fc-publisher")).await;
    fc_publisher.publish_slots(&slots(None, fc.clone()), 1);
    let memo_publisher = Publisher::start(Product::Memo, &dir.path().join("memo-publisher")).await;
    memo_publisher.publish(&[None, Some(memo.clone())], 1);
    let (fc_edge, memo_edge) = (
        Edge::start(fc_publisher.addr).await,
        Edge::start(memo_publisher.addr).await,
    );
    let host = Host::start_products(
        PRODUCT,
        &[
            (PRODUCT, settings(&fc_edge, &dir.path().join("fc"))),
            (
                Product::Memo,
                settings(&memo_edge, &dir.path().join("memo")),
            ),
        ],
    )
    .await;
    host.wait_ready().await;
    let health: serde_json::Value = reqwest::get(format!("http://{}/health", host.management))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["products"]["memo"]["ready"], true);
    assert_eq!(health["products"]["first-contact"]["ready"], true);
    check_answers(&host.client(), &[(1, &fc)], 4).await;
    let memo_client = tachyon_pir_cli::Client::new(&host.origin(), Product::Memo, None).unwrap();
    check_answers(&memo_client, &[(1, &memo)], 4).await;
    // A memo query is not a first-contact query.
    let mut wrong = fixture_query(&memo);
    wrong[..4].copy_from_slice(b"TFQ1");
    error_body(post_query(&host, wrong, "10.0.0.1").await, 409).await;
    host.stop().await;
}
