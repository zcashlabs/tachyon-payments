//! The registry serving module across real HTTP(S) (spec 9, 10, 11.5):
//! lookups only (the write API is the publisher's). A full sealed chunk and
//! an open chunk, an empty log and an empty open chunk without tables,
//! revocation, a corrupt row, and the floor as a chunk index.
mod support;

use std::sync::Arc;
use support::*;
use tachyon_pir_cli::{Client, LookupOutcome, Mount, ScopeSelector};
use tachyon_pir_protocol::{ErrorCode, Product, REGISTRY_CHUNK_ENTRIES, RegistryMapHead};

const PRODUCT: Product = Product::Registry;

/// Chunk 0, sealed: exactly 3,072 registrations.
fn sealed() -> Spec {
    Spec::new(PRODUCT, 0, 1, REGISTRY_CHUNK_ENTRIES as u32)
}
/// Chunk 1, open.
fn open() -> Spec {
    Spec::new(PRODUCT, 1, 2, 120)
}
/// Chunk 0 while it is still the open chunk, and that chunk rebuilt.
fn first_open() -> Spec {
    Spec::new(PRODUCT, 0, 3, 120)
}
fn first_open_rebuilt() -> Spec {
    Spec::new(PRODUCT, 0, 4, 120)
}

async fn started(
    chunks: &[Option<Arc<Fixture>>],
    retain: Option<u64>,
) -> (tempfile::TempDir, Publisher, Edge, Host) {
    let dir = tempdir();
    let publisher = Publisher::start(PRODUCT, &dir.path().join("publisher")).await;
    publisher.publish_registry(chunks);
    let edge = Edge::start(publisher.addr).await;
    let mut settings = registry_settings(&edge, &dir.path().join("host"));
    if let Some(retain) = retain {
        settings["retain_scopes"] = retain.into();
    }
    let host = Host::start(PRODUCT, settings).await;
    host.wait_ready().await;
    (dir, publisher, edge, host)
}

async fn head(host: &Host) -> RegistryMapHead {
    let response = reqwest::get(host.url("/map")).await.unwrap();
    assert_eq!(response.status(), 200);
    RegistryMapHead::parse(&response.bytes().await.unwrap()).unwrap()
}

/// Scenarios 2 and 4: cold bootstrap over TLS with a full sealed chunk and
/// an open chunk; exact answers for registered and unregistered addresses on
/// both mounts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_bootstrap_serves_sealed_and_open_chunks() {
    let (c0, c1) = (fixture(sealed()), fixture(open()));
    assert_eq!(c0.entry_count(), REGISTRY_CHUNK_ENTRIES);
    let (_dir, _publisher, _edge, host) =
        started(&[Some(c0.clone()), Some(c1.clone())], None).await;
    let map = head(&host).await;
    assert_eq!(map.log_sequence, REGISTRY_CHUNK_ENTRIES + 120);
    assert_eq!(map.open_chunk().unwrap().scope_id, 1);
    let chunk = reqwest::get(host.url(&format!("/map/0/{}", map.sealed_chunks[0].sha256)))
        .await
        .unwrap();
    assert_eq!(
        chunk.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
    let chunk = map.parse_chunk(0, &chunk.bytes().await.unwrap()).unwrap();
    assert_eq!(chunk.scopes[0].entries, REGISTRY_CHUNK_ENTRIES);

    check_answers(&host.client(), &[(0, &c0), (1, &c1)], 12).await;
    let legacy = Client::with_mount(&host.origin(), PRODUCT, None, Mount::Legacy).unwrap();
    check_answers(&legacy, &[(1, &c1)], 4).await;

    let metrics = host.metrics().await;
    for line in [
        "tachyon_registry_ready 1",
        &format!(
            "tachyon_registry_log_sequence {}",
            REGISTRY_CHUNK_ENTRIES + 120
        ),
        "tachyon_registry_tables_served 2",
        &format!(
            "tachyon_registry_entries_total {}",
            REGISTRY_CHUNK_ENTRIES + 120
        ),
        "tachyon_registry_open_scope_entries 120",
        "tachyon_registry_newest_scope 1",
        "pir_http_responses_total{endpoint=\"registry_query\",code=\"2xx\"} 56",
    ] {
        assert!(metrics.lines().any(|l| l == line), "{line} in\n{metrics}");
    }
    assert!(!metrics.contains("tachyon_registry_anchor_height"));
    host.stop().await;
}

/// An empty log is one open chunk 0 without tables; a chunk that just
/// filled is sealed and the next, empty chunk is open (spec 10.3). Lookups
/// in a chunk without tables are absent without a query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_open_chunks_serve_without_tables() {
    let c0 = fixture(sealed());
    let (_dir, publisher, _edge, host) = started(&[None], None).await;
    assert_eq!(head(&host).await.log_sequence, 0);
    assert_eq!(host.metric("tachyon_registry_tables_served").await, Some(0));
    let client = host.client();
    let map = client.fetch_map().await.unwrap();
    match client
        .lookup(&map, ScopeSelector::Index(0), &missing_key(PRODUCT, 0))
        .await
        .unwrap()
    {
        LookupOutcome::Absent { answer } => assert!(answer.queries.is_empty()),
        other => panic!("an empty log answered {other:?}"),
    }

    publisher.publish_registry(&[Some(c0.clone()), None]);
    host.wait_map(&publisher.head().sha256).await;
    let map = head(&host).await;
    assert_eq!(map.log_sequence, REGISTRY_CHUNK_ENTRIES);
    let open = map.open_chunk().unwrap();
    assert_eq!((open.scope_id, open.entries, open.tables.len()), (1, 0, 0));
    assert_eq!(host.metric("tachyon_registry_tables_served").await, Some(1));
    check_answers(&client, &[(0, &c0)], 6).await;
    let map = client.fetch_map().await.unwrap();
    let key = c0.entries[0].key().to_vec();
    match client
        .lookup(&map, ScopeSelector::Index(1), &key)
        .await
        .unwrap()
    {
        LookupOutcome::Absent { answer } => assert!(answer.queries.is_empty()),
        other => panic!("the empty open chunk answered {other:?}"),
    }
    host.stop().await;
}

/// Scenario 9: a recovery epoch revokes the dropped open-chunk session (410
/// on `query` and `public`); an unknown session is 409; another product's
/// magic is 400.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_epoch_revokes_dropped_sessions() {
    let (c0, c0b) = (fixture(first_open()), fixture(first_open_rebuilt()));
    let (_dir, publisher, _edge, host) = started(&[Some(c0.clone())], None).await;
    publisher.distribution.recover().unwrap();
    publisher.publish_registry(&[Some(c0b.clone())]);
    host.wait_map(&publisher.head().sha256).await;
    let revoked = error_body(post_query(&host, fixture_query(&c0), "10.0.0.1").await, 410).await;
    assert_eq!(revoked.code, ErrorCode::SessionUnavailable);
    error_body(
        reqwest::get(host.url(&format!("/public/{}", hex::encode(c0.session()))))
            .await
            .unwrap(),
        410,
    )
    .await;
    let mut unknown = fixture_query(&c0b);
    unknown[4..36].copy_from_slice(&[0xee; 32]);
    let stale = error_body(post_query(&host, unknown, "10.0.0.1").await, 409).await;
    assert_eq!(stale.map_sha256.unwrap().hex(), publisher.head().sha256);
    let mut magic = fixture_query(&c0b);
    magic[..4].copy_from_slice(b"TGQ1");
    error_body(post_query(&host, magic, "10.0.0.1").await, 400).await;
    check_answers(&host.client(), &[(0, &c0b)], 6).await;
    assert_eq!(
        host.metric("tachyon_registry_sessions_revoked_total").await,
        Some(1)
    );
    host.stop().await;
}

/// Scenario 10: a non-canonical row is refused; the previous publication
/// keeps serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_row_refuses_the_publication() {
    let (c0, bad) = (fixture(first_open()), corrupt_fixture(first_open()));
    let (_dir, publisher, _edge, host) = started(&[Some(c0.clone())], None).await;
    let served = publisher.head().sha256;
    publisher.publish_registry(&[Some(bad.clone())]);
    host.wait_failed_passes(1).await;
    host.wait_map(&served).await;
    check_answers(&host.client(), &[(0, &c0)], 6).await;
    error_body(
        post_query(&host, fixture_query(&bad), "10.0.0.1").await,
        409,
    )
    .await;
    assert_eq!(host.ready_status().await, 200);
    host.stop().await;
}

/// Spec 11.5: the floor is a chunk index: retaining one chunk serves only
/// the open chunk 1 and answers chunk 0's session 409.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_floor_is_a_chunk_index() {
    let (c0, c1) = (fixture(sealed()), fixture(open()));
    let (_dir, publisher, _edge, host) =
        started(&[Some(c0.clone()), Some(c1.clone())], Some(1)).await;
    let map = reqwest::get(host.url("/map")).await.unwrap();
    assert_eq!(map.headers()["x-floor-scope"], "1");
    assert_eq!(host.metric("tachyon_registry_tables_served").await, Some(1));
    assert_eq!(host.metric("tachyon_registry_floor_scope").await, Some(1));
    let stale = error_body(post_query(&host, fixture_query(&c0), "10.0.0.1").await, 409).await;
    assert_eq!(stale.map_sha256.unwrap().hex(), publisher.head().sha256);
    check_answers(&host.client(), &[(1, &c1)], 6).await;
    host.stop().await;
}
