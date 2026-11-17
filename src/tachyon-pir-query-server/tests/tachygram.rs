//! The tachygram query host across real HTTP(S): publications from a
//! publisher-side `Distribution` behind a TLS edge, the host in-process, and
//! wallet requests from the probe client (`tachyon-pir-cli`) or built here
//! with `zakura-pir-client`. Spec 10, 11.5, 11.6 and scenarios 2, 3, 4, 9, 10
//! and 13.
mod support;

use std::{sync::Arc, time::Duration};
use support::*;
use tachyon_pir_cli::{Client, LookupOutcome, Mount, ScopeSelector};
use tachyon_pir_protocol::{ErrorCode, PUBLISHED_GEOMETRY, Product};

const PRODUCT: Product = Product::Tachygram;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Scope 0 (sealed) and scope 1 (open), 300 entries each.
fn scope0() -> Arc<Fixture> {
    fixture(Spec::new(PRODUCT, 0, 1, 300))
}
fn scope1() -> Arc<Fixture> {
    fixture(Spec::new(PRODUCT, 1, 2, 300))
}
/// Scope 1 rebuilt with other entries, as after a reorg.
fn scope1_rebuilt() -> Arc<Fixture> {
    fixture(Spec::new(PRODUCT, 1, 3, 300))
}

async fn started(scopes: &[Option<Arc<Fixture>>]) -> (tempfile::TempDir, Publisher, Edge) {
    let dir = tempdir();
    let publisher = Publisher::start(PRODUCT, &dir.path().join("publisher")).await;
    publisher.publish(scopes, 1);
    let edge = Edge::start(publisher.addr).await;
    (dir, publisher, edge)
}

fn host_settings(edge: &Edge, dir: &tempfile::TempDir) -> serde_json::Value {
    let host = dir.path().join("host");
    std::fs::create_dir_all(&host).unwrap();
    settings(edge, &host)
}

/// Scenarios 2 and 4: cold bootstrap over TLS to ready, exact answers for
/// hits and misses on both mounts, the map headers and cache rules, the
/// management routes and metrics, and a clean stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_bootstrap_serves_exact_answers() {
    let (s0, s1) = (scope0(), scope1());
    let (dir, _publisher, edge) = started(&[Some(s0.clone()), Some(s1.clone())]).await;
    let host = Host::start(PRODUCT, host_settings(&edge, &dir)).await;
    host.wait_ready().await;

    let health: serde_json::Value = reqwest::get(format!("http://{}/health", host.management))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["ready"], true);
    assert_eq!(health["products"]["tachygram"]["phase"], "running");
    assert_eq!(health["products"]["tachygram"]["ready"], true);

    let map = reqwest::get(host.url("/map")).await.unwrap();
    assert_eq!(map.status(), 200);
    assert_eq!(map.headers()["cache-control"], "no-store");
    assert_eq!(map.headers()["content-type"], "application/json");
    assert_eq!(map.headers()["x-floor-scope"], "0");
    let sha = map.headers()["x-map-sha256"].to_str().unwrap().to_string();
    let head_bytes = map.bytes().await.unwrap();
    assert_eq!(tachyon_pir_protocol::map_sha256(&head_bytes).hex(), sha);
    let head = tachyon_pir_protocol::ChainMapHead::parse(&head_bytes).unwrap();
    let chunk = &head.sealed_chunks[0];
    let response = reqwest::get(host.url(&format!("/map/0/{}", chunk.sha256)))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
    let session = hex::encode(s1.session());
    let public = reqwest::get(host.url(&format!("/public/{session}")))
        .await
        .unwrap();
    assert_eq!(public.status(), 200);
    assert_eq!(public.headers()["content-type"], "application/octet-stream");
    assert_eq!(
        public.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
    assert_eq!(&public.bytes().await.unwrap()[..], &s1.tables[0].public[..]);

    let client = Client::new(&host.origin(), PRODUCT, None).unwrap();
    check_answers(&client, &[(0, &s0), (1, &s1)], 12).await;
    let legacy = Client::with_mount(&host.origin(), PRODUCT, None, Mount::Legacy).unwrap();
    check_answers(&legacy, &[(1, &s1)], 4).await;

    let metrics = host.metrics().await;
    for line in [
        "tachyon_tachygram_ready 1",
        &format!(
            "tachyon_tachygram_anchor_height {}",
            first_height(1) + ANCHOR_OFFSET
        ),
        "tachyon_tachygram_tables_served 2",
        "tachyon_tachygram_entries_total 600",
        "tachyon_tachygram_open_scope_entries 300",
        "tachyon_tachygram_sessions_revoked_total 0",
        "tachyon_tachygram_queries_total{result=\"answered\"} 56",
        "pir_http_arrivals_total{endpoint=\"tachygram_query\"} 56",
    ] {
        assert!(metrics.lines().any(|l| l == line), "{line} in\n{metrics}");
    }
    assert!(!metrics.contains(&session), "no session id becomes a label");
    host.stop().await;
}

/// Scenario 3: a new publication in the same recovery epoch is prepared
/// before it serves; the unchanged sealed table keeps its session and
/// runtime, and the displaced open table stays served in its retention
/// window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_publication_keeps_sealed_sessions() {
    let (s0, s1, s1b) = (scope0(), scope1(), scope1_rebuilt());
    let (dir, publisher, edge) = started(&[Some(s0.clone()), Some(s1.clone())]).await;
    let host = Host::start(PRODUCT, host_settings(&edge, &dir)).await;
    host.wait_ready().await;
    // Same anchor: the displaced view stays confirmed by the publisher.
    publisher.publish(&[Some(s0.clone()), Some(s1b.clone())], 1);
    host.wait_map(&publisher.head().sha256).await;
    let client = Client::new(&host.origin(), PRODUCT, None).unwrap();
    check_answers(&client, &[(0, &s0), (1, &s1b)], 6).await;
    // The displaced publication's session still answers (spec 11.6).
    let row = target_row(&s1, s1.entries[0].key());
    let (_, body) = query_body(PRODUCT, s1.session(), row);
    let response = post_query(&host, body, "10.0.0.1").await;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.bytes().await.unwrap().len(),
        PUBLISHED_GEOMETRY.response_len()
    );
    assert_eq!(
        host.metric("tachyon_tachygram_tables_served").await,
        Some(2)
    );
    host.stop().await;
}

/// Spec 10.4 and 10.5: every refusal of the query route and the object
/// routes, each with its error body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_and_unknown_requests_are_refused() {
    let s0 = scope0();
    let (dir, publisher, edge) = started(&[Some(s0.clone())]).await;
    let mut settings = host_settings(&edge, &dir);
    settings["admission"] = serde_json::json!({"upload_ms": 1000});
    let host = Host::start(PRODUCT, settings).await;
    host.wait_ready().await;
    let map_sha = publisher.head().sha256;
    let row = target_row(&s0, s0.entries[0].key());
    let (header, body) = query_body(PRODUCT, s0.session(), row);

    // A correct query answers with the echoed header.
    let ok = post_query(&host, body.clone(), "10.0.0.1").await;
    assert_eq!(ok.status(), 200);
    assert_eq!(ok.headers()["cache-control"], "no-store");
    let bytes = ok.bytes().await.unwrap();
    assert_eq!(bytes.len(), PUBLISHED_GEOMETRY.response_len());
    assert_eq!(&bytes[..52], &header.encode());

    let mut magic = body.clone();
    magic[..4].copy_from_slice(b"TMQ1");
    let body_400 = error_body(post_query(&host, magic, "10.0.0.1").await, 400).await;
    assert_eq!(body_400.code, ErrorCode::InvalidRequest);
    error_body(
        post_query(&host, body[..body.len() - 1].to_vec(), "10.0.0.1").await,
        400,
    )
    .await;
    let mut longer = body.clone();
    longer.push(0);
    error_body(post_query(&host, longer, "10.0.0.1").await, 400).await;
    error_body(
        post_query(&host, body[..20].to_vec(), "10.0.0.1").await,
        400,
    )
    .await;
    // Over 512 KiB: refused before the body is read, by declared length or
    // while reading a chunked body.
    let declared = raw_request(&host, &format!("content-length: {}\r\n", 600 * 1024), &[]).await;
    assert!(declared.starts_with("HTTP/1.1 413"), "{declared}");
    assert!(
        declared.contains("\"code\":\"invalid_request\""),
        "{declared}"
    );
    let mut chunked = Vec::new();
    for _ in 0..40 {
        chunked.extend_from_slice(format!("{:x}\r\n", 16384).as_bytes());
        chunked.extend_from_slice(&[0u8; 16384]);
        chunked.extend_from_slice(b"\r\n");
    }
    chunked.extend_from_slice(b"0\r\n\r\n");
    let streamed = raw_request(&host, "transfer-encoding: chunked\r\n", &chunked).await;
    assert!(streamed.starts_with("HTTP/1.1 413"), "{streamed}");

    // An unknown session: 409 naming the current map.
    let mut unknown = body.clone();
    unknown[4..36].copy_from_slice(&[0xee; 32]);
    let stale = error_body(post_query(&host, unknown, "10.0.0.1").await, 409).await;
    assert_eq!(stale.map_sha256.unwrap().hex(), map_sha);
    let stale = error_body(
        reqwest::get(host.url(&format!("/public/{}", "ee".repeat(32))))
            .await
            .unwrap(),
        409,
    )
    .await;
    assert_eq!(stale.map_sha256.unwrap().hex(), map_sha);
    let stale = error_body(
        reqwest::get(host.url(&format!("/map/0/{}", "ee".repeat(32))))
            .await
            .unwrap(),
        409,
    )
    .await;
    assert_eq!(stale.map_sha256.unwrap().hex(), map_sha);
    error_body(reqwest::get(host.url("/public/zz")).await.unwrap(), 400).await;
    error_body(reqwest::get(host.url("/map/x/00")).await.unwrap(), 400).await;
    error_body(reqwest::get(host.url("/query")).await.unwrap(), 405).await;
    error_body(
        reqwest::get(format!("http://{}/tachygram/v1/nothing", host.public))
            .await
            .unwrap(),
        404,
    )
    .await;

    // A slow upload: 408 after the configured deadline.
    let mut socket = tokio::net::TcpStream::connect(host.public).await.unwrap();
    let request = format!(
        "POST /tachygram/v1/query HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    socket.write_all(&body[..1000]).await.unwrap();
    let mut response = vec![0; 4096];
    let read = tokio::time::timeout(Duration::from_secs(10), socket.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    let text = String::from_utf8_lossy(&response[..read]);
    assert!(text.starts_with("HTTP/1.1 408"), "{text}");
    assert!(text.contains("\"code\":\"upload_timeout\""), "{text}");

    let metrics = host.metrics().await;
    for line in [
        "tachyon_tachygram_queries_total{result=\"answered\"} 1",
        "tachyon_tachygram_queries_total{result=\"invalid_request\"} 4",
        "tachyon_tachygram_queries_total{result=\"too_large\"} 2",
        "tachyon_tachygram_queries_total{result=\"stale_routing\"} 1",
        "tachyon_tachygram_queries_total{result=\"upload_timeout\"} 1",
    ] {
        assert!(metrics.lines().any(|l| l == line), "{line} in\n{metrics}");
    }
    host.stop().await;
}

/// Send a raw query request with `headers` and as much of `body` as the
/// server reads, and return the response head and body as text.
async fn raw_request(host: &Host, headers: &str, body: &[u8]) -> String {
    let mut socket = tokio::net::TcpStream::connect(host.public).await.unwrap();
    let head = format!(
        "POST /tachygram/v1/query HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n{headers}\r\n"
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    // The server may answer and close before the body is sent.
    let _ = socket.write_all(body).await;
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut response)).await;
    String::from_utf8_lossy(&response).into_owned()
}

/// Scenario 9: a recovery epoch revokes the sessions the next publication
/// drops (410 on every route), keeps the rest, and answers the new tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_epoch_revokes_dropped_sessions() {
    let (s0, s1, s1b) = (scope0(), scope1(), scope1_rebuilt());
    let (dir, publisher, edge) = started(&[Some(s0.clone()), Some(s1.clone())]).await;
    let host = Host::start(PRODUCT, host_settings(&edge, &dir)).await;
    host.wait_ready().await;
    publisher.distribution.recover().unwrap();
    let committed = publisher.publish(&[Some(s0.clone()), Some(s1b.clone())], 9);
    assert_eq!(committed.recovery_epoch, 2);
    host.wait_map(&publisher.head().sha256).await;

    let row = target_row(&s1, s1.entries[0].key());
    let (_, body) = query_body(PRODUCT, s1.session(), row);
    let revoked = error_body(post_query(&host, body, "10.0.0.1").await, 410).await;
    assert_eq!(revoked.code, ErrorCode::SessionUnavailable);
    error_body(
        reqwest::get(host.url(&format!("/public/{}", hex::encode(s1.session()))))
            .await
            .unwrap(),
        410,
    )
    .await;
    let client = Client::new(&host.origin(), PRODUCT, None).unwrap();
    check_answers(&client, &[(0, &s0), (1, &s1b)], 6).await;
    assert_eq!(
        host.metric("tachyon_tachygram_sessions_revoked_total")
            .await,
        Some(1)
    );
    assert_eq!(
        host.metric("tachyon_tachygram_recovery_epoch").await,
        Some(2)
    );
    host.stop().await;
}

async fn refused_publication_keeps_previous(bad: Arc<Fixture>) {
    let s0 = scope0();
    let (dir, publisher, edge) = started(&[Some(s0.clone())]).await;
    let host = Host::start(PRODUCT, host_settings(&edge, &dir)).await;
    host.wait_ready().await;
    let served = publisher.head().sha256;
    publisher.publish(&[Some(s0.clone()), Some(bad.clone())], 2);
    host.wait_failed_passes(1).await;
    host.wait_map(&served).await;
    let client = Client::new(&host.origin(), PRODUCT, None).unwrap();
    check_answers(&client, &[(0, &s0)], 8).await;
    let row = target_row(&bad, bad.entries[0].key());
    let (_, body) = query_body(PRODUCT, bad.session(), row);
    error_body(post_query(&host, body, "10.0.0.1").await, 409).await;
    assert_eq!(host.ready_status().await, 200);
    host.stop().await;
}

/// Scenario 10: a published table with a non-canonical row is refused by
/// row validation; the previous publication keeps serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_row_refuses_the_publication() {
    refused_publication_keeps_previous(corrupt_fixture(Spec::new(PRODUCT, 1, 2, 300))).await;
}

/// Scenario 10: a table whose published public setup is not the one its rows
/// prepare is refused after preparation; the previous publication keeps
/// serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_public_setup_refuses_the_publication() {
    refused_publication_keeps_previous(wrong_public_fixture(Spec::new(PRODUCT, 1, 2, 300))).await;
}

/// Scenario 13: refusals are 429 with `Retry-After: 1`, per client and for
/// the evaluation queue, and every admitted query completes correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn overload_refuses_with_retry_after_and_admitted_queries_complete() {
    let s0 = scope0();
    let (dir, _publisher, edge) = started(&[Some(s0.clone())]).await;
    let mut settings = host_settings(&edge, &dir);
    settings["admission"] =
        serde_json::json!({"executing": 1, "waiting": 0, "per_client": 1, "upload_ms": 10000});
    let host = Host::start(PRODUCT, settings).await;
    host.wait_ready().await;
    let row = target_row(&s0, s0.entries[0].key());
    let (_, body) = query_body(PRODUCT, s0.session(), row);

    // A client whose one slot is held by an upload in progress is refused.
    let mut socket = tokio::net::TcpStream::connect(host.public).await.unwrap();
    let request = format!(
        "POST /tachygram/v1/query HTTP/1.1\r\nhost: localhost\r\nx-forwarded-for: 10.9.9.9\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    socket.write_all(&body[..100]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    error_body(post_query(&host, body.clone(), "10.9.9.9").await, 429).await;
    socket.write_all(&body[100..]).await.unwrap();
    let mut response = Vec::new();
    let mut buffer = vec![0; 16384];
    while !response.windows(4).any(|w| w == b"\r\n\r\n")
        || response.len() < PUBLISHED_GEOMETRY.response_len()
    {
        let read = tokio::time::timeout(Duration::from_secs(10), socket.read(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert!(read > 0, "connection closed early");
        response.extend_from_slice(&buffer[..read]);
    }
    assert!(response.starts_with(b"HTTP/1.1 200"));

    // Bursts against one execution permit and no waiting room.
    let mut refused = 0;
    for burst in 0..10 {
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..32 {
            let (url, body) = (host.url("/query"), body.clone());
            tasks.spawn(async move {
                let response = reqwest::Client::new()
                    .post(url)
                    .header("x-forwarded-for", format!("10.{burst}.0.{i}"))
                    .body(body.clone())
                    .send()
                    .await
                    .unwrap();
                let status = response.status().as_u16();
                match status {
                    200 => {
                        let bytes = response.bytes().await.unwrap();
                        assert_eq!(bytes.len(), PUBLISHED_GEOMETRY.response_len());
                        assert_eq!(&bytes[..52], &body[..52]);
                    }
                    429 => {
                        error_body(response, 429).await;
                    }
                    other => panic!("unexpected HTTP {other}"),
                }
                status
            });
        }
        let statuses: Vec<u16> = tasks.join_all().await;
        assert!(statuses.contains(&200), "some queries are admitted");
        refused += statuses.iter().filter(|s| **s == 429).count();
        if refused > 0 {
            break;
        }
    }
    assert!(refused > 0, "a burst over one permit is refused");
    // Every admitted query decodes to the right row.
    let client = Client::new(&host.origin(), PRODUCT, None).unwrap();
    check_answers(&client, &[(0, &s0)], 4).await;
    host.stop().await;
}

/// Spec 11.6: the namespace answers 503 until the product is ready, and
/// `/ready` follows; the first publication makes it ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unavailable_until_the_first_publication() {
    let s0 = scope0();
    let dir = tempdir();
    let publisher = Publisher::start(PRODUCT, &dir.path().join("publisher")).await;
    let edge = Edge::start(publisher.addr).await;
    let host = Host::start(PRODUCT, host_settings(&edge, &dir)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(host.ready_status().await, 503);
    let body = error_body(reqwest::get(host.url("/map")).await.unwrap(), 503).await;
    assert_eq!(body.code, ErrorCode::TemporarilyUnavailable);
    error_body(
        reqwest::get(format!("http://{}/v1/tachygram/map", host.public))
            .await
            .unwrap(),
        503,
    )
    .await;
    let row = target_row(&s0, s0.entries[0].key());
    let (_, query) = query_body(PRODUCT, s0.session(), row);
    error_body(post_query(&host, query, "10.0.0.1").await, 503).await;
    error_body(
        reqwest::get(host.url(&format!("/public/{}", hex::encode(s0.session()))))
            .await
            .unwrap(),
        503,
    )
    .await;
    publisher.publish(&[Some(s0.clone())], 1);
    host.wait_ready().await;
    let client = Client::new(&host.origin(), PRODUCT, None).unwrap();
    check_answers(&client, &[(0, &s0)], 4).await;
    host.stop().await;
}

/// Spec 11.5: a host retaining fewer scopes reports its floor in
/// `X-Floor-Scope`, prepares nothing below it and answers its sessions 409.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retain_scopes_sets_the_host_floor() {
    let (s0, s1) = (scope0(), scope1());
    let (dir, publisher, edge) = started(&[Some(s0.clone()), Some(s1.clone())]).await;
    let mut settings = host_settings(&edge, &dir);
    settings["retain_scopes"] = 1.into();
    let host = Host::start(PRODUCT, settings).await;
    host.wait_ready().await;
    let map = reqwest::get(host.url("/map")).await.unwrap();
    assert_eq!(map.headers()["x-floor-scope"], "1");
    assert_eq!(
        host.metric("tachyon_tachygram_tables_served").await,
        Some(1)
    );
    assert_eq!(host.metric("tachyon_tachygram_floor_scope").await, Some(1));
    let row = target_row(&s0, s0.entries[0].key());
    let (_, body) = query_body(PRODUCT, s0.session(), row);
    let stale = error_body(post_query(&host, body, "10.0.0.1").await, 409).await;
    assert_eq!(stale.map_sha256.unwrap().hex(), publisher.head().sha256);
    let client = Client::new(&host.origin(), PRODUCT, None).unwrap();
    check_answers(&client, &[(1, &s1)], 6).await;
    let map = client.current_map().unwrap();
    assert_eq!((map.host_floor_scope(), map.floor_scope()), (1, 1));
    let before = client.stats();
    match client
        .lookup(&map, ScopeSelector::Index(0), s0.entries[0].key())
        .await
    {
        Ok(LookupOutcome::BelowFloor {
            scope_index: 0,
            floor_scope: 1,
        }) => {}
        other => panic!("a scope below the host floor answered {other:?}"),
    }
    assert_eq!(client.stats(), before, "nothing sent below the floor");
    host.stop().await;
}

/// Spec 11.6: a publisher of another major contract version is refused
/// before the host serves anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_major_mismatch_refuses_to_start() {
    let dir = tempdir();
    let publisher = Publisher::start(PRODUCT, &dir.path().join("publisher")).await;
    *publisher.discovery_version.lock().unwrap() = "999.0.0".into();
    let edge = Edge::start(publisher.addr).await;
    let host_dir = dir.path().join("host");
    std::fs::create_dir_all(&host_dir).unwrap();
    let settings = settings(&edge, &host_dir);
    let config = serde_json::json!({
        "management_listen": "127.0.0.1:1",
        "public_listen": "127.0.0.1:2",
        "tachygram": {"enabled": true, "settings": settings},
    });
    let config = tachyon_pir_query_server::Config::parse(config.to_string().as_bytes())
        .unwrap()
        .validate()
        .unwrap();
    let error =
        tachyon_pir_query_server::run(config, tachyon_pir_query_server::Shutdown::default())
            .await
            .unwrap_err();
    assert!(pir_sync_manager::is_incompatible(error.as_ref()), "{error}");
    assert!(!host_dir.join("state").exists(), "no state was opened");
}
