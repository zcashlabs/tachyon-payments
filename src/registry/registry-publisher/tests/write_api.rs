//! The write API over real HTTP (spec 9.3, scenario 14): challenge format,
//! expiry and restart; every refusal of the validation order with its status
//! and body; the upload deadline; idempotence; the abuse controls.
mod common;

use common::*;
use std::time::Duration;
use tachyon_pir_protocol::{Address, RegisterRequest, pow_hash, pow_meets};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn now() -> u64 {
    registry_publisher::challenge::now_unix()
}

/// A registration whose headers and first `sent` body bytes are written,
/// from client `forwarded`; the rest is held back.
async fn partial_post(
    server: &Server,
    forwarded: &str,
    body: &[u8],
    sent: usize,
) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(server.addr).await.unwrap();
    let head = format!(
        "POST /registry/v1/register HTTP/1.1\r\nHost: registry\r\nConnection: close\r\n\
         Content-Type: application/octet-stream\r\nContent-Length: {}\r\n\
         X-Forwarded-For: {forwarded}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&body[..sent]).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

/// The status code and body of the response on `stream`.
async fn raw_response(mut stream: tokio::net::TcpStream) -> (u16, String) {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(20), stream.read_to_end(&mut bytes))
        .await
        .expect("a response")
        .unwrap();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let status = text[9..12].parse().unwrap();
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_owned();
    (status, body)
}

async fn metrics(client: &reqwest::Client, server: &Server) -> String {
    client
        .get(server.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

/// Wait until `count` registrations are in flight on the listener.
async fn in_flight(client: &reqwest::Client, server: &Server, count: usize) {
    let line = format!("pir_http_inflight{{endpoint=\"register\"}} {count}\n");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !metrics(client, server).await.contains(&line) {
        assert!(
            std::time::Instant::now() < deadline,
            "never {count} registrations in flight"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn challenges_are_stateless_hmacs_that_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path(), false, |_| {}).await;
    let before = now();
    let issued = challenge(&client, &server).await;
    let after = now();
    assert_eq!(issued.difficulty_bits, 6);
    assert!((before + 600..=after + 600).contains(&issued.expires_unix));
    let bytes = issued.challenge.0;
    assert_eq!(bytes[..8], issued.expires_unix.to_le_bytes());
    // Recomputed from the secret alone: expires || HMAC(...)[0..24].
    let key = key_of(dir.path());
    assert_eq!(key.challenge(issued.expires_unix), bytes);
    // The legacy alias serves the same operation.
    let legacy: tachyon_pir_protocol::Challenge = client
        .get(server.url("/v1/registry/challenge"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(key.verify(&legacy.challenge.0, now()).is_ok());
    let first = address(1);
    let response = post(&client, &server, body(bytes, &first, 6), None).await;
    assert_eq!(response.status(), 200);
    // The same challenge serves another registration.
    let second = address(2);
    let response = client
        .post(server.url("/v1/registry/register"))
        .body(body(bytes, &second, 6))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let secret = std::fs::read(dir.path().join("challenge-secret")).unwrap();
    server.stop().await;

    // After a restart the secret is reused and the old challenge still works.
    let server = Server::start(dir.path(), false, |_| {}).await;
    assert_eq!(
        std::fs::read(dir.path().join("challenge-secret")).unwrap(),
        secret
    );
    let third = address(3);
    let response = post(&client, &server, body(bytes, &third, 6), None).await;
    assert_eq!(response.status(), 200);
    let registration: tachyon_pir_protocol::Registration = response.json().await.unwrap();
    assert_eq!(registration.visible_after_sequence, 2);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_refusal_has_its_status_and_message_in_the_spec_order() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path(), false, |_| {}).await;
    let good = address(10);
    let issued = challenge(&client, &server).await.challenge.0;
    let valid = body(issued, &good, 6);
    let refused = |body: Vec<u8>| {
        let client = client.clone();
        let server = &server;
        async move { refusal(post(&client, server, body, None).await).await }
    };
    let invalid = |message: &str| (400, "invalid_request".to_owned(), message.to_owned(), None);

    // Body length: shorter 400, longer 413, empty 400.
    assert_eq!(
        refused(valid[..1255].to_vec()).await,
        invalid("registration body must be exactly 1256 bytes")
    );
    assert_eq!(
        refused(Vec::new()).await,
        invalid("registration body must be exactly 1256 bytes")
    );
    let mut long = valid.clone();
    long.push(0);
    assert_eq!(
        refused(long).await,
        (
            413,
            "invalid_request".to_owned(),
            "registration body is longer than 1256 bytes".to_owned(),
            None
        )
    );
    // Challenge: altered, then expired (authentic, clock past it).
    let mut forged = valid.clone();
    forged[20] ^= 1;
    assert_eq!(refused(forged).await, invalid("challenge is not authentic"));
    let expired = key_of(dir.path()).challenge(now() - 1);
    assert_eq!(
        refused(body(expired, &good, 6)).await,
        invalid("challenge has expired")
    );
    // Proof of work: a nonce that misses 6 bits.
    let digest = good.digest();
    let miss = (0u64..)
        .map(u64::to_le_bytes)
        .find(|n| !pow_meets(&pow_hash(&issued, n, &digest), 6))
        .unwrap();
    let missed = RegisterRequest {
        challenge: issued,
        nonce: miss,
        address: good.clone(),
    }
    .encode();
    assert_eq!(
        refused(missed).await,
        invalid("proof of work does not meet 6 bits")
    );
    // pk equal to p: not canonical.
    let mut p = registry_publisher::validate::PALLAS_BASE_MODULUS_LE;
    let non_canonical = Address { pk: p, ek: good.ek };
    assert_eq!(
        refused(body(issued, &non_canonical, 6)).await,
        invalid("pk is not a canonical Pallas base field element")
    );
    // ek with coefficient 500 equal to q, and with one of 0xfff.
    for value in [3329, 0xfff] {
        let bad_ek = Address {
            pk: good.pk,
            ek: ek_with(&good, 500, value),
        };
        assert_eq!(
            refused(body(issued, &bad_ek, 6)).await,
            invalid("ek fails the ML-KEM modulus check")
        );
    }
    // The order: a forged challenge is reported before a bad pk, a bad proof
    // of work before a bad pk, a bad pk before a bad ek.
    p[0] = 0xff;
    let both = Address {
        pk: p,
        ek: ek_with(&good, 0, 4000),
    };
    let mut forged_both = body(issued, &both, 6);
    forged_both[9] ^= 0x80;
    assert_eq!(
        refused(forged_both).await,
        invalid("challenge is not authentic")
    );
    let digest = both.digest();
    let miss = (0u64..)
        .map(u64::to_le_bytes)
        .find(|n| !pow_meets(&pow_hash(&issued, n, &digest), 6))
        .unwrap();
    let missed_both = RegisterRequest {
        challenge: issued,
        nonce: miss,
        address: both.clone(),
    }
    .encode();
    assert_eq!(
        refused(missed_both).await,
        invalid("proof of work does not meet 6 bits")
    );
    assert_eq!(
        refused(body(issued, &both, 6)).await,
        invalid("pk is not a canonical Pallas base field element")
    );
    // A zero key16 cannot be ground (it needs a 128-bit digest prefix); the
    // check is exercised through `validate::check_address` in its unit tests.

    // Wrong method and unknown route carry an invalid_request body too.
    let response = client
        .get(server.url("/registry/v1/register"))
        .send()
        .await
        .unwrap();
    let (status, code, _, _) = refusal(response).await;
    assert_eq!((status, code.as_str()), (405, "invalid_request"));
    let response = client
        .get(server.url("/registry/v1/unknown"))
        .send()
        .await
        .unwrap();
    let (status, code, _, _) = refusal(response).await;
    assert_eq!((status, code.as_str()), (404, "invalid_request"));

    // Nothing was written; the counters name each outcome.
    assert_eq!(server.registry.store().log_sequence().unwrap(), 0);
    let text = metrics(&client, &server).await;
    for line in [
        "tachyon_registry_registrations_total{result=\"invalid\"} 9\n",
        "tachyon_registry_registrations_total{result=\"too_large\"} 1\n",
        "tachyon_registry_registrations_total{result=\"pow_rejected\"} 2\n",
        "tachyon_registry_registrations_total{result=\"created\"} 0\n",
        "tachyon_registry_pow_rejected_total 2\n",
    ] {
        assert!(text.contains(line), "missing {line:?} in\n{text}");
    }
    // The valid body is accepted after all of it.
    assert_eq!(post(&client, &server, valid, None).await.status(), 200);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_upload_is_refused_with_408() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path(), false, |s| s.upload_ms = 300).await;
    let issued = challenge(&client, &server).await.challenge.0;
    let valid = body(issued, &address(20), 6);
    let started = std::time::Instant::now();
    let stream = partial_post(&server, "198.51.100.20", &valid, 100).await;
    let (status, body) = raw_response(stream).await;
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert_eq!(status, 408);
    let body: tachyon_pir_protocol::ErrorBody = serde_json::from_str(&body).unwrap();
    assert_eq!(body.code.as_str(), "upload_timeout");
    assert_eq!(server.registry.store().log_sequence().unwrap(), 0);
    assert!(
        metrics(&client, &server)
            .await
            .contains("tachyon_registry_registrations_total{result=\"upload_timeout\"} 1\n")
    );
    // A body sent in time is accepted.
    assert_eq!(post(&client, &server, valid, None).await.status(), 200);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn registration_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path(), false, |_| {}).await;
    let a = address(30);
    let first = register(&client, &server, &a, 6).await;
    assert!(first.created);
    assert_eq!((first.chunk_id, first.visible_after_sequence), (0, 0));
    assert_eq!(first.digest.0, a.digest());
    let again = register(&client, &server, &a, 6).await;
    assert!(!again.created);
    assert_eq!(
        (again.chunk_id, again.visible_after_sequence, again.digest),
        (first.chunk_id, first.visible_after_sequence, first.digest)
    );
    let b = register(&client, &server, &address(31), 6).await;
    assert!(b.created);
    assert_eq!(b.visible_after_sequence, 1);
    assert_eq!(server.registry.store().log_sequence().unwrap(), 2);
    let text = metrics(&client, &server).await;
    assert!(text.contains("tachyon_registry_registrations_total{result=\"created\"} 2\n"));
    assert!(text.contains("tachyon_registry_registrations_total{result=\"existing\"} 1\n"));
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn per_client_concurrency_is_capped_and_other_clients_are_unaffected() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path(), false, |s| {
        s.per_client_concurrent = 2;
        s.upload_ms = 10_000;
    })
    .await;
    let issued = challenge(&client, &server).await.challenge.0;
    let bodies: Vec<Vec<u8>> = (40..44).map(|i| body(issued, &address(i), 6)).collect();
    // Two uploads in flight hold both of the client's slots.
    let mut first = partial_post(&server, "203.0.113.1", &bodies[0], 10).await;
    let second = partial_post(&server, "203.0.113.1", &bodies[1], 10).await;
    in_flight(&client, &server, 2).await;
    let (status, code, _, retry) =
        refusal(post(&client, &server, bodies[2].clone(), Some("203.0.113.1")).await).await;
    assert_eq!((status, code.as_str(), retry), (429, "overloaded", Some(1)));
    // Another client is unaffected.
    let other = post(&client, &server, bodies[3].clone(), Some("203.0.113.2")).await;
    assert_eq!(other.status(), 200);
    // Finishing one upload frees a slot.
    first.write_all(&bodies[0][10..]).await.unwrap();
    assert_eq!(raw_response(first).await.0, 200);
    let freed = post(&client, &server, bodies[2].clone(), Some("203.0.113.1")).await;
    assert_eq!(freed.status(), 200);
    drop(second);
    assert!(
        metrics(&client, &server)
            .await
            .contains("tachyon_registry_registrations_total{result=\"overloaded\"} 1\n")
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn per_client_rate_is_a_sliding_minute_with_retry_after() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path(), false, |s| s.per_client_per_minute = 2).await;
    let issued = challenge(&client, &server).await.challenge.0;
    let x = "192.0.2.7";
    for i in 50..52 {
        let response = post(&client, &server, body(issued, &address(i), 6), Some(x)).await;
        assert_eq!(response.status(), 200);
    }
    let (status, code, _, retry) =
        refusal(post(&client, &server, body(issued, &address(52), 6), Some(x)).await).await;
    assert_eq!((status, code.as_str()), (429, "overloaded"));
    let retry = retry.expect("Retry-After");
    assert!((1..=60).contains(&retry), "{retry}");
    // Another client is unaffected.
    let y = "192.0.2.8";
    let response = post(&client, &server, body(issued, &address(53), 6), Some(y)).await;
    assert_eq!(response.status(), 200);
    // A request accepted for processing counts even when its body is then
    // refused: client z spends its rate on two short bodies.
    let z = "192.0.2.9";
    for _ in 0..2 {
        let (status, ..) = refusal(post(&client, &server, vec![0; 10], Some(z)).await).await;
        assert_eq!(status, 400);
    }
    let (status, ..) =
        refusal(post(&client, &server, body(issued, &address(54), 6), Some(z)).await).await;
    assert_eq!(status, 429);
    assert_eq!(server.registry.store().log_sequence().unwrap(), 3);
    assert!(
        metrics(&client, &server)
            .await
            .contains("tachyon_registry_registrations_total{result=\"rate_limited\"} 2\n")
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_global_cap_covers_every_client_and_the_challenge_route() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path(), false, |s| s.global_per_minute = 3).await;
    let issued = challenge(&client, &server).await.challenge.0;
    for (i, who) in [(60, "198.51.100.1"), (61, "198.51.100.2")] {
        let response = post(&client, &server, body(issued, &address(i), 6), Some(who)).await;
        assert_eq!(response.status(), 200);
    }
    let response = post(
        &client,
        &server,
        body(issued, &address(62), 6),
        Some("198.51.100.3"),
    )
    .await;
    let (status, code, _, retry) = refusal(response).await;
    assert_eq!((status, code.as_str()), (429, "overloaded"));
    assert!((1..=60).contains(&retry.unwrap()));
    let response = client
        .get(server.url("/registry/v1/challenge"))
        .send()
        .await
        .unwrap();
    let (status, _, _, retry) = refusal(response).await;
    assert_eq!(status, 429);
    assert!(retry.is_some());
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn without_x_forwarded_for_the_peer_address_is_the_client() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path(), false, |s| s.per_client_per_minute = 1).await;
    let issued = challenge(&client, &server).await.challenge.0;
    assert_eq!(
        post(&client, &server, body(issued, &address(70), 6), None)
            .await
            .status(),
        200
    );
    // The same peer (127.0.0.1) is refused; a forwarded client is not.
    let (status, ..) =
        refusal(post(&client, &server, body(issued, &address(71), 6), None).await).await;
    assert_eq!(status, 429);
    let forwarded = post(
        &client,
        &server,
        body(issued, &address(71), 6),
        Some("203.0.113.70"),
    )
    .await;
    assert_eq!(forwarded.status(), 200);
    server.stop().await;
}
