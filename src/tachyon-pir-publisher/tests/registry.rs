//! The built binary hosting the registry against an in-process node double
//! (for the network identity only): readiness, the empty log's publication,
//! the write API, a registration visible in a later publication, SIGTERM and
//! a restart over the same state (spec 9, 11.4, 11.7, 11.8; scenarios 1, 12
//! and 14, publisher side).
mod support;

use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    path::Path,
    time::{Duration, Instant},
};
use support::{Publisher, client, free, released, write_config};
use tachyon_fixtures::{ChainConfig, NodeDouble, NodeOptions, SyntheticChain};
use tachyon_pir_protocol::{
    Address, Challenge, KEM_EK_BYTES, PK_BYTES, RegisterRequest, Registration, RegistryMapHead,
    registry::solve_pow,
};

const DIFFICULTY_BITS: u32 = 4;

async fn node() -> NodeDouble {
    let chain = SyntheticChain::generate(ChainConfig::with_seed(5), 12);
    NodeDouble::start(chain, NodeOptions::default())
        .await
        .unwrap()
}

fn config(node: &NodeDouble, management: SocketAddr, listen: SocketAddr, state: &Path) -> Value {
    json!({
        "management_listen": management.to_string(),
        "chain": {"rpc_url": node.url(), "no_auth": true, "poll_ms": 100, "epoch_length": 16},
        "registry": {"enabled": true, "settings": {
            "listen": listen.to_string(),
            "state_dir": state,
            "difficulty_bits": DIFFICULTY_BITS,
        }}
    })
}

/// A valid address: `pk` below 2^248 (so below the Pallas base modulus),
/// every `ek` coefficient zero (below q = 3,329), a fixed `ρ`.
fn address() -> Address {
    let mut pk = [0x5a; PK_BYTES];
    pk[PK_BYTES - 1] = 0;
    let mut ek = [0u8; KEM_EK_BYTES];
    ek[KEM_EK_BYTES - 32..].copy_from_slice(&[0xa5; 32]);
    Address { pk, ek }
}

async fn challenge(listen: SocketAddr, mount: &str) -> Challenge {
    let response = client()
        .get(format!("http://{listen}{mount}/challenge"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{mount}");
    assert_eq!(response.headers()["cache-control"], "no-store");
    response.json().await.unwrap()
}

/// Register `address` under a fresh challenge, solving the proof of work
/// with the protocol crate's client helper.
async fn register(listen: SocketAddr, address: &Address) -> Registration {
    let challenge = challenge(listen, "/registry/v1").await;
    assert_eq!(challenge.difficulty_bits, DIFFICULTY_BITS);
    let nonce = solve_pow(&challenge.challenge.0, &address.digest(), DIFFICULTY_BITS).unwrap();
    let body = RegisterRequest {
        challenge: challenge.challenge.0,
        nonce,
        address: address.clone(),
    }
    .encode();
    let response = client()
        .post(format!("http://{listen}/registry/v1/register"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.json().await.unwrap()
}

/// The eligible current envelope once its map head has `log_sequence`.
async fn published_at(listen: SocketAddr, log_sequence: u64) -> (Value, RegistryMapHead) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let current: Value = client()
            .get(format!("http://{listen}/provider/registry/v1/current"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if current["eligible"] == true
            && let Some(sha256) =
                current["publication"]["descriptor"]["map_head"]["sha256"].as_str()
        {
            let bytes = client()
                .get(format!(
                    "http://{listen}/provider/registry/v1/objects/{sha256}"
                ))
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let head = RegistryMapHead::parse(&bytes).unwrap();
            if head.log_sequence == log_sequence {
                assert_eq!(
                    current["publication"]["descriptor"]["recovery_epoch"],
                    current["fence"]["recovery_epoch"]
                );
                return (current, head);
            }
        }
        assert!(
            Instant::now() < deadline,
            "no publication at log_sequence {log_sequence}: {current}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn epoch(current: &Value) -> u64 {
    current["fence"]["recovery_epoch"].as_u64().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn registry_publishes_accepts_a_registration_and_recovers_across_restart() {
    let node = node().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("registry");
    let (management, listen) = (free(), free());
    let path = write_config(
        temp.path(),
        "publisher.json",
        &config(&node, management, listen, &state),
    );
    let ready =
        json!({"ready": true, "products": {"registry": {"phase": "running", "ready": true}}});

    let mut publisher = Publisher::spawn(&path, management, temp.path().join("first.log"));
    assert_eq!(publisher.ready(Duration::from_secs(25)).await, ready);

    // The empty log is published, bound to the node's network (spec 9.2).
    let (empty, head) = published_at(listen, 0).await;
    let genesis = node.chain().genesis_hash();
    assert_eq!(*head.genesis_hash.protocol_bytes(), genesis);
    assert_eq!(head.network, node.chain().chain_name());

    // Both mounts serve challenges; a registration is acknowledged and
    // appears in a later publication.
    challenge(listen, "/v1/registry").await;
    let address = address();
    let registration = register(listen, &address).await;
    assert!(registration.created);
    assert_eq!(registration.chunk_id, 0);
    assert_eq!(registration.visible_after_sequence, 0);
    assert_eq!(registration.digest.0, address.digest());
    let (first, head) = published_at(listen, 1).await;
    assert_eq!(head.open_scopes[0].entries, 1);
    assert!(number(&first["sequence"]) > number(&empty["sequence"]));

    let metrics = client()
        .get(format!("http://{listen}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for line in [
        "tachyon_registry_entries_total 1\n",
        "tachyon_registry_registrations_total{result=\"created\"} 1\n",
    ] {
        assert!(metrics.contains(line), "missing {line:?} in\n{metrics}");
    }

    let status = publisher.stop(libc::SIGTERM, Duration::from_secs(15)).await;
    assert!(status.success(), "{status}: {}", publisher.log());
    released(management).await;
    released(listen).await;

    // Restart: a new recovery epoch, the registration still published and
    // still known to the write API (idempotent, spec 9.3).
    let mut publisher = Publisher::spawn(&path, management, temp.path().join("second.log"));
    assert_eq!(publisher.ready(Duration::from_secs(25)).await, ready);
    let (second, head) = published_at(listen, 1).await;
    assert!(epoch(&second) > epoch(&first), "{first} -> {second}");
    assert_eq!(second["scope"], first["scope"]);
    assert_eq!(head.open_scopes[0].entries, 1);
    let again = register(listen, &address).await;
    assert!(!again.created);
    assert_eq!(again.visible_after_sequence, 0);
    let status = publisher.stop(libc::SIGTERM, Duration::from_secs(15)).await;
    assert!(status.success(), "{status}: {}", publisher.log());
}

fn number(value: &Value) -> u64 {
    value
        .as_u64()
        .unwrap_or_else(|| panic!("not a number: {value}"))
}
