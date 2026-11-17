//! The built binary hosting the tachygram publisher against an in-process
//! node double: readiness, discovery, the product listener, SIGTERM and a
//! restart over the same state (spec 11.4, 11.6, 11.8; scenarios 1 and 12,
//! publisher side).
mod support;

use serde_json::{Value, json};
use std::{net::SocketAddr, path::Path, time::Duration};
use support::{Publisher, client, free, released, write_config};
use tachyon_fixtures::{ChainConfig, NodeDouble, NodeOptions, SyntheticChain};

/// Activation at 8 and 16-block epochs: 40 blocks span epochs 0 and 1.
async fn node() -> NodeDouble {
    let chain = SyntheticChain::generate(ChainConfig::with_seed(7), 40);
    NodeDouble::start(chain, NodeOptions::default())
        .await
        .unwrap()
}

fn config(node: &NodeDouble, management: SocketAddr, listen: SocketAddr, state: &Path) -> Value {
    json!({
        "management_listen": management.to_string(),
        "chain": {"rpc_url": node.url(), "no_auth": true, "poll_ms": 100, "epoch_length": 16},
        "tachygram": {"enabled": true, "settings": {
            "listen": listen.to_string(),
            "state_dir": state,
        }}
    })
}

async fn get(url: String) -> reqwest::Response {
    let response = client().get(&url).send().await.unwrap();
    assert_eq!(response.status(), 200, "{url}");
    response
}

/// The current envelope, which must be eligible and name a publication.
async fn current(listen: SocketAddr) -> Value {
    let current: Value = get(format!("http://{listen}/provider/tachygram/v1/current"))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(current["eligible"], true, "{current}");
    let descriptor = &current["publication"]["descriptor"];
    assert!(descriptor.is_object(), "{current}");
    assert_eq!(
        descriptor["recovery_epoch"], current["fence"]["recovery_epoch"],
        "{current}"
    );
    current
}

fn number(value: &Value) -> u64 {
    value
        .as_u64()
        .unwrap_or_else(|| panic!("not a number: {value}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn publishes_serves_stops_on_sigterm_and_recovers_across_restart() {
    let node = node().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("tachygram");
    let (management, listen) = (free(), free());
    let path = write_config(
        temp.path(),
        "publisher.json",
        &config(&node, management, listen, &state),
    );

    let mut publisher = Publisher::spawn(&path, management, temp.path().join("first.log"));
    let ready = publisher.ready(Duration::from_secs(25)).await;
    assert_eq!(
        ready,
        json!({"ready": true, "products": {"tachygram": {"phase": "running", "ready": true}}})
    );
    let discovery = get(format!(
        "http://{management}{}",
        pir_service_discovery::PATH
    ))
    .await
    .bytes()
    .await
    .unwrap();
    assert_eq!(
        pir_service_discovery::Document::parse(&discovery).unwrap(),
        pir_service_discovery::Document::publisher()
    );
    // Discovery is served only on the management listener (spec 11.6).
    let elsewhere = client()
        .get(format!("http://{listen}{}", pir_service_discovery::PATH))
        .send()
        .await
        .unwrap();
    assert_eq!(elsewhere.status(), 404);

    let first = current(listen).await;
    let first_epoch = number(&first["fence"]["recovery_epoch"]);
    let first_generation = number(&first["publication"]["descriptor"]["generation"]);
    let metrics = get(format!("http://{listen}/metrics"))
        .await
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("tachyon_tachygram_"), "{metrics}");
    assert!(state.join("publisher.lock").exists());

    let status = publisher.stop(libc::SIGTERM, Duration::from_secs(15)).await;
    assert!(status.success(), "{status}: {}", publisher.log());
    released(management).await;
    released(listen).await;

    // Restart over the same state: ready again under a new recovery epoch
    // and a new generation (spec 11.4).
    let mut publisher = Publisher::spawn(&path, management, temp.path().join("second.log"));
    publisher.ready(Duration::from_secs(25)).await;
    let second = current(listen).await;
    let second_epoch = number(&second["fence"]["recovery_epoch"]);
    let second_generation = number(&second["publication"]["descriptor"]["generation"]);
    assert!(
        second_epoch > first_epoch,
        "recovery epoch {first_epoch} -> {second_epoch}"
    );
    assert!(
        second_generation > first_generation,
        "generation {first_generation} -> {second_generation}"
    );
    assert_eq!(second["scope"], first["scope"]);
    let status = publisher.stop(libc::SIGTERM, Duration::from_secs(15)).await;
    assert!(status.success(), "{status}: {}", publisher.log());
}

/// A listener the host cannot bind stops start-up with the owner and address
/// named, before any product state exists.
#[tokio::test(flavor = "multi_thread")]
async fn an_occupied_product_listener_refuses_start_up() {
    let node = node().await;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("tachygram");
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = occupied.local_addr().unwrap();
    let management = free();
    let path = write_config(
        temp.path(),
        "publisher.json",
        &config(&node, management, listen, &state),
    );
    let mut publisher = Publisher::spawn(&path, management, temp.path().join("publisher.log"));
    let status = publisher.exit(Duration::from_secs(20)).await;
    let log = publisher.log();
    assert_eq!(status.code(), Some(1), "{log}");
    assert!(
        log.contains(&format!("cannot bind tachygram listener {listen}")),
        "{log}"
    );
    assert!(!state.exists());
}
