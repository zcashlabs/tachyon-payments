//! Start-up and shutdown of the built binary against an in-process node
//! double (spec 11.1, 11.6, 11.8).
mod support;

use serde_json::{Value, json};
use std::time::Duration;
use support::{Publisher, client, free, released, write_config};
use tachyon_fixtures::{ChainConfig, NodeDouble, NodeOptions, SyntheticChain};

async fn node(activation_height: Option<u64>) -> NodeDouble {
    let chain = SyntheticChain::generate(
        ChainConfig {
            activation_height,
            ..ChainConfig::default()
        },
        12,
    );
    NodeDouble::start(chain, NodeOptions::default())
        .await
        .unwrap()
}

fn config(management: std::net::SocketAddr, rpc_url: String) -> Value {
    json!({
        "management_listen": management.to_string(),
        "chain": {"rpc_url": rpc_url, "no_auth": true, "poll_ms": 100, "epoch_length": 16}
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_to_start_when_the_node_lacks_nutachyon() {
    let node = node(None).await;
    let temp = tempfile::tempdir().unwrap();
    let management = free();
    let path = write_config(
        temp.path(),
        "publisher.json",
        &config(management, node.url()),
    );
    let mut publisher = Publisher::spawn(&path, management, temp.path().join("publisher.log"));
    let status = publisher.exit(Duration::from_secs(20)).await;
    let log = publisher.log();
    assert_eq!(status.code(), Some(1), "{log}");
    assert!(log.contains("lists no NuTachyon upgrade"), "{log}");
    assert!(log.contains("refusing to start"), "{log}");
    // The host never started: nothing was bound.
    assert!(!log.contains("starting publisher host"), "{log}");
    assert!(node.count("getblockchaininfo") >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_to_start_when_the_node_is_unreachable() {
    let temp = tempfile::tempdir().unwrap();
    let management = free();
    let path = write_config(
        temp.path(),
        "publisher.json",
        &config(management, format!("http://{}", free())),
    );
    let mut publisher = Publisher::spawn(&path, management, temp.path().join("publisher.log"));
    let status = publisher.exit(Duration::from_secs(20)).await;
    let log = publisher.log();
    assert_eq!(status.code(), Some(1), "{log}");
    assert!(log.contains("cannot read the network identity"), "{log}");
    assert!(!log.contains("starting publisher host"), "{log}");
}

/// With no products the host still checks the network, serves its management
/// routes and stops cleanly on either signal.
#[tokio::test(flavor = "multi_thread")]
async fn management_listener_serves_readiness_and_discovery_and_stops_on_signals() {
    let node = node(Some(8)).await;
    for signal in [libc::SIGTERM, libc::SIGINT] {
        let temp = tempfile::tempdir().unwrap();
        let management = free();
        let path = write_config(
            temp.path(),
            "publisher.json",
            &config(management, node.url()),
        );
        let mut publisher = Publisher::spawn(&path, management, temp.path().join("publisher.log"));
        let ready = publisher.ready(Duration::from_secs(20)).await;
        assert_eq!(ready, json!({"ready": true, "products": {}}));

        let health: Value = client()
            .get(format!("http://{management}/health"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(health, ready);

        let response = client()
            .get(format!(
                "http://{management}{}",
                pir_service_discovery::PATH
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            pir_service_discovery::Document::publisher()
                .to_json()
                .as_slice()
        );

        let log = publisher.log();
        assert!(log.contains("node network identity"), "{log}");
        assert!(log.contains("activation_height=8"), "{log}");

        let status = publisher.stop(signal, Duration::from_secs(10)).await;
        let log = publisher.log();
        assert!(status.success(), "{status}: {log}");
        assert!(log.contains("publisher stopped cleanly"), "{log}");
        released(management).await;
    }
}
