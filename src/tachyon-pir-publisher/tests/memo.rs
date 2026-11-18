//! The built binary hosting memo with first-contact (one definition, `memo`)
//! and, when compiled in, tachygram beside it, against an in-process node
//! double whose chain carries envelopes: readiness, both distribution
//! prefixes, metrics, SIGTERM and a restart over the same state (spec 7,
//! 11.4, 11.7, 11.8; scenarios 1 and 12, publisher side).
mod support;

use serde_json::{Value, json};
use std::{net::SocketAddr, path::Path, time::Duration};
use support::{Publisher, client, free, released, write_config};
use tachyon_fixtures::{
    AuthoredEntry, BlockSpec, ChainConfig, EntryKind, NodeDouble, NodeOptions, SyntheticChain,
    envelope::pack,
};

const PREFIXES: [&str; 2] = ["/provider/memo/v1/", "/provider/first-contact/v1/"];

/// Activation at 8 and 16-block epochs: 40 blocks span epochs 0 and 1. Block
/// 10 and block 30 carry valid envelopes with follow-ups, first contacts and
/// a wallet state; the random blocks carry random (invalid) memos and proof
/// stamps with tachygrams.
async fn node() -> NodeDouble {
    let mut chain = SyntheticChain::new(ChainConfig::with_seed(11));
    while chain.tip_height() < 40 {
        let height = chain.tip_height() + 1;
        let spec = if height == 10 || height == 30 {
            let mut entries = Vec::new();
            for (kind, count) in [
                (EntryKind::FollowUp, 3),
                (EntryKind::FirstContact, 2),
                (EntryKind::WalletState, 1),
            ] {
                for _ in 0..count {
                    entries.push(AuthoredEntry::random(chain.rng(), kind));
                }
            }
            let envelopes = pack(chain.rng(), entries);
            BlockSpec::new().envelopes(&envelopes)
        } else {
            chain.random_block_spec(height)
        };
        chain.append(spec);
    }
    NodeDouble::start(chain, NodeOptions::default())
        .await
        .unwrap()
}

struct Listeners {
    management: SocketAddr,
    memo: SocketAddr,
    tachygram: SocketAddr,
}

fn config(node: &NodeDouble, listeners: &Listeners, state: &Path) -> Value {
    let mut config = json!({
        "management_listen": listeners.management.to_string(),
        "chain": {"rpc_url": node.url(), "no_auth": true, "poll_ms": 100, "epoch_length": 16},
        "memo": {"enabled": true, "settings": {
            "listen": listeners.memo.to_string(),
            "state_dir": state.join("memo"),
        }},
        "first_contact": {"enabled": true, "settings": {}},
    });
    if cfg!(feature = "tachygram") {
        config["tachygram"] = json!({"enabled": true, "settings": {
            "listen": listeners.tachygram.to_string(),
            "state_dir": state.join("tachygram"),
        }});
    }
    config
}

async fn get(url: String) -> reqwest::Response {
    let response = client().get(&url).send().await.unwrap();
    assert_eq!(response.status(), 200, "{url}");
    response
}

/// The current envelope under `prefix`, which must be eligible and name a
/// publication made under its fence's recovery epoch.
async fn current(listen: SocketAddr, prefix: &str) -> Value {
    let current: Value = get(format!("http://{listen}{prefix}current"))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(current["eligible"], true, "{prefix}: {current}");
    let descriptor = &current["publication"]["descriptor"];
    assert!(descriptor.is_object(), "{prefix}: {current}");
    assert_eq!(
        descriptor["recovery_epoch"], current["fence"]["recovery_epoch"],
        "{prefix}: {current}"
    );
    current
}

/// The value of the unlabelled series `name` in a Prometheus text body.
fn gauge(metrics: &str, name: &str) -> f64 {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("{name} missing:\n{metrics}"))
        .trim()
        .parse()
        .unwrap()
}

fn number(value: &Value) -> u64 {
    value
        .as_u64()
        .unwrap_or_else(|| panic!("not a number: {value}"))
}

fn expected_ready() -> Value {
    let mut products = json!({"memo": {"phase": "running", "ready": true}});
    if cfg!(feature = "tachygram") {
        products["tachygram"] = json!({"phase": "running", "ready": true});
    }
    json!({"ready": true, "products": products})
}

/// Every served product's current envelope: memo, first-contact and, when
/// compiled, tachygram.
async fn currents(listeners: &Listeners) -> Vec<Value> {
    let mut currents = Vec::new();
    for prefix in PREFIXES {
        currents.push(current(listeners.memo, prefix).await);
    }
    if cfg!(feature = "tachygram") {
        currents.push(current(listeners.tachygram, "/provider/tachygram/v1/").await);
    }
    currents
}

#[tokio::test(flavor = "multi_thread")]
async fn memo_and_first_contact_publish_stop_on_sigterm_and_recover_across_restart() {
    let node = node().await;
    let temp = tempfile::tempdir().unwrap();
    let listeners = Listeners {
        management: free(),
        memo: free(),
        tachygram: free(),
    };
    let path = write_config(
        temp.path(),
        "publisher.json",
        &config(&node, &listeners, temp.path()),
    );

    let mut publisher =
        Publisher::spawn(&path, listeners.management, temp.path().join("first.log"));
    assert_eq!(
        publisher.ready(Duration::from_secs(25)).await,
        expected_ready()
    );
    let discovery = get(format!(
        "http://{}{}",
        listeners.management,
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

    let first = currents(&listeners).await;
    // The two products have their own scopes, fences and generations.
    assert_ne!(first[0]["scope"], first[1]["scope"]);
    let metrics = get(format!("http://{}/metrics", listeners.memo))
        .await
        .text()
        .await
        .unwrap();
    // Blocks 10 and 30 are below the anchor (38): 2 x 3 follow-ups, 2 x 2
    // first contacts, 2 x 1 wallet state.
    for (series, value) in [
        ("tachyon_memo_entries_total", 6.0),
        ("tachyon_memo_state_entries_total", 2.0),
        ("tachyon_memo_anchor_height", 38.0),
        ("tachyon_first_contact_entries_total", 4.0),
        ("tachyon_first_contact_anchor_height", 38.0),
    ] {
        assert_eq!(gauge(&metrics, series), value, "{series}:\n{metrics}");
    }
    assert!(
        metrics.contains("tachyon_memo_envelopes_invalid_total"),
        "{metrics}"
    );
    assert!(temp.path().join("memo").join("memo-v1.sqlite").exists());

    let status = publisher.stop(libc::SIGTERM, Duration::from_secs(15)).await;
    assert!(status.success(), "{status}: {}", publisher.log());
    released(listeners.management).await;
    released(listeners.memo).await;

    // Restart over the same state: ready again, every product under a new
    // recovery epoch and a new generation (spec 11.4).
    let mut publisher =
        Publisher::spawn(&path, listeners.management, temp.path().join("second.log"));
    assert_eq!(
        publisher.ready(Duration::from_secs(25)).await,
        expected_ready()
    );
    let second = currents(&listeners).await;
    for (first, second) in first.iter().zip(&second) {
        let epoch = |c: &Value| number(&c["fence"]["recovery_epoch"]);
        let generation = |c: &Value| number(&c["publication"]["descriptor"]["generation"]);
        assert!(
            epoch(second) > epoch(first),
            "{}: recovery epoch {} -> {}",
            first["scope"]["product"],
            epoch(first),
            epoch(second)
        );
        assert!(generation(second) > generation(first), "{second}");
        assert_eq!(second["scope"], first["scope"]);
    }
    let status = publisher.stop(libc::SIGTERM, Duration::from_secs(15)).await;
    assert!(status.success(), "{status}: {}", publisher.log());
}
