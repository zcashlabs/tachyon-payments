//! Scenario 1 through the publisher host: `configure` registered with
//! `pir_publisher::run_registered`, following the node double with the
//! host's own chain follower, with no consumer. Publications advance as the
//! chain grows, readiness follows the distribution, and the product listener
//! serves the distribution routes and `/metrics`.
mod common;

use common::*;
use pir_pipeline::{engine::PublisherEngine, ingestion::ChainSettings, service::Shutdown};
use std::{
    net::{SocketAddr, TcpListener},
    time::{Duration, Instant},
};
use tachygram_publisher::{TachygramConfig, TachygramSettings, configure};
use tachyon_pir_distribution::{Current, Inventory};
use tachyon_pir_protocol::{ChainMapHead, ScopeState};

/// Two distinct free loopback addresses: both are held while the second is
/// chosen, so the OS cannot hand out the same port twice.
fn free_ports() -> (SocketAddr, SocketAddr) {
    let a = TcpListener::bind("127.0.0.1:0").unwrap();
    let b = TcpListener::bind("127.0.0.1:0").unwrap();
    (a.local_addr().unwrap(), b.local_addr().unwrap())
}

async fn current(client: &reqwest::Client, listen: SocketAddr) -> Option<Current> {
    let response = client
        .get(format!("http://{listen}/provider/tachygram/v1/current"))
        .send()
        .await
        .ok()?;
    assert_eq!(response.headers()["cache-control"], "no-store");
    response.json().await.ok()
}

async fn object(client: &reqwest::Client, listen: SocketAddr, sha256: &str) -> Vec<u8> {
    let response = client
        .get(format!(
            "http://{listen}/provider/tachygram/v1/objects/{sha256}"
        ))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    response.bytes().await.unwrap().to_vec()
}

/// The served map head once the publication anchors at `anchor`.
async fn wait_for_anchor(
    client: &reqwest::Client,
    listen: SocketAddr,
    anchor: u32,
) -> (Current, ChainMapHead) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(current) = current(client, listen).await
            && current.eligible
            && let Some(publication) = &current.publication
        {
            let head = object(client, listen, &publication.descriptor.map_head.sha256).await;
            let head = ChainMapHead::parse(&head).unwrap();
            if head.anchor_height == anchor {
                return (current, head);
            }
        }
        assert!(
            Instant::now() < deadline,
            "no publication anchored at {anchor}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn publications_advance_without_consumers_under_the_registered_host() {
    let plan = Plan::fresh(&[(10, &[3]), (30, &[2])]);
    let mut chain = new_chain(9);
    grow(&mut chain, &plan, 20);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let (listen, management) = free_ports();
    let chain_settings = ChainSettings {
        rpc_url: node.url(),
        rpc_cookie: None,
        poll: Duration::from_millis(50),
    };
    let config = TachygramConfig::new(
        TachygramSettings {
            listen,
            state_dir: dir.path().to_path_buf(),
        },
        EPOCH_LENGTH,
    );
    assert!(configure(config.clone(), None, PublisherEngine).is_err());
    let definition = configure(config, Some(&chain_settings), PublisherEngine).unwrap();
    let shutdown = Shutdown::default();
    let host = tokio::spawn(pir_publisher::run_registered(
        management,
        vec![definition],
        Some(chain_settings),
        shutdown.clone(),
    ));
    let client = reqwest::Client::new();

    let (first, head) = wait_for_anchor(&client, listen, 18).await;
    // Readiness follows the eligible publication.
    let deadline = Instant::now() + Duration::from_secs(10);
    while client
        .get(format!("http://{management}/ready"))
        .send()
        .await
        .map(|r| r.status().as_u16())
        .unwrap_or(0)
        != 200
    {
        assert!(Instant::now() < deadline, "the host never became ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(head.open_scopes.len(), 1);

    // The chain grows; publications follow without any consumer.
    grow(&mut node.chain(), &plan, 60);
    let (second, head) = wait_for_anchor(&client, listen, 58).await;
    assert!(second.sequence > first.sequence);
    let generation = |c: &Current| c.publication.as_ref().unwrap().descriptor.generation;
    assert!(generation(&second) > generation(&first));
    assert!(head.open_scopes.iter().all(|s| s.state == ScopeState::Open));
    assert_eq!(head.open_scopes.len() as u64, epoch_of(58) + 1);

    // Every table served over HTTP decodes to the authored records.
    let reference = second.publication.as_ref().unwrap();
    let inventory: Inventory =
        serde_json::from_slice(&object(&client, listen, &reference.inventory.sha256).await)
            .unwrap();
    for scope in &head.open_scopes {
        let expected = oracle(&node.chain(), scope.scope_id, 58);
        let mut found = Vec::new();
        for table in &scope.tables {
            let objects = inventory
                .tables
                .iter()
                .find(|t| t.session_id == table.session_id.0)
                .unwrap();
            let rows = object(&client, listen, &objects.rows.sha256).await;
            let public = object(&client, listen, &objects.public.sha256).await;
            assert_eq!(public.len(), 45_056);
            for row in 0..4096u32 {
                let context = tachyon_pir_protocol::ScopeTables::row_context(
                    scope,
                    PRODUCT,
                    &head.genesis_hash,
                    table,
                    row,
                );
                let slice = &rows[row as usize * 8192..(row as usize + 1) * 8192];
                for entry in tachyon_pir_protocol::validate_row(PRODUCT, slice, &context).unwrap() {
                    let tachyon_pir_protocol::Entry::Tachygram(entry) = entry else {
                        panic!("not a tachygram entry");
                    };
                    found.push(entry);
                }
            }
        }
        found.sort_by_key(|e| (e.tachygram, e.blk_height, e.stamp_ordinal));
        assert_eq!(found, expected, "epoch {}", scope.scope_id);
    }

    // `authority/check` confirms the published subject and refutes one whose
    // anchor hash is wrong.
    let mut refuted = inventory.subject.clone();
    if let tachyon_pir_distribution::Subject::Chain { anchor, .. } = &mut refuted {
        anchor.hash[0] ^= 1;
    }
    let request = pir_publication::AuthorityCheckRequest {
        authority: pir_publication::AuthorityBinding {
            sequence: second.sequence,
            fence: second.fence,
            publication: reference.publication,
        },
        subjects: vec![inventory.subject.clone(), refuted],
    };
    let response: pir_publication::AuthorityCheckResponse<tachyon_pir_distribution::Subject> =
        client
            .post(format!(
                "http://{listen}/provider/tachygram/v1/authority/check"
            ))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    let answers: Vec<bool> = response
        .answers
        .iter()
        .map(|a| a.still_authoritative)
        .collect();
    assert_eq!(answers, [true, false]);

    let metrics = client
        .get(format!("http://{listen}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains("tachyon_tachygram_anchor_height 58\n"),
        "{metrics}"
    );
    assert!(metrics.contains("tachyon_tachygram_entries_total 5\n"));
    assert!(metrics.contains("pir_http_arrivals_total{endpoint=\"current\"}"));

    shutdown.request();
    tokio::time::timeout(Duration::from_secs(20), host)
        .await
        .expect("the host stops")
        .unwrap()
        .unwrap();
}
