//! Scenario 1 through the publisher host: `configure` (memo with
//! first-contact, one definition) registered with
//! `pir_publisher::run_registered`, following the node double with the
//! host's own chain follower, with no consumer. Both publications advance as
//! the chain grows, readiness follows both distributions, and the one
//! listener serves both distribution prefixes and `/metrics`.
mod common;

use common::*;
use memo_publisher::{FirstContactSettings, MemoConfig, MemoSettings, configure};
use pir_pipeline::{engine::PublisherEngine, ingestion::ChainSettings, service::Shutdown};
use std::{
    net::{SocketAddr, TcpListener},
    time::{Duration, Instant},
};
use tachyon_pir_distribution::{Current, Inventory};
use tachyon_pir_protocol::{ChainMapHead, Entry, Product, ScopeTables};

/// Two distinct free loopback addresses: both are held while the second is
/// chosen, so the OS cannot hand out the same port twice.
fn free_ports() -> (SocketAddr, SocketAddr) {
    let a = TcpListener::bind("127.0.0.1:0").unwrap();
    let b = TcpListener::bind("127.0.0.1:0").unwrap();
    (a.local_addr().unwrap(), b.local_addr().unwrap())
}

async fn current(
    client: &reqwest::Client,
    listen: SocketAddr,
    product: Product,
) -> Option<Current> {
    let response = client
        .get(format!(
            "http://{listen}{}current",
            product.distribution_prefix()
        ))
        .send()
        .await
        .ok()?;
    assert_eq!(response.headers()["cache-control"], "no-store");
    response.json().await.ok()
}

async fn object(
    client: &reqwest::Client,
    listen: SocketAddr,
    product: Product,
    sha256: &str,
) -> Vec<u8> {
    let response = client
        .get(format!(
            "http://{listen}{}objects/{sha256}",
            product.distribution_prefix()
        ))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    response.bytes().await.unwrap().to_vec()
}

/// The served map head of `product` once its publication anchors at
/// `anchor`.
async fn wait_for_anchor(
    client: &reqwest::Client,
    listen: SocketAddr,
    product: Product,
    anchor: u32,
) -> (Current, ChainMapHead) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(current) = current(client, listen, product).await
            && current.eligible
            && let Some(publication) = &current.publication
        {
            let head = object(
                client,
                listen,
                product,
                &publication.descriptor.map_head.sha256,
            )
            .await;
            let head = ChainMapHead::parse(&head).unwrap();
            if head.anchor_height == anchor {
                return (current, head);
            }
        }
        assert!(
            Instant::now() < deadline,
            "no {product} publication anchored at {anchor}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn both_publications_advance_without_consumers_under_the_registered_host() {
    let plan = Plan::new(vec![(
        10,
        vec![
            Item::Random {
                follow_ups: 3,
                first_contacts: 2,
                wallet_states: 1,
            },
            Item::Malformed(tachyon_fixtures::Malformation::TrailingBytes),
        ],
    )]);
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
    let config = MemoConfig::new(
        MemoSettings {
            listen,
            state_dir: dir.path().to_path_buf(),
        },
        EPOCH_LENGTH,
    )
    .with_first_contact(FirstContactSettings {});
    assert!(configure(config.clone(), None, PublisherEngine).is_err());
    let definition = configure(config, Some(&chain_settings), PublisherEngine).unwrap();
    assert_eq!(definition.name, "memo");
    assert_eq!(definition.resources.listeners, [listen]);
    assert_eq!(definition.resources.probes.len(), 2);
    let shutdown = Shutdown::default();
    let host = tokio::spawn(pir_publisher::run_registered(
        management,
        vec![definition],
        Some(chain_settings),
        shutdown.clone(),
    ));
    let client = reqwest::Client::new();

    let mut first = Vec::new();
    for product in PRODUCTS {
        first.push(wait_for_anchor(&client, listen, product, 18).await.0);
    }
    // Readiness follows both eligible publications.
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

    // The chain grows; both publications follow without any consumer.
    grow(&mut node.chain(), &plan, 60);
    for (product, first) in PRODUCTS.iter().zip(&first) {
        let (second, head) = wait_for_anchor(&client, listen, *product, 58).await;
        assert!(second.sequence > first.sequence);
        let generation = |c: &Current| c.publication.as_ref().unwrap().descriptor.generation;
        assert!(generation(&second) > generation(first));
        let expected = expected(&node.chain(), *product, 58);
        assert_eq!(head.open_scopes.len(), expected.len());

        // Every table served over HTTP decodes to the authored entries.
        let reference = second.publication.as_ref().unwrap();
        let inventory: Inventory = serde_json::from_slice(
            &object(&client, listen, *product, &reference.inventory.sha256).await,
        )
        .unwrap();
        for (scope, want) in head.open_scopes.iter().zip(&expected) {
            let mut found: Vec<Vec<u8>> = Vec::new();
            for table in &scope.tables {
                let objects = inventory
                    .tables
                    .iter()
                    .find(|t| t.session_id == table.session_id.0)
                    .unwrap();
                assert_eq!(objects.scope_index, want.index);
                let rows = object(&client, listen, *product, &objects.rows.sha256).await;
                let public = object(&client, listen, *product, &objects.public.sha256).await;
                assert_eq!(public.len(), 45_056);
                for row in 0..4096u32 {
                    let context = scope.row_context(*product, &head.genesis_hash, table, row);
                    let slice = &rows[row as usize * 8192..(row as usize + 1) * 8192];
                    for entry in
                        tachyon_pir_protocol::validate_row(*product, slice, &context).unwrap()
                    {
                        found.push(entry.to_bytes());
                    }
                }
            }
            let mut want: Vec<Vec<u8>> = want.entries.iter().map(Entry::to_bytes).collect();
            want.sort();
            found.sort();
            assert_eq!(found, want, "{product} scope {}", scope.scope_id);
        }

        // `authority/check` confirms the published subject and refutes one
        // whose anchor hash is wrong.
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
                    "http://{listen}{}authority/check",
                    product.distribution_prefix()
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
        assert_eq!(answers, [true, false], "{product}");
    }

    let metrics = client
        .get(format!("http://{listen}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(metric(&metrics, "tachyon_memo_anchor_height"), "58");
    assert_eq!(
        metric(&metrics, "tachyon_first_contact_anchor_height"),
        "58"
    );
    assert_eq!(metric(&metrics, "tachyon_memo_entries_total"), "3");
    assert_eq!(metric(&metrics, "tachyon_first_contact_entries_total"), "2");
    assert_eq!(
        metric(&metrics, "tachyon_memo_envelopes_invalid_total"),
        "1"
    );
    assert_eq!(metric(&metrics, "tachyon_memo_state_entries_total"), "1");
    assert!(metrics.contains("pir_http_arrivals_total{endpoint=\"memo_current\"}"));
    assert!(metrics.contains("pir_http_arrivals_total{endpoint=\"first_contact_current\"}"));

    shutdown.request();
    tokio::time::timeout(Duration::from_secs(20), host)
        .await
        .expect("the host stops")
        .unwrap()
        .unwrap();
}
