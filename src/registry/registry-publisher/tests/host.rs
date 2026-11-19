//! Scenario 1 through the publisher host: `configure` registered with
//! `pir_publisher::run_registered` and no chain settings (the registry
//! follows no chain), with no consumer. Readiness follows the distribution;
//! the one listener serves the write API at both mounts, the distribution
//! routes and `/metrics`; publications advance as registrations arrive.
mod common;

use common::*;
use pir_pipeline::{engine::PublisherEngine, service::Shutdown};
use registry_publisher::{RegistryConfig, configure};
use std::{
    net::{SocketAddr, TcpListener},
    time::{Duration, Instant},
};
use tachyon_pir_distribution::{Current, Inventory, Subject};
use tachyon_pir_protocol::{Challenge, Registration, RegistryMapHead};

fn free_ports() -> (SocketAddr, SocketAddr) {
    let a = TcpListener::bind("127.0.0.1:0").unwrap();
    let b = TcpListener::bind("127.0.0.1:0").unwrap();
    (a.local_addr().unwrap(), b.local_addr().unwrap())
}

async fn object(client: &reqwest::Client, listen: SocketAddr, sha256: &str) -> Vec<u8> {
    let response = client
        .get(format!(
            "http://{listen}/provider/registry/v1/objects/{sha256}"
        ))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    response.bytes().await.unwrap().to_vec()
}

async fn head_at(
    client: &reqwest::Client,
    listen: SocketAddr,
    log_sequence: u64,
) -> (Current, RegistryMapHead) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(response) = client
            .get(format!("http://{listen}/provider/registry/v1/current"))
            .send()
            .await
            && let Ok(current) = response.json::<Current>().await
            && current.eligible
            && let Some(publication) = &current.publication
        {
            let head = object(client, listen, &publication.descriptor.map_head.sha256).await;
            let head = RegistryMapHead::parse(&head).unwrap();
            if head.log_sequence == log_sequence {
                return (current, head);
            }
        }
        assert!(
            Instant::now() < deadline,
            "no publication at log_sequence {log_sequence}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_registered_host_serves_writes_and_publications_without_a_chain() {
    let dir = tempfile::tempdir().unwrap();
    let (listen, management) = free_ports();
    let config = RegistryConfig::new(settings(listen, dir.path()), identity());
    let mut ephemeral = config.clone();
    ephemeral.settings.listen = "127.0.0.1:0".parse().unwrap();
    assert!(configure(ephemeral, PublisherEngine).is_err());
    let definition = configure(config, PublisherEngine).unwrap();
    assert_eq!(definition.name, "registry");
    assert!(!definition.follows_chain());
    let shutdown = Shutdown::default();
    let host = tokio::spawn(pir_publisher::run_registered(
        management,
        vec![definition],
        None,
        shutdown.clone(),
    ));
    let client = reqwest::Client::new();

    let (first, head) = head_at(&client, listen, 0).await;
    assert_eq!(head.open_scopes.len(), 1);
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

    // The write API on the same listener, at both mounts.
    let challenge: Challenge = client
        .get(format!("http://{listen}/registry/v1/challenge"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let addresses = [address(300), address(301)];
    for (address, mount) in addresses.iter().zip(["/registry/v1", "/v1/registry"]) {
        let response = client
            .post(format!("http://{listen}{mount}/register"))
            .header("x-forwarded-for", "203.0.113.30")
            .body(body(
                challenge.challenge.0,
                address,
                challenge.difficulty_bits,
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let registration: Registration = response.json().await.unwrap();
        assert!(registration.created);
    }
    let (second, head) = head_at(&client, listen, 2).await;
    assert!(second.sequence > first.sequence);
    assert_eq!(head.open_scopes[0].entries, 2);

    // `authority/check` confirms the published subject and refutes a longer
    // log than the publisher holds.
    let reference = second.publication.as_ref().unwrap();
    let inventory: Inventory =
        serde_json::from_slice(&object(&client, listen, &reference.inventory.sha256).await)
            .unwrap();
    assert_eq!(inventory.subject, Subject::Registry { log_sequence: 2 });
    let request = pir_publication::AuthorityCheckRequest {
        authority: pir_publication::AuthorityBinding {
            sequence: second.sequence,
            fence: second.fence,
            publication: reference.publication,
        },
        subjects: vec![
            inventory.subject.clone(),
            Subject::Registry { log_sequence: 1 },
            Subject::Registry { log_sequence: 100 },
        ],
    };
    let response: pir_publication::AuthorityCheckResponse<Subject> = client
        .post(format!(
            "http://{listen}/provider/registry/v1/authority/check"
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
    assert_eq!(answers, [true, true, false]);

    let metrics = client
        .get(format!("http://{listen}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for line in [
        "tachyon_registry_entries_total 2\n",
        "tachyon_registry_registrations_total{result=\"created\"} 2\n",
        "pir_http_arrivals_total{endpoint=\"register\"} 2\n",
        "pir_http_arrivals_total{endpoint=\"challenge\"} 1\n",
    ] {
        assert!(metrics.contains(line), "missing {line:?} in\n{metrics}");
    }

    shutdown.request();
    tokio::time::timeout(Duration::from_secs(20), host)
        .await
        .expect("the host stops")
        .unwrap()
        .unwrap();
}
