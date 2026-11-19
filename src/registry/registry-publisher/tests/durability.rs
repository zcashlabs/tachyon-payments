//! Scenario 14, durable before acknowledgement, and scenario 12, restart:
//! the registry runs in a child process (this test binary re-executed), takes
//! registrations over HTTP and is killed with SIGKILL right after the last
//! `200`. Every acknowledged entry is in the log and in the next publication.
mod common;

use common::*;
use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    time::Duration,
};
use tachyon_pir_protocol::{Challenge, Registration};

/// Kills the child if the parent fails before killing it on purpose.
struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

const CHILD: &str = "REGISTRY_PUBLISHER_DURABILITY_CHILD";
const NAME: &str = "acknowledged_registrations_survive_a_kill";

/// The child: serve the registry with its publication loop until killed.
async fn child(dir: &str) -> ! {
    let server = Server::start(std::path::Path::new(dir), true, |_| {}).await;
    println!("LISTEN {}", server.addr);
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_registrations_survive_a_kill() {
    if let Ok(dir) = std::env::var(CHILD) {
        child(&dir).await;
    }
    let dir = tempfile::tempdir().unwrap();
    let mut process = Child(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", NAME, "--nocapture", "--test-threads=1"])
            .env(CHILD, dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    // Read the child's address on a thread, so a child that never starts
    // fails the test instead of hanging it.
    let stdout = process.0.stdout.take().unwrap();
    let (found, address_line) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { return };
            // libtest prints the test name on the same line first.
            if let Some(at) = line.find("LISTEN ") {
                let _ = found.send(line[at + "LISTEN ".len()..].trim().to_owned());
                return;
            }
        }
    });
    let addr = address_line
        .recv_timeout(Duration::from_secs(20))
        .expect("the child prints its address");
    let client = reqwest::Client::new();
    let challenge: Challenge = client
        .get(format!("http://{addr}/registry/v1/challenge"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let addresses: Vec<_> = (200..203).map(address).collect();
    let mut acknowledged = Vec::new();
    for address in &addresses {
        let response = client
            .post(format!("http://{addr}/registry/v1/register"))
            .body(body(
                challenge.challenge.0,
                address,
                challenge.difficulty_bits,
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        acknowledged.push(response.json::<Registration>().await.unwrap());
    }
    // Killed at once after the last acknowledgement: no graceful shutdown.
    process.0.kill().unwrap();
    process.0.wait().unwrap();

    let store = registry_store::Store::open(dir.path().join(registry_store::STORE_FILE), binding())
        .unwrap();
    for (address, registration) in addresses.iter().zip(&acknowledged) {
        let stored = store.lookup(&address.digest()).unwrap().unwrap();
        assert_eq!(stored.sequence, registration.visible_after_sequence);
        assert_eq!(stored.chunk_id, registration.chunk_id);
    }
    assert_eq!(store.log_sequence().unwrap(), 3);
    drop(store);

    // The restarted registry publishes them: the first map past each
    // acknowledged sequence contains its entry.
    let mut server = Server::start(dir.path(), false, |_| {}).await;
    server.publish().await;
    let published = Published::read(server.registry.distribution());
    assert_eq!(published.head.log_sequence, 3);
    assert!(published.head.recovery_epoch.get() >= 2);
    let rows = published.rows(server.registry.distribution(), 0);
    for address in &addresses {
        assert_eq!(published.find(&rows, 0, address), 1);
    }
    server.stop().await;
}
