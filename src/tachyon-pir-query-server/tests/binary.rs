//! The built `tachyon-pir-query-server` binary: `--check`, and a real run
//! against a publisher behind TLS that answers a lookup and stops cleanly on
//! SIGTERM.
mod support;

use std::{
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use support::*;
use tachyon_pir_cli::{Client, LookupOutcome, ScopeSelector};
use tachyon_pir_protocol::Product;

const PRODUCT: Product = Product::Tachygram;

const BIN: &str = env!("CARGO_BIN_EXE_tachyon-pir-query-server");

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn write_config(dir: &Path, value: &serde_json::Value) -> std::path::PathBuf {
    let path = dir.join("query-server.json");
    std::fs::write(&path, value.to_string()).unwrap();
    path
}

fn check(path: &Path) -> (bool, String) {
    let output = Command::new(BIN)
        .arg("--config")
        .arg(path)
        .arg("--check")
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn check_validates_offline_and_exits_one_on_errors() {
    let dir = tempdir();
    let settings = serde_json::json!({
        // Nothing listens here: --check contacts no publisher.
        "publisher": "https://localhost:1/",
        "state_dir": dir.path().join("state"),
        "role": "combined",
        "publisher_network": "test",
        "publisher_genesis": "cd".repeat(32),
        "activation_height": 8,
        "epoch_length": 4096,
    });
    let mut config = serde_json::json!({
        "management_listen": "127.0.0.1:9500",
        "public_listen": "127.0.0.1:9510",
        "tachygram": {"enabled": true, "settings": settings},
    });
    assert_eq!(
        check(&write_config(dir.path(), &config)),
        (true, String::new())
    );
    assert!(!dir.path().join("state").exists(), "--check opens no state");

    config["tachygram"]["settings"]["role"] = "worker".into();
    let (ok, stderr) = check(&write_config(dir.path(), &config));
    assert!(!ok && stderr.contains("combined"), "{stderr}");
    config["tachygram"]["settings"]["role"] = "combined".into();
    // A section of a product not compiled in; with every product compiled,
    // a registry section with chain parameters it does not take.
    let compiled = [
        ("memo", cfg!(feature = "memo")),
        ("first_contact", cfg!(feature = "first-contact")),
        ("registry", cfg!(feature = "registry")),
    ];
    let (section, refusal) = match compiled.iter().find(|(_, compiled)| !compiled) {
        Some((section, _)) => (*section, "not compiled"),
        None => ("registry", "do not apply"),
    };
    let mut other = config["tachygram"]["settings"].clone();
    other["state_dir"] = dir.path().join(section).display().to_string().into();
    config[section] = serde_json::json!({"enabled": true, "settings": other});
    let (ok, stderr) = check(&write_config(dir.path(), &config));
    assert!(!ok && stderr.contains(refusal), "{stderr}");
    config.as_object_mut().unwrap().remove(section);
    config["unknown"] = 1.into();
    let (ok, stderr) = check(&write_config(dir.path(), &config));
    assert!(!ok && stderr.contains("unknown field"), "{stderr}");
    let output = Command::new(BIN)
        .args(["--config", "/nonexistent/query-server.json", "--check"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
}

/// Scenario 2 for the binary: bootstrap over TLS, serve exact answers, and
/// drain on SIGTERM with exit status 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_binary_serves_and_stops_on_sigterm() {
    let s0 = fixture(Spec::new(PRODUCT, 0, 1, 300));
    let dir = tempdir();
    let publisher = Publisher::start(PRODUCT, &dir.path().join("publisher")).await;
    publisher.publish(&[Some(s0.clone())], 1);
    let edge = Edge::start(publisher.addr).await;
    let host_dir = dir.path().join("host");
    std::fs::create_dir_all(&host_dir).unwrap();
    let (management, public) = (free_port(), free_port());
    let config = serde_json::json!({
        "management_listen": format!("127.0.0.1:{management}"),
        "public_listen": format!("127.0.0.1:{public}"),
        "tachygram": {"enabled": true, "settings": settings(&edge, &host_dir)},
    });
    let path = write_config(&host_dir, &config);
    let mut child = tokio::process::Command::new(BIN)
        .arg("--config")
        .arg(&path)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let started = Instant::now();
    loop {
        let ready = reqwest::get(format!("http://127.0.0.1:{management}/ready")).await;
        if ready.is_ok_and(|r| r.status() == 200) {
            break;
        }
        assert!(child.try_wait().unwrap().is_none(), "the binary exited");
        assert!(started.elapsed() < Duration::from_secs(20), "not ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let client = Client::new(&format!("http://127.0.0.1:{public}"), PRODUCT, None).unwrap();
    let map = client.fetch_map().await.unwrap();
    for entry in s0.entries.iter().step_by(60) {
        match client
            .lookup(&map, ScopeSelector::Index(0), entry.key())
            .await
            .unwrap()
        {
            LookupOutcome::Found { entries, .. } => assert_eq!(entries, vec![entry.clone()]),
            other => panic!("hit answered {other:?}"),
        }
    }
    match client
        .lookup(&map, ScopeSelector::Index(0), &missing_key(PRODUCT, 0))
        .await
        .unwrap()
    {
        LookupOutcome::Absent { .. } => {}
        other => panic!("miss answered {other:?}"),
    }
    let pid = child.id().unwrap() as i32;
    // SAFETY: signalling a child process this test spawned and still owns.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("the binary stopped after SIGTERM")
        .unwrap();
    assert!(status.success(), "{status}");
    for port in [management, public] {
        assert!(
            std::net::TcpStream::connect(("127.0.0.1", port)).is_err(),
            "port {port} is still bound"
        );
    }
}
