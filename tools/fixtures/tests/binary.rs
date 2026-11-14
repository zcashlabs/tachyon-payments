//! The `tachyon-node-double` binary as a separate process: it prints its URL,
//! serves the spec's chain (the same chain an in-process build of the spec
//! gives), authenticates with a cookie, and obeys control edits.
mod common;

use common::{client, control};
use pir_node_rpc::{Credentials, RpcClient};
use serde_json::json;
use std::process::Stdio;
use std::time::Duration;
use tachyon_fixtures::{ChainSpec, SyntheticChain};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

fn spec() -> serde_json::Value {
    json!({
        "seed": 77,
        "activation_height": 4,
        "random_blocks": 20,
        "mix": {"tachyon_txs": [1, 3], "actions": [1, 2], "proof_percent": 50,
                "tachygrams": [3, 9], "memo_percent": 100, "memo_len": [4, 16]},
        "explicit_blocks": [
            {"txs": [{"actions": 3, "memo_hex": "deadbeef",
                      "stamp": {"type": "proof", "tachygrams": ["01".repeat(32), "02".repeat(32), "03".repeat(32)]}}]}
        ],
    })
}

/// Spawns the binary and returns it with the URL it printed.
async fn spawn(args: &[&str]) -> (Child, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tachyon-node-double"))
        .args(args)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        BufReader::new(stdout).read_line(&mut line),
    )
    .await
    .expect("the binary prints its address")
    .unwrap();
    let url = line.trim().to_owned();
    assert!(url.starts_with("http://127.0.0.1:"), "{url:?}");
    (child, url)
}

async fn hashes(rpc: &RpcClient) -> Vec<String> {
    let tip = rpc.tip_height().await.unwrap();
    let mut out = Vec::new();
    for height in 0..=tip {
        out.push(rpc.block_hash_display(height).await.unwrap());
    }
    out
}

fn local_hashes(chain: &SyntheticChain) -> Vec<String> {
    chain
        .blocks()
        .iter()
        .map(|block| block.hash_display())
        .collect()
}

#[tokio::test]
async fn binary_serves_the_spec_and_obeys_control_reorgs() {
    let dir = tempfile::tempdir().unwrap();
    let spec_path = dir.path().join("chain.json");
    std::fs::write(&spec_path, spec().to_string()).unwrap();
    let cookie = dir.path().join(".cookie");
    let (mut child, url) = spawn(&[
        "--spec",
        spec_path.to_str().unwrap(),
        "--listen",
        "127.0.0.1:0",
        "--auth",
        "__cookie__:pw",
        "--cookie",
        cookie.to_str().unwrap(),
    ])
    .await;
    let rpc = client(&url, Credentials::CookieFile(cookie));

    // The in-process model of the same spec is the oracle for the other process.
    let spec: ChainSpec = serde_json::from_value(spec()).unwrap();
    let mut local = spec.build().unwrap();
    assert_eq!(rpc.tip_height().await.unwrap(), 21);
    assert_eq!(hashes(&rpc).await, local_hashes(&local));
    let info: serde_json::Value = rpc.call("getblockchaininfo", json!([])).await.unwrap();
    assert_eq!(info["upgrades"]["fffffffc"]["activationheight"], 4);
    let tip: serde_json::Value = rpc
        .call("getblock", json!([local.tip().hash_display(), 2]))
        .await
        .unwrap();
    assert_eq!(tip["tx"][1]["tachyon"]["memo"], "deadbeef");
    assert_eq!(
        tip["tx"][1]["tachyon"]["stamp"]["tachygrams"][2],
        "03".repeat(32)
    );

    // Control endpoints need no RPC credentials.
    let before = hashes(&rpc).await;
    let state = control(
        &url,
        "reorg",
        json!({"fork_height": 12, "random_blocks": 12}),
    )
    .await;
    local.reorg_random(12, 12).unwrap();
    assert_eq!(state["tip"], 24);
    let after = hashes(&rpc).await;
    assert_eq!(after, local_hashes(&local));
    assert_eq!(after[..=12], before[..=12]);
    assert!((13..=21).all(|height| after[height] != before[height]));

    let state = control(&url, "append", json!({"random_blocks": 3})).await;
    local.append_random(3);
    assert_eq!(state["tip"], 27);
    assert_eq!(state["tip_hash"], local.tip().hash_display());
    assert_eq!(hashes(&rpc).await, local_hashes(&local));

    child.kill().await.unwrap();
}

#[tokio::test]
async fn binary_rejects_a_bad_spec() {
    let dir = tempfile::tempdir().unwrap();
    let spec_path = dir.path().join("chain.json");
    std::fs::write(&spec_path, r#"{"seed": 1, "unknown_field": true}"#).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_tachyon-node-double"))
        .args(["--spec", spec_path.to_str().unwrap()])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown_field"));
}
