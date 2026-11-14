//! Fault injection: delays, failed calls (HTTP and JSON-RPC), mismatched blocks
//! and outages, and the per-method request counts tests use as evidence.
mod common;

use common::{block_v2, client, control};
use pir_node_rpc::{Credentials, RpcError};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tachyon_fixtures::{
    BlockMismatch, ChainConfig, Failure, NodeDouble, NodeOptions, SyntheticChain, display,
};

async fn node() -> NodeDouble {
    NodeDouble::start(
        SyntheticChain::generate(ChainConfig::with_seed(41), 15),
        NodeOptions::default(),
    )
    .await
    .unwrap()
}

/// Whether a served block shows the injected inconsistency.
type Check<'a> = Box<dyn Fn(&Value) -> bool + 'a>;

fn http_status(result: Result<u64, RpcError>) -> Option<u16> {
    match result {
        Err(RpcError::Http(error)) => error.status().map(|status| status.as_u16()),
        _ => None,
    }
}

#[tokio::test]
async fn per_method_delay() {
    let node = node().await;
    let rpc = client(&node.url(), Credentials::None);
    node.set_delay(Some("getblockcount"), Duration::from_millis(400));
    let started = Instant::now();
    rpc.tip_height().await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(400));
    let started = Instant::now();
    rpc.block_hash_display(1).await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(400));
    node.clear_faults();
    let started = Instant::now();
    rpc.tip_height().await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(400));
}

#[tokio::test]
async fn fail_next_calls_with_http_500_or_an_rpc_error() {
    let node = node().await;
    let rpc = client(&node.url(), Credentials::None);
    node.fail_next(Some("getblockcount"), 2, Failure::Http(500));
    assert_eq!(http_status(rpc.tip_height().await), Some(500));
    // Other methods are unaffected.
    rpc.block_hash_display(2).await.unwrap();
    assert_eq!(http_status(rpc.tip_height().await), Some(500));
    assert_eq!(rpc.tip_height().await.unwrap(), 15);

    node.fail_next(
        None,
        1,
        Failure::Rpc {
            code: -28,
            message: "Loading block index...".into(),
        },
    );
    assert!(matches!(
        rpc.block_hash_display(1).await,
        Err(RpcError::Rpc(-28, message)) if message == "Loading block index..."
    ));
    rpc.block_hash_display(1).await.unwrap();

    // Failed calls are counted too.
    assert_eq!(node.count("getblockcount"), 3);
    assert_eq!(node.count("getblockhash"), 3);
}

#[tokio::test]
async fn mismatched_blocks_are_served_once_per_injection() {
    let node = node().await;
    let rpc = client(&node.url(), Credentials::None);
    let (block, next) = {
        let chain = node.chain();
        (
            chain.block(7).unwrap().clone(),
            chain.block(8).unwrap().clone(),
        )
    };
    let hash = block.hash_display();
    let clean = block_v2(&rpc, &hash).await;

    let cases: Vec<(BlockMismatch, Check)> = vec![
        (
            BlockMismatch::Hash,
            Box::new(|v: &Value| v["hash"] != json!(hash)),
        ),
        (
            BlockMismatch::Height,
            Box::new(|v: &Value| v["height"] == 8),
        ),
        (
            BlockMismatch::PreviousHash,
            Box::new(|v: &Value| v["previousblockhash"] != json!(display(&block.prev()))),
        ),
        (
            BlockMismatch::OtherBlock,
            Box::new(|v: &Value| v["hash"] == json!(next.hash_display()) && v["height"] == 8),
        ),
        (
            BlockMismatch::TxBlockHash,
            Box::new(|v: &Value| {
                v["tx"][0]["blockhash"] != json!(hash) && v["hash"] == json!(hash)
            }),
        ),
        (
            BlockMismatch::TxHeight,
            Box::new(|v: &Value| v["tx"][0]["height"] == 8),
        ),
        (
            BlockMismatch::Txid,
            Box::new(|v: &Value| {
                let last = block.txs().last().unwrap().txid_display();
                v["tx"].as_array().unwrap().last().unwrap()["txid"] != json!(last)
            }),
        ),
    ];
    for (kind, broken) in cases {
        node.mismatch_next(1, kind);
        let served = block_v2(&rpc, &hash).await;
        assert!(broken(&served), "{kind:?} not injected");
        assert_ne!(served, clean);
        assert_eq!(
            block_v2(&rpc, &hash).await,
            clean,
            "{kind:?} outlived its count"
        );
    }
}

#[tokio::test]
async fn outage_takes_the_server_down_and_keeps_the_chain() {
    let node = node().await;
    let addr = node.addr();
    let rpc = client(&node.url(), Credentials::None);
    let tip_hash = rpc.block_hash_display(15).await.unwrap();
    node.outage().await;
    assert!(!node.is_up());
    assert!(matches!(rpc.tip_height().await, Err(RpcError::Http(_))));
    let fresh = client(&node.url(), Credentials::None);
    assert!(matches!(
        fresh.tip_height().await,
        Err(RpcError::Http(error)) if error.is_connect()
    ));
    node.chain().append_random(2);
    node.restore().await.unwrap();
    assert!(node.is_up());
    assert_eq!(node.addr(), addr);
    assert_eq!(rpc.tip_height().await.unwrap(), 17);
    assert_eq!(rpc.block_hash_display(15).await.unwrap(), tip_hash);
    // Calls made during the outage never reached the server.
    assert_eq!(node.count("getblockcount"), 1);
}

#[tokio::test]
async fn control_endpoints_inject_faults_and_edit_the_chain() {
    let node = node().await;
    let url = node.url();
    let rpc = client(&url, Credentials::None);

    control(
        &url,
        "fail",
        json!({"method": "getblockhash", "count": 1, "http_status": 503}),
    )
    .await;
    assert!(matches!(
        rpc.block_hash_display(1).await,
        Err(RpcError::Http(error)) if error.status().map(|s| s.as_u16()) == Some(503)
    ));
    control(
        &url,
        "fail",
        json!({"count": 1, "rpc_code": -1, "rpc_message": "boom"}),
    )
    .await;
    assert!(matches!(rpc.tip_height().await, Err(RpcError::Rpc(-1, _))));

    control(&url, "delay", json!({"method": "getblockcount", "ms": 300})).await;
    let started = Instant::now();
    rpc.tip_height().await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(300));
    control(&url, "clear-faults", json!({})).await;

    let hash = rpc.block_hash_display(3).await.unwrap();
    control(&url, "mismatch", json!({"kind": "height", "count": 1})).await;
    assert_eq!(block_v2(&rpc, &hash).await["height"], 4);
    assert_eq!(block_v2(&rpc, &hash).await["height"], 3);

    let state = control(&url, "append", json!({"random_blocks": 2, "blocks": [{"txs": [{"actions": 2, "memo_hex": "0102", "stamp": {"type": "random_proof", "count": 3}}]}]})).await;
    assert_eq!(state["tip"], 18);
    {
        let chain = node.chain();
        let truth: Vec<_> = chain
            .block(16)
            .unwrap()
            .tachyon_txs()
            .map(|t| (t.own_actions, t.memo.to_vec(), t.stamp.tachygrams().len()))
            .collect();
        assert_eq!(truth, vec![(2, vec![1, 2], 3)]);
    }
    let state = control(
        &url,
        "reorg",
        json!({"fork_height": 10, "random_blocks": 3}),
    )
    .await;
    assert_eq!(state["tip"], 13);
    assert_eq!(state["generation"], 1);
    assert_eq!(state["tip_hash"], node.chain().tip().hash_display());

    let state: Value = reqwest::get(format!("{url}/control/state"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(state["counts"]["getblock"], 2);
    assert_eq!(state["up"], true);

    // Bad control requests are refused without effect.
    let refused = reqwest::Client::new()
        .post(format!("{url}/control/reorg"))
        .json(&json!({"fork_height": 99}))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status().as_u16(), 400);
    assert_eq!(node.chain().tip_height(), 13);

    // A timed outage, then automatic recovery.
    control(&url, "outage", json!({"duration_ms": 400, "delay_ms": 10})).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let fresh = client(&url, Credentials::None);
    assert!(fresh.tip_height().await.is_err());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(tip) = fresh.tip_height().await {
            assert_eq!(tip, 13);
            break;
        }
        assert!(Instant::now() < deadline, "the server did not come back");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn malformed_requests_get_json_rpc_errors() {
    let node = node().await;
    let http = reqwest::Client::new();
    let answer: Value = http
        .post(node.url())
        .body("{not json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(answer["error"]["code"], -32700);
    let answer: Value = http
        .post(node.url())
        .json(&json!({"jsonrpc": "1.0", "id": "x", "method": "getblock", "params": []}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(answer["error"]["code"], -32602);
    assert_eq!(answer["id"], "x");
    assert_eq!(answer["jsonrpc"], "1.0");
    let answer: Value = http
        .post(node.url())
        .json(&json!({"jsonrpc": "1.0", "id": 1, "method": "getblock", "params": [1, 7]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(answer["error"]["code"], -8);
}
