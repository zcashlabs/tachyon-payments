//! The served JSON has every field, type and fixed hex length of the responses
//! saved from the alpha testnet (`tests/data/`), for every object kind: block,
//! coinbase, Tachyon transaction with a proof stamp and with a pointer stamp,
//! actions, inputs, outputs, value pools and `getblockchaininfo`.
mod common;

use common::{conformance, sample};
use pir_node_rpc::Credentials;
use serde_json::{Value, json};
use tachyon_fixtures::{BlockSpec, ChainConfig, NodeDouble, NodeOptions, SyntheticChain, TxSpec};

/// POSTs one JSON-RPC 1.0 request, as `pir-node-rpc` frames it, and returns the
/// whole response envelope.
async fn raw_call(url: &str, method: &str, params: Value) -> Value {
    reqwest::Client::new()
        .post(url)
        .json(&json!({"jsonrpc": "1.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn chain() -> SyntheticChain {
    let mut chain = SyntheticChain::generate(ChainConfig::with_seed(11), 12);
    chain.append(
        BlockSpec::new()
            .tx(TxSpec::tachyon(3).random_proof(7).memo(vec![0, 1, 2, 255]))
            .tx(TxSpec::tachyon(1).pointer())
            .tx(TxSpec::tachyon(2).pointer().value_balance(174_146_070)),
    );
    chain.append_random(3);
    chain
}

#[tokio::test]
async fn getblock_verbosity_2_matches_the_testnet_shape() {
    let chain = chain();
    let middle = chain.block(13).unwrap().hash_display();
    let node = NodeDouble::start(chain, NodeOptions::default())
        .await
        .unwrap();
    let ours = raw_call(&node.url(), "getblock", json!([middle, 2])).await;
    let sample = sample("getblock-39000-v2.json");

    // Every kind of transaction in the sample is present in ours, so each is
    // compared.
    let kinds = |block: &Value| {
        let mut kinds: Vec<String> = block["result"]["tx"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tx| {
                tx.pointer("/tachyon/stamp/type")
                    .and_then(Value::as_str)
                    .unwrap_or("coinbase")
                    .to_owned()
            })
            .collect();
        kinds.sort();
        kinds.dedup();
        kinds
    };
    assert_eq!(kinds(&ours), kinds(&sample));
    let errors = conformance(&ours, &sample);
    assert!(errors.is_empty(), "shape differences: {errors:#?}");

    // The comparison is not vacuous: a dropped field is caught.
    let mut broken = ours.clone();
    broken["result"]["tx"][1]["tachyon"]
        .as_object_mut()
        .unwrap()
        .remove("memo");
    assert!(!conformance(&broken, &sample).is_empty());
}

#[tokio::test]
async fn getblockchaininfo_matches_the_testnet_shape() {
    let node = NodeDouble::start(chain(), NodeOptions::default())
        .await
        .unwrap();
    let ours = raw_call(&node.url(), "getblockchaininfo", json!([])).await;
    let errors = conformance(&ours, &sample("getblockchaininfo.json"));
    assert!(errors.is_empty(), "shape differences: {errors:#?}");
    let upgrade = &ours["result"]["upgrades"]["fffffffc"];
    assert_eq!(upgrade["name"], "NuTachyon");
    assert_eq!(upgrade["activationheight"], 8);
    assert_eq!(upgrade["status"], "active");
    assert_eq!(ours["result"]["chain"], "test");
    assert_eq!(ours["result"]["blocks"], 16);
}

#[tokio::test]
async fn nu_tachyon_follows_the_configured_activation() {
    let config = ChainConfig {
        activation_height: Some(30),
        ..ChainConfig::with_seed(2)
    };
    let node = NodeDouble::start(SyntheticChain::generate(config, 5), NodeOptions::default())
        .await
        .unwrap();
    let rpc = common::client(&node.url(), Credentials::None);
    let info: Value = rpc.call("getblockchaininfo", json!([])).await.unwrap();
    let upgrades = info["upgrades"].as_object().unwrap();
    assert_eq!(upgrades.len(), 12);
    assert_eq!(upgrades["fffffffc"]["activationheight"], 30);
    assert_eq!(upgrades["fffffffc"]["status"], "pending");
    assert_eq!(upgrades["77190ad9"]["status"], "pending");
    assert_eq!(upgrades["c8e71055"]["status"], "active");
    // No Tachyon transaction is generated before activation.
    assert!(
        node.chain()
            .blocks()
            .iter()
            .all(|block| block.txs().len() == 1)
    );

    let config = ChainConfig {
        activation_height: None,
        ..ChainConfig::with_seed(2)
    };
    let node = NodeDouble::start(SyntheticChain::generate(config, 5), NodeOptions::default())
        .await
        .unwrap();
    let rpc = common::client(&node.url(), Credentials::None);
    let info: Value = rpc.call("getblockchaininfo", json!([])).await.unwrap();
    let upgrades = info["upgrades"].as_object().unwrap();
    assert!(
        upgrades
            .values()
            .all(|upgrade| upgrade["name"] != "NuTachyon")
    );
    assert_eq!(upgrades.len(), 11);
}
