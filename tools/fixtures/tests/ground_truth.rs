//! The JSON served through `pir_node_rpc::RpcClient` round-trips the authored
//! ground truth: hashes and txids in display order, tachygrams unreversed, memos
//! as hex, stamps of both kinds. Parsed with `serde_json` only.
mod common;

use common::{block_v2, client};
use pir_node_rpc::{Credentials, RpcError};
use serde_json::{Value, json};
use tachyon_fixtures::{
    BasicAuth, Block, BlockSpec, ChainConfig, NodeDouble, NodeOptions, Stamp, SyntheticChain,
    TxSpec, display,
};

fn decode32(value: &Value) -> [u8; 32] {
    hex::decode(value.as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap()
}

/// Asserts that `served` is exactly `block` as authored.
fn assert_block(served: &Value, block: &Block, tip: u64) {
    assert_eq!(served["hash"], block.hash_display());
    assert_eq!(served["height"], block.height());
    assert_eq!(served["confirmations"], tip - block.height() + 1);
    if block.height() == 0 {
        assert!(served.get("previousblockhash").is_none());
    } else {
        assert_eq!(served["previousblockhash"], display(&block.prev()));
    }
    assert_eq!(served.get("nextblockhash").is_none(), block.height() == tip);
    let txs = served["tx"].as_array().unwrap();
    assert_eq!(txs.len(), block.txs().len());
    assert_eq!(served["nTx"], block.txs().len());
    for (served, tx) in txs.iter().zip(block.txs()) {
        assert_eq!(served["txid"], tx.txid_display());
        assert_eq!(served["blockhash"], block.hash_display());
        assert_eq!(served["height"], block.height());
        assert_eq!(served["in_active_chain"], true);
        assert_eq!(served.get("tachyon").is_some(), tx.tachyon().is_some());
    }
    // The Tachyon transactions, compared with the ground-truth view.
    for truth in block.tachyon_txs() {
        let served = &txs[truth.index];
        let mut txid = decode32(&served["txid"]);
        txid.reverse();
        assert_eq!(txid, truth.txid, "txids are printed reversed");
        let tachyon = &served["tachyon"];
        assert_eq!(
            tachyon["actions"].as_array().unwrap().len(),
            truth.own_actions as usize
        );
        assert_eq!(
            hex::decode(tachyon["memo"].as_str().unwrap()).unwrap(),
            truth.memo
        );
        let stamp = &tachyon["stamp"];
        assert_eq!(stamp["feeZat"], truth.stamp.fee_zat());
        match truth.stamp {
            Stamp::Proof(proof) => {
                assert_eq!(stamp["type"], "proof");
                assert_eq!(decode32(&stamp["anchor"]), proof.anchor);
                assert_eq!(decode32(&stamp["tachygramSet"]), proof.tachygram_set);
                assert_eq!(decode32(&stamp["coverage"]), proof.coverage);
                assert_eq!(
                    hex::decode(stamp["proof"].as_str().unwrap()).unwrap(),
                    proof.proof
                );
                let tachygrams: Vec<[u8; 32]> = stamp["tachygrams"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(decode32)
                    .collect();
                assert_eq!(tachygrams, proof.tachygrams, "tachygrams are not reversed");
                assert!(stamp.get("aggregateId").is_none());
            }
            Stamp::Pointer(pointer) => {
                assert_eq!(stamp["type"], "pointer");
                assert_eq!(
                    hex::decode(stamp["aggregateId"].as_str().unwrap()).unwrap(),
                    pointer.aggregate_id
                );
                assert!(stamp.get("tachygrams").is_none());
            }
        }
    }
}

fn authored_chain() -> (SyntheticChain, [u8; 32]) {
    let mut chain = SyntheticChain::generate(ChainConfig::with_seed(21), 20);
    let ascending = chain.fresh_tachygrams(5);
    let duplicate = ascending[2];
    chain.append(
        BlockSpec::new()
            // Arbitrary memo bytes, odd tachygram count, explicit tachygrams.
            .tx(TxSpec::tachyon(4)
                .memo(b"\x00TPM1\xffpayload".to_vec())
                .proof(ascending))
            // No own actions, a pointer stamp.
            .tx(TxSpec::tachyon(0).pointer())
            .tx(TxSpec::tachyon(2).random_proof(1)),
    );
    // A duplicate across blocks, explicitly asked for, served as authored.
    chain.append(BlockSpec::new().tx(TxSpec::tachyon(1).proof(vec![duplicate])));
    chain.append_random(10);
    (chain, duplicate)
}

#[tokio::test]
async fn served_blocks_round_trip_the_authored_chain() {
    let (chain, duplicate) = authored_chain();
    let expected = chain.clone();
    let node = NodeDouble::start(chain, NodeOptions::default())
        .await
        .unwrap();
    let rpc = client(&node.url(), Credentials::None);

    let tip = rpc.tip_height().await.unwrap();
    assert_eq!(tip, expected.tip_height());
    let mut proofs = 0;
    for height in 0..=tip {
        let hash = rpc.block_hash_display(height).await.unwrap();
        let block = expected.block(height).unwrap();
        assert_eq!(hash, block.hash_display());
        let served = block_v2(&rpc, &hash).await;
        assert_block(&served, block, tip);
        proofs += block.tachyon_txs().filter(|tx| tx.stamp.is_proof()).count();
    }
    assert!(proofs >= 3);
    let tachygrams = expected.active_tachygrams();
    assert_eq!(
        tachygrams.iter().filter(|t| **t == duplicate).count(),
        2,
        "the explicit duplicate is kept"
    );

    // Blocks were fetched by hash, never by height.
    assert_eq!(node.count("getblock"), (tip + 1) as usize);
    assert_eq!(node.count("getblockhash"), (tip + 1) as usize);
    for call in node.calls().iter().filter(|call| call.method == "getblock") {
        let selector = call.params[0].as_str().unwrap();
        assert_eq!(selector.len(), 64);
        assert_eq!(call.params[1], 2);
    }
    assert_eq!(node.counts()["getblockcount"], 1);
}

#[tokio::test]
async fn rpc_client_speaks_to_the_double_with_cookie_auth_and_batches() {
    let auth = BasicAuth {
        user: "__cookie__".into(),
        password: "s3cret".into(),
    };
    let dir = tempfile::tempdir().unwrap();
    let cookie = dir.path().join(".cookie");
    auth.write_cookie(&cookie).unwrap();
    let (chain, _) = authored_chain();
    let hashes: Vec<String> = chain.blocks().iter().map(Block::hash_display).collect();
    let options = NodeOptions {
        auth: Some(auth),
        ..NodeOptions::default()
    };
    let node = NodeDouble::start(chain, options).await.unwrap();

    // Reread cookie, read-once credentials, and a refusal.
    let rpc = client(&node.url(), Credentials::CookieFile(cookie.clone()));
    assert_eq!(rpc.tip_height().await.unwrap(), hashes.len() as u64 - 1);
    let fixed = client(&node.url(), Credentials::read_once(&cookie).unwrap());
    assert_eq!(fixed.block_hash_display(3).await.unwrap(), hashes[3]);
    let wrong = client(
        &node.url(),
        Credentials::Fixed {
            username: "__cookie__".into(),
            password: "wrong".into(),
        },
    );
    assert!(matches!(
        wrong.tip_height().await,
        Err(RpcError::InvalidCookie)
    ));
    let anonymous = client(&node.url(), Credentials::None);
    assert!(matches!(
        anonymous.tip_height().await,
        Err(RpcError::Http(error)) if error.status().map(|s| s.as_u16()) == Some(401)
    ));

    // A JSON-RPC 2.0 batch of block hashes, with a per-entry error.
    let mut params: Vec<Value> = (0..hashes.len() as u64).map(|h| json!([h])).collect();
    params.push(json!([hashes.len() as u64 + 5]));
    let answers = rpc.batch::<String>("getblockhash", params).await.unwrap();
    assert_eq!(answers.len(), hashes.len() + 1);
    for (answer, hash) in answers.iter().zip(&hashes) {
        assert_eq!(answer.as_ref().unwrap(), hash);
    }
    assert!(matches!(answers.last().unwrap(), Err((-8, _))));

    // The node's own errors, as the client reports them.
    assert!(matches!(
        rpc.call::<Value>("gettachyoninfo", json!([])).await,
        Err(RpcError::Rpc(-32601, _))
    ));
    assert!(matches!(
        rpc.block_hash_display(hashes.len() as u64).await,
        Err(RpcError::Rpc(-8, message)) if message == "Block height out of range"
    ));
    assert!(matches!(
        rpc.call::<Value>("getblock", json!(["ab".repeat(32), 2])).await,
        Err(RpcError::Rpc(-5, message)) if message == "Block not found"
    ));
    assert!(matches!(
        rpc.call::<Value>("getblock", json!([hashes.len() as u64, 2]))
            .await,
        Err(RpcError::Rpc(-8, _))
    ));
    // A height is also accepted, as a number or a string, at verbosity 1.
    let by_height: Value = rpc.call("getblock", json!(["4", 1])).await.unwrap();
    assert_eq!(by_height["hash"], hashes[4]);
    assert!(by_height["tx"][0].is_string());
}
