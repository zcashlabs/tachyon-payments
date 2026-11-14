//! Reorgs on a running double: hashes above the fork change, those at and below
//! it do not, and blocks reorged away are answered per the stale-block policy.
mod common;

use common::{block_v2, client};
use pir_node_rpc::{Credentials, RpcError};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tachyon_fixtures::{
    BlockSpec, ChainConfig, NodeDouble, NodeOptions, StaleBlocks, SyntheticChain, TxSpec,
};

async fn served_hashes(rpc: &pir_node_rpc::RpcClient) -> Vec<String> {
    let tip = rpc.tip_height().await.unwrap();
    let mut hashes = Vec::new();
    for height in 0..=tip {
        hashes.push(rpc.block_hash_display(height).await.unwrap());
    }
    hashes
}

#[tokio::test]
async fn reorg_replaces_blocks_above_the_fork_while_serving() {
    let node = NodeDouble::start(
        SyntheticChain::generate(ChainConfig::with_seed(31), 30),
        NodeOptions::default(),
    )
    .await
    .unwrap();
    let rpc = client(&node.url(), Credentials::None);
    let before = served_hashes(&rpc).await;
    let old_tip_txids: Vec<String> = node
        .chain()
        .tip()
        .txs()
        .iter()
        .map(|tx| tx.txid_display())
        .collect();

    // Readers keep calling while the chain is edited under them.
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let (rpc, stop) = (rpc.clone(), stop.clone());
        tokio::spawn(async move {
            let mut reads = 0u32;
            while !stop.load(Ordering::SeqCst) {
                rpc.tip_height().await.unwrap();
                reads += 1;
            }
            reads
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    node.chain().reorg_random(20, 15).unwrap();
    stop.store(true, Ordering::SeqCst);
    assert!(reader.await.unwrap() > 0);

    let after = served_hashes(&rpc).await;
    assert_eq!(after.len(), 36);
    assert_eq!(after[..=20], before[..=20]);
    for height in 21..=30 {
        assert_ne!(after[height], before[height], "height {height}");
    }
    // The first replaced block still links to the fork point.
    let first = block_v2(&rpc, &after[21]).await;
    assert_eq!(first["previousblockhash"], before[20]);
    let fork = block_v2(&rpc, &after[20]).await;
    assert_eq!(fork["nextblockhash"], after[21]);

    // The old branch is not served by default.
    assert!(matches!(
        rpc.call::<Value>("getblock", json!([before[25], 2])).await,
        Err(RpcError::Rpc(-5, _))
    ));
    // Its content is kept in the model.
    let hash = tachyon_fixtures::parse_display(&before[30]).unwrap();
    let stale = node
        .chain()
        .lookup(&hash)
        .map(|found| (found.active, found.block.txs().len()));
    assert_eq!(stale.map(|(active, _)| active), Some(false));
    assert_eq!(stale.unwrap().1, old_tip_txids.len());
}

#[tokio::test]
async fn identical_content_after_a_reorg_still_gets_new_hashes() {
    let mut chain = SyntheticChain::generate(ChainConfig::with_seed(32), 10);
    chain.append(BlockSpec::new().tx(TxSpec::tachyon(2).random_proof(3)));
    let replaced = chain.tip().clone();
    chain
        .reorg(10, vec![BlockSpec::from_block(&replaced)])
        .unwrap();
    let again = chain.tip();
    assert_eq!(again.height(), replaced.height());
    assert_eq!(again.prev(), replaced.prev());
    assert_ne!(
        again.hash(),
        replaced.hash(),
        "the fork salt separates the branches"
    );
    // The re-mined Tachyon transaction keeps its txid.
    assert_eq!(again.txs()[1].txid(), replaced.txs()[1].txid());
    assert_eq!(chain.generation(), 1);
}

#[tokio::test]
async fn stale_blocks_can_be_served_as_zcashd_does() {
    let options = NodeOptions {
        stale_blocks: StaleBlocks::Serve,
        ..NodeOptions::default()
    };
    let node = NodeDouble::start(
        SyntheticChain::generate(ChainConfig::with_seed(33), 12),
        options,
    )
    .await
    .unwrap();
    let rpc = client(&node.url(), Credentials::None);
    let old = node.chain().block(10).unwrap().hash_display();
    node.chain().reorg_random(8, 6).unwrap();
    let stale = block_v2(&rpc, &old).await;
    assert_eq!(stale["hash"], old);
    assert_eq!(stale["height"], 10);
    assert_eq!(stale["confirmations"], -1);
    assert!(stale.get("nextblockhash").is_none());
    for tx in stale["tx"].as_array().unwrap() {
        assert_eq!(tx["in_active_chain"], false);
        assert_eq!(tx["confirmations"], 0);
    }
    // getblockhash answers only the active chain.
    assert_ne!(rpc.block_hash_display(10).await.unwrap(), old);
}

#[tokio::test]
async fn a_reorg_above_the_tip_is_refused_and_changes_nothing() {
    let mut chain = SyntheticChain::generate(ChainConfig::with_seed(34), 5);
    let tip = chain.tip_hash();
    assert!(chain.reorg(6, Vec::new()).is_err());
    assert_eq!(chain.tip_hash(), tip);
    assert_eq!(chain.generation(), 0);
    // A reorg may also shorten the chain.
    chain.reorg(2, Vec::new()).unwrap();
    assert_eq!(chain.tip_height(), 2);
}
