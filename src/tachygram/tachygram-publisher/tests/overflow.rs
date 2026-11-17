//! Scenario 6, forced overflow: tachygrams ground against the public
//! placement so that under every seed some row of epoch 0's table holds more
//! than `SLOTS` entries. The scope falls back to seed 0 with overflow
//! segments, and every authored entry stays retrievable.
mod common;

use common::*;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use tachyon_pir_protocol::{BlockHash, MAX_SEEDS, ScopeState, ScopeTables, row};

const ROWS: u32 = 4096;

/// For each seed, `slots + 1` distinct nonzero keys whose row under that
/// seed is 0, ground in parallel.
fn grind(genesis: &BlockHash, scope_id: u64) -> Vec<[u8; 32]> {
    let per_seed = PRODUCT.slots() + 1;
    let seeds: Vec<u32> = (0..MAX_SEEDS).collect();
    let mut keys: Vec<[u8; 32]> = std::thread::scope(|scope| {
        let workers: Vec<_> = seeds
            .chunks(4)
            .map(|chunk| {
                scope.spawn(move || {
                    let mut found = Vec::new();
                    for &seed in chunk {
                        let mut counter = 0u64;
                        let mut hits = 0;
                        while hits < per_seed {
                            counter += 1;
                            let key: [u8; 32] = Sha256::new()
                                .chain_update(b"overflow-test")
                                .chain_update(seed.to_le_bytes())
                                .chain_update(counter.to_le_bytes())
                                .finalize()
                                .into();
                            if row(PRODUCT, genesis, scope_id, seed, ROWS, &key).unwrap() == 0 {
                                found.push(key);
                                hits += 1;
                            }
                        }
                    }
                    found
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|w| w.join().unwrap())
            .collect()
    });
    let distinct: HashSet<_> = keys.iter().collect();
    assert_eq!(distinct.len(), keys.len());
    // Interleave so later seeds' keys are not all late in ordinal order.
    keys.sort();
    keys
}

#[tokio::test(flavor = "multi_thread")]
async fn ground_tachygrams_force_overflow_segments_and_every_entry_stays_retrievable() {
    let mut chain = new_chain(8);
    let genesis = BlockHash::from_protocol_bytes(chain.genesis_hash());
    let keys = grind(&genesis, 0);
    assert_eq!(keys.len(), 64 * 205);
    // Epoch 0 is heights 8..=23: seven blocks of about 1,900 tachygrams.
    let mut plan = Plan::default();
    for (i, stamp) in keys.chunks(keys.len().div_ceil(7)).enumerate() {
        plan.0
            .insert(9 + i as u64, Content::Explicit(vec![stamp.to_vec()]));
    }
    grow(&mut chain, &plan, 30);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut tachygram = open(&node, dir.path()).await;
    let report = step(&mut tachygram, &node, 1).await.unwrap().unwrap();
    assert_eq!(report.anchor.height, 28);
    let published = Published::read(tachygram.distribution());
    let scope = published.scope(0);
    assert_eq!(scope.state, ScopeState::Open);
    assert_eq!(scope.entries, keys.len() as u64);
    assert_eq!(scope.n_buckets, 1);
    // No seed fits: seed 0 with at least one overflow segment.
    assert_eq!(scope.tables[0].seed, 0);
    assert!(
        scope.tables.len() >= 2,
        "overflow segments: {}",
        scope.tables.len()
    );
    assert!(
        scope.tables[1..]
            .iter()
            .all(|t| t.segment > 0 && t.entries > 0)
    );
    check_against_chain(&published, tachygram.distribution(), &node.chain());
    // Every entry is found exactly once through the query plan over every
    // segment of its bucket.
    let rows = published.rows(tachygram.distribution(), 0);
    let expected = oracle(&node.chain(), 0, 28);
    for entry in &expected {
        let mut found: Vec<&tachyon_pir_protocol::TachygramEntry> = Vec::new();
        for target in scope
            .query_plan(PRODUCT, &genesis, &entry.tachygram)
            .unwrap()
        {
            found.extend(
                rows[&(target.table.bucket, target.table.segment, target.row)]
                    .iter()
                    .filter(|e| e.tachygram == entry.tachygram),
            );
        }
        assert_eq!(found, [entry]);
    }
    let overflow = scope.tables.iter().filter(|t| t.segment > 0).count();
    assert!(tachygram.metrics().render().contains(&format!(
        "tachyon_tachygram_segments_overflow_total {overflow}\n"
    )));
}
