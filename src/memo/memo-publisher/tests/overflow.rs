//! Scenario 6, forced overflow: memo tags whose `key16`s are ground against
//! the public placement so that under every seed some row of epoch 0's table
//! holds more than `SLOTS` entries. The scope falls back to seed 0 with
//! overflow segments, and every authored entry stays retrievable through the
//! query plan.
mod common;

use common::*;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use tachyon_fixtures::{AuthoredEntry, EntryKind, envelope::tag_with_key16};
use tachyon_pir_protocol::{BlockHash, MAX_SEEDS, Product, ScopeState, ScopeTables, row};

const ROWS: u32 = 4096;

/// For each seed, `slots + 1` distinct nonzero `key16`s whose row under that
/// seed is 0, ground in parallel.
fn grind(genesis: &BlockHash, scope_id: u64) -> Vec<[u8; 16]> {
    let per_seed = Product::Memo.slots() + 1;
    let seeds: Vec<u32> = (0..MAX_SEEDS).collect();
    let mut keys: Vec<[u8; 16]> = std::thread::scope(|scope| {
        let workers: Vec<_> = seeds
            .chunks(2)
            .map(|chunk| {
                scope.spawn(move || {
                    let mut found = Vec::new();
                    for &seed in chunk {
                        let mut counter = 0u64;
                        let mut hits = 0;
                        while hits < per_seed {
                            counter += 1;
                            let digest: [u8; 32] = Sha256::new()
                                .chain_update(b"memo-overflow-test")
                                .chain_update(seed.to_le_bytes())
                                .chain_update(counter.to_le_bytes())
                                .finalize()
                                .into();
                            let key: [u8; 16] = digest[..16].try_into().unwrap();
                            if row(Product::Memo, genesis, scope_id, seed, ROWS, &key).unwrap() == 0
                            {
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
async fn ground_memo_tags_force_overflow_segments_and_every_entry_stays_retrievable() {
    let mut chain = new_chain(8);
    let genesis = BlockHash::from_protocol_bytes(chain.genesis_hash());
    let keys = grind(&genesis, 0);
    assert_eq!(keys.len(), 64 * 57);
    // Epoch 0 is heights 8-11: three blocks of about 1,216 follow-ups each.
    let mut plan = Plan::default();
    for (i, keys) in keys.chunks(keys.len().div_ceil(3)).enumerate() {
        let entries = keys
            .iter()
            .map(|key| {
                let tag = tag_with_key16(chain.rng(), *key);
                AuthoredEntry::random_with_tag(chain.rng(), EntryKind::FollowUp, tag)
            })
            .collect();
        plan.0.insert(9 + i as u64, vec![Item::Entries(entries)]);
    }
    grow(&mut chain, &plan, 30);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut publisher = open(&node, dir.path()).await;
    let reports = step(&mut publisher, &node, 1).await.unwrap();
    assert_eq!(reports.memo.unwrap().anchor.height, 28);
    let published = Published::read(&publisher, Product::Memo);
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
    assert_eq!(scope.segments(0).len(), scope.tables.len());
    // Every entry is found exactly once through every segment of its bucket
    // (check_against_chain looks up every authored key).
    check_both(&publisher, &node.chain());
    let overflow = scope.tables.iter().filter(|t| t.segment > 0).count();
    assert_eq!(
        metric(
            &publisher.metrics().render(),
            "tachyon_memo_segments_overflow_total"
        ),
        overflow.to_string()
    );
}
