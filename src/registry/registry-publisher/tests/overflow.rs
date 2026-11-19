//! Scenario 6, forced overflow: valid addresses ground (through `ρ`, which
//! the checks leave free) so that under every seed seven of them land in row
//! 0 of chunk 0, one more than a row's six slots. The chunk falls back to
//! seed 0 with overflow segments, and every address stays retrievable.
mod common;

use common::*;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use tachyon_pir_protocol::{Address, MAX_SEEDS, ScopeState, registry::ADDR_DOMAIN, row};

/// For each seed, `SLOTS + 1` valid addresses whose `key16` falls in row 0
/// of chunk 0 under that seed.
fn grind() -> Vec<Address> {
    let genesis = identity().genesis_hash;
    let per_seed = PRODUCT.slots() + 1;
    let seeds: Vec<u32> = (0..MAX_SEEDS).collect();
    let addresses: Vec<Address> = std::thread::scope(|scope| {
        let workers: Vec<_> = seeds
            .chunks(4)
            .map(|chunk| {
                scope.spawn(move || {
                    let mut found = Vec::new();
                    for &seed in chunk {
                        let base = address(10_000 + u64::from(seed));
                        // Everything but ρ is fixed: hash it once.
                        let prefix = Sha256::new()
                            .chain_update(ADDR_DOMAIN)
                            .chain_update(base.pk)
                            .chain_update(&base.ek[..1152]);
                        let mut counter = 0u64;
                        let mut hits = 0;
                        while hits < per_seed {
                            counter += 1;
                            let mut rho = [0u8; 32];
                            rho[..8].copy_from_slice(&counter.to_le_bytes());
                            rho[8..12].copy_from_slice(&seed.to_le_bytes());
                            let digest: [u8; 32] =
                                prefix.clone().chain_update(rho).finalize().into();
                            if row(PRODUCT, &genesis, 0, seed, 4096, &digest[..16]).unwrap() == 0 {
                                let mut ek = base.ek;
                                ek[1152..].copy_from_slice(&rho);
                                let ground = Address { pk: base.pk, ek };
                                assert_eq!(ground.digest(), digest);
                                found.push(ground);
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
    let keys: HashSet<_> = addresses.iter().map(Address::key16).collect();
    assert_eq!(keys.len(), addresses.len());
    addresses
}

#[tokio::test(flavor = "multi_thread")]
async fn ground_addresses_force_overflow_segments_and_all_stay_retrievable() {
    let ground = grind();
    assert_eq!(ground.len(), 64 * 7);
    // Interleave so the seeds' addresses are spread through the sequence.
    let mut ordered = ground.clone();
    ordered.sort_by_key(Address::key16);
    let dir = tempfile::tempdir().unwrap();
    seed_log(dir.path(), &ordered);
    let mut server = Server::start(dir.path(), false, |_| {}).await;
    let report = server.publish().await.unwrap();
    assert_eq!(report.log_sequence, ordered.len() as u64);
    let published = Published::read(server.registry.distribution());
    let scope = published.scope(0);
    assert_eq!(scope.state, ScopeState::Open);
    assert_eq!(scope.entries, ordered.len() as u64);
    assert_eq!(scope.n_buckets, 1);
    // No seed fits: seed 0 with overflow segments.
    assert_eq!(scope.tables[0].seed, 0);
    assert!(scope.tables.len() >= 2, "segments: {}", scope.tables.len());
    assert!(
        scope.tables[1..]
            .iter()
            .enumerate()
            .all(|(k, t)| t.segment as usize == k + 1 && t.entries > 0)
    );
    assert_eq!(report.tables_built, scope.tables.len());
    // Every address is found exactly once over every segment of its bucket.
    let rows = published.rows(server.registry.distribution(), 0);
    for address in &ordered {
        assert_eq!(published.find(&rows, 0, address), 1);
    }
    let overflow = scope.tables.len() - 1;
    assert!(server.registry.metrics().render().contains(&format!(
        "tachyon_registry_segments_overflow_total {overflow}\n"
    )));
    server.stop().await;
}
