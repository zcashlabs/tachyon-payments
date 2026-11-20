//! Checks of the registry end-to-end tests: map heads, chunk listings and
//! lookup answers judged against [`RegistryOracle`], a background prober,
//! and the scenario 6 address grinder.
//!
//! The expected values all come from [`crate::registry_oracle`]; this module
//! only compares them with the wallet-facing types the probe client returns
//! (`tachyon-pir-protocol`'s registry entries and scopes).
use crate::oracle::State;
use crate::registry_oracle::{EK_T_BYTES, ExpectedChunk, OracleAddress, RegistryOracle, Stream};
use crate::registry_topology::{Identity, registry_head};
use crate::topology::describe;
use futures_util::StreamExt as _;
use futures_util::stream;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tachyon_pir_cli::{Client, ClientError, Coverage, LookupOutcome, MapView, ScopeSelector};
use tachyon_pir_protocol::{
    Entry, Geometry, Product, RegistryMapHead, RegistryScope, ScopeState, ScopeTables as _,
};

/// Lookups in flight at once (the query host's admission allows more).
pub const PARALLEL: usize = 4;

pub fn oracle_state(state: ScopeState) -> State {
    match state {
        ScopeState::Open => State::Open,
        ScopeState::Sealed => State::Sealed,
    }
}

/// The listed registry chunk at `index`.
pub fn registry_scope(map: &MapView, index: u64) -> &RegistryScope {
    match map.scope(ScopeSelector::Index(index)) {
        Some((_, tachyon_pir_cli::ViewScope::Registry(scope))) => scope,
        other => panic!("chunk {index} is not a listed registry scope: {other:?}"),
    }
}

/// The session ids of a chunk's tables, in listing order.
pub fn sessions(map: &MapView, index: u64) -> Vec<[u8; 32]> {
    registry_scope(map, index)
        .tables
        .iter()
        .map(|t| t.session_id.0)
        .collect()
}

/// The map's `log_sequence`.
pub fn log_sequence_of(map: &MapView) -> u64 {
    registry_head(map).log_sequence
}

/// A registry head's identity fields (spec 10.3): protocol, profile,
/// network and genesis of the identity the publisher read at start-up,
/// `log_sequence`, `floor_scope` 0.
pub fn check_head(head: &RegistryMapHead, identity: &Identity, log_sequence: u64) {
    assert_eq!(head.protocol, "tachyon-registry-pir-v1");
    assert_eq!(
        head.profile,
        "reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1"
    );
    assert_eq!(head.network, identity.network, "map network");
    assert_eq!(head.genesis_hash, identity.block_hash(), "map genesis");
    assert_eq!(head.log_sequence, log_sequence, "map log_sequence");
    assert_eq!(head.floor_scope, 0);
}

/// The whole chunk listing against the oracle at `log_sequence`: sealed
/// chunks `0 .. log_sequence / 3072` of exactly 3,072 entries, one open
/// chunk holding the rest, `scope_id = chunk_id`, one bucket, and each
/// chunk's tables (spec 9.2, 10.3).
pub fn check_listing(scopes: &[&RegistryScope], oracle: &RegistryOracle, log_sequence: u64) {
    let expected = oracle.chunks(log_sequence);
    let listed: Vec<(u64, State, u64)> = scopes
        .iter()
        .map(|s| (s.scope_id, oracle_state(s.state), s.entries))
        .collect();
    let wanted: Vec<(u64, State, u64)> = expected
        .iter()
        .map(|c| (c.index, c.state, c.entries))
        .collect();
    assert_eq!(
        listed, wanted,
        "registry chunks at log_sequence {log_sequence} (chunk_id, state, entries)"
    );
    for (scope, chunk) in scopes.iter().zip(&expected) {
        check_chunk(scope, chunk);
    }
}

/// One chunk's tables: none when empty (spec 10.3), otherwise bucket 0's
/// segments `0 .. k` in order at `tachyon-4k8`, their entries summing to the
/// chunk's.
pub fn check_chunk(scope: &RegistryScope, chunk: &ExpectedChunk) {
    let index = chunk.index;
    assert_eq!(
        scope.n_buckets, 1,
        "chunk {index}: n_buckets = 1 (spec 9.2)"
    );
    if chunk.entries == 0 {
        assert!(scope.tables.is_empty(), "empty chunk {index} lists tables");
        return;
    }
    for (k, table) in scope.tables.iter().enumerate() {
        assert_eq!(table.bucket, 0, "chunk {index} table {k}: bucket");
        assert_eq!(table.segment, k as u32, "chunk {index} table {k}: segment");
        assert_eq!(table.geometry, Geometry::Tachyon4k8);
        assert!(table.seed < tachyon_pir_protocol::MAX_SEEDS);
    }
    let total: u64 = scope.tables.iter().map(|t| t.entries).sum();
    assert_eq!(total, chunk.entries, "chunk {index}: table entries");
}

/// A served map against the oracle at `log_sequence`: its head and its
/// whole listing.
pub fn check_map(map: &MapView, oracle: &RegistryOracle, identity: &Identity, log_sequence: u64) {
    check_head(registry_head(map), identity, log_sequence);
    let scopes: Vec<&RegistryScope> = (0..map.scopes().len() as u64)
        .map(|i| registry_scope(map, i))
        .collect();
    check_listing(&scopes, oracle, log_sequence);
}

/// One lookup: a key in a chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Probe {
    pub chunk: u64,
    pub key16: [u8; 16],
}

/// Lookups judged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tally {
    pub found: usize,
    pub absent: usize,
}

impl std::ops::AddAssign for Tally {
    fn add_assign(&mut self, other: Self) {
        self.found += other.found;
        self.absent += other.absent;
    }
}

/// Every entry of `chunk` visible at `log_sequence`, looked up in its chunk.
pub fn hits(oracle: &RegistryOracle, chunk: u64, log_sequence: u64) -> Vec<Probe> {
    oracle
        .in_chunk(chunk, log_sequence)
        .into_iter()
        .map(|r| Probe {
            chunk,
            key16: r.key16,
        })
        .collect()
}

/// Misses for a map at `log_sequence`: every `fresh` (never registered)
/// address in every chunk, and up to `per_chunk` registered addresses of
/// other chunks (or not yet visible) looked up in each chunk.
pub fn misses(
    oracle: &RegistryOracle,
    fresh: &[OracleAddress],
    log_sequence: u64,
    per_chunk: usize,
) -> Vec<Probe> {
    let chunks = oracle.chunks(log_sequence);
    let mut probes = Vec::new();
    for chunk in &chunks {
        probes.extend(fresh.iter().map(|a| Probe {
            chunk: chunk.index,
            key16: a.key16(),
        }));
        probes.extend(
            oracle
                .entries()
                .iter()
                .filter(|r| r.chunk != chunk.index || r.sequence >= log_sequence)
                .take(per_chunk)
                .map(|r| Probe {
                    chunk: chunk.index,
                    key16: r.key16,
                }),
        );
    }
    for probe in &probes {
        assert!(
            oracle
                .expected(probe.chunk, &probe.key16, log_sequence)
                .is_none()
        );
    }
    probes
}

/// `k` of `probes`, chosen deterministically by `seed`.
pub fn sample(probes: &[Probe], k: usize, seed: u64) -> Vec<Probe> {
    let mut stream = Stream::new("sample", seed);
    let mut pool = probes.to_vec();
    let mut out = Vec::new();
    while out.len() < k && !pool.is_empty() {
        let mut b = [0u8; 8];
        stream.fill(&mut b);
        let i = (u64::from_le_bytes(b) % pool.len() as u64) as usize;
        out.push(pool.swap_remove(i));
    }
    out
}

/// The probes looked up through `wallet` under `map`, `parallel` at once,
/// in probe order.
pub async fn lookups(
    wallet: &Arc<Client>,
    map: &Arc<MapView>,
    probes: &[Probe],
    parallel: usize,
) -> Vec<(Probe, Result<LookupOutcome, ClientError>)> {
    stream::iter(probes.iter().copied())
        .map(|probe| {
            let wallet = wallet.clone();
            let map = map.clone();
            async move {
                let outcome = wallet
                    .lookup(&map, ScopeSelector::Index(probe.chunk), &probe.key16)
                    .await;
                (probe, outcome)
            }
        })
        .buffered(parallel)
        .collect()
        .await
}

/// Looks every probe up under `map` and judges each answer against the
/// oracle at the map's `log_sequence`.
pub async fn check_lookups(
    wallet: &Arc<Client>,
    map: &Arc<MapView>,
    oracle: &RegistryOracle,
    probes: &[Probe],
) -> Tally {
    check_lookups_parallel(wallet, map, oracle, probes, PARALLEL).await
}

/// [`check_lookups`] with `parallel` lookups in flight.
pub async fn check_lookups_parallel(
    wallet: &Arc<Client>,
    map: &Arc<MapView>,
    oracle: &RegistryOracle,
    probes: &[Probe],
    parallel: usize,
) -> Tally {
    let mut tally = Tally::default();
    for (probe, outcome) in lookups(wallet, map, probes, parallel).await {
        let outcome = outcome.unwrap_or_else(|error| {
            panic!(
                "lookup of {} in chunk {} failed: {}",
                hex::encode(probe.key16),
                probe.chunk,
                describe(&error)
            )
        });
        tally += judge(map, oracle, probe, &outcome);
    }
    tally
}

/// One answer against the oracle at the map's `log_sequence`: the chunk's
/// coverage (state, first sequence, entries), the map it was answered
/// under, every segment of the bucket queried, and the entry: exactly the
/// registered address (`key16`, `pk`, `ek` bytes) or absence.
pub fn judge(
    map: &MapView,
    oracle: &RegistryOracle,
    probe: Probe,
    outcome: &LookupOutcome,
) -> Tally {
    let log_sequence = log_sequence_of(map);
    let what = format!(
        "key {} in chunk {} at log_sequence {log_sequence}",
        hex::encode(probe.key16),
        probe.chunk
    );
    let answer = outcome
        .answer()
        .unwrap_or_else(|| panic!("{what}: {outcome:?}"));
    let chunk = oracle
        .chunks(log_sequence)
        .into_iter()
        .find(|c| c.index == probe.chunk)
        .unwrap_or_else(|| panic!("{what}: the oracle lists no such chunk"));
    assert_eq!(answer.scope_index, probe.chunk, "{what}: scope index");
    assert_eq!(answer.scope_id, probe.chunk, "{what}: scope_id = chunk_id");
    let Coverage::Sequences {
        state,
        first_sequence,
        entries,
    } = answer.coverage
    else {
        panic!("{what}: a registry answer covers sequences")
    };
    assert_eq!(
        (oracle_state(state), first_sequence, entries),
        (chunk.state, chunk.first_sequence, chunk.entries),
        "{what}: coverage"
    );
    assert_eq!(answer.map_sha256, map.map_sha256(), "{what}: answered map");
    let segments = registry_scope(map, probe.chunk).tables().len();
    assert_eq!(
        answer.queries.len(),
        segments,
        "{what}: every segment of the bucket is queried (spec 6.4)"
    );
    let expected = oracle.expected(probe.chunk, &probe.key16, log_sequence);
    match (outcome, expected) {
        (LookupOutcome::Found { entries, .. }, Some(registered)) => {
            let [Entry::Registry(entry)] = entries.as_slice() else {
                panic!("{what}: expected exactly one registry entry, got {entries:?}");
            };
            assert_eq!(entry.key, registered.key16, "{what}: key16");
            assert_eq!(entry.address.pk, registered.address.pk, "{what}: pk");
            assert!(
                entry.address.ek[..] == registered.address.ek[..],
                "{what}: ek bytes differ"
            );
            Tally {
                found: 1,
                absent: 0,
            }
        }
        (LookupOutcome::Absent { .. }, None) => Tally {
            found: 0,
            absent: 1,
        },
        (outcome, expected) => panic!(
            "{what}: answered {outcome:?}, the oracle expects {:?}",
            expected.map(|r| (r.sequence, r.chunk))
        ),
    }
}

/// Everything a background prober saw.
#[derive(Debug, Default)]
pub struct Probed {
    pub answers: usize,
    pub unavailable: usize,
    pub transport: usize,
    pub stale: usize,
}

/// Looks probes up in a loop until `stop`, each under a freshly fetched map,
/// and judges every answer against the oracle at that map's `log_sequence`:
/// no wrong answer may be served at any point. The oracle is shared, so a
/// test may record registrations while the prober runs; it must record an
/// acknowledgement before any map can contain it (it does: the map can only
/// follow the `200`). The query host may refuse (`503`, or a `409` after the
/// client's one retry) and, when `down_allowed`, be unreachable.
pub fn spawn_prober(
    wallet: Arc<Client>,
    oracle: Arc<std::sync::Mutex<RegistryOracle>>,
    probes: Vec<Probe>,
    down_allowed: bool,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<Probed> {
    tokio::spawn(async move {
        let mut seen = Probed::default();
        let mut next = 0;
        while !stop.load(Ordering::Relaxed) {
            let probe = probes[next % probes.len()];
            next += 1;
            let result = match wallet.fetch_map().await {
                Ok(map) if map.scope(ScopeSelector::Index(probe.chunk)).is_none() => {
                    continue;
                }
                Ok(map) => wallet
                    .lookup(&map, ScopeSelector::Index(probe.chunk), &probe.key16)
                    .await
                    .map(|outcome| (map, outcome)),
                Err(error) => Err(error),
            };
            match result {
                Ok((map, outcome)) => {
                    let oracle = oracle.lock().expect("oracle").clone();
                    judge(&map, &oracle, probe, &outcome);
                    seen.answers += 1;
                }
                Err(ClientError::Unavailable { .. }) => seen.unavailable += 1,
                Err(ClientError::StaleRouting { .. }) => seen.stale += 1,
                Err(ClientError::Transport { .. }) if down_allowed => seen.transport += 1,
                Err(error) => panic!("the prober got {}", describe(&error)),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        seen
    })
}

/// Ground addresses for chunk `chunk_id` (scenario 6): for every seed `s`
/// below `MAX_SEEDS`, `SLOTS + 1` (7) valid addresses whose `key16` falls in
/// one row under `s`, so no seed places the chunk without overflow (spec
/// 6.4). Each starts from a generated valid address and varies only `ρ`
/// (the last 32 bytes of `ek`, which no check constrains), so every one
/// stays valid. Grinding uses the protocol crate's placement, as an
/// attacker would; the answers expected for them still come from the
/// oracle's record of the registrations.
pub fn grind_overflow(genesis: &[u8; 32], chunk_id: u64, seed: u64) -> Vec<OracleAddress> {
    let genesis = tachyon_pir_protocol::BlockHash::from_protocol_bytes(*genesis);
    let rows = Geometry::Tachyon4k8.rows();
    let per_row = Product::Registry.slots() + 1;
    let ground: Vec<OracleAddress> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..tachyon_pir_protocol::MAX_SEEDS)
            .map(|placement_seed| {
                let genesis = &genesis;
                scope.spawn(move || {
                    let base = crate::registry_oracle::AddressGen::new(
                        seed * 1000 + u64::from(placement_seed),
                    )
                    .address();
                    // Everything but ρ is fixed: hash that prefix once.
                    let prefix = Sha256::new()
                        .chain_update(crate::registry_oracle::ADDR_DOMAIN)
                        .chain_update(base.pk)
                        .chain_update(&base.ek[..EK_T_BYTES]);
                    let row_of = |key16: &[u8]| {
                        tachyon_pir_protocol::row(
                            Product::Registry,
                            genesis,
                            chunk_id,
                            placement_seed,
                            rows,
                            key16,
                        )
                        .expect("a key16")
                    };
                    let mut target = None;
                    let mut group = Vec::new();
                    let mut counter = 0u64;
                    while group.len() < per_row {
                        counter += 1;
                        let mut rho = [0u8; 32];
                        rho[..8].copy_from_slice(&counter.to_le_bytes());
                        rho[8..12].copy_from_slice(&placement_seed.to_le_bytes());
                        rho[12..20].copy_from_slice(&seed.to_le_bytes());
                        let digest: [u8; 32] = prefix.clone().chain_update(rho).finalize().into();
                        let row = row_of(&digest[..16]);
                        if *target.get_or_insert(row) == row {
                            let mut ek = base.ek.clone();
                            ek[EK_T_BYTES..].copy_from_slice(&rho);
                            let address = OracleAddress { pk: base.pk, ek };
                            assert_eq!(address.digest(), digest);
                            assert!(address.valid());
                            group.push(address);
                        }
                    }
                    group
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|w| w.join().expect("grinder"))
            .collect()
    });
    let mut keys: Vec<[u8; 16]> = ground.iter().map(OracleAddress::key16).collect();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(keys.len(), ground.len(), "ground keys are distinct");
    ground
}
