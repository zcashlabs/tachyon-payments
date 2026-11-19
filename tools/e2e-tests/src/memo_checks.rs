//! Checks of the memo and first-contact end-to-end tests: maps, scope
//! listings and lookup answers, judged against [`EnvelopeOracle`].
//!
//! The expected values all come from [`crate::memo_oracle`]; this module only
//! converts them into the wallet-facing types the probe client returns
//! (`tachyon-pir-protocol`'s entries and map scopes) and compares.
use crate::memo_oracle::{EnvelopeOracle, EnvelopeTruth, ScopeTruth};
use crate::oracle::State;
use crate::topology::{chain_head, describe};
use futures_util::StreamExt as _;
use futures_util::stream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tachyon_fixtures::{FirstContactTruth, FollowUpTruth};
use tachyon_pir_cli::{Client, ClientError, Coverage, LookupOutcome, MapView, ScopeSelector};
use tachyon_pir_protocol::{
    BlockHash, ChainMapHead, Entry, FirstContactEntry, Geometry, MemoEntry, Product, Scope,
    ScopeState,
};

/// An oracle entry as the wallet-facing table entry it must be served as.
pub trait Served: EnvelopeTruth {
    const PRODUCT: Product;
    fn entry(&self) -> Entry;
}

impl Served for FollowUpTruth {
    const PRODUCT: Product = Product::Memo;
    fn entry(&self) -> Entry {
        Entry::Memo(MemoEntry {
            key: self.key16(),
            ciphertext: self.ciphertext,
        })
    }
}

impl Served for FirstContactTruth {
    const PRODUCT: Product = Product::FirstContact;
    fn entry(&self) -> Entry {
        Entry::FirstContact(Box::new(FirstContactEntry {
            key: self.key16(),
            kem_ciphertext: self.kem_ciphertext,
            ciphertext: self.ciphertext,
        }))
    }
}

/// One lookup: a key in a scope (by scope index).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Probe {
    pub index: u64,
    pub key16: [u8; 16],
}

/// Lookups judged: hits, misses, and entries returned by the hits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tally {
    pub found: usize,
    pub absent: usize,
    pub entries: usize,
}

impl std::ops::AddAssign for Tally {
    fn add_assign(&mut self, other: Self) {
        self.found += other.found;
        self.absent += other.absent;
        self.entries += other.entries;
    }
}

pub fn oracle_state(state: ScopeState) -> State {
    match state {
        ScopeState::Open => State::Open,
        ScopeState::Sealed => State::Sealed,
    }
}

/// The listed chain scope at `index`.
pub fn chain_scope(map: &MapView, index: u64) -> &Scope {
    match map.scope(ScopeSelector::Index(index)) {
        Some((_, tachyon_pir_cli::ViewScope::Chain(scope))) => scope,
        other => panic!("scope {index} is not a listed chain scope: {other:?}"),
    }
}

/// The session ids of a scope's tables, in listing order.
pub fn sessions(map: &MapView, index: u64) -> Vec<[u8; 32]> {
    chain_scope(map, index)
        .tables
        .iter()
        .map(|t| t.session_id.0)
        .collect()
}

/// The map's anchor height.
pub fn anchor_of(map: &MapView) -> u64 {
    u64::from(chain_head(map).anchor_height)
}

/// A map head's identity fields against the oracle, anchored at `anchor`.
pub fn check_head<T: Served>(head: &ChainMapHead, oracle: &EnvelopeOracle<T>, anchor: u64) {
    let schedule = oracle.schedule();
    assert_eq!(head.protocol, T::PRODUCT.protocol());
    assert_eq!(head.network, "test");
    assert_eq!(
        head.genesis_hash,
        BlockHash::from_protocol_bytes(oracle.block_hash(0))
    );
    assert_eq!(
        u64::from(head.activation_height),
        schedule.activation_height
    );
    assert_eq!(u64::from(head.epoch_length), schedule.epoch_length);
    assert_eq!(u64::from(head.anchor_height), anchor, "map anchor");
    assert_eq!(
        head.anchor_hash,
        BlockHash::from_protocol_bytes(oracle.block_hash(anchor)),
        "map anchor hash"
    );
    assert_eq!(head.floor_scope, 0);
}

/// The whole listing (every scope from index 0, as the map lists them)
/// against the oracle's scopes at `anchor`: for first-contact this is the
/// span structure (scope ids are first epochs, heights, entries, state).
pub fn check_listing<T: Served>(scopes: &[&Scope], oracle: &EnvelopeOracle<T>, anchor: u64) {
    let expected = oracle.scopes(anchor);
    let listed: Vec<(u64, u32, u32, u64, ScopeState)> = scopes
        .iter()
        .map(|s| {
            (
                s.scope_id,
                s.first_height,
                s.last_height,
                s.entries,
                s.state,
            )
        })
        .collect();
    let wanted: Vec<(u64, u32, u32, u64, ScopeState)> = expected
        .iter()
        .map(|s| {
            (
                s.scope_id,
                s.first_height as u32,
                s.last_height as u32,
                s.entries,
                match s.state {
                    State::Open => ScopeState::Open,
                    State::Sealed => ScopeState::Sealed,
                },
            )
        })
        .collect();
    assert_eq!(
        listed,
        wanted,
        "{} scopes at anchor {anchor} (scope_id, first, last, entries, state)",
        T::PRODUCT.name()
    );
    for (scope, truth) in scopes.iter().zip(&expected) {
        check_scope(T::PRODUCT, scope, truth);
    }
}

/// One scope's tables against the oracle (spec 6.3, 10.3).
pub fn check_scope(product: Product, scope: &Scope, expected: &ScopeTruth) {
    let index = expected.index;
    assert_eq!(scope.scope_id, expected.scope_id, "scope {index} id");
    assert_eq!(
        oracle_state(scope.state),
        expected.state,
        "scope {index} state"
    );
    assert_eq!(
        u64::from(scope.first_height),
        expected.first_height,
        "scope {index} first height"
    );
    assert_eq!(
        u64::from(scope.last_height),
        expected.last_height,
        "scope {index} last height"
    );
    assert_eq!(scope.entries, expected.entries, "scope {index} entries");
    assert_eq!(scope.n_buckets, expected.n_buckets, "scope {index} buckets");
    if expected.entries == 0 {
        assert!(scope.tables.is_empty(), "empty scope {index} has tables");
        return;
    }
    for bucket in 0..scope.n_buckets {
        assert!(
            scope
                .tables
                .iter()
                .any(|t| t.bucket == bucket && t.segment == 0),
            "scope {index} bucket {bucket} has no segment 0"
        );
    }
    let total: u64 = scope.tables.iter().map(|t| t.entries).sum();
    assert_eq!(total, expected.entries, "scope {index} table entries");
    assert!(
        scope
            .tables
            .iter()
            .all(|t| t.geometry == Geometry::Tachyon4k8),
        "scope {index} geometry"
    );
    for table in &scope.tables {
        assert_eq!(
            table.session_id,
            table.expected_session_id(product, scope.scope_id),
            "scope {index} table {}/{} session id",
            table.bucket,
            table.segment
        );
    }
}

/// The map the query host served, head and whole listing, against the
/// oracle at `anchor`.
pub fn check_map<T: Served>(map: &MapView, oracle: &EnvelopeOracle<T>, anchor: u64) {
    check_head(chain_head(map), oracle, anchor);
    let scopes: Vec<&Scope> = map
        .scopes()
        .iter()
        .map(|scope| match scope {
            tachyon_pir_cli::ViewScope::Chain(scope) => scope,
            other => panic!("a chain product lists chain scopes: {other:?}"),
        })
        .collect();
    check_listing(&scopes, oracle, anchor);
}

/// Every key of every scope with entries (hits), and misses: `fresh` keys in
/// every scope with entries, and up to three keys of each other scope.
pub fn probes<T: Served>(
    oracle: &EnvelopeOracle<T>,
    anchor: u64,
    fresh: &[[u8; 16]],
) -> Vec<Probe> {
    let scopes: Vec<u64> = oracle
        .scopes(anchor)
        .into_iter()
        .filter(|s| s.entries > 0)
        .map(|s| s.index)
        .collect();
    let mut out = Vec::new();
    for &index in &scopes {
        for key16 in oracle.keys(index, anchor) {
            out.push(Probe { index, key16 });
        }
        for key16 in fresh {
            out.push(Probe {
                index,
                key16: *key16,
            });
        }
        for &other in scopes.iter().filter(|s| **s != index) {
            for key16 in oracle.keys(other, anchor).into_iter().take(3) {
                out.push(Probe { index, key16 });
            }
        }
    }
    out
}

/// Probes for every key of `index` at `anchor`.
pub fn scope_probes<T: Served>(oracle: &EnvelopeOracle<T>, index: u64, anchor: u64) -> Vec<Probe> {
    oracle
        .keys(index, anchor)
        .into_iter()
        .map(|key16| Probe { index, key16 })
        .collect()
}

/// Probes for `keys` in scope `index`.
pub fn key_probes(index: u64, keys: impl IntoIterator<Item = [u8; 16]>) -> Vec<Probe> {
    keys.into_iter()
        .map(|key16| Probe { index, key16 })
        .collect()
}

/// Looks every probe up through `wallet` under `map` (`parallel` in flight)
/// and judges each answer against the oracle at the map's anchor.
pub async fn check_lookups<T: Served>(
    wallet: &Arc<Client>,
    map: &Arc<MapView>,
    oracle: &EnvelopeOracle<T>,
    probes: &[Probe],
    parallel: usize,
) -> Tally {
    let anchor = anchor_of(map);
    let outcomes = lookups(wallet, map, probes, parallel).await;
    let mut tally = Tally::default();
    for (probe, outcome) in outcomes {
        let outcome = outcome.unwrap_or_else(|error| {
            panic!(
                "lookup of {} in scope {} failed: {}",
                hex::encode(probe.key16),
                probe.index,
                describe(&error)
            )
        });
        tally += judge(map, oracle, anchor, probe, &outcome);
    }
    tally
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
                    .lookup(&map, ScopeSelector::Index(probe.index), &probe.key16)
                    .await;
                (probe, outcome)
            }
        })
        .buffered(parallel)
        .collect()
        .await
}

/// One answer against the oracle: the scope's id and coverage, the map it was
/// answered under, and the entries: every entry with the key at a covered
/// height, in ordinal order, or absence.
pub fn judge<T: Served>(
    map: &MapView,
    oracle: &EnvelopeOracle<T>,
    anchor: u64,
    probe: Probe,
    outcome: &LookupOutcome,
) -> Tally {
    let expected = oracle.expected(probe.index, &probe.key16, anchor);
    let what = format!(
        "{} key {} in scope {} at anchor {anchor}",
        T::PRODUCT.name(),
        hex::encode(probe.key16),
        probe.index
    );
    let answer = outcome
        .answer()
        .unwrap_or_else(|| panic!("{what}: {outcome:?}"));
    let scope = oracle
        .scope(probe.index, anchor)
        .unwrap_or_else(|| panic!("{what}: the oracle lists no such scope"));
    assert_eq!(answer.scope_index, probe.index, "{what}: scope index");
    assert_eq!(answer.scope_id, scope.scope_id, "{what}: scope id");
    let Coverage::Heights {
        first_height,
        last_height,
        ..
    } = answer.coverage
    else {
        panic!("{what}: a chain answer covers heights")
    };
    assert_eq!(
        (u64::from(first_height), u64::from(last_height)),
        (scope.first_height, scope.last_height),
        "{what}: coverage"
    );
    assert_eq!(answer.map_sha256, map.map_sha256(), "{what}: answered map");
    match outcome {
        LookupOutcome::Found { entries, .. } if !expected.is_empty() => {
            let wanted: Vec<Entry> = expected.iter().map(Served::entry).collect();
            assert_eq!(
                entries,
                &wanted,
                "{what}: wrong entries (expected {} in ordinal order {:?})",
                wanted.len(),
                expected.iter().map(|e| e.ordinal()).collect::<Vec<_>>()
            );
            Tally {
                found: 1,
                absent: 0,
                entries: entries.len(),
            }
        }
        LookupOutcome::Absent { .. } if expected.is_empty() => Tally {
            found: 0,
            absent: 1,
            entries: 0,
        },
        outcome => panic!(
            "{what}: answered {outcome:?}, the oracle expects {} entries at ordinals {:?}",
            expected.len(),
            expected.iter().map(|e| e.ordinal()).collect::<Vec<_>>()
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

/// Looks probes up in a loop until `stop`, each time under a freshly fetched
/// map, and judges every answer against the oracle: no wrong answer may be
/// served at any point. The query host may refuse (`503`, or a `409` after
/// the client's one retry) and, when `down_allowed`, be unreachable; every
/// other failure fails the test.
pub fn spawn_prober<T: Served>(
    wallet: Arc<Client>,
    oracle: Arc<EnvelopeOracle<T>>,
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
                Ok(map) => wallet
                    .lookup(&map, ScopeSelector::Index(probe.index), &probe.key16)
                    .await
                    .map(|outcome| (map, outcome)),
                Err(error) => Err(error),
            };
            match result {
                Ok((map, outcome)) => {
                    judge(&map, &oracle, anchor_of(&map), probe, &outcome);
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

/// Ground keys for scope `scope_id` of `product` (scenario 6): for every
/// seed `s` below `MAX_SEEDS`, `SLOTS + 1` distinct nonzero `key16`s that
/// share one row under `s`, so no seed places the scope without overflow
/// (spec 6.4). Grinding uses the protocol crate's placement, as an attacker
/// would; expected answers still come from the authored chain.
pub fn grind_overflow(product: Product, genesis: &BlockHash, scope_id: u64) -> Vec<Vec<[u8; 16]>> {
    use sha2::Digest as _;
    let rows = Geometry::Tachyon4k8.rows();
    let per_row = product.slots() + 1;
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..tachyon_pir_protocol::MAX_SEEDS)
            .map(|seed| {
                scope.spawn(move || {
                    let base: [u8; 32] = sha2::Sha256::new()
                        .chain_update(b"tachyon-e2e/grind-memo\0")
                        .chain_update(product.name())
                        .chain_update(seed.to_le_bytes())
                        .finalize()
                        .into();
                    let candidate = |counter: u64| {
                        let mut key = [0u8; 16];
                        key[..8].copy_from_slice(&base[..8]);
                        key[8..].copy_from_slice(&counter.to_le_bytes());
                        key[0] |= 1; // never an all-zero key16 (spec 6.2)
                        key
                    };
                    let row_of = |key: &[u8; 16]| {
                        tachyon_pir_protocol::row(product, genesis, scope_id, seed, rows, key)
                            .expect("a key16")
                    };
                    let target = row_of(&candidate(0));
                    let mut group = vec![candidate(0)];
                    let mut counter = 1u64;
                    while group.len() < per_row {
                        let key = candidate(counter);
                        if row_of(&key) == target {
                            group.push(key);
                        }
                        counter += 1;
                    }
                    group
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|w| w.join().expect("grinder"))
            .collect()
    })
}

/// The row of `key16` under seed 0 in scope `scope_id`: where segment 0 puts
/// it when no seed fits (spec 6.4).
pub fn row_under_seed0(
    product: Product,
    genesis: &BlockHash,
    scope_id: u64,
    key16: &[u8; 16],
) -> u32 {
    tachyon_pir_protocol::row(
        product,
        genesis,
        scope_id,
        0,
        Geometry::Tachyon4k8.rows(),
        key16,
    )
    .expect("a key16")
}
