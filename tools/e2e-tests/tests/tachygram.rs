//! Tachygram end-to-end acceptance (spec 13.2 scenarios 1-4, 6 and 8-13; the
//! milestone M1 exit criterion of spec 14 without scenario 15, the live run).
//!
//! Every test runs the topology of spec 13.1: the in-process node double
//! serving an authored chain (16-block epochs), the publisher binary, the TLS
//! edge in front of it, the query-server binary trusting the edge, and the
//! probe client as the wallet on the query host's public listener. Expected
//! answers come only from [`TachygramOracle`], computed from the authored
//! chain.
use futures_util::StreamExt as _;
use futures_util::stream;
use reqwest::StatusCode;
use serde_json::json;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tachyon_fixtures::{BlockSpec, ChainConfig, SyntheticChain, TxMix, TxSpec};
use tachyon_pir_cli::{
    Client, ClientError, Coverage, LookupOutcome, MapView, Mount, ScopeSelector, ViewScope,
};
use tachyon_pir_e2e_tests::budget::Budget;
use tachyon_pir_e2e_tests::oracle::{ExpectedScope, Sampler, State, TachygramOracle};
use tachyon_pir_e2e_tests::topology::{
    ProductNames, PublisherHost, Topology, TopologyOptions, chain_head, describe, exclusive,
    wait_for, wait_limit, wait_map,
};
use tachyon_pir_e2e_tests::{Fault, Target, binary};
use tachyon_pir_protocol::{BlockHash, Entry, Geometry, Product, ScopeState, TachygramEntry};

/// Blocks per epoch on the synthetic network.
const EPOCH_LENGTH: u32 = 16;
/// Lookups in flight at once (the query host's admission allows more).
const PARALLEL: usize = 4;

fn publisher_binary() -> PathBuf {
    binary!("tachyon-pir-publisher-test")
}

fn query_binary() -> PathBuf {
    binary!("tachyon-pir-query-server-test")
}

fn options() -> TopologyOptions {
    TopologyOptions::new(ProductNames::TACHYGRAM, EPOCH_LENGTH)
}

// ---------------------------------------------------------------------------
// Authoring chains.

/// Random content without proof stamps: pointer stamps, actions and memos
/// only, so tachygrams appear only where a test puts them.
fn filler_config(seed: u64) -> ChainConfig {
    ChainConfig {
        mix: TxMix {
            tachyon_txs: [0, 2],
            actions: [1, 3],
            proof_percent: 0,
            tachygrams: [5, 9],
            memo_percent: 30,
            memo_len: [1, 40],
        },
        ..ChainConfig::with_seed(seed)
    }
}

/// A block whose Tachyon transactions alternate pointer stamps and one proof
/// stamp per entry of `stamps` (each that many fresh tachygrams), so stamp
/// ordinals count proof stamps only, in transaction order.
fn proof_block(chain: &mut SyntheticChain, stamps: &[u32]) -> BlockSpec {
    let mut block = BlockSpec::new().tx(TxSpec::tachyon(1).memo(b"lead".to_vec()));
    for &count in stamps {
        block = block
            .tx(TxSpec::tachyon(2).proof(chain.fresh_tachygrams(count)))
            .tx(TxSpec::tachyon(1).pointer());
    }
    block
}

/// Appends filler blocks and `proof_block`s (by height, from `plan`) up to
/// `tip`.
fn extend(chain: &mut SyntheticChain, tip: u64, plan: &[(u64, &[u32])]) {
    while chain.tip_height() < tip {
        let height = chain.tip_height() + 1;
        match plan.iter().find(|(h, _)| *h == height) {
            Some((_, stamps)) => {
                let block = proof_block(chain, stamps);
                chain.append(block);
            }
            None => chain.append_random(1),
        }
    }
}

/// The standard chain, tip 160 (anchor 158), activation 8, 16-block epochs.
///
/// - Epoch 0 (heights 8-23) and epoch 1 (24-39) hold tachygrams and are
///   sealed (their last block is at least 100 below the anchor). Their first
///   and last blocks carry stamps.
/// - Epochs 2-8 have pointer stamps only: listed, without tables.
/// - Epoch 9 (152-167) is the open newest scope, with a stamp in its first
///   block, at the anchor, and in the two blocks above the anchor (not yet
///   covered). Its block 155 also lists a tachygram of epoch 0 again, which a
///   later epoch may do (spec 8.5).
fn standard_chain(seed: u64) -> SyntheticChain {
    let mut chain = SyntheticChain::new(filler_config(seed));
    extend(
        &mut chain,
        151,
        &[
            (8, &[4]),
            (12, &[5, 9]),
            (23, &[6]),
            (24, &[8]),
            (31, &[5]),
            (39, &[4, 3, 6]),
        ],
    );
    extend(&mut chain, 154, &[(152, &[6])]);
    let reused = chain.blocks()[12].tachygrams()[0];
    let mut repeated = chain.fresh_tachygrams(3);
    repeated.push(reused);
    repeated.sort();
    let mut block = proof_block(&mut chain, &[5]);
    block = block.tx(TxSpec::tachyon(1).proof(repeated));
    chain.append(block);
    extend(&mut chain, 160, &[(158, &[7]), (159, &[5]), (160, &[6])]);
    chain
}

/// The standard chain's shape with proof stamps in the open newest epoch
/// only (heights 152, 155, the anchor 158, and 159-160 above it): one table,
/// for the scenario 10 tests, whose 10 s back-off leaves no time for more.
fn open_epoch_chain(seed: u64) -> SyntheticChain {
    let mut chain = SyntheticChain::new(filler_config(seed));
    extend(
        &mut chain,
        160,
        &[
            (152, &[6]),
            (155, &[5, 3]),
            (158, &[7]),
            (159, &[5]),
            (160, &[6]),
        ],
    );
    chain
}

// ---------------------------------------------------------------------------
// Checks against the oracle.

/// One lookup: a tachygram in a scope.
#[derive(Clone, Copy, Debug)]
struct Probe {
    epoch: u64,
    tachygram: [u8; 32],
}

/// Hits and misses checked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Tally {
    found: usize,
    absent: usize,
}

impl std::ops::AddAssign for Tally {
    fn add_assign(&mut self, other: Self) {
        self.found += other.found;
        self.absent += other.absent;
    }
}

fn oracle_state(state: ScopeState) -> State {
    match state {
        ScopeState::Open => State::Open,
        ScopeState::Sealed => State::Sealed,
    }
}

fn chain_scope(map: &MapView, index: u64) -> &tachyon_pir_protocol::Scope {
    match map.scope(ScopeSelector::Index(index)) {
        Some((_, ViewScope::Chain(scope))) => scope,
        other => panic!("scope {index} is not a listed chain scope: {other:?}"),
    }
}

/// The map's anchor height.
fn anchor_of(map: &MapView) -> u64 {
    u64::from(chain_head(map).anchor_height)
}

/// The map head and the whole scope listing against the oracle at the map's
/// anchor, which must be `anchor` (spec 10.3, 6.3, 6.5).
fn check_map(map: &MapView, oracle: &TachygramOracle, anchor: u64) {
    let head = chain_head(map);
    let schedule = oracle.schedule();
    assert_eq!(head.protocol, Product::Tachygram.protocol());
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
    let expected = oracle.expected_scopes(anchor);
    assert_eq!(map.scopes().len(), expected.len(), "scopes listed");
    for scope in &expected {
        check_scope(map, scope);
    }
}

fn check_scope(map: &MapView, expected: &ExpectedScope) {
    let scope = chain_scope(map, expected.index);
    let index = expected.index;
    assert_eq!(scope.scope_id, index, "scope {index} id");
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
    assert_eq!(
        scope.n_buckets,
        TachygramOracle::n_buckets(expected.entries),
        "scope {index} buckets"
    );
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
            .all(|t| t.geometry == Geometry::Tachyon4k8)
    );
}

/// The session ids of a scope's tables, in listing order.
fn sessions(map: &MapView, index: u64) -> Vec<[u8; 32]> {
    chain_scope(map, index)
        .tables
        .iter()
        .map(|t| t.session_id.0)
        .collect()
}

/// Every hit of every epoch with tables (records up to the anchor), and the
/// misses: fresh tachygrams in every listed scope with tables, tachygrams of
/// other epochs, and tachygrams above the anchor.
fn probes(oracle: &TachygramOracle, anchor: u64, fresh: &[[u8; 32]]) -> Vec<Probe> {
    let mut out = Vec::new();
    let epochs: Vec<u64> = oracle
        .epochs_with_records()
        .filter(|epoch| oracle.coverage(*epoch, anchor).is_some())
        .collect();
    for &epoch in &epochs {
        for record in oracle.records(epoch) {
            out.push(Probe {
                epoch,
                tachygram: record.tachygram,
            });
        }
        for tachygram in fresh {
            out.push(Probe {
                epoch,
                tachygram: *tachygram,
            });
        }
        for &other in epochs.iter().filter(|e| **e != epoch) {
            for record in oracle.records(other).iter().take(3) {
                out.push(Probe {
                    epoch,
                    tachygram: record.tachygram,
                });
            }
        }
    }
    out
}

/// Looks every probe up through `wallet` under `map` and compares the answer
/// with the oracle at the map's anchor: the exact record for a hit, absence
/// for a miss, the scope's coverage, and the map the answer was given under.
async fn check_lookups(
    wallet: &Arc<Client>,
    map: &Arc<MapView>,
    oracle: &TachygramOracle,
    probes: &[Probe],
) -> Tally {
    let anchor = anchor_of(map);
    let outcomes: Vec<(Probe, Result<LookupOutcome, ClientError>)> =
        stream::iter(probes.iter().copied())
            .map(|probe| {
                let wallet = wallet.clone();
                let map = map.clone();
                async move {
                    let outcome = wallet
                        .lookup(&map, ScopeSelector::Index(probe.epoch), &probe.tachygram)
                        .await;
                    (probe, outcome)
                }
            })
            .buffered(PARALLEL)
            .collect()
            .await;
    let mut tally = Tally::default();
    for (probe, outcome) in outcomes {
        let outcome = outcome.unwrap_or_else(|error| {
            panic!(
                "lookup of {} in epoch {} failed: {}",
                hex::encode(probe.tachygram),
                probe.epoch,
                describe(&error)
            )
        });
        tally += judge(map, oracle, anchor, probe, &outcome);
    }
    tally
}

/// One answer against the oracle.
fn judge(
    map: &MapView,
    oracle: &TachygramOracle,
    anchor: u64,
    probe: Probe,
    outcome: &LookupOutcome,
) -> Tally {
    let expected = oracle.expected(probe.epoch, &probe.tachygram, anchor);
    let what = format!(
        "tachygram {} in epoch {} at anchor {anchor}",
        hex::encode(probe.tachygram),
        probe.epoch
    );
    let answer = outcome
        .answer()
        .unwrap_or_else(|| panic!("{what}: {outcome:?}"));
    let (first, last) = oracle.coverage(probe.epoch, anchor).expect("listed scope");
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
        (first, last),
        "{what}: coverage"
    );
    assert_eq!(answer.map_sha256, map.map_sha256(), "{what}: answered map");
    match (outcome, expected) {
        (LookupOutcome::Found { entries, .. }, Some(record)) => {
            let expected_entry = Entry::Tachygram(TachygramEntry {
                tachygram: record.tachygram,
                blk_height: record.blk_height,
                stamp_ordinal: record.stamp_ordinal,
            });
            assert_eq!(entries, &vec![expected_entry], "{what}: wrong record");
            Tally {
                found: 1,
                absent: 0,
            }
        }
        (LookupOutcome::Absent { .. }, None) => Tally {
            found: 0,
            absent: 1,
        },
        (outcome, expected) => {
            panic!("{what}: answered {outcome:?}, the oracle expects {expected:?}")
        }
    }
}

/// `k` probes drawn from [`probes`] with a fixed seed.
fn sample_probes(
    oracle: &TachygramOracle,
    anchor: u64,
    fresh: &[[u8; 32]],
    k: usize,
    seed: u64,
) -> Vec<Probe> {
    Sampler::new(seed).sample(&probes(oracle, anchor, fresh), k)
}

/// Probes for records of `before` that `after` no longer lists in that epoch
/// (blocks reorged away): every one must now be absent.
fn vanished(before: &TachygramOracle, after: &TachygramOracle, epoch: u64) -> Vec<Probe> {
    before
        .records(epoch)
        .iter()
        .filter(|record| !after.epochs_of(&record.tachygram).contains(&epoch))
        .map(|record| Probe {
            epoch,
            tachygram: record.tachygram,
        })
        .collect()
}

/// Edits the node double's chain with `edit` and returns the oracle of the
/// result.
fn edit_chain(t: &Topology, edit: impl FnOnce(&mut SyntheticChain)) -> TachygramOracle {
    let mut chain = t.node.chain();
    edit(&mut chain);
    TachygramOracle::new(&chain, u64::from(EPOCH_LENGTH))
}

/// Waits until the query host serves a map anchored where `oracle` says a
/// caught-up publication is (height and hash), and checks it against the
/// oracle.
async fn wait_caught_up(
    wallet: &Client,
    oracle: &TachygramOracle,
    budget: &Budget,
) -> Arc<MapView> {
    let anchor = oracle.anchor();
    let hash = BlockHash::from_protocol_bytes(oracle.block_hash(anchor));
    let map = wait_map(
        wallet,
        &format!("the query host to serve anchor {anchor}"),
        wait_limit(budget),
        |map| {
            let head = chain_head(map);
            u64::from(head.anchor_height) == anchor && head.anchor_hash == hash
        },
    )
    .await;
    check_map(&map, oracle, anchor);
    map
}

/// `tachyon_tachygram_<name>` on the query host's management `/metrics`.
async fn query_metric(t: &Topology, series: &str) -> f64 {
    let name = format!("tachyon_tachygram_{series}");
    t.query
        .metric(&name)
        .await
        .unwrap_or_else(|| panic!("the query host exports no {name}"))
}

/// Everything the restart prober saw.
#[derive(Debug, Default)]
struct Probed {
    answers: usize,
    unavailable: usize,
    transport: usize,
    stale: usize,
}

/// Looks probes up in a loop until `stop`, each time under a freshly fetched
/// map, and judges every answer against the oracle: no wrong answer may be
/// served at any point. The query host may refuse (`503`, or a `409` after
/// the client's one retry) and, when `down_allowed`, be unreachable; every
/// other failure fails the test.
fn spawn_prober(
    wallet: Arc<Client>,
    oracle: Arc<TachygramOracle>,
    probes: Vec<Probe>,
    down_allowed: bool,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> tokio::task::JoinHandle<Probed> {
    tokio::spawn(async move {
        let mut seen = Probed::default();
        let mut next = 0;
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            let probe = probes[next % probes.len()];
            next += 1;
            let result = match wallet.fetch_map().await {
                Ok(map) => wallet
                    .lookup(&map, ScopeSelector::Index(probe.epoch), &probe.tachygram)
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

// ---------------------------------------------------------------------------
// Scenarios.

/// Scenario 1: with no query host at all, publications advance as the chain
/// grows (generation strictly increasing, anchor following the tip) and the
/// publisher stays healthy and ready.
#[tokio::test(flavor = "multi_thread")]
async fn publisher_without_consumers_advances() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = SyntheticChain::new(filler_config(101));
            extend(&mut chain, 30, &[(10, &[5]), (26, &[4])]);
            let node = tachyon_pir_e2e_tests::node::start_node(chain).await;
            let publisher = PublisherHost::start(
                publisher_binary(),
                ProductNames::TACHYGRAM,
                &node.url(),
                EPOCH_LENGTH,
                50,
                wait_limit(&budget),
            )
            .await;
            let mut generations = Vec::new();
            for round in 0..4u64 {
                let oracle = TachygramOracle::new(&node.chain(), u64::from(EPOCH_LENGTH));
                let anchor = oracle.anchor();
                let publication = publisher
                    .wait_anchor(anchor, oracle.block_hash(anchor), wait_limit(&budget))
                    .await;
                assert!(
                    publication.eligible,
                    "round {round}: published but not eligible"
                );
                let head = publisher.chain_map_head().await.expect("a map head");
                assert_eq!(u64::from(head.anchor_height), anchor);
                assert_eq!(head.generation, publication.inventory.generation);
                let newest = head.open_scopes.last().expect("an open newest scope");
                let expected = *oracle.expected_scopes(anchor).last().unwrap();
                assert_eq!(newest.scope_id, expected.index);
                assert_eq!(
                    newest.entries, expected.entries,
                    "round {round}: newest entries"
                );
                generations.push(publication.inventory.generation);
                for route in ["health", "ready"] {
                    let url = format!("http://{}/{route}", publisher.management());
                    tachyon_pir_e2e_tests::client::get(
                        &tachyon_pir_e2e_tests::client::plain(),
                        &url,
                    )
                    .await
                    .assert_status(StatusCode::OK, &url);
                }
                let series = format!("tachyon_tachygram_anchor_height {anchor}");
                wait_for(
                    "the publisher's anchor_height metric",
                    wait_limit(&budget),
                    || async {
                        let metrics = publisher.metrics().await;
                        if metrics.lines().any(|l| l == series) {
                            Ok(())
                        } else {
                            Err(metrics)
                        }
                    },
                )
                .await;
                // Grow the chain: two blocks, one with a proof stamp in the newest epoch.
                let mut chain = node.chain();
                let tip = chain.tip_height();
                extend(&mut chain, tip + 2 + round, &[(tip + 1, &[3])]);
            }
            assert!(
                generations.windows(2).all(|w| w[0] < w[1]),
                "generations must strictly increase: {generations:?}"
            );
        })
        .await
}

/// Scenarios 2 and 4: cold bootstrap over TLS from an empty state directory,
/// then n of n exact answers, hits and misses.
#[tokio::test(flavor = "multi_thread")]
async fn cold_bootstrap_over_tls_and_exact_answers() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = standard_chain(1);
            let fresh = chain.fresh_tachygrams(4);
            let oracle = TachygramOracle::new(&chain, u64::from(EPOCH_LENGTH));
            let anchor = oracle.anchor();
            let mut t = Topology::launch(
                chain,
                options(),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            t.publisher
                .wait_anchor(anchor, oracle.block_hash(anchor), wait_limit(&budget))
                .await;
            assert_eq!(
                std::fs::read_dir(t.query.state_dir()).unwrap().count(),
                0,
                "the query host starts from an empty state directory"
            );

            // Objects held at the edge: the host serves its listeners but is
            // not ready, and its namespace answers 503 (spec 11.6).
            let hold = t
                .edge
                .inject(Target::ProductObjects("tachygram".into()), Fault::Hold);
            t.query.start();
            wait_for(
                "an object fetch held at the edge",
                wait_limit(&budget),
                || async {
                    if t.edge.held() > 0 {
                        Ok(())
                    } else {
                        Err(format!("{} held", t.edge.held()))
                    }
                },
            )
            .await;
            let ready = t.query.management_get("ready").await;
            assert_eq!(
                ready.status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{}",
                ready.text()
            );
            let wallet = t.query.wallet();
            match wallet.fetch_map().await {
                Err(ClientError::Unavailable { body, .. }) => {
                    assert_eq!(body.code.as_str(), "temporarily_unavailable");
                }
                other => panic!("map before ready: {other:?}"),
            }
            assert!(
                t.edge.count_matching(&Target::Discovery) > 0,
                "discovery was checked before start (spec 11.6)"
            );
            t.edge.remove(hold);
            t.query.wait_ready(wait_limit(&budget)).await;

            let map = wallet.fetch_map().await.expect("the map once ready");
            check_map(&map, &oracle, anchor);
            wallet
                .accept_anchor(&map, &t.node.url())
                .await
                .expect("the anchor is the node's");
            let legacy =
                Client::with_mount(&t.query.origin(), Product::Tachygram, None, Mount::Legacy)
                    .unwrap()
                    .fetch_map()
                    .await
                    .expect("the legacy mount serves the map");
            assert_eq!(legacy.map_sha256(), map.map_sha256());

            let probes = probes(&oracle, anchor, &fresh);
            let mut tally = check_lookups(&wallet, &map, &oracle, &probes).await;
            // Above the anchor: not yet covered, so absent.
            let above: Vec<Probe> = oracle
                .records(9)
                .iter()
                .filter(|r| u64::from(r.blk_height) > anchor)
                .map(|r| Probe {
                    epoch: 9,
                    tachygram: r.tachygram,
                })
                .collect();
            assert_eq!(above.len(), 11);
            tally += check_lookups(&wallet, &map, &oracle, &above).await;
            // A scope without tables answers absent without a query.
            let before = wallet.stats().queries;
            let empty = wallet
                .lookup(&map, ScopeSelector::Index(5), &fresh[0])
                .await
                .unwrap();
            assert!(matches!(empty, LookupOutcome::Absent { .. }));
            assert_eq!(wallet.stats().queries, before);
            let hits: usize = (0..=9)
                .map(|e| {
                    oracle
                        .records(e)
                        .iter()
                        .filter(|r| u64::from(r.blk_height) <= anchor)
                        .count()
                })
                .sum();
            assert_eq!(tally.found, hits, "every covered tachygram was found");
            assert!(tally.absent >= 30, "{tally:?}");
            eprintln!("exact answers: {tally:?} in {:?}", budget.elapsed());
        })
        .await
}

/// Scenario 3: after the chain grows, the query host fetches only objects it
/// does not already hold (never one of the previous publication, never a
/// sealed table), the sealed scopes keep their session ids, and those
/// sessions still answer under the old map as under the new one.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_publication_fetches_only_changed_objects() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = standard_chain(3);
            let fresh = chain.fresh_tachygrams(2);
            let before = TachygramOracle::new(&chain, u64::from(EPOCH_LENGTH));
            let t = Topology::start(
                chain,
                options(),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let wallet = t.query.wallet();
            let old_map = wait_caught_up(&wallet, &before, &budget).await;
            let old = t.publisher.inventory().await.expect("published");
            assert_eq!(old.inventory.generation, chain_head(&old_map).generation);
            t.edge.reset_counts();

            let after = edit_chain(&t, |chain| {
                extend(chain, 164, &[(162, &[5]), (163, &[4])]);
            });
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            let new = t.publisher.inventory().await.expect("published");
            assert_eq!(new.inventory.generation, chain_head(&new_map).generation);
            assert!(new.inventory.generation > old.inventory.generation);

            let prefix = t.prefix();
            let fetched: BTreeSet<String> = t
                .edge
                .counts()
                .into_keys()
                .filter(|path| Target::Objects.matches(path))
                .collect();
            let held = old.object_paths(prefix);
            let refetched: Vec<&String> = fetched.intersection(&held).collect();
            assert!(
                refetched.is_empty(),
                "objects of the previous publication were fetched again: {refetched:?}"
            );
            let changed: BTreeSet<String> = new
                .object_paths(prefix)
                .difference(&held)
                .cloned()
                .collect();
            let missing: Vec<&String> = changed.difference(&fetched).collect();
            assert!(
                missing.is_empty(),
                "changed objects not fetched: {missing:?}"
            );
            assert!(
                changed
                    .iter()
                    .any(|path| new.table_object_paths(prefix).contains(path)),
                "the open scope's table changed"
            );
            for path in &fetched {
                assert_eq!(t.edge.count(path), 1, "{path} fetched more than once");
            }

            for sealed in [0, 1] {
                assert_eq!(chain_scope(&new_map, sealed).state, ScopeState::Sealed);
                assert_eq!(
                    sessions(&old_map, sealed),
                    sessions(&new_map, sealed),
                    "sealed scope {sealed} kept its sessions"
                );
            }
            assert_ne!(sessions(&old_map, 9), sessions(&new_map, 9));

            // Sealed sessions answer under the old map ..
            let sealed_probes: Vec<Probe> = probes(&before, before.anchor(), &fresh)
                .into_iter()
                .filter(|p| p.epoch < 2)
                .collect();
            let tally = check_lookups(&wallet, &old_map, &before, &sealed_probes).await;
            assert_eq!(
                tally.found,
                before.records(0).len() + before.records(1).len()
            );
            // .. and every scope answers under the new one.
            let mut tally = check_lookups(
                &wallet,
                &new_map,
                &after,
                &probes(&after, after.anchor(), &fresh)
                    .into_iter()
                    .filter(|p| p.epoch == 9)
                    .collect::<Vec<_>>(),
            )
            .await;
            tally += check_lookups(
                &wallet,
                &new_map,
                &after,
                &sample_probes(&after, after.anchor(), &fresh, 12, 3),
            )
            .await;
            assert!(tally.found > 30 && tally.absent > 5, "{tally:?}");
        })
        .await
}

/// Ground tachygrams for scope `scope_id`: for every seed `s` below
/// `MAX_SEEDS`, `SLOTS + 1` keys that share one row under `s`, so no seed
/// places the scope without overflow (spec 6.4). Grinding uses the protocol
/// crate's placement; expected answers still come from the authored chain.
fn grind_overflow(genesis: &BlockHash, scope_id: u64) -> Vec<Vec<[u8; 32]>> {
    let product = Product::Tachygram;
    let rows = Geometry::Tachyon4k8.rows();
    let per_row = product.slots() + 1;
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..tachyon_pir_protocol::MAX_SEEDS)
            .map(|seed| {
                scope.spawn(move || {
                    let base: [u8; 32] =
                        sha2_digest(&[b"tachyon-e2e/grind\0", &seed.to_le_bytes()]);
                    let candidate = |counter: u64| {
                        let mut key = base;
                        key[24..].copy_from_slice(&counter.to_le_bytes());
                        key
                    };
                    let row_of = |key: &[u8; 32]| {
                        tachyon_pir_protocol::row(product, genesis, scope_id, seed, rows, key)
                            .expect("a tachygram key")
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

fn sha2_digest(parts: &[&[u8]]) -> [u8; 32] {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

// ---------------------------------------------------------------------------
// Scenario 6: forced overflow.

/// Scenario 6 runs as this many tests (`forced_overflow_slices!`), one per
/// slice of the scope's tachygrams ([`overflow_slice`]); each starts its own
/// topology on the same chain ([`shared_overflow_chain`]). Each slice
/// sends about 1,310 queries (two per distinct row, about 61 ms of CPU each
/// on the two sides); slices that size stay near 17 s in the correctness
/// run, where every end-to-end binary's scenario 6 runs at once, under the
/// 25 s budget of [`Budget`].
const OVERFLOW_SLICES: usize = 6;
/// Row queries in flight at once in scenario 6's batched lookups. Alone,
/// more in flight (or more executing) finishes no sooner, and in the
/// correctness run it only takes CPU from the other test binaries.
const OVERFLOW_PARALLEL: usize = 12;

/// [`options`] with admission for [`OVERFLOW_PARALLEL`] queries in flight
/// from one client, eight of them executing at once.
fn overflow_options() -> TopologyOptions {
    options().query_setting(
        "admission",
        json!({"executing": 8, "waiting": 64, "wait_ms": 10000, "per_client": 16}),
    )
}

/// Scenario 6's chain, the same in every slice: epoch 0 lists every ground
/// tachygram (`SLOTS + 1` keys sharing one row under each seed, so no seed
/// fits, spec 6.4) in four blocks of proof stamps, and six other tachygrams
/// in block 15.
#[derive(Clone)]
struct OverflowChain {
    chain: SyntheticChain,
    genesis: BlockHash,
    /// The ground tachygrams, one group per seed.
    groups: Vec<Vec<[u8; 32]>>,
    /// The ground tachygrams, sorted.
    ground: Vec<[u8; 32]>,
    /// Tachygrams the chain never lists: misses.
    fresh: Vec<[u8; 32]>,
}

/// [`overflow_chain`], built once per test process and cloned for each
/// slice. The chain is deterministic, so each slice gets exactly the chain
/// it would build itself; the slices after the first skip the grind.
fn shared_overflow_chain() -> OverflowChain {
    static CHAIN: std::sync::OnceLock<OverflowChain> = std::sync::OnceLock::new();
    CHAIN.get_or_init(overflow_chain).clone()
}

fn overflow_chain() -> OverflowChain {
    let mut chain = SyntheticChain::new(filler_config(6));
    let genesis = BlockHash::from_protocol_bytes(chain.genesis_hash());
    let groups = grind_overflow(&genesis, 0);
    let mut ground: Vec<[u8; 32]> = groups.iter().flatten().copied().collect();
    ground.sort();
    ground.dedup();
    assert_eq!(
        ground.len(),
        groups.len() * (Product::Tachygram.slots() + 1)
    );
    extend(&mut chain, 9, &[]);
    // Epoch 0 (heights 8-23): four blocks of proof stamps of up to 1,700
    // ground tachygrams each, interleaved with pointer stamps.
    for block_keys in ground.chunks(ground.len().div_ceil(4)) {
        let mut block = BlockSpec::new().tx(TxSpec::tachyon(1));
        for stamp in block_keys.chunks(1700) {
            block = block
                .tx(TxSpec::tachyon(3).proof(stamp.to_vec()))
                .tx(TxSpec::tachyon(1).pointer());
        }
        chain.append(block);
    }
    extend(&mut chain, 30, &[(15, &[6])]);
    let fresh = chain.fresh_tachygrams(6);
    OverflowChain {
        chain,
        genesis,
        groups,
        ground,
        fresh,
    }
}

/// The slice of scenario 6 a tachygram belongs to: its row under seed 0
/// (`row0`, the row of segment 0 and, segment 1 being filled under seed 0
/// too, of segment 1), modulo [`OVERFLOW_SLICES`]. Every tachygram is in
/// exactly one slice, and tachygrams sharing a row share a slice, so the
/// slices together query each row of the scope's tachygrams once.
fn overflow_slice_of(row0: u32) -> usize {
    row0 as usize % OVERFLOW_SLICES
}

/// The probes of `records` (every record of the scope, as the oracle lists
/// them, in ordinal order) in slice `slice` ([`overflow_slice_of`]).
fn overflow_slice(records: &[Probe], row0: impl Fn(&[u8; 32]) -> u32, slice: usize) -> Vec<Probe> {
    records
        .iter()
        .filter(|p| overflow_slice_of(row0(&p.tachygram)) == slice)
        .copied()
        .collect()
}

/// Scenario 6, slice `slice` of [`OVERFLOW_SLICES`]: tachygrams ground so
/// that every seed overflows one row ([`overflow_chain`]) force an extra
/// segment. Every slice checks, on its own topology, that the slices
/// partition the oracle's records of the scope; that the scope has one
/// bucket of two segments, segment 0 under seed 0 and segment 1 holding the
/// overflowing row's excess, also under seed 0; and the publisher's
/// overflow metric. It then looks up every tachygram of its slice and six
/// tachygrams the chain never lists on the query host's public listener in
/// one batched lookup ([`Client::lookup_many`]). Every tachygram's plan
/// queries both segments, at its row under seed 0; each distinct row of the
/// plans is queried privately once and its decoded row shared by every
/// tachygram in it, and the client sends exactly that many queries. Every
/// answer is judged against the oracle: each tachygram of the slice is
/// found with its exact record, and each unlisted one is absent. The slice
/// holding the overflowing row also checks that each of the row's records
/// is found in the segment its ordinal rank puts it in, and that the row
/// fills segment 0 and spills the rest; the other slices check that they
/// hold none of it. Over all slices every tachygram of the scope, each
/// ground tachygram included, is looked up and found.
async fn forced_overflow(slice: usize) {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let OverflowChain {
                chain,
                genesis,
                groups,
                ground,
                fresh,
            } = shared_overflow_chain();
            let oracle = TachygramOracle::new(&chain, u64::from(EPOCH_LENGTH));
            let anchor = oracle.anchor();
            let records = oracle.records(0).to_vec();
            assert_eq!(records.len(), ground.len() + 6);
            let (_, last) = oracle.coverage(0, anchor).expect("epoch 0 is listed");
            assert!(
                records.iter().all(|r| u64::from(r.blk_height) <= last),
                "every record of epoch 0 is covered at the anchor"
            );

            // The row that overflows under seed 0, the seed buckets and
            // segment 0 are assigned under when no seed fits (spec 6.4).
            let row0 = |key: &[u8; 32]| {
                tachyon_pir_protocol::row(
                    Product::Tachygram,
                    &genesis,
                    0,
                    0,
                    Geometry::Tachyon4k8.rows(),
                    key,
                )
                .unwrap()
            };
            let hot = row0(&groups[0][0]);
            let hot_records: Vec<Probe> = records
                .iter()
                .filter(|r| row0(&r.tachygram) == hot)
                .map(|r| Probe {
                    epoch: 0,
                    tachygram: r.tachygram,
                })
                .collect();
            let spilled = hot_records.len() - Product::Tachygram.slots();
            let hot_slice = overflow_slice_of(hot);

            // Every tachygram of the scope: the overflowing row's, every other
            // ground tachygram and the six of block 15's proof stamp. The
            // slices partition them: their concatenation, sorted, is the
            // scope's tachygrams, sorted (the tachygrams are distinct, so one
            // in two slices would show).
            let all: Vec<Probe> = records
                .iter()
                .map(|r| Probe {
                    epoch: 0,
                    tachygram: r.tachygram,
                })
                .collect();
            let mut sorted: Vec<[u8; 32]> = all.iter().map(|p| p.tachygram).collect();
            sorted.sort();
            assert!(ground.iter().all(|g| sorted.binary_search(g).is_ok()));
            let mut union: Vec<[u8; 32]> = (0..OVERFLOW_SLICES)
                .flat_map(|i| overflow_slice(&all, row0, i))
                .map(|p| p.tachygram)
                .collect();
            union.sort();
            assert_eq!(union, sorted, "the slices partition the scope's tachygrams");
            let mine = overflow_slice(&all, row0, slice);

            let t = Topology::start(
                chain,
                overflow_options(),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let wallet = t.query.wallet();
            let map = wait_caught_up(&wallet, &oracle, &budget).await;
            let scope = chain_scope(&map, 0);
            assert_eq!(scope.n_buckets, 1);
            let segments: Vec<(u32, u32, u64)> = scope
                .tables
                .iter()
                .map(|t| (t.segment, t.seed, t.entries))
                .collect();
            assert_eq!(segments.len(), 2, "one overflow segment: {segments:?}");
            assert_eq!(
                segments[0].1, 0,
                "no seed fits: segment 0 is filled under seed 0"
            );
            assert_eq!(segments[1].2, spilled as u64, "the hot row's excess spills");
            assert_eq!(
                segments[1].1, 0,
                "the excess, from one row under seed 0, fits under seed 0"
            );

            // Every tachygram of this slice, and misses, in one batch.
            let mut probes = mine.clone();
            probes.extend(fresh.iter().map(|tachygram| Probe {
                epoch: 0,
                tachygram: *tachygram,
            }));
            let batch: Vec<[u8; 32]> = probes.iter().map(|p| p.tachygram).collect();
            let sent = wallet.stats().queries;
            let outcomes = wallet
                .lookup_many(&map, ScopeSelector::Index(0), &batch, OVERFLOW_PARALLEL)
                .await
                .unwrap_or_else(|e| panic!("lookup failed: {}", describe(&e)));
            let sent = wallet.stats().queries - sent;
            assert_eq!(outcomes.len(), probes.len());
            let mut tally = Tally::default();
            let mut in_segment = [0usize; 2];
            let mut rows = BTreeSet::new();
            for (probe, outcome) in probes.iter().zip(&outcomes) {
                tally += judge(&map, &oracle, anchor, *probe, outcome);
                let answer = outcome.answer().unwrap();
                assert_eq!(
                    answer.queries.len(),
                    2,
                    "every segment of the bucket is queried"
                );
                assert!(
                    answer
                        .queries
                        .iter()
                        .all(|q| q.row == row0(&probe.tachygram)),
                    "both segments are queried at the key's row under seed 0"
                );
                rows.extend(answer.queries.iter().map(|q| (q.segment, q.row)));
                // The hot row's entries in ordinal order (spec 8.2: height,
                // transaction index, position in the stamp, the oracle's
                // record order): the first SLOTS stay in segment 0, the rest
                // spill to segment 1 (spec 6.4).
                if let Some(rank) = hot_records
                    .iter()
                    .position(|h| h.tachygram == probe.tachygram)
                {
                    let hit = answer
                        .queries
                        .iter()
                        .find(|q| q.matches == 1)
                        .expect("a hot-row entry is found in one segment");
                    let expected = u32::from(rank >= Product::Tachygram.slots());
                    assert_eq!(
                        hit.segment, expected,
                        "hot-row entry of ordinal rank {rank} is in the wrong segment"
                    );
                    in_segment[hit.segment as usize] += 1;
                }
            }
            assert_eq!(
                tally.found,
                mine.len(),
                "every tachygram of the slice is found"
            );
            assert_eq!(tally.absent, fresh.len());
            assert_eq!(sent, rows.len() as u64, "each distinct row is queried once");
            eprintln!(
                "forced overflow, slice {slice}: {} lookups, {sent} queries, {:?}",
                batch.len(),
                budget.elapsed()
            );
            if slice == hot_slice {
                assert_eq!(
                    in_segment,
                    [Product::Tachygram.slots(), spilled],
                    "the overflowing row fills segment 0 and spills the rest"
                );
            } else {
                assert_eq!(
                    in_segment,
                    [0, 0],
                    "the overflowing row is in another slice"
                );
            }
            let metrics = t.publisher.metrics().await;
            assert!(
                metrics
                    .lines()
                    .any(|l| l == "tachyon_tachygram_segments_overflow_total 1"),
                "{metrics}"
            );
        })
        .await
}

/// One `#[tokio::test]` per slice of scenario 6, each running
/// [`forced_overflow`] on its slice. The listed slices must be exactly
/// `0..OVERFLOW_SLICES` in order (checked at compile time), so every slice,
/// and with them every tachygram of the scope, has its test.
macro_rules! forced_overflow_slices {
    ($($name:ident: $slice:literal),+ $(,)?) => {
        $(
            #[doc = concat!(
                "Scenario 6, slice ",
                stringify!($slice),
                " of the scope's tachygrams ([`overflow_slice`], [`forced_overflow`])."
            )]
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                forced_overflow($slice).await
            }
        )+
        const _: () = {
            let slices = [$($slice),+];
            assert!(slices.len() == OVERFLOW_SLICES);
            let mut i = 0;
            while i < slices.len() {
                assert!(slices[i] == i);
                i += 1;
            }
        };
    };
}

forced_overflow_slices! {
    forced_overflow_keeps_every_entry_retrievable_slice_0: 0,
    forced_overflow_keeps_every_entry_retrievable_slice_1: 1,
    forced_overflow_keeps_every_entry_retrievable_slice_2: 2,
    forced_overflow_keeps_every_entry_retrievable_slice_3: 3,
    forced_overflow_keeps_every_entry_retrievable_slice_4: 4,
    forced_overflow_keeps_every_entry_retrievable_slice_5: 5,
}

/// Scenario 8: a reorg inside the open newest scope gives a new generation in
/// the same recovery epoch; sealed sessions are kept; the new map's anchor is
/// the node's, the old one's no longer is; answers match the new chain,
/// including absence of the tachygrams reorged away.
#[tokio::test(flavor = "multi_thread")]
async fn shallow_reorg_answers_the_new_chain() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = standard_chain(8);
            let fresh = chain.fresh_tachygrams(3);
            let before = TachygramOracle::new(&chain, u64::from(EPOCH_LENGTH));
            let t = Topology::start(
                chain,
                options(),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let wallet = t.query.wallet();
            let old_map = wait_caught_up(&wallet, &before, &budget).await;

            let after = edit_chain(&t, |chain| {
                chain.reorg(155, Vec::new()).expect("fork below the tip");
                extend(chain, 162, &[(157, &[5]), (159, &[4, 3]), (160, &[6])]);
            });
            assert_eq!(after.anchor(), 160);
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            let (old_head, new_head) = (chain_head(&old_map), chain_head(&new_map));
            assert!(new_head.generation > old_head.generation);
            assert_eq!(
                new_head.recovery_epoch, old_head.recovery_epoch,
                "a rewind inside open scopes does not recover (spec 11.4)"
            );
            for sealed in [0, 1] {
                assert_eq!(sessions(&old_map, sealed), sessions(&new_map, sealed));
            }
            assert_ne!(sessions(&old_map, 9), sessions(&new_map, 9));
            wallet
                .accept_anchor(&new_map, &t.node.url())
                .await
                .expect("the new anchor is the node's");
            match wallet.accept_anchor(&old_map, &t.node.url()).await {
                Err(ClientError::AnchorMismatch { what: "anchor", .. }) => {}
                other => panic!("the reorged-away anchor was accepted: {other:?}"),
            }

            let gone = vanished(&before, &after, 9);
            assert_eq!(gone.len(), 7 + 5 + 6, "blocks 158-160 were replaced");
            let mut tally = check_lookups(&wallet, &new_map, &after, &gone).await;
            assert_eq!(tally.absent, gone.len());
            tally += check_lookups(
                &wallet,
                &new_map,
                &after,
                &probes(&after, 160, &fresh)
                    .into_iter()
                    .filter(|p| p.epoch == 9)
                    .collect::<Vec<_>>(),
            )
            .await;
            tally += check_lookups(
                &wallet,
                &new_map,
                &after,
                &sample_probes(&after, 160, &fresh, 16, 8),
            )
            .await;
            assert!(tally.found >= 6 + 5 + 4 + 5 + 4 + 3 + 6, "{tally:?}");
        })
        .await
}

/// Scenario 9: a reorg reaching sealed epoch 1 increments the recovery
/// epoch; the sessions of the rebuilt tables answer `410`, the untouched
/// sealed epoch 0 keeps its session and keeps answering, and the new map's
/// answers match the new chain.
#[tokio::test(flavor = "multi_thread")]
async fn reorg_into_sealed_scope_recovers_and_revokes() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = standard_chain(9);
            let fresh = chain.fresh_tachygrams(3);
            let before = TachygramOracle::new(&chain, u64::from(EPOCH_LENGTH));
            let t = Topology::start(
                chain,
                options(),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let wallet = t.query.wallet();
            let old_map = wait_caught_up(&wallet, &before, &budget).await;
            let old_probe = |epoch: u64| Probe {
                epoch,
                tachygram: before.records(epoch).last().unwrap().tachygram,
            };
            // Use every old table once, so the client holds its public setup.
            check_lookups(
                &wallet,
                &old_map,
                &before,
                &[old_probe(0), old_probe(1), old_probe(9)],
            )
            .await;
            let revoked_before = query_metric(&t, "sessions_revoked_total").await;

            let after = edit_chain(&t, |chain| {
                chain.reorg(30, Vec::new()).expect("fork below the tip");
                extend(
                    chain,
                    165,
                    &[(33, &[6]), (39, &[5]), (152, &[4]), (157, &[6])],
                );
            });
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            let (old_head, new_head) = (chain_head(&old_map), chain_head(&new_map));
            assert!(
                new_head.recovery_epoch.get() > old_head.recovery_epoch.get(),
                "a rewind into a sealed scope recovers (spec 11.4)"
            );
            assert!(new_head.generation > old_head.generation);
            assert_eq!(chain_scope(&new_map, 1).state, ScopeState::Sealed);
            assert_eq!(sessions(&old_map, 0), sessions(&new_map, 0));
            assert_ne!(sessions(&old_map, 1), sessions(&new_map, 1));

            for epoch in [1, 9] {
                let probe = old_probe(epoch);
                match wallet
                    .lookup(&old_map, ScopeSelector::Index(epoch), &probe.tachygram)
                    .await
                {
                    Err(ClientError::SessionRevoked { body, .. }) => {
                        assert_eq!(body.code.as_str(), "session_unavailable");
                    }
                    other => panic!("old epoch {epoch} session: expected 410, got {other:?}"),
                }
            }
            // Epoch 0's session is in the current publication: it still answers.
            let epoch0: Vec<Probe> = before
                .records(0)
                .iter()
                .map(|r| Probe {
                    epoch: 0,
                    tachygram: r.tachygram,
                })
                .collect();
            let tally = check_lookups(&wallet, &old_map, &before, &epoch0).await;
            assert_eq!(tally.found, epoch0.len());
            wait_for("revoked sessions counted", wait_limit(&budget), || async {
                let revoked = query_metric(&t, "sessions_revoked_total").await;
                if revoked >= revoked_before + 2.0 {
                    Ok(())
                } else {
                    Err(format!("{revoked}"))
                }
            })
            .await;

            let mut gone = vanished(&before, &after, 1);
            gone.extend(vanished(&before, &after, 9));
            assert_eq!(gone.len(), 5 + 4 + 3 + 6 + before.records(9).len());
            let mut tally = check_lookups(&wallet, &new_map, &after, &gone).await;
            assert_eq!(tally.absent, gone.len());
            let new_hits: Vec<Probe> = probes(&after, after.anchor(), &fresh)
                .into_iter()
                .filter(|p| p.epoch != 0)
                .collect();
            tally += check_lookups(&wallet, &new_map, &after, &new_hits).await;
            assert_eq!(
                tally.found,
                after.records(1).len() + after.records(9).len(),
                "{tally:?}"
            );
        })
        .await
}

/// Scenario 10's check that the query host still serves `old_map`, ready,
/// with exact answers at its anchor: the tachygrams the refused publication
/// would add are absent.
async fn previous_served(
    t: &Topology,
    wallet: &Arc<Client>,
    old_map: &MapView,
    before: &TachygramOracle,
    fresh: &[[u8; 32]],
) {
    let ready = t.query.management_get("ready").await;
    assert_eq!(ready.status, StatusCode::OK, "{}", ready.text());
    let map = wallet.fetch_map().await.expect("the previous map");
    assert_eq!(
        map.map_sha256(),
        old_map.map_sha256(),
        "the previous publication serves"
    );
    let probes = sample_probes(before, before.anchor(), fresh, 16, 10);
    check_lookups(wallet, &map, before, &probes).await;
    let above: Vec<Probe> = before
        .records(9)
        .iter()
        .filter(|r| u64::from(r.blk_height) > before.anchor())
        .map(|r| Probe {
            epoch: 9,
            tachygram: r.tachygram,
        })
        .collect();
    let tally = check_lookups(wallet, &map, before, &above).await;
    assert_eq!(tally.absent, above.len());
}

/// Scenario 10, for one fault on the new open table's rows object: the
/// object is refused (the pass fails), the query host keeps serving the
/// previous publication with exact answers, and switches once the object
/// arrives intact.
///
/// The host's next pass is held at `current` while the chain grows, so the
/// fault is in place before the new publication's objects are requested.
/// One refusal only: the sync manager backs off 10 s after a failed pass
/// (`pir_pipeline::consumer::polling`), so a second fault round would not fit
/// the budget; corruption and truncation are therefore two tests.
/// The chain has one table, the open epoch's.
async fn refused_object(seed: u64, fault: Fault) {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = open_epoch_chain(seed);
            let fresh = chain.fresh_tachygrams(2);
            let before = TachygramOracle::new(&chain, u64::from(EPOCH_LENGTH));
            let t = Topology::start(
                chain,
                options(),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let wallet = t.query.wallet();
            let old_map = wait_caught_up(&wallet, &before, &budget).await;
            let old = t.publisher.inventory().await.expect("published");
            let failed = "sync_passes_total{result=\"failed\"}";

            let hold = t
                .edge
                .inject(Target::Suffix("/current".into()), Fault::Hold);
            wait_for("a pass held at current", wait_limit(&budget), || async {
                if t.edge.held() > 0 {
                    Ok(())
                } else {
                    Err("none held".to_owned())
                }
            })
            .await;
            let after = edit_chain(&t, |chain| {
                extend(chain, 164, &[(162, &[5])]);
            });
            let new = t
                .publisher
                .wait_anchor(
                    after.anchor(),
                    after.block_hash(after.anchor()),
                    wait_limit(&budget),
                )
                .await;
            let rebuilt: Vec<String> = new
                .inventory
                .tables
                .iter()
                .filter(|table| !old.inventory.tables.contains(table))
                .map(|table| format!("{}objects/{}", t.prefix(), table.rows.sha256))
                .collect();
            assert_eq!(rebuilt.len(), 1, "one rebuilt table");
            let rows = rebuilt[0].clone();
            let failed_before = query_metric(&t, failed).await;
            t.edge.inject(Target::Path(rows.clone()), fault);
            t.edge.remove(hold);
            wait_for(
                "the faulted rows object refused",
                wait_limit(&budget),
                || async {
                    let fetched = t.edge.count(&rows);
                    let failures = query_metric(&t, failed).await - failed_before;
                    if fetched > 0 && failures > 0.0 {
                        Ok(())
                    } else {
                        Err(format!("{fetched} fetches, {failures} failed passes"))
                    }
                },
            )
            .await;
            previous_served(&t, &wallet, &old_map, &before, &fresh).await;
            let refused = query_metric(&t, failed).await - failed_before;

            t.edge.clear_faults();
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            assert_eq!(chain_head(&new_map).generation, new.inventory.generation);
            assert!(t.edge.count(&rows) >= 2, "fetched again intact");
            let mut probes = sample_probes(&after, after.anchor(), &fresh, 12, seed);
            probes.extend(after.records(9).iter().map(|r| Probe {
                epoch: 9,
                tachygram: r.tachygram,
            }));
            check_lookups(&wallet, &new_map, &after, &probes).await;
            eprintln!(
                "refused {refused} pass(es); switched after {:?}",
                budget.elapsed()
            );
        })
        .await
}

/// Scenario 10, corrupt: the rows object arrives with one bit flipped (same
/// length, wrong digest).
#[tokio::test(flavor = "multi_thread")]
async fn corrupt_object_is_refused_and_previous_publication_serves() {
    refused_object(10, Fault::CorruptBody).await;
}

/// Scenario 10, truncated: the rows object's connection is cut after half its
/// body.
#[tokio::test(flavor = "multi_thread")]
async fn truncated_object_is_refused_and_previous_publication_serves() {
    refused_object(20, Fault::TruncateBody).await;
}

/// Scenario 11: with the publisher unreachable the query host keeps serving
/// exact answers for `max_unconfirmed_seconds` after its last successful
/// authority check started, then answers `503`, and serves again once the
/// publisher is back.
#[tokio::test(flavor = "multi_thread")]
async fn publisher_outage_serves_within_authority_then_503() {
    const WINDOW: Duration = Duration::from_secs(3);
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = standard_chain(11);
            let fresh = chain.fresh_tachygrams(2);
            let oracle = TachygramOracle::new(&chain, u64::from(EPOCH_LENGTH));
            let options =
                options().query_setting("max_unconfirmed_seconds", json!(WINDOW.as_secs()));
            let t =
                Topology::start(chain, options, publisher_binary(), query_binary(), &budget).await;
            let wallet = t.query.wallet();
            let map = wait_caught_up(&wallet, &oracle, &budget).await;
            let probes = sample_probes(&oracle, oracle.anchor(), &fresh, 24, 11);
            check_lookups(&wallet, &map, &oracle, &probes).await;

            t.edge.outage().await;
            let down = Instant::now();
            let mut answered = 0;
            let refused_after = loop {
                let probe = probes[answered % probes.len()];
                match wallet
                    .lookup(&map, ScopeSelector::Index(probe.epoch), &probe.tachygram)
                    .await
                {
                    Ok(outcome) => {
                        judge(&map, &oracle, oracle.anchor(), probe, &outcome);
                        answered += 1;
                    }
                    Err(ClientError::Unavailable { body, .. }) => {
                        assert_eq!(body.code.as_str(), "temporarily_unavailable");
                        break down.elapsed();
                    }
                    Err(error) => panic!("during the outage: {}", describe(&error)),
                }
                assert!(
                    down.elapsed() < WINDOW + Duration::from_secs(3),
                    "still serving {:?} into the outage, beyond max_unconfirmed_seconds",
                    down.elapsed()
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            };
            assert!(
                refused_after >= WINDOW - Duration::from_secs(1),
                "refused after {refused_after:?}, before the {WINDOW:?} window ran out"
            );
            assert!(answered > 5, "only {answered} answers inside the window");
            eprintln!("outage: {answered} exact answers, 503 after {refused_after:?}");
            match wallet.fetch_map().await {
                Err(ClientError::Unavailable { .. }) => {}
                other => panic!("map after the window: {other:?}"),
            }

            t.edge.resume().await.expect("the edge listens again");
            let back = wait_map(
                &wallet,
                "serving after the outage",
                wait_limit(&budget),
                |_| true,
            )
            .await;
            assert_eq!(
                back.map_sha256(),
                map.map_sha256(),
                "the same publication serves again"
            );
            check_lookups(&wallet, &back, &oracle, &probes).await;
        })
        .await
}

/// Scenario 12: the publisher, then the query host, crash and restart. The
/// publisher republishes under a new recovery epoch and generation with the
/// same tables, so the query host fetches no table object; the query host
/// recovers from its state directory. A prober running throughout never
/// sees a wrong answer, and answers right after each restart are exact.
#[tokio::test(flavor = "multi_thread")]
async fn restart_of_each_process_recovers_without_wrong_answers() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = standard_chain(12);
            let fresh = chain.fresh_tachygrams(2);
            let oracle = Arc::new(TachygramOracle::new(&chain, u64::from(EPOCH_LENGTH)));
            let mut t = Topology::start(
                chain,
                options(),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let wallet = t.query.wallet();
            let map = wait_caught_up(&wallet, &oracle, &budget).await;
            let probes = sample_probes(&oracle, oracle.anchor(), &fresh, 20, 12);
            check_lookups(&wallet, &map, &oracle, &probes).await;
            let old = t.publisher.inventory().await.expect("published");
            let prefix = t.prefix();

            // Publisher crash.
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let prober = spawn_prober(
                t.query.wallet(),
                oracle.clone(),
                probes.clone(),
                false,
                stop.clone(),
            );
            t.edge.reset_counts();
            t.publisher.crash_restart(wait_limit(&budget)).await;
            let new = t
                .publisher
                .wait_publication("the republication", wait_limit(&budget), |p| {
                    p.inventory.recovery_epoch > old.inventory.recovery_epoch
                })
                .await;
            assert!(new.inventory.generation > old.inventory.generation);
            assert_eq!(
                new.inventory.tables, old.inventory.tables,
                "the same tables (spec 11.4)"
            );
            assert_eq!(new.inventory.subject, old.inventory.subject);
            let republished = wait_map(&wallet, "the republished map", wait_limit(&budget), |m| {
                chain_head(m).generation == new.inventory.generation
            })
            .await;
            check_map(&republished, &oracle, oracle.anchor());
            check_lookups(&wallet, &republished, &oracle, &probes).await;
            for scope in 0..=9 {
                assert_eq!(sessions(&map, scope), sessions(&republished, scope));
            }
            let tables = old.table_object_paths(prefix);
            let refetched: Vec<String> = t
                .edge
                .counts()
                .into_keys()
                .filter(|path| tables.contains(path))
                .collect();
            assert!(
                refetched.is_empty(),
                "table objects fetched again: {refetched:?}"
            );
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let seen = prober.await.expect("the prober saw no wrong answer");
            assert!(seen.answers > 0, "{seen:?}");
            eprintln!("publisher restart: {seen:?}");

            // Query host crash.
            t.edge.reset_counts();
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let prober = spawn_prober(
                t.query.wallet(),
                oracle.clone(),
                probes.clone(),
                true,
                stop.clone(),
            );
            t.query.crash_restart(wait_limit(&budget)).await;
            let wallet = t.query.wallet();
            let recovered = wallet.fetch_map().await.expect("a map right after ready");
            assert_eq!(chain_head(&recovered).generation, new.inventory.generation);
            check_map(&recovered, &oracle, oracle.anchor());
            check_lookups(&wallet, &recovered, &oracle, &probes).await;
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let seen = prober.await.expect("the prober saw no wrong answer");
            assert!(seen.answers > 0, "{seen:?}");
            let refetched: Vec<String> = t
                .edge
                .counts()
                .into_keys()
                .filter(|path| tables.contains(path))
                .collect();
            assert!(
                refetched.is_empty(),
                "the restarted query host refetched table objects its state \
                 directory holds: {refetched:?}"
            );
            eprintln!("query host restart: {seen:?}");
        })
        .await
}

/// Scenario 13: with admission at one executing and one waiting query, a
/// burst of concurrent lookups is partly refused with `429`,
/// `Retry-After: 1` and the `overloaded` body, while every admitted lookup
/// completes with the exact answer.
#[tokio::test(flavor = "multi_thread")]
async fn overload_refuses_with_retry_after_while_admitted_complete() {
    const BURST: usize = 32;
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = standard_chain(13);
            let fresh = chain.fresh_tachygrams(2);
            let oracle = Arc::new(TachygramOracle::new(&chain, u64::from(EPOCH_LENGTH)));
            let options = options().query_setting(
                "admission",
                json!({"executing": 1, "waiting": 1, "wait_ms": 200, "per_client": 64}),
            );
            let t =
                Topology::start(chain, options, publisher_binary(), query_binary(), &budget).await;
            let wallet = t.query.wallet();
            let map = wait_caught_up(&wallet, &oracle, &budget).await;
            let probes = sample_probes(&oracle, oracle.anchor(), &fresh, BURST, 13);
            // One lookup per table first, sequentially, so the burst is queries only.
            for epoch in [0, 1, 9] {
                let probe = probes.iter().find(|p| p.epoch == epoch).copied().unwrap();
                check_lookups(&wallet, &map, &oracle, &[probe]).await;
            }
            let overloaded_before = query_metric(&t, "queries_total{result=\"overloaded\"}").await;

            let barrier = Arc::new(tokio::sync::Barrier::new(BURST));
            let tasks: Vec<_> = probes
                .iter()
                .copied()
                .map(|probe| {
                    let (wallet, map, barrier) = (wallet.clone(), map.clone(), barrier.clone());
                    tokio::spawn(async move {
                        barrier.wait().await;
                        let outcome = wallet
                            .lookup(&map, ScopeSelector::Index(probe.epoch), &probe.tachygram)
                            .await;
                        (probe, outcome)
                    })
                })
                .collect();
            let (mut admitted, mut refused) = (0, 0);
            for task in tasks {
                let (probe, outcome) = task.await.expect("lookup task");
                match outcome {
                    Ok(outcome) => {
                        judge(&map, &oracle, oracle.anchor(), probe, &outcome);
                        admitted += 1;
                    }
                    Err(ClientError::Overloaded {
                        retry_after, body, ..
                    }) => {
                        assert_eq!(retry_after, Some(Duration::from_secs(1)));
                        assert_eq!(body.code.as_str(), "overloaded");
                        refused += 1;
                    }
                    Err(error) => panic!("burst lookup: {}", describe(&error)),
                }
            }
            eprintln!("overload: {admitted} admitted, {refused} refused");
            assert!(refused > 0, "no query was refused");
            assert!(admitted > 0, "no query was admitted");
            let overloaded = query_metric(&t, "queries_total{result=\"overloaded\"}").await;
            assert_eq!(overloaded - overloaded_before, refused as f64);
            // Admission recovers: sequential lookups all succeed.
            for probe in probes.iter().take(6) {
                check_lookups(&wallet, &map, &oracle, &[*probe]).await;
            }
        })
        .await
}
