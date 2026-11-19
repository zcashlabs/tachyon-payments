//! Memo end-to-end acceptance (spec 13.2 scenarios 1-13; with
//! `tests/first_contact.rs`, the milestone M2 exit criterion of spec 14).
//!
//! Every test runs the topology of spec 13.1: the in-process node double
//! serving an authored chain (16-block epochs), one publisher process with
//! memo and first-contact enabled (one definition, one listener serving both
//! distribution prefixes), the TLS edge mapping both prefixes to that
//! listener and discovery to management, the query-server binary trusting
//! the edge, and the probe client as the wallet. Expected answers come only
//! from [`MemoOracle`], computed from the authored envelopes.
use reqwest::StatusCode;
use serde_json::json;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tachyon_fixtures::envelope::{random_tag, tag_with_key16};
use tachyon_fixtures::{
    AuthoredEntry, AuthoredEnvelope, BlockSpec, ChainConfig, EntryKind, EnvelopeMix, Malformation,
    MalformedMemo, SyntheticChain, TxMix, TxSpec,
};
use tachyon_pir_cli::{Client, ClientError, LookupOutcome, MapView, Mount, ScopeSelector};
use tachyon_pir_e2e_tests::budget::Budget;
use tachyon_pir_e2e_tests::memo_checks::{
    Probe, Served, Tally, chain_scope, check_head, check_listing, check_lookups, check_map,
    grind_overflow, judge, key_probes, lookups, probes, row_under_seed0, scope_probes, sessions,
    spawn_prober,
};
use tachyon_pir_e2e_tests::memo_oracle::{Family, MemoOracle};
use tachyon_pir_e2e_tests::oracle::{Sampler, State};
use tachyon_pir_e2e_tests::topology::{
    ProductNames, PublisherHost, Topology, TopologyOptions, chain_head, describe, exclusive,
    metric_value, wait_for, wait_limit, wait_map,
};
use tachyon_pir_e2e_tests::{Fault, Target, binary};
use tachyon_pir_protocol::{BlockHash, Product, Scope, ScopeState};

/// Blocks per epoch on the synthetic network.
const EPOCH_LENGTH: u32 = 16;
/// Lookups in flight at once (the query host's admission allows more).
const PARALLEL: usize = 4;
const MEMO: ProductNames = ProductNames::MEMO;

fn publisher_binary() -> PathBuf {
    binary!("tachyon-pir-publisher-test")
}

fn query_binary() -> PathBuf {
    binary!("tachyon-pir-query-server-test")
}

/// Memo under test; the publisher hosts memo and first-contact.
fn options() -> TopologyOptions {
    let options = TopologyOptions::new(MEMO, EPOCH_LENGTH);
    assert_eq!(
        options.publish,
        [ProductNames::MEMO, ProductNames::FIRST_CONTACT]
    );
    options
}

fn oracle(chain: &SyntheticChain) -> MemoOracle {
    MemoOracle::new(chain, u64::from(EPOCH_LENGTH))
}

// ---------------------------------------------------------------------------
// Authoring chains.

/// Generated blocks carry pointer stamps and memos that never hold a memo
/// or first-contact entry: empty memos, wallet-state-only envelopes and
/// malformed envelopes of every kind. Follow-ups appear only where a test
/// puts them, so only those epochs have tables.
fn filler_config(seed: u64) -> ChainConfig {
    ChainConfig {
        mix: TxMix {
            tachyon_txs: [0, 2],
            actions: [1, 2],
            proof_percent: 0,
            tachygrams: [5, 9],
            memo_percent: 0,
            memo_len: [1, 40],
        },
        envelopes: Some(EnvelopeMix {
            envelope_percent: 60,
            malformed_percent: 25,
            follow_ups: [0, 0],
            first_contacts: [0, 0],
            wallet_states: [1, 2],
            wallet_state_len: [0, 24],
            repeat_tag_percent: 0,
            key16s: Vec::new(),
            key16_percent: 0,
        }),
        ..ChainConfig::with_seed(seed)
    }
}

fn fresh_tag(chain: &mut SyntheticChain) -> [u8; 32] {
    random_tag(chain.rng())
}

/// A follow-up with `tag`.
fn fu(chain: &mut SyntheticChain, tag: [u8; 32]) -> AuthoredEntry {
    AuthoredEntry::random_with_tag(chain.rng(), EntryKind::FollowUp, tag)
}

/// `n` follow-ups with fresh tags.
fn fus(chain: &mut SyntheticChain, n: usize) -> Vec<AuthoredEntry> {
    (0..n)
        .map(|_| AuthoredEntry::random(chain.rng(), EntryKind::FollowUp))
        .collect()
}

fn entry(chain: &mut SyntheticChain, kind: EntryKind) -> AuthoredEntry {
    AuthoredEntry::random(chain.rng(), kind)
}

/// A block with one transaction per envelope, after an empty-memo Tachyon
/// transaction (so transaction indices start at 2, the coinbase being 0).
fn block(envelopes: Vec<Vec<AuthoredEntry>>) -> BlockSpec {
    let mut block = BlockSpec::new().tx(TxSpec::tachyon(1));
    for entries in envelopes {
        block = block.tx(TxSpec::tachyon(1).envelope(&AuthoredEnvelope::new(entries)));
    }
    block
}

/// Appends generated filler blocks until the tip is `height`.
fn fill_to(chain: &mut SyntheticChain, height: u64) {
    while chain.tip_height() < height {
        chain.append_random(1);
    }
}

/// Appends `spec` at `height`, filling the blocks before it.
fn put(chain: &mut SyntheticChain, height: u64, spec: BlockSpec) {
    fill_to(chain, height - 1);
    chain.append(spec);
    assert_eq!(chain.tip_height(), height);
}

/// Tags repeated on the standard chain.
#[derive(Clone, Copy, Debug)]
struct Repeats {
    /// In epoch 0: two transactions of block 12, twice in the second one's
    /// envelope; and once more in epoch 1 (block 31).
    d: [u8; 32],
    /// In epoch 9: three transactions of block 155, the anchor block 158 and
    /// block 159 above the anchor.
    e: [u8; 32],
    /// Shares its `key16` with a follow-up of block 24 (epoch 1).
    collide: [u8; 32],
}

/// The standard chain, tip 160 (anchor 158), activation 8, 16-block epochs.
///
/// - Epoch 0 (heights 8-23) and epoch 1 (24-39) hold follow-ups and are
///   sealed (their last block is at least 100 below the anchor). Their first
///   and last blocks carry follow-ups.
/// - Epochs 2-8 have no follow-ups: listed, without tables.
/// - Epoch 9 (152-167) is the open newest scope, with follow-ups in its
///   first block, at the anchor and in the two blocks above the anchor (not
///   yet covered).
/// - Repeated tags ([`Repeats`]) across transactions, blocks, scopes and
///   within one envelope, and a `key16` collision.
fn standard_chain(seed: u64) -> (SyntheticChain, Repeats) {
    let mut chain = SyntheticChain::new(filler_config(seed));
    let d = fresh_tag(&mut chain);
    let e = fresh_tag(&mut chain);
    let b = fus(&mut chain, 3);
    put(&mut chain, 8, block(vec![b]));
    let first = vec![fu(&mut chain, d), entry(&mut chain, EntryKind::FollowUp)];
    let mut second = fus(&mut chain, 1);
    second.push(fu(&mut chain, d));
    second.push(entry(&mut chain, EntryKind::WalletState));
    second.push(fu(&mut chain, d));
    second.extend(fus(&mut chain, 2));
    put(&mut chain, 12, block(vec![first, second]));
    let b = fus(&mut chain, 5);
    put(&mut chain, 23, block(vec![b]));
    let at24 = fus(&mut chain, 3);
    let collide = tag_with_key16(chain.rng(), at24[1].key16());
    put(&mut chain, 24, block(vec![at24]));
    let mut b = vec![fu(&mut chain, d)];
    b.extend(fus(&mut chain, 4));
    put(&mut chain, 31, block(vec![b]));
    let b1 = fus(&mut chain, 4);
    let b2 = vec![
        fu(&mut chain, collide),
        entry(&mut chain, EntryKind::FollowUp),
    ];
    put(&mut chain, 39, block(vec![b1, b2]));
    let b = fus(&mut chain, 4);
    put(&mut chain, 152, block(vec![b]));
    let mut t1 = vec![fu(&mut chain, e)];
    t1.extend(fus(&mut chain, 2));
    let t2 = vec![fu(&mut chain, e)];
    let t3 = vec![entry(&mut chain, EntryKind::FollowUp), fu(&mut chain, e)];
    put(&mut chain, 155, block(vec![t1, t2, t3]));
    let mut b = fus(&mut chain, 4);
    b.push(fu(&mut chain, e));
    put(&mut chain, 158, block(vec![b]));
    let mut b = fus(&mut chain, 3);
    b.push(fu(&mut chain, e));
    put(&mut chain, 159, block(vec![b]));
    let b = fus(&mut chain, 2);
    put(&mut chain, 160, block(vec![b]));
    (chain, Repeats { d, e, collide })
}

/// `n` random nonzero `key16`s no entry of the chain uses (misses).
fn fresh_keys(chain: &mut SyntheticChain, n: usize) -> Vec<[u8; 16]> {
    (0..n)
        .map(|_| {
            let tag = fresh_tag(chain);
            let mut key = [0; 16];
            key.copy_from_slice(&tag[..16]);
            key
        })
        .collect()
}

fn key16(tag: &[u8; 32]) -> [u8; 16] {
    let mut key = [0; 16];
    key.copy_from_slice(&tag[..16]);
    key
}

// ---------------------------------------------------------------------------
// Checks.

/// Edits the node double's chain with `edit` and returns the oracle of the
/// result.
fn edit_chain(t: &Topology, edit: impl FnOnce(&mut SyntheticChain)) -> MemoOracle {
    let mut chain = t.node.chain();
    edit(&mut chain);
    oracle(&chain)
}

/// Waits until the query host serves a memo map anchored where `oracle` says
/// a caught-up publication is (height and hash), and checks it.
async fn wait_caught_up(wallet: &Client, oracle: &MemoOracle, budget: &Budget) -> Arc<MapView> {
    let anchor = oracle.anchor();
    let hash = BlockHash::from_protocol_bytes(oracle.block_hash(anchor));
    let map = wait_map(
        wallet,
        &format!("the query host to serve memo anchor {anchor}"),
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

/// `tachyon_memo_<series>` on the query host's management `/metrics`.
async fn query_metric(t: &Topology, series: &str) -> f64 {
    let name = format!("tachyon_memo_{series}");
    t.query
        .metric(&name)
        .await
        .unwrap_or_else(|| panic!("the query host exports no {name}"))
}

/// Waits until the publisher's `/metrics` on the memo listener shows
/// `series` at `value`.
async fn wait_publisher_metric(
    publisher: &PublisherHost,
    series: &str,
    value: u64,
    budget: &Budget,
) {
    wait_for(
        &format!("the publisher's {series} to be {value}"),
        wait_limit(budget),
        || async {
            let text = publisher.metrics().await;
            match metric_value(&text, series) {
                Some(v) if v == value as f64 => Ok(()),
                other => Err(format!("{other:?}")),
            }
        },
    )
    .await;
}

/// The invalid-envelope and wallet-state gauges of the publisher against the
/// envelopes of the blocks the journal stores (heights up to the anchor;
/// spec 5.2, 11.8).
async fn check_envelope_gauges(
    publisher: &PublisherHost,
    oracle: &MemoOracle,
    budget: &Budget,
) -> (usize, usize) {
    let counts = oracle.envelope_counts(oracle.anchor());
    let above = oracle.envelope_counts(oracle.tip());
    wait_publisher_metric(
        publisher,
        "tachyon_memo_envelopes_invalid_total",
        counts.malformed as u64,
        budget,
    )
    .await;
    wait_publisher_metric(
        publisher,
        "tachyon_memo_state_entries_total",
        counts.wallet_states as u64,
        budget,
    )
    .await;
    assert_eq!(counts.raw, 0, "the chain has no unclassified memos");
    (counts.malformed, above.malformed - counts.malformed)
}

fn sample(probes: Vec<Probe>, k: usize, seed: u64) -> Vec<Probe> {
    Sampler::new(seed).sample(&probes, k)
}

/// Holds the query host's next synchronization pass at `current`, so a
/// chain edit made meanwhile is first published in full: a pass that runs
/// into the publisher's withdrawal fails and backs off 10 s (spec 11.6),
/// which would make the test's duration depend on that race.
async fn hold_next_pass(t: &Topology, budget: &Budget) -> tachyon_pir_e2e_tests::FaultId {
    let hold = t
        .edge
        .inject(Target::Suffix("/memo/v1/current".into()), Fault::Hold);
    wait_for("a pass held at current", wait_limit(budget), || async {
        if t.edge.held() > 0 {
            Ok(())
        } else {
            Err("none held".to_owned())
        }
    })
    .await;
    hold
}

/// Waits for the publisher to publish `after`'s caught-up anchor, then lets
/// the held pass continue.
async fn release_after_publication(
    t: &Topology,
    hold: tachyon_pir_e2e_tests::FaultId,
    after: &MemoOracle,
    budget: &Budget,
) {
    let anchor = after.anchor();
    t.distribution()
        .wait_anchor(anchor, after.block_hash(anchor), wait_limit(budget))
        .await;
    t.edge.remove(hold);
}

// ---------------------------------------------------------------------------
// Scenarios.

/// Scenario 1: with no query host at all, publications of both products of
/// the memo definition advance as the chain grows (generation strictly
/// increasing, anchor following the tip, the memo scope listing and gauges
/// matching the oracle each round, epochs sealing on schedule) and the
/// publisher stays healthy and ready.
#[tokio::test(flavor = "multi_thread")]
async fn publisher_without_consumers_advances() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = SyntheticChain::new(filler_config(101));
            let b = fus(&mut chain, 4);
            put(&mut chain, 10, block(vec![b]));
            let b = fus(&mut chain, 3);
            put(&mut chain, 26, block(vec![b]));
            fill_to(&mut chain, 30);
            let node = tachyon_pir_e2e_tests::node::start_node(chain).await;
            let publisher = PublisherHost::start_products(
                publisher_binary(),
                &options().publish,
                &node.url(),
                EPOCH_LENGTH,
                50,
                wait_limit(&budget),
            )
            .await;
            let memo = publisher.distribution(ProductNames::MEMO);
            let first_contact = publisher.distribution(ProductNames::FIRST_CONTACT);
            assert_eq!(
                publisher.listen_of(ProductNames::MEMO),
                publisher.listen_of(ProductNames::FIRST_CONTACT),
                "memo and first-contact share one listener (spec 11.7)"
            );
            // Rounds grow the chain to tips 30, 62, 126 (epoch 0 seals at
            // anchor 123) and 127.
            let mut generations = Vec::new();
            let mut fc_generations = Vec::new();
            for (round, next_tip) in [62u64, 126, 127, 128].into_iter().enumerate() {
                let oracle = oracle(&node.chain());
                let anchor = oracle.anchor();
                let publication = memo
                    .wait_anchor(anchor, oracle.block_hash(anchor), wait_limit(&budget))
                    .await;
                assert!(publication.eligible, "round {round}: not eligible");
                let fc = first_contact
                    .wait_anchor(anchor, oracle.block_hash(anchor), wait_limit(&budget))
                    .await;
                fc_generations.push(fc.inventory.generation);
                let (head, scopes) = memo.chain_listing().await.expect("a memo map");
                assert_eq!(head.generation, publication.inventory.generation);
                check_head(&head, &oracle, anchor);
                let listed: Vec<&Scope> = scopes.iter().collect();
                check_listing(&listed, &oracle, anchor);
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
                let expected = oracle.scopes(anchor);
                let total: u64 = expected.iter().map(|s| s.entries).sum();
                let open: u64 = expected
                    .iter()
                    .filter(|s| s.state == State::Open)
                    .map(|s| s.entries)
                    .sum();
                for (series, value) in [
                    ("tachyon_memo_anchor_height", anchor),
                    ("tachyon_memo_entries_total", total),
                    ("tachyon_memo_open_scope_entries", open),
                ] {
                    wait_publisher_metric(&publisher, series, value, &budget).await;
                }
                check_envelope_gauges(&publisher, &oracle, &budget).await;
                if round == 2 {
                    assert_eq!(
                        oracle.scope(0, anchor).unwrap().state,
                        State::Sealed,
                        "epoch 0 sealed at anchor 124"
                    );
                }
                // Grow the chain, a follow-up block in the newest epoch first.
                let mut chain = node.chain();
                let tip = chain.tip_height();
                let b = fus(&mut chain, 2);
                put(&mut chain, tip + 1, block(vec![b]));
                fill_to(&mut chain, next_tip);
            }
            for list in [&generations, &fc_generations] {
                assert!(
                    list.windows(2).all(|w| w[0] < w[1]),
                    "generations must strictly increase: {list:?}"
                );
            }
        })
        .await
}

/// Scenarios 2, 4 and 5: cold bootstrap over TLS from an empty state
/// directory, then n of n exact answers, hits and misses (a hit returns
/// every entry for the key), and repeated tags returned in ordinal order:
/// across transactions and blocks, twice in one envelope, across scopes, and
/// a `key16` collision.
#[tokio::test(flavor = "multi_thread")]
async fn cold_bootstrap_exact_answers_and_duplicate_tags() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (mut chain, repeats) = standard_chain(1);
            let fresh = fresh_keys(&mut chain, 4);
            let oracle = oracle(&chain);
            let anchor = oracle.anchor();
            let mut t = Topology::launch(
                chain,
                options(),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            t.distribution()
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
                .inject(Target::ProductObjects("memo".into()), Fault::Hold);
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
            let legacy = Client::with_mount(&t.query.origin(), Product::Memo, None, Mount::Legacy)
                .unwrap()
                .fetch_map()
                .await
                .expect("the legacy mount serves the map");
            assert_eq!(legacy.map_sha256(), map.map_sha256());

            // Scenario 4: every key of every scope with entries, and misses.
            let all = probes(&oracle, anchor, &fresh);
            let mut tally = check_lookups(&wallet, &map, &oracle, &all, PARALLEL).await;
            let keys: usize = [0, 1, 9]
                .iter()
                .map(|e| oracle.keys(*e, anchor).len())
                .sum();
            let hits: Vec<&Probe> = all
                .iter()
                .filter(|p| !oracle.expected(p.index, &p.key16, anchor).is_empty())
                .collect();
            assert!(hits.len() >= keys, "every covered key is probed");
            assert_eq!(tally.found, hits.len(), "every covered key was found");
            let returned: usize = hits
                .iter()
                .map(|p| oracle.expected(p.index, &p.key16, anchor).len())
                .sum();
            assert_eq!(tally.entries, returned, "every covered entry was returned");
            assert!(tally.absent >= 30, "{tally:?}");
            // Above the anchor: not yet covered, so absent.
            let above: Vec<Probe> = oracle
                .entries_at(anchor + 1..)
                .iter()
                .filter(|e| e.tag != repeats.e)
                .map(|e| Probe {
                    index: 9,
                    key16: e.key16(),
                })
                .collect();
            assert_eq!(above.len(), 3 + 2);
            tally += check_lookups(&wallet, &map, &oracle, &above, PARALLEL).await;
            // A scope without tables answers absent without a query.
            let before = wallet.stats().queries;
            let empty = wallet
                .lookup(&map, ScopeSelector::Index(5), &fresh[0])
                .await
                .unwrap();
            assert!(matches!(empty, LookupOutcome::Absent { .. }));
            assert_eq!(wallet.stats().queries, before);

            // Scenario 5: repeated tags, every entry in ordinal order.
            let lookup = |index: u64, tag: [u8; 32]| {
                let (wallet, map) = (wallet.clone(), map.clone());
                async move {
                    let outcome = wallet
                        .lookup(&map, ScopeSelector::Index(index), &key16(&tag))
                        .await
                        .unwrap_or_else(|e| panic!("lookup: {}", describe(&e)));
                    match &outcome {
                        LookupOutcome::Found { entries, .. } => entries.clone(),
                        other => panic!("repeated tag in scope {index}: {other:?}"),
                    }
                }
            };
            let d0 = oracle.expected(0, &key16(&repeats.d), anchor);
            assert_eq!(
                d0.iter().map(|e| e.ordinal()).collect::<Vec<_>>(),
                [(12, 2, 0), (12, 3, 1), (12, 3, 3)],
                "authored: two transactions, twice in the second envelope"
            );
            let served = lookup(0, repeats.d).await;
            assert_eq!(served, d0.iter().map(Served::entry).collect::<Vec<_>>());
            let d1 = oracle.expected(1, &key16(&repeats.d), anchor);
            assert_eq!(d1.iter().map(|e| e.height).collect::<Vec<_>>(), [31]);
            assert_eq!(
                lookup(1, repeats.d).await,
                d1.iter().map(Served::entry).collect::<Vec<_>>(),
                "another scope holds only its own entry"
            );
            let e9 = oracle.expected(9, &key16(&repeats.e), anchor);
            assert_eq!(
                e9.iter().map(|e| e.ordinal()).collect::<Vec<_>>(),
                [(155, 2, 0), (155, 3, 0), (155, 4, 1), (158, 2, 4)],
                "three transactions of one block and the anchor block; not block 159"
            );
            assert_eq!(
                lookup(9, repeats.e).await,
                e9.iter().map(Served::entry).collect::<Vec<_>>()
            );
            let collided = oracle.expected(1, &key16(&repeats.collide), anchor);
            assert_eq!(collided.len(), 2, "a key16 collision is a repeated key");
            assert_ne!(collided[0].tag, collided[1].tag);
            assert_eq!(
                lookup(1, repeats.collide).await,
                collided.iter().map(Served::entry).collect::<Vec<_>>()
            );
            eprintln!("exact answers: {tally:?} in {:?}", budget.elapsed());
        })
        .await
}

/// Scenario 3: after the chain grows, the query host fetches only objects it
/// does not already hold (never one of the previous publication, never a
/// sealed table), the sealed epochs keep their session ids, and those
/// sessions still answer under the old map as under the new one.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_publication_fetches_only_changed_objects() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (mut chain, _) = standard_chain(3);
            let fresh = fresh_keys(&mut chain, 2);
            let before = oracle(&chain);
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
                let b = fus(chain, 3);
                put(chain, 161, block(vec![b]));
                let b = fus(chain, 2);
                put(chain, 162, block(vec![b]));
                fill_to(chain, 164);
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
            assert!(
                fetched.iter().all(|path| path.starts_with(prefix)),
                "the memo host fetched another product's objects: {fetched:?}"
            );
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
                "the open epoch's table changed"
            );
            for path in &fetched {
                assert_eq!(t.edge.count(path), 1, "{path} fetched more than once");
            }

            for sealed in [0, 1] {
                assert_eq!(chain_scope(&new_map, sealed).state, ScopeState::Sealed);
                assert_eq!(
                    sessions(&old_map, sealed),
                    sessions(&new_map, sealed),
                    "sealed epoch {sealed} kept its sessions"
                );
            }
            assert_ne!(sessions(&old_map, 9), sessions(&new_map, 9));

            // Sealed sessions answer under the old map ..
            let sealed_probes: Vec<Probe> = probes(&before, before.anchor(), &fresh)
                .into_iter()
                .filter(|p| p.index < 2)
                .collect();
            let tally = check_lookups(&wallet, &old_map, &before, &sealed_probes, PARALLEL).await;
            assert_eq!(
                tally.found,
                before.keys(0, before.anchor()).len() + before.keys(1, before.anchor()).len()
            );
            // .. and every scope answers under the new one.
            let mut tally = check_lookups(
                &wallet,
                &new_map,
                &after,
                &scope_probes(&after, 9, after.anchor()),
                PARALLEL,
            )
            .await;
            assert_eq!(
                tally.entries,
                after.scope_entries(9, after.anchor()).len(),
                "the grown epoch's new entries are served"
            );
            tally += check_lookups(
                &wallet,
                &new_map,
                &after,
                &sample(probes(&after, after.anchor(), &fresh), 16, 3),
                PARALLEL,
            )
            .await;
            assert!(tally.found > 20 && tally.absent > 3, "{tally:?}");
        })
        .await
}

/// Scenario 6: follow-up tags ground so that every seed overflows one row
/// force an extra segment; every entry remains retrievable, the overflowing
/// row's entries split between the segments in ordinal order, and a key
/// repeated across both segments returns all its entries in ordinal order.
#[tokio::test(flavor = "multi_thread")]
async fn forced_overflow_keeps_every_entry_retrievable() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let product = Product::Memo;
            let slots = Family::Memo.slots();
            assert_eq!(slots, product.slots());
            let mut chain = SyntheticChain::new(filler_config(6));
            let genesis = BlockHash::from_protocol_bytes(chain.genesis_hash());
            let groups = grind_overflow(product, &genesis, 0);
            let ground: BTreeSet<[u8; 16]> = groups.iter().flatten().copied().collect();
            assert_eq!(ground.len(), groups.len() * (slots + 1));
            // A ground key of the seed-0 group, repeated at the start and the
            // end of the epoch's entries.
            let repeated = groups[0][1];
            let mut tags: Vec<[u8; 32]> = Vec::new();
            for key in &ground {
                tags.push(tag_with_key16(chain.rng(), *key));
            }
            let repeat = |chain: &mut SyntheticChain| {
                let tag = tag_with_key16(chain.rng(), repeated);
                fu(chain, tag)
            };
            let mut entries = vec![repeat(&mut chain), repeat(&mut chain)];
            for tag in tags {
                entries.push(fu(&mut chain, tag));
            }
            entries.push(repeat(&mut chain));
            entries.push(repeat(&mut chain));
            fill_to(&mut chain, 8);
            // Epoch 0 (heights 8-23): four blocks of ground follow-ups.
            let quarter = entries.len().div_ceil(4);
            let chunks: Vec<Vec<AuthoredEntry>> =
                entries.chunks(quarter).map(|c| c.to_vec()).collect();
            for chunk in chunks {
                let spec = chain.entries_block(chunk);
                let height = chain.tip_height() + 1;
                put(&mut chain, height, spec);
            }
            let b = fus(&mut chain, 6);
            put(&mut chain, 15, block(vec![b]));
            fill_to(&mut chain, 30);
            let fresh = fresh_keys(&mut chain, 6);
            let oracle = oracle(&chain);
            let anchor = oracle.anchor();
            let records = oracle.scope_entries(0, anchor);
            assert_eq!(records.len(), ground.len() + 4 + 6);

            // The row that overflows under seed 0, the seed buckets and
            // segment 0 are assigned under when no seed fits (spec 6.4).
            let row0 = |key: &[u8; 16]| row_under_seed0(product, &genesis, 0, key);
            let hot = row0(&groups[0][0]);
            // The hot row's entries in ordinal order: the first SLOTS stay in
            // segment 0, the rest spill to segment 1 (spec 6.4).
            let hot_entries: Vec<_> = records.iter().filter(|r| row0(&r.key16()) == hot).collect();
            let spilled = hot_entries.len() - slots;
            let segment_of = |ordinal| {
                let rank = hot_entries
                    .iter()
                    .position(|r| r.ordinal() == ordinal)
                    .expect("a hot-row entry");
                usize::from(rank >= slots)
            };

            let t = Topology::start(
                chain,
                options(),
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

            // Every key of the overflowing row, a sample of the others, and misses.
            let hot_keys: BTreeSet<[u8; 16]> = hot_entries.iter().map(|r| r.key16()).collect();
            let others: Vec<Probe> = oracle
                .keys(0, anchor)
                .into_iter()
                .filter(|k| !hot_keys.contains(k))
                .map(|key16| Probe { index: 0, key16 })
                .collect();
            let mut lookups_to_make = key_probes(0, hot_keys.iter().copied());
            lookups_to_make.extend(Sampler::new(6).sample(&others, 60));
            lookups_to_make.extend(key_probes(0, fresh.iter().copied()));
            let outcomes = lookups(&wallet, &map, &lookups_to_make, 2 * PARALLEL).await;
            let mut tally = Tally::default();
            let mut in_segment = [0usize; 2];
            for (probe, outcome) in &outcomes {
                let outcome = outcome
                    .as_ref()
                    .unwrap_or_else(|e| panic!("lookup failed: {}", describe(e)));
                tally += judge(&map, &oracle, anchor, *probe, outcome);
                let answer = outcome.answer().unwrap();
                assert_eq!(
                    answer.queries.len(),
                    2,
                    "every segment of the bucket is queried"
                );
                if hot_keys.contains(&probe.key16) {
                    let mut wanted = [0usize; 2];
                    for entry in oracle.expected(0, &probe.key16, anchor) {
                        wanted[segment_of(entry.ordinal())] += 1;
                    }
                    let matches: Vec<usize> = answer.queries.iter().map(|q| q.matches).collect();
                    assert_eq!(
                        matches,
                        wanted.to_vec(),
                        "key {} found in the wrong segments",
                        hex::encode(probe.key16)
                    );
                    in_segment[0] += wanted[0];
                    in_segment[1] += wanted[1];
                }
            }
            assert_eq!(tally.found, hot_keys.len() + 60);
            assert_eq!(tally.absent, fresh.len());
            assert_eq!(
                in_segment,
                [slots, spilled],
                "the overflowing row fills segment 0 and spills the rest"
            );
            let repeated_entries = oracle.expected(0, &repeated, anchor);
            assert_eq!(repeated_entries.len(), 5);
            let split: Vec<usize> = repeated_entries
                .iter()
                .map(|e| segment_of(e.ordinal()))
                .collect();
            assert_eq!(
                (split[..2].to_vec(), split[3..].to_vec()),
                (vec![0, 0], vec![1, 1]),
                "the repeated key straddles both segments: {split:?}"
            );
            wait_publisher_metric(
                &t.publisher,
                "tachyon_memo_segments_overflow_total",
                1,
                &budget,
            )
            .await;
        })
        .await
}

/// Scenario 7: every malformation the fixtures author, and hand-authored
/// invalid envelopes whose other entries are well formed, sit among valid
/// envelopes in the same blocks. The invalid ones are skipped whole and
/// counted (`tachyon_memo_envelopes_invalid_total` over the stored blocks
/// only), none of their entries is served, a first-contact entry is not a
/// memo entry, every valid entry of those blocks is served, and ingestion
/// continues as more invalid envelopes arrive.
#[tokio::test(flavor = "multi_thread")]
async fn invalid_envelopes_are_skipped_and_counted() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = SyntheticChain::new(filler_config(7));
            let (blocks, poisoned, first_contacts) = invalid_blocks(&mut chain);
            fill_to(&mut chain, 9);
            for (height, spec) in [10u64, 11, 14, 17].into_iter().zip(blocks) {
                put(&mut chain, height, spec);
            }
            fill_to(&mut chain, 28);
            // Two more blocks with invalid envelopes, above the anchor.
            put(
                &mut chain,
                29,
                BlockSpec::new()
                    .tx(TxSpec::tachyon(1).malformed_memo(Malformation::BadMagic))
                    .tx(TxSpec::tachyon(1).malformed_memo(Malformation::CountZero)),
            );
            put(
                &mut chain,
                30,
                BlockSpec::new().tx(TxSpec::tachyon(1).malformed_memo(Malformation::RandomGarbage)),
            );
            let fresh = fresh_keys(&mut chain, 3);
            let before = oracle(&chain);
            let counts = before.envelope_counts(before.anchor());
            let kinds: BTreeSet<String> = chain
                .blocks()
                .iter()
                .flat_map(|b| b.tachyon_txs().collect::<Vec<_>>())
                .filter_map(|tx| tx.authored_memo.malformation())
                .map(|m| format!("{m:?}"))
                .collect();
            assert_eq!(
                kinds.len(),
                Malformation::ALL.len(),
                "every malformation is on the chain: {kinds:?}"
            );
            assert!(counts.valid >= 6 && counts.malformed >= 21, "{counts:?}");

            let t = Topology::start(
                chain,
                options(),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let wallet = t.query.wallet();
            let map = wait_caught_up(&wallet, &before, &budget).await;
            let (invalid, above) = check_envelope_gauges(&t.publisher, &before, &budget).await;
            assert!(
                above >= 3,
                "invalid envelopes above the anchor are not stored yet"
            );
            eprintln!("invalid envelopes stored: {invalid}, above the anchor: {above}");

            // Every valid entry of those blocks, and none of the invalid ones.
            let valid = scope_probes(&before, 0, before.anchor());
            let tally = check_lookups(&wallet, &map, &before, &valid, PARALLEL).await;
            assert_eq!(
                tally.entries,
                before.scope_entries(0, before.anchor()).len()
            );
            let mut misses = key_probes(0, poisoned.iter().copied());
            misses.extend(key_probes(0, first_contacts.iter().copied()));
            misses.extend(key_probes(0, fresh.iter().copied()));
            let tally = check_lookups(&wallet, &map, &before, &misses, PARALLEL).await;
            assert_eq!(tally.absent, misses.len(), "{tally:?}");
            assert!(poisoned.len() >= 25, "{} poisoned keys", poisoned.len());

            // Ingestion continues: more blocks, invalid and valid envelopes.
            let mut late_poisoned = Vec::new();
            let after = edit_chain(&t, |chain| {
                let (blocks, poisoned, _) = invalid_blocks(chain);
                late_poisoned = poisoned;
                for (height, spec) in [33u64, 35, 37, 39].into_iter().zip(blocks) {
                    put(chain, height, spec);
                }
                fill_to(chain, 42);
            });
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            let (later, _) = check_envelope_gauges(&t.publisher, &after, &budget).await;
            assert!(later >= invalid + 12, "{invalid} then {later}");
            let probes1 = scope_probes(&after, 1, after.anchor());
            let tally = check_lookups(&wallet, &new_map, &after, &probes1, PARALLEL).await;
            assert_eq!(tally.entries, after.scope_entries(1, after.anchor()).len());
            assert!(tally.found >= 8, "{tally:?}");
            let tally = check_lookups(
                &wallet,
                &new_map,
                &after,
                &key_probes(1, late_poisoned.iter().copied()),
                PARALLEL,
            )
            .await;
            assert_eq!(tally.absent, late_poisoned.len());
            let url = format!("http://{}/ready", t.publisher.management());
            tachyon_pir_e2e_tests::client::get(&tachyon_pir_e2e_tests::client::plain(), &url)
                .await
                .assert_status(StatusCode::OK, &url);
        })
        .await
}

/// Four blocks mixing valid envelopes (follow-ups, a first contact, wallet
/// states, a repeated tag) with every [`Malformation`] the fixtures author
/// and invalid envelopes authored here whose other entries are well formed.
/// Returns the blocks, the `key16`s of the follow-ups inside invalid
/// envelopes (never served), and of the valid first contacts (not memos).
fn invalid_blocks(chain: &mut SyntheticChain) -> (Vec<BlockSpec>, Vec<[u8; 16]>, Vec<[u8; 16]>) {
    let mut poisoned: Vec<[u8; 16]> = Vec::new();
    let mut first_contacts = Vec::new();
    let mut poison = |chain: &mut SyntheticChain, n: usize| {
        let entries = fus(chain, n);
        poisoned.extend(entries.iter().map(AuthoredEntry::key16));
        entries
    };
    let bad = |malformation: Malformation, bytes: Vec<u8>| {
        TxSpec::tachyon(1).malformed_bytes(MalformedMemo {
            malformation,
            bytes,
        })
    };
    let valid =
        |entries: Vec<AuthoredEntry>| TxSpec::tachyon(1).envelope(&AuthoredEnvelope::new(entries));
    let zero = |chain: &mut SyntheticChain| tag_with_key16(chain.rng(), [0; 16]);

    // Block 1.
    let fc = entry(chain, EntryKind::FirstContact);
    first_contacts.push(fc.key16());
    let mut v1 = fus(chain, 2);
    v1.push(entry(chain, EntryKind::WalletState));
    v1.push(fc);
    let mut trailing = AuthoredEnvelope::new(poison(chain, 2)).to_bytes();
    trailing.extend_from_slice(&[0x00, 0x5a]);
    let b1 = BlockSpec::new()
        .tx(valid(v1))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::BadMagic))
        .tx(TxSpec::tachyon(1))
        .tx(valid(fus(chain, 2)))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::MissingCount))
        .tx(bad(Malformation::TrailingBytes, trailing));

    // Block 2.
    let mut zero_key = poison(chain, 1);
    let ztag = zero(chain);
    zero_key.push(fu(chain, ztag));
    let b2 = BlockSpec::new()
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::CountZero))
        .tx(valid(fus(chain, 1)))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::CountAboveMax))
        .tx(bad(
            Malformation::FollowUpZeroKey,
            AuthoredEnvelope::new(zero_key).to_bytes(),
        ))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::CountExceedsEntries));

    // Block 3.
    let mut state_too_long = poison(chain, 1);
    let stag = random_tag(chain.rng());
    state_too_long.push(AuthoredEntry::random_sized(
        chain.rng(),
        EntryKind::WalletState,
        stag,
        1025,
    ));
    let mut v4 = fus(chain, 1);
    v4.push(entry(chain, EntryKind::WalletState));
    let b3 = BlockSpec::new()
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::UnknownKind))
        .tx(valid(v4))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::TruncatedBody))
        .tx(bad(
            Malformation::StateTooLong,
            AuthoredEnvelope::new(state_too_long).to_bytes(),
        ))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::TrailingBytes))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::StateTooLong));

    // Block 4.
    let mut fc_zero = poison(chain, 1);
    let ftag = zero(chain);
    fc_zero.push(AuthoredEntry::random_with_tag(
        chain.rng(),
        EntryKind::FirstContact,
        ftag,
    ));
    let repeated = fresh_tag(chain);
    let v5 = vec![fu(chain, repeated), fu(chain, repeated)];
    let mut truncated = AuthoredEnvelope::new(poison(chain, 2)).to_bytes();
    truncated.pop();
    let mut unknown = AuthoredEnvelope::new(poison(chain, 2)).to_bytes();
    // The second entry's kind byte: magic, count, then a 161-byte follow-up.
    assert_eq!(unknown[5 + 161], 0x01);
    unknown[5 + 161] = 0x04;
    let mut overcount = AuthoredEnvelope::new(poison(chain, 1)).to_bytes();
    overcount[4] = 2;
    let mut magic = AuthoredEnvelope::new(poison(chain, 1)).to_bytes();
    magic[3] = b'2';
    let seventeen = AuthoredEnvelope::new(poison(chain, 17)).to_bytes();
    let b4 = BlockSpec::new()
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::FollowUpZeroKey))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::FirstContactZeroKey))
        .tx(bad(
            Malformation::FirstContactZeroKey,
            AuthoredEnvelope::new(fc_zero).to_bytes(),
        ))
        .tx(valid(v5))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::RandomGarbage))
        .tx(bad(Malformation::TruncatedBody, truncated))
        .tx(bad(Malformation::UnknownKind, unknown))
        .tx(bad(Malformation::CountExceedsEntries, overcount))
        .tx(bad(Malformation::BadMagic, magic))
        .tx(bad(Malformation::CountAboveMax, seventeen))
        .tx(valid(fus(chain, 3)));
    (vec![b1, b2, b3, b4], poisoned, first_contacts)
}

/// Scenario 8: a reorg inside the open newest epoch gives a new generation
/// in the same recovery epoch; sealed sessions are kept; the new map's anchor
/// is the node's, the old one's no longer is; answers match the new chain,
/// including absence of the entries reorged away and a repeated key that
/// lost some of its entries.
#[tokio::test(flavor = "multi_thread")]
async fn shallow_reorg_answers_the_new_chain() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (mut chain, repeats) = standard_chain(8);
            let fresh = fresh_keys(&mut chain, 3);
            let before = oracle(&chain);
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

            let hold = hold_next_pass(&t, &budget).await;
            let after = edit_chain(&t, |chain| {
                chain.reorg(155, Vec::new()).expect("fork below the tip");
                let b = fus(chain, 5);
                put(chain, 157, block(vec![b]));
                let b1 = fus(chain, 4);
                let b2 = fus(chain, 3);
                put(chain, 159, block(vec![b1, b2]));
                let b = fus(chain, 6);
                put(chain, 160, block(vec![b]));
                fill_to(chain, 162);
            });
            release_after_publication(&t, hold, &after, &budget).await;
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

            // Entries of blocks 156-160 are gone; repeated key E keeps only
            // its block-155 entries.
            let gone: Vec<Probe> = before
                .entries_at(156..)
                .iter()
                .filter(|e| e.tag != repeats.e)
                .map(|e| Probe {
                    index: 9,
                    key16: e.key16(),
                })
                .collect();
            assert_eq!(gone.len(), 4 + 3 + 2);
            let mut tally = check_lookups(&wallet, &new_map, &after, &gone, PARALLEL).await;
            assert_eq!(tally.absent, gone.len());
            assert_eq!(
                after.expected(9, &key16(&repeats.e), 160).len(),
                3,
                "only block 155's entries remain"
            );
            tally += check_lookups(
                &wallet,
                &new_map,
                &after,
                &scope_probes(&after, 9, 160),
                PARALLEL,
            )
            .await;
            tally += check_lookups(
                &wallet,
                &new_map,
                &after,
                &sample(probes(&after, 160, &fresh), 16, 8),
                PARALLEL,
            )
            .await;
            assert!(tally.entries >= 4 + 3 + 3 + 5 + 7 + 6, "{tally:?}");
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
            let (mut chain, _) = standard_chain(9);
            let fresh = fresh_keys(&mut chain, 3);
            let before = oracle(&chain);
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
            let old_probe = |index: u64| Probe {
                index,
                key16: *before.keys(index, before.anchor()).last().unwrap(),
            };
            // Use every old table once, so the client holds its public setup.
            check_lookups(
                &wallet,
                &old_map,
                &before,
                &[old_probe(0), old_probe(1), old_probe(9)],
                PARALLEL,
            )
            .await;
            let revoked_before = query_metric(&t, "sessions_revoked_total").await;

            let hold = hold_next_pass(&t, &budget).await;
            let after = edit_chain(&t, |chain| {
                chain.reorg(30, Vec::new()).expect("fork below the tip");
                for (height, n) in [(33u64, 6usize), (39, 5), (152, 4), (157, 6)] {
                    let b = fus(chain, n);
                    put(chain, height, block(vec![b]));
                }
                fill_to(chain, 165);
            });
            release_after_publication(&t, hold, &after, &budget).await;
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

            for index in [1, 9] {
                let probe = old_probe(index);
                match wallet
                    .lookup(&old_map, ScopeSelector::Index(index), &probe.key16)
                    .await
                {
                    Err(ClientError::SessionRevoked { body, .. }) => {
                        assert_eq!(body.code.as_str(), "session_unavailable");
                    }
                    other => panic!("old epoch {index} session: expected 410, got {other:?}"),
                }
            }
            // Epoch 0's session is in the current publication: it still answers.
            let epoch0 = scope_probes(&before, 0, before.anchor());
            let tally = check_lookups(&wallet, &old_map, &before, &epoch0, PARALLEL).await;
            assert_eq!(tally.found, epoch0.len());
            eprintln!(
                "recovered after {:?}; failed sync passes {}",
                budget.elapsed(),
                query_metric(&t, "sync_passes_total{result=\"failed\"}").await
            );
            wait_for("revoked sessions counted", wait_limit(&budget), || async {
                let revoked = query_metric(&t, "sessions_revoked_total").await;
                if revoked >= revoked_before + 2.0 {
                    Ok(())
                } else {
                    Err(format!("{revoked}"))
                }
            })
            .await;

            // Entries reorged away are absent, the new chain's are found.
            let mut gone: Vec<Probe> = before
                .entries_at(31..=39)
                .iter()
                .map(|e| Probe {
                    index: 1,
                    key16: e.key16(),
                })
                .collect();
            gone.extend(before.entries_at(152..).iter().map(|e| Probe {
                index: 9,
                key16: e.key16(),
            }));
            let gone_absent = gone
                .iter()
                .filter(|p| after.expected(p.index, &p.key16, after.anchor()).is_empty())
                .count();
            // The colliding key of block 39 shares its key16 with a kept
            // entry of block 24: only that one is still found.
            assert_eq!(gone_absent, gone.len() - 1);
            let mut tally = check_lookups(&wallet, &new_map, &after, &gone, PARALLEL).await;
            assert_eq!(tally.absent, gone_absent);
            let new_hits: Vec<Probe> = probes(&after, after.anchor(), &fresh)
                .into_iter()
                .filter(|p| p.index != 0)
                .collect();
            tally += check_lookups(&wallet, &new_map, &after, &new_hits, PARALLEL).await;
            assert!(
                tally.entries
                    >= after.scope_entries(1, after.anchor()).len()
                        + after.scope_entries(9, after.anchor()).len(),
                "{tally:?}"
            );
        })
        .await
}

/// Scenario 10's check that the query host still serves `old_map`, ready,
/// with exact answers at its anchor: the entries the refused publication
/// would add are absent.
async fn previous_served(
    t: &Topology,
    wallet: &Arc<Client>,
    old_map: &MapView,
    before: &MemoOracle,
    fresh: &[[u8; 16]],
    added: &[[u8; 16]],
) {
    let ready = t.query.management_get("ready").await;
    assert_eq!(ready.status, StatusCode::OK, "{}", ready.text());
    let map = wallet.fetch_map().await.expect("the previous map");
    assert_eq!(
        map.map_sha256(),
        old_map.map_sha256(),
        "the previous publication serves"
    );
    let sampled = sample(probes(before, before.anchor(), fresh), 16, 10);
    check_lookups(wallet, &map, before, &sampled, PARALLEL).await;
    let tally = check_lookups(
        wallet,
        &map,
        before,
        &key_probes(9, added.iter().copied()),
        PARALLEL,
    )
    .await;
    assert_eq!(tally.absent, added.len());
}

/// Scenario 10, for one fault on the new open table's rows object: the
/// object is refused (the pass fails), the query host keeps serving the
/// previous publication with exact answers, and switches once the object
/// arrives intact.
///
/// The host's next pass is held at `current` while the chain grows, so the
/// fault is in place before the new publication's objects are requested.
/// One refusal per test: the sync manager backs off 10 s after a failed
/// pass, so corruption and truncation are two tests.
async fn refused_object(seed: u64, fault: Fault) {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (mut chain, _) = standard_chain(seed);
            let fresh = fresh_keys(&mut chain, 2);
            let before = oracle(&chain);
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
                .inject(Target::Suffix("/memo/v1/current".into()), Fault::Hold);
            wait_for("a pass held at current", wait_limit(&budget), || async {
                if t.edge.held() > 0 {
                    Ok(())
                } else {
                    Err("none held".to_owned())
                }
            })
            .await;
            let mut added = Vec::new();
            let after = edit_chain(&t, |chain| {
                let b = fus(chain, 5);
                added = b.iter().map(AuthoredEntry::key16).collect();
                put(chain, 162, block(vec![b]));
                fill_to(chain, 164);
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
            previous_served(&t, &wallet, &old_map, &before, &fresh, &added).await;
            let refused = query_metric(&t, failed).await - failed_before;

            t.edge.clear_faults();
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            assert_eq!(chain_head(&new_map).generation, new.inventory.generation);
            assert!(t.edge.count(&rows) >= 2, "fetched again intact");
            let mut probes = sample(probes(&after, after.anchor(), &fresh), 12, seed);
            probes.extend(scope_probes(&after, 9, after.anchor()));
            let tally = check_lookups(&wallet, &new_map, &after, &probes, PARALLEL).await;
            assert!(tally.found >= after.keys(9, after.anchor()).len());
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
            let (mut chain, _) = standard_chain(11);
            let fresh = fresh_keys(&mut chain, 2);
            let oracle = oracle(&chain);
            let options =
                options().query_setting("max_unconfirmed_seconds", json!(WINDOW.as_secs()));
            let t =
                Topology::start(chain, options, publisher_binary(), query_binary(), &budget).await;
            let wallet = t.query.wallet();
            let map = wait_caught_up(&wallet, &oracle, &budget).await;
            let probes = sample(probes(&oracle, oracle.anchor(), &fresh), 24, 11);
            check_lookups(&wallet, &map, &oracle, &probes, PARALLEL).await;

            t.edge.outage().await;
            let down = Instant::now();
            let mut answered = 0;
            let refused_after = loop {
                let probe = probes[answered % probes.len()];
                match wallet
                    .lookup(&map, ScopeSelector::Index(probe.index), &probe.key16)
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
            check_lookups(&wallet, &back, &oracle, &probes, PARALLEL).await;
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
            let (mut chain, _) = standard_chain(12);
            let fresh = fresh_keys(&mut chain, 2);
            let oracle = Arc::new(oracle(&chain));
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
            let probes = sample(probes(&oracle, oracle.anchor(), &fresh), 20, 12);
            check_lookups(&wallet, &map, &oracle, &probes, PARALLEL).await;
            let old = t.publisher.inventory().await.expect("published");
            let prefix = t.prefix();

            // Publisher crash.
            let stop = Arc::new(AtomicBool::new(false));
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
            check_lookups(&wallet, &republished, &oracle, &probes, PARALLEL).await;
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
            stop.store(true, Ordering::Relaxed);
            let seen = prober.await.expect("the prober saw no wrong answer");
            assert!(seen.answers > 0, "{seen:?}");
            eprintln!("publisher restart: {seen:?}");

            // Query host crash.
            t.edge.reset_counts();
            let stop = Arc::new(AtomicBool::new(false));
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
            check_lookups(&wallet, &recovered, &oracle, &probes, PARALLEL).await;
            stop.store(true, Ordering::Relaxed);
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
            let (mut chain, _) = standard_chain(13);
            let fresh = fresh_keys(&mut chain, 2);
            let oracle = Arc::new(oracle(&chain));
            let options = options().query_setting(
                "admission",
                json!({"executing": 1, "waiting": 1, "wait_ms": 200, "per_client": 64}),
            );
            let t =
                Topology::start(chain, options, publisher_binary(), query_binary(), &budget).await;
            let wallet = t.query.wallet();
            let map = wait_caught_up(&wallet, &oracle, &budget).await;
            let probes = sample(probes(&oracle, oracle.anchor(), &fresh), BURST, 13);
            // One lookup per table first, sequentially, so the burst is queries only.
            for index in [0, 1, 9] {
                let probe = probes.iter().find(|p| p.index == index).copied().unwrap();
                check_lookups(&wallet, &map, &oracle, &[probe], 1).await;
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
                            .lookup(&map, ScopeSelector::Index(probe.index), &probe.key16)
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
                check_lookups(&wallet, &map, &oracle, &[*probe], 1).await;
            }
        })
        .await
}
