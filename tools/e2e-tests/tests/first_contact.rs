//! First-contact end-to-end acceptance (spec 13.2 scenarios 1-13).
//!
//! Every test runs the topology of spec 13.1: the in-process node double
//! serving an authored chain, one publisher process with memo and
//! first-contact enabled (one definition, one listener serving both
//! distribution prefixes), the TLS edge mapping both prefixes to that
//! listener and discovery to management, the query-server binary trusting
//! the edge, and the probe client as the wallet. Expected answers and the
//! span structure come only from [`FirstContactOracle`], computed from the
//! authored envelopes with the span rule restated independently.
//!
//! Epochs are 2 blocks long, so a span closes by the 64-epoch rule after 128
//! blocks; the standard chain also closes a span by count (3,106 entries).
use reqwest::StatusCode;
use serde_json::json;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tachyon_fixtures::envelope::{random_tag, tag_with_key16};
use tachyon_fixtures::{
    AuthoredEntry, AuthoredEnvelope, BlockSpec, ChainConfig, EntryKind, EnvelopeMix,
    FirstContactTruth, Malformation, MalformedMemo, SyntheticChain, TxMix, TxSpec,
};
use tachyon_pir_cli::{Client, ClientError, LookupOutcome, MapView, Mount, ScopeSelector};
use tachyon_pir_e2e_tests::budget::Budget;
use tachyon_pir_e2e_tests::memo_checks::{
    Probe, Served, Tally, chain_scope, check_head, check_listing, check_lookups, check_map,
    grind_overflow, judge, key_probes, lookups, probes, row_under_seed0, scope_probes, sessions,
    spawn_prober,
};
use tachyon_pir_e2e_tests::memo_oracle::{
    FC_SPAN_TARGET, Family, FirstContactOracle, MemoOracle, SpanTruth,
};
use tachyon_pir_e2e_tests::oracle::{Sampler, State};
use tachyon_pir_e2e_tests::topology::{
    ProductNames, PublisherHost, Topology, TopologyOptions, chain_head, describe, exclusive,
    metric_value, wait_for, wait_limit, wait_map,
};
use tachyon_pir_e2e_tests::{Fault, Target, binary};
use tachyon_pir_protocol::{BlockHash, Product, Scope, ScopeState};

/// Blocks per epoch on the synthetic network (the smallest the publisher
/// accepts).
const EPOCH_LENGTH: u32 = 2;
/// Lookups in flight at once (the query host's admission allows more).
const PARALLEL: usize = 4;
/// Keys of the count-closed span 1 looked up per test (it holds 3,106).
const SPAN1_SAMPLE: usize = 40;
const FC: ProductNames = ProductNames::FIRST_CONTACT;

fn publisher_binary() -> PathBuf {
    binary!("tachyon-pir-publisher-test")
}

fn query_binary() -> PathBuf {
    binary!("tachyon-pir-query-server-test")
}

/// First-contact under test; the publisher hosts first-contact and memo.
fn options() -> TopologyOptions {
    let options = TopologyOptions::new(FC, EPOCH_LENGTH);
    assert_eq!(
        options.publish,
        [ProductNames::FIRST_CONTACT, ProductNames::MEMO]
    );
    options
}

fn oracle(chain: &SyntheticChain) -> FirstContactOracle {
    FirstContactOracle::new(chain, u64::from(EPOCH_LENGTH))
}

// ---------------------------------------------------------------------------
// Authoring chains.

/// Generated blocks carry pointer stamps and memos that never hold a memo
/// or first-contact entry: empty memos, wallet-state-only envelopes and
/// malformed envelopes of every kind.
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
            envelope_percent: 50,
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

/// A first contact with `tag`.
fn fc(chain: &mut SyntheticChain, tag: [u8; 32]) -> AuthoredEntry {
    AuthoredEntry::random_with_tag(chain.rng(), EntryKind::FirstContact, tag)
}

/// `n` first contacts with fresh tags.
fn fcs(chain: &mut SyntheticChain, n: usize) -> Vec<AuthoredEntry> {
    (0..n)
        .map(|_| AuthoredEntry::random(chain.rng(), EntryKind::FirstContact))
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

fn fill_to(chain: &mut SyntheticChain, height: u64) {
    while chain.tip_height() < height {
        chain.append_random(1);
    }
}

fn put(chain: &mut SyntheticChain, height: u64, spec: BlockSpec) {
    fill_to(chain, height - 1);
    chain.append(spec);
    assert_eq!(chain.tip_height(), height);
}

/// Tags repeated on the standard chain.
#[derive(Clone, Copy, Debug)]
struct Repeats {
    /// In span 0: block 8, two transactions of block 40, twice in the second
    /// one's envelope; and once in span 1 (block 136).
    d: [u8; 32],
    /// In span 2: blocks 140, 180, the anchor block 243 and block 244 above
    /// the anchor.
    e: [u8; 32],
    /// Shares its `key16` with the first entry of block 8.
    collide: [u8; 32],
    /// Whether block 100 carries follow-ups (one memo table).
    follow_ups: bool,
}

/// The standard chain's spans at anchor 243 (tip 245), with 2-block epochs
/// from activation 8 (epoch `e` is heights `8 + 2e` and `9 + 2e`):
///
/// - span 0: epochs 0-63 (heights 8-135), closed by the 64-epoch rule, a
///   handful of entries, entries in its first and last block; sealed from
///   anchor 235;
/// - span 1: epochs 64-65 (heights 136-139), closed by count: 1,504 entries
///   in epoch 64 (below the target, so no boundary at 137) and 1,602 in epoch
///   65, 3,106 in all; sealed from anchor 239;
/// - span 2: epochs 66 on (heights 140-), open, with entries in its first
///   block, at the anchor and above it.
fn standard_chain(seed: u64, tip: u64, follow_ups: bool) -> (SyntheticChain, Repeats) {
    let mut chain = SyntheticChain::new(filler_config(seed));
    let repeats = Repeats {
        d: fresh_tag(&mut chain),
        e: fresh_tag(&mut chain),
        collide: [0; 32],
        follow_ups,
    };
    let mut repeats = repeats;
    let mut first = fcs(&mut chain, 2);
    repeats.collide = tag_with_key16(chain.rng(), first[0].key16());
    first.push(fc(&mut chain, repeats.d));
    put(&mut chain, 8, block(vec![first]));
    grow(&mut chain, &repeats, tip);
    (chain, repeats)
}

/// Appends the standard chain's planned blocks above the tip, up to `tip`.
fn grow(chain: &mut SyntheticChain, repeats: &Repeats, tip: u64) {
    let plan: [u64; 13] = [
        40, 77, 100, 135, 136, 137, 138, 139, 140, 180, 243, 244, 245,
    ];
    for height in plan {
        if height <= chain.tip_height() || height > tip {
            continue;
        }
        let spec = match height {
            40 => {
                let t1 = vec![fc(chain, repeats.d), entry(chain, EntryKind::FirstContact)];
                let mut t2 = fcs(chain, 1);
                t2.push(fc(chain, repeats.d));
                t2.push(entry(chain, EntryKind::WalletState));
                t2.push(fc(chain, repeats.d));
                block(vec![t1, t2])
            }
            100 if repeats.follow_ups => {
                let fus = (0..3).map(|_| entry(chain, EntryKind::FollowUp)).collect();
                block(vec![fcs(chain, 1), fus])
            }
            135 => {
                let mut b = fcs(chain, 2);
                b.push(fc(chain, repeats.collide));
                block(vec![b])
            }
            136 => {
                let mut b = fcs(chain, 3);
                b.push(fc(chain, repeats.d));
                block(vec![b])
            }
            137 => chain.envelope_block(0, 1500, 0),
            139 => chain.envelope_block(0, 1600, 0),
            138 | 245 => block(vec![fcs(chain, 2)]),
            140 | 243 => {
                let mut b = fcs(chain, 3);
                b.push(fc(chain, repeats.e));
                block(vec![b])
            }
            180 => block(vec![vec![fc(chain, repeats.e)], fcs(chain, 2)]),
            244 => {
                let mut b = fcs(chain, 2);
                b.push(fc(chain, repeats.e));
                block(vec![b])
            }
            _ => block(vec![fcs(chain, 3)]),
        };
        put(chain, height, spec);
    }
    fill_to(chain, tip);
}

/// One open span (span 0, heights 8-) with first contacts in its first
/// block, at height 30, at the anchor 58 and above it, tip 60: one table, for
/// the scenario 10 tests, whose 10 s back-off leaves no time for more.
fn open_span_chain(seed: u64) -> SyntheticChain {
    let mut chain = SyntheticChain::new(filler_config(seed));
    for (height, sizes) in [
        (8u64, &[3usize][..]),
        (30, &[2, 2]),
        (58, &[3]),
        (59, &[2]),
        (60, &[1]),
    ] {
        let envelopes = sizes.iter().map(|n| fcs(&mut chain, *n)).collect();
        put(&mut chain, height, block(envelopes));
    }
    chain
}

fn fresh_keys(chain: &mut SyntheticChain, n: usize) -> Vec<[u8; 16]> {
    (0..n).map(|_| key16(&fresh_tag(chain))).collect()
}

fn key16(tag: &[u8; 32]) -> [u8; 16] {
    let mut key = [0; 16];
    key.copy_from_slice(&tag[..16]);
    key
}

/// `(first epoch, closing epoch)` of each span: the shape a test pins.
fn shape(spans: &[SpanTruth]) -> Vec<SpanShape> {
    spans
        .iter()
        .map(|s| (s.first_epoch, s.last_epoch))
        .collect()
}

/// A span's first epoch and closing epoch (`None` while open).
type SpanShape = (u64, Option<u64>);

/// The standard chain's spans at anchors 239 and above.
const STANDARD_SPANS: [SpanShape; 3] = [(0, Some(63)), (64, Some(65)), (66, None)];

// ---------------------------------------------------------------------------
// Checks.

fn edit_chain(t: &Topology, edit: impl FnOnce(&mut SyntheticChain)) -> FirstContactOracle {
    let mut chain = t.node.chain();
    edit(&mut chain);
    oracle(&chain)
}

/// Waits until the query host serves a first-contact map anchored where
/// `oracle` says a caught-up publication is, and checks it, span structure
/// included.
async fn wait_caught_up(
    wallet: &Client,
    oracle: &FirstContactOracle,
    budget: &Budget,
) -> Arc<MapView> {
    let anchor = oracle.anchor();
    let hash = BlockHash::from_protocol_bytes(oracle.block_hash(anchor));
    let map = wait_map(
        wallet,
        &format!("the query host to serve first-contact anchor {anchor}"),
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

async fn query_metric(t: &Topology, series: &str) -> f64 {
    let name = format!("tachyon_first_contact_{series}");
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
            let text = publisher.metric(FC, series).await;
            match text {
                Some(v) if v == value as f64 => Ok(()),
                other => Err(format!("{other:?}")),
            }
        },
    )
    .await;
}

/// Every probe of every span, with span 1 (3,105 keys) sampled.
fn fc_probes(
    oracle: &FirstContactOracle,
    anchor: u64,
    fresh: &[[u8; 16]],
    seed: u64,
) -> Vec<Probe> {
    let all = probes(oracle, anchor, fresh);
    let big: Vec<Probe> = all
        .iter()
        .copied()
        .filter(|p| p.index == 1 && oracle.scope(1, anchor).unwrap().entries > FC_SPAN_TARGET)
        .collect();
    let mut out: Vec<Probe> = all.into_iter().filter(|p| !big.contains(p)).collect();
    out.extend(Sampler::new(seed).sample(&big, SPAN1_SAMPLE));
    out
}

fn sample(probes: Vec<Probe>, k: usize, seed: u64) -> Vec<Probe> {
    Sampler::new(seed).sample(&probes, k)
}

/// Probes in scope `index` for the keys of `entries`.
fn entry_probes(index: u64, entries: &[FirstContactTruth]) -> Vec<Probe> {
    key_probes(index, entries.iter().map(|e| e.key16()))
}

/// Holds the query host's next synchronization pass at `current`, so a
/// chain edit made meanwhile is first published in full: a pass that runs
/// into the publisher's withdrawal fails and backs off 10 s (spec 11.6),
/// which would make the test's duration depend on that race.
async fn hold_next_pass(t: &Topology, budget: &Budget) -> tachyon_pir_e2e_tests::FaultId {
    let hold = t.edge.inject(
        Target::Suffix("/first-contact/v1/current".into()),
        Fault::Hold,
    );
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
    after: &FirstContactOracle,
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

/// Scenario 1, with the span structure: with no query host at all,
/// first-contact (and memo) publications advance as the chain grows; at
/// each anchor the published span listing (scope ids are first epochs,
/// heights, entries, sealed or open) equals the oracle's independently
/// computed spans, through a 64-epoch close, a count close, and both spans
/// sealing exactly when their last block is `SEAL_DEPTH` below the anchor.
#[tokio::test(flavor = "multi_thread")]
async fn publisher_without_consumers_advances_with_span_rule() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (chain, repeats) = standard_chain(101, 100, false);
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
            let first_contact = publisher.distribution(FC);
            let memo = publisher.distribution(ProductNames::MEMO);
            // (tip, expected span shape at anchor tip - 2, sealed spans).
            let rounds: [(u64, Vec<SpanShape>, usize); 4] = [
                (100, vec![(0, None)], 0),
                (150, STANDARD_SPANS.to_vec(), 0),
                (237, STANDARD_SPANS.to_vec(), 1),
                (241, STANDARD_SPANS.to_vec(), 2),
            ];
            let mut generations = Vec::new();
            let mut memo_generations = Vec::new();
            for (round, (tip, spans, sealed)) in rounds.into_iter().enumerate() {
                {
                    let mut chain = node.chain();
                    grow(&mut chain, &repeats, tip);
                }
                let oracle = oracle(&node.chain());
                let anchor = oracle.anchor();
                assert_eq!(anchor, tip - 2);
                assert_eq!(shape(&oracle.spans(anchor)), spans, "round {round}");
                let expected = oracle.scopes(anchor);
                assert_eq!(
                    expected.iter().filter(|s| s.state == State::Sealed).count(),
                    sealed,
                    "round {round}: sealed spans"
                );
                let publication = first_contact
                    .wait_anchor(anchor, oracle.block_hash(anchor), wait_limit(&budget))
                    .await;
                assert!(publication.eligible, "round {round}: not eligible");
                let (head, scopes) = first_contact.chain_listing().await.expect("a map");
                assert_eq!(head.generation, publication.inventory.generation);
                check_head(&head, &oracle, anchor);
                let listed: Vec<&Scope> = scopes.iter().collect();
                check_listing(&listed, &oracle, anchor);
                generations.push(publication.inventory.generation);
                let memo_publication = memo
                    .wait_anchor(anchor, oracle.block_hash(anchor), wait_limit(&budget))
                    .await;
                memo_generations.push(memo_publication.inventory.generation);
                for route in ["health", "ready"] {
                    let url = format!("http://{}/{route}", publisher.management());
                    tachyon_pir_e2e_tests::client::get(
                        &tachyon_pir_e2e_tests::client::plain(),
                        &url,
                    )
                    .await
                    .assert_status(StatusCode::OK, &url);
                }
                let total: u64 = expected.iter().map(|s| s.entries).sum();
                let open: u64 = expected
                    .iter()
                    .filter(|s| s.state == State::Open)
                    .map(|s| s.entries)
                    .sum();
                for (series, value) in [
                    ("tachyon_first_contact_anchor_height", anchor),
                    ("tachyon_first_contact_entries_total", total),
                    ("tachyon_first_contact_open_scope_entries", open),
                ] {
                    wait_publisher_metric(&publisher, series, value, &budget).await;
                }
                eprintln!(
                    "round {round}: anchor {anchor}, spans {:?} after {:?}",
                    shape(&oracle.spans(anchor)),
                    budget.elapsed()
                );
            }
            for list in [&generations, &memo_generations] {
                assert!(
                    list.windows(2).all(|w| w[0] < w[1]),
                    "generations must strictly increase: {list:?}"
                );
            }
        })
        .await
}

/// Scenarios 2, 4 and 5: cold bootstrap over TLS from empty state of one
/// query host serving first-contact and memo; then n of n exact answers,
/// hits and misses, over the three spans (a hit returns every entry for the
/// key; span 1 sampled); lookups by `scope_id` equal lookups by index; and
/// repeated tags returned in ordinal order: across transactions and blocks,
/// twice in one envelope, across spans, and a `key16` collision. The memo
/// namespace of the same host answers its own oracle.
#[tokio::test(flavor = "multi_thread")]
async fn cold_bootstrap_exact_answers_and_duplicate_tags() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (mut chain, repeats) = standard_chain(1, 245, true);
            let fresh = fresh_keys(&mut chain, 4);
            let oracle = oracle(&chain);
            let memo_oracle = MemoOracle::new(&chain, u64::from(EPOCH_LENGTH));
            let anchor = oracle.anchor();
            assert_eq!(shape(&oracle.spans(anchor)), STANDARD_SPANS);
            // Memo retains 64 scopes by default, fewer than the 118 two-block
            // epochs listed; first-contact retains every span.
            let options = options()
                .also_serve(ProductNames::MEMO)
                .query_setting("retain_scopes", json!(256));
            let mut t =
                Topology::launch(chain, options, publisher_binary(), query_binary(), &budget).await;
            t.distribution()
                .wait_anchor(anchor, oracle.block_hash(anchor), wait_limit(&budget))
                .await;
            for product in [FC, ProductNames::MEMO] {
                assert_eq!(
                    std::fs::read_dir(t.query.state_dir_of(product))
                        .unwrap()
                        .count(),
                    0,
                    "the query host starts from an empty state directory"
                );
            }

            let hold = t
                .edge
                .inject(Target::ProductObjects("first-contact".into()), Fault::Hold);
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
            assert!(t.edge.count_matching(&Target::Discovery) > 0);
            t.edge.remove(hold);
            t.query.wait_ready(wait_limit(&budget)).await;

            let map = wallet.fetch_map().await.expect("the map once ready");
            check_map(&map, &oracle, anchor);
            wallet
                .accept_anchor(&map, &t.node.url())
                .await
                .expect("the anchor is the node's");
            let legacy = Client::with_mount(
                &t.query.origin(),
                Product::FirstContact,
                None,
                Mount::Legacy,
            )
            .unwrap()
            .fetch_map()
            .await
            .expect("the legacy mount serves the map");
            assert_eq!(legacy.map_sha256(), map.map_sha256());

            // Scenario 4.
            let all = fc_probes(&oracle, anchor, &fresh, 1);
            let mut tally = check_lookups(&wallet, &map, &oracle, &all, PARALLEL).await;
            let hits: Vec<&Probe> = all
                .iter()
                .filter(|p| !oracle.expected(p.index, &p.key16, anchor).is_empty())
                .collect();
            assert!(
                hits.len()
                    >= oracle.keys(0, anchor).len() + oracle.keys(2, anchor).len() + SPAN1_SAMPLE
            );
            assert_eq!(tally.found, hits.len());
            assert!(tally.absent >= 20, "{tally:?}");
            // Above the anchor: not yet covered, so absent.
            let above: Vec<Probe> = oracle
                .entries_at(anchor + 1..)
                .iter()
                .filter(|e| e.tag != repeats.e)
                .map(|e| Probe {
                    index: 2,
                    key16: e.key16(),
                })
                .collect();
            assert_eq!(above.len(), 2 + 2);
            tally += check_lookups(&wallet, &map, &oracle, &above, PARALLEL).await;
            // A span is addressed by its scope id as well as its index.
            for (index, scope_id) in [(0u64, 0u64), (1, 64), (2, 66)] {
                let key = oracle.keys(index, anchor)[0];
                let by_index = wallet
                    .lookup(&map, ScopeSelector::Index(index), &key)
                    .await
                    .unwrap();
                let by_id = wallet
                    .lookup(&map, ScopeSelector::Id(scope_id), &key)
                    .await
                    .unwrap();
                judge(&map, &oracle, anchor, Probe { index, key16: key }, &by_id);
                assert_eq!(by_id.answer().unwrap().scope_id, scope_id);
                match (&by_index, &by_id) {
                    (
                        LookupOutcome::Found { entries: a, .. },
                        LookupOutcome::Found { entries: b, .. },
                    ) => assert_eq!(a, b),
                    other => panic!("span {index}: {other:?}"),
                }
            }

            // Scenario 5: repeated tags, every entry in ordinal order.
            let found = |outcome: LookupOutcome| match outcome {
                LookupOutcome::Found { entries, .. } => entries,
                other => panic!("a repeated tag: {other:?}"),
            };
            let d0 = oracle.expected(0, &key16(&repeats.d), anchor);
            assert_eq!(
                d0.iter().map(|e| e.ordinal()).collect::<Vec<_>>(),
                [(8, 2, 2), (40, 2, 0), (40, 3, 1), (40, 3, 3)],
                "authored: two blocks, two transactions, twice in one envelope"
            );
            let served = wallet
                .lookup(&map, ScopeSelector::Index(0), &key16(&repeats.d))
                .await
                .unwrap();
            assert_eq!(
                found(served),
                d0.iter().map(Served::entry).collect::<Vec<_>>()
            );
            let d1 = oracle.expected(1, &key16(&repeats.d), anchor);
            assert_eq!(d1.iter().map(|e| e.height).collect::<Vec<_>>(), [136]);
            let served = wallet
                .lookup(&map, ScopeSelector::Index(1), &key16(&repeats.d))
                .await
                .unwrap();
            assert_eq!(
                found(served),
                d1.iter().map(Served::entry).collect::<Vec<_>>()
            );
            let e2 = oracle.expected(2, &key16(&repeats.e), anchor);
            assert_eq!(
                e2.iter().map(|e| e.ordinal()).collect::<Vec<_>>(),
                [(140, 2, 3), (180, 2, 0), (243, 2, 3)],
                "not block 244, above the anchor"
            );
            let served = wallet
                .lookup(&map, ScopeSelector::Index(2), &key16(&repeats.e))
                .await
                .unwrap();
            assert_eq!(
                found(served),
                e2.iter().map(Served::entry).collect::<Vec<_>>()
            );
            let collided = oracle.expected(0, &key16(&repeats.collide), anchor);
            assert_eq!(collided.len(), 2, "a key16 collision is a repeated key");
            assert_ne!(collided[0].tag, collided[1].tag);
            let served = wallet
                .lookup(&map, ScopeSelector::Index(0), &key16(&repeats.collide))
                .await
                .unwrap();
            assert_eq!(
                found(served),
                collided.iter().map(Served::entry).collect::<Vec<_>>()
            );

            // The memo namespace of the same host.
            let memo_wallet = t.query.wallet_for(ProductNames::MEMO);
            let memo_map = memo_wallet.fetch_map().await.expect("the memo map");
            check_map(&memo_map, &memo_oracle, anchor);
            let memo_probes = probes(&memo_oracle, anchor, &fresh);
            let memo_tally = check_lookups(
                &memo_wallet,
                &memo_map,
                &memo_oracle,
                &memo_probes,
                PARALLEL,
            )
            .await;
            assert_eq!(memo_tally.found, 3, "block 100's follow-ups");
            eprintln!(
                "exact answers: {tally:?}, memo {memo_tally:?} in {:?}",
                budget.elapsed()
            );
        })
        .await
}

/// Scenario 3: after the chain grows inside open span 2, the query host
/// fetches only objects it does not already hold, the sealed spans keep
/// their session ids and still answer under the old map as under the new.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_publication_fetches_only_changed_objects() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (mut chain, _) = standard_chain(3, 245, false);
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
                let b = fcs(chain, 3);
                put(chain, 247, block(vec![b]));
                fill_to(chain, 249);
            });
            assert_eq!(shape(&after.spans(after.anchor())), STANDARD_SPANS);
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            let new = t.publisher.inventory().await.expect("published");
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
                "the first-contact host fetched another product's objects: {fetched:?}"
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
            let changed_tables: Vec<&String> = changed
                .iter()
                .filter(|path| new.table_object_paths(prefix).contains(*path))
                .collect();
            assert_eq!(changed_tables.len(), 2, "span 2's rows and public setup");
            for path in &fetched {
                assert_eq!(t.edge.count(path), 1, "{path} fetched more than once");
            }
            for sealed in [0, 1] {
                assert_eq!(chain_scope(&new_map, sealed).state, ScopeState::Sealed);
                assert_eq!(sessions(&old_map, sealed), sessions(&new_map, sealed));
            }
            assert_ne!(sessions(&old_map, 2), sessions(&new_map, 2));

            let sealed_probes: Vec<Probe> = fc_probes(&before, before.anchor(), &fresh, 3)
                .into_iter()
                .filter(|p| p.index < 2)
                .collect();
            let tally = check_lookups(&wallet, &old_map, &before, &sealed_probes, PARALLEL).await;
            assert!(tally.found >= before.keys(0, before.anchor()).len() + SPAN1_SAMPLE);
            let tally = check_lookups(
                &wallet,
                &new_map,
                &after,
                &scope_probes(&after, 2, after.anchor()),
                PARALLEL,
            )
            .await;
            assert_eq!(tally.entries, after.scope_entries(2, after.anchor()).len());
            check_lookups(
                &wallet,
                &new_map,
                &after,
                &sample(fc_probes(&after, after.anchor(), &fresh, 4), 16, 3),
                PARALLEL,
            )
            .await;
        })
        .await
}

/// Scenario 6: first-contact tags ground so that every seed overflows one
/// row (7 keys in a row of 6 slots) force an extra segment; every entry
/// remains retrievable, the row's entries split between the segments in
/// ordinal order, and a key repeated across both segments returns all its
/// entries in ordinal order.
#[tokio::test(flavor = "multi_thread")]
async fn forced_overflow_keeps_every_entry_retrievable() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let product = Product::FirstContact;
            let slots = Family::FirstContact.slots();
            assert_eq!(slots, product.slots());
            let mut chain = SyntheticChain::new(filler_config(6));
            let genesis = BlockHash::from_protocol_bytes(chain.genesis_hash());
            // Span 0's scope id is epoch 0.
            let groups = grind_overflow(product, &genesis, 0);
            let ground: BTreeSet<[u8; 16]> = groups.iter().flatten().copied().collect();
            assert_eq!(ground.len(), groups.len() * (slots + 1));
            let repeated = groups[0][1];
            let repeat = |chain: &mut SyntheticChain| {
                let tag = tag_with_key16(chain.rng(), repeated);
                fc(chain, tag)
            };
            let mut entries = vec![repeat(&mut chain), repeat(&mut chain)];
            for key in &ground {
                let tag = tag_with_key16(chain.rng(), *key);
                entries.push(fc(&mut chain, tag));
            }
            entries.push(repeat(&mut chain));
            entries.push(repeat(&mut chain));
            fill_to(&mut chain, 8);
            let half = entries.len().div_ceil(2);
            let chunks: Vec<Vec<AuthoredEntry>> =
                entries.chunks(half).map(|c| c.to_vec()).collect();
            for chunk in chunks {
                let spec = chain.entries_block(chunk);
                let height = chain.tip_height() + 1;
                put(&mut chain, height, spec);
            }
            let b = fcs(&mut chain, 6);
            put(&mut chain, 15, block(vec![b]));
            fill_to(&mut chain, 30);
            let fresh = fresh_keys(&mut chain, 6);
            let oracle = oracle(&chain);
            let anchor = oracle.anchor();
            assert_eq!(shape(&oracle.spans(anchor)), [(0, None)]);
            let records = oracle.scope_entries(0, anchor);
            assert_eq!(records.len(), ground.len() + 4 + 6);

            let row0 = |key: &[u8; 16]| row_under_seed0(product, &genesis, 0, key);
            let hot = row0(&groups[0][0]);
            let hot_entries: Vec<_> = records.iter().filter(|r| row0(&r.key16()) == hot).collect();
            let spilled = hot_entries.len() - slots;
            let segment_of = |ordinal| {
                let rank = hot_entries
                    .iter()
                    .position(|r| r.ordinal() == ordinal)
                    .expect("a hot-row entry");
                usize::from(rank >= slots)
            };

            // Every ground key is looked up: several clients, with admission
            // wide enough for all of them at once (they share one address, so
            // `per_client` covers them together).
            let options = options().query_setting(
                "admission",
                json!({"executing": 24, "waiting": 64, "wait_ms": 10000, "per_client": 64}),
            );
            let t =
                Topology::start(chain, options, publisher_binary(), query_binary(), &budget).await;
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
            assert_eq!(segments[0].1, 0, "no seed fits: segment 0 under seed 0");
            assert_eq!(segments[1].2, spilled as u64, "the hot row's excess spills");

            let hot_keys: BTreeSet<[u8; 16]> = hot_entries.iter().map(|r| r.key16()).collect();
            // Every key of the span: the hot row's, every other ground key and
            // the other first contacts; and misses.
            let keys = oracle.keys(0, anchor);
            assert!(ground.iter().all(|k| keys.contains(k)));
            let mut wanted_lookups = key_probes(0, hot_keys.iter().copied());
            wanted_lookups.extend(key_probes(
                0,
                keys.iter().copied().filter(|k| !hot_keys.contains(k)),
            ));
            wanted_lookups.extend(key_probes(0, fresh.iter().copied()));
            const CLIENTS: usize = 6;
            let wallets: Vec<_> = (0..CLIENTS).map(|_| t.query.wallet()).collect();
            let shares: Vec<Vec<Probe>> = (0..CLIENTS)
                .map(|c| {
                    wanted_lookups
                        .iter()
                        .copied()
                        .skip(c)
                        .step_by(CLIENTS)
                        .collect()
                })
                .collect();
            let outcomes: Vec<_> = futures_util::future::join_all(
                wallets
                    .iter()
                    .zip(&shares)
                    .map(|(wallet, share)| lookups(wallet, &map, share, 2 * PARALLEL)),
            )
            .await
            .into_iter()
            .flatten()
            .collect();
            assert_eq!(outcomes.len(), wanted_lookups.len());
            let mut tally = Tally::default();
            let mut in_segment = [0usize; 2];
            for (probe, outcome) in &outcomes {
                let outcome = outcome
                    .as_ref()
                    .unwrap_or_else(|e| panic!("lookup failed: {}", describe(e)));
                tally += judge(&map, &oracle, anchor, *probe, outcome);
                let answer = outcome.answer().unwrap();
                assert_eq!(answer.queries.len(), 2, "every segment is queried");
                if hot_keys.contains(&probe.key16) {
                    let mut wanted = [0usize; 2];
                    for entry in oracle.expected(0, &probe.key16, anchor) {
                        wanted[segment_of(entry.ordinal())] += 1;
                    }
                    let matches: Vec<usize> = answer.queries.iter().map(|q| q.matches).collect();
                    assert_eq!(matches, wanted.to_vec(), "key {}", hex::encode(probe.key16));
                    in_segment[0] += wanted[0];
                    in_segment[1] += wanted[1];
                }
            }
            assert_eq!(tally.found, keys.len(), "every key of the span is found");
            assert_eq!(keys.len(), ground.len() + 6);
            assert_eq!(tally.absent, fresh.len());
            assert_eq!(in_segment, [slots, spilled]);
            let split: Vec<usize> = oracle
                .expected(0, &repeated, anchor)
                .iter()
                .map(|e| segment_of(e.ordinal()))
                .collect();
            assert_eq!(split.len(), 5);
            assert_eq!(
                (split[..2].to_vec(), split[3..].to_vec()),
                (vec![0, 0], vec![1, 1]),
                "the repeated key straddles both segments: {split:?}"
            );
            wait_publisher_metric(
                &t.publisher,
                "tachyon_first_contact_segments_overflow_total",
                1,
                &budget,
            )
            .await;
        })
        .await
}

/// Four blocks mixing valid envelopes (first contacts, a few follow-ups in
/// the first two only, wallet states, a repeated tag) with every
/// [`Malformation`] the fixtures author and invalid envelopes authored here
/// whose other entries are well formed first contacts. Returns the blocks,
/// the `key16`s of first contacts inside invalid envelopes (never served),
/// and of the valid follow-ups (not first contacts).
fn invalid_blocks(chain: &mut SyntheticChain) -> (Vec<BlockSpec>, Vec<[u8; 16]>, Vec<[u8; 16]>) {
    let mut poisoned: Vec<[u8; 16]> = Vec::new();
    let mut follow_ups = Vec::new();
    let mut poison = |chain: &mut SyntheticChain, n: usize| {
        let entries = fcs(chain, n);
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
    let fu = entry(chain, EntryKind::FollowUp);
    follow_ups.push(fu.key16());
    let mut v1 = fcs(chain, 2);
    v1.push(entry(chain, EntryKind::WalletState));
    v1.push(fu);
    let mut trailing = AuthoredEnvelope::new(poison(chain, 2)).to_bytes();
    trailing.extend_from_slice(&[0x00, 0x5a]);
    let b1 = BlockSpec::new()
        .tx(valid(v1))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::BadMagic))
        .tx(TxSpec::tachyon(1))
        .tx(valid(fcs(chain, 2)))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::MissingCount))
        .tx(bad(Malformation::TrailingBytes, trailing));

    // Block 2.
    let mut zero_key = poison(chain, 1);
    let ztag = zero(chain);
    zero_key.push(fc(chain, ztag));
    let mut fu_zero = poison(chain, 1);
    let ftag = zero(chain);
    fu_zero.push(AuthoredEntry::random_with_tag(
        chain.rng(),
        EntryKind::FollowUp,
        ftag,
    ));
    let fu = entry(chain, EntryKind::FollowUp);
    follow_ups.push(fu.key16());
    let b2 = BlockSpec::new()
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::CountZero))
        .tx(valid(vec![entry(chain, EntryKind::FirstContact), fu]))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::CountAboveMax))
        .tx(bad(
            Malformation::FirstContactZeroKey,
            AuthoredEnvelope::new(zero_key).to_bytes(),
        ))
        .tx(bad(
            Malformation::FollowUpZeroKey,
            AuthoredEnvelope::new(fu_zero).to_bytes(),
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
    let mut v4 = fcs(chain, 1);
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
    let repeated = fresh_tag(chain);
    let v5 = vec![fc(chain, repeated), fc(chain, repeated)];
    let mut truncated = AuthoredEnvelope::new(poison(chain, 2)).to_bytes();
    truncated.pop();
    let mut unknown_entries = poison(chain, 1);
    unknown_entries.push(entry(chain, EntryKind::FollowUp));
    let mut unknown = AuthoredEnvelope::new(unknown_entries).to_bytes();
    // The second entry's kind byte: magic, count, then a 1,249-byte first contact.
    assert_eq!(unknown[5 + 1249], 0x01);
    unknown[5 + 1249] = 0x04;
    let mut overcount = AuthoredEnvelope::new(poison(chain, 1)).to_bytes();
    overcount[4] = 2;
    let mut magic = AuthoredEnvelope::new(poison(chain, 1)).to_bytes();
    magic[3] = b'2';
    let seventeen = AuthoredEnvelope::new(poison(chain, 17)).to_bytes();
    let b4 = BlockSpec::new()
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::FollowUpZeroKey))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::FirstContactZeroKey))
        .tx(valid(v5))
        .tx(TxSpec::tachyon(1).malformed_memo(Malformation::RandomGarbage))
        .tx(bad(Malformation::TruncatedBody, truncated))
        .tx(bad(Malformation::UnknownKind, unknown))
        .tx(bad(Malformation::CountExceedsEntries, overcount))
        .tx(bad(Malformation::BadMagic, magic))
        .tx(bad(Malformation::CountAboveMax, seventeen))
        .tx(valid(fcs(chain, 3)));
    (vec![b1, b2, b3, b4], poisoned, follow_ups)
}

/// The publisher's invalid-envelope and wallet-state gauges (memo metrics:
/// one parser feeds both products) against the blocks the journal stores.
async fn check_envelope_gauges(
    publisher: &PublisherHost,
    oracle: &FirstContactOracle,
    budget: &Budget,
) -> (usize, usize) {
    let counts = oracle.envelope_counts(oracle.anchor());
    let above = oracle.envelope_counts(oracle.tip());
    for (series, value) in [
        ("tachyon_memo_envelopes_invalid_total", counts.malformed),
        ("tachyon_memo_state_entries_total", counts.wallet_states),
    ] {
        wait_for(
            &format!("the publisher's {series} to be {value}"),
            wait_limit(budget),
            || async {
                let text = publisher.distribution(ProductNames::MEMO).metrics().await;
                match metric_value(&text, series) {
                    Some(v) if v == value as f64 => Ok(()),
                    other => Err(format!("{other:?}")),
                }
            },
        )
        .await;
    }
    assert_eq!(counts.raw, 0);
    (counts.malformed, above.malformed - counts.malformed)
}

/// Scenario 7: invalid envelopes of every kind among valid ones in the same
/// blocks are skipped whole and counted; none of their first contacts is
/// served, follow-ups are not first contacts, every valid first contact of
/// those blocks is served, and ingestion continues.
#[tokio::test(flavor = "multi_thread")]
async fn invalid_envelopes_are_skipped_and_counted() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = SyntheticChain::new(filler_config(7));
            let (blocks, poisoned, follow_ups) = invalid_blocks(&mut chain);
            fill_to(&mut chain, 9);
            // Blocks 10 and 11 (the follow-ups) share memo epoch 1.
            for (height, spec) in [10u64, 11, 14, 17].into_iter().zip(blocks) {
                put(&mut chain, height, spec);
            }
            fill_to(&mut chain, 28);
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
            let kinds: BTreeSet<String> = chain
                .blocks()
                .iter()
                .flat_map(|b| b.tachyon_txs().collect::<Vec<_>>())
                .filter_map(|tx| tx.authored_memo.malformation())
                .map(|m| format!("{m:?}"))
                .collect();
            assert_eq!(kinds.len(), Malformation::ALL.len(), "{kinds:?}");
            assert_eq!(shape(&before.spans(before.anchor())), [(0, None)]);

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
                invalid >= 21 && above >= 3,
                "{invalid} stored, {above} above"
            );

            let valid = scope_probes(&before, 0, before.anchor());
            let tally = check_lookups(&wallet, &map, &before, &valid, PARALLEL).await;
            assert_eq!(
                tally.entries,
                before.scope_entries(0, before.anchor()).len()
            );
            assert!(tally.found >= 8, "{tally:?}");
            let mut misses = key_probes(0, poisoned.iter().copied());
            misses.extend(key_probes(0, follow_ups.iter().copied()));
            misses.extend(key_probes(0, fresh.iter().copied()));
            let tally = check_lookups(&wallet, &map, &before, &misses, PARALLEL).await;
            assert_eq!(tally.absent, misses.len(), "{tally:?}");
            assert_eq!(poisoned.len(), 27, "poisoned first contacts");

            let mut late_poisoned = Vec::new();
            let after = edit_chain(&t, |chain| {
                let (blocks, poisoned, _) = invalid_blocks(chain);
                late_poisoned = poisoned;
                for (height, spec) in [34u64, 35, 37, 39].into_iter().zip(blocks) {
                    put(chain, height, spec);
                }
                fill_to(chain, 42);
            });
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            let (later, _) = check_envelope_gauges(&t.publisher, &after, &budget).await;
            assert!(later >= invalid + 21, "{invalid} then {later}");
            let late = entry_probes(0, &after.entries_at(31..));
            assert_eq!(late.len(), 11, "the new valid first contacts");
            let tally = check_lookups(&wallet, &new_map, &after, &late, PARALLEL).await;
            assert_eq!(tally.found, late.len());
            let tally = check_lookups(
                &wallet,
                &new_map,
                &after,
                &key_probes(0, late_poisoned.iter().copied()),
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

/// Scenario 8: a reorg below the boundary of a closed span that is not yet
/// sealed stays inside open scopes: a new generation in the same recovery
/// epoch. The boundary the removed block decided is gone, so the new chain's
/// spans differ (span 1 no longer closes by count and is now the open
/// newest span, span 2 is gone); span 0 keeps its session; the new anchor is
/// the node's and the old one is not; answers match the new chain.
#[tokio::test(flavor = "multi_thread")]
async fn shallow_reorg_removes_an_unsealed_span_boundary() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (mut chain, _) = standard_chain(8, 200, false);
            let fresh = fresh_keys(&mut chain, 3);
            let before = oracle(&chain);
            assert_eq!(shape(&before.spans(198)), STANDARD_SPANS);
            assert!(
                before.scopes(198).iter().all(|s| s.state == State::Open),
                "no span is sealed yet"
            );
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
                chain.reorg(138, Vec::new()).expect("fork below the tip");
                for (height, n) in [(141u64, 3usize), (160, 2), (199, 4), (200, 2)] {
                    let b = fcs(chain, n);
                    put(chain, height, block(vec![b]));
                }
                fill_to(chain, 202);
            });
            release_after_publication(&t, hold, &after, &budget).await;
            assert_eq!(after.anchor(), 200);
            assert_eq!(shape(&after.spans(200)), [(0, Some(63)), (64, None)]);
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            let (old_head, new_head) = (chain_head(&old_map), chain_head(&new_map));
            assert!(new_head.generation > old_head.generation);
            assert_eq!(
                new_head.recovery_epoch, old_head.recovery_epoch,
                "a rewind inside open scopes does not recover (spec 11.4)"
            );
            assert_eq!(sessions(&old_map, 0), sessions(&new_map, 0));
            assert_ne!(sessions(&old_map, 1), sessions(&new_map, 1));
            wallet
                .accept_anchor(&new_map, &t.node.url())
                .await
                .expect("the new anchor is the node's");
            match wallet.accept_anchor(&old_map, &t.node.url()).await {
                Err(ClientError::AnchorMismatch { what: "anchor", .. }) => {}
                other => panic!("the reorged-away anchor was accepted: {other:?}"),
            }

            // Old entries above the fork (block 139's 1,600 sampled, and old
            // span 2's) are absent from the new span 1.
            let mut gone =
                Sampler::new(8).sample(&entry_probes(1, &before.entries_at(139..=139)), 20);
            gone.extend(entry_probes(1, &before.entries_at(140..)));
            let mut tally = check_lookups(&wallet, &new_map, &after, &gone, PARALLEL).await;
            assert_eq!(tally.absent, gone.len());
            // Kept entries of blocks 136-138 and the new ones are found.
            let kept = entry_probes(1, &after.entries_at(136..=138));
            let kept = [
                Sampler::new(9).sample(&kept, 20),
                entry_probes(1, &after.entries_at(139..)),
            ]
            .concat();
            tally += check_lookups(&wallet, &new_map, &after, &kept, PARALLEL).await;
            assert!(tally.found >= 20 + 11, "{tally:?}");
            tally += check_lookups(
                &wallet,
                &new_map,
                &after,
                &sample(fc_probes(&after, 200, &fresh, 8), 16, 8),
                PARALLEL,
            )
            .await;
            eprintln!("shallow reorg: {tally:?}");
        })
        .await
}

/// Scenario 9: a reorg into sealed span 1 (closed by count, its last block
/// `SEAL_DEPTH` below the anchor) increments the recovery epoch; the
/// sessions of the old span 1 and span 2 answer `410`, the untouched sealed
/// span 0 keeps its session and keeps answering, and the new map's spans and
/// answers match the new chain (span 1 no longer closes by count).
#[tokio::test(flavor = "multi_thread")]
async fn reorg_into_sealed_span_recovers_and_revokes() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (mut chain, _) = standard_chain(9, 245, false);
            let fresh = fresh_keys(&mut chain, 3);
            let before = oracle(&chain);
            assert_eq!(before.scope(1, 243).unwrap().state, State::Sealed);
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
            check_lookups(
                &wallet,
                &old_map,
                &before,
                &[old_probe(0), old_probe(1), old_probe(2)],
                PARALLEL,
            )
            .await;
            let revoked_before = query_metric(&t, "sessions_revoked_total").await;

            let hold = hold_next_pass(&t, &budget).await;
            let after = edit_chain(&t, |chain| {
                chain.reorg(137, Vec::new()).expect("fork below the tip");
                for (height, n) in [(140u64, 2usize), (150, 3), (247, 2)] {
                    let b = fcs(chain, n);
                    put(chain, height, block(vec![b]));
                }
                fill_to(chain, 249);
            });
            release_after_publication(&t, hold, &after, &budget).await;
            assert_eq!(shape(&after.spans(247)), [(0, Some(63)), (64, None)]);
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            let (old_head, new_head) = (chain_head(&old_map), chain_head(&new_map));
            assert!(
                new_head.recovery_epoch.get() > old_head.recovery_epoch.get(),
                "a rewind into a sealed span recovers (spec 11.4)"
            );
            assert!(new_head.generation > old_head.generation);
            assert_eq!(sessions(&old_map, 0), sessions(&new_map, 0));
            for index in [1, 2] {
                let probe = old_probe(index);
                match wallet
                    .lookup(&old_map, ScopeSelector::Index(index), &probe.key16)
                    .await
                {
                    Err(ClientError::SessionRevoked { body, .. }) => {
                        assert_eq!(body.code.as_str(), "session_unavailable");
                    }
                    other => panic!("old span {index} session: expected 410, got {other:?}"),
                }
            }
            let span0 = scope_probes(&before, 0, before.anchor());
            let tally = check_lookups(&wallet, &old_map, &before, &span0, PARALLEL).await;
            assert_eq!(tally.found, span0.len());
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

            let mut gone =
                Sampler::new(19).sample(&entry_probes(1, &before.entries_at(138..=139)), 20);
            gone.extend(entry_probes(1, &before.entries_at(140..)));
            let mut tally = check_lookups(&wallet, &new_map, &after, &gone, PARALLEL).await;
            assert_eq!(tally.absent, gone.len());
            let kept = Sampler::new(29).sample(&entry_probes(1, &after.entries_at(136..=137)), 20);
            tally += check_lookups(&wallet, &new_map, &after, &kept, PARALLEL).await;
            tally += check_lookups(
                &wallet,
                &new_map,
                &after,
                &entry_probes(1, &after.entries_at(138..)),
                PARALLEL,
            )
            .await;
            tally += check_lookups(
                &wallet,
                &new_map,
                &after,
                &sample(fc_probes(&after, after.anchor(), &fresh, 9), 16, 9),
                PARALLEL,
            )
            .await;
            assert!(tally.found >= 20 + 7, "{tally:?}");
        })
        .await
}

/// Scenario 10, for one fault on the open span's new rows object: refused,
/// the previous publication keeps serving exact answers, and the host
/// switches once the object arrives intact. The chain has one table, the
/// open span's.
async fn refused_object(seed: u64, fault: Fault) {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut chain = open_span_chain(seed);
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

            let hold = t.edge.inject(
                Target::Suffix("/first-contact/v1/current".into()),
                Fault::Hold,
            );
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
                let b = fcs(chain, 4);
                added = b.iter().map(AuthoredEntry::key16).collect::<Vec<_>>();
                put(chain, 62, block(vec![b]));
                fill_to(chain, 64);
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
            // The previous publication serves, ready, with exact answers.
            let ready = t.query.management_get("ready").await;
            assert_eq!(ready.status, StatusCode::OK, "{}", ready.text());
            let map = wallet.fetch_map().await.expect("the previous map");
            assert_eq!(map.map_sha256(), old_map.map_sha256());
            check_lookups(
                &wallet,
                &map,
                &before,
                &sample(fc_probes(&before, before.anchor(), &fresh, seed), 16, 10),
                PARALLEL,
            )
            .await;
            let tally = check_lookups(
                &wallet,
                &map,
                &before,
                &key_probes(0, added.iter().copied()),
                PARALLEL,
            )
            .await;
            assert_eq!(tally.absent, added.len());

            t.edge.clear_faults();
            let new_map = wait_caught_up(&wallet, &after, &budget).await;
            assert_eq!(chain_head(&new_map).generation, new.inventory.generation);
            assert!(t.edge.count(&rows) >= 2, "fetched again intact");
            let tally = check_lookups(
                &wallet,
                &new_map,
                &after,
                &scope_probes(&after, 0, after.anchor()),
                PARALLEL,
            )
            .await;
            assert_eq!(tally.entries, after.scope_entries(0, after.anchor()).len());
            eprintln!("switched after {:?}", budget.elapsed());
        })
        .await
}

/// Scenario 10, corrupt: the open span's rows object arrives with one bit
/// flipped.
#[tokio::test(flavor = "multi_thread")]
async fn corrupt_object_is_refused_and_previous_publication_serves() {
    refused_object(10, Fault::CorruptBody).await;
}

/// Scenario 10, truncated: the open span's rows object is cut after half its
/// body.
#[tokio::test(flavor = "multi_thread")]
async fn truncated_object_is_refused_and_previous_publication_serves() {
    refused_object(20, Fault::TruncateBody).await;
}

/// Scenario 11: with the publisher unreachable the query host keeps serving
/// exact answers for `max_unconfirmed_seconds`, then answers `503`, and
/// serves again once the publisher is back.
#[tokio::test(flavor = "multi_thread")]
async fn publisher_outage_serves_within_authority_then_503() {
    const WINDOW: Duration = Duration::from_secs(3);
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (mut chain, _) = standard_chain(11, 245, false);
            let fresh = fresh_keys(&mut chain, 2);
            let oracle = oracle(&chain);
            let options =
                options().query_setting("max_unconfirmed_seconds", json!(WINDOW.as_secs()));
            let t =
                Topology::start(chain, options, publisher_binary(), query_binary(), &budget).await;
            let wallet = t.query.wallet();
            let map = wait_caught_up(&wallet, &oracle, &budget).await;
            let probes = sample(fc_probes(&oracle, oracle.anchor(), &fresh, 11), 24, 11);
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
            assert_eq!(back.map_sha256(), map.map_sha256());
            check_lookups(&wallet, &back, &oracle, &probes, PARALLEL).await;
        })
        .await
}

/// Scenario 12: the publisher, then the query host, crash and restart; the
/// publisher republishes the same tables under a new recovery epoch and
/// generation, the query host recovers from its state directory without
/// fetching a table, and a prober running throughout never sees a wrong
/// answer.
#[tokio::test(flavor = "multi_thread")]
async fn restart_of_each_process_recovers_without_wrong_answers() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let (mut chain, _) = standard_chain(12, 245, false);
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
            let probes = sample(fc_probes(&oracle, oracle.anchor(), &fresh, 12), 20, 12);
            check_lookups(&wallet, &map, &oracle, &probes, PARALLEL).await;
            let old = t.publisher.inventory().await.expect("published");
            let prefix = t.prefix();

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
                "the same tables"
            );
            assert_eq!(new.inventory.subject, old.inventory.subject);
            let republished = wait_map(&wallet, "the republished map", wait_limit(&budget), |m| {
                chain_head(m).generation == new.inventory.generation
            })
            .await;
            check_map(&republished, &oracle, oracle.anchor());
            check_lookups(&wallet, &republished, &oracle, &probes, PARALLEL).await;
            for scope in 0..=2 {
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
                "the restarted query host refetched table objects: {refetched:?}"
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
            let (mut chain, _) = standard_chain(13, 245, false);
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
            let probes = sample(fc_probes(&oracle, oracle.anchor(), &fresh, 13), BURST, 13);
            for index in [0, 1, 2] {
                let probe = Probe {
                    index,
                    key16: oracle.keys(index, oracle.anchor())[0],
                };
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
            for probe in probes.iter().take(6) {
                check_lookups(&wallet, &map, &oracle, &[*probe], 1).await;
            }
        })
        .await
}
