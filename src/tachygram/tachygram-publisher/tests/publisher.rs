//! The tachygram publisher against the node double, driven in-process with
//! `pir_pipeline::ingestion::catch_up` and `PublisherEngine`. The oracle is
//! always the fixture's authored chain.
mod common;

use common::*;
use pir_pipeline::{
    FailurePolicy,
    engine::PublisherEngine,
    ingestion::{ChainFeed, Ingestion, IngestionExecutor},
    service::Shutdown,
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tachygram_publisher::{Error, Tachygram};
use tachyon_fixtures::{ChainConfig, NodeDouble};
use tachyon_pir_protocol::{ScopeState, ScopeTables};

/// Epochs 0 and 2 seal; epoch 7 is an older open scope ending at its own
/// last block; epoch 11 is the newest, ending at the anchor. Block 12 carries
/// two proof stamps, so ordinals advance within a block.
fn mixed_plan() -> Plan {
    Plan::fresh(&[
        (10, &[4]),
        (12, &[3, 5]),
        (45, &[6]),
        (120, &[7]),
        (190, &[2]),
    ])
}

#[tokio::test(flavor = "multi_thread")]
async fn the_map_lists_every_epoch_once_and_tables_hold_exactly_the_authored_records() {
    let mut chain = new_chain(1);
    let plan = mixed_plan();
    grow(&mut chain, &plan, 200);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut tachygram = open(&node, dir.path()).await;
    let report = step(&mut tachygram, &node, 1).await.unwrap().unwrap();
    assert_eq!(report.anchor.height, 198);
    assert_eq!(report.tables_built, 4);
    let published = Published::read(tachygram.distribution());
    assert!(published.eligible);
    check_against_chain(&published, tachygram.distribution(), &node.chain());
    // Epochs 0-4 are sealed (one chunk), 5-11 open.
    assert_eq!(published.chunks.len(), 1);
    assert_eq!(published.chunks[0].scopes.len(), 5);
    assert_eq!(published.head.open_scopes.len(), 7);
    assert_eq!(published.scope(7).state, ScopeState::Open);
    assert_eq!(published.scope(7).last_height, 135);
    // Two stamps in block 12 have ordinals 1 and 2 after block 10's 0.
    let ordinals: BTreeSet<u32> = published
        .entries(tachygram.distribution(), 0)
        .iter()
        .map(|e| e.stamp_ordinal)
        .collect();
    assert_eq!(ordinals, BTreeSet::from([0, 1, 2]));
    // Every authored tachygram is found exactly once through the client's
    // query plan, and every other epoch's tachygram is absent.
    let genesis = published.genesis();
    for epoch in [0u64, 2, 7, 11] {
        let scope = published.scope(epoch);
        let rows = published.rows(tachygram.distribution(), epoch);
        for expected in oracle(&node.chain(), epoch, 198) {
            let mut found: Vec<&tachyon_pir_protocol::TachygramEntry> = Vec::new();
            for target in scope
                .query_plan(PRODUCT, &genesis, &expected.tachygram)
                .unwrap()
            {
                let entries = &rows[&(target.table.bucket, target.table.segment, target.row)];
                found.extend(entries.iter().filter(|e| e.tachygram == expected.tachygram));
            }
            assert_eq!(found, [&expected]);
        }
    }
    let metrics = tachygram.metrics().render();
    assert!(metrics.contains("tachyon_tachygram_ingested_height 198\n"));
    assert!(metrics.contains("tachyon_tachygram_anchor_height 198\n"));
    assert!(metrics.contains("tachyon_tachygram_entries_total 27\n"));
    assert!(metrics.contains("tachyon_tachygram_open_scope_entries 9\n"));
    assert!(metrics.contains("tachyon_tachygram_segments_overflow_total 0\n"));
    assert!(metrics.contains("tachyon_tachygram_publication_seconds_count 1\n"));
    // An unchanged pass offers nothing.
    assert!(step(&mut tachygram, &node, 2).await.unwrap().is_none());
}

/// Epochs 0 and 2 hold tachygrams and seal by height 170.
fn sealing_plan() -> Plan {
    Plan::fresh(&[(10, &[3]), (15, &[4]), (45, &[5]), (50, &[2])])
}

/// Spec 6.5: sealed tables are a pure function of the chain. Publishing after
/// every block and publishing once at the end, from an empty state directory,
/// give byte-identical tables, chunks and map content.
#[tokio::test(flavor = "multi_thread")]
async fn tables_are_identical_across_publication_cadence_and_rebuild_from_empty_state() {
    let plan = sealing_plan();
    let mut chain = new_chain(2);
    grow(&mut chain, &plan, 9);
    let node = node(chain).await;
    let every = tempfile::tempdir().unwrap();
    let mut eager = open(&node, every.path()).await;
    let mut publications = 0;
    let mut generations = BTreeSet::new();
    for height in 10..=170 {
        grow(&mut node.chain(), &plan, height);
        if let Some(report) = step(&mut eager, &node, height).await.unwrap() {
            assert_eq!(report.anchor.height as u64, height - 2);
            publications += 1;
            generations.insert(report.committed.generation);
        }
    }
    assert_eq!(publications, 161, "one publication per block");
    assert_eq!(generations.len(), 161);
    // Rebuilds only when an epoch's entries change: blocks 10, 15, 45, 50.
    assert_eq!(eager.tables_built(), 4);
    let a = Published::read(eager.distribution());

    let empty = tempfile::tempdir().unwrap();
    let mut once = open(&node, empty.path()).await;
    step(&mut once, &node, 1).await.unwrap().unwrap();
    assert_eq!(once.tables_built(), 2);
    let b = Published::read(once.distribution());

    assert_eq!(a.head.sealed_chunks, b.head.sealed_chunks);
    assert_eq!(a.chunks, b.chunks);
    assert_eq!(a.content(), b.content());
    for epoch in [0, 2] {
        assert_eq!(a.scope(epoch).state, ScopeState::Sealed);
        assert_eq!(a.tables(epoch), b.tables(epoch));
    }
    assert_eq!(a.inventory.tables, b.inventory.tables);
    check_against_chain(&b, once.distribution(), &node.chain());
}

/// Scenario 12 (publisher side) and spec 6.5 across a restart: the restarted
/// publisher republishes its previous publication under a new recovery epoch
/// and generation without rebuilding anything, then continues without
/// rebuilding sealed tables, and ends identical to a publisher that never
/// restarted.
#[tokio::test(flavor = "multi_thread")]
async fn restart_restores_the_previous_publication_and_never_rebuilds_its_tables() {
    let plan = sealing_plan();
    let mut chain = new_chain(3);
    grow(&mut chain, &plan, 140);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let before = {
        let mut first = open(&node, dir.path()).await;
        step(&mut first, &node, 1).await.unwrap().unwrap();
        assert_eq!(first.tables_built(), 2);
        let published = Published::read(first.distribution());
        assert_eq!(published.scope(0).state, ScopeState::Sealed);
        assert_eq!(published.scope(2).state, ScopeState::Open);
        published
    };
    let mut second = open(&node, dir.path()).await;
    // A restart starts withdrawn in a new recovery epoch.
    let current = second.distribution().current();
    assert!(!current.eligible);
    assert_eq!(
        current.fence.recovery_epoch,
        before.inventory.recovery_epoch + 1
    );
    // The first pass restores, and the unchanged chain needs no publication.
    assert!(step(&mut second, &node, 1).await.unwrap().is_none());
    let restored = Published::read(second.distribution());
    assert!(restored.eligible);
    assert_eq!(
        restored.inventory.recovery_epoch,
        before.inventory.recovery_epoch + 1
    );
    assert_eq!(
        restored.head.recovery_epoch.get(),
        restored.inventory.recovery_epoch
    );
    assert!(restored.head.generation > before.head.generation);
    assert_eq!(restored.content(), before.content());
    assert_eq!(restored.inventory.tables, before.inventory.tables);
    assert_eq!(restored.chunks, before.chunks);
    assert_eq!(second.tables_built(), 0);

    grow(&mut node.chain(), &plan, 170);
    step(&mut second, &node, 2).await.unwrap().unwrap();
    assert_eq!(
        second.tables_built(),
        0,
        "sealed and unchanged tables reused"
    );
    let after = Published::read(second.distribution());
    assert_eq!(after.scope(2).state, ScopeState::Sealed);
    assert_eq!(after.tables(0), before.tables(0));
    assert_eq!(after.tables(2), before.tables(2));
    check_against_chain(&after, second.distribution(), &node.chain());

    let fresh_dir = tempfile::tempdir().unwrap();
    let mut fresh = open(&node, fresh_dir.path()).await;
    step(&mut fresh, &node, 1).await.unwrap().unwrap();
    let fresh = Published::read(fresh.distribution());
    assert_eq!(after.content(), fresh.content());
    assert_eq!(after.chunks, fresh.chunks);
    assert_eq!(after.inventory.tables, fresh.inventory.tables);
}

/// Scenario 8: a rewind that stays inside open scopes withdraws, then
/// publishes a new generation in the same recovery epoch whose open-scope
/// content follows the new chain; sealed sessions survive.
#[tokio::test(flavor = "multi_thread")]
async fn a_shallow_reorg_publishes_a_new_generation_matching_the_new_chain() {
    let plan = Plan::fresh(&[(10, &[3]), (45, &[4]), (160, &[5])]);
    let mut chain = new_chain(4);
    grow(&mut chain, &plan, 170);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut tachygram = open(&node, dir.path()).await;
    step(&mut tachygram, &node, 1).await.unwrap().unwrap();
    let before = Published::read(tachygram.distribution());
    let open_epoch = epoch_of(160);
    assert_eq!(before.scope(open_epoch).state, ScopeState::Open);

    let fence = tachygram.distribution().current().fence;
    let replacement = Plan::fresh(&[(164, &[6])]);
    {
        let mut chain = node.chain();
        chain.reorg(162, Vec::new()).unwrap();
        grow(&mut chain, &replacement, 172);
    }
    let report = step(&mut tachygram, &node, 2).await.unwrap().unwrap();
    assert_eq!(report.anchor.height, 170);
    assert_eq!(
        report.tables_built, 1,
        "only the changed open scope is rebuilt"
    );
    let after = Published::read(tachygram.distribution());
    assert!(after.head.generation > before.head.generation);
    assert_eq!(after.head.recovery_epoch, before.head.recovery_epoch);
    let after_fence = tachygram.distribution().current().fence;
    assert_eq!(after_fence.recovery_epoch, fence.recovery_epoch);
    assert!(
        after_fence.latest_withdrawal > fence.latest_withdrawal,
        "the publication anchored above the kept block was withdrawn"
    );
    for epoch in [0, 2] {
        assert_eq!(after.scope(epoch).state, ScopeState::Sealed);
        assert_eq!(after.tables(epoch), before.tables(epoch));
    }
    let old: BTreeSet<_> = before
        .scope(open_epoch)
        .tables
        .iter()
        .map(|t| t.session_id.0)
        .collect();
    assert!(old.is_disjoint(&after.sessions()));
    check_against_chain(&after, tachygram.distribution(), &node.chain());
}

/// Scenario 9: a rewind into a sealed scope increments the recovery epoch
/// durably before the rewind, and the affected sessions are absent from the
/// new publication, whose content follows the new chain.
#[tokio::test(flavor = "multi_thread")]
async fn a_reorg_into_a_sealed_scope_recovers_and_drops_its_sessions() {
    let plan = Plan::fresh(&[(10, &[3]), (45, &[4])]);
    let mut chain = new_chain(5);
    grow(&mut chain, &plan, 170);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut tachygram = open(&node, dir.path()).await;
    step(&mut tachygram, &node, 1).await.unwrap().unwrap();
    let before = Published::read(tachygram.distribution());
    assert_eq!(before.scope(2).state, ScopeState::Sealed);

    let replacement = Plan::fresh(&[(50, &[6])]);
    {
        let mut chain = node.chain();
        chain.reorg(47, Vec::new()).unwrap();
        grow(&mut chain, &replacement, 175);
    }
    let report = step(&mut tachygram, &node, 2).await.unwrap().unwrap();
    assert_eq!(report.anchor.height, 173);
    let after = Published::read(tachygram.distribution());
    assert_eq!(
        after.head.recovery_epoch.get(),
        before.head.recovery_epoch.get() + 1
    );
    assert_eq!(after.tables(0), before.tables(0), "epoch 0 is untouched");
    let revoked: BTreeSet<_> = before
        .scope(2)
        .tables
        .iter()
        .map(|t| t.session_id.0)
        .collect();
    assert!(revoked.is_disjoint(&after.sessions()));
    check_against_chain(&after, tachygram.distribution(), &node.chain());
    assert!(
        tachygram
            .metrics()
            .render()
            .contains("tachyon_tachygram_sessions_revoked_total 1\n")
    );
}

/// Spec 8.3: a tachygram repeated within an epoch stops ingestion with the
/// stop policy, the faulty block is never written, and nothing containing it
/// is published, in-process and under the real scheduler.
#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_tachygram_stops_ingestion_and_is_never_published() {
    let mut chain = new_chain(6);
    let tachygrams = chain.fresh_tachygrams(3);
    let mut plan = Plan::default();
    plan.0
        .insert(10, Content::Explicit(vec![tachygrams.clone()]));
    plan.0
        .insert(20, Content::Explicit(vec![vec![tachygrams[1]]]));
    grow(&mut chain, &plan, 15);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut tachygram = open(&node, dir.path()).await;
    step(&mut tachygram, &node, 1).await.unwrap().unwrap();
    let before = Published::read(tachygram.distribution());
    assert_eq!(before.head.anchor_height, 13);

    grow(&mut node.chain(), &plan, 30);
    let error = step(&mut tachygram, &node, 2).await.unwrap_err();
    assert!(
        matches!(&error, Error::Integrity(_)),
        "an integrity fault: {error}"
    );
    assert_eq!(tachygram.writer.failed(&error), FailurePolicy::Stop);
    assert_eq!(tachygram.writer.store().committed().unwrap().height, 19);
    assert!(
        tachygram
            .publisher
            .publish_pending(&PublisherEngine)
            .await
            .unwrap()
            .is_none()
    );
    let after = Published::read(tachygram.distribution());
    assert_eq!(after.head, before.head);

    // The root scheduler stops the product on it.
    let (sender, observations) = tokio::sync::watch::channel(None);
    let feed = ChainFeed {
        chain: Arc::new(Canonical(client(&node))),
        observations,
    };
    sender.send_replace(Some(observe(&node, 3)));
    let Tachygram { writer, .. } = tachygram;
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        PublisherEngine.ingest(writer, feed, Shutdown::default()),
    )
    .await
    .expect("the scheduler stops");
    assert!(matches!(result, Err(Error::Integrity(_))));
}

/// Spec 11.1: a node that lists no `NuTachyon` upgrade is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_nutachyon_is_refused() {
    let chain = tachyon_fixtures::SyntheticChain::generate(
        ChainConfig {
            activation_height: None,
            ..ChainConfig::with_seed(7)
        },
        5,
    );
    let node = NodeDouble::start(chain, Default::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let refused = Tachygram::open(&config(dir.path()), client(&node), Shutdown::default()).await;
    assert!(matches!(refused, Err(Error::Network(_))));
    assert!(!dir.path().join("tachygram-v1.sqlite").exists());
}

/// Spec 11.3: snapshots offered while none is being published replace each
/// other, so only the latest is published; ingestion never waits for it.
#[tokio::test(flavor = "multi_thread")]
async fn offered_snapshots_coalesce_to_the_latest() {
    let plan = Plan::default();
    let mut chain = new_chain(10);
    grow(&mut chain, &plan, 30);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut tachygram = open(&node, dir.path()).await;
    let canonical = Canonical(client(&node));
    for (pass, height) in [(1, 30), (2, 35), (3, 41)] {
        grow(&mut node.chain(), &plan, height);
        pir_pipeline::ingestion::catch_up(&mut tachygram.writer, &canonical, &observe(&node, pass))
            .await
            .unwrap();
    }
    assert!(tachygram.distribution().current().publication.is_none());
    let report = tachygram
        .publisher
        .publish_pending(&PublisherEngine)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.anchor.height, 39);
    assert_eq!(report.committed.generation, 1);
    assert!(
        tachygram
            .publisher
            .publish_pending(&PublisherEngine)
            .await
            .unwrap()
            .is_none()
    );
    check_against_chain(
        &Published::read(tachygram.distribution()),
        tachygram.distribution(),
        &node.chain(),
    );
}

/// The canonical guard withdraws as soon as the root follower shows the
/// anchor left the chain, before the writer reconciles; the next pass
/// publishes on the new chain.
#[tokio::test(flavor = "multi_thread")]
async fn the_canonical_guard_withdraws_an_orphaned_anchor() {
    let plan = Plan::default();
    let mut chain = new_chain(11);
    grow(&mut chain, &plan, 40);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut tachygram = open(&node, dir.path()).await;
    step(&mut tachygram, &node, 1).await.unwrap().unwrap();
    assert!(tachygram.distribution().current().eligible);
    let (sender, observations) = tokio::sync::watch::channel(None);
    let stop = Shutdown::default();
    let guard = tokio::spawn(tachygram.guard(
        ChainFeed {
            chain: Arc::new(Canonical(client(&node))),
            observations,
        },
        stop.clone(),
    ));
    {
        let mut chain = node.chain();
        chain.reorg(35, Vec::new()).unwrap();
        grow(&mut chain, &plan, 42);
    }
    sender.send_replace(Some(observe(&node, 2)));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while tachygram.distribution().current().eligible {
        assert!(std::time::Instant::now() < deadline, "not withdrawn");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop.request();
    guard.await.unwrap().unwrap();
    let report = step(&mut tachygram, &node, 3).await.unwrap().unwrap();
    assert_eq!(report.anchor.height, 40);
    assert!(tachygram.distribution().current().eligible);
    check_against_chain(
        &Published::read(tachygram.distribution()),
        tachygram.distribution(),
        &node.chain(),
    );
}
