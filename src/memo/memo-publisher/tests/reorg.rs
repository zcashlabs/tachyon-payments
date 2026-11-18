//! Reorgs (spec 11.4, scenarios 8 and 9), each product judged against its own
//! last publication.
mod common;

use common::*;
use pir_pipeline::{ingestion::ChainFeed, service::Shutdown};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tachyon_fixtures::Malformation;
use tachyon_pir_protocol::{Product, ScopeState};

/// Scenario 8: a rewind that stays inside open scopes of both products
/// withdraws both, then publishes a new generation of each in the same
/// recovery epoch whose open-scope content follows the new chain; sealed
/// sessions survive.
#[tokio::test(flavor = "multi_thread")]
async fn a_shallow_reorg_publishes_a_new_generation_of_both_matching_the_new_chain() {
    let both = |n| Item::Random {
        follow_ups: n,
        first_contacts: n,
        wallet_states: 0,
    };
    let malformed = |m| Item::Malformed(m);
    let plan = Plan::new(vec![
        (10, vec![both(2)]),
        (160, vec![both(2)]),
        (
            163,
            vec![
                malformed(Malformation::BadMagic),
                malformed(Malformation::CountZero),
            ],
        ),
    ]);
    let mut chain = new_chain(4);
    grow(&mut chain, &plan, 170);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut publisher = open(&node, dir.path()).await;
    step(&mut publisher, &node, 1).await.unwrap();
    let before: Vec<Published> = PRODUCTS
        .iter()
        .map(|product| Published::read(&publisher, *product))
        .collect();
    let fences: Vec<_> = PRODUCTS
        .iter()
        .map(|product| publisher.distribution(*product).unwrap().current().fence)
        .collect();
    assert_eq!(before[0].scope(0).state, ScopeState::Sealed);
    assert_eq!(before[0].scope(epoch_of(160)).state, ScopeState::Open);
    assert_eq!(before[1].scope(0).state, ScopeState::Open);
    let text = publisher.metrics().render();
    assert_eq!(metric(&text, "tachyon_memo_envelopes_invalid_total"), "2");

    let replacement = Plan::new(vec![(
        164,
        vec![both(1), malformed(Malformation::TruncatedBody)],
    )]);
    {
        let mut chain = node.chain();
        chain.reorg(162, Vec::new()).unwrap();
        grow(&mut chain, &replacement, 172);
    }
    let reports = step(&mut publisher, &node, 2).await.unwrap();
    // Memo: epoch 38 (160-163) keeps its entries and its tables; epoch 39
    // gains one. First-contact: the one open span changes.
    assert_eq!(reports.memo.as_ref().unwrap().tables_built, 1);
    assert_eq!(reports.first_contact.as_ref().unwrap().tables_built, 1);
    for (i, product) in PRODUCTS.iter().enumerate() {
        let report = reports.get(*product).unwrap();
        assert_eq!(report.anchor.height, 170);
        let after = Published::read(&publisher, *product);
        assert!(after.head.generation > before[i].head.generation);
        assert_eq!(after.head.recovery_epoch, before[i].head.recovery_epoch);
        let fence = publisher.distribution(*product).unwrap().current().fence;
        assert_eq!(fence.recovery_epoch, fences[i].recovery_epoch);
        assert!(
            fence.latest_withdrawal > fences[i].latest_withdrawal,
            "{product}: the publication anchored above the kept block was withdrawn"
        );
    }
    let memo = Published::read(&publisher, Product::Memo);
    assert_eq!(
        memo.tables(0),
        before[0].tables(0),
        "sealed epoch 0 survives"
    );
    let first_contact = Published::read(&publisher, Product::FirstContact);
    assert!(
        before[1]
            .scope_sessions(0)
            .is_disjoint(&first_contact.sessions())
    );
    check_both(&publisher, &node.chain());
    let text = publisher.metrics().render();
    assert_eq!(metric(&text, "tachyon_memo_sessions_revoked_total"), "0");
    assert_eq!(
        metric(&text, "tachyon_first_contact_sessions_revoked_total"),
        "0"
    );
    // The envelope gauges describe the journal: the rewind removed block 163's
    // two invalid envelopes, the replacement chain carries one.
    assert_eq!(metric(&text, "tachyon_memo_envelopes_invalid_total"), "1");
}

/// Scenario 9: a rewind into a sealed memo epoch while the first-contact
/// span it reaches is still open. Memo recovers (its recovery epoch
/// increments durably before the rewind and the reached epoch's sessions are
/// absent from the new publication); first-contact only withdraws and
/// publishes a new generation in its own recovery epoch. Both follow the new
/// chain.
#[tokio::test(flavor = "multi_thread")]
async fn a_reorg_into_a_sealed_memo_epoch_recovers_memo_and_only_withdraws_first_contact() {
    let plan = Plan::new(vec![
        (10, vec![first_contacts(1)]),
        (
            45,
            vec![Item::Random {
                follow_ups: 2,
                first_contacts: 1,
                wallet_states: 0,
            }],
        ),
    ]);
    let mut chain = new_chain(5);
    grow(&mut chain, &plan, 170);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut publisher = open(&node, dir.path()).await;
    step(&mut publisher, &node, 1).await.unwrap();
    let memo_before = Published::read(&publisher, Product::Memo);
    let fc_before = Published::read(&publisher, Product::FirstContact);
    // Epoch 9 is heights 44-47, sealed under anchor 168.
    assert_eq!(memo_before.scope(9).state, ScopeState::Sealed);
    assert_eq!(fc_before.scope(0).state, ScopeState::Open);
    let fc_fence = publisher
        .first_contact_distribution()
        .unwrap()
        .current()
        .fence;

    let replacement = Plan::new(vec![
        (47, vec![follow_ups(1)]),
        (50, vec![first_contacts(1)]),
    ]);
    {
        let mut chain = node.chain();
        chain.reorg(46, Vec::new()).unwrap();
        grow(&mut chain, &replacement, 175);
    }
    step(&mut publisher, &node, 2).await.unwrap();
    let memo = Published::read(&publisher, Product::Memo);
    assert_eq!(
        memo.head.recovery_epoch.get(),
        memo_before.head.recovery_epoch.get() + 1
    );
    assert_eq!(
        memo.tables(0),
        memo_before.tables(0),
        "epoch 0 is untouched"
    );
    assert!(memo_before.scope_sessions(9).is_disjoint(&memo.sessions()));
    let fc = Published::read(&publisher, Product::FirstContact);
    assert_eq!(fc.head.recovery_epoch, fc_before.head.recovery_epoch);
    assert!(fc.head.generation > fc_before.head.generation);
    let fence = publisher
        .first_contact_distribution()
        .unwrap()
        .current()
        .fence;
    assert_eq!(fence.recovery_epoch, fc_fence.recovery_epoch);
    assert!(fence.latest_withdrawal > fc_fence.latest_withdrawal);
    let old: BTreeSet<_> = fc_before.scope_sessions(0);
    assert!(old.is_disjoint(&fc.sessions()));
    check_both(&publisher, &node.chain());
    let text = publisher.metrics().render();
    assert_eq!(metric(&text, "tachyon_memo_sessions_revoked_total"), "1");
    assert_eq!(
        metric(&text, "tachyon_first_contact_sessions_revoked_total"),
        "0"
    );
}

/// The canonical guards withdraw both products as soon as the root follower
/// shows the anchor left the chain, before the writer reconciles; the next
/// pass publishes both on the new chain.
#[tokio::test(flavor = "multi_thread")]
async fn the_canonical_guards_withdraw_both_products_on_an_orphaned_anchor() {
    let plan = Plan::default();
    let mut chain = new_chain(11);
    grow(&mut chain, &plan, 40);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut publisher = open(&node, dir.path()).await;
    step(&mut publisher, &node, 1).await.unwrap();
    let eligible = |publisher: &memo_publisher::MemoPublisher| {
        PRODUCTS.map(|product| publisher.distribution(product).unwrap().current().eligible)
    };
    assert_eq!(eligible(&publisher), [true, true]);
    let (sender, observations) = tokio::sync::watch::channel(None);
    let stop = Shutdown::default();
    let guard = tokio::spawn(publisher.guard(
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
    while eligible(&publisher) != [false, false] {
        assert!(std::time::Instant::now() < deadline, "not withdrawn");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop.request();
    guard.await.unwrap().unwrap();
    let reports = step(&mut publisher, &node, 3).await.unwrap();
    for product in PRODUCTS {
        assert_eq!(reports.get(product).unwrap().anchor.height, 40);
    }
    check_both(&publisher, &node.chain());
}
