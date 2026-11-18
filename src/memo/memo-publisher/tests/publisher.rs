//! The memo publisher against the node double, driven in-process with
//! `pir_pipeline::ingestion::catch_up` and `PublisherEngine`. The oracle is
//! always the fixture's authored envelopes.
mod common;

use common::*;
use memo_publisher::{Error, MemoConfig, MemoPublisher, MemoSettings};
use pir_pipeline::{engine::PublisherEngine, service::Shutdown};
use tachyon_fixtures::{ChainConfig, EntryKind, Malformation, NodeDouble};
use tachyon_pir_protocol::{Product, ScopeState};

/// Scenarios 1, 5 and 7 together, on 4-block epochs anchored at 148:
/// memo epochs 0 to 9 seal (0 and 2 hold entries), epoch 30 is an older open scope ending at its
/// own last block, epoch 35 is the newest, ending at the anchor; one
/// first-contact span (epochs 0 to 35, open). Repeated tags within an
/// envelope, a block and across blocks; every malformation in blocks that
/// also carry valid envelopes; wallet-state entries.
#[tokio::test(flavor = "multi_thread")]
async fn both_products_list_every_scope_once_and_tables_hold_exactly_the_authored_entries() {
    let mut chain = new_chain(1);
    let memo_tag = repeated(&mut chain, EntryKind::FollowUp, 6);
    let fc_tag = repeated(&mut chain, EntryKind::FirstContact, 3);
    let malformed = |range: std::ops::Range<usize>| {
        Malformation::ALL[range]
            .iter()
            .map(|m| Item::Malformed(*m))
            .collect::<Vec<_>>()
    };
    let mut block9 = vec![Item::Entries(memo_tag[..3].to_vec())];
    block9.extend(malformed(0..4));
    block9.push(Item::Random {
        follow_ups: 4,
        first_contacts: 2,
        wallet_states: 2,
    });
    block9.push(Item::Entries(fc_tag[..2].to_vec()));
    let mut block17 = malformed(4..8);
    block17.push(follow_ups(5));
    block17.push(Item::Entries(memo_tag[3..5].to_vec()));
    let mut block130 = vec![follow_ups(3), first_contacts(1)];
    block130.extend(malformed(8..12));
    let plan = Plan::new(vec![
        (9, block9),
        (
            10,
            vec![
                Item::Entries(memo_tag[5..].to_vec()),
                Item::Entries(fc_tag[2..].to_vec()),
            ],
        ),
        (17, block17),
        (130, block130),
        (
            148,
            vec![Item::Random {
                follow_ups: 2,
                first_contacts: 1,
                wallet_states: 1,
            }],
        ),
    ]);
    grow(&mut chain, &plan, 150);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut publisher = open(&node, dir.path()).await;
    let reports = step(&mut publisher, &node, 1).await.unwrap();
    let memo = reports.memo.as_ref().unwrap();
    let first_contact = reports.first_contact.as_ref().unwrap();
    assert_eq!(memo.anchor.height, 148);
    assert_eq!(first_contact.anchor.height, 148);
    // Memo epochs 0, 2, 30 and 35 hold entries; one first-contact span.
    assert_eq!(memo.tables_built, 4);
    assert_eq!(first_contact.tables_built, 1);
    check_both(&publisher, &node.chain());

    let memo = Published::read(&publisher, Product::Memo);
    // Epochs 0-9 are sealed (one chunk), 10-35 open.
    assert_eq!(memo.chunks.len(), 1);
    assert_eq!(memo.chunks[0].scopes.len(), 10);
    assert_eq!(memo.head.open_scopes.len(), 26);
    assert_eq!(memo.scope(30).state, ScopeState::Open);
    assert_eq!(memo.scope(30).last_height, 131);
    assert_eq!(memo.scope(35).last_height, 148);
    // Scenario 5: the repeated tag's six entries (blocks 9, 10, 17) are all
    // returned, in ordinal order, from the epochs they were mined in.
    let key: [u8; 16] = memo_tag[0].key16();
    let rows = memo.rows(publisher.memo_distribution(), 0);
    assert_eq!(memo.lookup(&rows, 0, &key).len(), 4);
    let rows = memo.rows(publisher.memo_distribution(), 2);
    assert_eq!(memo.lookup(&rows, 2, &key).len(), 2);
    let fc = Published::read(&publisher, Product::FirstContact);
    assert!(fc.chunks.is_empty());
    assert_eq!(fc.head.open_scopes.len(), 1);
    assert_eq!(fc.scope(0).first_height, 8);
    assert_eq!(fc.scope(0).last_height, 148);
    let rows = fc.rows(publisher.first_contact_distribution().unwrap(), 0);
    assert_eq!(fc.lookup(&rows, 0, &fc_tag[0].key16()).len(), 3);

    // Scenario 7: twelve malformed envelopes skipped and counted; ingestion
    // reached the tip and the valid envelopes beside them are published.
    let counts = node.chain().envelope_counts(..);
    assert_eq!(counts.malformed, 12);
    assert_eq!(counts.wallet_states, 3);
    let text = publisher.metrics().render();
    for (name, value) in [
        ("tachyon_memo_envelopes_invalid_total", "12".to_string()),
        ("tachyon_memo_state_entries_total", "3".to_string()),
        ("tachyon_memo_ingested_height", "148".to_string()),
        ("tachyon_first_contact_ingested_height", "148".to_string()),
        ("tachyon_memo_anchor_height", "148".to_string()),
        ("tachyon_first_contact_anchor_height", "148".to_string()),
        ("tachyon_memo_entries_total", counts.follow_ups.to_string()),
        (
            "tachyon_first_contact_entries_total",
            counts.first_contacts.to_string(),
        ),
        (
            "tachyon_first_contact_open_scope_entries",
            counts.first_contacts.to_string(),
        ),
        ("tachyon_memo_segments_overflow_total", "0".to_string()),
        ("tachyon_memo_publication_seconds_count", "1".to_string()),
        (
            "tachyon_first_contact_publication_seconds_count",
            "1".to_string(),
        ),
    ] {
        assert_eq!(metric(&text, name), value, "{name}");
    }
    let open_memo: u64 = memo.head.open_scopes.iter().map(|s| s.entries).sum();
    assert_eq!(
        metric(&text, "tachyon_memo_open_scope_entries"),
        open_memo.to_string()
    );
    // An unchanged pass offers nothing to either product.
    assert!(step(&mut publisher, &node, 2).await.unwrap().is_empty());
}

/// Spec 6.5 MUST, and the 64-epoch span rule: span 0 holds four entries and
/// closes when it reaches 64 epochs (block 263), sealing at 363. Publishing
/// every 15 blocks and publishing once from an empty state directory give
/// byte-identical sealed tables, chunks and map content for both products.
#[tokio::test(flavor = "multi_thread")]
async fn sealed_tables_are_identical_across_cadence_and_rebuild_and_a_span_closes_at_64_epochs() {
    let plan = Plan::new(vec![
        (
            10,
            vec![Item::Random {
                follow_ups: 3,
                first_contacts: 2,
                wallet_states: 0,
            }],
        ),
        (17, vec![follow_ups(2)]),
        (100, vec![first_contacts(2)]),
        (300, vec![first_contacts(1)]),
    ]);
    let mut chain = new_chain(2);
    grow(&mut chain, &plan, 9);
    let node = node(chain).await;
    let eager_dir = tempfile::tempdir().unwrap();
    let mut eager = open(&node, eager_dir.path()).await;
    let mut generations = Vec::new();
    for (pass, height) in (20..=375).step_by(15).enumerate() {
        grow(&mut node.chain(), &plan, height);
        let reports = step(&mut eager, &node, pass as u64 + 1).await.unwrap();
        for product in PRODUCTS {
            let report = reports.get(product).expect("a publication per pass");
            assert_eq!(u64::from(report.anchor.height), height - 2);
        }
        generations.push(reports.memo.unwrap().committed.generation);
    }
    assert!(generations.windows(2).all(|w| w[0] < w[1]));
    // Rebuilds only when a scope's entries change: memo epochs 0 and 2 once;
    // span 0 after blocks 10 and 100, span 1 after block 300.
    assert_eq!(eager.tables_built(Product::Memo), 2);
    assert_eq!(eager.tables_built(Product::FirstContact), 3);
    grow(&mut node.chain(), &plan, 375);
    step(&mut eager, &node, 100).await.unwrap();

    let empty = tempfile::tempdir().unwrap();
    let mut once = open(&node, empty.path()).await;
    step(&mut once, &node, 1).await.unwrap();
    assert_eq!(once.tables_built(Product::Memo), 2);
    assert_eq!(once.tables_built(Product::FirstContact), 2);
    check_both(&once, &node.chain());

    for product in PRODUCTS {
        let a = Published::read(&eager, product);
        let b = Published::read(&once, product);
        assert_eq!(a.head.sealed_chunks, b.head.sealed_chunks, "{product}");
        assert_eq!(a.chunks, b.chunks, "{product}");
        assert_eq!(a.content(), b.content(), "{product}");
        assert_eq!(a.inventory.tables, b.inventory.tables, "{product}");
    }
    let memo = Published::read(&once, Product::Memo);
    for epoch in [0, 2] {
        assert_eq!(memo.scope(epoch).state, ScopeState::Sealed);
        assert!(!memo.scope(epoch).tables.is_empty());
    }
    // The span closed by covering 64 epochs, not by count, and sealed.
    let fc = Published::read(&once, Product::FirstContact);
    assert_eq!(spans(&node.chain(), 373), [(0, Some(63)), (64, None)]);
    let span = fc.scope(0);
    assert_eq!(
        (
            span.scope_id,
            span.state,
            span.first_height,
            span.last_height
        ),
        (0, ScopeState::Sealed, 8, 263)
    );
    assert_eq!(span.entries, 4);
    let next = fc.scope(1);
    assert_eq!(
        (
            next.scope_id,
            next.state,
            next.first_height,
            next.last_height
        ),
        (64, ScopeState::Open, 264, 373)
    );
    assert_eq!(fc.chunks.len(), 1);
    assert_eq!(fc.inventory.newest_scope, 1);
}

/// Scenario 12 (publisher side) and spec 6.5 across a restart: the restarted
/// publisher republishes both previous publications under a new recovery
/// epoch and generation without rebuilding anything, continues without
/// rebuilding, and ends identical to a publisher that never restarted.
#[tokio::test(flavor = "multi_thread")]
async fn restart_restores_both_publications_and_never_rebuilds_their_tables() {
    let plan = Plan::new(vec![
        (
            10,
            vec![Item::Random {
                follow_ups: 2,
                first_contacts: 2,
                wallet_states: 1,
            }],
        ),
        (130, vec![follow_ups(1)]),
    ]);
    let mut chain = new_chain(3);
    grow(&mut chain, &plan, 140);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let before: Vec<Published> = {
        let mut first = open(&node, dir.path()).await;
        step(&mut first, &node, 1).await.unwrap();
        assert_eq!(first.tables_built(Product::Memo), 2);
        assert_eq!(first.tables_built(Product::FirstContact), 1);
        PRODUCTS
            .iter()
            .map(|product| Published::read(&first, *product))
            .collect()
    };
    assert_eq!(before[0].scope(0).state, ScopeState::Sealed);
    let mut second = open(&node, dir.path()).await;
    for (product, before) in PRODUCTS.iter().zip(&before) {
        // A restart starts withdrawn in a new recovery epoch.
        let current = second.distribution(*product).unwrap().current();
        assert!(!current.eligible, "{product}");
        assert_eq!(
            current.fence.recovery_epoch,
            before.inventory.recovery_epoch + 1
        );
    }
    // The first pass restores both, and the unchanged chain needs nothing.
    assert!(step(&mut second, &node, 1).await.unwrap().is_empty());
    for (product, before) in PRODUCTS.iter().zip(&before) {
        let restored = Published::read(&second, *product);
        assert!(restored.eligible, "{product}");
        assert_eq!(
            restored.head.recovery_epoch.get(),
            before.inventory.recovery_epoch + 1
        );
        assert!(restored.head.generation > before.head.generation);
        assert_eq!(restored.content(), before.content());
        assert_eq!(restored.inventory.tables, before.inventory.tables);
        assert_eq!(restored.chunks, before.chunks);
        assert_eq!(second.tables_built(*product), 0);
    }
    // The journal's envelope counts survive the restart.
    assert_eq!(
        metric(
            &second.metrics().render(),
            "tachyon_memo_state_entries_total"
        ),
        "1"
    );

    grow(&mut node.chain(), &plan, 170);
    let reports = step(&mut second, &node, 2).await.unwrap();
    assert!(reports.memo.is_some() && reports.first_contact.is_some());
    for product in PRODUCTS {
        assert_eq!(second.tables_built(product), 0, "{product}: tables reused");
    }
    check_both(&second, &node.chain());

    let fresh_dir = tempfile::tempdir().unwrap();
    let mut fresh = open(&node, fresh_dir.path()).await;
    step(&mut fresh, &node, 1).await.unwrap();
    for product in PRODUCTS {
        let after = Published::read(&second, product);
        let fresh = Published::read(&fresh, product);
        assert_eq!(after.content(), fresh.content(), "{product}");
        assert_eq!(after.chunks, fresh.chunks, "{product}");
        assert_eq!(after.inventory.tables, fresh.inventory.tables, "{product}");
    }
}

/// Spec 11.3: snapshots offered while none is being published replace each
/// other, per product, so only the latest is published; ingestion never
/// waits for it.
#[tokio::test(flavor = "multi_thread")]
async fn offered_snapshots_coalesce_to_the_latest_for_each_product() {
    let plan = Plan::default();
    let mut chain = new_chain(10);
    grow(&mut chain, &plan, 30);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut publisher = open(&node, dir.path()).await;
    let canonical = Canonical(client(&node));
    for (pass, height) in [(1, 30), (2, 35), (3, 41)] {
        grow(&mut node.chain(), &plan, height);
        pir_pipeline::ingestion::catch_up(&mut publisher.writer, &canonical, &observe(&node, pass))
            .await
            .unwrap();
    }
    for product in PRODUCTS {
        assert!(
            publisher
                .distribution(product)
                .unwrap()
                .current()
                .publication
                .is_none()
        );
    }
    let memo = publisher
        .memo
        .publish_pending(&PublisherEngine)
        .await
        .unwrap()
        .unwrap();
    let first_contact = publisher
        .first_contact
        .as_mut()
        .unwrap()
        .publish_pending(&PublisherEngine)
        .await
        .unwrap()
        .unwrap();
    for report in [&memo, &first_contact] {
        assert_eq!(report.anchor.height, 39);
        assert_eq!(report.committed.generation, 1);
    }
    assert_eq!(first_contact.product, Product::FirstContact);
    assert!(step(&mut publisher, &node, 4).await.unwrap().is_empty());
    check_both(&publisher, &node.chain());
}

/// Without first-contact, the definition publishes memo only and leaves the
/// first-contact distribution untouched; the journal still holds
/// first-contact entries.
#[tokio::test(flavor = "multi_thread")]
async fn memo_alone_publishes_no_first_contact() {
    let plan = Plan::default();
    let mut chain = new_chain(12);
    grow(&mut chain, &plan, 20);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let config = MemoConfig::new(
        MemoSettings {
            listen: "127.0.0.1:9401".parse().unwrap(),
            state_dir: dir.path().to_path_buf(),
        },
        EPOCH_LENGTH,
    );
    let mut publisher = MemoPublisher::open(&config, client(&node), Shutdown::default())
        .await
        .unwrap();
    assert!(publisher.first_contact.is_none());
    assert!(publisher.distribution(Product::FirstContact).is_none());
    let reports = step(&mut publisher, &node, 1).await.unwrap();
    assert_eq!(reports.memo.unwrap().anchor.height, 18);
    assert!(reports.first_contact.is_none());
    check_against_chain(
        &Published::read(&publisher, Product::Memo),
        publisher.memo_distribution(),
        &node.chain(),
    );
    assert!(!dir.path().join("distribution/first-contact").exists());
    assert!(
        !publisher
            .metrics()
            .render()
            .contains("tachyon_first_contact_")
    );
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
    let refused =
        MemoPublisher::open(&config(dir.path()), client(&node), Shutdown::default()).await;
    assert!(matches!(refused, Err(Error::Network(_))));
    assert!(!dir.path().join("memo-v1.sqlite").exists());
}
