//! The first-contact span rule by count (spec 7.2) and a reorg into a sealed
//! span (spec 11.4, scenario 9 for first-contact).
mod common;

use common::*;
use tachyon_pir_protocol::{Product, ScopeState};

/// 3,072 first-contact entries are reached in block 10, inside epoch 0
/// (heights 8-11): the span closes at the epoch boundary, block 11, with
/// block 11's entries included, and seals 100 blocks later. Span 1 starts at
/// epoch 1 and stays open below the target. A rewind to block 10 then reaches
/// the sealed span (and the sealed memo epoch 0): both products recover, the
/// span's old sessions are gone, and the replacement block 11 closes it again
/// by count.
#[tokio::test(flavor = "multi_thread")]
async fn a_span_closes_by_count_at_its_epoch_boundary_and_a_reorg_into_it_recovers() {
    let plan = Plan::new(vec![
        (9, vec![first_contacts(1500)]),
        (10, vec![first_contacts(1572)]),
        (
            11,
            vec![Item::Random {
                follow_ups: 1,
                first_contacts: 2,
                wallet_states: 0,
            }],
        ),
        (20, vec![first_contacts(1)]),
    ]);
    let mut chain = new_chain(6);
    grow(&mut chain, &plan, 125);
    let node = node(chain).await;
    let dir = tempfile::tempdir().unwrap();
    let mut publisher = open(&node, dir.path()).await;
    step(&mut publisher, &node, 1).await.unwrap();
    assert_eq!(spans(&node.chain(), 123), [(0, Some(0)), (1, None)]);
    check_both(&publisher, &node.chain());
    let before = Published::read(&publisher, Product::FirstContact);
    let span = before.scope(0);
    assert_eq!(
        (
            span.scope_id,
            span.state,
            span.first_height,
            span.last_height
        ),
        (0, ScopeState::Sealed, 8, 11)
    );
    assert_eq!(span.entries, 3074);
    assert_eq!(span.n_buckets, 1);
    let next = before.scope(1);
    assert_eq!(
        (
            next.scope_id,
            next.state,
            next.first_height,
            next.last_height
        ),
        (1, ScopeState::Open, 12, 123)
    );
    assert_eq!(next.entries, 1);
    let memo_before = Published::read(&publisher, Product::Memo);
    assert_eq!(memo_before.scope(0).state, ScopeState::Sealed);

    let replacement = Plan::new(vec![(11, vec![first_contacts(1)])]);
    {
        let mut chain = node.chain();
        chain.reorg(10, Vec::new()).unwrap();
        grow(&mut chain, &replacement, 126);
    }
    step(&mut publisher, &node, 2).await.unwrap();
    let after = Published::read(&publisher, Product::FirstContact);
    assert_eq!(
        after.head.recovery_epoch.get(),
        before.head.recovery_epoch.get() + 1
    );
    assert!(before.scope_sessions(0).is_disjoint(&after.sessions()));
    let span = after.scope(0);
    assert_eq!(
        (span.state, span.last_height, span.entries),
        (ScopeState::Sealed, 11, 3073)
    );
    let memo = Published::read(&publisher, Product::Memo);
    assert_eq!(
        memo.head.recovery_epoch.get(),
        memo_before.head.recovery_epoch.get() + 1
    );
    assert!(
        memo.scope(0).tables.is_empty(),
        "epoch 0 lost its follow-up"
    );
    check_both(&publisher, &node.chain());
    let text = publisher.metrics().render();
    // Every table of the scopes the rewind reached: span 0 and span 1; memo
    // epoch 0.
    assert_eq!(
        metric(&text, "tachyon_first_contact_sessions_revoked_total"),
        "2"
    );
    assert_eq!(metric(&text, "tachyon_memo_sessions_revoked_total"), "1");
}
