//! First-contact spans (spec 7.2): decided at epoch boundaries from chain
//! data, closing at the first boundary with at least `FC_SPAN_TARGET` entries
//! or `FC_SPAN_MAX_EPOCHS` epochs, durable, and removed and re-decided
//! identically across rewinds.
mod common;

use common::*;
use memo_store::{Error, FC_SPAN_MAX_EPOCHS, FC_SPAN_TARGET, STORE_FILE, Span, Store};

/// The constants come from the protocol crate (spec 15).
#[test]
fn the_span_constants_are_the_spec_values() {
    assert_eq!(FC_SPAN_TARGET, 3072);
    assert_eq!(FC_SPAN_MAX_EPOCHS, 64);
    assert_eq!(
        FC_SPAN_TARGET,
        tachyon_pir_protocol::constants::FC_SPAN_TARGET
    );
}

/// Hands out fresh tag numbers.
struct Tags(u32);

impl Tags {
    fn take(&mut self, count: u32) -> Vec<Memo> {
        let memos = first_contact_memos(self.0, count);
        self.0 += count;
        memos
    }
}

/// `(first_epoch, last_epoch, entries, closed_by height)` of each span.
fn shape(store: &Store) -> Vec<(u32, Option<u32>, u64, Option<u32>)> {
    store
        .spans()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s.first_epoch,
                s.last_epoch,
                s.entries,
                s.closed_by.map(|b| b.height),
            )
        })
        .collect()
}

/// A span closes at the end of the first epoch where it holds at least 3,072
/// entries: not at a boundary with 3,071, not in the middle of the epoch that
/// reaches the target, and a single epoch holding exactly 3,072 is a span of
/// its own.
#[test]
fn a_span_closes_by_count_at_the_first_boundary_reaching_the_target() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    let mut tags = Tags(1);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&dir.path().join(STORE_FILE), &binding);
    // Epoch 0 (heights 8..=11): 1,000 entries. Epoch 1 (12..=15): 2,000.
    chain.push(tags.take(1000));
    chain.fill_epoch(0);
    chain.push_empty(1);
    chain.push(tags.take(2000));
    chain.fill_epoch(1);
    chain.sync(&mut store);
    assert_eq!(shape(&store), [(0, None, 3000, None)]);
    // Epoch 2 (16..=19): 71 more, so the boundary at 19 sees 3,071.
    chain.push(tags.take(71));
    chain.fill_epoch(2);
    chain.sync(&mut store);
    assert_eq!(shape(&store), [(0, None, 3071, None)]);
    // Epoch 3 (20..=23): the target is reached at 20 and passed at 21, but
    // the span stays open until the epoch's last block.
    chain.push(tags.take(1));
    chain.push(tags.take(5));
    chain.push_empty(1);
    chain.sync(&mut store);
    assert_eq!(store.committed().unwrap().height, 22);
    assert_eq!(shape(&store), [(0, None, 3077, None)]);
    chain.push_empty(1);
    chain.sync(&mut store);
    assert_eq!(shape(&store), [(0, Some(3), 3077, Some(23))]);
    let span = store.spans().unwrap()[0];
    assert_eq!(
        span,
        Span {
            index: 0,
            first_epoch: 0,
            last_epoch: Some(3),
            entries: 3077,
            closed_by: Some(chain.id(23)),
            last_height: 23,
        }
    );
    assert!(span.is_closed() && span.contains(0) && span.contains(3) && !span.contains(4));
    assert_eq!(span.scope_id(), 0);
    // The next span starts at epoch 4. It is listed once a block of epoch 4
    // is stored, and its scope is valid (and empty) before that.
    assert!(store.first_contact_entries(4).unwrap().is_empty());
    assert!(matches!(
        store.first_contact_entries(3),
        Err(Error::UnknownSpan(3))
    ));
    // Epoch 4 (24..=27) holds exactly 3,072 entries: a span of one epoch.
    chain.push(tags.take(3000));
    chain.push(tags.take(72));
    chain.sync(&mut store);
    assert_eq!(
        shape(&store),
        [(0, Some(3), 3077, Some(23)), (4, None, 3072, None)]
    );
    let open_span = store.spans().unwrap()[1];
    assert!(!open_span.is_closed() && open_span.contains(1000));
    assert_eq!(open_span.last_height, 25);
    chain.fill_epoch(4);
    chain.push(tags.take(3));
    chain.sync(&mut store);
    assert_eq!(
        shape(&store),
        [
            (0, Some(3), 3077, Some(23)),
            (4, Some(4), 3072, Some(27)),
            (5, None, 3, None)
        ]
    );
    chain.oracle(chain.tip_height()).check(&store);
    assert_eq!(store.first_contact_entries(4).unwrap().len(), 3072);
    assert_eq!(store.first_contact_entries(5).unwrap().len(), 3);
}

/// A span closes after 64 epochs whatever it holds, at exactly the last block
/// of its 64th epoch, and the next span starts counting at the following
/// epoch. With 4-block epochs, 64 epochs are 256 blocks.
#[test]
fn a_span_closes_after_64_epochs() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    let mut tags = Tags(1);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&dir.path().join(STORE_FILE), &binding);
    // Span 0: one entry in epoch 10 and five in epoch 63.
    chain.fill_epoch(9);
    chain.push(tags.take(1));
    chain.fill_epoch(62);
    chain.sync(&mut store);
    // 63 complete epochs: still open.
    assert_eq!(store.committed().unwrap().height, 8 + 63 * 4 - 1);
    assert_eq!(shape(&store), [(0, None, 1, None)]);
    chain.push(tags.take(5));
    chain.push_empty(2);
    chain.sync(&mut store);
    assert_eq!(shape(&store), [(0, None, 6, None)]);
    chain.push_empty(1);
    chain.sync(&mut store);
    let boundary = 8 + 64 * 4 - 1;
    assert_eq!(store.committed().unwrap().height, boundary);
    assert_eq!(shape(&store), [(0, Some(63), 6, Some(boundary))]);
    // Span 1: no entries at all, epochs 64..=127.
    chain.fill_epoch(126);
    chain.sync(&mut store);
    assert_eq!(
        shape(&store),
        [(0, Some(63), 6, Some(boundary)), (64, None, 0, None)]
    );
    chain.fill_epoch(127);
    chain.push_empty(1);
    chain.sync(&mut store);
    let boundary1 = 8 + 128 * 4 - 1;
    assert_eq!(
        shape(&store),
        [
            (0, Some(63), 6, Some(boundary)),
            (64, Some(127), 0, Some(boundary1)),
            (128, None, 0, None)
        ]
    );
    chain.oracle(chain.tip_height()).check(&store);
}

/// The two rules interleave: a span closed by count is followed by one that
/// closes after 64 epochs one entry short of the target, then by one closed
/// by count in its first epoch. One-block epochs make every block an epoch
/// boundary.
#[test]
fn count_and_epoch_rules_interleave() {
    let binding = binding_with(1);
    let mut chain = HandChain::new(&binding);
    let mut tags = Tags(1);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&dir.path().join(STORE_FILE), &binding);
    // Epoch e is height 8 + e.
    chain.push_empty(1);
    chain.push(tags.take(3072));
    chain.sync(&mut store);
    assert_eq!(shape(&store), [(0, Some(1), 3072, Some(9))]);
    // Span 1: epochs 2..=65, with 3,071 entries in epoch 30.
    chain.fill_epoch(29);
    chain.push(tags.take(3071));
    chain.fill_epoch(64);
    chain.sync(&mut store);
    assert_eq!(shape(&store)[1], (2, None, 3071, None));
    chain.fill_epoch(65);
    chain.sync(&mut store);
    assert_eq!(shape(&store)[1], (2, Some(65), 3071, Some(8 + 65)));
    // Span 2: 3,072 entries in its first epoch, 66.
    chain.push(tags.take(3072));
    chain.push(tags.take(1));
    chain.sync(&mut store);
    assert_eq!(
        shape(&store),
        [
            (0, Some(1), 3072, Some(9)),
            (2, Some(65), 3071, Some(73)),
            (66, Some(66), 3072, Some(74)),
            (67, None, 1, None)
        ]
    );
    chain.oracle(chain.tip_height()).check(&store);
}

/// A chain with a span closed by count at the block that ends epoch 0 (which
/// itself holds no entry), a span closed after 64 epochs, and an open span.
/// Epochs are two blocks.
fn boundary_chain() -> (memo_store::Binding, HandChain) {
    let binding = binding_with(2);
    let mut chain = HandChain::new(&binding);
    let mut tags = Tags(1);
    chain.push(tags.take(3072));
    chain.push_empty(1);
    for epoch in 1..=64 {
        chain.push(tags.take(epoch % 3));
        chain.push(vec![Memo::Valid(vec![follow_up(epoch, epoch)])]);
    }
    chain.push(tags.take(7));
    chain.push_empty(12);
    (binding, chain)
}

/// A rewind across a span boundary removes the boundary; one to the deciding
/// block or above keeps it; re-appending re-decides it identically, across
/// reopen and cadence (spec 6.5 determinism).
#[test]
fn rewinding_across_a_boundary_removes_it_and_reappending_redecides_it() {
    let (binding, chain) = boundary_chain();
    let tip = chain.tip_height();
    // Span 0 is decided by block 9 (last of epoch 0), span 1 (epochs 1..=64)
    // by block 8 + 65 * 2 - 1 = 137.
    let span1_end = 137;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(STORE_FILE);
    let mut store = open(&path, &binding);
    chain.sync(&mut store);
    chain.oracle(tip).check(&store);
    let full = Snapshot::of(&store, tip + 1);
    assert_eq!(
        full.spans
            .iter()
            .map(|s| (s.first_epoch, s.last_epoch, s.closed_by))
            .collect::<Vec<_>>(),
        [
            (0, Some(0), Some(chain.id(9))),
            (1, Some(64), Some(chain.id(span1_end))),
            (65, None, None)
        ]
    );

    // Keeping the deciding block keeps the boundary.
    store.rewind(Some(&chain.id(span1_end))).unwrap();
    chain.oracle(span1_end).check(&store);
    assert_eq!(store.spans().unwrap().len(), 2);
    assert!(store.spans().unwrap()[1].is_closed());
    // One block below it removes the boundary: span 1 is open again.
    store.rewind(Some(&chain.id(span1_end - 1))).unwrap();
    chain.oracle(span1_end - 1).check(&store);
    let spans = store.spans().unwrap();
    assert_eq!(spans.len(), 2);
    assert_eq!(
        (
            spans[1].first_epoch,
            spans[1].last_epoch,
            spans[1].closed_by
        ),
        (1, None, None)
    );
    assert_eq!(spans[1].last_height, span1_end - 1);
    assert!(matches!(
        store.first_contact_entries(65),
        Err(Error::UnknownSpan(65))
    ));
    drop(store);
    let mut store = open(&path, &binding);
    chain.sync(&mut store);
    assert_eq!(Snapshot::of(&store, tip + 1), full);

    // Block 8 holds span 0's 3,072 entries but block 9 decides it.
    store.rewind(Some(&chain.id(9))).unwrap();
    assert_eq!(shape(&store), [(0, Some(0), 3072, Some(9))]);
    store.rewind(Some(&chain.id(8))).unwrap();
    assert_eq!(shape(&store), [(0, None, 3072, None)]);
    chain.oracle(8).check(&store);
    chain.sync(&mut store);
    assert_eq!(Snapshot::of(&store, tip + 1), full);

    // Another cadence: several sessions, rewinds across both boundaries.
    let other_path = dir.path().join("other.sqlite");
    let mut other = open(&other_path, &binding);
    chain.append(&mut other, ACTIVATION, 100);
    other.rewind(Some(&chain.id(8))).unwrap();
    drop(other);
    let mut other = open(&other_path, &binding);
    chain.append(&mut other, 9, tip);
    other.rewind(Some(&chain.id(120))).unwrap();
    drop(other);
    let mut other = open(&other_path, &binding);
    chain.sync(&mut other);
    assert_eq!(Snapshot::of(&other, tip + 1), full);
    other.rewind(None).unwrap();
    assert_eq!(other.spans().unwrap(), Vec::new());
    chain.sync(&mut other);
    assert_eq!(Snapshot::of(&other, tip + 1), full);
}

/// A reorg that changes an epoch's first-contact count moves the boundary:
/// the replaced branch's boundary goes, and the new branch's is decided from
/// its own data.
#[test]
fn a_reorg_redecides_spans_from_the_new_branch() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    let mut tags = Tags(1);
    chain.push(tags.take(2000));
    chain.fill_epoch(0);
    chain.push(tags.take(1500));
    chain.fill_epoch(1);
    chain.fill_epoch(3);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&dir.path().join(STORE_FILE), &binding);
    chain.sync(&mut store);
    assert_eq!(
        shape(&store),
        [(0, Some(1), 3500, Some(15)), (2, None, 0, None)]
    );
    let old_boundary = store.spans().unwrap()[0].closed_by.unwrap();
    // The new branch forks at 11 and drops epoch 1's entries; epoch 3 brings
    // the span to the target instead.
    chain.truncate(11);
    chain.fill_epoch(2);
    chain.push(tags.take(1072));
    chain.fill_epoch(3);
    chain.push_empty(1);
    store.rewind(Some(&chain.id(11))).unwrap();
    assert_eq!(shape(&store), [(0, None, 2000, None)]);
    chain.sync(&mut store);
    chain.oracle(chain.tip_height()).check(&store);
    let spans = store.spans().unwrap();
    assert_eq!(
        shape(&store),
        [(0, Some(3), 3072, Some(23)), (4, None, 0, None)]
    );
    assert_eq!(old_boundary.height, 15);
    assert_eq!(spans[0].closed_by, Some(chain.id(23)));
    assert_ne!(store.block_hash(15).unwrap(), Some(old_boundary.hash));
}

/// Boundaries are written in the append that decides them: if that write
/// fails, the block is not stored either.
#[test]
fn a_failed_boundary_write_fails_the_append() {
    let binding = binding_with(1);
    let mut chain = HandChain::new(&binding);
    chain.push(first_contact_memos(1, 3072));
    chain.push_empty(1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(STORE_FILE);
    let mut store = open(&path, &binding);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail BEFORE INSERT ON span_boundaries
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();
    assert!(matches!(
        store.append(&chain.indexed(ACTIVATION)),
        Err(Error::Sql(_))
    ));
    assert_eq!(store.committed(), None);
    assert_eq!(store.spans().unwrap(), Vec::new());
    assert!(store.first_contact_entries(0).unwrap().is_empty());
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TRIGGER fail;")
        .unwrap();
    chain.sync(&mut store);
    chain.oracle(chain.tip_height()).check(&store);
    assert_eq!(
        shape(&store),
        [(0, Some(0), 3072, Some(8)), (1, None, 0, None)]
    );
}

/// Stored boundaries that contradict the stored counts or the span rule are
/// reported as corruption.
#[test]
fn tampered_boundaries_are_reported_corrupt() {
    let binding = binding_with(1);
    let mut chain = HandChain::new(&binding);
    chain.push(first_contact_memos(1, 3072));
    chain.push(first_contact_memos(5000, 10));
    chain.push_empty(1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(STORE_FILE);
    let mut store = open(&path, &binding);
    chain.sync(&mut store);
    chain.oracle(chain.tip_height()).check(&store);
    for sql in [
        "DELETE FROM span_boundaries;",
        "UPDATE span_boundaries SET entries = 3071;",
        "UPDATE blocks SET first_contact_entries = 3071 WHERE height = 8;",
        "INSERT INTO span_boundaries VALUES (1, 1, 1, 10, 9);",
        "UPDATE span_boundaries SET first_epoch = 1, last_epoch = 1;",
    ] {
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(sql)
            .unwrap();
        assert!(
            matches!(store.spans(), Err(Error::Corrupt(_))),
            "{sql}: {:?}",
            store.spans()
        );
        // Restore by rebuilding the store from the chain.
        store.rewind(None).unwrap();
        chain.sync(&mut store);
        chain.oracle(chain.tip_height()).check(&store);
    }
}
