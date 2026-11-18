//! The journal against hand-built chains with authored envelopes (spec
//! sections 5, 7.1, 7.3 and 11.8).
mod common;

use common::*;
use memo_store::{
    Binding, Counts, EntryFault, EpochSummary, Error, IndexedBlock, Kind, Ordinal, Record,
    STORE_FILE, Store,
};
use std::time::Instant;
use tachyon_chain::{BlockId, EpochSchedule, Hash};
use tachyon_pir_protocol::Entry;
use tachyon_pir_protocol::envelope::{EnvelopeEntry, EnvelopeError};

fn store_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join(STORE_FILE)
}

/// A chain over several epochs mixing every entry kind, empty memos, invalid
/// memos and gaps in transaction indices.
fn mixed_chain(binding: &Binding, blocks: u32) -> HandChain {
    let mut chain = HandChain::new(binding);
    let malformed = malformed_memos();
    for i in 0..blocks {
        let n = i * 100;
        match i % 5 {
            0 => chain.push(vec![
                Memo::Valid(vec![follow_up(n, n), first_contact(n + 1, n + 1)]),
                Memo::Empty,
                Memo::Valid(vec![wallet_state(n + 2, 3), follow_up(n + 3, n + 3)]),
            ]),
            1 => chain.push_at(vec![
                (2, malformed[i as usize % malformed.len()].clone()),
                (
                    5,
                    Memo::Valid(vec![first_contact(n, n), first_contact(n + 1, n + 1)]),
                ),
                (
                    9,
                    Memo::Valid((0..16).map(|k| follow_up(n + k, n + k)).collect()),
                ),
            ]),
            2 => chain.push(Vec::new()),
            3 => chain.push(vec![Memo::Empty, Memo::Empty]),
            _ => chain.push(vec![Memo::Valid(vec![
                follow_up(n, n),
                wallet_state(n + 1, 0),
                first_contact(n + 2, n + 2),
                follow_up(n + 3, n + 3),
            ])]),
        };
    }
    chain
}

/// Appended from the activation height, a chain's entries, counts and spans
/// match the authored content in every read.
#[test]
fn appended_chain_matches_the_authored_content() {
    let binding = hand_binding();
    let chain = mixed_chain(&binding, 40);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    assert_eq!(store.committed(), None);
    assert_eq!(store.epochs().unwrap(), Vec::<u32>::new());
    assert_eq!(store.spans().unwrap(), Vec::new());
    assert_eq!(store.totals().unwrap(), Counts::default());
    chain.sync(&mut store);
    let oracle = chain.oracle(chain.tip_height());
    oracle.check(&store);
    assert_eq!(store.epochs().unwrap(), (0..10).collect::<Vec<_>>());
    assert_eq!(store.committed(), Some(chain.id(chain.tip_height())));
    // Eight blocks of each kind of content.
    let totals = store.totals().unwrap();
    assert_eq!(
        totals,
        Counts {
            envelopes_valid: 8 * 2 + 8 * 2 + 8,
            envelopes_invalid: 8,
            memo_entries: 8 * 2 + 8 * 16 + 8 * 2,
            first_contact_entries: 8 + 8 * 2 + 8,
            state_entries: 8 + 8,
        }
    );
    // Ordinals carry the real transaction index, gaps included.
    let epoch0 = store.memo_entries(0).unwrap();
    let ordinals: Vec<(u32, u32, u8)> = epoch0
        .iter()
        .map(|r| (r.ordinal.height, r.ordinal.tx_index, r.ordinal.entry_index))
        .collect();
    let mut want = vec![(8, 1, 0), (8, 3, 1)];
    want.extend((0..16).map(|k| (9, 9, k)));
    assert_eq!(ordinals, want);
}

/// Scenario 7 at the journal level: each malformed memo is skipped whole and
/// counted, ingestion continues, and the other transactions' entries in the
/// same block are stored.
#[test]
fn invalid_envelopes_are_skipped_and_counted() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    let malformed = malformed_memos();
    let mut memos = vec![Memo::Valid(vec![follow_up(1, 1)])];
    memos.extend(malformed.iter().cloned());
    memos.push(Memo::Valid(vec![first_contact(2, 2), follow_up(3, 3)]));
    chain.push(memos);
    // The next block holds only invalid memos and is still stored.
    chain.push(malformed.clone());

    let indexed = chain.indexed(ACTIVATION);
    let invalid: Vec<u32> = indexed.invalid.iter().map(|i| i.tx_index).collect();
    assert_eq!(invalid, (2..2 + malformed.len() as u32).collect::<Vec<_>>());
    assert_eq!(indexed.envelopes_valid, 2);
    for (record, memo) in indexed.invalid.iter().zip(&malformed) {
        let Memo::Invalid(_, why) = memo else {
            unreachable!()
        };
        let expected_kind = match *why {
            "one byte" | "magic only" | "truncated" | "count larger than entries" => "truncated",
            "bad magic" => "magic",
            "trailing byte" => "trailing",
            "zero entries" | "17 entries" => "count",
            "unknown kind" => "kind",
            "zero follow-up key" | "zero first-contact key after a good entry" => "zero",
            "wallet state over 1024 bytes" => "state",
            other => panic!("unexpected {other}"),
        };
        let got_kind = match record.error {
            EnvelopeError::Truncated { .. } => "truncated",
            EnvelopeError::BadMagic(_) => "magic",
            EnvelopeError::TrailingBytes { .. } => "trailing",
            EnvelopeError::EntryCount(_) => "count",
            EnvelopeError::UnknownKind { .. } => "kind",
            EnvelopeError::ZeroKey { .. } => "zero",
            EnvelopeError::StateTooLong { .. } => "state",
        };
        assert_eq!(got_kind, expected_kind, "{why}: {:?}", record.error);
        assert_eq!(
            record.txid,
            chain.blocks[0].block.txs[record.tx_index as usize - 1].txid
        );
    }

    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    chain.sync(&mut store);
    chain.oracle(chain.tip_height()).check(&store);
    assert_eq!(store.committed().unwrap().height, ACTIVATION + 1);
    assert_eq!(
        store.envelopes_invalid_total().unwrap(),
        2 * malformed.len() as u64
    );
    // The entry before the zero first-contact key in the same envelope is
    // not stored: the envelope is skipped whole.
    let memo = store.memo_entries(0).unwrap();
    assert_eq!(
        memo.iter().map(|r| r.tag).collect::<Vec<_>>(),
        [tag(1), tag(3)]
    );
    let last = 2 + malformed.len() as u32;
    assert_eq!(
        memo.iter().map(|r| r.ordinal).collect::<Vec<_>>(),
        [
            Ordinal {
                height: ACTIVATION,
                tx_index: 1,
                entry_index: 0
            },
            Ordinal {
                height: ACTIVATION,
                tx_index: last,
                entry_index: 1
            }
        ]
    );
    let fc = store.first_contact_entries(0).unwrap();
    assert_eq!(fc.len(), 1);
    assert_eq!(fc[0].tag, tag(2));
}

/// An empty memo is neither a valid nor an invalid envelope.
#[test]
fn an_empty_memo_is_not_counted() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    chain.push(vec![Memo::Empty, Memo::Empty, Memo::Empty]);
    chain.push(vec![Memo::Empty, Memo::Valid(vec![follow_up(1, 1)])]);
    let indexed = chain.indexed(ACTIVATION);
    assert!(indexed.invalid.is_empty());
    assert_eq!(indexed.counts(), Counts::default());
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    chain.sync(&mut store);
    chain.oracle(chain.tip_height()).check(&store);
    assert_eq!(
        store.totals().unwrap(),
        Counts {
            envelopes_valid: 1,
            memo_entries: 1,
            ..Counts::default()
        }
    );
    assert_eq!(store.envelopes_invalid_total().unwrap(), 0);
}

/// Scenario 5 at the journal level: every entry of a repeated tag is kept, in
/// ordinal order, within one envelope, across transactions and across blocks
/// (spec 5.3). No first-entry-wins.
#[test]
fn duplicate_tags_are_all_kept_in_ordinal_order() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    // Tag 7 repeated: twice in one envelope, in a later transaction, and in
    // the next block, with different ciphertexts. Tag 8 is used both as a
    // follow-up and a first-contact tag, and repeated in first contact.
    chain.push(vec![
        Memo::Valid(vec![follow_up(7, 1), follow_up(7, 2), first_contact(8, 3)]),
        Memo::Valid(vec![follow_up(8, 4), follow_up(7, 5)]),
    ]);
    chain.push_at(vec![
        (4, Memo::Valid(vec![first_contact(8, 6), follow_up(7, 1)])),
        (6, Memo::Valid(vec![first_contact(8, 3)])),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    chain.sync(&mut store);
    chain.oracle(chain.tip_height()).check(&store);
    let memo = store.memo_entries(0).unwrap();
    let sevens: Vec<(Ordinal, [u8; 128])> = memo
        .iter()
        .filter(|r| r.tag == tag(7))
        .map(|r| (r.ordinal, r.ciphertext))
        .collect();
    let ord = |height, tx_index, entry_index| Ordinal {
        height,
        tx_index,
        entry_index,
    };
    assert_eq!(
        sevens,
        [
            (ord(8, 1, 0), filler(1, 1)),
            (ord(8, 1, 1), filler(2, 1)),
            (ord(8, 2, 1), filler(5, 1)),
            (ord(9, 4, 1), filler(1, 1)),
        ]
    );
    let fc = store.first_contact_entries(0).unwrap();
    assert_eq!(
        fc.iter().map(|r| (r.ordinal, r.tag)).collect::<Vec<_>>(),
        [
            (ord(8, 1, 2), tag(8)),
            (ord(9, 4, 0), tag(8)),
            (ord(9, 6, 0), tag(8))
        ]
    );
    // Identical content at two ordinals is two entries.
    assert_eq!(fc[0].entry(), fc[2].entry());
    assert!(memo.windows(2).all(|w| w[0].ordinal < w[1].ordinal));
}

/// Wallet-state entries are counted and never stored as entries.
#[test]
fn wallet_state_entries_are_counted_not_indexed() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    chain.push(vec![
        Memo::Valid(vec![wallet_state(1, 0), wallet_state(1, 1024)]),
        Memo::Valid(vec![
            wallet_state(2, 17),
            follow_up(3, 3),
            wallet_state(4, 5),
        ]),
    ]);
    let indexed = chain.indexed(ACTIVATION);
    assert_eq!(indexed.state_entries, 4);
    assert_eq!(indexed.memo.len(), 1);
    // The follow-up keeps its envelope position, wallet-state entries
    // included.
    assert_eq!(indexed.memo[0].ordinal.entry_index, 1);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    chain.sync(&mut store);
    chain.oracle(chain.tip_height()).check(&store);
    assert_eq!(store.state_entries_total().unwrap(), 4);
    assert_eq!(store.memo_entries(0).unwrap().len(), 1);
    assert!(store.first_contact_entries(0).unwrap().is_empty());
    assert_eq!(
        store.epoch_summary(0).unwrap().unwrap().counts,
        Counts {
            envelopes_valid: 2,
            memo_entries: 1,
            state_entries: 4,
            ..Counts::default()
        }
    );
}

/// The generic `entries(kind, scope)` read agrees with the typed reads and
/// yields the protocol crate's entry layouts.
#[test]
fn entries_by_kind_and_scope() {
    let binding = hand_binding();
    let chain = mixed_chain(&binding, 12);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    chain.sync(&mut store);
    for epoch in 0..3 {
        let generic: Vec<Record> = store.entries(Kind::Memo, epoch).unwrap().collect();
        let typed = store.memo_entries(epoch).unwrap();
        assert_eq!(generic.len(), typed.len());
        for (g, t) in generic.iter().zip(&typed) {
            assert_eq!(g, &Record::Memo(t.clone()));
            assert_eq!(g.ordinal(), t.ordinal);
            assert_eq!(g.tag(), &t.tag);
            let Entry::Memo(entry) = g.entry() else {
                panic!("not a memo entry")
            };
            assert_eq!(entry.key[..], t.tag[..16]);
            assert_eq!(entry.ciphertext, t.ciphertext);
        }
    }
    // The open span starts at epoch 0 (nothing has closed it).
    let fc: Vec<Record> = store.entries(Kind::FirstContact, 0).unwrap().collect();
    assert_eq!(fc.len(), 2 * (1 + 2 + 1) + 1 + 2);
    for record in &fc {
        let Entry::FirstContact(entry) = record.entry() else {
            panic!("not a first-contact entry")
        };
        assert_eq!(entry.key[..], record.tag()[..16]);
        assert_eq!(
            entry.kem_ciphertext[..],
            filler::<1088>(
                u32::from_be_bytes(record.tag()[1..5].try_into().unwrap()),
                2
            )[..]
        );
    }
    // A memo epoch with no stored block is empty; a first-contact scope that
    // starts no span is refused.
    assert_eq!(store.entries(Kind::Memo, 99).unwrap().len(), 0);
    for scope in [1, 2, 99] {
        assert!(matches!(
            store.entries(Kind::FirstContact, scope),
            Err(Error::UnknownSpan(s)) if s == scope
        ));
    }
}

/// Every committed append survives dropping and reopening the connection, and
/// appending continues from the reopened tip.
#[test]
fn appends_are_durable_across_reopen() {
    let binding = hand_binding();
    let chain = mixed_chain(&binding, 30);
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    chain.append(&mut store, ACTIVATION, 20);
    let before = Snapshot::of(&store, 40);
    drop(store);
    let mut store = open(&path, &binding);
    assert_eq!(Snapshot::of(&store, 40), before);
    chain.sync(&mut store);
    drop(store);
    let store = open(&path, &binding);
    chain.oracle(chain.tip_height()).check(&store);
    assert_eq!(store.committed(), Some(chain.id(chain.tip_height())));
    assert_eq!(store.binding(), &binding);
}

/// Rewinding into the middle of an epoch and re-appending the same blocks
/// reproduces the store exactly; a store built in several sessions with
/// rewinds equals one built in one (spec 6.5 determinism).
#[test]
fn rewind_and_reappend_reproduce_the_store() {
    let binding = hand_binding();
    let chain = mixed_chain(&binding, 40);
    let tip = chain.tip_height();
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&dir.path().join("a.sqlite"), &binding);
    chain.sync(&mut store);
    let full = Snapshot::of(&store, tip + 2);
    // Height 30 is inside epoch 5 (heights 28..=31).
    let keep = store.checkpoint_at_or_below(30).unwrap().unwrap();
    assert_eq!(keep, chain.id(30));
    store.rewind(Some(&keep)).unwrap();
    assert_eq!(store.committed(), Some(keep));
    chain.oracle(30).check(&store);
    for height in 31..=tip {
        assert_eq!(store.block_hash(height).unwrap(), None);
    }
    chain.append(&mut store, 31, tip);
    assert_eq!(Snapshot::of(&store, tip + 2), full);

    let path = dir.path().join("b.sqlite");
    let mut other = open(&path, &binding);
    chain.append(&mut other, ACTIVATION, 17);
    drop(other);
    let mut other = open(&path, &binding);
    chain.append(&mut other, 18, 41);
    // Across an epoch boundary: 22 is in epoch 3, 41 in epoch 8.
    other.rewind(Some(&chain.id(22))).unwrap();
    chain.oracle(22).check(&other);
    drop(other);
    let mut other = open(&path, &binding);
    chain.sync(&mut other);
    assert_eq!(Snapshot::of(&other, tip + 2), full);
}

/// After a reorg the store rewinds to the fork point and follows the new
/// branch; blocks of the new branch are refused until it does.
#[test]
fn rewind_follows_a_reorg() {
    let binding = hand_binding();
    let mut chain = mixed_chain(&binding, 32);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    chain.sync(&mut store);
    let old_tip = chain.tip_height();
    let before = Snapshot::of(&store, old_tip + 5);
    chain.truncate(30);
    chain.push(vec![Memo::Valid(vec![
        follow_up(5000, 1),
        first_contact(5001, 2),
    ])]);
    chain.push(vec![Memo::Invalid(vec![9; 9], "junk")]);
    // The new branch ends one block above the old tip.
    for _ in 0..8 {
        chain.push(vec![Memo::Valid(vec![follow_up(5002, 3)])]);
    }
    assert_eq!(chain.tip_height(), old_tip + 1);
    // The new block 31 names block 30, not the stored tip.
    assert!(matches!(
        store.append(&chain.indexed(31)),
        Err(Error::HeightGap { height: 31, .. })
    ));
    // The new block at the old tip + 1 does not name the stored tip.
    assert!(matches!(
        store.append(&chain.indexed(old_tip + 1)),
        Err(Error::ParentMismatch { .. })
    ));
    assert_eq!(Snapshot::of(&store, old_tip + 5), before);
    let fork = chain.id(30);
    assert_eq!(store.checkpoint_at_or_below(30).unwrap(), Some(fork));
    store.rewind(Some(&fork)).unwrap();
    chain.sync(&mut store);
    chain.oracle(chain.tip_height()).check(&store);
}

/// A rewind to `None` empties the store, which accepts the activation block
/// again.
#[test]
fn rewind_to_none_empties_the_store() {
    let binding = hand_binding();
    let chain = mixed_chain(&binding, 20);
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    chain.sync(&mut store);
    store.rewind(None).unwrap();
    let empty = |store: &Store| {
        assert_eq!(store.committed(), None);
        assert_eq!(store.epochs().unwrap(), Vec::<u32>::new());
        assert_eq!(store.spans().unwrap(), Vec::new());
        assert_eq!(store.totals().unwrap(), Counts::default());
        assert_eq!(store.checkpoint_at_or_below(u32::MAX).unwrap(), None);
        for height in 0..=30 {
            assert_eq!(store.block_hash(height).unwrap(), None);
        }
        for epoch in 0..6 {
            assert_eq!(store.epoch_summary(epoch).unwrap(), None);
            assert!(store.memo_entries(epoch).unwrap().is_empty());
        }
        assert!(store.first_contact_entries(0).unwrap().is_empty());
    };
    empty(&store);
    drop(store);
    let mut store = open(&path, &binding);
    empty(&store);
    store.rewind(None).unwrap();
    empty(&store);
    chain.sync(&mut store);
    chain.oracle(chain.tip_height()).check(&store);
}

/// A rewind target must be a stored block, by height and hash; rewinding to
/// the tip changes nothing.
#[test]
fn rewind_requires_a_stored_block() {
    let binding = hand_binding();
    let chain = mixed_chain(&binding, 12);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    chain.sync(&mut store);
    let before = Snapshot::of(&store, 25);
    for target in [
        BlockId {
            height: 15,
            hash: Hash::from_protocol_bytes([9; 32]),
        },
        BlockId {
            height: ACTIVATION - 1,
            hash: hand_hash(ACTIVATION - 1, 0),
        },
        BlockId {
            height: chain.tip_height() + 1,
            hash: chain.id(15).hash,
        },
    ] {
        match store.rewind(Some(&target)) {
            Err(Error::UnknownCheckpoint(id)) => assert_eq!(id, target),
            other => panic!("rewind to {target:?}: {other:?}"),
        }
        assert_eq!(Snapshot::of(&store, 25), before);
    }
    let tip = store.committed().unwrap();
    store.rewind(Some(&tip)).unwrap();
    assert_eq!(Snapshot::of(&store, 25), before);
}

/// Every stored block is a checkpoint; heights below the activation block have
/// none.
#[test]
fn checkpoints_and_block_hashes() {
    let binding = hand_binding();
    let chain = mixed_chain(&binding, 14);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    chain.sync(&mut store);
    let tip = chain.tip_height();
    for height in 0..ACTIVATION {
        assert_eq!(store.checkpoint_at_or_below(height).unwrap(), None);
        assert_eq!(store.block_hash(height).unwrap(), None);
    }
    for height in ACTIVATION..=tip {
        assert_eq!(
            store.checkpoint_at_or_below(height).unwrap(),
            Some(chain.id(height))
        );
        assert_eq!(
            store.block_hash(height).unwrap(),
            Some(chain.id(height).hash)
        );
    }
    for height in [tip + 1, 1000, u32::MAX] {
        assert_eq!(
            store.checkpoint_at_or_below(height).unwrap(),
            Some(chain.id(tip))
        );
        assert_eq!(store.block_hash(height).unwrap(), None);
    }
}

/// Every journal starts at the activation height (spec 7.3): an empty store
/// refuses any other first block.
#[test]
fn the_journal_starts_at_the_activation_height() {
    let binding = hand_binding();
    let schedule = binding.schedule().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    let mut chain = HandChain::new(&binding);
    chain.push_empty(3);
    for height in [ACTIVATION + 1, ACTIVATION + 2] {
        assert!(matches!(
            store.append(&chain.indexed(height)),
            Err(Error::NotAtActivation { height: h, activation: ACTIVATION }) if h == height
        ));
    }
    // A block below activation is in no epoch and is refused too.
    let mut below = chain.indexed(ACTIVATION);
    below.id.height = ACTIVATION - 1;
    below.epoch = schedule.epoch_of(ACTIVATION - 1);
    assert_eq!(below.epoch, None);
    assert!(matches!(
        store.append(&below),
        Err(Error::NotAtActivation {
            height: 7,
            activation: ACTIVATION
        })
    ));
    // With entries, the block's entries are refused first.
    let mut entries = HandChain::new(&binding);
    entries.push(vec![Memo::Valid(vec![follow_up(1, 1)])]);
    let mut below = entries.indexed(ACTIVATION);
    below.id.height = ACTIVATION - 1;
    below.epoch = None;
    below.memo[0].ordinal.height = ACTIVATION - 1;
    assert!(matches!(
        store.append(&below),
        Err(Error::Entry {
            fault: EntryFault::BelowActivation,
            ..
        })
    ));
    assert_eq!(store.committed(), None);
    chain.sync(&mut store);
    assert_eq!(store.committed(), Some(chain.id(ACTIVATION + 2)));
}

/// A block that does not extend the tip by height and parent is refused and
/// nothing is written.
#[test]
fn wrong_parent_or_height_is_refused() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    chain.push(vec![Memo::Valid(vec![follow_up(1, 1)])]);
    chain.push(vec![Memo::Valid(vec![first_contact(2, 2)])]);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    chain.sync(&mut store);
    let tip = store.committed().unwrap();
    let before = Snapshot::of(&store, 20);
    let mut next = chain.clone();
    next.push(vec![Memo::Valid(vec![follow_up(3, 3)])]);
    let attempt = |height: u32, prev: Hash| {
        let mut block = next.indexed(tip.height + 1);
        block.id.height = height;
        block.epoch = next.schedule.epoch_of(height);
        for record in &mut block.memo {
            record.ordinal.height = height;
        }
        block.prev = prev;
        block
    };
    for height in [tip.height + 2, tip.height, tip.height - 1, tip.height + 100] {
        assert!(
            matches!(store.append(&attempt(height, tip.hash)), Err(Error::HeightGap { tip: t, height: h }) if t == tip && h == height),
            "height {height}"
        );
    }
    let wrong = Hash::from_protocol_bytes([0xee; 32]);
    assert!(matches!(
        store.append(&attempt(tip.height + 1, wrong)),
        Err(Error::ParentMismatch { tip: t, prev, .. }) if t == tip && prev == wrong
    ));
    assert_eq!(Snapshot::of(&store, 20), before);
    store.append(&attempt(tip.height + 1, tip.hash)).unwrap();
}

/// A block whose epoch disagrees with the store's schedule (built with
/// another schedule) is refused.
#[test]
fn an_epoch_from_another_schedule_is_refused() {
    let binding = hand_binding();
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    let mut chain = HandChain::new(&binding);
    chain.push_empty(7);
    chain.append(&mut store, ACTIVATION, ACTIVATION + 5);
    let other = EpochSchedule::new(ACTIVATION, 2).unwrap();
    let block = IndexedBlock::new(&chain.blocks[6].block, &other);
    match store.append(&block) {
        Err(Error::EpochMismatch {
            height,
            claimed,
            expected,
        }) => assert_eq!(
            (height, claimed, expected),
            (ACTIVATION + 6, Some(3), Some(1))
        ),
        other => panic!("{other:?}"),
    }
    assert_eq!(store.committed(), Some(chain.id(ACTIVATION + 5)));
}

/// A hand-built block whose entries could not come from a valid envelope is
/// refused and nothing is written.
#[test]
fn malformed_hand_built_entries_are_refused() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    chain.push(vec![
        Memo::Valid(vec![follow_up(1, 1), first_contact(2, 2), follow_up(3, 3)]),
        Memo::Valid(vec![follow_up(4, 4), first_contact(5, 5)]),
    ]);
    let good = chain.indexed(ACTIVATION);
    assert_eq!((good.memo.len(), good.first_contact.len()), (3, 2));
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    type Tamper = fn(&mut IndexedBlock);
    let cases: [(Tamper, Kind, EntryFault); 6] = [
        (
            |b| b.memo[1].ordinal.height += 1,
            Kind::Memo,
            EntryFault::ForeignHeight,
        ),
        (|b| b.memo.swap(0, 1), Kind::Memo, EntryFault::Order),
        (
            |b| b.first_contact[1] = b.first_contact[0].clone(),
            Kind::FirstContact,
            EntryFault::Order,
        ),
        (
            |b| b.memo[2].tag[..16].fill(0),
            Kind::Memo,
            EntryFault::ZeroKey,
        ),
        (
            |b| b.first_contact[0].tag[..16].fill(0),
            Kind::FirstContact,
            EntryFault::ZeroKey,
        ),
        // A first-contact entry at a follow-up entry's ordinal.
        (
            |b| b.first_contact[0].ordinal = b.memo[0].ordinal,
            Kind::FirstContact,
            EntryFault::Order,
        ),
    ];
    for (tamper, kind, fault) in cases {
        let mut block = good.clone();
        tamper(&mut block);
        match store.append(&block) {
            Err(Error::Entry {
                kind: k, fault: f, ..
            }) => assert_eq!((k, f), (kind, fault)),
            other => panic!("{kind:?} {fault:?}: {other:?}"),
        }
        assert_eq!(store.committed(), None);
    }
    store.append(&good).unwrap();
}

/// A store is bound to its network and schedule: reopening with any field
/// changed is refused and changes nothing.
#[test]
fn a_binding_mismatch_is_refused() {
    let binding = hand_binding();
    let chain = mixed_chain(&binding, 6);
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    chain.sync(&mut store);
    let before = Snapshot::of(&store, 20);
    drop(store);
    let changes: [fn(&mut Binding); 4] = [
        |b| b.chain = "main".to_owned(),
        |b| b.genesis_hash = Hash::from_protocol_bytes([0x5b; 32]),
        |b| b.activation_height += 1,
        |b| b.epoch_length = 4096,
    ];
    for change in changes {
        let mut wrong = binding.clone();
        change(&mut wrong);
        match Store::open(&path, wrong.clone()) {
            Err(Error::BindingMismatch { stored, requested }) => {
                assert_eq!(*stored, binding);
                assert_eq!(*requested, wrong);
            }
            Err(other) => panic!("{other:?}"),
            Ok(_) => panic!("opened with {wrong:?}"),
        }
    }
    assert!(matches!(
        Store::open(
            &path,
            Binding {
                epoch_length: 0,
                ..binding.clone()
            }
        ),
        Err(Error::InvalidBinding(_))
    ));
    assert_eq!(Snapshot::of(&open(&path, &binding), 20), before);
}

/// A file whose spans were decided under other span constants is refused.
#[test]
fn a_span_rule_mismatch_is_refused() {
    let binding = hand_binding();
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    drop(open(&path, &binding));
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("UPDATE binding SET fc_span_max_epochs = 32;")
        .unwrap();
    assert!(matches!(
        Store::open(&path, binding),
        Err(Error::SpanRuleMismatch {
            stored_max_epochs: 32,
            max_epochs: 64,
            stored_target: 3072,
            target: 3072
        })
    ));
}

/// Another SQLite database, or a future schema version, is refused.
#[test]
fn foreign_databases_and_unknown_schemas_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let foreign = dir.path().join("foreign.sqlite");
    rusqlite::Connection::open(&foreign)
        .unwrap()
        .execute_batch("CREATE TABLE other (x INTEGER);")
        .unwrap();
    assert!(matches!(
        Store::open(&foreign, hand_binding()),
        Err(Error::NotAStore)
    ));
    let path = store_path(&dir);
    drop(open(&path, &hand_binding()));
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("PRAGMA user_version=2;")
        .unwrap();
    assert!(matches!(
        Store::open(&path, hand_binding()),
        Err(Error::UnsupportedSchema(2))
    ));
}

/// The store runs in WAL mode with full synchronous commits.
#[test]
fn the_database_is_durable_by_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    assert_eq!(STORE_FILE, "memo-v1.sqlite");
    let store = open(&path, &hand_binding());
    let probe = rusqlite::Connection::open(&path).unwrap();
    let mode: String = probe
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    let version: u32 = probe
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, memo_store::SCHEMA_VERSION);
    drop(store);
}

/// A failure after the block row and some entries were written rolls back the
/// whole block.
#[test]
fn a_failed_append_writes_nothing() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    chain.push(vec![Memo::Valid(vec![follow_up(1, 1)])]);
    chain.push(vec![
        Memo::Valid(vec![follow_up(40, 40), follow_up(41, 41)]),
        Memo::Valid(vec![first_contact(42, 42)]),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    chain.append(&mut store, ACTIVATION, ACTIVATION);
    let before = Snapshot::of(&store, 20);
    // Another connection makes the first-contact insert fail.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER fail BEFORE INSERT ON first_contact_entries WHEN NEW.tag = x'{}'
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
            hex::encode(tag(42))
        ))
        .unwrap();
    let block = chain.indexed(ACTIVATION + 1);
    assert!(matches!(store.append(&block), Err(Error::Sql(_))));
    assert_eq!(Snapshot::of(&store, 20), before);
    drop(store);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TRIGGER fail;")
        .unwrap();
    let mut store = open(&path, &binding);
    assert_eq!(Snapshot::of(&store, 20), before);
    store.append(&block).unwrap();
    chain.oracle(chain.tip_height()).check(&store);
}

/// Entry rows that no longer match their blocks' counts are reported as
/// corruption rather than summarised.
#[test]
fn entries_missing_from_their_blocks_are_reported_corrupt() {
    let binding = hand_binding();
    let mut chain = HandChain::new(&binding);
    chain.push(vec![Memo::Valid(vec![
        follow_up(1, 1),
        follow_up(2, 2),
        first_contact(3, 3),
    ])]);
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    chain.sync(&mut store);
    assert!(store.epoch_summary(0).unwrap().is_some());
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "DELETE FROM memo_entries WHERE tag = ?1",
            [tag(2).as_slice()],
        )
        .unwrap();
    assert!(matches!(store.epoch_summary(0), Err(Error::Corrupt(_))));
}

/// One memo epoch of 100,000 follow-up entries is parsed, appended and read
/// back well inside the test budget.
#[test]
fn an_epoch_of_100k_memo_entries_is_cheap_to_read() {
    const BLOCKS: u32 = 100;
    const PER_BLOCK: u32 = 1000;
    const TOTAL: usize = (BLOCKS * PER_BLOCK) as usize;
    let binding = binding_with(4096);
    let mut chain = HandChain::new(&binding);
    for b in 0..BLOCKS {
        let entries: Vec<EnvelopeEntry> = (0..PER_BLOCK)
            .map(|k| {
                let n = b * PER_BLOCK + k;
                // Every 100th entry repeats an earlier tag.
                follow_up(if n % 100 == 99 { n - 50 } else { n }, n)
            })
            .collect();
        chain.push(
            entries
                .chunks(16)
                .map(|chunk| Memo::Valid(chunk.to_vec()))
                .collect(),
        );
    }
    let oracle = chain.oracle(chain.tip_height());
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    let started = Instant::now();
    chain.sync(&mut store);
    let appended = started.elapsed();
    drop(store);
    let mut store = open(&path, &binding);

    let started = Instant::now();
    let records = store.memo_entries(0).unwrap();
    let read_time = started.elapsed();
    let started = Instant::now();
    let summary = store.epoch_summary(0).unwrap().unwrap();
    let totals = store.totals().unwrap();
    let summary_time = started.elapsed();
    eprintln!(
        "100k memo epoch: parse+append {appended:?}, memo_entries {read_time:?}, \
         summary+totals {summary_time:?}"
    );
    assert_eq!(records.len(), TOTAL);
    assert_eq!(memo_rows(&records), oracle.memo[&0]);
    assert_eq!(
        summary,
        EpochSummary {
            epoch: 0,
            first_height: ACTIVATION,
            last_height: ACTIVATION + BLOCKS - 1,
            counts: Counts {
                envelopes_valid: u64::from(BLOCKS) * 63,
                memo_entries: TOTAL as u64,
                ..Counts::default()
            },
        }
    );
    assert_eq!(totals, summary.counts);
    let reads = read_time + summary_time;
    assert!(reads.as_secs_f64() < 10.0, "reads took {reads:?}");

    let keep = chain.id(ACTIVATION + BLOCKS / 2 - 1);
    let started = Instant::now();
    store.rewind(Some(&keep)).unwrap();
    eprintln!("100k memo epoch: rewind of half {:?}", started.elapsed());
    let kept = store.memo_entries(0).unwrap();
    assert_eq!(memo_rows(&kept), oracle.memo[&0][..TOTAL / 2]);
}
