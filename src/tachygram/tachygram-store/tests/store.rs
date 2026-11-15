//! The journal against fixture chains decoded through `tachyon-chain` and
//! hand-built blocks (spec sections 8.1 to 8.6).
mod common;

use common::*;
use std::time::Instant;
use tachygram_store::{
    Binding, EpochSummary, Error, IndexedBlock, IntegrityFault, ProofStamp, STORE_FILE,
    StampOrdinal, Store,
};
use tachyon_chain::{BlockId, Hash};
use tachyon_fixtures::{BlockSpec, TxSpec};

fn store_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join(STORE_FILE)
}

/// A fixture chain appended from the activation height matches the
/// independent oracle in every read, and stamp ordinals restart at zero at
/// every epoch boundary.
#[test]
fn appended_fixture_chain_matches_the_oracle_across_epochs() {
    let chain = fixture_chain(11, 60);
    let binding = fixture_binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    assert_eq!(store.committed(), None);
    assert_eq!(store.epochs().unwrap(), Vec::<u32>::new());
    append_range(&mut store, &chain, ACTIVATION, 60);
    let oracle = Oracle::of(&chain, ACTIVATION, 60, EPOCH_LENGTH);
    oracle.check(&store);
    // The chain is rich enough to show ordinals restarting: several epochs
    // each hold more than one proof stamp, and each starts at ordinal zero.
    let busy: Vec<_> = oracle.stamps.values().filter(|s| s.len() >= 2).collect();
    assert!(busy.len() >= 5, "only {} busy epochs", busy.len());
    for (epoch, stamps) in &oracle.stamps {
        assert_eq!(stamps[0].0, 0, "epoch {epoch} starts at ordinal zero");
        let heights = (
            ACTIVATION + epoch * EPOCH_LENGTH,
            ACTIVATION + epoch * EPOCH_LENGTH + EPOCH_LENGTH - 1,
        );
        assert!(
            stamps
                .iter()
                .all(|s| (heights.0..=heights.1).contains(&s.1))
        );
    }
    assert_eq!(oracle.heights.len(), 14);
    assert_eq!(
        store.committed(),
        Some(BlockId {
            height: 60,
            hash: Hash::from_protocol_bytes(chain.block(60).unwrap().hash())
        })
    );
}

/// The publisher may also start below activation: those blocks are stored in
/// no epoch, carry no stamps and are checkpoints like any other.
#[test]
fn blocks_below_activation_are_stored_in_no_epoch() {
    let chain = fixture_chain(12, 20);
    let binding = fixture_binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    append_range(&mut store, &chain, 0, 20);
    Oracle::of(&chain, 0, 20, EPOCH_LENGTH).check(&store);
    let genesis = Hash::from_protocol_bytes(chain.genesis_hash());
    assert_eq!(store.block_hash(0).unwrap(), Some(genesis));
    assert_eq!(
        store.checkpoint_at_or_below(ACTIVATION - 1).unwrap(),
        Some(BlockId {
            height: ACTIVATION - 1,
            hash: Hash::from_protocol_bytes(chain.block(7).unwrap().hash())
        })
    );
    assert_eq!(store.epochs().unwrap()[0], 0);
}

/// Every committed append survives dropping and reopening the connection, and
/// appending continues from the reopened tip.
#[test]
fn appends_are_durable_across_reopen() {
    let chain = fixture_chain(13, 40);
    let binding = fixture_binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    append_range(&mut store, &chain, ACTIVATION, 25);
    let before = Snapshot::of(&store, 40);
    drop(store);
    let mut store = open(&path, &binding);
    assert_eq!(Snapshot::of(&store, 40), before);
    append_range(&mut store, &chain, 26, 40);
    drop(store);
    let store = open(&path, &binding);
    Oracle::of(&chain, ACTIVATION, 40, EPOCH_LENGTH).check(&store);
    assert_eq!(store.committed().unwrap().height, 40);
    assert_eq!(store.binding(), &binding);
}

/// Rewinding into the middle of an epoch and re-appending the same blocks
/// reproduces the store exactly; a store built in several sessions equals one
/// built in one (spec 6.5 determinism).
#[test]
fn ordinals_after_rewind_and_reappend_are_identical() {
    let chain = fixture_chain(14, 50);
    let binding = fixture_binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&dir.path().join("a.sqlite"), &binding);
    append_range(&mut store, &chain, ACTIVATION, 50);
    let full = Snapshot::of(&store, 50);
    // Height 30 is inside epoch 5 (heights 28..=31).
    let keep = store.checkpoint_at_or_below(30).unwrap().unwrap();
    assert_eq!(keep.height, 30);
    store.rewind(Some(&keep)).unwrap();
    assert_eq!(store.committed(), Some(keep));
    Oracle::of(&chain, ACTIVATION, 30, EPOCH_LENGTH).check(&store);
    for height in 31..=50 {
        assert_eq!(store.block_hash(height).unwrap(), None);
    }
    append_range(&mut store, &chain, 31, 50);
    assert_eq!(Snapshot::of(&store, 50), full);

    // Another cadence: several sessions, a rewind across an epoch boundary.
    let path = dir.path().join("b.sqlite");
    let mut other = open(&path, &binding);
    append_range(&mut other, &chain, ACTIVATION, 17);
    drop(other);
    let mut other = open(&path, &binding);
    append_range(&mut other, &chain, 18, 41);
    let keep = other.checkpoint_at_or_below(22).unwrap().unwrap();
    other.rewind(Some(&keep)).unwrap();
    drop(other);
    let mut other = open(&path, &binding);
    append_range(&mut other, &chain, 23, 50);
    assert_eq!(Snapshot::of(&other, 50), full);
}

/// After a reorg the store rewinds to the fork point and follows the new
/// branch; blocks of the new branch are refused until it does.
#[test]
fn rewind_follows_a_reorg() {
    let mut chain = fixture_chain(15, 40);
    let binding = fixture_binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    append_range(&mut store, &chain, ACTIVATION, 40);
    chain.reorg_random(33, 9).unwrap();
    let schedule = store.schedule();
    let before = Snapshot::of(&store, 42);
    // The new block 34 names block 33, not the stored tip 40.
    assert!(matches!(
        store.append(&indexed(&chain, 34, &schedule)),
        Err(Error::HeightGap { height: 34, .. })
    ));
    // The new block 41 follows height 40 but not the stored block 40.
    assert!(matches!(
        store.append(&indexed(&chain, 41, &schedule)),
        Err(Error::ParentMismatch { height: 41, .. })
    ));
    assert_eq!(Snapshot::of(&store, 42), before);
    let fork = BlockId {
        height: 33,
        hash: Hash::from_protocol_bytes(chain.block(33).unwrap().hash()),
    };
    assert_eq!(store.checkpoint_at_or_below(33).unwrap(), Some(fork));
    store.rewind(Some(&fork)).unwrap();
    append_range(&mut store, &chain, 34, 42);
    Oracle::of(&chain, ACTIVATION, 42, EPOCH_LENGTH).check(&store);
}

/// A rewind to `None` empties the store, which then accepts a first block at
/// any height again.
#[test]
fn rewind_to_none_empties_the_store() {
    let chain = fixture_chain(16, 30);
    let binding = fixture_binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    append_range(&mut store, &chain, ACTIVATION, 30);
    store.rewind(None).unwrap();
    let empty = |store: &Store| {
        assert_eq!(store.committed(), None);
        assert_eq!(store.epochs().unwrap(), Vec::<u32>::new());
        assert_eq!(store.checkpoint_at_or_below(u32::MAX).unwrap(), None);
        for height in 0..=30 {
            assert_eq!(store.block_hash(height).unwrap(), None);
        }
        for epoch in 0..6 {
            assert_eq!(store.epoch_summary(epoch).unwrap(), None);
            assert!(store.stamps(epoch).unwrap().is_empty());
            assert!(store.epoch_records(epoch).unwrap().is_empty());
            assert_eq!(store.epoch_tachygrams(epoch).unwrap().len(), 0);
        }
    };
    empty(&store);
    drop(store);
    let mut store = open(&path, &binding);
    empty(&store);
    // Rewinding an empty store to None is allowed and changes nothing.
    store.rewind(None).unwrap();
    empty(&store);
    append_range(&mut store, &chain, 20, 30);
    Oracle::of(&chain, 20, 30, EPOCH_LENGTH).check(&store);
}

/// A rewind target must be a stored block, by height and hash.
#[test]
fn rewind_requires_a_stored_block() {
    let chain = fixture_chain(17, 20);
    let binding = fixture_binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    append_range(&mut store, &chain, 10, 20);
    let before = Snapshot::of(&store, 20);
    let stored = store.checkpoint_at_or_below(15).unwrap().unwrap();
    for target in [
        BlockId {
            height: 15,
            hash: Hash::from_protocol_bytes([9; 32]),
        },
        BlockId {
            height: 9,
            hash: Hash::from_protocol_bytes(chain.block(9).unwrap().hash()),
        },
        BlockId {
            height: 21,
            hash: stored.hash,
        },
    ] {
        match store.rewind(Some(&target)) {
            Err(Error::UnknownCheckpoint(id)) => assert_eq!(id, target),
            other => panic!("rewind to {target:?}: {other:?}"),
        }
        assert_eq!(Snapshot::of(&store, 20), before);
    }
    // Rewinding to the tip is a no-op.
    let tip = store.committed().unwrap();
    store.rewind(Some(&tip)).unwrap();
    assert_eq!(Snapshot::of(&store, 20), before);
}

/// Every stored block is a checkpoint; heights below the first stored block
/// have none.
#[test]
fn checkpoints_and_block_hashes() {
    let chain = fixture_chain(18, 30);
    let binding = fixture_binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    append_range(&mut store, &chain, 12, 25);
    let id = |height: u32| BlockId {
        height,
        hash: Hash::from_protocol_bytes(chain.block(u64::from(height)).unwrap().hash()),
    };
    for height in 0..12 {
        assert_eq!(store.checkpoint_at_or_below(height).unwrap(), None);
        assert_eq!(store.block_hash(height).unwrap(), None);
    }
    for height in 12..=25 {
        assert_eq!(
            store.checkpoint_at_or_below(height).unwrap(),
            Some(id(height))
        );
        assert_eq!(store.block_hash(height).unwrap(), Some(id(height).hash));
    }
    for height in [26, 100, u32::MAX] {
        assert_eq!(store.checkpoint_at_or_below(height).unwrap(), Some(id(25)));
        assert_eq!(store.block_hash(height).unwrap(), None);
    }
    assert_eq!(store.committed(), Some(id(25)));
}

/// A block that does not extend the tip by height and parent is refused and
/// nothing is written.
#[test]
fn wrong_parent_or_height_is_refused() {
    let binding = hand_binding();
    let schedule = schedule(&binding);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    let tip = append_hand(
        &mut store,
        ACTIVATION,
        vec![vec![proof(vec![tg(1)])], vec![proof(vec![tg(2)])]],
    );
    let before = Snapshot::of(&store, 20);
    let attempt = |height: u32, prev: Hash| {
        IndexedBlock::new(
            &hand_block(height, prev, vec![proof(vec![tg(99)])]),
            &schedule,
        )
    };
    for (height, prev) in [
        (tip.height + 2, tip.hash),
        (tip.height, tip.hash),
        (tip.height - 1, tip.hash),
        (tip.height + 100, tip.hash),
    ] {
        assert!(
            matches!(store.append(&attempt(height, prev)), Err(Error::HeightGap { tip: t, height: h }) if t == tip && h == height),
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
    let other = tachyon_chain::EpochSchedule::new(ACTIVATION, 2).unwrap();
    let block = hand_block(ACTIVATION + 6, hand_hash(0, 0), vec![proof(vec![tg(1)])]);
    match store.append(&IndexedBlock::new(&block, &other)) {
        Err(Error::EpochMismatch {
            height,
            claimed,
            expected,
        }) => {
            assert_eq!(
                (height, claimed, expected),
                (ACTIVATION + 6, Some(3), Some(1))
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(store.committed(), None);
}

/// A tachygram already stored in the same epoch is an integrity fault naming
/// the first occurrence; nothing of the block is written, across reopen too.
#[test]
fn a_duplicate_in_the_epoch_is_refused() {
    let binding = hand_binding();
    let schedule = schedule(&binding);
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    // Epoch 0 is heights 8..=11.
    let tip = append_hand(
        &mut store,
        ACTIVATION,
        vec![
            vec![proof(vec![tg(1), tg(2)]), pointer(), proof(vec![tg(3)])],
            vec![proof(vec![tg(4)])],
        ],
    );
    let before = Snapshot::of(&store, 20);
    let block = hand_block(
        tip.height + 1,
        tip.hash,
        vec![proof(vec![tg(10), tg(11)]), proof(vec![tg(12), tg(3)])],
    );
    let error = store
        .append(&IndexedBlock::new(&block, &schedule))
        .unwrap_err();
    assert_eq!(
        error.integrity_fault(),
        Some(&IntegrityFault::DuplicateInEpoch {
            epoch: 0,
            height: 10,
            tachygram: tg(3),
            first_height: 8,
            first_stamp_ordinal: StampOrdinal(1),
        })
    );
    assert_eq!(Snapshot::of(&store, 20), before);
    drop(store);
    let mut store = open(&path, &binding);
    assert_eq!(Snapshot::of(&store, 20), before);
    // The same block without the duplicate is accepted, and its stamps take
    // the ordinals the refused block would have taken.
    let block = hand_block(
        tip.height + 1,
        tip.hash,
        vec![proof(vec![tg(10), tg(11)]), proof(vec![tg(12)])],
    );
    store.append(&IndexedBlock::new(&block, &schedule)).unwrap();
    let ordinals: Vec<u32> = store
        .stamps(0)
        .unwrap()
        .iter()
        .map(|s| s.ordinal.get())
        .collect();
    assert_eq!(ordinals, [0, 1, 2, 3, 4]);
}

/// A tachygram listed twice in one block, by two stamps or by one, is an
/// integrity fault and nothing is written.
#[test]
fn a_duplicate_in_the_block_is_refused() {
    let binding = hand_binding();
    let schedule = schedule(&binding);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    let tip = append_hand(&mut store, ACTIVATION, vec![vec![proof(vec![tg(1)])]]);
    let before = Snapshot::of(&store, 20);
    for stamps in [
        vec![proof(vec![tg(5), tg(6)]), proof(vec![tg(7), tg(5)])],
        vec![proof(vec![tg(5), tg(5)])],
    ] {
        let block = hand_block(tip.height + 1, tip.hash, stamps);
        let error = store
            .append(&IndexedBlock::new(&block, &schedule))
            .unwrap_err();
        assert_eq!(
            error.integrity_fault(),
            Some(&IntegrityFault::DuplicateInBlock {
                height: tip.height + 1,
                tachygram: tg(5),
            })
        );
        assert_eq!(Snapshot::of(&store, 20), before);
    }
}

/// An all-zero tachygram is an integrity fault (spec 6.2) and nothing is
/// written.
#[test]
fn an_all_zero_tachygram_is_refused() {
    let binding = hand_binding();
    let schedule = schedule(&binding);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    let tip = append_hand(&mut store, ACTIVATION, vec![vec![proof(vec![tg(1)])]]);
    let before = Snapshot::of(&store, 20);
    let block = hand_block(
        tip.height + 1,
        tip.hash,
        vec![proof(vec![tg(2)]), proof(vec![tg(3), [0; 32], tg(4)])],
    );
    let error = store
        .append(&IndexedBlock::new(&block, &schedule))
        .unwrap_err();
    assert_eq!(
        error.integrity_fault(),
        Some(&IntegrityFault::ZeroTachygram {
            height: tip.height + 1,
            tx_index: 2,
        })
    );
    assert_eq!(Snapshot::of(&store, 20), before);
}

/// A proof stamp below the activation height has no epoch and is refused.
#[test]
fn a_proof_stamp_below_activation_is_refused() {
    let binding = hand_binding();
    let schedule = schedule(&binding);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    let block = hand_block(3, hand_hash(2, 0), vec![pointer(), proof(vec![tg(1)])]);
    let indexed = IndexedBlock::new(&block, &schedule);
    assert_eq!(indexed.epoch, None);
    assert_eq!(
        store.append(&indexed).unwrap_err().integrity_fault(),
        Some(&IntegrityFault::StampBelowActivation {
            height: 3,
            tx_index: 2
        })
    );
    assert_eq!(store.committed(), None);
    // A pointer-only block below activation contributes nothing and is stored.
    let block = hand_block(3, hand_hash(2, 0), vec![pointer()]);
    store.append(&IndexedBlock::new(&block, &schedule)).unwrap();
    assert_eq!(store.committed().unwrap().height, 3);
    assert_eq!(store.epochs().unwrap(), Vec::<u32>::new());
}

/// Stamps must be in strictly ascending transaction order.
#[test]
fn stamps_out_of_transaction_order_are_refused() {
    let binding = hand_binding();
    let schedule = schedule(&binding);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    let block = hand_block(
        ACTIVATION,
        hand_hash(7, 0),
        vec![proof(vec![tg(1)]), proof(vec![tg(2)])],
    );
    let mut indexed = IndexedBlock::new(&block, &schedule);
    indexed.stamps.swap(0, 1);
    assert!(matches!(
        store.append(&indexed),
        Err(Error::StampOrder {
            height: ACTIVATION,
            tx_index: 1
        })
    ));
    indexed.stamps[1].tx_index = indexed.stamps[0].tx_index;
    assert!(matches!(
        store.append(&indexed),
        Err(Error::StampOrder { .. })
    ));
    assert_eq!(store.committed(), None);
}

/// Consensus forbids reusing a tachygram of the current or previous epoch, but
/// the journal's fault is only a duplicate within one epoch (spec 8.3), so the
/// same tachygram in two epochs is stored in both.
#[test]
fn the_same_tachygram_in_different_epochs_is_accepted() {
    let binding = hand_binding();
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    // Epoch 0 is 8..=11, epoch 1 is 12..=15, epoch 2 is 16..=19.
    let mut blocks = vec![Vec::new(); 12];
    blocks[0] = vec![proof(vec![tg(1), tg(2)])];
    blocks[4] = vec![proof(vec![tg(2)])];
    blocks[11] = vec![proof(vec![tg(1), tg(2)])];
    append_hand(&mut store, ACTIVATION, blocks);
    assert_eq!(store.epochs().unwrap(), [0, 1, 2]);
    let records = |epoch| {
        store
            .epoch_records(epoch)
            .unwrap()
            .into_iter()
            .map(|r| (r.tachygram, r.blk_height, r.stamp_ordinal.get()))
            .collect::<Vec<_>>()
    };
    assert_eq!(records(0), [(tg(1), 8, 0), (tg(2), 8, 0)]);
    assert_eq!(records(1), [(tg(2), 12, 0)]);
    assert_eq!(records(2), [(tg(1), 19, 0), (tg(2), 19, 0)]);
}

/// Pointer stamps contribute nothing: no stamp, no ordinal, no tachygram.
#[test]
fn pointer_stamps_contribute_nothing() {
    let binding = hand_binding();
    let schedule = schedule(&binding);
    let block = hand_block(
        ACTIVATION,
        hand_hash(7, 0),
        vec![
            pointer(),
            proof(vec![tg(1), tg(2)]),
            pointer(),
            proof(vec![tg(3)]),
        ],
    );
    let hand = IndexedBlock::new(&block, &schedule);
    assert_eq!(hand.epoch, Some(0));
    assert_eq!(
        hand.stamps,
        [
            ProofStamp {
                tx_index: 2,
                txid: block.txs[1].txid,
                tachygrams: vec![tg(1), tg(2)],
            },
            ProofStamp {
                tx_index: 4,
                txid: block.txs[3].txid,
                tachygrams: vec![tg(3)],
            },
        ]
    );
    assert_eq!(hand.tachygram_count(), 3);

    // Through the fixture chain and the decoder too.
    let mut chain = fixture_chain(19, 9);
    chain.append(
        BlockSpec::new()
            .tx(TxSpec::tachyon(2).pointer())
            .tx(TxSpec::tachyon(1).pointer()),
    );
    let tachygrams = chain.fresh_tachygrams(5);
    chain.append(
        BlockSpec::new()
            .tx(TxSpec::tachyon(1).pointer())
            .tx(TxSpec::tachyon(2).proof(tachygrams.clone())),
    );
    let binding = fixture_binding(&chain, 64);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    let schedule = store.schedule();
    let pointers_only = indexed(&chain, 10, &schedule);
    assert!(pointers_only.stamps.is_empty());
    let mixed = indexed(&chain, 11, &schedule);
    assert_eq!(mixed.stamps.len(), 1);
    assert_eq!(mixed.stamps[0].tx_index, 2);
    assert_eq!(mixed.stamps[0].tachygrams, tachygrams);
    append_range(&mut store, &chain, ACTIVATION, 11);
    Oracle::of(&chain, ACTIVATION, 11, 64).check(&store);
    let stamps = store.stamps(0).unwrap();
    let last = stamps.last().unwrap();
    assert_eq!(
        (last.height, last.tx_index, last.tachygram_count),
        (11, 2, 5)
    );
    assert_eq!(last.ordinal.get() as usize, stamps.len() - 1);
}

/// Tachygram counts are stored as the data gives them, odd and empty counts
/// included, and a stamp's tachygrams keep their order in stamp order while
/// records are sorted.
#[test]
fn odd_and_empty_tachygram_counts_are_stored_as_given() {
    let binding = hand_binding();
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&store_path(&dir), &binding);
    let seventeen: Vec<[u8; 32]> = (100..117).rev().map(tg).collect();
    append_hand(
        &mut store,
        ACTIVATION,
        vec![
            vec![proof(vec![tg(9)]), proof(vec![tg(3), tg(1), tg(2)])],
            vec![proof(Vec::new()), proof(seventeen.clone())],
        ],
    );
    let counts: Vec<(u32, u32, u32)> = store
        .stamps(0)
        .unwrap()
        .iter()
        .map(|s| (s.ordinal.get(), s.height, s.tachygram_count))
        .collect();
    assert_eq!(counts, [(0, 8, 1), (1, 8, 3), (2, 9, 0), (3, 9, 17)]);
    let mut in_stamp_order = vec![(0, tg(9)), (1, tg(3)), (1, tg(1)), (1, tg(2))];
    in_stamp_order.extend(seventeen.iter().map(|t| (3, *t)));
    let got: Vec<(u32, [u8; 32])> = store
        .epoch_tachygrams(0)
        .unwrap()
        .map(|(o, t)| (o.get(), t))
        .collect();
    assert_eq!(got, in_stamp_order);
    let records = store.epoch_records(0).unwrap();
    assert_eq!(records.len(), 21);
    assert!(records.windows(2).all(|w| w[0].tachygram < w[1].tachygram));
    assert_eq!(
        store.epoch_summary(0).unwrap(),
        Some(EpochSummary {
            epoch: 0,
            entries: 21,
            stamps: 4,
            first_height: 8,
            last_height: 9,
        })
    );
}

/// A store is bound to its network and schedule: reopening with any field
/// changed is refused and changes nothing.
#[test]
fn a_binding_mismatch_is_refused() {
    let binding = hand_binding();
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    append_hand(&mut store, ACTIVATION, vec![vec![proof(vec![tg(1)])]]);
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
    let mut zero = binding.clone();
    zero.epoch_length = 0;
    assert!(matches!(
        Store::open(&path, zero),
        Err(Error::InvalidBinding(_))
    ));
    assert!(matches!(
        Store::open(
            dir.path().join("new.sqlite"),
            Binding {
                epoch_length: 0,
                ..binding.clone()
            }
        ),
        Err(Error::InvalidBinding(_))
    ));
    assert_eq!(Snapshot::of(&open(&path, &binding), 20), before);
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
    let store = open(&path, &hand_binding());
    let probe = rusqlite::Connection::open(&path).unwrap();
    let mode: String = probe
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    let version: u32 = probe
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, tachygram_store::SCHEMA_VERSION);
    drop(store);
}

/// A failure after the block row and some tachygrams were written rolls back
/// the whole block.
#[test]
fn a_failed_append_writes_nothing() {
    let binding = hand_binding();
    let schedule = schedule(&binding);
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    let tip = append_hand(&mut store, ACTIVATION, vec![vec![proof(vec![tg(1)])]]);
    let before = Snapshot::of(&store, 20);
    // Another connection makes the last tachygram's insert fail.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER fail BEFORE INSERT ON tachygrams WHEN NEW.tg = x'{}'
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
            hex::encode(tg(42))
        ))
        .unwrap();
    let block = hand_block(
        tip.height + 1,
        tip.hash,
        vec![proof(vec![tg(40), tg(41)]), proof(vec![tg(42)])],
    );
    let indexed = IndexedBlock::new(&block, &schedule);
    assert!(matches!(store.append(&indexed), Err(Error::Sql(_))));
    assert_eq!(Snapshot::of(&store, 20), before);
    drop(store);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TRIGGER fail;")
        .unwrap();
    let mut store = open(&path, &binding);
    assert_eq!(Snapshot::of(&store, 20), before);
    store.append(&indexed).unwrap();
    assert_eq!(store.epoch_summary(0).unwrap().unwrap().entries, 4);
}

/// An epoch of 200,000 tachygrams (about the testnet's rate, spec 3) is
/// appended and read back well inside the test budget.
#[test]
fn an_epoch_of_200k_tachygrams_is_cheap_to_read() {
    const BLOCKS: u32 = 100;
    const PER_STAMP: u32 = 500;
    const STAMPS_PER_BLOCK: u32 = 4;
    const TOTAL: usize = (BLOCKS * PER_STAMP * STAMPS_PER_BLOCK) as usize;
    let binding = Binding {
        epoch_length: 4096,
        ..hand_binding()
    };
    let schedule = schedule(&binding);
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    // Pseudo-random tachygrams (splitmix64), so inserts land all over the key
    // space as real tachygrams do.
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move || {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    };
    let mut random_tg = move || {
        let mut bytes = [0u8; 32];
        for chunk in bytes.chunks_mut(8) {
            chunk.copy_from_slice(&next().to_le_bytes());
        }
        bytes
    };
    let started = Instant::now();
    let mut prev = hand_hash(ACTIVATION - 1, 0);
    let mut expected = Vec::with_capacity(TOTAL);
    for offset in 0..BLOCKS {
        let height = ACTIVATION + offset;
        let stamps = (0..STAMPS_PER_BLOCK)
            .map(|s| {
                let tachygrams: Vec<[u8; 32]> = (0..PER_STAMP).map(|_| random_tg()).collect();
                let ordinal = offset * STAMPS_PER_BLOCK + s;
                expected.extend(tachygrams.iter().map(|t| (StampOrdinal(ordinal), *t)));
                proof(tachygrams)
            })
            .collect();
        let block = hand_block(height, prev, stamps);
        store.append(&IndexedBlock::new(&block, &schedule)).unwrap();
        prev = block.id.hash;
    }
    let appended = started.elapsed();
    drop(store);
    let mut store = open(&path, &binding);

    let started = Instant::now();
    let records = store.epoch_records(0).unwrap();
    let records_time = started.elapsed();
    let started = Instant::now();
    let tachygrams: Vec<_> = store.epoch_tachygrams(0).unwrap().collect();
    let tachygrams_time = started.elapsed();
    let started = Instant::now();
    let summary = store.epoch_summary(0).unwrap().unwrap();
    let stamps = store.stamps(0).unwrap();
    let summary_time = started.elapsed();
    eprintln!(
        "200k epoch: append {appended:?}, epoch_records {records_time:?}, \
         epoch_tachygrams {tachygrams_time:?}, summary+stamps {summary_time:?}"
    );

    assert_eq!(records.len(), TOTAL);
    assert!(records.windows(2).all(|w| w[0].tachygram < w[1].tachygram));
    let mut sorted: Vec<[u8; 32]> = expected.iter().map(|(_, t)| *t).collect();
    sorted.sort_unstable();
    assert!(records.iter().map(|r| r.tachygram).eq(sorted));
    assert!(
        records
            .iter()
            .all(|r| { r.stamp_ordinal.get() / STAMPS_PER_BLOCK + ACTIVATION == r.blk_height })
    );
    assert_eq!(tachygrams, expected);
    assert_eq!(
        summary,
        EpochSummary {
            epoch: 0,
            entries: TOTAL as u64,
            stamps: BLOCKS * STAMPS_PER_BLOCK,
            first_height: ACTIVATION,
            last_height: ACTIVATION + BLOCKS - 1,
        }
    );
    assert_eq!(stamps.len(), (BLOCKS * STAMPS_PER_BLOCK) as usize);
    assert!(stamps.iter().all(|s| s.tachygram_count == PER_STAMP));
    // Reads are what the publisher repeats at every publication of an open
    // epoch; they must stay a small part of the budget.
    let reads = records_time + tachygrams_time + summary_time;
    assert!(reads.as_secs_f64() < 10.0, "reads took {reads:?}");

    // A rewind halfway through the epoch cascades over 100,000 tachygrams.
    let keep = store
        .checkpoint_at_or_below(ACTIVATION + BLOCKS / 2 - 1)
        .unwrap()
        .unwrap();
    let started = Instant::now();
    store.rewind(Some(&keep)).unwrap();
    eprintln!("200k epoch: rewind of half {:?}", started.elapsed());
    let summary = store.epoch_summary(0).unwrap().unwrap();
    assert_eq!(summary.entries, TOTAL as u64 / 2);
    assert_eq!(summary.last_height, keep.height);
    let kept: Vec<_> = store.epoch_tachygrams(0).unwrap().collect();
    assert_eq!(kept, expected[..TOTAL / 2]);
}

/// Tachygram rows that no longer match their stamps' counts are reported as
/// corruption rather than summarised.
#[test]
fn tachygrams_missing_from_their_stamps_are_reported_corrupt() {
    let binding = hand_binding();
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut store = open(&path, &binding);
    append_hand(
        &mut store,
        ACTIVATION,
        vec![vec![proof(vec![tg(1), tg(2)])]],
    );
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("DELETE FROM tachygrams WHERE tg = ?1", [tg(2).as_slice()])
        .unwrap();
    assert!(matches!(store.epoch_summary(0), Err(Error::Corrupt(_))));
}
