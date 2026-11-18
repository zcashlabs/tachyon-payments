//! The journal against `tachyon-fixtures` chains with generated envelopes,
//! served as `getblock <hash> 2` JSON and decoded by `tachyon-chain`, checked
//! against the generator's authored ground truth (spec 5, 7, 13.2 scenarios
//! 5 and 7 at the journal level).
mod common;

use common::*;
use memo_store::{Binding, Counts, IndexedBlock, Ordinal, STORE_FILE, Store};
use std::collections::{BTreeMap, HashMap};
use tachyon_chain::{DecodeOptions, Hash, TachyonBlock, decode_block};
use tachyon_fixtures::json::{self, Placement};
use tachyon_fixtures::{ChainConfig, EnvelopeMix, SyntheticChain, TxMix};

fn binding(chain: &SyntheticChain, epoch_length: u32) -> Binding {
    Binding {
        chain: chain.chain_name().to_owned(),
        genesis_hash: Hash::from_protocol_bytes(chain.genesis_hash()),
        activation_height: u32::try_from(chain.activation_height().unwrap()).unwrap(),
        epoch_length,
    }
}

/// The fixture block at `height`, served as JSON and decoded.
fn decoded(chain: &SyntheticChain, height: u32) -> TachyonBlock {
    let block = chain.block(u64::from(height)).unwrap();
    let value = json::block(block, Placement::of(chain, block, true), 2);
    let hash = Hash::from_protocol_bytes(block.hash());
    decode_block(&value, &hash, Some(height), DecodeOptions::default()).unwrap()
}

/// Appends the fixture chain's blocks the store does not hold, through its
/// tip.
fn sync(store: &mut Store, chain: &SyntheticChain) {
    let schedule = store.schedule();
    let from = store.committed().map_or(ACTIVATION, |tip| tip.height + 1);
    for height in from..=chain.tip_height() as u32 {
        store
            .append(&IndexedBlock::new(&decoded(chain, height), &schedule))
            .unwrap();
    }
}

/// The oracle for the fixture chain from the activation height to its tip,
/// built from the generator's authored truth (never from the memo bytes).
fn oracle(chain: &SyntheticChain, store: &Store) -> Oracle {
    let schedule = store.schedule();
    let tip = chain.tip_height() as u32;
    let mut oracle = Oracle::new(schedule);
    oracle.tip = Some(tip);
    let ordinal = |(height, tx_index, entry_index): (u64, usize, usize)| Ordinal {
        height: height as u32,
        tx_index: tx_index as u32,
        entry_index: entry_index as u8,
    };
    for epoch in 0..=schedule.epoch_of(tip).unwrap() {
        let first = schedule.first_height(epoch).unwrap();
        let last = schedule.last_height(epoch).unwrap().min(tip);
        let heights = u64::from(first)..=u64::from(last);
        oracle.heights.insert(epoch, (first, last));
        let counts = chain.envelope_counts(heights.clone());
        assert_eq!(counts.raw, 0, "every memo is authored");
        oracle.counts.insert(
            epoch,
            Counts {
                envelopes_valid: counts.valid as u64,
                envelopes_invalid: counts.malformed as u64,
                memo_entries: counts.follow_ups as u64,
                first_contact_entries: counts.first_contacts as u64,
                state_entries: counts.wallet_states as u64,
            },
        );
        let memo: Vec<MemoRow> = chain
            .follow_ups(heights.clone())
            .into_iter()
            .map(|t| (ordinal(t.ordinal()), t.tag, t.ciphertext))
            .collect();
        if !memo.is_empty() {
            oracle.memo.insert(epoch, memo);
        }
        let first_contact: Vec<FirstContactRow> = chain
            .first_contacts(heights)
            .into_iter()
            .map(|t| {
                (
                    ordinal(t.ordinal()),
                    t.tag,
                    t.kem_ciphertext.to_vec(),
                    t.ciphertext,
                )
            })
            .collect();
        if !first_contact.is_empty() {
            oracle.first_contact.insert(epoch, first_contact);
        }
    }
    oracle
}

fn envelope_chain(seed: u64, blocks: u64, txs: [u32; 2], mix: EnvelopeMix) -> SyntheticChain {
    let config = ChainConfig {
        mix: TxMix {
            tachyon_txs: txs,
            ..TxMix::default()
        },
        envelopes: Some(mix),
        ..ChainConfig::with_seed(seed)
    };
    SyntheticChain::generate(config, blocks)
}

/// A generated chain of valid, malformed and empty memos with repeated tags
/// and wallet-state entries is stored exactly as authored, across epochs.
#[test]
fn a_generated_chain_matches_the_authored_truth() {
    let mix = EnvelopeMix {
        envelope_percent: 85,
        malformed_percent: 20,
        follow_ups: [0, 6],
        first_contacts: [0, 3],
        wallet_states: [0, 2],
        repeat_tag_percent: 25,
        ..EnvelopeMix::default()
    };
    let chain = envelope_chain(21, 80, [0, 5], mix);
    let binding = binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&dir.path().join(STORE_FILE), &binding);
    sync(&mut store, &chain);
    let oracle = oracle(&chain, &store);
    oracle.check(&store);
    // The chain exercises what it is meant to: malformed memos (scenario 7),
    // repeated tags (scenario 5) and wallet-state entries.
    let totals = oracle.totals();
    assert!(totals.envelopes_invalid >= 10, "{totals:?}");
    assert!(totals.state_entries >= 10, "{totals:?}");
    assert!(totals.memo_entries >= 100 && totals.first_contact_entries >= 20);
    let mut seen: HashMap<[u8; 32], usize> = HashMap::new();
    for rows in oracle.memo.values() {
        for row in rows {
            *seen.entry(row.1).or_default() += 1;
        }
    }
    assert!(seen.values().any(|&n| n >= 2), "no repeated tag");
    assert_eq!(
        store.envelopes_invalid_total().unwrap(),
        totals.envelopes_invalid
    );
    assert_eq!(store.state_entries_total().unwrap(), totals.state_entries);
}

/// After a reorg of the generated chain the store rewinds to the fork and
/// matches the new branch's truth.
#[test]
fn a_generated_chain_survives_a_reorg() {
    let mut chain = envelope_chain(22, 50, [0, 5], EnvelopeMix::default());
    let binding = binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(STORE_FILE);
    let mut store = open(&path, &binding);
    sync(&mut store, &chain);
    oracle(&chain, &store).check(&store);
    chain.reorg_random(37, 12).unwrap();
    let fork = store.checkpoint_at_or_below(37).unwrap().unwrap();
    assert_eq!(
        fork.hash,
        Hash::from_protocol_bytes(chain.block(37).unwrap().hash())
    );
    store.rewind(Some(&fork)).unwrap();
    drop(store);
    let mut store = open(&path, &binding);
    sync(&mut store, &chain);
    oracle(&chain, &store).check(&store);
}

/// Spans decided over a generated chain heavy in first-contact entries match
/// the rule recomputed from the authored counts; one-block epochs make every
/// block a boundary.
#[test]
fn generated_first_contact_spans_follow_the_rule() {
    let mix = EnvelopeMix {
        envelope_percent: 100,
        malformed_percent: 5,
        follow_ups: [0, 0],
        first_contacts: [10, 16],
        wallet_states: [0, 0],
        repeat_tag_percent: 5,
        ..EnvelopeMix::default()
    };
    let chain = envelope_chain(23, 160, [6, 10], mix);
    let binding = binding(&chain, 1);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&dir.path().join(STORE_FILE), &binding);
    sync(&mut store, &chain);
    let oracle = oracle(&chain, &store);
    oracle.check(&store);
    let spans = store.spans().unwrap();
    let closed_by_count = spans
        .iter()
        .filter(|s| s.is_closed() && s.entries >= memo_store::FC_SPAN_TARGET)
        .count();
    assert!(closed_by_count >= 3, "{spans:?}");
    // Each closed span's previous epoch boundary was below the target.
    let per_epoch: BTreeMap<u32, u64> = oracle
        .counts
        .iter()
        .map(|(e, c)| (*e, c.first_contact_entries))
        .collect();
    for span in spans.iter().filter(|s| s.is_closed()) {
        let last = span.last_epoch.unwrap();
        let before: u64 = (span.first_epoch..last).map(|e| per_epoch[&e]).sum();
        assert!(before < memo_store::FC_SPAN_TARGET);
        assert_eq!(before + per_epoch[&last], span.entries);
    }
}

/// Without the envelope generator a fixture chain's memos are random bytes:
/// each non-empty one is an invalid envelope, counted, and nothing is stored.
#[test]
fn random_memo_bytes_are_counted_invalid() {
    let config = ChainConfig {
        mix: TxMix {
            tachyon_txs: [1, 4],
            memo_percent: 60,
            ..TxMix::default()
        },
        ..ChainConfig::with_seed(24)
    };
    let chain = SyntheticChain::generate(config, 40);
    let binding = binding(&chain, EPOCH_LENGTH);
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&dir.path().join(STORE_FILE), &binding);
    sync(&mut store, &chain);
    let mut nonempty = 0u64;
    for height in ACTIVATION..=chain.tip_height() as u32 {
        for tx in chain.block(u64::from(height)).unwrap().tachyon_txs() {
            if !tx.memo.is_empty() {
                // A precondition on the authored bytes, not a parse: random
                // bytes that began with the magic could be an envelope.
                assert!(!tx.memo.starts_with(b"TPM1"));
                nonempty += 1;
            }
        }
    }
    assert!(nonempty >= 20);
    assert_eq!(
        store.totals().unwrap(),
        Counts {
            envelopes_invalid: nonempty,
            ..Counts::default()
        }
    );
    for epoch in store.epochs().unwrap() {
        assert!(store.memo_entries(epoch).unwrap().is_empty());
    }
    assert!(store.first_contact_entries(0).unwrap().is_empty());
}
