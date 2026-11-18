//! The synthetic envelope generator (spec section 5, scenarios 5 to 7): byte
//! layout against hand-written bytes, malformations against a structural walk
//! written here, the node double serving the authored bytes, the ordinal
//! ground truth, repeated tags and ground keys, the binary's envelope specs,
//! determinism, and a cross-check against the products' parser.
mod common;

use common::{block_v2, client, control};
use pir_node_rpc::Credentials;
use serde_json::json;
use std::collections::{BTreeMap, HashSet};
use std::process::Stdio;
use std::time::Duration;
use tachyon_fixtures::envelope::{self, tag_with_key16};
use tachyon_fixtures::{
    AuthoredEntry, AuthoredEnvelope, AuthoredMemo, BlockSpec, ChainConfig, ChainSpec, EntryKind,
    EnvelopeMix, Malformation, NodeDouble, NodeOptions, Rng, SyntheticChain, TxJson, TxSpec,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

// ---------------------------------------------------------------------------
// An independent structural walk of spec 5.1, written for these tests only.
// ---------------------------------------------------------------------------

/// Which rules the walk enforces; turning one off shows a malformation breaks
/// that rule and no other.
#[derive(Clone, Copy)]
struct Rules {
    magic: bool,
    count: bool,
    zero_key: bool,
    state_len: bool,
}

const ALL_RULES: Rules = Rules {
    magic: true,
    count: true,
    zero_key: true,
    state_len: true,
};

#[derive(Debug, PartialEq, Eq)]
enum Violation {
    ShortMagic,
    BadMagic,
    MissingCount,
    Count(u8),
    /// The input ends inside entry `index`'s kind or tag.
    TruncatedHeader {
        index: usize,
    },
    UnknownKind {
        index: usize,
        offset: usize,
    },
    ZeroKey {
        index: usize,
        kind: u8,
    },
    StateTooLong {
        index: usize,
        len: usize,
    },
    /// The input ends inside entry `index`'s body.
    TruncatedBody {
        index: usize,
    },
    Trailing {
        offset: usize,
    },
}

/// A walked entry: kind, tag and body bytes.
type Walked = (u8, [u8; 32], Vec<u8>);

fn walk(bytes: &[u8], rules: Rules) -> Result<Vec<Walked>, Violation> {
    if bytes.len() < 4 {
        return Err(Violation::ShortMagic);
    }
    if rules.magic && &bytes[..4] != b"TPM1" {
        return Err(Violation::BadMagic);
    }
    let Some(&count) = bytes.get(4) else {
        return Err(Violation::MissingCount);
    };
    if rules.count && !(1..=16).contains(&count) {
        return Err(Violation::Count(count));
    }
    let mut at = 5;
    let mut out = Vec::new();
    for index in 0..count as usize {
        if bytes.len() < at + 33 {
            return Err(Violation::TruncatedHeader { index });
        }
        let kind = bytes[at];
        let tag: [u8; 32] = bytes[at + 1..at + 33].try_into().unwrap();
        let body_start = at + 33;
        let body_len = match kind {
            1 => 128,
            2 => 1088 + 128,
            3 => {
                if bytes.len() < body_start + 2 {
                    return Err(Violation::TruncatedBody { index });
                }
                let len = bytes[body_start] as usize | (bytes[body_start + 1] as usize) << 8;
                if rules.state_len && len > 1024 {
                    return Err(Violation::StateTooLong { index, len });
                }
                2 + len
            }
            _ => return Err(Violation::UnknownKind { index, offset: at }),
        };
        if rules.zero_key && kind != 3 && tag[..16] == [0; 16] {
            return Err(Violation::ZeroKey { index, kind });
        }
        if bytes.len() < body_start + body_len {
            return Err(Violation::TruncatedBody { index });
        }
        out.push((kind, tag, bytes[body_start..body_start + body_len].to_vec()));
        at = body_start + body_len;
    }
    if at != bytes.len() {
        return Err(Violation::Trailing { offset: at });
    }
    Ok(out)
}

/// The authored entries as walked triples, written independently of the
/// crate's encoder.
fn expected_walk(envelope: &AuthoredEnvelope) -> Vec<Walked> {
    envelope
        .entries
        .iter()
        .map(|entry| match entry {
            AuthoredEntry::FollowUp { tag, ciphertext } => (1, *tag, ciphertext.to_vec()),
            AuthoredEntry::FirstContact {
                tag,
                kem_ciphertext,
                ciphertext,
            } => (2, *tag, [&kem_ciphertext[..], &ciphertext[..]].concat()),
            AuthoredEntry::WalletState { tag, ciphertext } => {
                let len = ciphertext.len() as u16;
                (3, *tag, [&len.to_le_bytes()[..], ciphertext].concat())
            }
        })
        .collect()
}

fn cat(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}

// ---------------------------------------------------------------------------
// Byte layout.
// ---------------------------------------------------------------------------

#[test]
fn each_kind_encodes_to_the_spec_bytes() {
    let follow_up = AuthoredEntry::FollowUp {
        tag: [0x11; 32],
        ciphertext: [0x22; 128],
    };
    let first_contact = AuthoredEntry::FirstContact {
        tag: [0x33; 32],
        kem_ciphertext: [0x44; 1088],
        ciphertext: [0x55; 128],
    };
    let state = |len: usize| AuthoredEntry::WalletState {
        tag: [0x66; 32],
        ciphertext: vec![0x77; len],
    };

    let one = AuthoredEnvelope::new(vec![follow_up.clone()]).to_bytes();
    assert_eq!(
        one,
        cat(&[b"TPM1", &[0x01], &[0x01], &[0x11; 32], &[0x22; 128]])
    );
    assert_eq!(one.len(), 4 + 1 + 1 + 32 + 128);

    let fc = AuthoredEnvelope::new(vec![first_contact.clone()]).to_bytes();
    assert_eq!(
        fc,
        cat(&[
            b"TPM1",
            &[0x01],
            &[0x02],
            &[0x33; 32],
            &[0x44; 1088],
            &[0x55; 128]
        ])
    );
    assert_eq!(fc.len(), 5 + 33 + 1216);

    for (len, le) in [
        (0usize, [0x00, 0x00]),
        (5, [0x05, 0x00]),
        (300, [0x2c, 0x01]),
        (1024, [0x00, 0x04]),
    ] {
        let bytes = AuthoredEnvelope::new(vec![state(len)]).to_bytes();
        assert_eq!(
            bytes,
            cat(&[
                b"TPM1",
                &[0x01],
                &[0x03],
                &[0x66; 32],
                &le,
                &vec![0x77; len]
            ]),
            "wallet state of {len} bytes"
        );
    }

    let mixed =
        AuthoredEnvelope::new(vec![state(2), follow_up.clone(), first_contact.clone()]).to_bytes();
    assert_eq!(
        mixed,
        cat(&[
            b"TPM1",
            &[0x03],
            &[0x03],
            &[0x66; 32],
            &[0x02, 0x00],
            &[0x77; 2],
            &[0x01],
            &[0x11; 32],
            &[0x22; 128],
            &[0x02],
            &[0x33; 32],
            &[0x44; 1088],
            &[0x55; 128],
        ])
    );

    let sixteen = AuthoredEnvelope::new(vec![follow_up; 16]);
    assert!(sixteen.is_valid());
    let bytes = sixteen.to_bytes();
    assert_eq!(bytes[4], 0x10);
    assert_eq!(bytes.len(), 5 + 16 * 161);
    assert_eq!(sixteen.encoded_len(), bytes.len());
}

#[test]
fn random_envelopes_walk_back_to_what_was_authored() {
    let mut rng = Rng::new("layout", 3);
    for _ in 0..200 {
        let follow_ups = rng.range(0, 6) as usize;
        let first_contacts = rng.range(0, 3) as usize;
        let wallet_states = rng.range(u64::from(follow_ups + first_contacts == 0), 3) as usize;
        let envelope =
            AuthoredEnvelope::random(&mut rng, follow_ups, first_contacts, wallet_states);
        assert!(envelope.is_valid());
        let bytes = envelope.to_bytes();
        assert_eq!(walk(&bytes, ALL_RULES), Ok(expected_walk(&envelope)));
        let mut seen = [0; 3];
        for entry in &envelope.entries {
            seen[entry.kind().byte() as usize - 1] += 1;
            assert_ne!(entry.key16(), [0; 16]);
        }
        assert_eq!(seen, [follow_ups, first_contacts, wallet_states]);
    }
}

#[test]
fn invalid_authored_envelopes_are_refused_as_valid() {
    let mut rng = Rng::new("invalid", 1);
    let entry = AuthoredEntry::random(&mut rng, EntryKind::FollowUp);
    assert!(!AuthoredEnvelope::new(vec![]).is_valid());
    assert!(!AuthoredEnvelope::new(vec![entry.clone(); 17]).is_valid());
    let zero = AuthoredEntry::random_with_tag(&mut rng, EntryKind::FirstContact, [0; 32]);
    assert!(!AuthoredEnvelope::new(vec![entry.clone(), zero]).is_valid());
    let long = AuthoredEntry::random_sized(&mut rng, EntryKind::WalletState, [9; 32], 1025);
    assert!(!AuthoredEnvelope::new(vec![long]).is_valid());
    let refused = std::panic::catch_unwind(|| {
        TxSpec::tachyon(1).envelope(&AuthoredEnvelope::new(vec![entry; 17]))
    });
    assert!(refused.is_err());
}

// ---------------------------------------------------------------------------
// Malformations.
// ---------------------------------------------------------------------------

#[test]
fn every_malformation_breaks_exactly_its_rule() {
    let off = |f: fn(&mut Rules)| {
        let mut rules = ALL_RULES;
        f(&mut rules);
        rules
    };
    for seed in 0..60 {
        let mut rng = Rng::new("malformations", seed);
        for malformation in Malformation::ALL {
            let memo = malformation.author(&mut rng);
            assert_eq!(memo.malformation, malformation);
            let bytes = memo.bytes;
            let refused = walk(&bytes, ALL_RULES);
            let context = format!("{malformation:?} seed {seed}: {refused:?}");
            match malformation {
                Malformation::BadMagic => {
                    assert_eq!(refused, Err(Violation::BadMagic), "{context}");
                    assert_ne!(&bytes[..4], b"TPM1");
                    assert!(walk(&bytes, off(|r| r.magic = false)).is_ok(), "{context}");
                }
                Malformation::MissingCount => {
                    assert_eq!(refused, Err(Violation::MissingCount), "{context}");
                    assert_eq!(bytes, b"TPM1");
                }
                Malformation::CountZero => {
                    assert_eq!(refused, Err(Violation::Count(0)), "{context}");
                    assert_eq!(bytes, b"TPM1\x00");
                }
                Malformation::CountAboveMax => {
                    assert_eq!(refused, Err(Violation::Count(17)), "{context}");
                    let entries = walk(&bytes, off(|r| r.count = false)).unwrap();
                    assert_eq!(entries.len(), 17);
                }
                Malformation::CountExceedsEntries => {
                    let count = bytes[4] as usize;
                    assert!((2..=16).contains(&count), "{context}");
                    assert_eq!(
                        refused,
                        Err(Violation::TruncatedHeader { index: count - 1 }),
                        "{context}"
                    );
                    let mut repaired = bytes.clone();
                    repaired[4] -= 1;
                    assert_eq!(walk(&repaired, ALL_RULES).unwrap().len(), count - 1);
                }
                Malformation::UnknownKind => {
                    let Err(Violation::UnknownKind { offset, .. }) = refused else {
                        panic!("{context}");
                    };
                    assert!(![1, 2, 3].contains(&bytes[offset]));
                    let mut repaired = bytes.clone();
                    repaired[offset] = 1;
                    assert!(walk(&repaired, ALL_RULES).is_ok(), "{context}");
                }
                Malformation::TruncatedBody => {
                    let count = bytes[4] as usize;
                    assert_eq!(
                        refused,
                        Err(Violation::TruncatedBody { index: count - 1 }),
                        "{context}"
                    );
                }
                Malformation::TrailingBytes => {
                    let Err(Violation::Trailing { offset }) = refused else {
                        panic!("{context}");
                    };
                    assert!((1..=32).contains(&(bytes.len() - offset)), "{context}");
                    assert!(walk(&bytes[..offset], ALL_RULES).is_ok());
                }
                Malformation::StateTooLong => {
                    let Err(Violation::StateTooLong { len, .. }) = refused else {
                        panic!("{context}");
                    };
                    assert!((1025..=1100).contains(&len), "{context}");
                    assert!(
                        walk(&bytes, off(|r| r.state_len = false)).is_ok(),
                        "{context}"
                    );
                }
                Malformation::FollowUpZeroKey | Malformation::FirstContactZeroKey => {
                    let want = if malformation == Malformation::FollowUpZeroKey {
                        1
                    } else {
                        2
                    };
                    let Err(Violation::ZeroKey { kind, .. }) = refused else {
                        panic!("{context}");
                    };
                    assert_eq!(kind, want, "{context}");
                    let entries = walk(&bytes, off(|r| r.zero_key = false)).unwrap();
                    let zero = entries.iter().filter(|e| e.1[..16] == [0; 16]).count();
                    assert_eq!(zero, 1, "{context}");
                }
                Malformation::RandomGarbage => {
                    assert!(
                        matches!(refused, Err(Violation::BadMagic | Violation::ShortMagic)),
                        "{context}"
                    );
                    assert!((1..=300).contains(&bytes.len()));
                    assert!(!bytes.starts_with(b"TPM1"));
                }
            }
            assert_eq!(
                AuthoredEnvelope::malformed(malformation, &mut Rng::new("same", seed)),
                AuthoredEnvelope::malformed(malformation, &mut Rng::new("same", seed)),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Chains, ground truth and the served JSON.
// ---------------------------------------------------------------------------

fn envelope_config(seed: u64) -> ChainConfig {
    ChainConfig {
        activation_height: Some(2),
        envelopes: Some(EnvelopeMix {
            envelope_percent: 90,
            malformed_percent: 20,
            follow_ups: [0, 6],
            first_contacts: [0, 2],
            wallet_states: [0, 2],
            repeat_tag_percent: 25,
            ..EnvelopeMix::default()
        }),
        ..ChainConfig::with_seed(seed)
    }
}

/// A chain with random envelopes, then explicit envelope, malformed and raw
/// memos.
fn envelope_chain(seed: u64) -> SyntheticChain {
    let mut chain = SyntheticChain::generate(envelope_config(seed), 30);
    let block = chain.envelope_block(20, 4, 3);
    chain.append(block);
    let mut block = BlockSpec::new().tx(TxSpec::tachyon(1).memo(*b"raw bytes"));
    for malformation in Malformation::ALL {
        block = block.tx(TxSpec::tachyon(1).malformed_memo(malformation));
    }
    block = block.tx(TxSpec::tachyon(2).random_proof(3));
    chain.append(block);
    chain
}

/// Independently of the crate's accessors, the follow-up and first-contact
/// entries of `chain` in ordinal order, from the authored memos.
/// A follow-up's ordinal and tag.
type Placed = (u64, usize, usize, [u8; 32]);

fn walk_truth(chain: &SyntheticChain) -> (Vec<Placed>, usize) {
    let mut follow_ups = Vec::new();
    let mut first_contacts = 0;
    for block in chain.blocks() {
        for (tx_index, tx) in block.txs().iter().enumerate() {
            if let Some(AuthoredMemo::Envelope(envelope)) = tx.authored_memo() {
                for (entry_index, entry) in envelope.entries.iter().enumerate() {
                    match entry {
                        AuthoredEntry::FollowUp { tag, .. } => {
                            follow_ups.push((block.height(), tx_index, entry_index, *tag))
                        }
                        AuthoredEntry::FirstContact { .. } => first_contacts += 1,
                        AuthoredEntry::WalletState { .. } => {}
                    }
                }
            }
        }
    }
    (follow_ups, first_contacts)
}

#[test]
fn ground_truth_lists_entries_in_ordinal_order() {
    let chain = envelope_chain(5);
    let follow_ups = chain.follow_ups(..);
    let first_contacts = chain.first_contacts(..);
    let (walked, walked_first_contacts) = walk_truth(&chain);
    assert_eq!(
        follow_ups
            .iter()
            .map(|e| (e.height, e.tx_index, e.entry_index, e.tag))
            .collect::<Vec<_>>(),
        walked
    );
    assert_eq!(first_contacts.len(), walked_first_contacts);
    for list in [
        follow_ups.iter().map(|e| e.ordinal()).collect::<Vec<_>>(),
        first_contacts.iter().map(|e| e.ordinal()).collect(),
    ] {
        assert!(!list.is_empty());
        assert!(
            list.windows(2).all(|w| w[0] < w[1]),
            "strictly ascending ordinals"
        );
        assert!(
            list.iter().all(|&(_, tx_index, _)| tx_index >= 1),
            "the coinbase is tx 0"
        );
    }

    // The explicit envelope block holds exactly the requested entries.
    let height = chain.tip_height() - 1;
    let counts = chain.block(height).unwrap().envelope_counts();
    assert_eq!(
        (
            counts.follow_ups,
            counts.first_contacts,
            counts.wallet_states
        ),
        (20, 4, 3)
    );
    assert_eq!((counts.malformed, counts.raw, counts.empty), (0, 0, 0));
    assert_eq!(chain.follow_ups(height..=height).len(), 20);
    assert_eq!(chain.first_contacts(height..=height).len(), 4);

    // The last block: one raw memo, one of each malformation, one empty memo.
    let tip = chain.tip();
    let counts = tip.envelope_counts();
    assert_eq!(counts.raw, 1);
    assert_eq!(counts.malformed, Malformation::ALL.len());
    assert_eq!((counts.valid, counts.empty), (0, 1));
    let authored: Vec<_> = tip
        .tachyon_txs()
        .filter_map(|tx| tx.authored_memo.malformation())
        .collect();
    assert_eq!(authored, Malformation::ALL);

    // Per-block counts add up and cover every Tachyon transaction.
    let mut total = tachyon_fixtures::EnvelopeCounts::default();
    for block in chain.blocks() {
        let counts = block.envelope_counts();
        assert_eq!(
            counts.empty + counts.valid + counts.malformed + counts.raw,
            block.tachyon_txs().count()
        );
        assert_eq!(counts.follow_ups, block.follow_ups().len());
        total += counts;
    }
    assert_eq!(total, chain.envelope_counts(..));
    assert_eq!(total.follow_ups, follow_ups.len());
    assert!(total.valid > 10 && total.malformed > Malformation::ALL.len() && total.empty > 0);
}

#[tokio::test]
async fn the_node_double_serves_the_authored_memo_bytes() {
    let chain = envelope_chain(8);
    let node = NodeDouble::start(chain, NodeOptions::default())
        .await
        .unwrap();
    let rpc = client(&node.url(), Credentials::None);
    let blocks = node.chain().blocks().to_vec();
    let mut envelopes = 0;
    for block in &blocks {
        let served = block_v2(&rpc, &block.hash_display()).await;
        let txs = served["tx"].as_array().unwrap();
        assert_eq!(txs.len(), block.txs().len());
        for (tx, json) in block.txs().iter().zip(txs) {
            let Some(authored) = tx.authored_memo() else {
                assert!(json.get("tachyon").is_none());
                continue;
            };
            let memo = json["tachyon"]["memo"].as_str().unwrap();
            match authored {
                AuthoredMemo::Empty => assert_eq!(memo, ""),
                AuthoredMemo::Envelope(envelope) => {
                    envelopes += 1;
                    let bytes = hex::decode(memo).unwrap();
                    assert_eq!(walk(&bytes, ALL_RULES), Ok(expected_walk(envelope)));
                }
                AuthoredMemo::Malformed(malformed) => {
                    assert_eq!(memo, hex::encode(&malformed.bytes));
                    assert!(walk(&malformed.bytes, ALL_RULES).is_err());
                }
                AuthoredMemo::Raw(bytes) => assert_eq!(memo, hex::encode(bytes)),
            }
            assert_eq!(memo, hex::encode(authored.to_bytes()));
        }
    }
    assert!(envelopes > 10);
}

#[test]
fn repeated_tags_are_all_kept_in_ordinal_order() {
    let mut chain = SyntheticChain::new(ChainConfig {
        activation_height: Some(1),
        ..ChainConfig::with_seed(21)
    });
    let tag = [0xab; 32];
    let rng = chain.rng();
    let twice = AuthoredEnvelope::new(vec![
        AuthoredEntry::random_with_tag(rng, EntryKind::FollowUp, tag),
        AuthoredEntry::random(rng, EntryKind::WalletState),
        AuthoredEntry::random_with_tag(rng, EntryKind::FollowUp, tag),
    ]);
    let once = AuthoredEnvelope::new(vec![
        AuthoredEntry::random(rng, EntryKind::FollowUp),
        AuthoredEntry::random_with_tag(rng, EntryKind::FollowUp, tag),
        AuthoredEntry::random_with_tag(rng, EntryKind::FirstContact, tag),
    ]);
    chain.append(BlockSpec::new().envelopes(&[twice, once]));
    chain.append_random(1);
    let rng = chain.rng();
    let later = AuthoredEnvelope::new(vec![
        AuthoredEntry::random_with_tag(rng, EntryKind::FirstContact, tag),
        AuthoredEntry::random_with_tag(rng, EntryKind::FollowUp, tag),
    ]);
    chain.append(BlockSpec::new().envelopes(&[later]));
    let ordinals: Vec<_> = chain
        .follow_ups(..)
        .into_iter()
        .filter(|e| e.tag == tag)
        .map(|e| e.ordinal())
        .collect();
    assert_eq!(ordinals, [(1, 1, 0), (1, 1, 2), (1, 2, 1), (3, 1, 1)]);
    let ordinals: Vec<_> = chain
        .first_contacts(..)
        .into_iter()
        .filter(|e| e.tag == tag)
        .map(|e| e.ordinal())
        .collect();
    assert_eq!(ordinals, [(1, 2, 2), (3, 1, 0)]);
    // Equal tags, different ciphertexts: every entry is kept as authored.
    let ciphertexts: HashSet<_> = chain
        .follow_ups(..)
        .into_iter()
        .filter(|e| e.tag == tag)
        .map(|e| e.ciphertext)
        .collect();
    assert_eq!(ciphertexts.len(), 4);
}

#[test]
fn the_mix_repeats_tags_across_blocks_and_within_envelopes() {
    let chain = SyntheticChain::generate(
        ChainConfig {
            envelopes: Some(EnvelopeMix {
                envelope_percent: 100,
                malformed_percent: 0,
                follow_ups: [2, 8],
                repeat_tag_percent: 40,
                ..EnvelopeMix::default()
            }),
            ..envelope_config(4)
        },
        25,
    );
    let mut by_key: BTreeMap<[u8; 16], Vec<(u64, usize, usize)>> = BTreeMap::new();
    for entry in chain.follow_ups(..) {
        by_key
            .entry(entry.key16())
            .or_default()
            .push(entry.ordinal());
    }
    let across_blocks = by_key
        .values()
        .any(|ordinals| ordinals.iter().map(|o| o.0).collect::<HashSet<_>>().len() > 1);
    let within_envelope = by_key.values().any(|ordinals| {
        let mut txs: Vec<_> = ordinals.iter().map(|o| (o.0, o.1)).collect();
        let len = txs.len();
        txs.dedup();
        txs.len() < len
    });
    assert!(across_blocks && within_envelope);
    for ordinals in by_key.values() {
        assert!(ordinals.windows(2).all(|w| w[0] < w[1]));
    }
}

#[test]
fn ground_keys_are_placed_as_given() {
    let keys: Vec<[u8; 16]> = (1..=5u8).map(|i| [i; 16]).collect();
    // Explicitly: 40 follow-ups on the given keys, packed into envelopes.
    let mut chain = SyntheticChain::new(ChainConfig::with_seed(2));
    chain.append_random(9);
    let entries = (0..40)
        .map(|i| {
            let tag = tag_with_key16(chain.rng(), keys[i % keys.len()]);
            AuthoredEntry::random_with_tag(chain.rng(), EntryKind::FollowUp, tag)
        })
        .collect::<Vec<_>>();
    let block = chain.entries_block(entries.clone());
    assert!(
        block.txs.len() >= 3,
        "40 entries need at least three envelopes"
    );
    chain.append(block);
    let truth = chain.follow_ups(chain.tip_height()..);
    assert_eq!(
        truth.iter().map(|e| e.tag).collect::<Vec<_>>(),
        entries.iter().map(|e| *e.tag()).collect::<Vec<_>>()
    );

    // Through the mix (and so through the binary's spec).
    let chain = SyntheticChain::generate(
        ChainConfig {
            envelopes: Some(EnvelopeMix {
                key16s: keys.clone(),
                key16_percent: 50,
                first_contacts: [1, 2],
                ..EnvelopeMix::default()
            }),
            ..envelope_config(6)
        },
        20,
    );
    let ground = |key: [u8; 16]| keys.contains(&key);
    assert!(chain.follow_ups(..).iter().any(|e| ground(e.key16())));
    assert!(chain.first_contacts(..).iter().any(|e| ground(e.key16())));
    assert!(chain.follow_ups(..).iter().any(|e| !ground(e.key16())));
}

#[test]
fn generation_is_deterministic_from_the_seed() {
    let a = envelope_chain(9);
    let b = envelope_chain(9);
    let c = envelope_chain(10);
    assert_eq!(a.blocks(), b.blocks());
    assert_eq!(a.follow_ups(..), b.follow_ups(..));
    assert_eq!(a.first_contacts(..), b.first_contacts(..));
    assert_ne!(a.follow_ups(..), c.follow_ups(..));
    // A reorg re-authoring a block keeps its envelopes and their truth.
    let mut d = a.clone();
    let height = d.tip_height() - 1;
    let original = d.block(height).unwrap().clone();
    d.reorg(height - 1, vec![BlockSpec::from_block(&original)])
        .unwrap();
    assert_eq!(d.tip().follow_ups(), original.follow_ups());
    assert_eq!(d.tip().txs()[1..], original.txs()[1..]);
}

#[test]
fn chains_without_an_envelope_mix_keep_their_random_memos() {
    // Without `envelopes`, generated memos are the mix's random bytes, recorded
    // as garbage that does not start with the magic.
    let chain = SyntheticChain::generate(ChainConfig::with_seed(1), 60);
    let counts = chain.envelope_counts(..);
    assert!(counts.malformed > 0 && counts.empty > 0);
    assert_eq!(counts.valid + counts.raw, 0);
    for block in chain.blocks() {
        for tx in block.tachyon_txs() {
            if let Some(malformation) = tx.authored_memo.malformation() {
                assert_eq!(malformation, Malformation::RandomGarbage);
                assert!(!tx.memo.starts_with(b"TPM1"));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Chain spec JSON and the binary.
// ---------------------------------------------------------------------------

fn spec_json() -> serde_json::Value {
    let mut rng = Rng::new("spec", 1);
    let explicit = AuthoredEnvelope::random(&mut rng, 2, 1, 1);
    json!({
        "seed": 31,
        "activation_height": 3,
        "random_blocks": 15,
        "mix": {"tachyon_txs": [1, 4]},
        "envelopes": {"envelope_percent": 85, "malformed_percent": 25,
                      "follow_ups": [0, 5], "first_contacts": [0, 2], "wallet_states": [0, 1],
                      "wallet_state_len": [0, 40], "repeat_tag_percent": 20,
                      "key16s": ["0f".repeat(16)], "key16_percent": 10},
        "explicit_blocks": [{"txs": [
            serde_json::to_value(TxJson::with_envelope(&explicit)).unwrap(),
            {"malformed": "trailing_bytes", "stamp": {"type": "random_proof", "count": 3}},
            {"malformed": "first_contact_zero_key"},
            {"memo_hex": "54504d31"},
        ]}],
    })
}

#[test]
fn chain_specs_author_envelopes_and_refuse_bad_ones() {
    let spec: ChainSpec = serde_json::from_value(spec_json()).unwrap();
    let chain = spec.build().unwrap();
    let tip: Vec<_> = chain.tip().tachyon_txs().collect();
    assert_eq!(tip.len(), 4);
    let explicit = AuthoredEnvelope::random(&mut Rng::new("spec", 1), 2, 1, 1);
    assert_eq!(tip[0].authored_memo, &AuthoredMemo::Envelope(explicit));
    assert_eq!(
        tip[1].authored_memo.malformation(),
        Some(Malformation::TrailingBytes)
    );
    assert_eq!(
        tip[2].authored_memo.malformation(),
        Some(Malformation::FirstContactZeroKey)
    );
    assert_eq!(tip[3].authored_memo, &AuthoredMemo::Raw(b"TPM1".to_vec()));
    let counts = chain.envelope_counts(..chain.tip_height());
    assert!(counts.valid > 0 && counts.malformed > 0 && counts.first_contacts > 0);
    assert_eq!(spec.build().unwrap().blocks(), chain.blocks());
    let round_trip: ChainSpec =
        serde_json::from_str(&serde_json::to_string(&spec).unwrap()).unwrap();
    assert_eq!(round_trip, spec);

    let refused_parse = |patch: serde_json::Value| {
        let mut value = spec_json();
        json_patch(&mut value, patch);
        serde_json::from_value::<ChainSpec>(value).is_err()
    };
    assert!(refused_parse(json!({"envelopes": {"bogus": 1}})));
    assert!(refused_parse(json!({"envelopes": {"key16s": ["00"]}})));
    assert!(refused_parse(
        json!({"explicit_blocks": [{"txs": [{"malformed": "nonsense"}]}]})
    ));
    assert!(refused_parse(json!({"explicit_blocks": [{"txs": [
        {"envelope": {"entries": [{"kind": "follow_up", "tag": "", "ciphertext": "", "x": 1}]}}
    ]}]})));
    assert!(refused_parse(json!({"explicit_blocks": [{"txs": [
        {"envelope": {"entries": [{"kind": "unknown", "tag": ""}]}}
    ]}]})));

    let refused_build = |patch: serde_json::Value| {
        let mut value = spec_json();
        json_patch(&mut value, patch);
        serde_json::from_value::<ChainSpec>(value)
            .unwrap()
            .build()
            .is_err()
    };
    let zero_key =
        json!({"kind": "follow_up", "tag": "00".repeat(32), "ciphertext": "11".repeat(128)});
    let short_ct =
        json!({"kind": "follow_up", "tag": "01".repeat(32), "ciphertext": "11".repeat(127)});
    let long_state =
        json!({"kind": "wallet_state", "tag": "01".repeat(32), "ciphertext": "11".repeat(1025)});
    for entry in [zero_key, short_ct, long_state] {
        assert!(refused_build(
            json!({"explicit_blocks": [{"txs": [{"envelope": {"entries": [entry]}}]}]})
        ));
    }
    assert!(refused_build(
        json!({"explicit_blocks": [{"txs": [{"envelope": {"entries": []}}]}]})
    ));
    assert!(refused_build(json!({"explicit_blocks": [{"txs": [
        {"memo_hex": "00", "malformed": "count_zero"}
    ]}]})));
    assert!(refused_build(
        json!({"envelopes": {"wallet_state_len": [0, 1025]}})
    ));
    assert!(refused_build(
        json!({"envelopes": {"key16s": ["00".repeat(16)], "key16_percent": 5}})
    ));
}

/// Replaces top-level keys of `value` with those of `patch`.
fn json_patch(value: &mut serde_json::Value, patch: serde_json::Value) {
    for (key, field) in patch.as_object().unwrap() {
        value[key] = field.clone();
    }
}

#[tokio::test]
async fn the_binary_serves_random_envelope_chains() {
    let dir = tempfile::tempdir().unwrap();
    let spec_path = dir.path().join("chain.json");
    std::fs::write(&spec_path, spec_json().to_string()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_tachyon-node-double"))
        .args(["--spec", spec_path.to_str().unwrap()])
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut line),
    )
    .await
    .expect("the binary prints its address")
    .unwrap();
    let url = line.trim().to_owned();
    let rpc = client(&url, Credentials::None);

    let spec: ChainSpec = serde_json::from_value(spec_json()).unwrap();
    let mut local = spec.build().unwrap();
    // Random blocks appended through the control endpoint carry envelopes too.
    control(&url, "append", json!({"random_blocks": 4})).await;
    local.append_random(4);

    let mut kinds = HashSet::new();
    for block in local.blocks() {
        let served = block_v2(&rpc, &block.hash_display()).await;
        assert_eq!(served["hash"], block.hash_display());
        for (tx, json) in block.txs().iter().zip(served["tx"].as_array().unwrap()) {
            if let Some(authored) = tx.authored_memo() {
                assert_eq!(
                    json["tachyon"]["memo"].as_str().unwrap(),
                    hex::encode(authored.to_bytes())
                );
                kinds.insert(std::mem::discriminant(authored));
            }
        }
    }
    // Empty, valid, malformed and raw memos all crossed the process boundary.
    assert_eq!(kinds.len(), 4);
    let appended = local.envelope_counts(local.tip_height() - 3..);
    assert!(appended.valid + appended.malformed > 0);
    child.kill().await.unwrap();
}

// ---------------------------------------------------------------------------
// Cross-check against the products' parser (dev-dependency only).
// ---------------------------------------------------------------------------

#[test]
fn the_protocol_parser_agrees_with_the_authored_truth() {
    use tachyon_pir_protocol::envelope::{Field, parse};
    use tachyon_pir_protocol::{EnvelopeEntry, EnvelopeError};

    let same = |authored: &AuthoredEnvelope, parsed: &[EnvelopeEntry]| {
        assert_eq!(authored.entries.len(), parsed.len());
        for (a, p) in authored.entries.iter().zip(parsed) {
            match (a, p) {
                (
                    AuthoredEntry::FollowUp { tag, ciphertext },
                    EnvelopeEntry::FollowUp {
                        tag: t,
                        ciphertext: c,
                    },
                ) => assert!(tag == t && ciphertext == &**c),
                (
                    AuthoredEntry::FirstContact {
                        tag,
                        kem_ciphertext,
                        ciphertext,
                    },
                    EnvelopeEntry::FirstContact {
                        tag: t,
                        kem_ciphertext: k,
                        ciphertext: c,
                    },
                ) => assert!(tag == t && kem_ciphertext == &**k && ciphertext == &**c),
                (
                    AuthoredEntry::WalletState { tag, ciphertext },
                    EnvelopeEntry::WalletState {
                        tag: t,
                        ciphertext: c,
                    },
                ) => assert!(tag == t && ciphertext == c),
                (a, p) => panic!("kind mismatch: {a:?} vs {p:?}"),
            }
        }
    };

    let chain = envelope_chain(12);
    let mut valid = 0;
    for block in chain.blocks() {
        for tx in block.tachyon_txs() {
            match tx.authored_memo {
                AuthoredMemo::Envelope(envelope) => {
                    let parsed = parse(tx.memo).expect("an authored envelope parses");
                    same(envelope, &parsed.entries);
                    valid += 1;
                }
                AuthoredMemo::Malformed(_) => assert!(parse(tx.memo).is_err()),
                AuthoredMemo::Empty | AuthoredMemo::Raw(_) => {}
            }
        }
    }
    assert!(valid > 10);

    for seed in 0..40 {
        let mut rng = Rng::new("cross-check", seed);
        let envelope = AuthoredEnvelope::random(&mut rng, 3, 1, 2);
        same(&envelope, &parse(&envelope.to_bytes()).unwrap().entries);
        for malformation in Malformation::ALL {
            let bytes = malformation.author(&mut rng).bytes;
            let error = parse(&bytes).expect_err("a malformation is refused");
            let expected = match malformation {
                Malformation::BadMagic => matches!(error, EnvelopeError::BadMagic(_)),
                Malformation::MissingCount => matches!(
                    error,
                    EnvelopeError::Truncated {
                        field: Field::EntryCount,
                        ..
                    }
                ),
                Malformation::CountZero => matches!(error, EnvelopeError::EntryCount(0)),
                Malformation::CountAboveMax => matches!(error, EnvelopeError::EntryCount(17)),
                Malformation::CountExceedsEntries => matches!(
                    error,
                    EnvelopeError::Truncated {
                        field: Field::Kind,
                        ..
                    }
                ),
                Malformation::UnknownKind => matches!(error, EnvelopeError::UnknownKind { .. }),
                Malformation::TruncatedBody => matches!(
                    error,
                    EnvelopeError::Truncated {
                        field: Field::Ciphertext
                            | Field::KemCiphertext
                            | Field::StateLen
                            | Field::StateCiphertext,
                        ..
                    }
                ),
                Malformation::TrailingBytes => {
                    matches!(error, EnvelopeError::TrailingBytes { .. })
                }
                Malformation::StateTooLong => matches!(error, EnvelopeError::StateTooLong { .. }),
                Malformation::FollowUpZeroKey | Malformation::FirstContactZeroKey => {
                    matches!(error, EnvelopeError::ZeroKey { .. })
                }
                Malformation::RandomGarbage => matches!(
                    error,
                    EnvelopeError::BadMagic(_)
                        | EnvelopeError::Truncated {
                            field: Field::Magic,
                            ..
                        }
                ),
            };
            assert!(expected, "{malformation:?} seed {seed}: {error:?}");
        }
    }
    // Spec 5.2's helpers in the generator agree with the parser's key16.
    let mut rng = Rng::new("key16", 0);
    let entry = AuthoredEntry::random(&mut rng, EntryKind::FirstContact);
    let parsed = parse(&AuthoredEnvelope::new(vec![entry.clone()]).to_bytes()).unwrap();
    assert_eq!(parsed.entries[0].key16(), entry.key16());
    assert_eq!(envelope::key16(entry.tag()), entry.key16());
}
