//! The on-chain envelope (spec 5, 6.2): known answers from
//! `tests/vectors/envelope.json` (built independently by
//! `tests/vectors/envelope.py`), round trips, every refusal, and a seeded
//! mutation test that `parse` never panics and that whatever it accepts
//! re-encodes to the same bytes.

use serde_json::Value;
use sha2::{Digest, Sha256};
use tachyon_pir_protocol::envelope::{
    self, Envelope, EnvelopeEntry, EnvelopeError, Field, KIND_FIRST_CONTACT, KIND_FOLLOW_UP,
    KIND_WALLET_STATE, MAGIC, MAX_ENVELOPE_BYTES, parse,
};
use tachyon_pir_protocol::{
    Entry, KEM_CT_BYTES, MAX_ENVELOPE_ENTRIES, MAX_STATE_BYTES, MEMO_CT_BYTES,
};

fn vectors() -> Value {
    serde_json::from_str(include_str!("vectors/envelope.json")).expect("envelope.json parses")
}

fn hex_of(v: &Value) -> Vec<u8> {
    hex::decode(v.as_str().expect("hex string")).expect("hex")
}

fn sha_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn int(v: &Value) -> usize {
    usize::try_from(v.as_u64().expect("integer")).unwrap()
}

/// A deterministic generator (SplitMix64), so failures reproduce.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).unwrap()).unwrap()
    }

    fn byte(&mut self) -> u8 {
        self.next().to_le_bytes()[0]
    }

    fn fill(&mut self, out: &mut [u8]) {
        for b in out {
            *b = self.byte();
        }
    }

    fn bytes(&mut self, n: usize) -> Vec<u8> {
        let mut out = vec![0; n];
        self.fill(&mut out);
        out
    }

    fn tag(&mut self) -> [u8; 32] {
        let mut tag = [0u8; 32];
        self.fill(&mut tag);
        tag[0] |= 1; // a nonzero key16
        tag
    }
}

fn follow_up(tag: [u8; 32], fill: u8) -> EnvelopeEntry {
    EnvelopeEntry::FollowUp {
        tag,
        ciphertext: Box::new([fill; MEMO_CT_BYTES]),
    }
}

fn first_contact(tag: [u8; 32], fill: u8) -> EnvelopeEntry {
    EnvelopeEntry::FirstContact {
        tag,
        kem_ciphertext: Box::new([fill; KEM_CT_BYTES]),
        ciphertext: Box::new([fill ^ 0xff; MEMO_CT_BYTES]),
    }
}

fn wallet_state(tag: [u8; 32], len: usize) -> EnvelopeEntry {
    EnvelopeEntry::WalletState {
        tag,
        ciphertext: (0..len).map(|i| i.to_le_bytes()[0]).collect(),
    }
}

fn random_entry(rng: &mut Rng) -> EnvelopeEntry {
    let tag = rng.tag();
    match rng.below(3) {
        0 => {
            let mut ct = Box::new([0u8; MEMO_CT_BYTES]);
            rng.fill(&mut ct[..]);
            EnvelopeEntry::FollowUp {
                tag,
                ciphertext: ct,
            }
        }
        1 => {
            let mut kem = Box::new([0u8; KEM_CT_BYTES]);
            let mut ct = Box::new([0u8; MEMO_CT_BYTES]);
            rng.fill(&mut kem[..]);
            rng.fill(&mut ct[..]);
            EnvelopeEntry::FirstContact {
                tag,
                kem_ciphertext: kem,
                ciphertext: ct,
            }
        }
        _ => {
            let len = match rng.below(4) {
                0 => 0,
                1 => MAX_STATE_BYTES,
                _ => rng.below(MAX_STATE_BYTES + 1),
            };
            EnvelopeEntry::WalletState {
                tag,
                ciphertext: rng.bytes(len),
            }
        }
    }
}

fn random_envelope(rng: &mut Rng) -> Envelope {
    let n = 1 + rng.below(MAX_ENVELOPE_ENTRIES);
    Envelope {
        entries: (0..n).map(|_| random_entry(rng)).collect(),
    }
}

/// A test-side encoder with no checks, to build what `encode` refuses.
fn raw(count: usize, entries: &[EnvelopeEntry]) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    out.push(u8::try_from(count).unwrap());
    for e in entries {
        out.push(e.kind());
        out.extend_from_slice(e.tag());
        match e {
            EnvelopeEntry::FollowUp { ciphertext, .. } => out.extend_from_slice(&ciphertext[..]),
            EnvelopeEntry::FirstContact {
                kem_ciphertext,
                ciphertext,
                ..
            } => {
                out.extend_from_slice(&kem_ciphertext[..]);
                out.extend_from_slice(&ciphertext[..]);
            }
            EnvelopeEntry::WalletState { ciphertext, .. } => {
                out.extend_from_slice(&u16::try_from(ciphertext.len()).unwrap().to_le_bytes());
                out.extend_from_slice(ciphertext);
            }
        }
    }
    out
}

fn round_trip(env: &Envelope) -> Vec<u8> {
    let bytes = env.encode().expect("encodes");
    assert_eq!(&parse(&bytes).expect("parses"), env);
    assert_eq!(
        bytes.len(),
        5 + env
            .entries
            .iter()
            .map(EnvelopeEntry::encoded_len)
            .sum::<usize>()
    );
    bytes
}

#[test]
fn known_answer_valid_envelopes() {
    let v = vectors();
    let cases = v["valid"].as_array().unwrap();
    assert_eq!(cases.len(), 9);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let bytes = hex_of(&case["hex"]);
        assert_eq!(bytes.len(), int(&case["len"]), "{name}");
        assert_eq!(sha_hex(&bytes), case["sha256"].as_str().unwrap(), "{name}");
        let env = parse(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
        let expected = case["entries"].as_array().unwrap();
        assert_eq!(env.entries.len(), expected.len(), "{name}");
        for (entry, want) in env.entries.iter().zip(expected) {
            assert_eq!(usize::from(entry.kind()), int(&want["kind"]), "{name}");
            assert_eq!(entry.tag().to_vec(), hex_of(&want["tag"]), "{name}");
            assert_eq!(entry.key16()[..], entry.tag()[..16], "{name}");
            let s = |k: &str| want[k].as_str().unwrap().to_owned();
            match entry {
                EnvelopeEntry::FollowUp { ciphertext, .. } => {
                    assert_eq!(sha_hex(&ciphertext[..]), s("ciphertext_sha256"), "{name}");
                    let memo = Entry::Memo(entry.memo_entry().unwrap());
                    assert_eq!(sha_hex(&memo.to_bytes()), s("table_entry_sha256"), "{name}");
                    assert!(entry.first_contact_entry().is_none());
                }
                EnvelopeEntry::FirstContact {
                    kem_ciphertext,
                    ciphertext,
                    ..
                } => {
                    assert_eq!(sha_hex(&kem_ciphertext[..]), s("kem_ciphertext_sha256"));
                    assert_eq!(sha_hex(&ciphertext[..]), s("ciphertext_sha256"), "{name}");
                    let fc = Entry::FirstContact(Box::new(entry.first_contact_entry().unwrap()));
                    assert_eq!(sha_hex(&fc.to_bytes()), s("table_entry_sha256"), "{name}");
                    assert!(entry.memo_entry().is_none());
                }
                EnvelopeEntry::WalletState { ciphertext, .. } => {
                    assert_eq!(ciphertext.len(), int(&want["state_len"]), "{name}");
                    assert_eq!(sha_hex(ciphertext), s("state_sha256"), "{name}");
                    assert!(entry.memo_entry().is_none());
                    assert!(entry.first_contact_entry().is_none());
                }
            }
        }
        assert_eq!(env.encode().unwrap(), bytes, "{name}");
    }
}

fn expected_error(v: &Value) -> EnvelopeError {
    let index = || int(&v["index"]);
    match v["type"].as_str().unwrap() {
        "BadMagic" => EnvelopeError::BadMagic(hex_of(&v["magic"]).try_into().unwrap()),
        "Truncated" => EnvelopeError::Truncated {
            field: match v["field"].as_str().unwrap() {
                "Magic" => Field::Magic,
                "EntryCount" => Field::EntryCount,
                "Kind" => Field::Kind,
                "Tag" => Field::Tag,
                "Ciphertext" => Field::Ciphertext,
                "KemCiphertext" => Field::KemCiphertext,
                "StateLen" => Field::StateLen,
                "StateCiphertext" => Field::StateCiphertext,
                other => panic!("field {other}"),
            },
            index: v["index"].as_u64().map(|i| usize::try_from(i).unwrap()),
            offset: int(&v["offset"]),
            needed: int(&v["needed"]),
            available: int(&v["available"]),
        },
        "EntryCount" => EnvelopeError::EntryCount(int(&v["count"])),
        "UnknownKind" => EnvelopeError::UnknownKind {
            index: index(),
            kind: u8::try_from(int(&v["kind"])).unwrap(),
        },
        "StateTooLong" => EnvelopeError::StateTooLong {
            index: index(),
            len: int(&v["len"]),
        },
        "ZeroKey" => EnvelopeError::ZeroKey { index: index() },
        "TrailingBytes" => EnvelopeError::TrailingBytes {
            offset: int(&v["offset"]),
            count: int(&v["count"]),
        },
        other => panic!("error type {other}"),
    }
}

#[test]
fn known_answer_invalid_envelopes() {
    let v = vectors();
    let cases = v["invalid"].as_array().unwrap();
    assert_eq!(cases.len(), 28);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let bytes = hex_of(&case["hex"]);
        assert_eq!(parse(&bytes), Err(expected_error(&case["error"])), "{name}");
    }
}

#[test]
fn round_trips_every_kind_and_count() {
    let mut rng = Rng(1);
    round_trip(&Envelope {
        entries: vec![follow_up(rng.tag(), 7)],
    });
    round_trip(&Envelope {
        entries: vec![first_contact(rng.tag(), 9)],
    });
    for len in [0, 1, 2, 255, 256, MAX_STATE_BYTES - 1, MAX_STATE_BYTES] {
        let bytes = round_trip(&Envelope {
            entries: vec![wallet_state(rng.tag(), len)],
        });
        assert_eq!(bytes[38..40], u16::try_from(len).unwrap().to_le_bytes());
    }
    // Every count with every kind present from three entries on.
    for n in 1..=MAX_ENVELOPE_ENTRIES {
        let entries = (0..n)
            .map(|i| match i % 3 {
                0 => follow_up(rng.tag(), u8::try_from(i).unwrap()),
                1 => first_contact(rng.tag(), u8::try_from(i).unwrap()),
                _ => wallet_state(rng.tag(), i * 61),
            })
            .collect();
        let bytes = round_trip(&Envelope { entries });
        assert_eq!(bytes[4], u8::try_from(n).unwrap());
    }
    // The largest envelope.
    let max = round_trip(&Envelope {
        entries: (0..MAX_ENVELOPE_ENTRIES)
            .map(|_| first_contact(rng.tag(), 1))
            .collect(),
    });
    assert_eq!(max.len(), MAX_ENVELOPE_BYTES);
    // Random envelopes.
    for _ in 0..200 {
        round_trip(&random_envelope(&mut rng));
    }
}

#[test]
fn accessors() {
    let tag = [0x5a; 32];
    for (entry, kind) in [
        (follow_up(tag, 1), KIND_FOLLOW_UP),
        (first_contact(tag, 2), KIND_FIRST_CONTACT),
        (wallet_state(tag, 3), KIND_WALLET_STATE),
    ] {
        assert_eq!(entry.kind(), kind);
        assert_eq!(entry.tag(), &tag);
        assert_eq!(entry.key16(), [0x5a; 16]);
    }
    let mut tag = [0u8; 32];
    for (i, b) in tag.iter_mut().enumerate() {
        *b = u8::try_from(i + 1).unwrap();
    }
    let memo = follow_up(tag, 0xee).memo_entry().unwrap();
    assert_eq!(memo.key[..], tag[..16]);
    assert_eq!(memo.ciphertext, [0xee; MEMO_CT_BYTES]);
    let fc = first_contact(tag, 0x0f).first_contact_entry().unwrap();
    assert_eq!(fc.key[..], tag[..16]);
    assert_eq!(fc.kem_ciphertext, [0x0f; KEM_CT_BYTES]);
    assert_eq!(fc.ciphertext, [0xf0; MEMO_CT_BYTES]);
    assert!(wallet_state(tag, 0).memo_entry().is_none());
    assert!(wallet_state(tag, 0).first_contact_entry().is_none());
}

/// `encode` refuses with the error `parse` gives for the same bytes.
fn refused_alike(env: &Envelope, expected: EnvelopeError) {
    assert_eq!(env.encode(), Err(expected.clone()));
    assert_eq!(parse(&raw(env.entries.len(), &env.entries)), Err(expected));
}

#[test]
fn encode_refuses_what_parse_refuses() {
    let mut rng = Rng(2);
    assert_eq!(
        Envelope { entries: vec![] }.encode(),
        Err(EnvelopeError::EntryCount(0))
    );
    refused_alike(&Envelope { entries: vec![] }, EnvelopeError::EntryCount(0));
    let seventeen = Envelope {
        entries: (0..17).map(|_| follow_up(rng.tag(), 3)).collect(),
    };
    refused_alike(&seventeen, EnvelopeError::EntryCount(17));
    assert_eq!(
        Envelope {
            entries: (0..300).map(|_| wallet_state(rng.tag(), 0)).collect()
        }
        .encode(),
        Err(EnvelopeError::EntryCount(300))
    );

    for len in [MAX_STATE_BYTES + 1, usize::from(u16::MAX)] {
        refused_alike(
            &Envelope {
                entries: vec![follow_up(rng.tag(), 1), wallet_state(rng.tag(), len)],
            },
            EnvelopeError::StateTooLong { index: 1, len },
        );
    }
    // Past u16: encode still refuses rather than truncating the length.
    assert_eq!(
        Envelope {
            entries: vec![wallet_state(rng.tag(), 70_000)]
        }
        .encode(),
        Err(EnvelopeError::StateTooLong {
            index: 0,
            len: 70_000
        })
    );

    let mut zero16 = rng.tag();
    zero16[..16].fill(0);
    for entry in [follow_up(zero16, 1), first_contact(zero16, 1)] {
        refused_alike(
            &Envelope {
                entries: vec![
                    wallet_state(rng.tag(), 4),
                    first_contact(rng.tag(), 2),
                    entry,
                ],
            },
            EnvelopeError::ZeroKey { index: 2 },
        );
    }
    for entry in [follow_up([0; 32], 1), first_contact([0; 32], 1)] {
        refused_alike(
            &Envelope {
                entries: vec![entry],
            },
            EnvelopeError::ZeroKey { index: 0 },
        );
    }
    // The first failing entry is reported.
    refused_alike(
        &Envelope {
            entries: vec![
                follow_up(rng.tag(), 1),
                wallet_state(rng.tag(), MAX_STATE_BYTES + 5),
                follow_up([0; 32], 1),
            ],
        },
        EnvelopeError::StateTooLong {
            index: 1,
            len: MAX_STATE_BYTES + 5,
        },
    );
    // A zero wallet-state tag is not a key (spec 6.2 names memo and
    // first-contact only) and is accepted.
    round_trip(&Envelope {
        entries: vec![wallet_state([0; 32], 10), follow_up(rng.tag(), 1)],
    });
}

#[test]
fn truncation_at_every_length_and_trailing_bytes() {
    let mut rng = Rng(3);
    let env = Envelope {
        entries: vec![
            follow_up(rng.tag(), 1),
            first_contact(rng.tag(), 2),
            wallet_state(rng.tag(), 3),
            wallet_state(rng.tag(), 0),
        ],
    };
    let bytes = env.encode().unwrap();
    for cut in 0..bytes.len() {
        match parse(&bytes[..cut]) {
            Err(EnvelopeError::Truncated {
                offset,
                needed,
                available,
                ..
            }) => {
                assert!(offset <= cut && available == cut - offset && available < needed);
            }
            other => panic!("cut at {cut}: {other:?}"),
        }
    }
    for extra in [1, 2, 33, 1000] {
        let mut long = bytes.clone();
        long.extend(rng.bytes(extra));
        assert_eq!(
            parse(&long),
            Err(EnvelopeError::TrailingBytes {
                offset: bytes.len(),
                count: extra
            })
        );
    }
    // Over MAX_ENVELOPE_BYTES never parses.
    let max = Envelope {
        entries: (0..MAX_ENVELOPE_ENTRIES)
            .map(|_| first_contact(rng.tag(), 4))
            .collect(),
    };
    let mut over = max.encode().unwrap();
    over.push(0);
    assert!(over.len() > MAX_ENVELOPE_BYTES && parse(&over).is_err());
}

#[test]
fn unknown_kinds() {
    let mut rng = Rng(4);
    let env = Envelope {
        entries: vec![follow_up(rng.tag(), 1), follow_up(rng.tag(), 2)],
    };
    let good = env.encode().unwrap();
    let second = 5 + 33 + MEMO_CT_BYTES;
    for kind in (0..=u8::MAX).filter(|k| !(1..=3).contains(k)) {
        let mut bytes = good.clone();
        bytes[5] = kind;
        assert_eq!(
            parse(&bytes),
            Err(EnvelopeError::UnknownKind { index: 0, kind })
        );
        let mut bytes = good.clone();
        bytes[second] = kind;
        assert_eq!(
            parse(&bytes),
            Err(EnvelopeError::UnknownKind { index: 1, kind })
        );
    }
}

/// A mutation of `bytes`: byte flips, cuts, insertions, and edits aimed at
/// the count, kinds and state lengths.
fn mutate(rng: &mut Rng, mut bytes: Vec<u8>) -> Vec<u8> {
    for _ in 0..1 + rng.below(3) {
        if bytes.is_empty() {
            bytes.push(rng.byte());
            continue;
        }
        let at = rng.below(bytes.len());
        match rng.below(7) {
            0 => bytes[at] ^= 1 << rng.below(8),
            1 => bytes[at] = rng.byte(),
            2 => bytes.truncate(at),
            3 => bytes.insert(at, rng.byte()),
            4 => {
                bytes.remove(at);
            }
            5 if bytes.len() > 4 => bytes[4] = u8::try_from(rng.below(18)).unwrap(),
            _ => {
                // An entry-start-ish byte set to a small kind, or a u16 length.
                let v = u8::try_from(rng.below(5)).unwrap();
                bytes[at] = v;
            }
        }
    }
    bytes
}

fn check_property(bytes: &[u8]) -> bool {
    match parse(bytes) {
        Ok(env) => {
            assert_eq!(env.encode().as_deref(), Ok(bytes), "{}", hex::encode(bytes));
            assert!(bytes.len() <= MAX_ENVELOPE_BYTES);
            true
        }
        Err(_) => false,
    }
}

#[test]
fn parse_never_panics_and_accepted_bytes_re_encode() {
    let mut rng = Rng(0x7470_6d31);
    let (mut accepted, mut total) = (0, 0);
    // Pure noise, with and without the magic in front.
    for _ in 0..2000 {
        let len = rng.below(64);
        let mut bytes = rng.bytes(len);
        if rng.below(2) == 0 {
            let mut framed = MAGIC.to_vec();
            framed.append(&mut bytes);
            bytes = framed;
        }
        accepted += usize::from(check_property(&bytes));
        total += 1;
    }
    // Mutations of valid envelopes.
    for _ in 0..3000 {
        let env = random_envelope(&mut rng);
        let good = env.encode().unwrap();
        assert!(check_property(&good));
        let bad = mutate(&mut rng, good);
        accepted += usize::from(check_property(&bad));
        total += 1;
    }
    // Random envelopes of the smallest kinds: count, kinds and state lengths
    // drawn so that many parse.
    for _ in 0..2000 {
        let mut bytes = MAGIC.to_vec();
        let n = rng.below(18);
        bytes.push(u8::try_from(n).unwrap());
        for _ in 0..n {
            let kind = u8::try_from(rng.below(5)).unwrap();
            bytes.push(kind);
            let tag = if rng.below(8) == 0 {
                [0; 32]
            } else {
                rng.tag()
            };
            bytes.extend_from_slice(&tag);
            match kind {
                1 => bytes.extend(rng.bytes(MEMO_CT_BYTES)),
                3 => {
                    let len = rng.below(40);
                    bytes.extend_from_slice(&u16::try_from(len).unwrap().to_le_bytes());
                    bytes.extend(rng.bytes(len));
                }
                _ => {
                    let len = rng.below(8);
                    bytes.extend(rng.bytes(len));
                }
            }
        }
        accepted += usize::from(check_property(&bytes));
        total += 1;
    }
    assert_eq!(total, 7000);
    // The generators reach both outcomes, so the property is exercised.
    assert!(accepted > 200 && accepted < total - 2000, "{accepted}");
}

#[test]
fn module_constants() {
    assert_eq!(MAGIC, *b"TPM1");
    assert_eq!(
        (KIND_FOLLOW_UP, KIND_FIRST_CONTACT, KIND_WALLET_STATE),
        (1, 2, 3)
    );
    assert_eq!(envelope::FOLLOW_UP_BODY_BYTES, 128);
    assert_eq!(envelope::FIRST_CONTACT_BODY_BYTES, 1216);
    assert_eq!(MAX_ENVELOPE_BYTES, 4 + 1 + 16 * (33 + 1216));
}
