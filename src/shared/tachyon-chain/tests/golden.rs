//! Golden tests (spec section 13.1): the decoder against frozen testnet blocks.
//!
//! `tests/golden/` holds raw `getblock <hash> 2` and `getblockchaininfo` results
//! captured verbatim by `tests/golden/capture.py`, and `expectations.json`, which
//! that script computes from the raw JSON with Python. The expectations share no
//! code with the decoder, so they are an independent oracle.
//!
//! The `node_client` module serves the same fixtures over real HTTP from a
//! minimal in-test JSON-RPC responder and checks `NodeClient` end to end.
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tachyon_chain::{
    AGGREGATE_ID_BYTES, BlockId, DecodeError, DecodeOptions, EPOCH_LENGTH, EpochSchedule, Hash,
    HashParseError, IdentityError, NetworkIdentity, Stamp, TachyonBlock, ZipRuleViolation,
    decode_block,
};

const STRICT: DecodeOptions = DecodeOptions {
    strict_zip_rules: true,
};
const LAX: DecodeOptions = DecodeOptions {
    strict_zip_rules: false,
};

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

fn read_json(name: &str) -> Value {
    let path = golden_dir().join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path:?}: {e}"))
}

fn manifest() -> Value {
    read_json("manifest.json")
}

fn expectations() -> Value {
    read_json("expectations.json")
}

/// The raw `getblock` result of the frozen block at `height`.
fn raw_block(height: u64) -> Value {
    read_json(&format!("block-{height}.json"))
}

fn hash(displayed: &Value) -> Hash {
    displayed.as_str().unwrap().parse().unwrap()
}

/// Decodes the frozen block at `height` as requested by its own hash and height.
fn decode_frozen(raw: &Value, options: DecodeOptions) -> Result<TachyonBlock, DecodeError> {
    let requested = hash(&raw["hash"]);
    let height = u32::try_from(raw["height"].as_u64().unwrap()).unwrap();
    decode_block(raw, &requested, Some(height), options)
}

/// Asserts that `block` is exactly what `expected` (one expectations entry) says.
fn assert_matches(block: &TachyonBlock, expected: &Value) {
    let at = expected["height"].as_u64().unwrap();
    assert_eq!(u64::from(block.id.height), at);
    assert_eq!(
        block.id.hash.to_string(),
        expected["hash"].as_str().unwrap()
    );
    assert_eq!(
        hex::encode(block.id.hash.protocol_bytes()),
        expected["hash_protocol_hex"].as_str().unwrap(),
        "block {at} hash byte order"
    );
    assert_eq!(block.prev.to_string(), expected["prev"].as_str().unwrap());
    assert_eq!(
        hex::encode(block.prev.protocol_bytes()),
        expected["prev_protocol_hex"].as_str().unwrap()
    );
    let txs = expected["tachyon_txs"].as_array().unwrap();
    assert_eq!(block.txs.len(), txs.len(), "block {at} Tachyon tx count");
    for (tx, want) in block.txs.iter().zip(txs) {
        let index = want["tx_index"].as_u64().unwrap();
        assert_eq!(u64::from(tx.tx_index), index);
        assert_eq!(tx.txid.to_string(), want["txid"].as_str().unwrap());
        assert_eq!(
            hex::encode(tx.txid.protocol_bytes()),
            want["txid_protocol_hex"].as_str().unwrap(),
            "block {at} tx {index} txid byte order"
        );
        assert_eq!(
            u64::from(tx.own_actions),
            want["own_actions"].as_u64().unwrap()
        );
        assert_eq!(hex::encode(&tx.memo), want["memo_hex"].as_str().unwrap());
        assert_eq!(tx.memo.len() as u64, want["memo_len"].as_u64().unwrap());
        match (&tx.stamp, want["stamp"].as_str().unwrap()) {
            (
                Stamp::Proof {
                    anchor,
                    tachygram_set,
                    tachygrams,
                },
                "proof",
            ) => {
                assert_eq!(hex::encode(anchor), want["anchor"].as_str().unwrap());
                assert_eq!(
                    hex::encode(tachygram_set),
                    want["tachygram_set"].as_str().unwrap()
                );
                assert_eq!(
                    tachygrams.len() as u64,
                    want["tachygram_count"].as_u64().unwrap()
                );
                assert_eq!(
                    tachygrams.first().map(hex::encode),
                    want["first_tachygram"].as_str().map(str::to_owned)
                );
                assert_eq!(
                    tachygrams.last().map(hex::encode),
                    want["last_tachygram"].as_str().map(str::to_owned)
                );
                let mut digest = Sha256::new();
                for tachygram in tachygrams {
                    digest.update(tachygram);
                }
                assert_eq!(
                    hex::encode(digest.finalize()),
                    want["tachygrams_sha256"].as_str().unwrap(),
                    "block {at} tx {index} tachygrams"
                );
                assert_eq!(tx.stamp.tachygrams(), tachygrams.as_slice());
                assert!(tx.stamp.is_proof());
            }
            (Stamp::Pointer { aggregate_id }, "pointer") => {
                assert_eq!(
                    hex::encode(aggregate_id),
                    want["aggregate_id"].as_str().unwrap()
                );
                assert_eq!(
                    want["aggregate_id_len"].as_u64().unwrap(),
                    AGGREGATE_ID_BYTES as u64
                );
                assert!(tx.stamp.tachygrams().is_empty());
                assert!(!tx.stamp.is_proof());
            }
            (stamp, kind) => panic!("block {at} tx {index}: decoded {stamp:?}, expected {kind}"),
        }
    }
}

/// Every frozen block decodes to exactly what the independent expectations say,
/// requested with or without its height.
#[test]
fn every_frozen_block_decodes_to_its_expectations() {
    let expectations = expectations();
    let blocks = expectations["blocks"].as_array().unwrap();
    assert!(!blocks.is_empty());
    for expected in blocks {
        let raw = read_json(expected["file"].as_str().unwrap());
        let block = decode_frozen(&raw, LAX).unwrap();
        assert_matches(&block, expected);
        let unheighted = decode_block(&raw, &block.id.hash, None, LAX).unwrap();
        assert_eq!(unheighted, block);
    }
}

/// Epoch arithmetic agrees with the expectations' Python arithmetic on every
/// frozen block, with the activation height read from the frozen node info.
#[test]
fn frozen_blocks_fall_in_their_epochs() {
    let expectations = expectations();
    let activation = expectations["network"]["activation_height"]
        .as_u64()
        .unwrap();
    let schedule = EpochSchedule::new(u32::try_from(activation).unwrap(), EPOCH_LENGTH).unwrap();
    for expected in expectations["blocks"].as_array().unwrap() {
        let height = u32::try_from(expected["height"].as_u64().unwrap()).unwrap();
        assert_eq!(
            schedule.epoch_of(height).map(u64::from),
            expected["epoch"].as_u64(),
            "block {height}"
        );
    }
    assert_eq!(schedule.epoch_of(4103), Some(0));
    assert_eq!(schedule.epoch_of(4104), Some(1));
}

/// Strict decoding accepts a frozen block exactly when the Python checks of
/// the same draft-ZIP rules pass on its raw JSON.
#[test]
fn strict_rules_agree_with_the_raw_data() {
    for expected in expectations()["blocks"].as_array().unwrap() {
        let raw = read_json(expected["file"].as_str().unwrap());
        let txs = expected["tachyon_txs"].as_array().unwrap();
        let follows = expected["tachygrams_distinct_in_block"] == true
            && txs.iter().all(|tx| {
                tx["own_actions"].as_u64().unwrap() <= 4095
                    && (tx["stamp"] == "pointer"
                        || (tx["tachygrams_strictly_ascending"] == true
                            && (2..=8190).contains(&tx["tachygram_count"].as_u64().unwrap())))
            });
        let strict = decode_frozen(&raw, STRICT);
        assert_eq!(strict.is_ok(), follows, "block {}", expected["height"]);
        if let Ok(strict) = strict {
            assert_eq!(strict, decode_frozen(&raw, LAX).unwrap());
        }
    }
}

/// The fixture set holds the blocks the golden layer promises, unchanged since
/// capture.
#[test]
fn fixtures_cover_the_required_blocks() {
    let manifest = manifest();
    let expectations = expectations();
    for entry in manifest["blocks"].as_array().unwrap() {
        let bytes = std::fs::read(golden_dir().join(entry["file"].as_str().unwrap())).unwrap();
        assert_eq!(bytes.len() as u64, entry["bytes"].as_u64().unwrap());
        assert_eq!(
            hex::encode(Sha256::digest(&bytes)),
            entry["sha256"].as_str().unwrap(),
            "{} changed since capture",
            entry["file"]
        );
    }
    let info = std::fs::read(golden_dir().join("getblockchaininfo.json")).unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(&info)),
        manifest["getblockchaininfo"]["sha256"].as_str().unwrap()
    );

    let activation = expectations["network"]["activation_height"]
        .as_u64()
        .unwrap();
    let blocks = expectations["blocks"].as_array().unwrap();
    let has = |test: &dyn Fn(&Value) -> bool| blocks.iter().any(test);
    let txs = |block: &Value| block["tachyon_txs"].as_array().unwrap().clone();
    assert!(has(&|b| b["height"] == 0), "genesis");
    assert!(has(&|b| b["height"] == activation), "activation block");
    assert!(
        has(&|b| b["height"] == activation + 4095),
        "last of epoch 0"
    );
    assert!(
        has(&|b| b["height"] == activation + 4096),
        "first of epoch 1"
    );
    assert!(
        has(&|b| b["height"].as_u64().unwrap() > activation && txs(b).is_empty()),
        "a post-activation block without Tachyon transactions"
    );
    assert!(
        has(&|b| txs(b)
            .iter()
            .any(|t| t["stamp"] == "proof" && t["tachygram_count"].as_u64().unwrap() % 2 == 1)),
        "a proof stamp with an odd tachygram count"
    );
    assert!(
        has(&|b| txs(b).iter().any(|t| t["stamp"] == "pointer")),
        "a pointer stamp"
    );
    assert!(
        has(&|b| txs(b)
            .first()
            .is_some_and(|t| t["tx_index"].as_u64().unwrap() > 1)),
        "a non-Tachyon transaction after the coinbase and before the Tachyon ones"
    );
    let largest = blocks
        .iter()
        .flat_map(txs)
        .filter_map(|t| t["tachygram_count"].as_u64())
        .max()
        .unwrap();
    assert!(largest >= 100, "the largest proof stamp seen has {largest}");
}

/// The frozen `getblockchaininfo` names the testnet, activating at height 8.
#[test]
fn network_identity_parses_the_frozen_node_info() {
    let expected = expectations()["network"].clone();
    let genesis = hash(&expected["genesis_hash"]);
    assert_eq!(
        hex::encode(genesis.protocol_bytes()),
        expected["genesis_protocol_hex"].as_str().unwrap()
    );
    let identity =
        NetworkIdentity::from_blockchaininfo(&read_json("getblockchaininfo.json"), genesis)
            .unwrap();
    assert_eq!(
        identity,
        NetworkIdentity {
            chain: expected["chain"].as_str().unwrap().to_owned(),
            genesis_hash: genesis,
            activation_height: u32::try_from(expected["activation_height"].as_u64().unwrap())
                .unwrap(),
        }
    );
    assert_eq!(identity.chain, "test");
    assert_eq!(identity.activation_height, 8);
    // The genesis hash the manifest records is block 0's own hash.
    assert_eq!(hash(&raw_block(0)["hash"]), genesis);
}

/// The frozen node info with the `NuTachyon` entry edited by `edit`.
fn edited_info(edit: impl FnOnce(&mut serde_json::Map<String, Value>)) -> Value {
    let mut info = read_json("getblockchaininfo.json");
    edit(info["upgrades"].as_object_mut().unwrap());
    info
}

fn tachyon_branch(upgrades: &serde_json::Map<String, Value>) -> String {
    upgrades
        .iter()
        .find(|(_, upgrade)| upgrade["name"] == "NuTachyon")
        .map(|(branch, _)| branch.clone())
        .unwrap()
}

/// Without exactly one usable `NuTachyon` entry there is no network identity,
/// and the publisher refuses to start.
#[test]
fn network_identity_requires_one_tachyon_upgrade() {
    let identity = |info: &Value| NetworkIdentity::from_blockchaininfo(info, Hash::ZERO);
    let missing = edited_info(|upgrades| {
        let branch = tachyon_branch(upgrades);
        upgrades.remove(&branch);
    });
    assert!(matches!(
        identity(&missing),
        Err(IdentityError::MissingTachyonUpgrade)
    ));
    let mut no_upgrades = read_json("getblockchaininfo.json");
    no_upgrades.as_object_mut().unwrap().remove("upgrades");
    assert!(matches!(
        identity(&no_upgrades),
        Err(IdentityError::MissingTachyonUpgrade)
    ));
    let twice = edited_info(|upgrades| {
        let entry = upgrades[&tachyon_branch(upgrades)].clone();
        upgrades.insert("00000000".into(), entry);
    });
    assert!(matches!(
        identity(&twice),
        Err(IdentityError::AmbiguousTachyonUpgrade(2))
    ));
    let beyond = edited_info(|upgrades| {
        let branch = tachyon_branch(upgrades);
        upgrades[&branch]["activationheight"] = json!(1u64 << 32);
    });
    assert!(matches!(
        identity(&beyond),
        Err(IdentityError::ActivationOutOfRange(4_294_967_296))
    ));
    let mut no_chain = read_json("getblockchaininfo.json");
    no_chain.as_object_mut().unwrap().remove("chain");
    assert!(matches!(identity(&no_chain), Err(IdentityError::Shape(_))));
    // A pending upgrade still fixes the activation height.
    let pending = edited_info(|upgrades| {
        let branch = tachyon_branch(upgrades);
        upgrades[&branch]["status"] = json!("pending");
        upgrades[&branch]["activationheight"] = json!(50_000);
    });
    assert_eq!(identity(&pending).unwrap().activation_height, 50_000);
}

/// The frozen block with proof and pointer stamps that the mutation tests edit.
const MIXED: u64 = 40180;

/// The `MIXED` block with `edit` applied, decoded as requested by its own id.
fn decode_edited(edit: impl FnOnce(&mut Value)) -> Result<TachyonBlock, DecodeError> {
    let mut raw = raw_block(MIXED);
    edit(&mut raw);
    decode_block(
        &raw,
        &hash(&raw_block(MIXED)["hash"]),
        Some(MIXED as u32),
        LAX,
    )
}

/// Index of the first transaction in the `MIXED` block whose stamp is `kind`.
fn first_stamp(kind: &str) -> usize {
    raw_block(MIXED)["tx"]
        .as_array()
        .unwrap()
        .iter()
        .position(|tx| tx["tachyon"]["stamp"]["type"] == kind)
        .unwrap()
}

/// A block that is not the requested one is refused, by hash or by height.
#[test]
fn a_block_other_than_the_requested_one_is_refused() {
    let raw = raw_block(MIXED);
    let id = BlockId {
        height: MIXED as u32,
        hash: hash(&raw["hash"]),
    };
    let other = hash(&raw_block(MIXED + 2)["hash"]);
    match decode_block(&raw, &other, None, LAX) {
        Err(DecodeError::HashMismatch {
            requested,
            returned,
        }) => {
            assert_eq!((requested, returned), (other, id.hash));
        }
        other => panic!("{other:?}"),
    }
    match decode_block(&raw, &id.hash, Some(id.height + 1), LAX) {
        Err(DecodeError::HeightMismatch {
            requested,
            returned,
        }) => assert_eq!((requested, returned), (id.height + 1, id.height)),
        other => panic!("{other:?}"),
    }
    // A block that claims the requested hash but whose transactions name
    // another block, or another height, is refused too.
    let named = hash(&raw_block(MIXED + 2)["hash"]);
    match decode_edited(|raw| raw["tx"][2]["blockhash"] = json!(named.to_string())) {
        Err(DecodeError::TxBlockHash {
            tx_index: 2,
            block,
            named: got,
            ..
        }) => assert_eq!((block, got), (id.hash, named)),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        decode_edited(|raw| raw["tx"][0]["height"] = json!(MIXED - 1)),
        Err(DecodeError::TxHeight {
            tx_index: 0,
            named,
            ..
        }) if named == MIXED - 1
    ));
    assert!(matches!(
        decode_edited(|raw| raw["nTx"] = json!(99)),
        Err(DecodeError::TxCount {
            declared: 99,
            listed: 4
        })
    ));
    // A transaction without blockhash or height fields is still accepted.
    let bare = decode_edited(|raw| {
        for tx in raw["tx"].as_array_mut().unwrap() {
            let tx = tx.as_object_mut().unwrap();
            tx.remove("blockhash");
            tx.remove("height");
        }
    })
    .unwrap();
    assert_eq!(bare, decode_frozen(&raw_block(MIXED), LAX).unwrap());
}

/// Hex fields must be hex of exactly their width.
#[test]
fn truncated_and_malformed_hex_is_refused() {
    let proof = first_stamp("proof");
    let pointer = first_stamp("pointer");
    let truncate = |text: &Value, by: usize| {
        let text = text.as_str().unwrap();
        json!(text[..text.len() - by])
    };
    let refused = |result: Result<TachyonBlock, DecodeError>| match result {
        Err(error) => error,
        Ok(_) => panic!("accepted"),
    };
    let length = |error: DecodeError| match error {
        DecodeError::Length {
            field,
            expected,
            actual,
        } => (field.to_string(), expected, actual),
        other => panic!("{other:?}"),
    };
    let odd = |error: DecodeError| match error {
        DecodeError::Hex {
            field,
            source: hex::FromHexError::OddLength,
        } => field.to_string(),
        other => panic!("{other:?}"),
    };

    let error = refused(decode_edited(|raw| {
        raw["tx"][1]["txid"] = truncate(&raw["tx"][1]["txid"], 2)
    }));
    assert_eq!(length(error), ("tx[1].txid".into(), 32, 31));
    let error = refused(decode_edited(|raw| {
        raw["tx"][1]["txid"] = truncate(&raw["tx"][1]["txid"], 1)
    }));
    assert_eq!(odd(error), "tx[1].txid");
    let error = refused(decode_edited(|raw| {
        raw["previousblockhash"] = truncate(&raw["previousblockhash"], 2)
    }));
    assert_eq!(length(error), ("previousblockhash".into(), 32, 31));
    let error = refused(decode_edited(|raw| {
        let stamp = &mut raw["tx"][proof]["tachyon"]["stamp"];
        stamp["anchor"] = truncate(&stamp["anchor"], 1);
    }));
    assert_eq!(odd(error), format!("tx[{proof}].tachyon.stamp.anchor"));
    let error = refused(decode_edited(|raw| {
        let stamp = &mut raw["tx"][proof]["tachyon"]["stamp"];
        stamp["tachygramSet"] = json!(format!("{}00", stamp["tachygramSet"].as_str().unwrap()));
    }));
    assert_eq!(
        length(error),
        (format!("tx[{proof}].tachyon.stamp.tachygramSet"), 32, 33)
    );
    let error = refused(decode_edited(|raw| {
        let tachygrams = &mut raw["tx"][proof]["tachyon"]["stamp"]["tachygrams"];
        tachygrams[3] = truncate(&tachygrams[3], 2);
    }));
    assert_eq!(
        length(error),
        (format!("tx[{proof}].tachyon.stamp.tachygrams[3]"), 32, 31)
    );
    let error = refused(decode_edited(|raw| {
        let stamp = &mut raw["tx"][pointer]["tachyon"]["stamp"];
        stamp["aggregateId"] = truncate(&stamp["aggregateId"], 64);
    }));
    assert_eq!(
        length(error),
        (format!("tx[{pointer}].tachyon.stamp.aggregateId"), 64, 32)
    );
    let error = refused(decode_edited(|raw| {
        raw["tx"][pointer]["tachyon"]["memo"] = json!("abc")
    }));
    assert_eq!(odd(error), format!("tx[{pointer}].tachyon.memo"));
    assert!(matches!(
        decode_edited(|raw| raw["tx"][pointer]["tachyon"]["memo"] = json!("zz")),
        Err(DecodeError::Hex {
            source: hex::FromHexError::InvalidHexCharacter { c: 'z', index: 0 },
            ..
        })
    ));
    assert!(matches!(
        decode_edited(|raw| raw["hash"] = json!("00")),
        Err(DecodeError::Length {
            expected: 32,
            actual: 1,
            ..
        })
    ));
}

/// The frozen memos are all empty, so a non-empty one is decoded from an edit:
/// the memo is the hex's bytes, left to right.
#[test]
fn memos_are_decoded_left_to_right() {
    let pointer = first_stamp("pointer");
    let block =
        decode_edited(|raw| raw["tx"][pointer]["tachyon"]["memo"] = json!("00ff5450")).unwrap();
    let tx = block
        .txs
        .iter()
        .find(|tx| tx.tx_index as usize == pointer)
        .unwrap();
    assert_eq!(tx.memo, [0x00, 0xff, 0x54, 0x50]);
    assert!(
        decode_frozen(&raw_block(MIXED), LAX)
            .unwrap()
            .txs
            .iter()
            .all(|tx| tx.memo.is_empty())
    );
}

/// The same txid twice in a block is refused.
#[test]
fn duplicate_txids_are_refused() {
    let raw = raw_block(MIXED);
    let txid = hash(&raw["tx"][1]["txid"]);
    match decode_edited(|raw| raw["tx"][3]["txid"] = raw["tx"][1]["txid"].clone()) {
        Err(DecodeError::DuplicateTxid {
            txid: got,
            first_index: 1,
            second_index: 3,
        }) => assert_eq!(got, txid),
        other => panic!("{other:?}"),
    }
    // A duplicated transaction, not only its id, is refused the same way.
    assert!(matches!(
        decode_edited(|raw| {
            let tx = raw["tx"][2].clone();
            let txs = raw["tx"].as_array_mut().unwrap();
            txs.push(tx);
            raw["nTx"] = json!(5);
        }),
        Err(DecodeError::DuplicateTxid {
            first_index: 2,
            second_index: 4,
            ..
        })
    ));
}

/// Stamps must be one of the two known shapes, with their own fields only.
#[test]
fn malformed_stamps_are_refused() {
    let proof = first_stamp("proof");
    let pointer = first_stamp("pointer");
    assert!(matches!(
        decode_edited(|raw| raw["tx"][proof]["tachyon"]["stamp"]["type"] = json!("aggregate")),
        Err(DecodeError::UnknownStampType { tx_index, kind }) if tx_index as usize == proof && kind == "aggregate"
    ));
    for field in ["anchor", "tachygramSet", "tachygrams"] {
        match decode_edited(|raw| {
            raw["tx"][proof]["tachyon"]["stamp"]
                .as_object_mut()
                .unwrap()
                .remove(field);
        }) {
            Err(DecodeError::MissingField(missing)) => {
                assert_eq!(
                    missing.to_string(),
                    format!("tx[{proof}].tachyon.stamp.{field}")
                )
            }
            other => panic!("{field}: {other:?}"),
        }
    }
    assert!(matches!(
        decode_edited(|raw| {
            raw["tx"][pointer]["tachyon"]["stamp"]
                .as_object_mut()
                .unwrap()
                .remove("aggregateId");
        }),
        Err(DecodeError::MissingField(_))
    ));
    assert!(matches!(
        decode_edited(|raw| raw["tx"][pointer]["tachyon"]["stamp"]["tachygrams"] = json!([])),
        Err(DecodeError::ForeignStampField {
            kind: "pointer",
            ..
        })
    ));
    assert!(matches!(
        decode_edited(|raw| {
            let aggregate = raw["tx"][pointer]["tachyon"]["stamp"]["aggregateId"].clone();
            raw["tx"][proof]["tachyon"]["stamp"]["aggregateId"] = aggregate;
        }),
        Err(DecodeError::ForeignStampField { kind: "proof", .. })
    ));
    // A bundle without a memo or a stamp, or a verbosity-1 block, has the wrong
    // shape.
    for remove in ["memo", "stamp", "actions"] {
        assert!(matches!(
            decode_edited(|raw| {
                raw["tx"][pointer]["tachyon"]
                    .as_object_mut()
                    .unwrap()
                    .remove(remove);
            }),
            Err(DecodeError::Shape(_))
        ));
    }
    assert!(matches!(
        decode_edited(|raw| {
            let ids: Vec<Value> = raw["tx"]
                .as_array()
                .unwrap()
                .iter()
                .map(|tx| tx["txid"].clone())
                .collect();
            raw["tx"] = Value::Array(ids);
        }),
        Err(DecodeError::Shape(_))
    ));
}

/// Tachygram counts come from the data: an empty proof stamp is decoded as
/// empty when the draft-ZIP rules are off.
#[test]
fn lax_decoding_reads_counts_from_the_data() {
    let proof = first_stamp("proof");
    let block = decode_edited(|raw| {
        raw["tx"][proof]["tachyon"]["stamp"]["tachygrams"] = json!([]);
        raw["tx"][proof]["tachyon"]["actions"] = json!([]);
    })
    .unwrap();
    let tx = &block.txs[0];
    assert_eq!(tx.tx_index as usize, proof);
    assert_eq!(tx.own_actions, 0);
    assert!(tx.stamp.is_proof() && tx.stamp.tachygrams().is_empty());
}

/// Each draft-ZIP rule, broken by an edit, is refused only in strict mode.
#[test]
fn strict_mode_enforces_each_draft_zip_rule() {
    let proof = first_stamp("proof");
    let pointer = first_stamp("pointer");
    let strict = |edit: &dyn Fn(&mut Value)| {
        let mut raw = raw_block(MIXED);
        edit(&mut raw);
        assert!(decode_frozen(&raw, LAX).is_ok(), "lax decoding refused it");
        match decode_frozen(&raw, STRICT) {
            Err(DecodeError::ZipRule {
                tx_index,
                violation,
                ..
            }) => (tx_index as usize, violation),
            other => panic!("{other:?}"),
        }
    };
    assert!(decode_frozen(&raw_block(MIXED), STRICT).is_ok());

    let too_many = strict(&|raw| raw["tx"][pointer]["tachyon"]["actions"] = json!(vec![0; 4096]));
    assert_eq!(too_many, (pointer, ZipRuleViolation::TooManyActions(4096)));
    let at_most = {
        let mut raw = raw_block(MIXED);
        raw["tx"][pointer]["tachyon"]["actions"] = json!(vec![0; 4095]);
        decode_frozen(&raw, STRICT).unwrap()
    };
    let widened = at_most
        .txs
        .iter()
        .find(|tx| tx.tx_index as usize == pointer);
    assert_eq!(widened.unwrap().own_actions, 4095);

    let one = strict(&|raw| {
        let tachygrams = &mut raw["tx"][proof]["tachyon"]["stamp"]["tachygrams"];
        tachygrams.as_array_mut().unwrap().truncate(1);
    });
    assert_eq!(one, (proof, ZipRuleViolation::TachygramCount(1)));
    let swapped = strict(&|raw| {
        let tachygrams = raw["tx"][proof]["tachyon"]["stamp"]["tachygrams"]
            .as_array_mut()
            .unwrap();
        tachygrams.swap(4, 5);
    });
    assert_eq!(swapped, (proof, ZipRuleViolation::TachygramOrder(5)));
    let repeated = strict(&|raw| {
        let tachygrams = raw["tx"][proof]["tachyon"]["stamp"]["tachygrams"]
            .as_array_mut()
            .unwrap();
        tachygrams[6] = tachygrams[5].clone();
    });
    assert_eq!(repeated, (proof, ZipRuleViolation::TachygramOrder(6)));
    // A second proof stamp in the block that repeats one of the first's
    // tachygrams.
    let across = strict(&|raw| {
        let first = raw["tx"][proof]["tachyon"]["stamp"].clone();
        let tachygrams = first["tachygrams"].as_array().unwrap();
        let mut second = first.clone();
        second["tachygrams"] = json!([
            "0000000000000000000000000000000000000000000000000000000000000001",
            tachygrams[0].clone(),
        ]);
        raw["tx"][pointer]["tachyon"]["stamp"] = second;
    });
    assert_eq!(
        across,
        (
            pointer,
            ZipRuleViolation::DuplicateTachygram {
                item: 1,
                first_tx_index: proof as u32,
            }
        )
    );
}

/// The genesis block's parent is the zero hash; only genesis may omit it.
#[test]
fn only_genesis_may_lack_a_parent() {
    // Zakura prints the zero hash for genesis; zcashd omits the field.
    let mut genesis = raw_block(0);
    assert_eq!(genesis["previousblockhash"], "00".repeat(32));
    let block = decode_frozen(&genesis, LAX).unwrap();
    assert_eq!(block.prev, Hash::ZERO);
    assert_eq!(block.id.height, 0);
    genesis.as_object_mut().unwrap().remove("previousblockhash");
    assert_eq!(decode_frozen(&genesis, LAX).unwrap(), block);
    assert!(matches!(
        decode_edited(|raw| {
            raw.as_object_mut().unwrap().remove("previousblockhash");
        }),
        Err(DecodeError::MissingPrevious(h)) if u64::from(h) == MIXED
    ));
    assert!(matches!(
        decode_edited(|raw| raw["height"] = json!(1u64 << 32)),
        Err(DecodeError::OutOfRange {
            value: 4_294_967_296,
            ..
        })
    ));
}

/// Hash parsing refuses what is not a 64-digit hex hash.
#[test]
fn display_hashes_parse_strictly() {
    let displayed = raw_block(MIXED)["hash"].as_str().unwrap().to_owned();
    let parsed: Hash = displayed.parse().unwrap();
    let mut reversed = hex::decode(&displayed).unwrap();
    reversed.reverse();
    assert_eq!(parsed.protocol_bytes().as_slice(), reversed.as_slice());
    assert_eq!(parsed.to_string(), displayed);
    assert_eq!(
        Hash::parse_display(&displayed[..62]),
        Err(HashParseError::Length(31))
    );
    assert!(matches!(
        Hash::parse_display(&displayed[..63]),
        Err(HashParseError::Hex(_))
    ));
}

mod node_client {
    //! `NodeClient` over real HTTP against a minimal JSON-RPC responder that
    //! serves the frozen fixtures.
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tachyon_chain::{Credentials, HttpPolicy, NodeClient, NodeError, RpcClient, RpcError};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// One JSON-RPC call the responder answers: method and params.
    type Call = (String, Value);
    /// How the responder answers a call: `Ok(result)` or `Err((code, message))`.
    type Handler = dyn Fn(&str, &Value) -> Result<Value, (i64, String)> + Send + Sync;

    /// Serves `handler` over HTTP/1.1, one request per connection, recording
    /// every call. Returns the URL and the record.
    async fn serve(handler: Arc<Handler>) -> (String, Arc<Mutex<Vec<Call>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let record = calls.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let handler = handler.clone();
                let record = record.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buffer = [0u8; 8192];
                    let head = loop {
                        let n = socket.read(&mut buffer).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&buffer[..n]);
                        if let Some(at) = request.windows(4).position(|b| b == b"\r\n\r\n") {
                            break at;
                        }
                    };
                    let text = String::from_utf8_lossy(&request[..head]).to_ascii_lowercase();
                    let length: usize = text
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .map_or(0, |value| value.trim().parse().unwrap());
                    let mut body = request[head + 4..].to_vec();
                    while body.len() < length {
                        let n = socket.read(&mut buffer).await.unwrap();
                        assert!(n > 0, "request body truncated");
                        body.extend_from_slice(&buffer[..n]);
                    }
                    let request: Value = serde_json::from_slice(&body).unwrap();
                    let method = request["method"].as_str().unwrap().to_owned();
                    let params = request["params"].clone();
                    record
                        .lock()
                        .unwrap()
                        .push((method.clone(), params.clone()));
                    let answer = match handler(&method, &params) {
                        Ok(result) => json!({"result": result, "error": null, "id": request["id"]}),
                        Err((code, message)) => json!({
                            "result": null,
                            "error": {"code": code, "message": message},
                            "id": request["id"],
                        }),
                    }
                    .to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                        answer.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        (url, calls)
    }

    /// The frozen chain as a node would serve it: node info, the tip (the
    /// highest frozen block), hashes by height and blocks by display hash.
    #[derive(Clone)]
    struct Frozen {
        info: Value,
        tip: u64,
        hashes: Vec<(u64, String)>,
        blocks: Vec<(String, Value)>,
    }

    impl Frozen {
        fn load() -> Self {
            let manifest = manifest();
            let mut hashes = vec![(0, manifest["genesis_hash"].as_str().unwrap().to_owned())];
            let mut blocks = Vec::new();
            for entry in manifest["blocks"].as_array().unwrap() {
                let display = entry["hash"].as_str().unwrap().to_owned();
                hashes.push((entry["height"].as_u64().unwrap(), display.clone()));
                blocks.push((display, read_json(entry["file"].as_str().unwrap())));
            }
            let tip = hashes.iter().map(|(height, _)| *height).max().unwrap();
            Self {
                info: read_json("getblockchaininfo.json"),
                tip,
                hashes,
                blocks,
            }
        }

        fn answer(&self, method: &str, params: &Value) -> Result<Value, (i64, String)> {
            let not_found = || Err((-5, "Block not found".to_owned()));
            match method {
                "getblockchaininfo" => Ok(self.info.clone()),
                "getblockcount" => Ok(json!(self.tip)),
                "getblockhash" => {
                    let height = params[0].as_u64().unwrap();
                    match self.hashes.iter().find(|(h, _)| *h == height) {
                        Some((_, hash)) => Ok(json!(hash)),
                        None => Err((-8, "Block height out of range".to_owned())),
                    }
                }
                "getblock" => {
                    assert_eq!(params[1], 2, "getblock is read at verbosity 2");
                    let wanted = params[0].as_str().unwrap();
                    match self.blocks.iter().find(|(hash, _)| hash == wanted) {
                        Some((_, block)) => Ok(block.clone()),
                        None => not_found(),
                    }
                }
                other => panic!("NodeClient called {other}"),
            }
        }
    }

    const POLICY: HttpPolicy = HttpPolicy {
        connection_timeout: Some(Duration::from_secs(5)),
        request_timeout: Duration::from_secs(20),
        response_limit: 16 * 1024 * 1024,
    };

    async fn client_for(
        handler: Arc<Handler>,
        options: DecodeOptions,
    ) -> (NodeClient, Arc<Mutex<Vec<Call>>>) {
        let (url, calls) = serve(handler).await;
        let rpc = RpcClient::new(url, Credentials::None, "tachyon-chain-test", POLICY).unwrap();
        (NodeClient::new(rpc, options), calls)
    }

    async fn frozen_client() -> (NodeClient, Arc<Mutex<Vec<Call>>>, Frozen) {
        let frozen = Frozen::load();
        let serving = frozen.clone();
        let (client, calls) = client_for(
            Arc::new(move |method: &str, params: &Value| serving.answer(method, params)),
            LAX,
        )
        .await;
        (client, calls, frozen)
    }

    fn methods(calls: &Mutex<Vec<Call>>) -> Vec<String> {
        calls
            .lock()
            .unwrap()
            .iter()
            .map(|(method, _)| method.clone())
            .collect()
    }

    #[tokio::test]
    async fn network_identity_is_read_from_the_node() {
        let (client, calls, _) = frozen_client().await;
        let expected = expectations()["network"].clone();
        let identity = client.network_identity().await.unwrap();
        assert_eq!(identity.chain, expected["chain"].as_str().unwrap());
        assert_eq!(identity.genesis_hash, hash(&expected["genesis_hash"]));
        assert_eq!(
            u64::from(identity.activation_height),
            expected["activation_height"].as_u64().unwrap()
        );
        assert_eq!(methods(&calls), ["getblockchaininfo", "getblockhash"]);
        assert_eq!(calls.lock().unwrap()[1].1, json!([0]));
    }

    #[tokio::test]
    async fn tip_and_block_hashes_are_read_in_protocol_order() {
        let (client, calls, frozen) = frozen_client().await;
        let tip = client.tip().await.unwrap();
        let tip_hash = &frozen
            .hashes
            .iter()
            .find(|(h, _)| *h == frozen.tip)
            .unwrap()
            .1;
        assert_eq!(u64::from(tip.height), frozen.tip);
        assert_eq!(tip.hash.to_string(), *tip_hash);
        assert_eq!(methods(&calls), ["getblockcount", "getblockhash"]);
        for (height, displayed) in &frozen.hashes {
            let got = client
                .block_hash(u32::try_from(*height).unwrap())
                .await
                .unwrap();
            let mut protocol = hex::decode(displayed).unwrap();
            protocol.reverse();
            assert_eq!(got.protocol_bytes().as_slice(), protocol.as_slice());
        }
        assert!(matches!(
            client.block_hash(7).await,
            Err(NodeError::Rpc(RpcError::Rpc(-8, _)))
        ));
    }

    /// Every frozen block, fetched by hash over HTTP, decodes to its expectations.
    #[tokio::test]
    async fn blocks_are_fetched_by_hash_and_verified() {
        let (client, calls, _) = frozen_client().await;
        for expected in expectations()["blocks"].as_array().unwrap() {
            let id = BlockId {
                height: u32::try_from(expected["height"].as_u64().unwrap()).unwrap(),
                hash: hash(&expected["hash"]),
            };
            let by_hash = client.block(&id.hash).await.unwrap();
            assert_matches(&by_hash, expected);
            assert_eq!(client.block_at(&id).await.unwrap(), by_hash);
            let (method, params) = calls.lock().unwrap().last().unwrap().clone();
            assert_eq!(method, "getblock");
            assert_eq!(params, json!([expected["hash"], 2]));
        }
        assert!(
            methods(&calls).iter().all(|method| method == "getblock"),
            "blocks are read with getblock alone"
        );
        let unknown = Hash::from_protocol_bytes([7; 32]);
        assert!(matches!(
            client.block(&unknown).await,
            Err(NodeError::Rpc(RpcError::Rpc(-5, _)))
        ));
    }

    /// A node that answers with another block than the one asked for, or at
    /// another height, is refused.
    #[tokio::test]
    async fn a_node_serving_the_wrong_block_is_refused() {
        let frozen = Frozen::load();
        let substitute = raw_block(MIXED + 2);
        let (client, _) = client_for(
            Arc::new(move |method: &str, params: &Value| match method {
                "getblock" => Ok(substitute.clone()),
                _ => frozen.answer(method, params),
            }),
            LAX,
        )
        .await;
        let wanted = hash(&raw_block(MIXED)["hash"]);
        assert!(matches!(
            client.block(&wanted).await,
            Err(NodeError::Decode(DecodeError::HashMismatch { requested, .. })) if requested == wanted
        ));

        let (client, _, _) = frozen_client().await;
        let id = BlockId {
            height: MIXED as u32 + 1,
            hash: wanted,
        };
        assert!(matches!(
            client.block_at(&id).await,
            Err(NodeError::Decode(DecodeError::HeightMismatch { .. }))
        ));
    }

    /// The client's decode options apply to blocks it fetches.
    #[tokio::test]
    async fn strict_clients_apply_the_draft_zip_rules() {
        let frozen = Frozen::load();
        let proof = first_stamp("proof");
        let wanted = raw_block(MIXED)["hash"].as_str().unwrap().to_owned();
        let mut unordered = raw_block(MIXED);
        unordered["tx"][proof]["tachyon"]["stamp"]["tachygrams"]
            .as_array_mut()
            .unwrap()
            .reverse();
        let handler: Arc<Handler> = Arc::new(move |method: &str, params: &Value| {
            if method == "getblock" && params[0] == wanted {
                return Ok(unordered.clone());
            }
            frozen.answer(method, params)
        });
        let hash = hash(&raw_block(MIXED)["hash"]);
        let (lax, _) = client_for(handler.clone(), LAX).await;
        assert!(lax.block(&hash).await.is_ok());
        let (strict, _) = client_for(handler, STRICT).await;
        assert!(strict.options().strict_zip_rules);
        assert!(matches!(
            strict.block(&hash).await,
            Err(NodeError::Decode(DecodeError::ZipRule {
                violation: ZipRuleViolation::TachygramOrder(1),
                ..
            }))
        ));
    }

    /// A node without the `NuTachyon` upgrade gives no identity; a malformed
    /// hash or tip is refused.
    #[tokio::test]
    async fn a_node_without_tachyon_or_with_bad_answers_is_refused() {
        let frozen = Frozen::load();
        let mut info = frozen.info.clone();
        let branch = tachyon_branch(info["upgrades"].as_object().unwrap());
        info["upgrades"].as_object_mut().unwrap().remove(&branch);
        let (client, _) = client_for(
            Arc::new(move |method: &str, params: &Value| match method {
                "getblockchaininfo" => Ok(info.clone()),
                "getblockhash" if params[0] == 5 => Ok(json!("abcd")),
                "getblockcount" => Ok(json!(1u64 << 32)),
                _ => frozen.answer(method, params),
            }),
            LAX,
        )
        .await;
        assert!(matches!(
            client.network_identity().await,
            Err(NodeError::Identity(IdentityError::MissingTachyonUpgrade))
        ));
        assert!(matches!(
            client.block_hash(5).await,
            Err(NodeError::Hash {
                method: "getblockhash",
                source: HashParseError::Length(2)
            })
        ));
        assert!(matches!(
            client.tip().await,
            Err(NodeError::TipOutOfRange(4_294_967_296))
        ));
    }

    /// `NodeClient::connect` builds a working client under the shared policy.
    #[tokio::test]
    async fn connect_uses_the_shared_policy() {
        let frozen = Frozen::load();
        let tip = frozen.tip;
        let (url, calls) = serve(Arc::new(move |method: &str, params: &Value| {
            frozen.answer(method, params)
        }))
        .await;
        let client = NodeClient::connect(url.clone(), Credentials::None, LAX).unwrap();
        assert_eq!(client.rpc().url(), url);
        assert_eq!(client.options(), LAX);
        assert_eq!(u64::from(client.tip().await.unwrap().height), tip);
        assert_eq!(methods(&calls), ["getblockcount", "getblockhash"]);
    }
}
