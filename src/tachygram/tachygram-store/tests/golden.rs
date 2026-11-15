//! Stamp extraction from the frozen alpha-testnet blocks of
//! `tachyon-chain/tests/golden/`, checked against that directory's
//! `expectations.json`, which the capture script computes in Python from the
//! raw JSON (an oracle independent of the decoder and of this crate).
//!
//! The frozen blocks are not contiguous, so each is appended to a store of its
//! own; that still runs real testnet tachygrams through the integrity checks
//! of spec 8.3 and 6.2.
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tachygram_store::{Binding, EpochSummary, IndexedBlock, Store};
use tachyon_chain::{DecodeOptions, EPOCH_LENGTH, Hash, TachyonBlock, decode_block};

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../shared/tachyon-chain/tests/golden")
}

fn read_json(name: &str) -> Value {
    let path = golden_dir().join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path:?}: {e}"))
}

fn decode(raw: &Value) -> TachyonBlock {
    let hash: Hash = raw["hash"].as_str().unwrap().parse().unwrap();
    let height = u32::try_from(raw["height"].as_u64().unwrap()).unwrap();
    decode_block(raw, &hash, Some(height), DecodeOptions::default()).unwrap()
}

/// The testnet binding, from the expectations' network section.
fn testnet_binding(expectations: &Value) -> Binding {
    let network = &expectations["network"];
    Binding {
        chain: network["chain"].as_str().unwrap().to_owned(),
        genesis_hash: network["genesis_hash"].as_str().unwrap().parse().unwrap(),
        activation_height: u32::try_from(network["activation_height"].as_u64().unwrap()).unwrap(),
        epoch_length: EPOCH_LENGTH,
    }
}

#[test]
fn frozen_testnet_blocks_yield_their_proof_stamps() {
    let expectations = read_json("expectations.json");
    let binding = testnet_binding(&expectations);
    let schedule = binding.schedule().unwrap();
    let blocks = expectations["blocks"].as_array().unwrap();
    assert_eq!(blocks.len(), 10);
    let (mut proofs, mut pointers, mut odd) = (0, 0, 0);
    let dir = tempfile::tempdir().unwrap();
    for expected in blocks {
        let at = expected["height"].as_u64().unwrap() as u32;
        let raw = read_json(expected["file"].as_str().unwrap());
        let indexed = IndexedBlock::new(&decode(&raw), &schedule);
        assert_eq!(indexed.id.height, at);
        assert_eq!(
            indexed.id.hash.to_string(),
            expected["hash"].as_str().unwrap()
        );
        assert_eq!(indexed.prev.to_string(), expected["prev"].as_str().unwrap());
        assert_eq!(
            indexed.epoch.map(u64::from),
            expected["epoch"].as_u64(),
            "block {at} epoch"
        );
        // Proof stamps in transaction order; pointer stamps contribute nothing.
        let want: Vec<&Value> = expected["tachyon_txs"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|tx| tx["stamp"] == "proof")
            .collect();
        assert_eq!(
            want.len() as u64,
            expected["proof_stamps"].as_u64().unwrap()
        );
        assert_eq!(indexed.stamps.len(), want.len(), "block {at} proof stamps");
        proofs += want.len();
        pointers += expected["pointer_stamps"].as_u64().unwrap();
        for (stamp, want) in indexed.stamps.iter().zip(&want) {
            assert_eq!(
                u64::from(stamp.tx_index),
                want["tx_index"].as_u64().unwrap()
            );
            assert_eq!(stamp.txid.to_string(), want["txid"].as_str().unwrap());
            let count = want["tachygram_count"].as_u64().unwrap();
            assert_eq!(stamp.tachygrams.len() as u64, count, "block {at} count");
            odd += usize::from(count % 2 == 1);
            assert_eq!(
                stamp.tachygrams.first().map(hex::encode).as_deref(),
                want["first_tachygram"].as_str()
            );
            assert_eq!(
                stamp.tachygrams.last().map(hex::encode).as_deref(),
                want["last_tachygram"].as_str()
            );
            let mut digest = Sha256::new();
            for tachygram in &stamp.tachygrams {
                digest.update(tachygram);
            }
            assert_eq!(
                hex::encode(digest.finalize()),
                want["tachygrams_sha256"].as_str().unwrap(),
                "block {at} tachygrams"
            );
        }

        // Each frozen block alone, as the first block of its own store.
        let mut store =
            Store::open(dir.path().join(format!("{at}.sqlite")), binding.clone()).unwrap();
        store.append(&indexed).unwrap();
        assert_eq!(store.committed(), Some(indexed.id));
        let Some(epoch) = indexed.epoch else {
            assert!(indexed.stamps.is_empty());
            assert_eq!(store.epochs().unwrap(), Vec::<u32>::new());
            continue;
        };
        let stamps = store.stamps(epoch).unwrap();
        assert_eq!(stamps.len(), indexed.stamps.len());
        for (ordinal, (stored, stamp)) in stamps.iter().zip(&indexed.stamps).enumerate() {
            assert_eq!(stored.ordinal.get() as usize, ordinal);
            assert_eq!(stored.height, at);
            assert_eq!(stored.tx_index, stamp.tx_index);
            assert_eq!(stored.txid, stamp.txid);
            assert_eq!(stored.tachygram_count as usize, stamp.tachygrams.len());
        }
        let records = store.epoch_records(epoch).unwrap();
        assert_eq!(records.len(), indexed.tachygram_count());
        assert!(records.windows(2).all(|w| w[0].tachygram < w[1].tachygram));
        assert!(records.iter().all(|r| r.blk_height == at));
        let in_order: Vec<[u8; 32]> = indexed
            .stamps
            .iter()
            .flat_map(|s| s.tachygrams.iter().copied())
            .collect();
        assert!(
            store
                .epoch_tachygrams(epoch)
                .unwrap()
                .map(|(_, t)| t)
                .eq(in_order)
        );
        assert_eq!(
            store.epoch_summary(epoch).unwrap(),
            Some(EpochSummary {
                epoch,
                entries: indexed.tachygram_count() as u64,
                stamps: indexed.stamps.len() as u32,
                first_height: at,
                last_height: at,
            })
        );
    }
    // The frozen set covers what it was chosen for (tachyon-chain's README).
    assert_eq!(proofs, 5);
    assert!(pointers > 0);
    assert!(odd > 0, "an odd tachygram count");
    let epoch_of = |height: u32| schedule.epoch_of(height);
    assert_eq!(
        (epoch_of(0), epoch_of(4103), epoch_of(4104)),
        (None, Some(0), Some(1))
    );
}
