//! The node's JSON shapes, rendered from the authored model.
//!
//! Field names, nesting and value types follow responses saved from the alpha
//! testnet's Zakura node (`tests/data/`): `getblock <hash> 2` and
//! `getblockchaininfo`. Values the services do not read (roots, solutions,
//! transparent scripts) are well-formed filler of the same length.
use crate::hash::{Hash32, base58check, display, sha256d};
use crate::model::{
    BLOCK_VERSION, Block, Coinbase, Stamp, SyntheticChain, TX_VERSION, TachyonBundle, Tx, TxBody,
};
use serde_json::{Value, json};

/// Branch ids and names of the upgrades the testnet lists, with the testnet's
/// activation heights. NuTachyon is appended at the configured height.
const UPGRADES: &[(&str, &str, u64)] = &[
    ("5ba81b19", "Overwinter", 1),
    ("76b809bb", "Sapling", 1),
    ("2bb40e60", "Blossom", 1),
    ("f5b9230b", "Heartwood", 1),
    ("e9ff75a6", "Canopy", 1),
    ("c2d6d0b4", "NU5", 2),
    ("c8e71055", "NU6", 3),
    ("4dec4df0", "NU6.1", 4),
    ("5437f330", "NU6.2", 5),
    ("37a5165b", "NU6.3", 6),
    ("77190ad9", "NU7", 8),
];
/// NuTachyon's branch id on the testnet.
pub const NU_TACHYON_BRANCH_ID: &str = "fffffffc";
/// The testnet transaction version group id.
const VERSION_GROUP_ID: &str = "74616368";
/// The testnet's P2PKH address prefix.
const TESTNET_P2PKH_PREFIX: [u8; 2] = [0x1d, 0x25];
/// Equihash solution length of the testnet's blocks, in bytes.
const SOLUTION_BYTES: usize = 1344;
/// The finalized depth the testnet reports in `header_chain`.
const FINALIZED_DEPTH: u64 = 1000;
const ZAT_PER_ZEC: f64 = 100_000_000.0;
/// Pools the testnet lists in `valuePools`, in order. Only transparent and
/// Tachyon carry value on the synthetic chain.
const POOLS: &[&str] = &[
    "transparent",
    "sprout",
    "sapling",
    "orchard",
    "lockbox",
    "ironwood",
    "tachyon",
];

fn zec(zat: i64) -> f64 {
    zat as f64 / ZAT_PER_ZEC
}

/// The upgrades `getblockchaininfo` lists, as (branch id, name, activation
/// height). Other upgrades activate no later than NuTachyon.
pub fn upgrades(chain: &SyntheticChain) -> Vec<(&'static str, &'static str, u64)> {
    let cap = chain.activation_height().unwrap_or(u64::MAX);
    let mut out: Vec<_> = UPGRADES
        .iter()
        .map(|(id, name, height)| (*id, *name, (*height).min(cap)))
        .collect();
    if let Some(activation) = chain.activation_height() {
        out.push((NU_TACHYON_BRANCH_ID, "NuTachyon", activation));
    }
    out
}

/// The branch id in force at `height`: the latest upgrade active there.
fn branch_at(chain: &SyntheticChain, height: u64) -> &'static str {
    upgrades(chain)
        .into_iter()
        .filter(|(_, _, activation)| *activation <= height)
        .max_by_key(|(_, _, activation)| *activation)
        .map_or("00000000", |(id, _, _)| id)
}

/// The `getblockchaininfo` result.
pub fn blockchain_info(chain: &SyntheticChain) -> Value {
    let tip = chain.tip();
    let tip_height = tip.height();
    let mut upgrades_json = serde_json::Map::new();
    for (id, name, height) in upgrades(chain) {
        let status = if height <= tip_height {
            "active"
        } else {
            "pending"
        };
        upgrades_json.insert(
            id.to_owned(),
            json!({"name": name, "activationheight": height, "status": status}),
        );
    }
    let finalized = chain
        .block(tip_height.saturating_sub(FINALIZED_DEPTH))
        .expect("height at or below the tip");
    let size_on_disk: usize = chain.blocks().iter().map(Block::size).sum();
    let best = json!({"height": tip_height, "hash": tip.hash_display()});
    json!({
        "chain": chain.chain_name(),
        "blocks": tip_height,
        "headers": tip_height,
        "difficulty": 1.0,
        "verificationprogress": 1.0,
        "chainwork": 0,
        "pruned": false,
        "size_on_disk": size_on_disk,
        "commitments": 0,
        "bestblockhash": tip.hash_display(),
        "estimatedheight": tip_height,
        "chainSupply": chain_supply(tip),
        "valuePools": value_pools(tip, false),
        "nsmValueBalanceZat": 0,
        "upgrades": upgrades_json,
        "consensus": {
            "chaintip": branch_at(chain, tip_height),
            "nextblock": branch_at(chain, tip_height + 1),
        },
        "header_chain": {
            "mode": "integrated",
            "state_version": 1,
            "header_best_semantics": "synthetic node double: header best is the active tip",
            "header_best": best,
            "verified_best": best,
            "finalized": {"height": finalized.height(), "hash": finalized.hash_display()},
            "alarms": {
                "resource_stalled": false,
                "header_best_body_unavailable": null,
                "migrated_pin_refuted": null,
            },
        },
    })
}

fn chain_supply(block: &Block) -> Value {
    let total = block.transparent_supply_zat() + block.tachyon_supply_zat();
    json!({"chainValue": zec(total), "chainValueZat": total, "monitored": true})
}

fn value_pools(block: &Block, with_delta: bool) -> Value {
    let pools = POOLS
        .iter()
        .map(|id| {
            let (value, delta, monitored) = match *id {
                "transparent" => (
                    block.transparent_supply_zat(),
                    block.transparent_delta_zat(),
                    true,
                ),
                "tachyon" => (block.tachyon_supply_zat(), block.tachyon_delta_zat(), true),
                _ => (0, 0, false),
            };
            let mut pool = json!({
                "id": id,
                "chainValue": zec(value),
                "chainValueZat": value,
                "monitored": monitored,
            });
            if with_delta {
                pool["valueDelta"] = json!(zec(delta));
                pool["valueDeltaZat"] = json!(delta);
            }
            pool
        })
        .collect();
    Value::Array(pools)
}

/// Where a served block sits relative to the active chain.
#[derive(Clone, Copy, Debug)]
pub struct Placement {
    /// Whether the block is on the active chain.
    pub active: bool,
    /// The active tip height.
    pub tip: u64,
    /// The next active block's display hash, if any.
    pub next: Option<Hash32>,
}

impl Placement {
    /// Where `block` sits in `chain`.
    pub fn of(chain: &SyntheticChain, block: &Block, active: bool) -> Self {
        Self {
            active,
            tip: chain.tip_height(),
            next: active
                .then(|| chain.block(block.height() + 1).map(Block::hash))
                .flatten(),
        }
    }

    /// `confirmations` of the block: -1 off the active chain, as zcashd reports.
    fn block_confirmations(&self, height: u64) -> i64 {
        if self.active {
            (self.tip - height + 1) as i64
        } else {
            -1
        }
    }

    /// `confirmations` of a transaction in the block: 0 off the active chain.
    fn tx_confirmations(&self, height: u64) -> i64 {
        if self.active {
            (self.tip - height + 1) as i64
        } else {
            0
        }
    }
}

/// The `getblock` result at `verbosity` 1 (txids) or 2 (transaction objects);
/// verbosity 0 is the hex of [`Block::bytes`].
pub fn block(block: &Block, placement: Placement, verbosity: u8) -> Value {
    if verbosity == 0 {
        return json!(hex::encode(block.bytes()));
    }
    let txs: Vec<Value> = block
        .txs()
        .iter()
        .map(|tx| {
            if verbosity == 1 {
                json!(tx.txid_display())
            } else {
                transaction(tx, block, placement)
            }
        })
        .collect();
    let mut out = json!({
        "hash": block.hash_display(),
        "confirmations": placement.block_confirmations(block.height()),
        "size": block.size(),
        "height": block.height(),
        "version": BLOCK_VERSION,
        "merkleroot": display(&block.merkle_root()),
        "blockcommitments": filler_hex(block, b"blockcommitments"),
        "finalsaplingroot": filler_hex(block, b"finalsaplingroot"),
        "finalorchardroot": filler_hex(block, b"finalorchardroot"),
        "nTx": block.txs().len(),
        "tx": txs,
        "time": block.time(),
        "nonce": display(&block.salt_nonce()),
        "solution": "00".repeat(SOLUTION_BYTES),
        "bits": "2007ffff",
        "difficulty": 1.0,
        "chainSupply": chain_supply(block),
        "valuePools": value_pools(block, true),
        "trees": {"ironwood": {"size": 0}},
    });
    if block.height() > 0 {
        out["previousblockhash"] = json!(display(&block.prev()));
    }
    if let Some(next) = placement.next {
        out["nextblockhash"] = json!(display(&next));
    }
    out
}

/// A 32-byte filler field derived from the block hash.
fn filler_hex(block: &Block, label: &[u8]) -> String {
    display(&sha256d(&[label, &block.hash()]))
}

impl Block {
    /// The header `nonce` the node double prints: the fork salt, little-endian,
    /// padded to 32 bytes.
    pub fn salt_nonce(&self) -> Hash32 {
        let mut nonce = [0u8; 32];
        nonce[..8].copy_from_slice(&self.salt().to_le_bytes());
        nonce
    }
}

/// One `tx[]` entry of a verbosity-2 `getblock`.
pub fn transaction(tx: &Tx, block: &Block, placement: Placement) -> Value {
    let height = block.height();
    let (vin, vout) = match tx.body() {
        TxBody::Coinbase(coinbase) => (
            json!([{"coinbase": coinbase_script(coinbase), "sequence": u32::MAX}]),
            json!([coinbase_output(coinbase)]),
        ),
        TxBody::Tachyon(_) => (json!([]), json!([])),
    };
    let mut out = json!({
        "in_active_chain": placement.active,
        "hex": hex::encode(tx.bytes()),
        "height": height,
        "confirmations": placement.tx_confirmations(height),
        "vin": vin,
        "vout": vout,
        "vShieldedSpend": [],
        "vShieldedOutput": [],
        "vjoinsplit": [],
        "orchard": {"actions": [], "valueBalance": 0.0, "valueBalanceZat": 0},
        "valueBalance": 0.0,
        "valueBalanceZat": 0,
        "size": tx.size(),
        "time": block.time(),
        "txid": tx.txid_display(),
        "authdigest": display(&tx.auth_digest()),
        "overwintered": true,
        "version": TX_VERSION,
        "versiongroupid": VERSION_GROUP_ID,
        "locktime": 0,
        "expiryheight": tx.expiry_height(),
        "blockhash": block.hash_display(),
        "blocktime": block.time(),
    });
    if let Some(bundle) = tx.tachyon() {
        out["tachyon"] = tachyon(bundle);
    }
    out
}

/// The `tx[].tachyon` object. Tachygrams, anchors and other non-hash values are
/// printed unreversed (spec section 3).
pub fn tachyon(bundle: &TachyonBundle) -> Value {
    let actions: Vec<Value> = bundle
        .actions
        .iter()
        .map(|action| {
            json!({
                "cv": hex::encode(action.cv),
                "rk": hex::encode(action.rk),
                "spendAuthSig": hex::encode(action.spend_auth_sig),
            })
        })
        .collect();
    let stamp = match &bundle.stamp {
        Stamp::Proof(proof) => json!({
            "type": "proof",
            "feeZat": proof.fee_zat,
            "coverage": hex::encode(proof.coverage),
            "anchor": hex::encode(proof.anchor),
            "tachygramSet": hex::encode(proof.tachygram_set),
            "tachygrams": proof.tachygrams.iter().map(hex::encode).collect::<Vec<_>>(),
            "proof": hex::encode(&proof.proof),
        }),
        Stamp::Pointer(pointer) => json!({
            "type": "pointer",
            "feeZat": pointer.fee_zat,
            "aggregateId": hex::encode(pointer.aggregate_id),
        }),
    };
    json!({
        "actions": actions,
        "valueBalance": zec(bundle.value_balance_zat),
        "valueBalanceZat": bundle.value_balance_zat,
        "cvFeeMax": hex::encode(bundle.cv_fee_max),
        "bindingSig": hex::encode(bundle.binding_sig),
        "memo": hex::encode(&bundle.memo),
        "stamp": stamp,
    })
}

/// The coinbase input script: the height push the node writes, then miner data.
fn coinbase_script(coinbase: &Coinbase) -> String {
    let height = coinbase.height.to_le_bytes();
    let len = height
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(1, |last| last + 1);
    let mut script = vec![len as u8];
    script.extend_from_slice(&height[..len]);
    script.extend_from_slice(&coinbase.extra);
    hex::encode(script)
}

fn coinbase_output(coinbase: &Coinbase) -> Value {
    let hash = hex::encode(coinbase.pubkey_hash);
    let mut payload = TESTNET_P2PKH_PREFIX.to_vec();
    payload.extend_from_slice(&coinbase.pubkey_hash);
    json!({
        "value": zec(coinbase.value_zat as i64),
        "valueZat": coinbase.value_zat,
        "n": 0,
        "scriptPubKey": {
            "asm": format!("OP_DUP OP_HASH160 {hash} OP_EQUALVERIFY OP_CHECKSIG"),
            "hex": format!("76a914{hash}88ac"),
            "reqSigs": 1,
            "type": "pubkeyhash",
            "addresses": [base58check(&payload)],
        },
    })
}
