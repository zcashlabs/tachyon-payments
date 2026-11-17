//! A pure decoder from the node's `getblock <hash> 2` result to [`TachyonBlock`].
//!
//! The decoder reads the JSON form (spec section 11.1). It refuses a block whose
//! hash, height or transaction placement differs from what was requested, any
//! malformed or duplicated txid, and any hex field of the wrong length. Counts are
//! read from the data; the draft-ZIP limits are checked only when
//! [`DecodeOptions::strict_zip_rules`] is set (spec section 3).
use crate::block::{AGGREGATE_ID_BYTES, Stamp, TACHYGRAM_BYTES, TachyonBlock, TachyonTx};
use crate::hash::HashParseError;
use crate::{BlockId, Hash};
use serde::Deserialize;
use serde::de::IgnoredAny;
use std::collections::HashMap;
use std::fmt;

/// Most actions a bundle may have under the draft ZIPs.
pub const MAX_BUNDLE_ACTIONS: u32 = 4095;
/// Fewest tachygrams a proof stamp may have under the draft ZIPs.
pub const MIN_STAMP_TACHYGRAMS: usize = 2;
/// Most tachygrams a proof stamp may have under the draft ZIPs.
pub const MAX_STAMP_TACHYGRAMS: usize = 8190;

/// How strictly blocks are decoded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DecodeOptions {
    /// Enforce the draft-ZIP limits of spec section 3 as integrity checks:
    /// 0..=4,095 actions per bundle, 2..=8,190 tachygrams per proof stamp in
    /// strictly ascending byte order, and no tachygram twice in a block. Off
    /// for the alpha testnet, which does not follow them yet.
    pub strict_zip_rules: bool,
}

/// Where in a block a refused value sits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Field {
    /// The transaction's index in the block, or `None` for a block field.
    pub tx_index: Option<u32>,
    /// The JSON path within the block or transaction.
    pub name: &'static str,
    /// The element of a list field.
    pub item: Option<usize>,
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(tx_index) = self.tx_index {
            write!(f, "tx[{tx_index}].")?;
        }
        f.write_str(self.name)?;
        if let Some(item) = self.item {
            write!(f, "[{item}]")?;
        }
        Ok(())
    }
}

/// A draft-ZIP limit a block breaks, checked only under `strict_zip_rules`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ZipRuleViolation {
    #[error("bundle has {0} actions, more than {MAX_BUNDLE_ACTIONS}")]
    TooManyActions(u32),
    #[error(
        "proof stamp has {0} tachygrams, outside {MIN_STAMP_TACHYGRAMS}..={MAX_STAMP_TACHYGRAMS}"
    )]
    TachygramCount(usize),
    #[error("tachygram {0} is not strictly above its predecessor")]
    TachygramOrder(usize),
    #[error("tachygram {item} repeats one of transaction {first_tx_index}")]
    DuplicateTachygram { item: usize, first_tx_index: u32 },
}

/// A `getblock` result this crate refuses.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("block JSON has an unexpected shape: {0}")]
    Shape(#[source] serde_json::Error),
    #[error("{field} is not hex: {source}")]
    Hex {
        field: Field,
        #[source]
        source: hex::FromHexError,
    },
    #[error("{field} is {actual} bytes, expected {expected}")]
    Length {
        field: Field,
        expected: usize,
        actual: usize,
    },
    #[error("{field} is {value}, beyond u32")]
    OutOfRange { field: Field, value: u64 },
    #[error("node returned block {returned} for requested block {requested}")]
    HashMismatch { requested: Hash, returned: Hash },
    #[error("node returned height {returned} for a block requested at height {requested}")]
    HeightMismatch { requested: u32, returned: u32 },
    #[error("block at height {0} has no previousblockhash")]
    MissingPrevious(u32),
    #[error("block {hash} is not on the node's active chain (confirmations {confirmations})")]
    Stale { hash: Hash, confirmations: i64 },
    #[error("block declares {declared} transactions and lists {listed}")]
    TxCount { declared: u64, listed: usize },
    #[error("transaction {tx_index} ({txid}) names block {named}, not {block}")]
    TxBlockHash {
        tx_index: u32,
        txid: Hash,
        block: Hash,
        named: Hash,
    },
    #[error("transaction {tx_index} ({txid}) names height {named}, not {block}")]
    TxHeight {
        tx_index: u32,
        txid: Hash,
        block: u32,
        named: u64,
    },
    #[error("transaction {txid} appears at indices {first_index} and {second_index}")]
    DuplicateTxid {
        txid: Hash,
        first_index: u32,
        second_index: u32,
    },
    #[error("transaction {tx_index} has unknown stamp type {kind:?}")]
    UnknownStampType { tx_index: u32, kind: String },
    #[error("{0} is missing")]
    MissingField(Field),
    #[error("{field} does not belong in a {kind} stamp")]
    ForeignStampField { field: Field, kind: &'static str },
    #[error("transaction {tx_index} ({txid}) breaks a draft-ZIP rule: {violation}")]
    ZipRule {
        tx_index: u32,
        txid: Hash,
        violation: ZipRuleViolation,
    },
}

#[derive(Deserialize)]
struct RawBlock {
    hash: String,
    height: u64,
    previousblockhash: Option<String>,
    #[serde(rename = "nTx")]
    n_tx: Option<u64>,
    confirmations: Option<i64>,
    tx: Vec<RawTx>,
}

#[derive(Deserialize)]
struct RawTx {
    txid: String,
    blockhash: Option<String>,
    height: Option<u64>,
    tachyon: Option<RawBundle>,
}

#[derive(Deserialize)]
struct RawBundle {
    actions: Vec<IgnoredAny>,
    memo: String,
    stamp: RawStamp,
}

#[derive(Deserialize)]
struct RawStamp {
    #[serde(rename = "type")]
    kind: String,
    anchor: Option<String>,
    #[serde(rename = "tachygramSet")]
    tachygram_set: Option<String>,
    tachygrams: Option<Vec<String>>,
    #[serde(rename = "aggregateId")]
    aggregate_id: Option<String>,
}

fn block_field(name: &'static str) -> Field {
    Field {
        tx_index: None,
        name,
        item: None,
    }
}

fn tx_field(tx_index: u32, name: &'static str) -> Field {
    Field {
        tx_index: Some(tx_index),
        name,
        item: None,
    }
}

fn parse_hash(field: Field, displayed: &str) -> Result<Hash, DecodeError> {
    Hash::parse_display(displayed).map_err(|error| match error {
        HashParseError::Hex(source) => DecodeError::Hex { field, source },
        HashParseError::Length(actual) => DecodeError::Length {
            field,
            expected: 32,
            actual,
        },
    })
}

fn parse_hex(field: Field, text: &str) -> Result<Vec<u8>, DecodeError> {
    hex::decode(text).map_err(|source| DecodeError::Hex { field, source })
}

fn parse_fixed<const N: usize>(field: Field, text: &str) -> Result<[u8; N], DecodeError> {
    parse_hex(field, text)?
        .try_into()
        .map_err(|bytes: Vec<u8>| DecodeError::Length {
            field,
            expected: N,
            actual: bytes.len(),
        })
}

fn to_u32(field: Field, value: u64) -> Result<u32, DecodeError> {
    u32::try_from(value).map_err(|_| DecodeError::OutOfRange { field, value })
}

fn to_index(field: Field, index: usize) -> Result<u32, DecodeError> {
    to_u32(field, index as u64)
}

/// Decodes `result`, the node's `getblock <requested_hash> 2` result, into the
/// block's Tachyon content.
///
/// The block's own hash must be `requested_hash` and, when `requested_height`
/// is given, its height must be that height. A block whose `confirmations` is
/// negative (reorged away, still served by hash) is [`DecodeError::Stale`]; a
/// missing `confirmations` is accepted. Every transaction's `blockhash`
/// and `height`, where the node prints them, must name this block.
pub fn decode_block(
    result: &serde_json::Value,
    requested_hash: &Hash,
    requested_height: Option<u32>,
    options: DecodeOptions,
) -> Result<TachyonBlock, DecodeError> {
    let raw = RawBlock::deserialize(result).map_err(DecodeError::Shape)?;
    let hash = parse_hash(block_field("hash"), &raw.hash)?;
    if hash != *requested_hash {
        return Err(DecodeError::HashMismatch {
            requested: *requested_hash,
            returned: hash,
        });
    }
    // A block reorged away may still be served by hash, with negative
    // confirmations (spec section 3); it is not canonical.
    if let Some(confirmations) = raw.confirmations
        && confirmations < 0
    {
        return Err(DecodeError::Stale {
            hash,
            confirmations,
        });
    }
    let height = to_u32(block_field("height"), raw.height)?;
    if let Some(requested) = requested_height
        && requested != height
    {
        return Err(DecodeError::HeightMismatch {
            requested,
            returned: height,
        });
    }
    let prev = match (&raw.previousblockhash, height) {
        (Some(prev), _) => parse_hash(block_field("previousblockhash"), prev)?,
        (None, 0) => Hash::ZERO,
        (None, height) => return Err(DecodeError::MissingPrevious(height)),
    };
    if let Some(declared) = raw.n_tx
        && declared != raw.tx.len() as u64
    {
        return Err(DecodeError::TxCount {
            declared,
            listed: raw.tx.len(),
        });
    }

    let mut first_index: HashMap<Hash, u32> = HashMap::with_capacity(raw.tx.len());
    let mut seen_tachygrams: HashMap<[u8; 32], u32> = HashMap::new();
    let mut txs = Vec::new();
    for (index, tx) in raw.tx.iter().enumerate() {
        let tx_index = to_index(block_field("tx"), index)?;
        let txid = parse_hash(tx_field(tx_index, "txid"), &tx.txid)?;
        if let Some(first_index) = first_index.insert(txid, tx_index) {
            return Err(DecodeError::DuplicateTxid {
                txid,
                first_index,
                second_index: tx_index,
            });
        }
        if let Some(named) = &tx.blockhash {
            let named = parse_hash(tx_field(tx_index, "blockhash"), named)?;
            if named != hash {
                return Err(DecodeError::TxBlockHash {
                    tx_index,
                    txid,
                    block: hash,
                    named,
                });
            }
        }
        if let Some(named) = tx.height
            && named != u64::from(height)
        {
            return Err(DecodeError::TxHeight {
                tx_index,
                txid,
                block: height,
                named,
            });
        }
        let Some(bundle) = &tx.tachyon else {
            continue;
        };
        let decoded = decode_bundle(tx_index, txid, bundle)?;
        if options.strict_zip_rules {
            check_zip_rules(&decoded, &mut seen_tachygrams)?;
        }
        txs.push(decoded);
    }
    Ok(TachyonBlock {
        id: BlockId { height, hash },
        prev,
        txs,
    })
}

fn decode_bundle(tx_index: u32, txid: Hash, bundle: &RawBundle) -> Result<TachyonTx, DecodeError> {
    let field = |name| tx_field(tx_index, name);
    let own_actions = to_index(field("tachyon.actions"), bundle.actions.len())?;
    let memo = parse_hex(field("tachyon.memo"), &bundle.memo)?;
    let raw = &bundle.stamp;
    let stamp = match raw.kind.as_str() {
        "proof" => {
            if raw.aggregate_id.is_some() {
                return Err(DecodeError::ForeignStampField {
                    field: field("tachyon.stamp.aggregateId"),
                    kind: "proof",
                });
            }
            let required = |value: &Option<String>, name| {
                value
                    .as_deref()
                    .ok_or(DecodeError::MissingField(field(name)))
                    .and_then(|text| parse_fixed::<TACHYGRAM_BYTES>(field(name), text))
            };
            let anchor = required(&raw.anchor, "tachyon.stamp.anchor")?;
            let tachygram_set = required(&raw.tachygram_set, "tachyon.stamp.tachygramSet")?;
            let listed = raw
                .tachygrams
                .as_ref()
                .ok_or(DecodeError::MissingField(field("tachyon.stamp.tachygrams")))?;
            let tachygrams = listed
                .iter()
                .enumerate()
                .map(|(item, text)| {
                    parse_fixed::<TACHYGRAM_BYTES>(
                        Field {
                            item: Some(item),
                            ..field("tachyon.stamp.tachygrams")
                        },
                        text,
                    )
                })
                .collect::<Result<_, _>>()?;
            Stamp::Proof {
                anchor,
                tachygram_set,
                tachygrams,
            }
        }
        "pointer" => {
            for (present, name) in [
                (raw.anchor.is_some(), "tachyon.stamp.anchor"),
                (raw.tachygram_set.is_some(), "tachyon.stamp.tachygramSet"),
                (raw.tachygrams.is_some(), "tachyon.stamp.tachygrams"),
            ] {
                if present {
                    return Err(DecodeError::ForeignStampField {
                        field: field(name),
                        kind: "pointer",
                    });
                }
            }
            let name = "tachyon.stamp.aggregateId";
            let text = raw
                .aggregate_id
                .as_deref()
                .ok_or(DecodeError::MissingField(field(name)))?;
            Stamp::Pointer {
                aggregate_id: parse_fixed::<AGGREGATE_ID_BYTES>(field(name), text)?,
            }
        }
        other => {
            return Err(DecodeError::UnknownStampType {
                tx_index,
                kind: other.to_owned(),
            });
        }
    };
    Ok(TachyonTx {
        tx_index,
        txid,
        memo,
        own_actions,
        stamp,
    })
}

/// The draft-ZIP limits of spec section 3 that one block can show.
fn check_zip_rules(tx: &TachyonTx, seen: &mut HashMap<[u8; 32], u32>) -> Result<(), DecodeError> {
    let violation = |violation| DecodeError::ZipRule {
        tx_index: tx.tx_index,
        txid: tx.txid,
        violation,
    };
    if tx.own_actions > MAX_BUNDLE_ACTIONS {
        return Err(violation(ZipRuleViolation::TooManyActions(tx.own_actions)));
    }
    let Stamp::Proof { tachygrams, .. } = &tx.stamp else {
        return Ok(());
    };
    if !(MIN_STAMP_TACHYGRAMS..=MAX_STAMP_TACHYGRAMS).contains(&tachygrams.len()) {
        return Err(violation(ZipRuleViolation::TachygramCount(
            tachygrams.len(),
        )));
    }
    if let Some(item) = tachygrams.windows(2).position(|pair| pair[0] >= pair[1]) {
        return Err(violation(ZipRuleViolation::TachygramOrder(item + 1)));
    }
    // Strict ascent rules out repeats within the stamp, so every repeat found
    // here is of an earlier stamp in the block.
    for (item, tachygram) in tachygrams.iter().enumerate() {
        if let Some(&first_tx_index) = seen.get(tachygram) {
            return Err(violation(ZipRuleViolation::DuplicateTachygram {
                item,
                first_tx_index,
            }));
        }
        seen.insert(*tachygram, tx.tx_index);
    }
    Ok(())
}
