//! JSON chain specs: the input of the `tachyon-node-double` binary and the bodies
//! of its control endpoints.
//!
//! A spec is replayed through the same builder calls a test would make, so a test
//! process that builds [`ChainSpec::build`] itself (and applies the same control
//! edits) holds the same chain the binary serves, hashes included.
use crate::envelope::{AuthoredEnvelope, EnvelopeJson, EnvelopeMix, Malformation};
use crate::model::{
    BlockSpec, ChainConfig, DEFAULT_FEE_ZAT, FixtureError, SyntheticChain, TxMix, TxSpec,
};
use serde::{Deserialize, Serialize};

/// A whole chain: network, generator mix, then `random_blocks` generated blocks
/// after genesis followed by `explicit_blocks`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChainSpec {
    pub seed: u64,
    pub chain: Option<String>,
    /// Omitted: the testnet's 8. `null`: NuTachyon is not advertised.
    #[serde(default = "default_activation")]
    pub activation_height: Option<u64>,
    pub genesis_time: Option<u64>,
    pub block_interval: Option<u64>,
    pub mix: TxMix,
    /// Random envelopes in generated blocks (and in random blocks appended
    /// through the control endpoints); replaces the mix's random memos.
    pub envelopes: Option<EnvelopeMix>,
    pub random_blocks: u64,
    pub explicit_blocks: Vec<BlockJson>,
}

fn default_activation() -> Option<u64> {
    Some(crate::model::DEFAULT_ACTIVATION_HEIGHT)
}

impl Default for ChainSpec {
    fn default() -> Self {
        Self {
            seed: 0,
            chain: None,
            activation_height: default_activation(),
            genesis_time: None,
            block_interval: None,
            mix: TxMix::default(),
            envelopes: None,
            random_blocks: 0,
            explicit_blocks: Vec::new(),
        }
    }
}

impl ChainSpec {
    /// The chain configuration this spec names.
    pub fn config(&self) -> ChainConfig {
        let defaults = ChainConfig::default();
        ChainConfig {
            seed: self.seed,
            chain: self.chain.clone().unwrap_or(defaults.chain),
            activation_height: self.activation_height,
            genesis_time: self.genesis_time.unwrap_or(defaults.genesis_time),
            block_interval: self.block_interval.unwrap_or(defaults.block_interval),
            mix: self.mix.clone(),
            envelopes: self.envelopes.clone(),
        }
    }

    /// The chain this spec describes.
    pub fn build(&self) -> Result<SyntheticChain, FixtureError> {
        if let Some(mix) = &self.envelopes {
            mix.check()?;
        }
        let blocks = to_specs(&self.explicit_blocks)?;
        let mut chain = SyntheticChain::new(self.config());
        chain.append_random(self.random_blocks);
        for block in blocks {
            chain.append(block);
        }
        Ok(chain)
    }
}

/// Converts JSON blocks to builder specs.
pub fn to_specs(blocks: &[BlockJson]) -> Result<Vec<BlockSpec>, FixtureError> {
    blocks.iter().map(BlockJson::to_spec).collect()
}

/// A block in JSON: its Tachyon transactions (a coinbase is always added).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlockJson {
    pub txs: Vec<TxJson>,
    pub time: Option<u64>,
}

/// A Tachyon transaction in JSON. At most one of `memo_hex`, `envelope` and
/// `malformed` may be given.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TxJson {
    /// Own actions, with random filler values.
    pub actions: u32,
    /// The memo bytes as hex (raw, unclassified bytes in the ground truth).
    pub memo_hex: String,
    /// A valid envelope as the memo.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envelope: Option<EnvelopeJson>,
    /// An envelope malformed this way as the memo, drawn from the chain's
    /// stream (so an in-process build of the spec draws the same bytes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub malformed: Option<Malformation>,
    pub stamp: StampJson,
    pub fee_zat: u64,
    pub value_balance_zat: i64,
}

impl Default for TxJson {
    fn default() -> Self {
        Self {
            actions: 1,
            memo_hex: String::new(),
            envelope: None,
            malformed: None,
            stamp: StampJson::Pointer,
            fee_zat: DEFAULT_FEE_ZAT,
            value_balance_zat: 0,
        }
    }
}

/// A stamp in JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum StampJson {
    /// A pointer stamp with a random aggregate id.
    Pointer,
    /// A proof stamp with `count` fresh ascending tachygrams.
    RandomProof { count: u32 },
    /// A proof stamp with exactly these tachygrams (hex, unreversed), in order.
    Proof { tachygrams: Vec<String> },
}

impl BlockJson {
    pub fn to_spec(&self) -> Result<BlockSpec, FixtureError> {
        let txs = self
            .txs
            .iter()
            .map(TxJson::to_spec)
            .collect::<Result<_, _>>()?;
        Ok(BlockSpec {
            txs,
            time: self.time,
        })
    }
}

impl TxJson {
    /// A one-action pointer-stamp transaction carrying `envelope`.
    pub fn with_envelope(envelope: &AuthoredEnvelope) -> Self {
        Self {
            envelope: Some(envelope.into()),
            ..Self::default()
        }
    }

    /// A one-action pointer-stamp transaction carrying an envelope malformed as
    /// `malformation`.
    pub fn with_malformed(malformation: Malformation) -> Self {
        Self {
            malformed: Some(malformation),
            ..Self::default()
        }
    }

    pub fn to_spec(&self) -> Result<TxSpec, FixtureError> {
        let memo = hex::decode(&self.memo_hex)
            .map_err(|error| FixtureError::Spec(format!("memo_hex: {error}")))?;
        let given = usize::from(!memo.is_empty())
            + usize::from(self.envelope.is_some())
            + usize::from(self.malformed.is_some());
        if given > 1 {
            return Err(FixtureError::Spec(
                "give at most one of memo_hex, envelope and malformed".to_owned(),
            ));
        }
        let mut tx = TxSpec::tachyon(self.actions)
            .memo(memo)
            .fee(self.fee_zat)
            .value_balance(self.value_balance_zat);
        if let Some(envelope) = &self.envelope {
            tx = tx.envelope(&envelope.to_envelope()?);
        }
        if let Some(malformation) = self.malformed {
            tx = tx.malformed_memo(malformation);
        }
        Ok(match &self.stamp {
            StampJson::Pointer => tx.pointer(),
            StampJson::RandomProof { count } => tx.random_proof(*count),
            StampJson::Proof { tachygrams } => tx.proof(
                tachygrams
                    .iter()
                    .map(|hex| {
                        hex::decode(hex)
                            .ok()
                            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                            .ok_or_else(|| {
                                FixtureError::Spec(format!("tachygram {hex:?} is not 32 hex bytes"))
                            })
                    })
                    .collect::<Result<_, _>>()?,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_is_defaulted_omitted_and_nullable() {
        let omitted: ChainSpec = serde_json::from_str("{}").unwrap();
        assert_eq!(omitted.activation_height, Some(8));
        let null: ChainSpec = serde_json::from_str(r#"{"activation_height": null}"#).unwrap();
        assert_eq!(null.activation_height, None);
        let set: ChainSpec = serde_json::from_str(r#"{"activation_height": 3}"#).unwrap();
        assert_eq!(set.activation_height, Some(3));
        assert!(serde_json::from_str::<ChainSpec>(r#"{"bogus": 1}"#).is_err());
    }

    #[test]
    fn explicit_blocks_follow_random_ones() {
        let spec: ChainSpec = serde_json::from_value(serde_json::json!({
            "seed": 9,
            "random_blocks": 10,
            "explicit_blocks": [{"txs": [
                {"actions": 2, "memo_hex": "abcd", "stamp": {"type": "proof", "tachygrams": ["11".repeat(32)]}},
                {"stamp": {"type": "random_proof", "count": 3}},
            ]}],
        }))
        .unwrap();
        let chain = spec.build().unwrap();
        assert_eq!(chain.tip_height(), 11);
        let truth: Vec<_> = chain.tip().tachyon_txs().collect();
        assert_eq!(truth.len(), 2);
        assert_eq!(truth[0].own_actions, 2);
        assert_eq!(truth[0].memo, &[0xab, 0xcd]);
        assert_eq!(truth[0].stamp.tachygrams(), &[[0x11; 32]]);
        assert_eq!(truth[1].stamp.tachygrams().len(), 3);
        assert_eq!(spec.build().unwrap().tip_hash(), chain.tip_hash());
    }
}
