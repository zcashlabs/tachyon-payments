//! The network a node serves (spec section 11.1).
use crate::Hash;
use serde::Deserialize;
use std::collections::BTreeMap;

/// The `getblockchaininfo.upgrades` entry name of the Tachyon upgrade.
pub const TACHYON_UPGRADE_NAME: &str = "NuTachyon";

/// The network a node is on and where its Tachyon pool begins.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NetworkIdentity {
    /// `getblockchaininfo.chain`, for example `"test"`.
    pub chain: String,
    /// The hash of block 0, in protocol order.
    pub genesis_hash: Hash,
    /// The `activationheight` of the `NuTachyon` upgrade.
    pub activation_height: u32,
}

/// A `getblockchaininfo` result that names no usable Tachyon network.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error("getblockchaininfo has an unexpected shape: {0}")]
    Shape(#[source] serde_json::Error),
    #[error("the node lists no {TACHYON_UPGRADE_NAME} upgrade")]
    MissingTachyonUpgrade,
    #[error("the node lists {TACHYON_UPGRADE_NAME} under {0} branch ids")]
    AmbiguousTachyonUpgrade(usize),
    #[error("{TACHYON_UPGRADE_NAME} activation height {0} is beyond u32")]
    ActivationOutOfRange(u64),
}

#[derive(Deserialize)]
struct RawInfo {
    chain: String,
    #[serde(default)]
    upgrades: BTreeMap<String, RawUpgrade>,
}

#[derive(Deserialize)]
struct RawUpgrade {
    name: String,
    activationheight: u64,
}

impl NetworkIdentity {
    /// The identity of a node whose `getblockchaininfo` result is `info` and
    /// whose block 0 has hash `genesis_hash`.
    ///
    /// The activation height is the `activationheight` of the one `upgrades`
    /// entry named `NuTachyon`, whatever its branch id and status; no such entry
    /// is [`IdentityError::MissingTachyonUpgrade`], and the publisher refuses
    /// to start.
    pub fn from_blockchaininfo(
        info: &serde_json::Value,
        genesis_hash: Hash,
    ) -> Result<Self, IdentityError> {
        let info = RawInfo::deserialize(info).map_err(IdentityError::Shape)?;
        let mut tachyon = info
            .upgrades
            .values()
            .filter(|upgrade| upgrade.name == TACHYON_UPGRADE_NAME);
        let upgrade = tachyon.next().ok_or(IdentityError::MissingTachyonUpgrade)?;
        let others = tachyon.count();
        if others > 0 {
            return Err(IdentityError::AmbiguousTachyonUpgrade(others + 1));
        }
        let activation_height = u32::try_from(upgrade.activationheight)
            .map_err(|_| IdentityError::ActivationOutOfRange(upgrade.activationheight))?;
        Ok(Self {
            chain: info.chain,
            genesis_hash,
            activation_height,
        })
    }
}
