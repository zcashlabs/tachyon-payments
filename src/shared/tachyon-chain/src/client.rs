//! Node reads over `pir_node_rpc::RpcClient` (spec section 11.1).
//!
//! Only `getblockchaininfo`, `getblockcount`, `getblockhash` and `getblock` at
//! verbosity 2 are called. Blocks are fetched by hash, so a reorg cannot change
//! the block under a read, and every block is checked against the requested
//! hash (and height, when known) by [`decode_block`].
use crate::decode::{DecodeError, DecodeOptions, decode_block};
use crate::hash::HashParseError;
use crate::identity::{IdentityError, NetworkIdentity};
use crate::{BlockId, Hash, TachyonBlock};
use pir_node_rpc::{Credentials, HttpPolicy, RpcClient, RpcError};
use serde_json::json;

/// The JSON-RPC request id this crate's clients send, as the node logs it.
pub const REQUEST_ID: &str = "tachyon-chain";

/// A failed node read.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error("invalid block: {0}")]
    Decode(#[from] DecodeError),
    #[error("invalid network identity: {0}")]
    Identity(#[from] IdentityError),
    #[error("{method} returned an invalid hash: {source}")]
    Hash {
        method: &'static str,
        #[source]
        source: HashParseError,
    },
    #[error("getblockcount returned {0}, beyond u32")]
    TipOutOfRange(u64),
}

/// A Tachyon view of one node.
#[derive(Clone)]
pub struct NodeClient {
    rpc: RpcClient,
    options: DecodeOptions,
}

impl NodeClient {
    /// A client over `rpc` that decodes blocks with `options`.
    pub fn new(rpc: RpcClient, options: DecodeOptions) -> Self {
        Self { rpc, options }
    }

    /// A client for the node at `url`, authenticating with `credentials`, under
    /// [`HttpPolicy::SHARED`] (120 s per call, 64 MiB per response).
    pub fn connect(
        url: impl Into<String>,
        credentials: Credentials,
        options: DecodeOptions,
    ) -> Result<Self, NodeError> {
        let rpc = RpcClient::new(url.into(), credentials, REQUEST_ID, HttpPolicy::SHARED)?;
        Ok(Self::new(rpc, options))
    }

    /// The transport this client reads through.
    pub fn rpc(&self) -> &RpcClient {
        &self.rpc
    }

    /// How this client decodes blocks.
    pub fn options(&self) -> DecodeOptions {
        self.options
    }

    /// The node's network: `chain` from `getblockchaininfo`, the genesis hash
    /// from `getblockhash 0`, and the `NuTachyon` activation height.
    pub async fn network_identity(&self) -> Result<NetworkIdentity, NodeError> {
        let info: serde_json::Value = self.rpc.call("getblockchaininfo", json!([])).await?;
        let genesis = self.block_hash(0).await?;
        Ok(NetworkIdentity::from_blockchaininfo(&info, genesis)?)
    }

    /// The node's tip: `getblockcount`, then `getblockhash` at that height.
    pub async fn tip(&self) -> Result<BlockId, NodeError> {
        let height = self.rpc.tip_height().await?;
        let height = u32::try_from(height).map_err(|_| NodeError::TipOutOfRange(height))?;
        Ok(BlockId {
            height,
            hash: self.block_hash(height).await?,
        })
    }

    /// The hash of the node's block at `height`.
    pub async fn block_hash(&self, height: u32) -> Result<Hash, NodeError> {
        let displayed = self.rpc.block_hash_display(u64::from(height)).await?;
        Hash::parse_display(&displayed).map_err(|source| NodeError::Hash {
            method: "getblockhash",
            source,
        })
    }

    /// The block with hash `hash`, refused unless the node returns that block.
    pub async fn block(&self, hash: &Hash) -> Result<TachyonBlock, NodeError> {
        self.fetch(hash, None).await
    }

    /// The block `id`, refused unless the node returns that hash at that height.
    pub async fn block_at(&self, id: &BlockId) -> Result<TachyonBlock, NodeError> {
        self.fetch(&id.hash, Some(id.height)).await
    }

    async fn fetch(&self, hash: &Hash, height: Option<u32>) -> Result<TachyonBlock, NodeError> {
        let result: serde_json::Value = self
            .rpc
            .call("getblock", json!([hash.display_hex(), 2]))
            .await?;
        Ok(decode_block(&result, hash, height, self.options)?)
    }
}
