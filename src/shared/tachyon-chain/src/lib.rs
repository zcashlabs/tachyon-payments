//! Tachyon testnet node reads for the payment-protocol PIR services
//! (spec sections 3, 4.2 and 11.1).
//!
//! - [`decode_block`] turns a `getblock <hash> 2` result into a [`TachyonBlock`],
//!   refusing a block that is not the one requested.
//! - [`EpochSchedule`] does the epoch arithmetic; [`target_height`] applies the
//!   confirmation depth.
//! - [`NodeClient`] reads the network identity, the tip, block hashes and blocks
//!   over `pir_node_rpc::RpcClient`.
//!
//! Block hashes and txids are [`Hash`]es in protocol order, printed and parsed in
//! display order. Tachygrams, anchors, tachygram sets and aggregate ids are byte
//! arrays decoded from the node's hex left to right.
mod block;
mod client;
mod decode;
mod epoch;
mod hash;
mod identity;

pub use block::{AGGREGATE_ID_BYTES, Stamp, TACHYGRAM_BYTES, TachyonBlock, TachyonTx};
pub use client::{NodeClient, NodeError, REQUEST_ID};
pub use decode::{
    DecodeError, DecodeOptions, Field, MAX_BUNDLE_ACTIONS, MAX_STAMP_TACHYGRAMS,
    MIN_STAMP_TACHYGRAMS, ZipRuleViolation, decode_block,
};
pub use epoch::{CONFIRMATION_DEPTH, EPOCH_LENGTH, EpochError, EpochSchedule, target_height};
pub use hash::{BlockId, Hash, HashParseError};
pub use identity::{IdentityError, NetworkIdentity, TACHYON_UPGRADE_NAME};
/// The transport, re-exported so callers can build a [`NodeClient`] without
/// naming `pir-node-rpc` themselves.
pub use pir_node_rpc::{Credentials, HttpPolicy, RpcClient, RpcError};
