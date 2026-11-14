//! Test fixtures for the Tachyon PIR services (spec sections 4.2 and 13.1).
//!
//! - [`SyntheticChain`]: a deterministic synthetic Tachyon chain built from a
//!   seed and builder calls, with reorgs. Its accessors are the ground truth
//!   end-to-end tests check the services against; they return content as
//!   authored and never parse the served JSON.
//! - [`NodeDouble`]: a JSON-RPC server answering `getblockchaininfo`,
//!   `getblockcount`, `getblockhash` and `getblock` in the shape of the alpha
//!   testnet's Zakura node, with fault injection, request counting and
//!   control endpoints.
//! - The `tachyon-node-double` binary serves a [`ChainSpec`] file as a separate
//!   process.
//!
//! This crate deliberately does not depend on `tachyon-chain`: it is the oracle
//! the decoder is tested against.
pub mod hash;
pub mod json;
pub mod model;
mod rng;
pub mod server;
pub mod spec;

pub use hash::{Hash32, display, parse_display};
pub use model::{
    Action, ActionsSpec, Block, BlockSpec, ChainConfig, Coinbase, FixtureError, Found,
    PointerStamp, ProofStamp, Stamp, StampSpec, SyntheticChain, TachyonBundle, TachyonTxTruth, Tx,
    TxBody, TxMix, TxSpec,
};
pub use server::{
    BasicAuth, BlockMismatch, Failure, NodeDouble, NodeOptions, RecordedCall, StaleBlocks,
};
pub use spec::{BlockJson, ChainSpec, StampJson, TxJson};
