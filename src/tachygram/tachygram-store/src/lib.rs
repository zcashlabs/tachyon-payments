//! The tachygram product's journal, `tachygram-v1.sqlite` (spec sections 8.5
//! and 8.6), following `wallet-pir`'s `receiver-store`.
//!
//! - [`IndexedBlock::new`] extracts a block's proof stamps and their tachygrams
//!   from a decoded [`tachyon_chain::TachyonBlock`] (spec 8.2).
//! - [`Store`] keeps a contiguous run of blocks, appended one at a time
//!   atomically and durably, rewound to any stored block, and assigns each proof
//!   stamp its `stamp_ordinal` within its epoch (spec 8.1). Uniqueness faults of
//!   spec 8.3 and all-zero tachygrams (spec 6.2) are refused as
//!   [`IntegrityFault`]s and nothing is written.
//! - The read API serves the publisher: an epoch's stamps, its 40-byte record
//!   fields in tachygram order, its tachygrams in stamp order (the OSS interface
//!   of spec 8.6) and a summary.
mod block;
mod error;
mod store;

pub use block::{IndexedBlock, ProofStamp, StampOrdinal};
pub use error::{Error, IntegrityFault};
pub use store::{
    Binding, EpochSummary, SCHEMA_VERSION, STORE_FILE, StampRecord, Store, TachygramRecord,
};
