//! The memo and first-contact products' journal, `memo-v1.sqlite` (spec
//! sections 5, 7.1 to 7.3), following `wallet-pir`'s `receiver-store`.
//!
//! - [`IndexedBlock::new`] parses each Tachyon transaction's memo as an
//!   envelope (spec 5): an empty memo is skipped, an invalid one is skipped
//!   whole and counted, and a valid one contributes its follow-up and
//!   first-contact entries with their ordinals and counts its wallet-state
//!   entries (spec 5.2).
//! - [`Store`] keeps a contiguous run of blocks from the activation height,
//!   appended one at a time atomically and durably and rewound to any stored
//!   block. It stores every entry, duplicate tags included (spec 5.3), and
//!   decides the first-contact span boundaries at epoch boundaries from the
//!   chain alone (spec 7.2).
//! - The read API serves the publisher: a memo epoch's entries, a
//!   first-contact span's entries, the spans, epoch summaries and the counts
//!   behind the memo metrics (spec 11.8).
mod block;
mod error;
mod store;

pub use block::{
    Counts, FirstContactRecord, IndexedBlock, InvalidEnvelope, Kind, MemoRecord, Ordinal,
};
pub use error::{EntryFault, Error};
pub use store::{Binding, EpochSummary, Record, SCHEMA_VERSION, STORE_FILE, Span, Store};
/// The span constants of spec 15, re-exported from the protocol crate.
pub use tachyon_pir_protocol::constants::{FC_SPAN_MAX_EPOCHS, FC_SPAN_TARGET};
