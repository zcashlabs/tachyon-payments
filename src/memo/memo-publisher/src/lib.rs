//! The memo and first-contact products' publisher (spec sections 5, 7, 10.3
//! and 11, `docs/spec/spec-tachyon-payment-pir.md`).
//!
//! Memo and first-contact run as **one** publisher definition (spec 11.7):
//! one listener, one state directory, one journal (`memo-v1.sqlite`) and one
//! chain follower feeding two independent publications.
//!
//! - [`configure`] declares the definition for
//!   `pir_publisher::run_registered`: one private listener serving
//!   `/provider/memo/v1/`, `/provider/first-contact/v1/` when first-contact is
//!   enabled, and `/metrics`; one state directory; a readiness probe on each
//!   enabled prefix's `current`.
//! - [`Writer`] is the journal writer the root chain follower drives through
//!   `pir_pipeline::ingestion::Ingestion` (spec 11.2): it appends
//!   `memo-store` blocks fetched through `tachyon-chain` (invalid envelopes
//!   skipped and counted, spec 5.2), withdraws or recovers each product
//!   against its own last publication before every rewind (spec 11.4), and
//!   offers each caught-up journal state to both products.
//! - [`PublicationTask`] publishes the latest offered state of one product
//!   through `PublisherExecutor` (spec 11.3): memo scopes are epochs,
//!   first-contact scopes are spans; sealed scopes built once, open scopes
//!   recomputed, map chunks and head, the inventory, and the commit through
//!   `tachyon-pir-distribution`.
//! - [`MemoPublisher`] opens all of it, for the managed [`run`] and for tests.
mod config;
mod core;
mod error;
mod hooks;
mod metrics;
mod publication;
mod service;
mod writer;

pub use config::{FirstContactSettings, MemoConfig, MemoSettings};
pub use error::{Error, Result};
pub use metrics::{Metrics, PublicationFigures};
pub use publication::{PublicationTask, Report};
pub use service::{
    DEFINITION_NAME, FIRST_CONTACT_READINESS_PATH, MEMO_READINESS_PATH, MemoPublisher, configure,
    distribution_ready, run,
};
pub use writer::Writer;
