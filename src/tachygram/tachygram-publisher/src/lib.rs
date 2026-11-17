//! The tachygram product's publisher (spec sections 8, 10.3 and 11,
//! `docs/spec/spec-tachyon-payment-pir.md`).
//!
//! - [`configure`] declares the product for `pir_publisher::run_registered`:
//!   one private listener serving `/provider/tachygram/v1/` and `/metrics`,
//!   one state directory, a readiness probe on `current`.
//! - [`Writer`] is the journal writer the root chain follower drives through
//!   `pir_pipeline::ingestion::Ingestion` (spec 11.2): it appends
//!   `tachygram-store` blocks fetched through `tachyon-chain`, withdraws or
//!   recovers before every rewind (spec 11.4), stops on integrity faults
//!   (spec 8.3) and offers each caught-up journal state for publication.
//! - [`PublicationTask`] publishes the latest offered state through
//!   `PublisherExecutor` (spec 11.3): one scope per epoch, sealed scopes built
//!   once, open scopes recomputed, map chunks and head, the inventory, and the
//!   commit through `tachyon-pir-distribution`.
//! - [`Tachygram`] opens all of it, for the managed [`run`] and for tests.
mod config;
mod core;
mod error;
mod hooks;
mod metrics;
mod publication;
mod service;
mod writer;

pub use config::{TachygramConfig, TachygramSettings};
pub use error::{Error, Result};
pub use metrics::{Metrics, PublicationFigures};
pub use publication::{PublicationTask, Report};
pub use service::{READINESS_PATH, Tachygram, configure, distribution_ready, run};
pub use writer::Writer;
