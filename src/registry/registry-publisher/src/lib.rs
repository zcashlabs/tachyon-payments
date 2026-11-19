//! The registry product's publisher (spec sections 9, 10.3 and 11,
//! `docs/spec/spec-tachyon-payment-pir.md`).
//!
//! - [`configure`] declares the product for `pir_publisher::run_registered`:
//!   one private listener serving `/provider/registry/v1/`, the write API at
//!   `/registry/v1/` and `/v1/registry/`, and `/metrics`; one state
//!   directory; a readiness probe on `current`. It follows no chain.
//! - The write API (spec 9.3) validates a registration in the spec's order
//!   ([`challenge`], [`validate`]) under the abuse controls ([`limits`],
//!   `pir_control::admission::ClientSlots`) and appends it durably to
//!   `registry-store` before answering.
//! - [`PublicationTask`] is the `pir_pipeline` `Publisher` loop (spec 9.2
//!   Cadence): one publication whenever the log grew, coalesced, at most once
//!   per `publish_ms`; sealed chunks built once, the open chunk rebuilt when
//!   it changed; map chunks and head, the inventory, and the commit through
//!   `tachyon-pir-distribution`.
//! - [`Registry`] opens all of it, for the managed [`run`] and for tests.
pub mod challenge;
mod config;
mod core;
mod error;
mod hooks;
pub mod limits;
mod metrics;
mod publication;
mod service;
pub mod validate;
mod write;

pub use config::{
    DEFAULT_GLOBAL_PER_MINUTE, DEFAULT_PER_CLIENT_CONCURRENT, DEFAULT_PER_CLIENT_PER_MINUTE,
    DEFAULT_PUBLISH_MS, DEFAULT_UPLOAD_MS, RegistryConfig, RegistryIdentity, RegistrySettings,
};
pub use error::{Error, Result};
pub use metrics::{ENDPOINTS, Metrics, Outcome, PublicationFigures, RESULTS};
pub use publication::{PublicationTask, Report};
pub use service::{READINESS_PATH, Registry, configure, distribution_ready, run};
pub use write::MOUNT;
