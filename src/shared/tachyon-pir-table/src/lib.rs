//! The shared bucket-row table of the Tachyon payment PIR services (spec
//! `docs/spec/spec-tachyon-payment-pir.md`, sections 6, 10.3 and 10.4):
//! building a scope's tables with seed search and overflow segments,
//! validating them, digests, and the P8 table runtime that answers queries.
//!
//! A serving library (`tools/ci/boundary.json`): no chain or ingestion
//! dependencies. Placement, row encoding and row validation come from
//! `tachyon-pir-protocol`; this crate decides where entries go and prepares
//! tables for PIR.
//!
//! - [`build_scope`]: a scope's entries, in ordinal order, to its
//!   [`ScopeTables`] at `tachyon-4k8`.
//! - [`validate_table`], [`validate_scope`], [`validate_published`]: every
//!   row through `validate_row`, plus the table rules.
//! - [`TableRuntime`]: build, public setup, answer.
//! - [`prepare`], [`describe`], [`prepare_published`]: a runtime with its map
//!   `Table` entry and session id, for the query host and the publisher.

mod build;
mod digest;
mod error;
mod prepare;
mod runtime;
mod validate;

pub use build::{BuiltTable, ScopeTables, build_scope};
pub use digest::{public_sha256, rows_sha256};
pub use error::Error;
pub use prepare::{
    DescribedTable, PreparedTable, describe, describe_in, prepare, prepare_in, prepare_published,
    prepare_published_in,
};
pub use runtime::TableRuntime;
pub use validate::{validate_published, validate_scope, validate_table};
