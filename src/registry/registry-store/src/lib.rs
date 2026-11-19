//! The registry product's registration log, `registry-v1.sqlite` (spec
//! sections 9.2 to 9.4), following `tachygram-store` and `memo-store`.
//!
//! - [`Store`] keeps an append-only log of addresses, each at a gap-free
//!   `sequence` counting from 0, bound to one network identity and to
//!   [`REGISTRY_CHUNK_ENTRIES`]. A chunk is `sequence / REGISTRY_CHUNK_ENTRIES`
//!   (spec 9.2, 9.4).
//! - [`Store::register`] is the write path behind `POST /registry/v1/register`
//!   (spec 9.3): idempotent, serialised across threads, and durable (WAL,
//!   `synchronous=FULL`) before it returns.
//! - The read API serves the publisher: the log sequence of the registry map
//!   (spec 10.3), a chunk's entries in sequence order, and lookup by digest.
//!
//! Entries are never deleted or changed; there is no rewind. The store checks
//! only what its own integrity needs (exact lengths, digest consistency, a
//! nonzero `key16`); the `pk` canonical and `ek` modulus checks of spec 9.1
//! belong to the publisher's request validation.
mod error;
mod store;

pub use error::Error;
pub use store::{Binding, Registration, SCHEMA_VERSION, STORE_FILE, Store, chunk_of, chunk_start};
/// Entries per chunk (spec 15), re-exported from the protocol crate.
pub use tachyon_pir_protocol::constants::REGISTRY_CHUNK_ENTRIES;
