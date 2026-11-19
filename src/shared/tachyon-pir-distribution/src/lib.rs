//! The publisher-to-query-host distribution contract shared by every Tachyon
//! PIR product (spec 4.2, 11.3-11.6), over `wallet-pir`'s `pir-publication`,
//! `pir-sync-manager` and `pir-pipeline` used unchanged.
//!
//! - [`contract`]: product naming (prefix, schema, scope), the envelope
//!   [`Descriptor`], the [`Inventory`] a publication consists of and the
//!   authority [`Subject`].
//! - [`publisher`]: the publisher's [`Distribution`]: object store, durable
//!   authority and generation counter, publication, withdrawal, recovery,
//!   restore after restart, retention, and the `current`, `objects/:id` and
//!   `authority/check` routes.
//! - [`consumer`]: the query host's [`Follower`] adapter for
//!   `ConsumerEngine` over `HttpsSource`, and the [`Publications`] registry
//!   that classifies sessions as found, revoked (`410`), unknown (`409`) or
//!   unavailable (`503`).
//!
//! No chain, store or publisher-host dependency: chain facts reach this crate
//! only as the [`Subject`] a product supplies and checks.
pub mod consumer;
pub mod contract;
pub mod publisher;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub use consumer::{
    Engine, Follower, FollowerConfig, Preparer, Publication, Publications, ServingPolicy,
    ServingReport, SessionLookup, Table, VerifiedPublication, VerifiedTable, follow, lost_race,
};
pub use contract::{
    BlockRef, Current, Descriptor, Inventory, ObjectLimits, Product, ScopeBinding, SessionId,
    Subject, TableObjects,
};
pub use publisher::{
    Candidate, CommitRefusal, Committed, Contents, Distribution, DistributionOptions, Draft,
    PublisherHooks,
};
