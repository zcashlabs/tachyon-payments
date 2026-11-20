//! Diagnostic probe client for the Tachyon payment PIR services (spec
//! `docs/spec/spec-tachyon-payment-pir.md`, sections 2, 4.2, 10, 12.1, 14 M1).
//!
//! A testing and diagnostics tool, and the "wallet" of the end-to-end tests.
//! It speaks the wallet-facing protocol of all four products over one mount
//! (`/<product>/v1`):
//!
//! - [`Client::fetch_map`]: `GET map`, `X-Map-Sha256` check, head validation,
//!   every sealed chunk fetched (cached by digest) and checked, whole-listing
//!   check; returns a [`MapView`].
//! - [`Client::lookup`]: floor refusal, the query plan of spec 6.3 and 6.4,
//!   one private query per segment of the key's bucket, response framing
//!   checks, row validation (spec 8.4), results concatenated in segment
//!   (ordinal) order. A 409 refetches the map and retries once.
//! - [`Client::lookup_many`]: many keys of one scope, each answered as
//!   [`Client::lookup`] answers it, with each distinct row of their query
//!   plans queried once (a bounded number at a time); a lookup is a batch of
//!   one.
//! - [`Client::dummy_query`]: the same requests for a random bucket and rows
//!   (spec 12.3).
//! - [`Client::accept_anchor`]: the map's genesis and anchor hashes against a
//!   node's `getblockhash` (spec 12.1).

pub mod client;
pub mod error;
pub mod map_view;
pub mod report;

pub use client::{
    AnchorAccepted, Answer, Client, LookupOutcome, Mount, QueriedTable, Stats, parse_key,
    query_body,
};
pub use error::{ClientError, Route, parse_retry_after, status_error};
pub use map_view::{
    Coverage, MapChunkScopes, MapHead, MapView, ScopeSelector, ViewScope, parse_floor_header,
};
