//! Query host of the Tachyon payment PIR services (spec
//! `docs/spec/spec-tachyon-payment-pir.md`, sections 4.1, 10 and 11.5-11.8).
//!
//! One process serves every compiled product (Cargo features `memo`,
//! `first-contact`, `tachygram`, `registry`; only `tachygram` has a serving
//! module in milestone M1) in the `combined` role. Every product shares one
//! serving core:
//!
//! - [`config`]: the JSON configuration, validated offline (`--check`).
//! - [`sync`] and [`prepare`]: `tachyon_pir_distribution::follow` over
//!   `HttpsSource`, with a preparer that checks the map against the
//!   configuration and the inventory, validates every row, builds the table
//!   runtime on a dedicated pool and checks its public setup.
//! - [`serving`]: per-product state, admission and the routes `map`,
//!   `map/{base}/{sha256}`, `public/{session_id}` and `query`.
//! - [`host`]: discovery preflight, listeners, management, drain.
//!
//! A product module ([`product::ProductModule`], e.g. [`tachygram`]) is
//! constants only: its section name, metric names, endpoint labels, default
//! retention and map family.

pub mod config;
pub mod host;
pub mod map;
pub mod metrics;
pub mod prepare;
pub mod product;
pub mod response;
pub mod serving;
pub mod sync;
#[cfg(feature = "tachygram")]
pub mod tachygram;

pub use config::{Config, Validated};
pub use host::{Listeners, discover, run, serve};
pub use pir_pipeline::service::Shutdown;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
