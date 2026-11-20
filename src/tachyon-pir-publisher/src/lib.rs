//! The Tachyon PIR publisher host (spec sections 4.1, 11.7, 11.8).
//!
//! One process, one chain follower, one publisher definition per compiled and
//! enabled product, hosted by `pir_publisher::run_registered`. This library
//! holds the configuration schema, its static rules and the start-up sequence
//! so that tests and the binary share them:
//!
//! 1. [`Config::load`] parses the JSON file; unknown fields are refused at
//!    every level.
//! 2. [`Config::plan`] applies every static rule without touching the network
//!    (this is `--check`) and returns the [`Plan`]: the management listener,
//!    the root chain settings and the product definitions.
//! 3. [`run`] reads the node's network identity once and refuses to start
//!    without `NuTachyon` (spec 11.1), then hands the plan to
//!    `run_registered` until the [`Shutdown`] is requested.

pub mod config;
mod layout;
mod products;
mod startup;

pub use config::{Chain, ChainPlan, Config, Plan, Product, Section};
pub use pir_pipeline::service::Shutdown;
pub use startup::{check_network, run};

/// Any start-up or configuration failure, as `pir_publisher` reports them.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The configuration section names of the products compiled into this build.
pub const PRODUCTS: &[&str] = &[
    #[cfg(feature = "memo")]
    "memo",
    #[cfg(feature = "first-contact")]
    "first_contact",
    #[cfg(feature = "tachygram")]
    "tachygram",
    #[cfg(feature = "registry")]
    "registry",
];
