//! The publisher configuration (spec 11.7) and its static rules.
//!
//! ```json
//! {
//!   "management_listen": "127.0.0.1:9400",
//!   "chain": { "rpc_url": "http://127.0.0.1:18232", "no_auth": true, "poll_ms": 1000,
//!              "epoch_length": 4096 },
//!   "tachygram": { "enabled": true, "settings": { "listen": "127.0.0.1:9402",
//!                  "state_dir": "/var/lib/tachyon-pir/tachygram" } }
//! }
//! ```
//!
//! Static checks open no store, socket or credential and never contact the
//! node. Existing prefixes of state directories are resolved so that aliases
//! between products are detected.
use crate::{Result, layout, products};
use pir_pipeline::{ingestion::ChainSettings, publisher::PublisherDefinition};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};

/// The whole configuration file. Every product section may be omitted; a
/// section for a product this build does not include is refused.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Plain-HTTP `/health`, `/ready` and `/.well-known/pir-service-discovery`;
    /// a loopback address with a fixed port.
    pub management_listen: SocketAddr,
    /// The one node the chain products follow and every product's network
    /// identity comes from (each distribution scope names the genesis hash,
    /// spec 11.3). Required when any product is enabled; when present, the
    /// node's network identity is checked at start-up.
    #[serde(default)]
    pub chain: Option<Chain>,
    #[serde(default)]
    pub memo: Option<Section>,
    #[serde(default)]
    pub first_contact: Option<Section>,
    #[serde(default)]
    pub tachygram: Option<Section>,
    #[serde(default)]
    pub registry: Option<Section>,
}

/// The `chain` section.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chain {
    /// The node's JSON-RPC endpoint: `http` or `https`, without credentials.
    pub rpc_url: String,
    /// An absolute path to the node's cookie file; exclusive with `no_auth`.
    #[serde(default)]
    pub rpc_cookie: Option<PathBuf>,
    /// Explicitly permit an RPC endpoint with authentication disabled.
    #[serde(default)]
    pub no_auth: bool,
    /// The follower's tip poll interval, nonzero.
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
    /// Blocks per epoch on this network (spec 15), nonzero. Bound into every
    /// chain product's journal and distribution scope, so it has no default.
    pub epoch_length: u32,
}

fn default_poll_ms() -> u64 {
    1000
}

/// One product section: `{ "enabled": bool, "settings": {...} }`. Settings
/// are checked against the product's own schema when the product is compiled
/// in, whether or not it is enabled; an enabled product needs them.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Section {
    pub enabled: bool,
    #[serde(default)]
    pub settings: Option<serde_json::Value>,
}

/// The four products, by configuration section.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Product {
    Memo,
    FirstContact,
    Tachygram,
    Registry,
}

impl Product {
    pub const ALL: [Product; 4] = [
        Product::Memo,
        Product::FirstContact,
        Product::Tachygram,
        Product::Registry,
    ];

    /// The configuration section name.
    pub const fn section(self) -> &'static str {
        match self {
            Self::Memo => "memo",
            Self::FirstContact => "first_contact",
            Self::Tachygram => "tachygram",
            Self::Registry => "registry",
        }
    }

    /// The Cargo feature that compiles the product in.
    pub const fn feature(self) -> &'static str {
        match self {
            Self::Memo => "memo",
            Self::FirstContact => "first-contact",
            Self::Tachygram => "tachygram",
            Self::Registry => "registry",
        }
    }

    /// Whether this build includes the product.
    pub const fn compiled(self) -> bool {
        match self {
            Self::Memo => cfg!(feature = "memo"),
            Self::FirstContact => cfg!(feature = "first-contact"),
            Self::Tachygram => cfg!(feature = "tachygram"),
            Self::Registry => cfg!(feature = "registry"),
        }
    }
}

/// The root chain source, validated.
#[derive(Clone, Debug)]
pub struct ChainPlan {
    /// What `run_registered`'s follower and the chain products read.
    pub settings: ChainSettings,
    pub epoch_length: u32,
}

/// Everything `run` needs, after every static rule passed.
pub struct Plan {
    pub management_listen: SocketAddr,
    pub chain: Option<ChainPlan>,
    /// One definition per enabled product, memo and first-contact sharing one.
    pub publishers: Vec<PublisherDefinition<axum::Router>>,
}

impl Plan {
    /// The definition names, in registration order.
    pub fn product_names(&self) -> Vec<&'static str> {
        self.publishers.iter().map(|p| p.name).collect()
    }
}

impl Config {
    /// Parse the configuration file at `path`.
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .map_err(|error| format!("cannot read configuration {}: {error}", path.display()))?;
        Self::from_json(&bytes)
            .map_err(|error| format!("configuration {}: {error}", path.display()).into())
    }

    /// Parse a configuration document.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// The section configured for `product`, if any.
    pub fn section(&self, product: Product) -> Option<&Section> {
        match product {
            Product::Memo => self.memo.as_ref(),
            Product::FirstContact => self.first_contact.as_ref(),
            Product::Tachygram => self.tachygram.as_ref(),
            Product::Registry => self.registry.as_ref(),
        }
    }

    /// Whether `product`'s section is present and enabled.
    pub fn enabled(&self, product: Product) -> bool {
        self.section(product).is_some_and(|section| section.enabled)
    }

    /// Apply every static rule, in this order, and build the product
    /// definitions (which acquires nothing):
    ///
    /// 1. `first_contact` enabled requires `memo` enabled (spec 11.7).
    /// 2. A section for a product not compiled in is refused, naming the
    ///    Cargo feature (spec 4.2).
    /// 3. The `chain` section, when present: a credential-free `http(s)` URL,
    ///    exactly one of `no_auth` and an absolute `rpc_cookie`, nonzero
    ///    `poll_ms` and `epoch_length`. It is required when any product is
    ///    enabled.
    /// 4. Each compiled product's settings against its own schema (unknown
    ///    fields refused); an enabled product needs settings.
    /// 5. The resource layout `run_registered` will enforce: a loopback
    ///    management listener, private product listeners with fixed ports and
    ///    no sharing, absolute state directories disjoint between products.
    pub fn plan(&self) -> Result<Plan> {
        if self.enabled(Product::FirstContact) && !self.enabled(Product::Memo) {
            return Err(
                "first_contact is enabled but memo is not: first-contact runs inside \
                 the memo publisher and shares its state directory (spec 11.7)"
                    .into(),
            );
        }
        for product in Product::ALL {
            if self.section(product).is_some() && !product.compiled() {
                return Err(format!(
                    "the `{}` section configures a product this binary does not include; \
                     rebuild with the `{}` feature (compiled products: {})",
                    product.section(),
                    product.feature(),
                    compiled_list()
                )
                .into());
            }
        }
        let chain = self.chain.as_ref().map(Chain::plan).transpose()?;
        if chain.is_none()
            && let Some(product) = Product::ALL.into_iter().find(|p| self.enabled(*p))
        {
            return Err(format!(
                "{} is enabled but there is no chain section",
                product.section()
            )
            .into());
        }
        let publishers = products::definitions(self, chain.as_ref())?;
        layout::validate(self.management_listen, &publishers)?;
        Ok(Plan {
            management_listen: self.management_listen,
            chain,
            publishers,
        })
    }
}

fn compiled_list() -> String {
    if crate::PRODUCTS.is_empty() {
        "none".into()
    } else {
        crate::PRODUCTS.join(", ")
    }
}

impl Chain {
    /// Check the section without reading the cookie.
    pub fn plan(&self) -> Result<ChainPlan> {
        let url = url::Url::parse(&self.rpc_url)
            .map_err(|error| format!("chain rpc_url {:?}: {error}", self.rpc_url))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(format!(
                "chain rpc_url {:?} must be an http(s) URL with a host",
                self.rpc_url
            )
            .into());
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err("chain rpc_url must not carry credentials; use rpc_cookie".into());
        }
        match (&self.rpc_cookie, self.no_auth) {
            (Some(_), true) => {
                return Err("chain sets both rpc_cookie and no_auth; choose one".into());
            }
            (None, false) => {
                return Err("chain needs rpc_cookie, or an explicit \"no_auth\": true".into());
            }
            (Some(cookie), false) if !cookie.is_absolute() => {
                return Err(
                    format!("chain rpc_cookie {} must be absolute", cookie.display()).into(),
                );
            }
            _ => {}
        }
        if self.poll_ms == 0 {
            return Err("chain poll_ms must be nonzero".into());
        }
        if self.epoch_length == 0 {
            return Err("chain epoch_length must be at least one block".into());
        }
        Ok(ChainPlan {
            settings: ChainSettings {
                rpc_url: self.rpc_url.clone(),
                rpc_cookie: self.rpc_cookie.clone(),
                poll: Duration::from_millis(self.poll_ms),
            },
            epoch_length: self.epoch_length,
        })
    }
}

/// Parse a compiled product's settings against its schema, naming the
/// section in the error.
#[cfg_attr(not(feature = "tachygram"), allow(dead_code))]
pub(crate) fn settings<T: serde::de::DeserializeOwned>(
    product: Product,
    section: &Section,
) -> Result<Option<T>> {
    let parsed = section
        .settings
        .clone()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| format!("{}.settings: {error}", product.section()))?;
    if section.enabled && parsed.is_none() {
        return Err(format!("{} is enabled but has no settings", product.section()).into());
    }
    Ok(parsed.filter(|_| section.enabled))
}
