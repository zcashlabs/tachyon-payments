//! Query-server configuration (spec 11.7): JSON, every struct refusing
//! unknown fields. [`Config::validate`] is offline: it opens no state, binds
//! no socket and reads no certificate, so `--check` can run anywhere.

use crate::{
    Error,
    product::{self, MapFamily, ProductModule, SECTIONS},
};
use serde::Deserialize;
use std::{
    net::{IpAddr, SocketAddr},
    path::{Component, Path, PathBuf},
    time::Duration,
};
use tachyon_pir_distribution::ScopeBinding;
use tachyon_pir_protocol::{BlockHash, MAX_QUERY_BODY_BYTES, UPLOAD_DEADLINE_SECONDS};

/// The only role version 1 supports (spec 4.1).
pub const ROLE_COMBINED: &str = "combined";
/// Default `max_unconfirmed_seconds` (spec 11.6).
pub const DEFAULT_MAX_UNCONFIRMED_SECONDS: u64 = 600;
/// Default interval between successful synchronization passes.
pub const DEFAULT_POLL_MS: u64 = 1000;

/// The configuration file.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Operator listener: `/health`, `/ready`, `/metrics`. Loopback only.
    pub management_listen: SocketAddr,
    /// The wallet-facing listener every enabled product is mounted on,
    /// behind the deployment's HTTPS edge.
    pub public_listen: SocketAddr,
    #[serde(default)]
    pub memo: Option<Section>,
    #[serde(default)]
    pub first_contact: Option<Section>,
    #[serde(default)]
    pub tachygram: Option<Section>,
    #[serde(default)]
    pub registry: Option<Section>,
}

/// One product's section.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Section {
    pub enabled: bool,
    /// Required when enabled.
    #[serde(default)]
    pub settings: Option<Settings>,
}

/// One product's settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The publisher's HTTPS origin, `https://host[:port]/`.
    pub publisher: String,
    /// This product's own state directory (floor, object cache, lock).
    pub state_dir: PathBuf,
    /// PEM trust roots for the publisher; the system roots without it.
    #[serde(default)]
    pub publisher_ca: Option<PathBuf>,
    /// Must be `combined` in version 1.
    pub role: String,
    /// The `network` every map must carry.
    pub publisher_network: String,
    /// The genesis block hash every map and envelope must carry, display order.
    pub publisher_genesis: BlockHash,
    /// The NuTachyon activation height the publisher's chain scopes start
    /// at; required for chain products. With `epoch_length` it fixes the
    /// distribution scope's `source` (spec 11.3).
    #[serde(default)]
    pub activation_height: Option<u32>,
    /// The publisher's epoch length; required for chain products.
    #[serde(default)]
    pub epoch_length: Option<u32>,
    /// Serve the newest `retain_scopes` scopes (spec 11.5); the product's
    /// default when absent.
    #[serde(default)]
    pub retain_scopes: Option<u64>,
    /// Keep serving this long after the last successful authority check
    /// started (spec 11.6), default 600.
    #[serde(default)]
    pub max_unconfirmed_seconds: Option<u64>,
    /// Admission limits (spec 10.6).
    #[serde(default)]
    pub admission: Option<Admission>,
    /// Interval between successful synchronization passes, default 1000.
    #[serde(default)]
    pub poll_ms: Option<u64>,
}

/// Admission limits of one product (spec 10.4, 10.6). The defaults are
/// Receiver's starting values and the spec's upload deadline; they are to be
/// calibrated in qualification.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    /// Queries evaluated at once.
    #[serde(default = "defaults::executing")]
    pub executing: usize,
    /// Queries that may wait for an evaluation slot.
    #[serde(default = "defaults::waiting")]
    pub waiting: usize,
    /// How long a query waits for an evaluation slot.
    #[serde(default = "defaults::wait_ms")]
    pub wait_ms: u64,
    /// Queries one client may have in flight, uploads included.
    #[serde(default = "defaults::per_client")]
    pub per_client: usize,
    /// How long a query's upload may take before `408`.
    #[serde(default = "defaults::upload_ms")]
    pub upload_ms: u64,
}

mod defaults {
    pub fn executing() -> usize {
        2
    }
    pub fn waiting() -> usize {
        8
    }
    pub fn wait_ms() -> u64 {
        2000
    }
    pub fn per_client() -> usize {
        2
    }
    pub fn upload_ms() -> u64 {
        super::UPLOAD_DEADLINE_SECONDS * 1000
    }
}

impl Default for Admission {
    fn default() -> Self {
        Self {
            executing: defaults::executing(),
            waiting: defaults::waiting(),
            wait_ms: defaults::wait_ms(),
            per_client: defaults::per_client(),
            upload_ms: defaults::upload_ms(),
        }
    }
}

impl Admission {
    pub fn wait(&self) -> Duration {
        Duration::from_millis(self.wait_ms)
    }
    pub fn upload(&self) -> Duration {
        Duration::from_millis(self.upload_ms)
    }
    /// The query body limit, fixed by the spec (512 KiB, spec 10.4).
    pub const fn body_limit(&self) -> usize {
        MAX_QUERY_BODY_BYTES
    }
}

/// A validated configuration.
#[derive(Clone, Debug)]
pub struct Validated {
    pub management_listen: SocketAddr,
    pub public_listen: SocketAddr,
    pub products: Vec<ProductConfig>,
}

/// One enabled, compiled product.
#[derive(Clone, Debug)]
pub struct ProductConfig {
    pub module: &'static ProductModule,
    pub publisher: String,
    pub publisher_ca: Option<PathBuf>,
    pub state_dir: PathBuf,
    pub network: String,
    pub genesis: BlockHash,
    pub chain: Option<ChainParams>,
    pub retain_scopes: Option<u64>,
    pub max_unconfirmed: Duration,
    pub admission: Admission,
    pub poll: Duration,
}

/// The chain parameters that shape a chain product's scopes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainParams {
    pub activation_height: u32,
    pub epoch_length: u32,
}

impl ProductConfig {
    /// The distribution scope binding the envelope must match (spec 11.3).
    pub fn binding(&self) -> ScopeBinding {
        let genesis = *self.genesis.protocol_bytes();
        match (self.module.map, self.chain) {
            (MapFamily::Chain, Some(chain)) => ScopeBinding::chain(
                genesis,
                u64::from(chain.activation_height),
                u64::from(chain.epoch_length),
            ),
            (MapFamily::Chain, None) => unreachable!("validated chain products have parameters"),
        }
    }
}

fn private(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    }
}

/// `path` without `.` and `..` components, for overlap checks.
fn normalize(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                result.pop();
            }
            Component::CurDir => {}
            other => result.push(other),
        }
    }
    result
}

impl Config {
    /// Parse a configuration file's bytes; unknown fields are refused.
    pub fn parse(bytes: &[u8]) -> Result<Config, Error> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn sections(&self) -> [Option<&Section>; 4] {
        [
            self.memo.as_ref(),
            self.first_contact.as_ref(),
            self.tachygram.as_ref(),
            self.registry.as_ref(),
        ]
    }

    /// Every static rule: compiled products only, settings of enabled
    /// products, role, listeners, paths and limits.
    pub fn validate(&self) -> Result<Validated, Error> {
        if !self.management_listen.ip().is_loopback() {
            return Err("management_listen must be a loopback address".into());
        }
        for (name, addr) in [
            ("management_listen", self.management_listen),
            ("public_listen", self.public_listen),
        ] {
            if !private(addr) || addr.port() == 0 {
                return Err(format!(
                    "{name} needs a fixed loopback or private address, not {addr}"
                )
                .into());
            }
        }
        if self.management_listen.port() == self.public_listen.port()
            && (self.management_listen.ip() == self.public_listen.ip()
                || self.management_listen.ip().is_unspecified()
                || self.public_listen.ip().is_unspecified())
        {
            return Err("management_listen and public_listen conflict".into());
        }
        let mut products: Vec<ProductConfig> = Vec::new();
        for ((section_name, product, feature), section) in SECTIONS.iter().zip(self.sections()) {
            let Some(section) = section else { continue };
            let Some(module) = product::module(*product) else {
                return Err(format!(
                    "configuration section `{section_name}` configures {product}, which is not \
                     compiled into this build (Cargo feature `{feature}`)"
                )
                .into());
            };
            if !section.enabled {
                continue;
            }
            let settings = section
                .settings
                .as_ref()
                .ok_or_else(|| format!("enabled product {section_name} has no settings"))?;
            let config = settings
                .validate(module)
                .map_err(|error| format!("{section_name}: {error}"))?;
            let root = normalize(&config.state_dir);
            if let Some(other) = products.iter().find(|other| {
                let existing = normalize(&other.state_dir);
                root.starts_with(&existing) || existing.starts_with(&root)
            }) {
                return Err(format!(
                    "{section_name} and {} have overlapping state directories",
                    other.module.section
                )
                .into());
            }
            products.push(config);
        }
        if products.is_empty() {
            return Err("no compiled product is enabled".into());
        }
        Ok(Validated {
            management_listen: self.management_listen,
            public_listen: self.public_listen,
            products,
        })
    }
}

impl Settings {
    fn validate(&self, module: &'static ProductModule) -> Result<ProductConfig, Error> {
        if self.role != ROLE_COMBINED {
            return Err(format!(
                "role {:?} is not supported; version 1 supports only \"{ROLE_COMBINED}\"",
                self.role
            )
            .into());
        }
        // Offline: checks the origin's syntax and scheme, reads no certificate.
        pir_sync_manager::HttpsSource::new(
            &self.publisher,
            module.distribution().prefix(),
            1,
            &[],
        )?;
        if !self.state_dir.is_absolute() {
            return Err("state_dir must be absolute".into());
        }
        if self.publisher_ca.as_ref().is_some_and(|p| !p.is_absolute()) {
            return Err("publisher_ca must be absolute".into());
        }
        if self.publisher_network.is_empty() {
            return Err("publisher_network is empty".into());
        }
        if *self.publisher_genesis.protocol_bytes() == [0; 32] {
            return Err("publisher_genesis is all zero".into());
        }
        let chain = match module.map {
            MapFamily::Chain => {
                let (Some(activation_height), Some(epoch_length)) =
                    (self.activation_height, self.epoch_length)
                else {
                    return Err(format!(
                        "{} follows the chain: activation_height and epoch_length are required",
                        module.product
                    )
                    .into());
                };
                if epoch_length == 0 {
                    return Err("epoch_length must be positive".into());
                }
                Some(ChainParams {
                    activation_height,
                    epoch_length,
                })
            }
        };
        let retain_scopes = match self.retain_scopes {
            Some(0) => return Err("retain_scopes must be at least 1".into()),
            Some(n) => Some(n),
            None => module.default_retain_scopes,
        };
        let max_unconfirmed = self
            .max_unconfirmed_seconds
            .unwrap_or(DEFAULT_MAX_UNCONFIRMED_SECONDS);
        if !(1..=86_400).contains(&max_unconfirmed) {
            return Err("max_unconfirmed_seconds must be within 1..=86400".into());
        }
        let admission = self.admission.unwrap_or_default();
        if admission.executing == 0 || admission.per_client == 0 {
            return Err("admission executing and per_client must be positive".into());
        }
        if admission.executing > 1024 || admission.waiting > 65_536 || admission.wait_ms > 60_000 {
            return Err("admission limits are out of range".into());
        }
        if !(1..=600_000).contains(&admission.upload_ms) {
            return Err("admission upload_ms must be within 1..=600000".into());
        }
        let poll = self.poll_ms.unwrap_or(DEFAULT_POLL_MS);
        if !(10..=3_600_000).contains(&poll) {
            return Err("poll_ms must be within 10..=3600000".into());
        }
        Ok(ProductConfig {
            module,
            publisher: self.publisher.clone(),
            publisher_ca: self.publisher_ca.clone(),
            state_dir: self.state_dir.clone(),
            network: self.publisher_network.clone(),
            genesis: self.publisher_genesis,
            chain,
            retain_scopes,
            max_unconfirmed: Duration::from_secs(max_unconfirmed),
            admission,
            poll: Duration::from_millis(poll),
        })
    }
}
