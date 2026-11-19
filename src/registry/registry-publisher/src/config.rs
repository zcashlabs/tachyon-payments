//! The registry publisher's configuration (spec 9.2, 9.3, 11.7).
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use tachyon_pir_distribution::publisher::DEFAULT_PUBLISHER_RETENTION;
use tachyon_pir_protocol::{BlockHash, REGISTRY_DEFAULT_DIFFICULTY_BITS};

/// Default `per_client_concurrent` (spec 9.3).
pub const DEFAULT_PER_CLIENT_CONCURRENT: usize = 2;
/// Default `per_client_per_minute` (spec 9.3).
pub const DEFAULT_PER_CLIENT_PER_MINUTE: u32 = 6;
/// Default `global_per_minute` (spec 9.3).
pub const DEFAULT_GLOBAL_PER_MINUTE: u32 = 600;
/// Default `upload_ms` (spec 9.3).
pub const DEFAULT_UPLOAD_MS: u64 = 15_000;
/// Default `publish_ms` (spec 9.2).
pub const DEFAULT_PUBLISH_MS: u64 = 1_000;

/// The `registry.settings` object of the publisher configuration
/// (spec 11.7, 9.2, 9.3). Unknown fields are refused.
///
/// ```json
/// { "listen": "127.0.0.1:9403", "state_dir": "/var/lib/tachyon-pir/registry",
///   "difficulty_bits": 20 }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrySettings {
    /// The product's private listener: the distribution prefix
    /// `/provider/registry/v1/`, the write API at `/registry/v1/` and
    /// `/v1/registry/`, and `GET /metrics`.
    pub listen: SocketAddr,
    /// Absolute; holds `registry-v1.sqlite`, `challenge-secret`,
    /// `distribution/registry/` and the `publisher.lock`.
    pub state_dir: PathBuf,
    /// Proof-of-work difficulty in leading zero bits (default 20).
    #[serde(default = "default_difficulty_bits")]
    pub difficulty_bits: u32,
    /// Registrations one client may have in flight (default 2).
    #[serde(default = "default_per_client_concurrent")]
    pub per_client_concurrent: usize,
    /// Registrations one client may start per sliding minute (default 6).
    #[serde(default = "default_per_client_per_minute")]
    pub per_client_per_minute: u32,
    /// Write-API requests accepted per sliding minute in total (default 600).
    #[serde(default = "default_global_per_minute")]
    pub global_per_minute: u32,
    /// Deadline for receiving a registration body (default 15,000 ms).
    #[serde(default = "default_upload_ms")]
    pub upload_ms: u64,
    /// Least time between the starts of two publications (default 1,000 ms).
    #[serde(default = "default_publish_ms")]
    pub publish_ms: u64,
}

fn default_difficulty_bits() -> u32 {
    REGISTRY_DEFAULT_DIFFICULTY_BITS
}
fn default_per_client_concurrent() -> usize {
    DEFAULT_PER_CLIENT_CONCURRENT
}
fn default_per_client_per_minute() -> u32 {
    DEFAULT_PER_CLIENT_PER_MINUTE
}
fn default_global_per_minute() -> u32 {
    DEFAULT_GLOBAL_PER_MINUTE
}
fn default_upload_ms() -> u64 {
    DEFAULT_UPLOAD_MS
}
fn default_publish_ms() -> u64 {
    DEFAULT_PUBLISH_MS
}

impl RegistrySettings {
    /// Settings with every default (spec 9.2, 9.3).
    pub fn new(listen: SocketAddr, state_dir: PathBuf) -> Self {
        Self {
            listen,
            state_dir,
            difficulty_bits: default_difficulty_bits(),
            per_client_concurrent: DEFAULT_PER_CLIENT_CONCURRENT,
            per_client_per_minute: DEFAULT_PER_CLIENT_PER_MINUTE,
            global_per_minute: DEFAULT_GLOBAL_PER_MINUTE,
            upload_ms: DEFAULT_UPLOAD_MS,
            publish_ms: DEFAULT_PUBLISH_MS,
        }
    }
}

/// The network identity the registry is bound to (spec 9.2 Identity): the
/// node's chain name and genesis hash, read once at start-up by the binary.
/// The registry follows no blocks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryIdentity {
    /// `getblockchaininfo.chain`, for example `test`; the map's `network`.
    pub chain: String,
    /// The genesis block hash in protocol order: the map's `genesis_hash`,
    /// the distribution scope's chain and the placement hash input.
    pub genesis_hash: BlockHash,
}

impl RegistryIdentity {
    pub fn new(chain: impl Into<String>, genesis_hash: BlockHash) -> Self {
        Self {
            chain: chain.into(),
            genesis_hash,
        }
    }
}

/// Everything the registry publisher needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryConfig {
    pub settings: RegistrySettings,
    pub identity: RegistryIdentity,
    /// How long a displaced publication's objects stay served
    /// (`PUBLISHER_RETENTION_SECONDS`, spec 11.3).
    pub retention: Duration,
}

impl RegistryConfig {
    /// `settings` bound to `identity`, with the spec's 600 s retention.
    pub fn new(settings: RegistrySettings, identity: RegistryIdentity) -> Self {
        Self {
            settings,
            identity,
            retention: DEFAULT_PUBLISHER_RETENTION,
        }
    }

    pub fn upload(&self) -> Duration {
        Duration::from_millis(self.settings.upload_ms)
    }

    pub fn publish_interval(&self) -> Duration {
        Duration::from_millis(self.settings.publish_ms)
    }

    /// Checks what can be checked without touching the filesystem: an
    /// absolute state directory, a fixed port, a difficulty of at most 256
    /// bits, nonzero limits and upload deadline, and a named network. The
    /// host additionally requires a private listener and state directories
    /// disjoint from other products'.
    pub fn validate(&self) -> Result<(), String> {
        let s = &self.settings;
        if !s.state_dir.is_absolute() {
            return Err(format!(
                "registry state_dir {} must be absolute",
                s.state_dir.display()
            ));
        }
        if s.listen.port() == 0 {
            return Err("registry listen needs a fixed port".into());
        }
        if s.difficulty_bits > 256 {
            return Err(format!(
                "registry difficulty_bits {} is above 256",
                s.difficulty_bits
            ));
        }
        if s.per_client_concurrent == 0 {
            return Err("registry per_client_concurrent must be at least 1".into());
        }
        if s.per_client_per_minute == 0 {
            return Err("registry per_client_per_minute must be at least 1".into());
        }
        if s.global_per_minute == 0 {
            return Err("registry global_per_minute must be at least 1".into());
        }
        if s.upload_ms == 0 {
            return Err("registry upload_ms must be at least 1".into());
        }
        if self.identity.chain.is_empty() {
            return Err("registry network identity has no chain name".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> RegistryIdentity {
        RegistryIdentity::new("test", BlockHash::from_protocol_bytes([7; 32]))
    }

    #[test]
    fn settings_default_and_refuse_unknown_fields() {
        let settings: RegistrySettings = serde_json::from_str(
            r#"{"listen": "127.0.0.1:9403", "state_dir": "/var/lib/tachyon-pir/registry",
                "difficulty_bits": 20}"#,
        )
        .unwrap();
        assert_eq!(
            settings,
            RegistrySettings::new(
                "127.0.0.1:9403".parse().unwrap(),
                "/var/lib/tachyon-pir/registry".into()
            )
        );
        assert_eq!(settings.difficulty_bits, 20);
        assert_eq!(settings.per_client_concurrent, 2);
        assert_eq!(settings.per_client_per_minute, 6);
        assert_eq!(settings.global_per_minute, 600);
        assert_eq!(settings.upload_ms, 15_000);
        assert_eq!(settings.publish_ms, 1_000);
        let config = RegistryConfig::new(settings.clone(), identity());
        assert_eq!(config.validate(), Ok(()));
        assert_eq!(config.retention, Duration::from_secs(600));
        let all: RegistrySettings = serde_json::from_str(
            r#"{"listen": "127.0.0.1:9403", "state_dir": "/r", "difficulty_bits": 4,
                "per_client_concurrent": 1, "per_client_per_minute": 3,
                "global_per_minute": 9, "upload_ms": 50, "publish_ms": 10}"#,
        )
        .unwrap();
        assert_eq!(
            (all.per_client_concurrent, all.per_client_per_minute),
            (1, 3)
        );
        assert_eq!(
            (all.global_per_minute, all.upload_ms, all.publish_ms),
            (9, 50, 10)
        );
        for unknown in [
            r#"{"listen": "127.0.0.1:9403", "state_dir": "/r", "epoch_length": 16}"#,
            r#"{"listen": "127.0.0.1:9403", "state_dir": "/r", "enabled": true}"#,
        ] {
            assert!(serde_json::from_str::<RegistrySettings>(unknown).is_err());
        }
        assert!(
            serde_json::from_str::<RegistrySettings>(r#"{"listen": "127.0.0.1:9403"}"#).is_err()
        );
    }

    #[test]
    fn validate_refuses_each_bad_value() {
        let base = || RegistrySettings::new("127.0.0.1:9403".parse().unwrap(), "/r".into());
        let refused = |change: fn(&mut RegistrySettings)| {
            let mut settings = base();
            change(&mut settings);
            RegistryConfig::new(settings, identity())
                .validate()
                .unwrap_err()
        };
        assert!(refused(|s| s.state_dir = "relative".into()).contains("absolute"));
        assert!(refused(|s| s.listen = "127.0.0.1:0".parse().unwrap()).contains("port"));
        assert!(refused(|s| s.difficulty_bits = 257).contains("256"));
        assert!(refused(|s| s.per_client_concurrent = 0).contains("per_client_concurrent"));
        assert!(refused(|s| s.per_client_per_minute = 0).contains("per_client_per_minute"));
        assert!(refused(|s| s.global_per_minute = 0).contains("global_per_minute"));
        assert!(refused(|s| s.upload_ms = 0).contains("upload_ms"));
        let mut nameless = RegistryConfig::new(base(), identity());
        nameless.identity.chain.clear();
        assert!(nameless.validate().is_err());
        let mut zero = base();
        zero.difficulty_bits = 0;
        zero.publish_ms = 0;
        assert_eq!(RegistryConfig::new(zero, identity()).validate(), Ok(()));
    }
}
