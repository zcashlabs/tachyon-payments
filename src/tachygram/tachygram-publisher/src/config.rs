//! The tachygram product's publisher configuration (spec 11.7).
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use tachyon_chain::DecodeOptions;
use tachyon_pir_distribution::publisher::DEFAULT_PUBLISHER_RETENTION;

/// The `tachygram.settings` object of the publisher configuration
/// (spec 11.7). Unknown fields are refused.
///
/// ```json
/// { "listen": "127.0.0.1:9402", "state_dir": "/var/lib/tachyon-pir/tachygram" }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TachygramSettings {
    /// The product's private listener: the distribution prefix
    /// `/provider/tachygram/v1/` and `GET /metrics`.
    pub listen: SocketAddr,
    /// Absolute; holds `tachygram-v1.sqlite`, `distribution/tachygram/` and
    /// the `publisher.lock`. Disjoint from every other product's.
    pub state_dir: PathBuf,
}

/// Everything the tachygram publisher needs besides the root chain settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TachygramConfig {
    pub listen: SocketAddr,
    pub state_dir: PathBuf,
    /// Blocks per epoch, from the configuration's `chain.epoch_length`
    /// (spec 11.7, 15). Bound into the journal and the distribution scope.
    pub epoch_length: u32,
    /// Block decoding; `strict_zip_rules` stays off for the alpha testnet
    /// (spec 3).
    pub decode: DecodeOptions,
    /// How long a displaced publication's objects stay served
    /// (`PUBLISHER_RETENTION_SECONDS`, spec 11.3).
    pub retention: Duration,
}

impl TachygramConfig {
    /// The configuration of `settings` on a network with `epoch_length`-block
    /// epochs, with lenient decoding and the spec's 600 s retention.
    pub fn new(settings: TachygramSettings, epoch_length: u32) -> Self {
        Self {
            listen: settings.listen,
            state_dir: settings.state_dir,
            epoch_length,
            decode: DecodeOptions::default(),
            retention: DEFAULT_PUBLISHER_RETENTION,
        }
    }

    /// Checks what can be checked without touching the filesystem or the
    /// network: an absolute state directory, a fixed port and a nonzero epoch
    /// length. The host additionally requires a private listener and state
    /// directories disjoint from other products'.
    pub fn validate(&self) -> Result<(), String> {
        if !self.state_dir.is_absolute() {
            return Err(format!(
                "tachygram state_dir {} must be absolute",
                self.state_dir.display()
            ));
        }
        if self.listen.port() == 0 {
            return Err("tachygram listen needs a fixed port".into());
        }
        if self.epoch_length == 0 {
            return Err("chain epoch_length must be at least one block".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_refuse_unknown_fields_and_validate() {
        let settings: TachygramSettings = serde_json::from_str(
            r#"{"listen": "127.0.0.1:9402", "state_dir": "/var/lib/tachyon-pir/tachygram"}"#,
        )
        .unwrap();
        let config = TachygramConfig::new(settings.clone(), 4096);
        assert_eq!(config.validate(), Ok(()));
        assert_eq!(config.retention, Duration::from_secs(600));
        assert!(!config.decode.strict_zip_rules);
        assert!(
            serde_json::from_str::<TachygramSettings>(
                r#"{"listen": "127.0.0.1:9402", "state_dir": "/x", "epoch_length": 16}"#
            )
            .is_err()
        );
        let relative = TachygramConfig::new(
            TachygramSettings {
                state_dir: "tachygram".into(),
                ..settings.clone()
            },
            4096,
        );
        assert!(relative.validate().is_err());
        let ephemeral = TachygramConfig::new(
            TachygramSettings {
                listen: "127.0.0.1:0".parse().unwrap(),
                ..settings.clone()
            },
            4096,
        );
        assert!(ephemeral.validate().is_err());
        assert!(TachygramConfig::new(settings, 0).validate().is_err());
    }
}
