//! The memo publisher's configuration (spec 11.7): the `memo.settings` and
//! `first_contact.settings` objects and the product configuration built from
//! them.
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use tachyon_chain::DecodeOptions;
use tachyon_pir_distribution::publisher::DEFAULT_PUBLISHER_RETENTION;

/// The `memo.settings` object of the publisher configuration (spec 11.7).
/// Unknown fields are refused.
///
/// ```json
/// { "listen": "127.0.0.1:9401", "state_dir": "/var/lib/tachyon-pir/memo" }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoSettings {
    /// The memo definition's private listener: `/provider/memo/v1/`,
    /// `/provider/first-contact/v1/` when first-contact is enabled, and
    /// `GET /metrics`.
    pub listen: SocketAddr,
    /// Absolute; holds `memo-v1.sqlite`, `distribution/memo/`,
    /// `distribution/first-contact/` and the `publisher.lock`. Disjoint from
    /// every other product's.
    pub state_dir: PathBuf,
}

/// The `first_contact.settings` object (spec 11.7). First-contact shares the
/// memo listener and state directory, so it has no settings of its own;
/// unknown fields are refused, so `{}` is the only valid value.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FirstContactSettings {}

/// Everything the memo publisher needs besides the root chain settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoConfig {
    pub listen: SocketAddr,
    pub state_dir: PathBuf,
    /// Blocks per epoch, from the configuration's `chain.epoch_length`
    /// (spec 11.7, 15). Bound into the journal and both distribution scopes.
    pub epoch_length: u32,
    /// Whether the first-contact product is published (its section is
    /// enabled). The journal holds first-contact entries either way.
    pub first_contact: bool,
    /// Block decoding; `strict_zip_rules` stays off for the alpha testnet
    /// (spec 3).
    pub decode: DecodeOptions,
    /// How long a displaced publication's objects stay served
    /// (`PUBLISHER_RETENTION_SECONDS`, spec 11.3).
    pub retention: Duration,
}

impl MemoConfig {
    /// The memo product of `settings` on a network with `epoch_length`-block
    /// epochs, without first-contact, with lenient decoding and the spec's
    /// 600 s retention.
    pub fn new(settings: MemoSettings, epoch_length: u32) -> Self {
        Self {
            listen: settings.listen,
            state_dir: settings.state_dir,
            epoch_length,
            first_contact: false,
            decode: DecodeOptions::default(),
            retention: DEFAULT_PUBLISHER_RETENTION,
        }
    }

    /// Also publish first-contact (an enabled `first_contact` section).
    pub fn with_first_contact(mut self, _settings: FirstContactSettings) -> Self {
        self.first_contact = true;
        self
    }

    /// Checks what can be checked without touching the filesystem or the
    /// network: an absolute state directory, a fixed port and a nonzero epoch
    /// length. The host additionally requires a private listener and state
    /// directories disjoint from other products'.
    pub fn validate(&self) -> Result<(), String> {
        if !self.state_dir.is_absolute() {
            return Err(format!(
                "memo state_dir {} must be absolute",
                self.state_dir.display()
            ));
        }
        if self.listen.port() == 0 {
            return Err("memo listen needs a fixed port".into());
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
        let settings: MemoSettings = serde_json::from_str(
            r#"{"listen": "127.0.0.1:9401", "state_dir": "/var/lib/tachyon-pir/memo"}"#,
        )
        .unwrap();
        let config = MemoConfig::new(settings.clone(), 4096);
        assert_eq!(config.validate(), Ok(()));
        assert!(!config.first_contact);
        assert_eq!(config.retention, Duration::from_secs(600));
        assert!(!config.decode.strict_zip_rules);
        assert!(
            serde_json::from_str::<MemoSettings>(
                r#"{"listen": "127.0.0.1:9401", "state_dir": "/x", "epoch_length": 16}"#
            )
            .is_err()
        );
        let first_contact: FirstContactSettings = serde_json::from_str("{}").unwrap();
        assert!(
            MemoConfig::new(settings.clone(), 4096)
                .with_first_contact(first_contact)
                .first_contact
        );
        assert!(serde_json::from_str::<FirstContactSettings>(r#"{"listen": "x"}"#).is_err());
        let relative = MemoConfig::new(
            MemoSettings {
                state_dir: "memo".into(),
                ..settings.clone()
            },
            4096,
        );
        assert!(relative.validate().is_err());
        let ephemeral = MemoConfig::new(
            MemoSettings {
                listen: "127.0.0.1:0".parse().unwrap(),
                ..settings.clone()
            },
            4096,
        );
        assert!(ephemeral.validate().is_err());
        assert!(MemoConfig::new(settings, 0).validate().is_err());
    }
}
