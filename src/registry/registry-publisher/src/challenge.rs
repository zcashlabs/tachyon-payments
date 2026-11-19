//! Stateless registration challenges (spec 9.3):
//!
//! ```text
//! challenge = expires_unix(u64 LE) || HMAC-SHA256(secret,
//!             "tachyon-pir/registry/v1/challenge\0" || expires_unix(u64 LE))[0..24]
//! ```
//!
//! `secret` is 32 random bytes kept in `state_dir/challenge-secret` (mode
//! 0600), created at the registry's first start and reused afterwards, so a
//! challenge survives a restart. Verification recomputes the tag from the body
//! alone and compares it in constant time.
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use tachyon_pir_protocol::REGISTRY_CHALLENGE_SECONDS;

/// Domain of the challenge tag.
pub const CHALLENGE_DOMAIN: &[u8] = b"tachyon-pir/registry/v1/challenge\0";
/// The secret's file name under the state directory.
pub const SECRET_FILE: &str = "challenge-secret";
/// Bytes of the secret.
pub const SECRET_BYTES: usize = 32;
/// Bytes of the truncated tag in a challenge.
pub const TAG_BYTES: usize = 24;

type HmacSha256 = Hmac<Sha256>;

/// Why a challenge was refused; `Display` is the `400` message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ChallengeRefusal {
    #[error("challenge is not authentic")]
    Forged,
    #[error("challenge has expired")]
    Expired,
}

/// The registry's challenge key.
#[derive(Clone)]
pub struct ChallengeKey {
    secret: [u8; SECRET_BYTES],
}

impl std::fmt::Debug for ChallengeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ChallengeKey(..)")
    }
}

/// Seconds since the Unix epoch on the publisher's clock.
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl ChallengeKey {
    pub fn from_secret(secret: [u8; SECRET_BYTES]) -> Self {
        Self { secret }
    }

    /// Read `state_dir/challenge-secret`, or create it with 32 random bytes
    /// (mode 0600, synced, renamed into place, directory synced) when it does
    /// not exist. A secret of the wrong length, or one readable by group or
    /// others, is refused.
    pub fn load_or_create(state_dir: &Path) -> std::io::Result<Self> {
        let path = state_dir.join(SECRET_FILE);
        match std::fs::File::open(&path) {
            Ok(mut file) => {
                let mode = file.metadata()?.permissions().mode();
                if mode & 0o077 != 0 {
                    return Err(std::io::Error::other(format!(
                        "{} has mode {:o}; it must be 0600",
                        path.display(),
                        mode & 0o777
                    )));
                }
                let mut bytes = Vec::with_capacity(SECRET_BYTES + 1);
                Read::by_ref(&mut file)
                    .take(SECRET_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)?;
                let secret: [u8; SECRET_BYTES] = bytes.try_into().map_err(|_| {
                    std::io::Error::other(format!(
                        "{} must hold exactly {SECRET_BYTES} bytes",
                        path.display()
                    ))
                })?;
                Ok(Self { secret })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut secret = [0u8; SECRET_BYTES];
                getrandom::getrandom(&mut secret).map_err(std::io::Error::other)?;
                let temporary = state_dir.join(format!("{SECRET_FILE}.tmp"));
                let _ = std::fs::remove_file(&temporary);
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(&temporary)?;
                file.write_all(&secret)?;
                file.sync_all()?;
                drop(file);
                std::fs::rename(&temporary, &path)?;
                pir_control::durable::sync_dir(state_dir)?;
                tracing::info!("registry challenge secret created");
                Ok(Self { secret })
            }
            Err(error) => Err(error),
        }
    }

    fn mac(&self, expires_unix: u64) -> HmacSha256 {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC takes any key");
        mac.update(CHALLENGE_DOMAIN);
        mac.update(&expires_unix.to_le_bytes());
        mac
    }

    /// The challenge expiring at `expires_unix`.
    pub fn challenge(&self, expires_unix: u64) -> [u8; 32] {
        let tag = self.mac(expires_unix).finalize().into_bytes();
        let mut challenge = [0u8; 32];
        challenge[..8].copy_from_slice(&expires_unix.to_le_bytes());
        challenge[8..].copy_from_slice(&tag[..TAG_BYTES]);
        challenge
    }

    /// The challenge issued at `now_unix`: it expires
    /// `REGISTRY_CHALLENGE_SECONDS` (600) later.
    pub fn issue(&self, now_unix: u64) -> ([u8; 32], u64) {
        let expires = now_unix.saturating_add(REGISTRY_CHALLENGE_SECONDS);
        (self.challenge(expires), expires)
    }

    /// Authentic (constant-time tag comparison), then not expired: refused
    /// once `now_unix` passes `expires_unix`.
    pub fn verify(&self, challenge: &[u8; 32], now_unix: u64) -> Result<u64, ChallengeRefusal> {
        let expires = u64::from_le_bytes(challenge[..8].try_into().expect("8 bytes"));
        self.mac(expires)
            .verify_truncated_left(&challenge[8..])
            .map_err(|_| ChallengeRefusal::Forged)?;
        if now_unix > expires {
            return Err(ChallengeRefusal::Expired);
        }
        Ok(expires)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenge_layout_matches_an_independent_hmac() {
        let key = ChallengeKey::from_secret([0x42; 32]);
        let (challenge, expires) = key.issue(1_794_669_400);
        assert_eq!(expires, 1_794_670_000);
        assert_eq!(challenge[..8], 1_794_670_000u64.to_le_bytes());
        // HMAC-SHA256 by RFC 2104 over sha2 directly.
        use sha2::Digest;
        let mut inner = sha2::Sha256::new();
        inner.update([0x42 ^ 0x36; 32]);
        inner.update([0x36; 32]);
        inner.update(CHALLENGE_DOMAIN);
        inner.update(expires.to_le_bytes());
        let mut outer = sha2::Sha256::new();
        outer.update([0x42 ^ 0x5c; 32]);
        outer.update([0x5c; 32]);
        outer.update(inner.finalize());
        assert_eq!(challenge[8..], outer.finalize()[..24]);
        assert_eq!(key.verify(&challenge, 1_794_669_400), Ok(expires));
        assert_eq!(key.verify(&challenge, expires), Ok(expires));
        assert_eq!(
            key.verify(&challenge, expires + 1),
            Err(ChallengeRefusal::Expired)
        );
    }

    #[test]
    fn forged_and_altered_challenges_are_refused() {
        let key = ChallengeKey::from_secret([1; 32]);
        let (challenge, expires) = key.issue(1000);
        for byte in 0..32 {
            let mut altered = challenge;
            altered[byte] ^= 0x01;
            assert_eq!(
                key.verify(&altered, 1000),
                Err(ChallengeRefusal::Forged),
                "byte {byte}"
            );
        }
        // Moving the expiry forward without the tag is refused, even when the
        // altered challenge is otherwise unexpired.
        let mut later = challenge;
        later[..8].copy_from_slice(&(expires + 3600).to_le_bytes());
        assert_eq!(key.verify(&later, 1000), Err(ChallengeRefusal::Forged));
        // Another key's challenge is refused; a forged one is refused before
        // its expiry is looked at.
        let other = ChallengeKey::from_secret([2; 32]);
        assert_eq!(
            key.verify(&other.issue(1000).0, 1000),
            Err(ChallengeRefusal::Forged)
        );
        assert_eq!(
            key.verify(&other.issue(0).0, 10_000),
            Err(ChallengeRefusal::Forged)
        );
    }

    #[test]
    fn secret_is_created_private_and_reused() {
        let dir = tempfile::tempdir().unwrap();
        let key = ChallengeKey::load_or_create(dir.path()).unwrap();
        let path = dir.path().join(SECRET_FILE);
        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), 32);
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert!(!dir.path().join("challenge-secret.tmp").exists());
        let (challenge, _) = key.issue(now_unix());
        let again = ChallengeKey::load_or_create(dir.path()).unwrap();
        assert_eq!(again.secret, key.secret);
        assert!(again.verify(&challenge, now_unix()).is_ok());
        // Two fresh state directories get different secrets.
        let other = tempfile::tempdir().unwrap();
        let fresh = ChallengeKey::load_or_create(other.path()).unwrap();
        assert_ne!(fresh.secret, key.secret);
        // A widened mode or a wrong length is refused.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(ChallengeKey::load_or_create(dir.path()).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&path, [0u8; 33]).unwrap();
        assert!(ChallengeKey::load_or_create(dir.path()).is_err());
        std::fs::write(&path, [0u8; 31]).unwrap();
        assert!(ChallengeKey::load_or_create(dir.path()).is_err());
    }
}
