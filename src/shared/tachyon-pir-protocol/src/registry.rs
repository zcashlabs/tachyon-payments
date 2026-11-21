//! Registry wire types (spec 9.1-9.3): address and digest, the challenge and
//! registration JSON, proof of work and the registration body.
//!
//! The `pk` and `ek` validity checks and the HMAC challenges live in the
//! publisher and are not here.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::bytes::Bytes32;
use crate::constants::{ADDR_BYTES, KEM_EK_BYTES, PK_BYTES, POW_NONCE_BYTES, REGISTER_BODY_BYTES};

/// Domain of the address digest.
pub const ADDR_DOMAIN: &[u8] = b"tachyon-pir/registry/v1/addr\0";
/// Domain of the proof-of-work hash.
pub const POW_DOMAIN: &[u8] = b"tachyon-pir/registry/v1/pow\0";
/// Path of the challenge route under the registry mount.
pub const CHALLENGE_ROUTE: &str = "challenge";
/// Path of the registration route under the registry mount.
pub const REGISTER_ROUTE: &str = "register";

/// A body or field of the wrong length.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    #[error("address is {0} bytes, expected 1216")]
    AddressLength(usize),
    #[error("registration body is {0} bytes, expected 1256")]
    BodyLength(usize),
}

/// A registry address, `pk(32) || ek(1184)` (ML-KEM-768, provisional).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Address {
    pub pk: [u8; PK_BYTES],
    pub ek: [u8; KEM_EK_BYTES],
}

impl Address {
    /// Splits 1,216 bytes into `pk` and `ek`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, RegistryError> {
        if bytes.len() != ADDR_BYTES {
            return Err(RegistryError::AddressLength(bytes.len()));
        }
        Ok(Self {
            pk: bytes[..PK_BYTES].try_into().expect("pk"),
            ek: bytes[PK_BYTES..].try_into().expect("ek"),
        })
    }

    /// `pk || ek`.
    pub fn to_bytes(&self) -> [u8; ADDR_BYTES] {
        let mut out = [0u8; ADDR_BYTES];
        out[..PK_BYTES].copy_from_slice(&self.pk);
        out[PK_BYTES..].copy_from_slice(&self.ek);
        out
    }

    /// `SHA-256("tachyon-pir/registry/v1/addr\0" || addr)`.
    pub fn digest(&self) -> [u8; 32] {
        Sha256::new()
            .chain_update(ADDR_DOMAIN)
            .chain_update(self.pk)
            .chain_update(self.ek)
            .finalize()
            .into()
    }

    /// `digest[0..16]`: the registry key.
    pub fn key16(&self) -> [u8; 16] {
        self.digest()[..16].try_into().expect("16 bytes")
    }
}

/// `GET /registry/v1/challenge`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Challenge {
    /// 32 bytes, plain hex.
    pub challenge: Bytes32,
    pub difficulty_bits: u32,
    pub expires_unix: u64,
}

/// `200` response of `POST /registry/v1/register`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    pub chunk_id: u64,
    /// The address digest, plain hex.
    pub digest: Bytes32,
    /// False when the address was already registered.
    pub created: bool,
    /// The entry's log sequence; every map with `log_sequence` above it
    /// contains the entry.
    pub visible_after_sequence: u64,
}

/// `SHA-256("tachyon-pir/registry/v1/pow\0" || challenge || nonce || digest)`.
pub fn pow_hash(
    challenge: &[u8; 32],
    nonce: &[u8; POW_NONCE_BYTES],
    digest: &[u8; 32],
) -> [u8; 32] {
    Sha256::new()
        .chain_update(POW_DOMAIN)
        .chain_update(challenge)
        .chain_update(nonce)
        .chain_update(digest)
        .finalize()
        .into()
}

/// Leading zero bits of `hash`, most significant bit of byte 0 first.
pub fn leading_zero_bits(hash: &[u8; 32]) -> u32 {
    let mut bits = 0;
    for byte in hash {
        if *byte == 0 {
            bits += 8;
        } else {
            return bits + byte.leading_zeros();
        }
    }
    bits
}

/// Whether `hash` has at least `difficulty_bits` leading zero bits.
pub fn pow_meets(hash: &[u8; 32], difficulty_bits: u32) -> bool {
    leading_zero_bits(hash) >= difficulty_bits
}

/// The first nonce, counting up from 0 as a little-endian `u64`, whose proof
/// of work meets `difficulty_bits`. A client helper; expected work is
/// `2^difficulty_bits` hashes. `None` if no `u64` nonce meets it.
pub fn solve_pow(
    challenge: &[u8; 32],
    digest: &[u8; 32],
    difficulty_bits: u32,
) -> Option<[u8; POW_NONCE_BYTES]> {
    if difficulty_bits > 256 {
        return None;
    }
    (0..=u64::MAX)
        .map(u64::to_le_bytes)
        .find(|nonce| pow_meets(&pow_hash(challenge, nonce, digest), difficulty_bits))
}

/// The registration body, `challenge(32) || nonce(8) || addr(1216)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterRequest {
    pub challenge: [u8; 32],
    pub nonce: [u8; POW_NONCE_BYTES],
    pub address: Address,
}

impl RegisterRequest {
    /// Parses a body of exactly 1,256 bytes.
    pub fn parse(body: &[u8]) -> Result<Self, RegistryError> {
        if body.len() != REGISTER_BODY_BYTES {
            return Err(RegistryError::BodyLength(body.len()));
        }
        Ok(Self {
            challenge: body[..32].try_into().expect("challenge"),
            nonce: body[32..32 + POW_NONCE_BYTES].try_into().expect("nonce"),
            address: Address::from_bytes(&body[32 + POW_NONCE_BYTES..])?,
        })
    }

    /// The body bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(REGISTER_BODY_BYTES);
        out.extend_from_slice(&self.challenge);
        out.extend_from_slice(&self.nonce);
        out.extend_from_slice(&self.address.to_bytes());
        out
    }

    /// The proof-of-work hash this body carries, bound to its address digest.
    pub fn pow_hash(&self) -> [u8; 32] {
        pow_hash(&self.challenge, &self.nonce, &self.address.digest())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leading_zero_bits_counts_across_bytes() {
        let mut h = [0xffu8; 32];
        assert_eq!(leading_zero_bits(&h), 0);
        h[0] = 0;
        h[1] = 0x10;
        assert_eq!(leading_zero_bits(&h), 11);
        assert!(pow_meets(&h, 11));
        assert!(!pow_meets(&h, 12));
        assert_eq!(leading_zero_bits(&[0; 32]), 256);
        assert!(pow_meets(&[0; 32], 256));
        assert!(!pow_meets(&[0; 32], 257));
        assert!(pow_meets(&h, 0));
    }

    #[test]
    fn body_parses_exactly() {
        let request = RegisterRequest {
            challenge: [1; 32],
            nonce: [2; 8],
            address: Address {
                pk: [3; 32],
                ek: [4; 1184],
            },
        };
        let body = request.encode();
        assert_eq!(body.len(), 1256);
        assert_eq!(RegisterRequest::parse(&body), Ok(request));
        assert_eq!(
            RegisterRequest::parse(&body[..1255]),
            Err(RegistryError::BodyLength(1255))
        );
        let mut long = body.clone();
        long.push(0);
        assert_eq!(
            RegisterRequest::parse(&long),
            Err(RegistryError::BodyLength(1257))
        );
        assert_eq!(
            Address::from_bytes(&[0; 1215]),
            Err(RegistryError::AddressLength(1215))
        );
    }

    #[test]
    fn json_refuses_unknown_fields() {
        let json = format!(
            r#"{{"challenge":"{}","difficulty_bits":20,"expires_unix":1794670000}}"#,
            "ab".repeat(32)
        );
        let challenge: Challenge = serde_json::from_str(&json).unwrap();
        assert_eq!(challenge.difficulty_bits, 20);
        assert_eq!(serde_json::to_string(&challenge).unwrap(), json);
        let extra = json.replace('}', r#","extra":1}"#);
        assert!(serde_json::from_str::<Challenge>(&extra).is_err());

        let json = format!(
            r#"{{"chunk_id":12,"digest":"{}","created":true,"visible_after_sequence":4821}}"#,
            "cd".repeat(32)
        );
        let registration: Registration = serde_json::from_str(&json).unwrap();
        assert_eq!(registration.visible_after_sequence, 4821);
        assert_eq!(serde_json::to_string(&registration).unwrap(), json);
        assert!(serde_json::from_str::<Registration>(&json.replace('}', r#","x":0}"#)).is_err());
        assert!(serde_json::from_str::<Registration>(r#"{"chunk_id":12}"#).is_err());
    }

    #[test]
    fn solve_pow_meets_difficulty() {
        let nonce = solve_pow(&[1; 32], &[2; 32], 8).unwrap();
        assert!(pow_meets(&pow_hash(&[1; 32], &nonce, &[2; 32]), 8));
        assert_eq!(solve_pow(&[1; 32], &[2; 32], 257), None);
        assert_eq!(solve_pow(&[1; 32], &[2; 32], 0), Some([0; 8]));
    }
}
