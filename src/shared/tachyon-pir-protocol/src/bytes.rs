//! Small 32-byte value types and their JSON forms (spec 3, "Byte order").
//!
//! - [`BlockHash`]: a block hash held in protocol order and written in JSON in
//!   display order (reversed), the hex the node prints. Used for
//!   `genesis_hash` and `anchor_hash`.
//! - [`Bytes32`]: any other 32-byte value (table digests, session ids,
//!   challenges, address digests), written as plain hex, never reversed.
//!
//! Both accept only 64 lowercase hex digits, so every value has one encoding.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A string that is not 64 lowercase hex digits.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("expected 64 lowercase hex digits")]
pub struct HexError;

fn parse_hex32(text: &str) -> Result<[u8; 32], HexError> {
    if text.len() != 64 || !text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(HexError);
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(text, &mut out).map_err(|_| HexError)?;
    Ok(out)
}

/// A block hash in protocol order; JSON and [`fmt::Display`] use display order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BlockHash([u8; 32]);

impl BlockHash {
    /// The hash whose protocol-order bytes are `bytes`.
    pub const fn from_protocol_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Protocol-order bytes: the input to every hash in the spec.
    pub const fn protocol_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The hash the node prints as `displayed`.
    pub fn parse_display(displayed: &str) -> Result<Self, HexError> {
        let mut bytes = parse_hex32(displayed)?;
        bytes.reverse();
        Ok(Self(bytes))
    }

    /// Display-order hex, as the node prints it.
    pub fn display_hex(&self) -> String {
        let mut bytes = self.0;
        bytes.reverse();
        hex::encode(bytes)
    }
}

impl fmt::Display for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display_hex())
    }
}

impl fmt::Debug for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockHash({})", self.display_hex())
    }
}

impl FromStr for BlockHash {
    type Err = HexError;

    fn from_str(displayed: &str) -> Result<Self, Self::Err> {
        Self::parse_display(displayed)
    }
}

impl Serialize for BlockHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.display_hex())
    }
}

impl<'de> Deserialize<'de> for BlockHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse_display(&text).map_err(serde::de::Error::custom)
    }
}

/// A 32-byte value written as plain hex: a digest, session id or challenge.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Bytes32(pub [u8; 32]);

impl Bytes32 {
    /// The bytes.
    pub const fn bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hex.
    pub fn hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl From<[u8; 32]> for Bytes32 {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl fmt::Display for Bytes32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex())
    }
}

impl fmt::Debug for Bytes32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bytes32({})", self.hex())
    }
}

impl FromStr for Bytes32 {
    type Err = HexError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        parse_hex32(text).map(Self)
    }
}

impl Serialize for Bytes32 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.hex())
    }
}

impl<'de> Deserialize<'de> for Bytes32 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// The publisher's recovery epoch, as the map writes it: a JSON string holding
/// a canonical decimal `u64` (no sign, no leading zero), at least 1 because
/// `pir_publication` refuses a zero recovery epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecoveryEpoch(u64);

/// A recovery epoch that is zero or not a canonical decimal string.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("recovery_epoch must be a canonical decimal string of a positive u64")]
pub struct RecoveryEpochError;

impl RecoveryEpoch {
    /// `value`, if positive.
    pub const fn new(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// The epoch.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl FromStr for RecoveryEpoch {
    type Err = RecoveryEpochError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let canonical =
            !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) && !text.starts_with('0');
        if !canonical {
            return Err(RecoveryEpochError);
        }
        let value: u64 = text.parse().map_err(|_| RecoveryEpochError)?;
        Self::new(value).ok_or(RecoveryEpochError)
    }
}

impl fmt::Display for RecoveryEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for RecoveryEpoch {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for RecoveryEpoch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_hash_json_is_display_order() {
        let displayed = "05a60a92d99d85997cce3b87616c089f6124d7342af37106edc76126334a2c38";
        let hash: BlockHash = serde_json::from_str(&format!("\"{displayed}\"")).unwrap();
        assert_eq!(hash.protocol_bytes()[0], 0x38);
        assert_eq!(hash.protocol_bytes()[31], 0x05);
        assert_eq!(
            serde_json::to_string(&hash).unwrap(),
            format!("\"{displayed}\"")
        );
        assert_eq!(hash.to_string(), displayed);
    }

    #[test]
    fn bytes32_json_is_plain_hex() {
        let value = Bytes32(std::array::from_fn(|i| i as u8));
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, format!("\"{}\"", hex::encode(value.0)));
        assert_eq!(serde_json::from_str::<Bytes32>(&json).unwrap(), value);
    }

    #[test]
    fn hex_must_be_64_lowercase_digits() {
        let ok = "ab".repeat(32);
        assert!(ok.parse::<Bytes32>().is_ok());
        for bad in [
            "AB".repeat(32),
            "ab".repeat(31),
            "ab".repeat(33),
            format!("{}zz", "ab".repeat(31)),
        ] {
            assert!(bad.parse::<Bytes32>().is_err(), "{bad}");
            assert!(BlockHash::parse_display(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn recovery_epoch_is_a_canonical_positive_decimal_string() {
        let epoch: RecoveryEpoch = serde_json::from_str("\"3\"").unwrap();
        assert_eq!(epoch.get(), 3);
        assert_eq!(serde_json::to_string(&epoch).unwrap(), "\"3\"");
        let max: RecoveryEpoch = serde_json::from_str("\"18446744073709551615\"").unwrap();
        assert_eq!(max.get(), u64::MAX);
        for bad in [
            "3",
            "\"0\"",
            "\"03\"",
            "\"+3\"",
            "\"-1\"",
            "\"\"",
            "\"18446744073709551616\"",
            "\" 3\"",
        ] {
            assert!(serde_json::from_str::<RecoveryEpoch>(bad).is_err(), "{bad}");
        }
    }
}
