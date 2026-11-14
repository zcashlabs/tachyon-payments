//! Block and transaction hashes, held in protocol byte order.
//!
//! The node's RPC prints block hashes and transaction ids as hex of the
//! reversed bytes ("display order"). This crate, and every hash input of the
//! services, holds them in protocol order (spec section 3, "Byte order of
//! 32-byte values"), as `wallet-pir`'s `pir-chain/src/hash.rs` does. The
//! conversions here are the only ones between the two orders.
//!
//! Tachygrams, anchors, tachygram sets and aggregate ids are not hashes in this
//! sense: they are plain byte arrays decoded from the RPC hex left to right.
use std::fmt;
use std::str::FromStr;

/// A 32-byte block hash or transaction id in protocol byte order.
///
/// [`fmt::Display`] and [`FromStr`] use display order, the hex the node and the
/// explorer print.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Hash([u8; 32]);

/// A string that is not a display-order hash.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum HashParseError {
    #[error("hash is not hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("hash is {0} bytes, not 32")]
    Length(usize),
}

impl Hash {
    /// The all-zero hash: the `prev` of the genesis block.
    pub const ZERO: Self = Self([0; 32]);

    /// The hash whose protocol-order bytes are `bytes`.
    pub const fn from_protocol_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// This hash's protocol-order bytes.
    pub const fn protocol_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The hash the node prints as `displayed` (display order, 64 hex digits).
    pub fn parse_display(displayed: &str) -> Result<Self, HashParseError> {
        let mut bytes: [u8; 32] = hex::decode(displayed)?
            .try_into()
            .map_err(|bytes: Vec<u8>| HashParseError::Length(bytes.len()))?;
        bytes.reverse();
        Ok(Self(bytes))
    }

    /// The display-order hex of this hash, as the node prints it.
    pub fn display_hex(&self) -> String {
        let mut bytes = self.0;
        bytes.reverse();
        hex::encode(bytes)
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display_hex())
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({})", self.display_hex())
    }
}

impl FromStr for Hash {
    type Err = HashParseError;

    fn from_str(displayed: &str) -> Result<Self, Self::Err> {
        Self::parse_display(displayed)
    }
}

/// A block's height and hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BlockId {
    pub height: u32,
    pub hash: Hash,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Display order reverses protocol order.
    #[test]
    fn display_order_reverses_protocol_bytes() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xab;
        bytes[31] = 0x01;
        let hash = Hash::from_protocol_bytes(bytes);
        let displayed = hash.to_string();
        assert_eq!(displayed.len(), 64);
        assert!(displayed.starts_with("01") && displayed.ends_with("ab"));
        assert_eq!(displayed.parse::<Hash>().unwrap(), hash);
        assert_eq!(format!("{hash:?}"), format!("Hash({displayed})"));
        assert_eq!(hash.protocol_bytes(), &bytes);
    }

    #[test]
    fn malformed_hashes_are_refused() {
        let good = "01".repeat(32);
        assert!(matches!(
            Hash::parse_display(&good[2..]),
            Err(HashParseError::Length(31))
        ));
        assert!(matches!(
            Hash::parse_display(&good[1..]),
            Err(HashParseError::Hex(hex::FromHexError::OddLength))
        ));
        assert!(matches!(
            Hash::parse_display(&format!("{good}00")),
            Err(HashParseError::Length(33))
        ));
        assert!(matches!(
            Hash::parse_display(&format!("zz{}", &good[2..])),
            Err(HashParseError::Hex(_))
        ));
        assert!(matches!(
            Hash::parse_display(""),
            Err(HashParseError::Length(0))
        ));
    }
}
