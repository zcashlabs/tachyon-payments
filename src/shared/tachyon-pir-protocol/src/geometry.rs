//! Table geometries (spec 6.1) and their exact P8 message sizes (spec 6.6).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zakura_pir_protocol::native::P8;

use crate::constants::{HEADER_BYTES, ROW_BYTES};

/// Plaintext coefficients per row: one byte each under P8.
pub const COLS: usize = ROW_BYTES / P8.entry_bytes();

/// A table shape. Only these two exist in version 1; only
/// [`crate::PUBLISHED_GEOMETRY`] is published.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Geometry {
    /// 4,096 rows of 8,192 bytes.
    Tachyon4k8,
    /// 2,048 rows of 8,192 bytes; defined, not published.
    Tachyon2k8,
}

/// A string that names no geometry.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown geometry {0:?}")]
pub struct UnknownGeometry(pub String);

impl Geometry {
    /// Both geometries.
    pub const ALL: [Geometry; 2] = [Geometry::Tachyon4k8, Geometry::Tachyon2k8];

    /// The geometry's name, as written in the map and hashed into seeds.
    pub const fn name(self) -> &'static str {
        match self {
            Geometry::Tachyon4k8 => "tachyon-4k8",
            Geometry::Tachyon2k8 => "tachyon-2k8",
        }
    }

    /// Rows per table.
    pub const fn rows(self) -> u32 {
        match self {
            Geometry::Tachyon4k8 => 4096,
            Geometry::Tachyon2k8 => 2048,
        }
    }

    /// Bytes of all rows of one table.
    pub const fn table_bytes(self) -> usize {
        self.rows() as usize * ROW_BYTES
    }

    /// The single accepted query payload length (after the header),
    /// `P8.request_len_bits(rows, P8.dithered_bits)`.
    pub const fn payload_len(self) -> usize {
        P8.request_len_bits(self.rows() as usize, P8.dithered_bits)
    }

    /// Exact request body: header and payload.
    pub const fn request_len(self) -> usize {
        HEADER_BYTES + self.payload_len()
    }

    /// Packed reply after the echoed header, `P8.response_len(8192)`.
    pub const fn reply_len(self) -> usize {
        P8.response_len(COLS)
    }

    /// Exact response body: echoed header and reply.
    pub const fn response_len(self) -> usize {
        HEADER_BYTES + self.reply_len()
    }

    /// Public setup material of one table, `P8.public_len(8192)`.
    pub const fn public_len(self) -> usize {
        P8.public_len(COLS)
    }

    /// The geometry named `name`; `None` for any other string.
    pub fn from_name(name: &str) -> Option<Geometry> {
        Geometry::ALL.into_iter().find(|g| g.name() == name)
    }
}

impl fmt::Display for Geometry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Geometry {
    type Err = UnknownGeometry;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Geometry::from_name(name).ok_or_else(|| UnknownGeometry(name.to_string()))
    }
}

impl Serialize for Geometry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for Geometry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        name.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_match_spec_6_6() {
        let g = Geometry::Tachyon4k8;
        assert_eq!(g.payload_len(), 28_160);
        assert_eq!(g.request_len(), 28_212);
        assert_eq!(g.reply_len(), 10_240);
        assert_eq!(g.response_len(), 10_292);
        assert_eq!(g.public_len(), 45_056);
        assert_eq!(g.table_bytes(), 32 << 20);
        let g = Geometry::Tachyon2k8;
        assert_eq!(g.payload_len(), 20_992);
        assert_eq!(g.request_len(), 21_044);
        assert_eq!(g.reply_len(), 10_240);
        assert_eq!(g.response_len(), 10_292);
        assert_eq!(g.public_len(), 45_056);
        assert_eq!(g.table_bytes(), 16 << 20);
        assert_eq!(COLS, 8192);
    }

    #[test]
    fn names_parse_and_others_are_refused() {
        for g in Geometry::ALL {
            assert_eq!(g.name().parse::<Geometry>(), Ok(g));
            assert_eq!(g.to_string(), g.name());
            let json = serde_json::to_string(&g).unwrap();
            assert_eq!(serde_json::from_str::<Geometry>(&json).unwrap(), g);
        }
        for bad in ["txid-4k8", "tachyon-8k8", "Tachyon-4k8", "", "tachyon-4k8 "] {
            assert!(bad.parse::<Geometry>().is_err(), "{bad:?}");
            assert!(serde_json::from_str::<Geometry>(&format!("{bad:?}")).is_err());
        }
        assert_eq!(crate::PUBLISHED_GEOMETRY, Geometry::Tachyon4k8);
    }
}
