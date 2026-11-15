//! The four PIR products and their per-product wire constants.

use std::fmt;
use std::str::FromStr;

use crate::constants::{KEM_CT_BYTES, KEM_EK_BYTES, MEMO_CT_BYTES, PK_BYTES, ROW_BYTES};

/// One PIR product (spec 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Product {
    /// The epoched memo database, `tag => memo`.
    Memo,
    /// The first-contact memo database, `tag => KEM.c || memo`.
    FirstContact,
    /// The epoched tachygram database.
    Tachygram,
    /// The off-chain address registry, `H(addr) => addr`.
    Registry,
}

/// A string that names no product.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown product {0:?}")]
pub struct UnknownProduct(pub String);

impl Product {
    /// Every product, in spec order.
    pub const ALL: [Product; 4] = [
        Product::Memo,
        Product::FirstContact,
        Product::Tachygram,
        Product::Registry,
    ];

    /// The product name used in seeds, paths and configuration.
    pub const fn name(self) -> &'static str {
        match self {
            Product::Memo => "memo",
            Product::FirstContact => "first-contact",
            Product::Tachygram => "tachygram",
            Product::Registry => "registry",
        }
    }

    /// The map's `protocol` string (spec 10.3), also hashed into session ids.
    pub const fn protocol(self) -> &'static str {
        match self {
            Product::Memo => "tachyon-memo-pir-v1",
            Product::FirstContact => "tachyon-first-contact-pir-v1",
            Product::Tachygram => "tachyon-tachygram-pir-v1",
            Product::Registry => "tachyon-registry-pir-v1",
        }
    }

    /// Placement hash domain, trailing NUL included (spec 6.3).
    pub const fn placement_domain(self) -> &'static [u8] {
        match self {
            Product::Memo => b"tachyon-pir/memo/v1/place\0",
            Product::FirstContact => b"tachyon-pir/first-contact/v1/place\0",
            Product::Tachygram => b"tachyon-pir/tachygram/v1/place\0",
            Product::Registry => b"tachyon-pir/registry/v1/place\0",
        }
    }

    /// Query header magic (spec 10.4).
    pub const fn query_magic(self) -> [u8; 4] {
        match self {
            Product::Memo => *b"TMQ1",
            Product::FirstContact => *b"TFQ1",
            Product::Tachygram => *b"TGQ1",
            Product::Registry => *b"TRQ1",
        }
    }

    /// Canonical public mount (spec 10.1).
    pub const fn mount(self) -> &'static str {
        match self {
            Product::Memo => "/memo/v1",
            Product::FirstContact => "/first-contact/v1",
            Product::Tachygram => "/tachygram/v1",
            Product::Registry => "/registry/v1",
        }
    }

    /// Legacy alias of the public mount (spec 10.1).
    pub const fn legacy_mount(self) -> &'static str {
        match self {
            Product::Memo => "/v1/memo",
            Product::FirstContact => "/v1/first-contact",
            Product::Tachygram => "/v1/tachygram",
            Product::Registry => "/v1/registry",
        }
    }

    /// Publisher-to-query-host distribution prefix, trailing slash included
    /// (spec 11.3).
    pub const fn distribution_prefix(self) -> &'static str {
        match self {
            Product::Memo => "/provider/memo/v1/",
            Product::FirstContact => "/provider/first-contact/v1/",
            Product::Tachygram => "/provider/tachygram/v1/",
            Product::Registry => "/provider/registry/v1/",
        }
    }

    /// Distribution schema string, `tachyon-<product>-prepared-distribution-v1`
    /// (spec 11.3).
    pub const fn distribution_schema(self) -> &'static str {
        match self {
            Product::Memo => "tachyon-memo-prepared-distribution-v1",
            Product::FirstContact => "tachyon-first-contact-prepared-distribution-v1",
            Product::Tachygram => "tachyon-tachygram-prepared-distribution-v1",
            Product::Registry => "tachyon-registry-prepared-distribution-v1",
        }
    }

    /// Bytes of the stored key: `key16`, or the full tachygram (spec 6.3).
    pub const fn key_len(self) -> usize {
        match self {
            Product::Tachygram => 32,
            _ => 16,
        }
    }

    /// Bytes of one entry (spec 6.2).
    pub const fn entry_bytes(self) -> usize {
        match self {
            Product::Memo => 16 + MEMO_CT_BYTES,
            Product::FirstContact => 16 + KEM_CT_BYTES + MEMO_CT_BYTES,
            Product::Tachygram => 32 + 4 + 4,
            Product::Registry => 16 + PK_BYTES + KEM_EK_BYTES,
        }
    }

    /// Entries per row, `floor(ROW_BYTES / ENTRY_BYTES)`.
    pub const fn slots(self) -> usize {
        ROW_BYTES / self.entry_bytes()
    }

    /// Target mean entries per row, which sizes `n_buckets` (spec 6.2).
    pub const fn target_load(self) -> u64 {
        match self {
            Product::Memo => 34,
            Product::FirstContact => 1,
            Product::Tachygram => 124,
            Product::Registry => 1,
        }
    }

    /// Whether the product's scopes are built from the chain. Registry is not:
    /// its map has no heights or anchor.
    pub const fn is_chain(self) -> bool {
        !matches!(self, Product::Registry)
    }

    /// Whether a scope's index equals its `scope_id` (spec 10.3): true for all
    /// but first-contact, whose index is the span's position.
    pub const fn index_is_scope_id(self) -> bool {
        !matches!(self, Product::FirstContact)
    }

    /// The product whose `protocol` string is `protocol`.
    pub fn from_protocol(protocol: &str) -> Option<Product> {
        Product::ALL.into_iter().find(|p| p.protocol() == protocol)
    }

    /// The leading `key_len` bytes of a 32-byte tag, digest or tachygram: the
    /// key a lookup uses (spec 6.3).
    pub fn key_of(self, value: &[u8; 32]) -> &[u8] {
        &value[..self.key_len()]
    }
}

impl fmt::Display for Product {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Product {
    type Err = UnknownProduct;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Product::ALL
            .into_iter()
            .find(|p| p.name() == name)
            .ok_or_else(|| UnknownProduct(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_of_spec_6_2() {
        let expect = [
            (Product::Memo, 144, 56, 34, 139_264),
            (Product::FirstContact, 1232, 6, 1, 4_096),
            (Product::Tachygram, 40, 204, 124, 507_904),
            (Product::Registry, 1232, 6, 1, 4_096),
        ];
        for (product, entry, slots, target, per_table) in expect {
            assert_eq!(product.entry_bytes(), entry, "{product}");
            assert_eq!(product.slots(), slots, "{product}");
            assert_eq!(product.target_load(), target, "{product}");
            assert_eq!(product.target_load() * 4096, per_table, "{product}");
        }
    }

    #[test]
    fn names_and_strings_round_trip() {
        for product in Product::ALL {
            assert_eq!(product.name().parse::<Product>(), Ok(product));
            assert_eq!(Product::from_protocol(product.protocol()), Some(product));
            let name = product.name();
            assert_eq!(product.protocol(), format!("tachyon-{name}-pir-v1"));
            assert_eq!(
                product.placement_domain(),
                format!("tachyon-pir/{name}/v1/place\0").as_bytes()
            );
            assert_eq!(product.mount(), format!("/{name}/v1"));
            assert_eq!(product.legacy_mount(), format!("/v1/{name}"));
            assert_eq!(
                product.distribution_prefix(),
                format!("/provider/{name}/v1/")
            );
            assert_eq!(
                product.distribution_schema(),
                format!("tachyon-{name}-prepared-distribution-v1")
            );
        }
        assert!("first_contact".parse::<Product>().is_err());
        assert_eq!(Product::from_protocol("tachyon-memo-pir-v2"), None);
        let magics: Vec<_> = Product::ALL.iter().map(|p| p.query_magic()).collect();
        assert_eq!(magics, [*b"TMQ1", *b"TFQ1", *b"TGQ1", *b"TRQ1"]);
    }

    #[test]
    fn key_of_truncates_to_key16_except_tachygram() {
        let value: [u8; 32] = std::array::from_fn(|i| i as u8);
        assert_eq!(Product::Memo.key_of(&value), &value[..16]);
        assert_eq!(Product::Registry.key_of(&value), &value[..16]);
        assert_eq!(Product::Tachygram.key_of(&value), &value[..]);
    }
}
