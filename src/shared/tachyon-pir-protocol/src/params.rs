//! PIR parameters per (product, geometry): public seeds, query masks and the
//! packing setup (spec 6.6).

use std::fmt;

use sha2::{Digest, Sha256};
use zakura_pir_protocol::native::{NativeSetup, P8};

use crate::geometry::{COLS, Geometry};
use crate::product::Product;

/// Purpose of a public PIR seed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SeedPurpose {
    /// Seeds `P8.public_query_masks`.
    QueryMasks,
    /// Seeds `NativeSetup::new`.
    PackingSetup,
}

impl SeedPurpose {
    /// The purpose string hashed into the seed.
    pub const fn name(self) -> &'static str {
        match self {
            SeedPurpose::QueryMasks => "query-masks",
            SeedPurpose::PackingSetup => "packing-setup",
        }
    }
}

/// `SHA-256("tachyon-pir/v1/native-p8/" || product || "\0" || geometry || "\0" || purpose)`.
pub fn seed(product: Product, geometry: Geometry, purpose: SeedPurpose) -> [u8; 32] {
    Sha256::new()
        .chain_update(b"tachyon-pir/v1/native-p8/")
        .chain_update(product.name())
        .chain_update(b"\0")
        .chain_update(geometry.name())
        .chain_update(b"\0")
        .chain_update(purpose.name())
        .finalize()
        .into()
}

/// The data-independent P8 parameters both sides derive for one product and
/// geometry. Deriving them takes well under a millisecond; a client derives
/// them once per product and geometry.
pub struct PirParams {
    product: Product,
    geometry: Geometry,
    masks: Vec<Vec<u64>>,
    setup: NativeSetup,
}

impl PirParams {
    /// `masks = P8.public_query_masks(seed("query-masks"), rows, 8192)` and
    /// `setup = NativeSetup::new(P8.params(), seed("packing-setup"))`.
    pub fn new(product: Product, geometry: Geometry) -> Self {
        let masks = P8
            .public_query_masks(
                seed(product, geometry, SeedPurpose::QueryMasks),
                geometry.rows() as usize,
                COLS,
            )
            // Both geometries are whole multiples of the ring degree in rows
            // and columns and far below the size bound, so this cannot fail;
            // the unit tests derive every (product, geometry).
            .expect("both geometries are valid P8 shapes");
        let setup = NativeSetup::new(
            P8.params(),
            seed(product, geometry, SeedPurpose::PackingSetup),
        );
        Self {
            product,
            geometry,
            masks,
            setup,
        }
    }

    /// The product.
    pub fn product(&self) -> Product {
        self.product
    }

    /// The geometry.
    pub fn geometry(&self) -> Geometry {
        self.geometry
    }

    /// Rows per table.
    pub fn rows(&self) -> usize {
        self.geometry.rows() as usize
    }

    /// Plaintext columns per row (8,192).
    pub fn cols(&self) -> usize {
        COLS
    }

    /// The public query masks, `rows / 2048` vectors of 2,048 words.
    pub fn masks(&self) -> &[Vec<u64>] {
        &self.masks
    }

    /// The packing setup.
    pub fn setup(&self) -> &NativeSetup {
        &self.setup
    }

    /// Exact query payload length after the header.
    pub fn payload_len(&self) -> usize {
        self.geometry.payload_len()
    }

    /// Exact reply length after the echoed header.
    pub fn reply_len(&self) -> usize {
        self.geometry.reply_len()
    }

    /// Exact public setup length.
    pub fn public_len(&self) -> usize {
        self.geometry.public_len()
    }
}

impl fmt::Debug for PirParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PirParams")
            .field("product", &self.product)
            .field("geometry", &self.geometry)
            .field("setup_id", &hex::encode(self.setup.id()))
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::PROFILE;
    use zakura_pir_protocol::native::{D, Q, Q_BITS};

    /// The profile name, rebuilt from P8's fields: p, gadget bits x digits,
    /// mask bits, dithered query bits, response bits.
    #[test]
    fn profile_string_names_p8() {
        assert_eq!(
            P8.nearest_bits, None,
            "two-mask P8 has only a dithered width"
        );
        let name = format!(
            "reinspiring-two-mask-p{}-g{}x{}-m{}-dq{}-r{}-v1",
            P8.p_bits, P8.gadget_bits, P8.ell, P8.mask_bits, P8.dithered_bits, P8.response_bits
        );
        assert_eq!(name, PROFILE);
        assert_eq!(D, 2048);
        assert_eq!(Q, 1 << 54);
        assert_eq!(Q_BITS, 54);
        assert_eq!(P8.entry_bytes(), 1);
    }

    #[test]
    fn sizes_match_spec_6_6() {
        let p = PirParams::new(Product::Memo, Geometry::Tachyon4k8);
        assert_eq!(p.payload_len(), 28_160);
        assert_eq!(p.reply_len(), 10_240);
        assert_eq!(p.public_len(), 45_056);
        assert_eq!(p.payload_len(), P8.request_len_bits(4096, 28));
        let p = PirParams::new(Product::Memo, Geometry::Tachyon2k8);
        assert_eq!(p.payload_len(), 20_992);
        assert_eq!(p.reply_len(), 10_240);
        assert_eq!(p.public_len(), 45_056);
    }

    #[test]
    fn every_product_and_geometry_derives_distinct_parameters() {
        let mut ids = std::collections::HashSet::new();
        let mut masks = std::collections::HashSet::new();
        for product in Product::ALL {
            for geometry in Geometry::ALL {
                let p = PirParams::new(product, geometry);
                assert_eq!(p.masks().len(), geometry.rows() as usize / D);
                assert!(
                    p.masks()
                        .iter()
                        .all(|m| m.len() == D && m.iter().all(|w| *w < Q))
                );
                assert!(ids.insert(p.setup().id()));
                assert!(masks.insert(p.masks()[0][..8].to_vec()));
                // Deterministic: a second derivation is identical.
                let again = PirParams::new(product, geometry);
                assert_eq!(again.setup().id(), p.setup().id());
                assert_eq!(again.masks(), p.masks());
            }
        }
    }
}
