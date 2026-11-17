//! What one product's serving module supplies to the shared serving core:
//! constants only. Everything that differs between products on the wire
//! (mounts, magic, protocol string, entry layout, placement) is already a
//! constant of `tachyon_pir_protocol::Product`; a module adds its
//! configuration section, metric names and fixed endpoint labels, its
//! default retention, and the map family it publishes.

use pir_control::public_routes::PublicMount;
use tachyon_pir_protocol::Product;

/// The shape of a product's map (spec 10.3). The chain products (memo,
/// first-contact, tachygram) share one map type; the registry's map is a
/// variant without heights or anchor, added with the registry module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapFamily {
    /// `ChainMapHead` with `Scope` objects.
    Chain,
}

/// The fixed metric labels of one product's four public routes. Labels are
/// constants: no path segment or client byte ever becomes one (spec 10.4).
#[derive(Clone, Copy, Debug)]
pub struct Endpoints {
    pub map: &'static str,
    pub map_chunk: &'static str,
    pub public: &'static str,
    pub query: &'static str,
}

impl Endpoints {
    pub const fn all(&self) -> [&'static str; 4] {
        [self.map, self.map_chunk, self.public, self.query]
    }
}

/// One compiled product serving module.
#[derive(Debug)]
pub struct ProductModule {
    pub product: Product,
    /// The configuration section name, as in the publisher's configuration.
    pub section: &'static str,
    /// The Cargo feature that compiles the module.
    pub feature: &'static str,
    /// Prefix of the product's metrics, `tachyon_<product>_` (spec 11.8).
    pub metric_prefix: &'static str,
    pub endpoints: Endpoints,
    /// `retain_scopes` when the configuration gives none (spec 11.5); `None`
    /// serves every scope.
    pub default_retain_scopes: Option<u64>,
    pub map: MapFamily,
}

impl ProductModule {
    /// The canonical and legacy public mounts (spec 10.1).
    pub const fn mount(&self) -> PublicMount {
        PublicMount::new(self.product.mount(), self.product.legacy_mount())
    }

    /// The distribution contract's name for this product.
    pub fn distribution(&self) -> tachyon_pir_distribution::Product {
        tachyon_pir_distribution::Product::from_name(self.product.name())
            .expect("both crates name the same four products")
    }

    /// The metric label of a public request path, if it is one of this
    /// product's routes.
    pub fn endpoint(&self, path: &str) -> Option<&'static str> {
        let operation = self.mount().operation(path)?;
        if operation == "/map" {
            Some(self.endpoints.map)
        } else if operation.starts_with("/map/") {
            Some(self.endpoints.map_chunk)
        } else if operation.starts_with("/public/") {
            Some(self.endpoints.public)
        } else if operation == "/query" {
            Some(self.endpoints.query)
        } else {
            None
        }
    }
}

/// Every configuration section a query host may name, compiled or not, with
/// the product it configures (spec 11.7).
pub const SECTIONS: [(&str, Product, &str); 4] = [
    ("memo", Product::Memo, "memo"),
    ("first_contact", Product::FirstContact, "first-contact"),
    ("tachygram", Product::Tachygram, "tachygram"),
    ("registry", Product::Registry, "registry"),
];

/// The serving modules compiled into this build.
pub const MODULES: &[&ProductModule] = &[
    #[cfg(feature = "memo")]
    &crate::memo::MODULE,
    #[cfg(feature = "first-contact")]
    &crate::first_contact::MODULE,
    #[cfg(feature = "tachygram")]
    &crate::tachygram::MODULE,
];

/// The compiled serving module of `product`.
pub fn module(product: Product) -> Option<&'static ProductModule> {
    MODULES.iter().copied().find(|m| m.product == product)
}
