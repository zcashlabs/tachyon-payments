//! The first-contact serving module (spec 7.2): constants only. Scopes are
//! spans of whole epochs: `scope_id` is the span's first epoch and the scope
//! index is the span's position, which the shared core uses for the floor
//! and retention (spec 10.3, 11.5). Independent of the memo module: a
//! separate mount and a separate distribution prefix.

use crate::product::{Endpoints, MapFamily, ProductModule};
use tachyon_pir_protocol::Product;

pub const MODULE: ProductModule = ProductModule {
    product: Product::FirstContact,
    section: "first_contact",
    feature: "first-contact",
    metric_prefix: "tachyon_first_contact_",
    endpoints: Endpoints {
        map: "first_contact_map",
        map_chunk: "first_contact_map_chunk",
        public: "first_contact_public",
        query: "first_contact_query",
    },
    // Spec 11.5: first-contact retains every span by default.
    default_retain_scopes: None,
    map: MapFamily::Chain,
};
