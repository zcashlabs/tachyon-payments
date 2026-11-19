//! The registry serving module (spec 9, 10): constants only. Lookups only;
//! the write API is the publisher's (spec 9.3). Scopes are chunks with
//! `scope_id = scope index = chunk_id`, one bucket each; rows are validated
//! with every `key16` equal to its address digest prefix (spec 8.4).

use crate::product::{Endpoints, MapFamily, ProductModule};
use tachyon_pir_protocol::Product;

pub const MODULE: ProductModule = ProductModule {
    product: Product::Registry,
    section: "registry",
    feature: "registry",
    metric_prefix: "tachyon_registry_",
    endpoints: Endpoints {
        map: "registry_map",
        map_chunk: "registry_map_chunk",
        public: "registry_public",
        query: "registry_query",
    },
    // Spec 11.5: the registry retains every chunk by default.
    default_retain_scopes: None,
    map: MapFamily::Registry,
};
