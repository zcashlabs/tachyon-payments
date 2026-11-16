//! The tachygram serving module (spec 8): constants only. Tachygram rows are
//! validated with the scope's height range, which the shared core takes from
//! the map's `Scope` through `ScopeTables::row_context`.

use crate::product::{Endpoints, MapFamily, ProductModule};
use tachyon_pir_protocol::Product;

pub const MODULE: ProductModule = ProductModule {
    product: Product::Tachygram,
    section: "tachygram",
    feature: "tachygram",
    metric_prefix: "tachyon_tachygram_",
    endpoints: Endpoints {
        map: "tachygram_map",
        map_chunk: "tachygram_map_chunk",
        public: "tachygram_public",
        query: "tachygram_query",
    },
    default_retain_scopes: Some(64),
    map: MapFamily::Chain,
};
