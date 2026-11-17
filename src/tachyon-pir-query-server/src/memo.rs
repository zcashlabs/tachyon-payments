//! The memo serving module (spec 7.1): constants only. Memo scopes are
//! epochs; keys may repeat (spec 5.3), which row validation allows for this
//! product (non-decreasing keys), and overflow segments are listed in the map.

use crate::product::{Endpoints, MapFamily, ProductModule};
use tachyon_pir_protocol::Product;

pub const MODULE: ProductModule = ProductModule {
    product: Product::Memo,
    section: "memo",
    feature: "memo",
    metric_prefix: "tachyon_memo_",
    endpoints: Endpoints {
        map: "memo_map",
        map_chunk: "memo_map_chunk",
        public: "memo_public",
        query: "memo_query",
    },
    default_retain_scopes: Some(64),
    map: MapFamily::Chain,
};
