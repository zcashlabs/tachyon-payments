//! Product registration: one `PublisherDefinition` per enabled, compiled
//! product. Building a definition only validates settings and declares
//! resources; product state is opened when the host starts it.
//!
//! Adding a product is: an optional dependency behind its feature in
//! `Cargo.toml`, removing its `compile_error!` in `lib.rs`, and one block
//! below. Memo and first-contact register as **one** definition on the memo
//! listener (spec 11.7), so the memo block reads both sections.
use crate::{ChainPlan, Config, Result};
use pir_pipeline::publisher::PublisherDefinition;

#[cfg_attr(not(feature = "tachygram"), allow(unused_variables))]
pub(crate) fn definitions(
    config: &Config,
    chain: Option<&ChainPlan>,
) -> Result<Vec<PublisherDefinition<axum::Router>>> {
    #[cfg_attr(not(feature = "tachygram"), allow(unused_mut))]
    let mut publishers = Vec::new();
    #[cfg(feature = "tachygram")]
    if let Some(definition) = tachygram(config, chain)? {
        publishers.push(definition);
    }
    Ok(publishers)
}

#[cfg(feature = "tachygram")]
fn tachygram(
    config: &Config,
    chain: Option<&ChainPlan>,
) -> Result<Option<PublisherDefinition<axum::Router>>> {
    use crate::config::{Product, settings};
    use tachygram_publisher::{TachygramConfig, TachygramSettings};
    let Some(section) = config.section(Product::Tachygram) else {
        return Ok(None);
    };
    let Some(settings) = settings::<TachygramSettings>(Product::Tachygram, section)? else {
        return Ok(None);
    };
    let chain = chain.ok_or("tachygram is enabled but there is no chain section")?;
    let product = TachygramConfig::new(settings, chain.epoch_length);
    product
        .validate()
        .map_err(|error| format!("tachygram: {error}"))?;
    let definition = tachygram_publisher::configure(
        product,
        Some(&chain.settings),
        pir_pipeline::engine::PublisherEngine,
    )
    .map_err(|error| format!("tachygram: {error}"))?;
    Ok(Some(definition))
}
