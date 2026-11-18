//! Product registration: one `PublisherDefinition` per enabled, compiled
//! product. Building a definition only validates settings and declares
//! resources; product state is opened when the host starts it.
//!
//! Adding a product is: an optional dependency behind its feature in
//! `Cargo.toml`, removing its `compile_error!` in `lib.rs`, and one block
//! below. Memo and first-contact register as **one** definition, named
//! `memo`, on the memo listener (spec 11.7), so the memo block reads both
//! sections.
use crate::{ChainPlan, Config, Result};
use pir_pipeline::publisher::PublisherDefinition;

#[cfg_attr(
    not(any(feature = "memo", feature = "tachygram")),
    allow(unused_variables)
)]
pub(crate) fn definitions(
    config: &Config,
    chain: Option<&ChainPlan>,
) -> Result<Vec<PublisherDefinition<axum::Router>>> {
    #[cfg_attr(not(any(feature = "memo", feature = "tachygram")), allow(unused_mut))]
    let mut publishers = Vec::new();
    #[cfg(feature = "memo")]
    if let Some(definition) = memo(config, chain)? {
        publishers.push(definition);
    }
    #[cfg(feature = "tachygram")]
    if let Some(definition) = tachygram(config, chain)? {
        publishers.push(definition);
    }
    Ok(publishers)
}

/// The settings of `product`'s section, checked whenever present; `None`
/// when the section is absent or disabled.
#[cfg(any(feature = "memo", feature = "tachygram"))]
fn section_settings<T: serde::de::DeserializeOwned>(
    config: &Config,
    product: crate::Product,
) -> Result<Option<T>> {
    match config.section(product) {
        Some(section) => crate::config::settings(product, section),
        None => Ok(None),
    }
}

/// Memo, with first-contact when its section is enabled (spec 11.7).
#[cfg(feature = "memo")]
fn memo(
    config: &Config,
    chain: Option<&ChainPlan>,
) -> Result<Option<PublisherDefinition<axum::Router>>> {
    use crate::Product;
    use memo_publisher::{MemoConfig, MemoSettings};
    let settings = section_settings::<MemoSettings>(config, Product::Memo)?;
    #[cfg(feature = "first-contact")]
    let first_contact =
        section_settings::<memo_publisher::FirstContactSettings>(config, Product::FirstContact)?;
    let Some(settings) = settings else {
        return Ok(None);
    };
    let chain = chain.ok_or("memo is enabled but there is no chain section")?;
    #[cfg_attr(not(feature = "first-contact"), allow(unused_mut))]
    let mut product = MemoConfig::new(settings, chain.epoch_length);
    #[cfg(feature = "first-contact")]
    if let Some(first_contact) = first_contact {
        product = product.with_first_contact(first_contact);
    }
    product
        .validate()
        .map_err(|error| format!("memo: {error}"))?;
    let definition = memo_publisher::configure(
        product,
        Some(&chain.settings),
        pir_pipeline::engine::PublisherEngine,
    )
    .map_err(|error| format!("memo: {error}"))?;
    Ok(Some(definition))
}

#[cfg(feature = "tachygram")]
fn tachygram(
    config: &Config,
    chain: Option<&ChainPlan>,
) -> Result<Option<PublisherDefinition<axum::Router>>> {
    use tachygram_publisher::{TachygramConfig, TachygramSettings};
    let Some(settings) = section_settings::<TachygramSettings>(config, crate::Product::Tachygram)?
    else {
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
