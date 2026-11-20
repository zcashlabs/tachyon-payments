//! Product registration: one `PublisherDefinition` per enabled, compiled
//! product. Building a definition only validates settings and declares
//! resources; product state is opened when the host starts it.
//!
//! Adding a product is: an optional dependency behind its feature in
//! `Cargo.toml`, removing its `compile_error!` in `lib.rs`, and one block
//! below. Memo and first-contact register as **one** definition, named
//! `memo`, on the memo listener (spec 11.7), so the memo block reads both
//! sections.
//!
//! The registry follows no chain, but its definition is bound to the node's
//! network identity (spec 9.2), which is read only at start-up: `--check`
//! validates its settings and resources under a stand-in identity, and
//! `Plan::into_definitions` builds the definition that is run.
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
#[cfg(any(feature = "memo", feature = "tachygram", feature = "registry"))]
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

/// The enabled registry's settings, checked offline (whenever the section is
/// present, enabled or not); `None` when it is absent or disabled.
#[cfg(feature = "registry")]
pub(crate) fn registry_settings(
    config: &Config,
) -> Result<Option<registry_publisher::RegistrySettings>> {
    let settings: Option<registry_publisher::RegistrySettings> =
        section_settings(config, crate::Product::Registry)?;
    if let Some(settings) = &settings {
        registry_for_check(settings.clone())?;
    }
    Ok(settings)
}

/// The registry identity of the node's network: its `chain` name and genesis
/// hash in protocol order (spec 9.2).
#[cfg(feature = "registry")]
pub(crate) fn registry_identity(
    network: &tachyon_chain::NetworkIdentity,
) -> registry_publisher::RegistryIdentity {
    registry_publisher::RegistryIdentity::new(
        network.chain.clone(),
        tachyon_pir_protocol::BlockHash::from_protocol_bytes(
            *network.genesis_hash.protocol_bytes(),
        ),
    )
}

/// The registry definition bound to `identity`. It does not follow the chain.
#[cfg(feature = "registry")]
pub(crate) fn registry(
    settings: registry_publisher::RegistrySettings,
    identity: registry_publisher::RegistryIdentity,
) -> Result<PublisherDefinition<axum::Router>> {
    let config = registry_publisher::RegistryConfig::new(settings, identity);
    config
        .validate()
        .map_err(|error| format!("registry: {error}"))?;
    let definition = registry_publisher::configure(config, pir_pipeline::engine::PublisherEngine)
        .map_err(|error| format!("registry: {error}"))?;
    Ok(definition)
}

/// The registry definition under a stand-in identity, for the static checks
/// only (settings and declared resources); it is never handed to the host.
/// `RegistryConfig::validate`'s one identity rule, a named chain, holds for
/// every identity `NodeClient::network_identity` returns.
#[cfg(feature = "registry")]
pub(crate) fn registry_for_check(
    settings: registry_publisher::RegistrySettings,
) -> Result<PublisherDefinition<axum::Router>> {
    let unchecked = registry_publisher::RegistryIdentity::new(
        "read-from-the-node-at-start-up",
        tachyon_pir_protocol::BlockHash::from_protocol_bytes([0; 32]),
    );
    registry(settings, unchecked)
}
