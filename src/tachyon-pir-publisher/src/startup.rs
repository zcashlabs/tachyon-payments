//! The start-up sequence: static plan, one network-identity check, then the
//! host until shutdown.
use crate::{ChainPlan, Config, Result, Shutdown};
use tachyon_chain::{
    Credentials, DecodeOptions, IdentityError, NetworkIdentity, NodeClient, NodeError,
};

/// Run the publisher described by `config` until `shutdown` is requested.
///
/// Refuses to start (returning an error before any listener is bound) when a
/// static rule fails, when the node cannot be read, or when it lists no
/// `NuTachyon` upgrade (spec 11.1). A shutdown requested during the identity
/// check returns `Ok` without starting the host.
pub async fn run(config: Config, shutdown: Shutdown) -> Result<()> {
    let plan = config.plan()?;
    if let Some(chain) = &plan.chain {
        let identity = tokio::select! {
            identity = check_network(chain) => identity?,
            () = shutdown.cancelled() => return Ok(()),
        };
        tracing::info!(
            chain = %identity.chain,
            genesis = %identity.genesis_hash.display_hex(),
            activation_height = identity.activation_height,
            epoch_length = chain.epoch_length,
            "node network identity"
        );
    }
    tracing::info!(
        management = %plan.management_listen,
        products = ?plan.product_names(),
        "starting publisher host"
    );
    pir_publisher::run_registered(
        plan.management_listen,
        plan.publishers,
        plan.chain.map(|chain| chain.settings),
        shutdown,
    )
    .await
    .map_err(|error| format!("publisher host: {error}").into())
}

/// Read the node's network identity once: `getblockchaininfo` and
/// `getblockhash 0`. An absent `NuTachyon` upgrade is an error.
pub async fn check_network(chain: &ChainPlan) -> Result<NetworkIdentity> {
    let url = &chain.settings.rpc_url;
    let credentials = match &chain.settings.rpc_cookie {
        Some(path) => Credentials::CookieFile(path.clone()),
        None => Credentials::None,
    };
    let node = NodeClient::connect(url.clone(), credentials, DecodeOptions::default())
        .map_err(|error| format!("cannot reach the node at {url}: {error}"))?;
    match node.network_identity().await {
        Ok(identity) => Ok(identity),
        Err(NodeError::Identity(IdentityError::MissingTachyonUpgrade)) => Err(format!(
            "the node at {url} lists no {} upgrade in getblockchaininfo; refusing to start (spec 11.1)",
            tachyon_chain::TACHYON_UPGRADE_NAME
        )
        .into()),
        Err(error) => Err(format!(
            "cannot read the network identity of the node at {url}: {error}; refusing to start"
        )
        .into()),
    }
}
