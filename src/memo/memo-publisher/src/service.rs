//! Registration with the publisher host and the managed run: journal, the
//! two distributions, routes, the canonical guards, the publication tasks and
//! the root-scheduled writer (after `receiver-indexer/src/managed.rs`).
//!
//! Memo and first-contact are **one** publisher definition (spec 11.7): one
//! listener serving both distribution prefixes, one state directory, one
//! journal and one writer.
use crate::{
    config::MemoConfig,
    core::{Core, Loaded, Side, distribution_product, seed_built, store_path},
    error::{BoxError, Error, Result},
    hooks::ChainHooks,
    metrics::Metrics,
    publication::PublicationTask,
    writer::{Slot, Writer},
};
use axum::{
    Router,
    extract::{Request, State},
    http::header,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};
use memo_store::{Binding, Store};
use pir_observability::HttpMetrics;
use pir_pipeline::{
    ingestion::{ChainFeed, ChainSettings, IngestionExecutor},
    publisher::{
        PublisherDefinition, PublisherExecutor, PublisherResources, ReadinessProbe, ReadinessRule,
    },
    service::{Http, Shutdown, Tasks},
};
use std::{sync::Arc, time::Duration};
use tachyon_chain::{Credentials, NetworkIdentity, NodeClient, NodeError};
use tachyon_pir_distribution::{
    Current, Distribution, DistributionOptions, ObjectLimits, ScopeBinding, Subject,
};
use tachyon_pir_protocol::Product;
use tokio::sync::watch;

/// The publisher definition's name under `run_registered`.
pub const DEFINITION_NAME: &str = "memo";

/// The memo readiness probe path (spec 11.3 prefix).
pub const MEMO_READINESS_PATH: &str = "/provider/memo/v1/current";

/// The first-contact readiness probe path, probed when first-contact is
/// enabled.
pub const FIRST_CONTACT_READINESS_PATH: &str = "/provider/first-contact/v1/current";

/// Ready when the envelope is eligible and names a publication.
pub fn distribution_ready(bytes: &[u8]) -> bool {
    serde_json::from_slice::<Current>(bytes)
        .is_ok_and(|current| current.eligible && current.publication.is_some())
}

/// Declare the memo publisher's resources (and first-contact's, when
/// `config.first_contact`) without opening state, sockets or credentials,
/// for `pir_publisher::run_registered`.
pub fn configure<E: PublisherExecutor + IngestionExecutor>(
    config: MemoConfig,
    chain: Option<&ChainSettings>,
    executor: E,
) -> pir_pipeline::publisher::Result<PublisherDefinition<axum::Router>> {
    let chain = chain
        .cloned()
        .ok_or("the memo publisher needs the root chain settings")?;
    config.validate()?;
    let mut probes = vec![ReadinessProbe {
        address: config.listen,
        path: MEMO_READINESS_PATH,
        rule: ReadinessRule::Body(distribution_ready),
    }];
    if config.first_contact {
        probes.push(ReadinessProbe {
            address: config.listen,
            path: FIRST_CONTACT_READINESS_PATH,
            rule: ReadinessRule::Body(distribution_ready),
        });
    }
    let resources = PublisherResources {
        listeners: vec![config.listen],
        writable_paths: vec![config.state_dir.clone()],
        probes,
    };
    Ok(
        PublisherDefinition::new(DEFINITION_NAME, resources, move |context| async move {
            let feed = context
                .chain
                .ok_or("the memo publisher started without the root chain")?;
            pir_pipeline::publisher::dedicated_runtime("memo-publisher", move || async move {
                run(
                    config,
                    chain,
                    feed,
                    context.http,
                    context.shutdown,
                    executor,
                )
                .await
            })
            .await
        })
        .following_chain(),
    )
}

/// One opened memo publisher: the writer for the root scheduler, one
/// publication task per product, and the shared state behind the routes.
pub struct MemoPublisher {
    pub writer: Writer,
    pub memo: PublicationTask,
    /// Present when first-contact is enabled.
    pub first_contact: Option<PublicationTask>,
    core: Arc<Core>,
}

impl MemoPublisher {
    /// Read the node's network identity (refusing a node without
    /// `NuTachyon`, spec 11.1) and open the publisher's state.
    pub async fn open(config: &MemoConfig, node: NodeClient, shutdown: Shutdown) -> Result<Self> {
        let identity = node.network_identity().await.map_err(identity_error)?;
        Self::open_with(config, node, identity, shutdown)
    }

    /// Open the journal and the distributions under `state_dir`, bound to
    /// `identity`, and take each previous publication's built scopes so they
    /// are not rebuilt.
    pub fn open_with(
        config: &MemoConfig,
        node: NodeClient,
        identity: NetworkIdentity,
        shutdown: Shutdown,
    ) -> Result<Self> {
        config.validate().map_err(Error::other)?;
        let lock = pir_pipeline::service::lock(&config.state_dir)?;
        let binding = Binding::new(&identity, config.epoch_length);
        let schedule = binding.schedule()?;
        let store = Store::open(store_path(&config.state_dir), binding)?;
        let open = |product: Product| -> Result<Arc<Side>> {
            let distribution = Distribution::open(
                &config.state_dir,
                distribution_product(product),
                ScopeBinding::chain(
                    *identity.genesis_hash.protocol_bytes(),
                    u64::from(identity.activation_height),
                    u64::from(config.epoch_length),
                ),
                Arc::new(ChainHooks {
                    node: node.clone(),
                    product,
                }),
                DistributionOptions {
                    retention: config.retention,
                    limits: ObjectLimits::SPEC,
                },
            )?;
            Ok(Arc::new(Side::new(product, distribution)))
        };
        let memo = open(Product::Memo)?;
        let first_contact = config
            .first_contact
            .then(|| open(Product::FirstContact))
            .transpose()?;
        let metrics = Arc::new(Metrics::new(config.first_contact));
        let totals = store.totals()?;
        metrics.journal(store.committed().map(|c| c.height), &totals);
        let core = Arc::new(Core {
            node,
            identity,
            schedule,
            metrics,
            memo,
            first_contact,
            rewinds: std::sync::Mutex::new(0),
            _lock: lock,
        });
        for side in core.sides() {
            match seed_built(side, &store) {
                Ok(0) => {}
                Ok(scopes) => tracing::info!(
                    product = %side.product,
                    scopes,
                    "reusing the previous publication's tables"
                ),
                Err(error) => tracing::warn!(
                    product = %side.product,
                    %error,
                    "previous tables not reused; rebuilding"
                ),
            }
        }
        let mut slots = Vec::new();
        let mut task = |side: &Arc<Side>| {
            let (sender, pending) = watch::channel(None);
            slots.push(Slot {
                side: side.clone(),
                sender,
            });
            PublicationTask {
                core: core.clone(),
                side: side.clone(),
                pending,
                shutdown: shutdown.clone(),
                closed: false,
                woken: false,
                attempt: None,
            }
        };
        let memo = task(&core.memo);
        let first_contact = core.first_contact.as_ref().map(&mut task);
        Ok(Self {
            writer: Writer {
                core: core.clone(),
                store,
                slots,
                totals,
                restore: true,
            },
            memo,
            first_contact,
            core,
        })
    }

    /// The memo distribution.
    pub fn memo_distribution(&self) -> &Distribution {
        &self.core.memo.distribution
    }

    /// The first-contact distribution, when enabled.
    pub fn first_contact_distribution(&self) -> Option<&Distribution> {
        self.core
            .first_contact
            .as_ref()
            .map(|side| &side.distribution)
    }

    /// The distribution of `product` (memo or first-contact), when published.
    pub fn distribution(&self, product: Product) -> Option<&Distribution> {
        self.core.side(product).map(|side| &side.distribution)
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.core.metrics.clone()
    }

    /// Tables `product` built (not reused) since open.
    pub fn tables_built(&self, product: Product) -> u64 {
        self.core
            .side(product)
            .map_or(0, |side| side.tables_built())
    }

    /// The listener's routes: both distribution prefixes and `GET /metrics`,
    /// observed by fixed endpoint labels.
    pub fn router(&self) -> Router {
        router(&self.core)
    }

    /// The canonical guards of both products over `feed` (see [`guard`]).
    pub fn guard(
        &self,
        feed: ChainFeed,
        stop: Shutdown,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let guards: Vec<_> = self
            .core
            .sides()
            .map(|side| guard(self.core.clone(), side.clone(), feed.clone(), stop.clone()))
            .collect();
        async move {
            for result in futures_util::future::join_all(guards).await {
                result?;
            }
            Ok(())
        }
    }
}

fn identity_error(error: NodeError) -> Error {
    match error {
        NodeError::Identity(error) => Error::Network(error.to_string()),
        other => Error::from(other),
    }
}

fn router(core: &Arc<Core>) -> Router {
    let metrics = core.metrics.clone();
    let mut router = Router::new();
    for side in core.sides() {
        router = side.distribution.mount(router);
    }
    router
        .route(
            "/metrics",
            get(move || {
                let metrics = metrics.clone();
                async move {
                    (
                        [
                            (header::CONTENT_TYPE, "text/plain; version=0.0.4"),
                            (header::CACHE_CONTROL, "no-store"),
                        ],
                        metrics.render(),
                    )
                        .into_response()
                }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            core.metrics.http.clone(),
            observe,
        ))
}

/// Fixed endpoint labels; paths and client bytes never become labels. Both
/// prefixes are labelled whatever is mounted: an unmounted prefix answers
/// `404` under its label.
fn endpoint(path: &str) -> Option<&'static str> {
    if path == "/metrics" {
        return Some("metrics");
    }
    let labels = [
        (
            Product::Memo,
            ["memo_current", "memo_authority_check", "memo_objects"],
        ),
        (
            Product::FirstContact,
            [
                "first_contact_current",
                "first_contact_authority_check",
                "first_contact_objects",
            ],
        ),
    ];
    for (product, [current, check, objects]) in labels {
        let Some(rest) = path.strip_prefix(product.distribution_prefix()) else {
            continue;
        };
        return match rest {
            "current" => Some(current),
            "authority/check" => Some(check),
            object if object.starts_with("objects/") => Some(objects),
            _ => None,
        };
    }
    None
}

async fn observe(State(metrics): State<HttpMetrics>, request: Request, next: Next) -> Response {
    let endpoint = endpoint(request.uri().path());
    pir_observability::observe(metrics, endpoint, request, next).await
}

/// Withdraw `side`'s current publication as soon as the root follower shows a
/// block it depends on is no longer canonical, independently of the writer,
/// which may be preparing (after Receiver's guard). The writer's
/// `withdraw_above` then decides whether the rewind needs a recovery.
pub(crate) async fn guard(
    core: Arc<Core>,
    side: Arc<Side>,
    mut feed: ChainFeed,
    stop: Shutdown,
) -> Result<()> {
    let product = side.product;
    let genesis = core.identity.genesis_hash.display_hex();
    let mut cached: Option<([u8; 32], Subject)> = None;
    loop {
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            changed = feed.observations.changed() => if changed.is_err() {
                return Err(Error::other("root chain feed stopped"));
            }
        }
        let Some(observation) = feed.observations.borrow_and_update().clone() else {
            continue;
        };
        if observation.genesis != genesis {
            side.distribution.withdraw()?;
            return Err(Error::Network(format!(
                "root chain moved to genesis {}",
                observation.genesis
            )));
        }
        let current = side.distribution.current();
        let Some(publication) = current.publication.filter(|_| current.eligible) else {
            continue;
        };
        let subject = match &cached {
            Some((identity, subject)) if *identity == publication.publication => subject.clone(),
            _ => match Loaded::read(&side.distribution, product) {
                Ok(Some(loaded)) => {
                    cached = Some((publication.publication, loaded.inventory.subject.clone()));
                    loaded.inventory.subject
                }
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(%product, %error, "canonical guard deferred");
                    continue;
                }
            },
        };
        for block in subject.blocks() {
            if block.height > observation.tip.height {
                continue;
            }
            match feed.chain.hash_at(block.height).await {
                Ok(actual) => {
                    let mut display = block.hash;
                    display.reverse();
                    if actual != hex::encode(display) {
                        if side.distribution.withdraw().is_err() {
                            tracing::error!(%product, "withdrawal is not durable");
                        }
                        *side.served.lock().unwrap() = None;
                        tracing::warn!(
                            %product,
                            height = block.height,
                            "publication withdrawn: a block it depends on left the chain"
                        );
                        break;
                    }
                }
                Err(error) => tracing::warn!(%product, %error, "canonical guard deferred"),
            }
        }
    }
}

/// Read the node's identity, retrying while the node is unreachable; a node
/// without `NuTachyon` is refused at once (spec 11.1).
async fn identity(node: &NodeClient, poll: Duration, stop: &Shutdown) -> Result<NetworkIdentity> {
    loop {
        match node.network_identity().await {
            Ok(identity) => return Ok(identity),
            Err(NodeError::Identity(error)) => return Err(Error::Network(error.to_string())),
            Err(error) => tracing::warn!(%error, "memo publisher waits for the node"),
        }
        tokio::select! {
            _ = stop.cancelled() => return Err(Error::other("stopped before the node answered")),
            _ = tokio::time::sleep(poll.max(Duration::from_millis(100))) => {}
        }
    }
}

/// Run the definition until shutdown or a stopping fault: the listener, the
/// canonical guards, one publication task per product and the
/// root-scheduled writer.
pub async fn run<E: PublisherExecutor + IngestionExecutor>(
    config: MemoConfig,
    chain: ChainSettings,
    feed: ChainFeed,
    http: Http<axum::Router>,
    stop: Shutdown,
    executor: E,
) -> pir_pipeline::publisher::Result<()> {
    let credentials = match &chain.rpc_cookie {
        Some(path) => Credentials::CookieFile(path.clone()),
        None => Credentials::None,
    };
    let node = NodeClient::connect(chain.rpc_url.clone(), credentials, config.decode)?;
    let identity = identity(&node, chain.poll, &stop).await?;
    let publisher = MemoPublisher::open_with(&config, node, identity, stop.clone())?;
    let mut tasks = Tasks::default();
    let app = publisher.router();
    let listen = config.listen;
    let serve_stop = stop.clone();
    let serving = tokio::spawn(async move {
        tokio::select! {
            result = http.serve(listen, app) => result.map_err(BoxError::from),
            _ = serve_stop.cancelled() => Ok(()),
        }
    });
    tasks.own(&serving);
    let guarding = tokio::spawn(publisher.guard(feed.clone(), stop.clone()));
    tasks.own(&guarding);
    let MemoPublisher {
        writer,
        memo,
        first_contact,
        ..
    } = publisher;
    let mut publishing = Vec::new();
    for task in std::iter::once(memo).chain(first_contact) {
        let executor = executor.clone();
        let publish_stop = stop.clone();
        let handle = tokio::spawn(async move {
            executor
                .run(task, publish_stop)
                .await
                .map_err(BoxError::from)
        });
        tasks.own(&handle);
        publishing.push(handle);
    }
    let ingested = async {
        let result = executor.ingest(writer, feed, stop.clone()).await;
        stop.request();
        result.map_err(BoxError::from)
    };
    let joined =
        |stop: Shutdown, task: tokio::task::JoinHandle<std::result::Result<(), BoxError>>| async move {
            let result = task.await;
            stop.request();
            result.map_err(BoxError::from)?
        };
    let guarded = async {
        let result = guarding.await;
        stop.request();
        result.map_err(BoxError::from)?.map_err(BoxError::from)
    };
    let published = async {
        let results = futures_util::future::join_all(
            publishing
                .into_iter()
                .map(|handle| joined(stop.clone(), handle)),
        )
        .await;
        results
            .into_iter()
            .collect::<std::result::Result<(), BoxError>>()
    };
    let (ingested, served, published, guarded) =
        tokio::join!(ingested, joined(stop.clone(), serving), published, guarded);
    ingested.and(published).and(guarded).and(served)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_labels_are_fixed() {
        assert_eq!(endpoint("/provider/memo/v1/current"), Some("memo_current"));
        assert_eq!(
            endpoint("/provider/memo/v1/objects/ab"),
            Some("memo_objects")
        );
        assert_eq!(
            endpoint("/provider/first-contact/v1/authority/check"),
            Some("first_contact_authority_check")
        );
        assert_eq!(endpoint("/metrics"), Some("metrics"));
        assert_eq!(endpoint("/provider/memo/v1/elsewhere"), None);
        assert_eq!(endpoint("/provider/tachygram/v1/current"), None);
    }
}
