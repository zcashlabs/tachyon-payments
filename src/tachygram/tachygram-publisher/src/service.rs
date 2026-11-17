//! Registration with the publisher host and the managed run: journal,
//! distribution, routes, the canonical guard, the publication task and the
//! root-scheduled writer (after `receiver-indexer/src/managed.rs`).
use crate::{
    config::TachygramConfig,
    core::{Core, Loaded, Memo, Offer, PRODUCT, Stats, seed_memo, store_path},
    error::{BoxError, Error, Result},
    hooks::ChainHooks,
    metrics::Metrics,
    publication::PublicationTask,
    writer::Writer,
};
use axum::{
    Router,
    extract::{Request, State},
    http::header,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};
use pir_observability::HttpMetrics;
use pir_pipeline::{
    ingestion::{ChainFeed, ChainSettings, IngestionExecutor},
    publisher::{
        PublisherDefinition, PublisherExecutor, PublisherResources, ReadinessProbe, ReadinessRule,
    },
    service::{Http, Shutdown, Tasks},
};
use std::{sync::Arc, time::Duration};
use tachygram_store::{Binding, Store};
use tachyon_chain::{Credentials, NetworkIdentity, NodeClient, NodeError};
use tachyon_pir_distribution::{
    Current, Distribution, DistributionOptions, ObjectLimits, ScopeBinding, Subject,
};
use tokio::sync::watch;

/// The readiness probe path (spec 11.3 prefix).
pub const READINESS_PATH: &str = "/provider/tachygram/v1/current";

/// Ready when the envelope is eligible and names a publication.
pub fn distribution_ready(bytes: &[u8]) -> bool {
    serde_json::from_slice::<Current>(bytes)
        .is_ok_and(|current| current.eligible && current.publication.is_some())
}

/// Declare the tachygram publisher's resources without opening state,
/// sockets or credentials, for `pir_publisher::run_registered`.
pub fn configure<E: PublisherExecutor + IngestionExecutor>(
    config: TachygramConfig,
    chain: Option<&ChainSettings>,
    executor: E,
) -> pir_pipeline::publisher::Result<PublisherDefinition<axum::Router>> {
    let chain = chain
        .cloned()
        .ok_or("the tachygram publisher needs the root chain settings")?;
    config.validate()?;
    let resources = PublisherResources {
        listeners: vec![config.listen],
        writable_paths: vec![config.state_dir.clone()],
        probes: vec![ReadinessProbe {
            address: config.listen,
            path: READINESS_PATH,
            rule: ReadinessRule::Body(distribution_ready),
        }],
    };
    Ok(
        PublisherDefinition::new("tachygram", resources, move |context| async move {
            let feed = context
                .chain
                .ok_or("the tachygram publisher started without the root chain")?;
            pir_pipeline::publisher::dedicated_runtime("tachygram-publisher", move || async move {
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

/// One opened tachygram publisher: the writer for the root scheduler, the
/// publication task, and the shared state behind the routes.
pub struct Tachygram {
    pub writer: Writer,
    pub publisher: PublicationTask,
    core: Arc<Core>,
}

impl Tachygram {
    /// Read the node's network identity (refusing a node without
    /// `NuTachyon`, spec 11.1) and open the publisher's state.
    pub async fn open(
        config: &TachygramConfig,
        node: NodeClient,
        shutdown: Shutdown,
    ) -> Result<Self> {
        let identity = node.network_identity().await.map_err(identity_error)?;
        Self::open_with(config, node, identity, shutdown)
    }

    /// Open the journal and distribution under `state_dir`, bound to
    /// `identity`, and take the previous publication's built scopes so they
    /// are not rebuilt.
    pub fn open_with(
        config: &TachygramConfig,
        node: NodeClient,
        identity: NetworkIdentity,
        shutdown: Shutdown,
    ) -> Result<Self> {
        config.validate().map_err(Error::other)?;
        let lock = pir_pipeline::service::lock(&config.state_dir)?;
        let binding = Binding::new(&identity, config.epoch_length);
        let schedule = binding.schedule()?;
        let store = Store::open(store_path(&config.state_dir), binding)?;
        let distribution = Distribution::open(
            &config.state_dir,
            tachyon_pir_distribution::Product::Tachygram,
            ScopeBinding::chain(
                *identity.genesis_hash.protocol_bytes(),
                u64::from(identity.activation_height),
                u64::from(config.epoch_length),
            ),
            Arc::new(ChainHooks { node: node.clone() }),
            DistributionOptions {
                retention: config.retention,
                limits: ObjectLimits::SPEC,
            },
        )?;
        let metrics = Arc::new(Metrics::default());
        metrics.ingested(store.committed().map(|c| c.height));
        let core = Arc::new(Core {
            distribution,
            node,
            identity,
            schedule,
            metrics,
            memo: std::sync::Mutex::new(Memo::default()),
            rewinds: std::sync::Mutex::new(0),
            offer: std::sync::Mutex::new(Offer::default()),
            served: std::sync::Mutex::new(None),
            stats: Stats::default(),
            _lock: lock,
        });
        match seed_memo(&core, &store) {
            Ok(0) => {}
            Ok(scopes) => {
                tracing::info!(scopes, "tachygram reuses its previous publication's tables")
            }
            Err(error) => {
                tracing::warn!(%error, "tachygram previous tables not reused; rebuilding")
            }
        }
        let (slot, pending) = watch::channel(None);
        Ok(Self {
            writer: Writer {
                core: core.clone(),
                store,
                slot,
                restore: true,
            },
            publisher: PublicationTask {
                core: core.clone(),
                pending,
                shutdown,
                closed: false,
                woken: false,
                attempt: None,
            },
            core,
        })
    }

    pub fn distribution(&self) -> &Distribution {
        &self.core.distribution
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.core.metrics.clone()
    }

    /// Tables built (not reused) since open.
    pub fn tables_built(&self) -> u64 {
        self.core.tables_built()
    }

    /// The product listener's routes: the distribution prefix and
    /// `GET /metrics`, observed by fixed endpoint labels.
    pub fn router(&self) -> Router {
        router(&self.core)
    }

    /// The canonical guard over `feed` (see [`guard`]).
    pub fn guard(
        &self,
        feed: ChainFeed,
        stop: Shutdown,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        guard(self.core.clone(), feed, stop)
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
    core.distribution
        .mount(Router::new())
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

/// Fixed endpoint labels; paths and client bytes never become labels.
fn endpoint(path: &str) -> Option<&'static str> {
    let rest = path.strip_prefix(PRODUCT.distribution_prefix());
    match rest {
        Some("current") => Some("current"),
        Some("authority/check") => Some("authority_check"),
        Some(object) if object.starts_with("objects/") => Some("objects"),
        _ if path == "/metrics" => Some("metrics"),
        _ => None,
    }
}

async fn observe(State(metrics): State<HttpMetrics>, request: Request, next: Next) -> Response {
    let endpoint = endpoint(request.uri().path());
    pir_observability::observe(metrics, endpoint, request, next).await
}

/// Withdraw the current publication as soon as the root follower shows a
/// block it depends on is no longer canonical, independently of the writer,
/// which may be preparing (after Receiver's guard). The writer's
/// `withdraw_above` then decides whether the rewind needs a recovery.
pub(crate) async fn guard(core: Arc<Core>, mut feed: ChainFeed, stop: Shutdown) -> Result<()> {
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
            core.distribution.withdraw()?;
            return Err(Error::Network(format!(
                "root chain moved to genesis {}",
                observation.genesis
            )));
        }
        let current = core.distribution.current();
        let Some(publication) = current.publication.filter(|_| current.eligible) else {
            continue;
        };
        let subject = match &cached {
            Some((identity, subject)) if *identity == publication.publication => subject.clone(),
            _ => match Loaded::read(&core.distribution) {
                Ok(Some(loaded)) => {
                    cached = Some((publication.publication, loaded.inventory.subject.clone()));
                    loaded.inventory.subject
                }
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(%error, "tachygram canonical guard deferred");
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
                        if core.distribution.withdraw().is_err() {
                            tracing::error!("tachygram withdrawal is not durable");
                        }
                        *core.served.lock().unwrap() = None;
                        tracing::warn!(
                            height = block.height,
                            "tachygram publication withdrawn: a block it depends on left the chain"
                        );
                        break;
                    }
                }
                Err(error) => tracing::warn!(%error, "tachygram canonical guard deferred"),
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
            Err(error) => tracing::warn!(%error, "tachygram waits for the node"),
        }
        tokio::select! {
            _ = stop.cancelled() => return Err(Error::other("stopped before the node answered")),
            _ = tokio::time::sleep(poll.max(Duration::from_millis(100))) => {}
        }
    }
}

/// Run the product until shutdown or a stopping fault: the listener, the
/// canonical guard, the publication task and the root-scheduled writer.
pub async fn run<E: PublisherExecutor + IngestionExecutor>(
    config: TachygramConfig,
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
    let tachygram = Tachygram::open_with(&config, node, identity, stop.clone())?;
    let mut tasks = Tasks::default();
    let app = tachygram.router();
    let listen = config.listen;
    let serve_stop = stop.clone();
    let serving = tokio::spawn(async move {
        tokio::select! {
            result = http.serve(listen, app) => result.map_err(BoxError::from),
            _ = serve_stop.cancelled() => Ok(()),
        }
    });
    tasks.own(&serving);
    let guarding = tokio::spawn(tachygram.guard(feed.clone(), stop.clone()));
    tasks.own(&guarding);
    let Tachygram {
        writer, publisher, ..
    } = tachygram;
    let publishing_executor = executor.clone();
    let publish_stop = stop.clone();
    let publishing = tokio::spawn(async move {
        publishing_executor
            .run(publisher, publish_stop)
            .await
            .map_err(BoxError::from)
    });
    tasks.own(&publishing);
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
    let (ingested, served, published, guarded) = tokio::join!(
        ingested,
        joined(stop.clone(), serving),
        joined(stop.clone(), publishing),
        guarded
    );
    ingested.and(published).and(guarded).and(served)
}
