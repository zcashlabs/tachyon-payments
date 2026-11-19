//! Registration with the publisher host and the managed run: the log, the
//! distribution, the write API, the routes and the publication loop.
use crate::{
    challenge::ChallengeKey,
    config::RegistryConfig,
    core::{Core, Memo, PRODUCT, Stats, seed_memo},
    error::{BoxError, Error, Result},
    hooks::RegistryHooks,
    limits::RateLimits,
    metrics::Metrics,
    publication::PublicationTask,
    write::{self, MOUNT, WriteApi},
};
use axum::{
    Router,
    extract::{Request, State},
    http::header,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};
use pir_control::admission::ClientSlots;
use pir_observability::HttpMetrics;
use pir_pipeline::{
    publisher::{
        PublisherDefinition, PublisherExecutor, PublisherResources, ReadinessProbe, ReadinessRule,
    },
    service::{Http, Shutdown, Tasks},
};
use registry_store::{Binding, STORE_FILE, Store};
use std::sync::Arc;
use tachyon_pir_distribution::{
    Current, Distribution, DistributionOptions, ObjectLimits, ScopeBinding,
};
use tachyon_pir_protocol::REGISTRY_CHUNK_ENTRIES;
use tokio::sync::watch;

/// The readiness probe path (spec 11.3 prefix).
pub const READINESS_PATH: &str = "/provider/registry/v1/current";

/// Ready when the envelope is eligible and names a publication.
pub fn distribution_ready(bytes: &[u8]) -> bool {
    serde_json::from_slice::<Current>(bytes)
        .is_ok_and(|current| current.eligible && current.publication.is_some())
}

/// Declare the registry publisher's resources without opening state or
/// sockets, for `pir_publisher::run_registered`. The registry follows no
/// chain (spec 9.2): the definition does not ask for the root chain feed.
pub fn configure<E: PublisherExecutor>(
    config: RegistryConfig,
    executor: E,
) -> pir_pipeline::publisher::Result<PublisherDefinition<axum::Router>> {
    config.validate()?;
    let listen = config.settings.listen;
    let resources = PublisherResources {
        listeners: vec![listen],
        writable_paths: vec![config.settings.state_dir.clone()],
        probes: vec![ReadinessProbe {
            address: listen,
            path: READINESS_PATH,
            rule: ReadinessRule::Body(distribution_ready),
        }],
    };
    Ok(PublisherDefinition::new(
        "registry",
        resources,
        move |context| async move {
            pir_pipeline::publisher::dedicated_runtime("registry-publisher", move || async move {
                run(config, context.http, context.shutdown, executor).await
            })
            .await
        },
    ))
}

/// One opened registry publisher: the publication loop and the shared state
/// behind the routes.
pub struct Registry {
    publisher: Option<PublicationTask>,
    core: Arc<Core>,
    write: Arc<WriteApi>,
}

impl Registry {
    /// Open the log (bound to the identity, spec 9.2), the challenge secret,
    /// and the distribution under `state_dir`, and take the previous
    /// publication's built chunks so they are not rebuilt. The previous
    /// publication itself is restored by the publication task's first step.
    pub fn open(config: &RegistryConfig, shutdown: Shutdown) -> Result<Self> {
        config.validate().map_err(Error::other)?;
        let settings = &config.settings;
        std::fs::create_dir_all(&settings.state_dir)?;
        let lock = pir_pipeline::service::lock(&settings.state_dir)?;
        let store = Arc::new(Store::open(
            settings.state_dir.join(STORE_FILE),
            Binding {
                chain: config.identity.chain.clone(),
                genesis_hash: config.identity.genesis_hash,
            },
        )?);
        let key = ChallengeKey::load_or_create(&settings.state_dir)?;
        let distribution = Distribution::open(
            &settings.state_dir,
            tachyon_pir_distribution::Product::Registry,
            ScopeBinding::registry(
                *config.identity.genesis_hash.protocol_bytes(),
                REGISTRY_CHUNK_ENTRIES,
            ),
            Arc::new(RegistryHooks {
                store: store.clone(),
            }),
            DistributionOptions {
                retention: config.retention,
                limits: ObjectLimits::SPEC,
            },
        )?;
        let (log, receiver) = watch::channel(store.log_sequence()?);
        let core = Arc::new(Core {
            store,
            distribution,
            identity: config.identity.clone(),
            metrics: Arc::new(Metrics::default()),
            memo: std::sync::Mutex::new(Memo::default()),
            log,
            served: std::sync::Mutex::new(None),
            stats: Stats::default(),
            _lock: lock,
        });
        match seed_memo(&core) {
            Ok(0) => {}
            Ok(chunks) => {
                tracing::info!(chunks, "registry reuses its previous publication's tables")
            }
            Err(error) => {
                tracing::warn!(%error, "registry previous tables not reused; rebuilding")
            }
        }
        let write = Arc::new(WriteApi {
            core: core.clone(),
            key,
            difficulty_bits: settings.difficulty_bits,
            slots: ClientSlots::new(settings.per_client_concurrent),
            limits: RateLimits::new(settings.global_per_minute, settings.per_client_per_minute),
            upload: config.upload(),
        });
        Ok(Self {
            publisher: Some(PublicationTask {
                core: core.clone(),
                log: receiver,
                shutdown,
                interval: config.publish_interval(),
                restore: true,
                failed: false,
                closed: false,
                last_start: None,
            }),
            core,
            write,
        })
    }

    /// The publication loop, until [`Self::take_publisher`] moves it out.
    pub fn publisher(&mut self) -> &mut PublicationTask {
        self.publisher
            .as_mut()
            .expect("the publication loop was taken")
    }

    /// Move the publication loop out, to run it with
    /// `PublisherExecutor::run`; `None` once taken.
    pub fn take_publisher(&mut self) -> Option<PublicationTask> {
        self.publisher.take()
    }

    pub fn distribution(&self) -> &Distribution {
        &self.core.distribution
    }

    /// The registration log.
    pub fn store(&self) -> &Store {
        &self.core.store
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.core.metrics.clone()
    }

    /// Tables built (not reused) since open.
    pub fn tables_built(&self) -> u64 {
        self.core.tables_built()
    }

    /// The product listener's routes: the distribution prefix, the write API
    /// at both mounts and `GET /metrics`, observed by fixed endpoint labels.
    /// Serve it with `into_make_service_with_connect_info::<SocketAddr>()`
    /// for the peer-address fallback of the client key.
    pub fn router(&self) -> Router {
        let metrics = self.core.metrics.clone();
        let router = self.core.distribution.mount(Router::new()).route(
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
        );
        write::mount(router, self.write.clone())
            .fallback(write::not_found)
            .layer(axum::middleware::from_fn_with_state(
                self.core.metrics.http.clone(),
                observe,
            ))
    }
}

/// Fixed endpoint labels; paths and client bytes never become labels.
fn endpoint(path: &str) -> Option<&'static str> {
    if let Some(rest) = path.strip_prefix(PRODUCT.distribution_prefix()) {
        return match rest {
            "current" => Some("current"),
            "authority/check" => Some("authority_check"),
            object if object.starts_with("objects/") => Some("objects"),
            _ => None,
        };
    }
    match MOUNT.operation(path) {
        Some("/challenge") => Some("challenge"),
        Some("/register") => Some("register"),
        _ if path == "/metrics" => Some("metrics"),
        _ => None,
    }
}

async fn observe(State(metrics): State<HttpMetrics>, request: Request, next: Next) -> Response {
    let endpoint = endpoint(request.uri().path());
    pir_observability::observe(metrics, endpoint, request, next).await
}

/// Run the product until shutdown or a stopping fault: the listener and the
/// publication loop.
///
/// The host's listener adapter serves the router without connection info,
/// so behind `run_registered` a request without `X-Forwarded-For` or
/// `X-Real-IP` is keyed as `unknown` (one shared client); the reverse proxy
/// in front sets `X-Forwarded-For` (spec 9.3).
pub async fn run<E: PublisherExecutor>(
    config: RegistryConfig,
    http: Http<axum::Router>,
    stop: Shutdown,
    executor: E,
) -> pir_pipeline::publisher::Result<()> {
    let mut registry = Registry::open(&config, stop.clone())?;
    let mut tasks = Tasks::default();
    let app = registry.router();
    let listen = config.settings.listen;
    let serve_stop = stop.clone();
    let serving = tokio::spawn(async move {
        tokio::select! {
            result = http.serve(listen, app) => result.map_err(BoxError::from),
            _ = serve_stop.cancelled() => Ok(()),
        }
    });
    tasks.own(&serving);
    let publisher = registry
        .take_publisher()
        .ok_or("the registry publication loop is missing")?;
    let publish_stop = stop.clone();
    let publishing = tokio::spawn(async move {
        executor
            .run(publisher, publish_stop)
            .await
            .map_err(BoxError::from)
    });
    tasks.own(&publishing);
    let joined =
        |stop: Shutdown, task: tokio::task::JoinHandle<std::result::Result<(), BoxError>>| async move {
            let result = task.await;
            stop.request();
            result.map_err(BoxError::from)?
        };
    let (served, published) = tokio::join!(
        joined(stop.clone(), serving),
        joined(stop.clone(), publishing)
    );
    published.and(served)
}
