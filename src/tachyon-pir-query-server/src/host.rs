//! The host: discovery preflight, the management and public listeners, one
//! synchronization per product, and the drain on stop (after
//! `wallet-pir/src/pir-query-server/src/host.rs`).

use crate::{
    Error,
    config::Validated,
    response,
    serving::{self, Phase, ProductState},
    sync,
};
use axum::{
    Json, Router,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use pir_pipeline::{consumer::polling::SyncManager, service::Shutdown};
use serde_json::{Value, json};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpListener, task::JoinSet};

/// Upper bound on draining wallet requests already accepted at stop.
pub const PUBLIC_DRAIN: Duration = Duration::from_secs(60);

/// The two listeners, bound by the caller.
pub struct Listeners {
    pub management: TcpListener,
    pub public: TcpListener,
}

/// Every product's state, shared by the management routes.
#[derive(Clone)]
struct Products(Arc<Vec<Arc<ProductState>>>);

impl Products {
    fn snapshot(&self) -> (bool, Value) {
        let mut products = serde_json::Map::new();
        let mut all = !self.0.is_empty();
        for state in self.0.iter() {
            let ready = state.ready();
            all &= ready;
            products.insert(
                state.module.product.name().into(),
                json!({"phase": state.phase().name(), "ready": ready}),
            );
        }
        (
            all,
            json!({
                "ready": all,
                "identity": pir_control::Identity::process(),
                "products": products,
            }),
        )
    }
}

fn management_router(products: Products, http: pir_observability::HttpMetrics) -> Router {
    Router::new()
        .route(
            "/health",
            get(|State((p, _)): State<(Products, _)>| async move {
                (
                    [(header::CACHE_CONTROL, response::NO_STORE)],
                    Json(p.snapshot().1),
                )
            }),
        )
        .route(
            "/ready",
            get(|State((p, _)): State<(Products, _)>| async move {
                let (ready, body) = p.snapshot();
                let status = if ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                (
                    status,
                    [(header::CACHE_CONTROL, response::NO_STORE)],
                    Json(body),
                )
            }),
        )
        .route(
            "/metrics",
            get(
                |State((p, http)): State<(Products, pir_observability::HttpMetrics)>| async move {
                    let mut body = http.render();
                    for state in p.0.iter() {
                        body.push_str(&state.render_metrics());
                    }
                    (
                        [
                            (header::CONTENT_TYPE, "text/plain; version=0.0.4"),
                            (header::CACHE_CONTROL, response::NO_STORE),
                        ],
                        body,
                    )
                },
            ),
        )
        .with_state((products, http))
}

async fn observe(
    State((products, metrics)): State<(Products, pir_observability::HttpMetrics)>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let label = products
        .0
        .iter()
        .find_map(|state| state.module.endpoint(path));
    pir_observability::observe(metrics, label, request, next).await
}

fn public_router(products: &Products, http: &pir_observability::HttpMetrics) -> Router {
    let mut router = Router::new();
    let mut endpoints = Vec::new();
    for state in products.0.iter() {
        endpoints.extend(state.module.endpoints.all());
        router = router.merge(serving::router(state.clone()));
    }
    http.initialize(&endpoints);
    router
        .fallback(|| async { StatusCode::NOT_FOUND.into_response() })
        .layer(middleware::map_response(response::default_no_store))
        .layer(middleware::from_fn_with_state(
            (products.clone(), http.clone()),
            observe,
        ))
}

/// Check every product's publisher discovery document (spec 11.6). Any
/// failure refuses to start, as `wallet-pir`'s host does.
pub async fn discover(config: &Validated) -> Result<(), Error> {
    for product in &config.products {
        let follower = sync::follower_config(product)?;
        let roots = match &follower.publisher_ca {
            Some(pem) => vec![reqwest::Certificate::from_pem(pem)?],
            None => Vec::new(),
        };
        let source = pir_sync_manager::HttpsSource::new(
            &product.publisher,
            product.module.distribution().prefix(),
            follower.download_budget,
            &roots,
        )?;
        source.discover().await?;
    }
    Ok(())
}

/// Bind both listeners and serve until `shutdown`.
pub async fn run(config: Validated, shutdown: Shutdown) -> Result<(), Error> {
    discover(&config).await?;
    let listeners = Listeners {
        management: TcpListener::bind(config.management_listen).await?,
        public: TcpListener::bind(config.public_listen).await?,
    };
    serve_bound(config, listeners, shutdown).await
}

/// Threads of the preparation pool: half the host, so preparing a new
/// publication never takes every core from queries.
fn preparation_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| (n.get() / 2).max(1))
        .unwrap_or(1)
}

/// Run the discovery preflight, then serve on `listeners` until `shutdown`
/// or a fatal error.
pub async fn serve(
    config: Validated,
    listeners: Listeners,
    shutdown: Shutdown,
) -> Result<(), Error> {
    discover(&config).await?;
    serve_bound(config, listeners, shutdown).await
}

async fn serve_bound(
    config: Validated,
    listeners: Listeners,
    shutdown: Shutdown,
) -> Result<(), Error> {
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(preparation_threads())
            .thread_name(|i| format!("pir-prepare-{i}"))
            .build()?,
    );
    let mut states = Vec::new();
    let mut tasks = Vec::new();
    for product in &config.products {
        let state = ProductState::new(product);
        let tracked = sync::open(product, state.clone(), pool.clone())?;
        tasks.push((product.module.product.name(), product.poll, tracked));
        states.push(state);
    }
    let products = Products(Arc::new(states));
    let http = pir_observability::HttpMetrics::default();

    let management_stop = Shutdown::default();
    let management = {
        let stop = management_stop.clone();
        let app = management_router(products.clone(), http.clone());
        tokio::spawn(async move {
            axum::serve(listeners.management, app)
                .with_graceful_shutdown(async move { stop.cancelled().await })
                .await
        })
    };
    let public_stop = Shutdown::default();
    let mut public = {
        let stop = public_stop.clone();
        let app =
            public_router(&products, &http).into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(async move {
            axum::serve(listeners.public, app)
                .with_graceful_shutdown(async move { stop.cancelled().await })
                .await
        })
    };

    let sync_stop = Shutdown::default();
    let mut group: JoinSet<(usize, Option<Error>)> = JoinSet::new();
    for (index, (_, poll, tracked)) in tasks.into_iter().enumerate() {
        let stop = sync_stop.clone();
        group.spawn(async move {
            let result = SyncManager::new(tracked)
                .with_success_interval(poll)
                .run_until(stop.cancelled())
                .await;
            (index, result)
        });
    }

    let mut management = management;
    let mut fatal: Option<Error> = None;
    let mut public_done = false;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = &mut management => {
                fatal = Some("management HTTP service stopped".into());
                break;
            }
            _ = &mut public => {
                public_done = true;
                fatal = Some("public HTTP service stopped".into());
                break;
            }
            joined = group.join_next(), if !group.is_empty() => {
                match joined {
                    Some(Ok((index, Some(error)))) => {
                        let state = &products.0[index];
                        state.withdraw();
                        state.set_phase(Phase::Failed);
                        if error.downcast_ref::<pir_sync_manager::DiscoveryError>().is_some() {
                            tracing::error!(product = state.module.product.name(), %error,
                                "publisher service discovery refused this host; stopping");
                            fatal = Some(error);
                            break;
                        }
                        tracing::error!(product = state.module.product.name(), %error,
                            "synchronization stopped; the product no longer serves");
                    }
                    Some(Ok((index, None))) => products.0[index].set_phase(Phase::Stopped),
                    Some(Err(_)) => {
                        fatal = Some("a synchronization task panicked".into());
                        break;
                    }
                    None => {}
                }
            }
        }
    }

    // Drain in admission order: refuse new wallet requests, let accepted
    // ones finish, then stop synchronization and management.
    for state in products.0.iter() {
        if state.phase() == Phase::Running {
            state.set_phase(Phase::Stopping);
        }
        state.withdraw();
    }
    public_stop.request();
    if !public_done
        && tokio::time::timeout(PUBLIC_DRAIN, &mut public)
            .await
            .is_err()
    {
        tracing::warn!("wallet requests outlasted the shutdown drain; closing the listener");
        public.abort();
        let _ = public.await;
    }
    sync_stop.request();
    while let Some(joined) = group.join_next().await {
        if let Ok((index, _)) = joined
            && products.0[index].phase() != Phase::Failed
        {
            products.0[index].set_phase(Phase::Stopped);
        }
    }
    management_stop.request();
    if !management.is_finished() {
        management.abort();
    }
    let _ = management.await;
    match fatal {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
