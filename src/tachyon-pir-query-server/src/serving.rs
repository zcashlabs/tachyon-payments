//! The serving core every product shares: its state (publications, admission,
//! metrics) and its four public routes (spec 10.2), mounted at the product's
//! canonical and legacy prefixes with `pir_control::PublicMount` (spec 10.1).

use crate::{
    config::{Admission, ProductConfig},
    metrics::{Gauges, ProductMetrics, QueryOutcome},
    prepare::MapCache,
    product::ProductModule,
    response::{self, IMMUTABLE, NO_STORE},
};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, Path, Request, State, rejection::PathRejection},
    http::{HeaderValue, header},
    response::Response,
    routing::{get, post},
};
use pir_control::admission::{BodyError, ClientSlots, Queue, client_key, read_body};
use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tachyon_pir_distribution::{Publications, ServingPolicy, SessionId, SessionLookup};
use tachyon_pir_protocol::{
    ErrorStatus, FrameError, HEADER_BYTES, PUBLISHED_GEOMETRY, PirParams,
    query::{check_request_len, encode_response, parse_request},
};
use tachyon_pir_table::TableRuntime;

/// Lifecycle phase of a product, as `/health` reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl Phase {
    pub const fn name(self) -> &'static str {
        match self {
            Phase::Running => "running",
            Phase::Stopping => "stopping",
            Phase::Stopped => "stopped",
            Phase::Failed => "failed",
        }
    }
}

/// One product's serving state, shared by its routes, its synchronization
/// and the management listener.
pub struct ProductState {
    pub module: &'static ProductModule,
    pub publications: Publications<TableRuntime>,
    pub maps: MapCache,
    pub metrics: ProductMetrics,
    params: PirParams,
    queue: Queue,
    clients: ClientSlots,
    admission: Admission,
    /// Cleared when the host withdraws the product: its namespace answers 503.
    serving: AtomicBool,
    phase: Mutex<Phase>,
}

impl ProductState {
    pub fn new(config: &ProductConfig) -> Arc<Self> {
        let admission = config.admission;
        Arc::new(Self {
            module: config.module,
            publications: Publications::new(ServingPolicy {
                max_unconfirmed: config.max_unconfirmed,
                ..ServingPolicy::default()
            }),
            maps: MapCache::default(),
            metrics: ProductMetrics::default(),
            params: PirParams::new(config.module.product, PUBLISHED_GEOMETRY),
            queue: Queue::new(
                admission.executing,
                admission.waiting,
                admission.wait(),
                true,
            ),
            clients: ClientSlots::new(admission.per_client),
            admission,
            serving: AtomicBool::new(true),
            phase: Mutex::new(Phase::Running),
        })
    }

    pub fn phase(&self) -> Phase {
        *self.phase.lock().unwrap()
    }

    pub fn set_phase(&self, phase: Phase) {
        *self.phase.lock().unwrap() = phase;
    }

    /// Stop answering wallets: every route answers 503 from now on.
    pub fn withdraw(&self) {
        self.serving.store(false, Ordering::SeqCst);
    }

    fn serving(&self) -> bool {
        self.serving.load(Ordering::SeqCst)
    }

    /// Ready: running, not withdrawn, and a confirmed publication serves.
    pub fn ready(&self) -> bool {
        self.phase() == Phase::Running && self.serving() && self.publications.is_ready()
    }

    pub fn gauges(&self) -> Gauges {
        let mut gauges = Gauges {
            ready: self.ready(),
            ..Gauges::default()
        };
        if let Some(current) = self.publications.current() {
            gauges.generation = Some(current.generation());
            gauges.recovery_epoch = Some(current.recovery_epoch());
            gauges.tables_served = current.tables().count() as u64;
            gauges.runtime_held_bytes = current.tables().map(|t| t.runtime.held_bytes()).sum();
            let info = self.maps.get(current.map_sha256());
            gauges.floor_scope = Some(
                current
                    .floor_scope()
                    .max(info.as_ref().map_or(0, |i| i.floor_scope)),
            );
            if let Some(info) = info {
                gauges.anchor_height = info.anchor_height;
                gauges.log_sequence = info.log_sequence;
                gauges.newest_scope = info.newest_scope;
                gauges.entries_total = Some(info.entries_total);
                gauges.open_scope_entries = Some(info.open_scope_entries);
            }
        }
        gauges
    }

    pub fn render_metrics(&self) -> String {
        self.metrics
            .render(self.module.metric_prefix, &self.gauges())
    }
}

type Shared = State<Arc<ProductState>>;

/// The product's public routes at both of its prefixes.
pub fn router(state: Arc<ProductState>) -> Router {
    state
        .module
        .mount()
        .mount(
            Router::new(),
            [
                ("/map", get(map)),
                ("/map/:base/:sha256", get(map_chunk)),
                ("/public/:session", get(public)),
                ("/query", post(query)),
            ],
        )
        .with_state(state)
}

fn parse_hex32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(text, &mut out).ok()?;
    Some(out)
}

fn bad_request(message: &'static str) -> Response {
    response::error(ErrorStatus::BadRequest, message)
}

/// `GET map`: the current map head, its digest and this host's floor.
async fn map(State(s): Shared) -> Response {
    if !s.serving() {
        return response::unavailable();
    }
    let Some(current) = s.publications.current() else {
        return response::unavailable();
    };
    let floor = current.floor_scope().max(
        s.maps
            .get(current.map_sha256())
            .map_or(0, |info| info.floor_scope),
    );
    let mut response = response::ok(
        "application/json",
        NO_STORE,
        Body::from(Bytes::from_owner(current.map_head().clone())),
    );
    let headers = response.headers_mut();
    headers.insert(
        "x-map-sha256",
        HeaderValue::from_str(current.map_sha256()).expect("hex is a header value"),
    );
    headers.insert(
        "x-floor-scope",
        HeaderValue::from_str(&floor.to_string()).expect("a number is a header value"),
    );
    response
}

/// `GET map/{base}/{sha256}`: an immutable sealed map chunk of the current map.
async fn map_chunk(
    State(s): Shared,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Response {
    if !s.serving() {
        return response::unavailable();
    }
    let Ok(Path((base, sha256))) = path else {
        return bad_request("malformed map chunk path");
    };
    let (Ok(base), Some(_)) = (base.parse::<u64>(), parse_hex32(&sha256)) else {
        return bad_request("malformed map chunk path");
    };
    let Some(current) = s.publications.current() else {
        return response::unavailable();
    };
    let Some(bytes) = current.map_chunk(&sha256) else {
        return response::stale(current.map_sha256());
    };
    let listed_base = s
        .maps
        .get(current.map_sha256())
        .and_then(|info| info.chunk_bases.get(&sha256).copied());
    if listed_base != Some(base) {
        return bad_request("map chunk base does not match its digest");
    }
    response::ok(
        "application/json",
        IMMUTABLE,
        Body::from(Bytes::from_owner(bytes.clone())),
    )
}

/// `GET public/{session_id}`: a table's public setup, immutable.
async fn public(State(s): Shared, path: Result<Path<String>, PathRejection>) -> Response {
    if !s.serving() {
        return response::unavailable();
    }
    let Some(session) = path.ok().and_then(|Path(text)| parse_hex32(&text)) else {
        return bad_request("malformed session id");
    };
    match s.publications.session(&session) {
        SessionLookup::Found { table, .. } => response::ok(
            "application/octet-stream",
            IMMUTABLE,
            Body::from(Bytes::from_owner(table.public.clone())),
        ),
        SessionLookup::Revoked => response::revoked(),
        SessionLookup::Unknown { map_sha256 } => response::stale(&map_sha256),
        SessionLookup::Unavailable => response::unavailable(),
    }
}

/// A refusal for a session that is not served, counted.
fn not_found<T>(s: &ProductState, lookup: SessionLookup<T>) -> Response {
    match lookup {
        SessionLookup::Found { .. } => unreachable!("only refusals reach here"),
        SessionLookup::Revoked => {
            s.metrics.query(QueryOutcome::Revoked);
            response::revoked()
        }
        SessionLookup::Unknown { map_sha256 } => {
            s.metrics.query(QueryOutcome::Stale);
            response::stale(&map_sha256)
        }
        SessionLookup::Unavailable => {
            s.metrics.query(QueryOutcome::Unavailable);
            response::unavailable()
        }
    }
}

fn frame_error(s: &ProductState, error: &FrameError) -> Response {
    match error.status() {
        ErrorStatus::PayloadTooLarge => too_large(s),
        _ => {
            s.metrics.query(QueryOutcome::Invalid);
            bad_request("malformed query: wrong magic or length")
        }
    }
}

fn too_large(s: &ProductState) -> Response {
    s.metrics.query(QueryOutcome::TooLarge);
    response::error(ErrorStatus::PayloadTooLarge, "query body is over 512 KiB")
}

fn overloaded(s: &ProductState) -> Response {
    s.metrics.query(QueryOutcome::Overloaded);
    response::overloaded()
}

/// `POST query`: one encrypted row query (spec 10.4).
///
/// Order: readiness, the client's slot, the bounded upload, framing (limit,
/// header, magic), the session, the exact length for its geometry, then an
/// evaluation permit. The answer runs on the blocking pool holding every
/// guard; the session is looked up again afterwards, so an answer from a
/// publication fenced or revoked meanwhile is never sent.
async fn query(State(s): Shared, request: Request) -> Response {
    if !s.serving() || !s.publications.is_ready() {
        s.metrics.query(QueryOutcome::Unavailable);
        return response::unavailable();
    }
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip());
    let Some(client) = s.clients.try_acquire(&client_key(request.headers(), peer)) else {
        return overloaded(&s);
    };
    let limit = s.admission.body_limit();
    let declared = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|length| length > limit as u64) {
        return too_large(&s);
    }
    let body = match read_body(request.into_body(), limit, s.admission.upload()).await {
        Ok(body) => body,
        Err(BodyError::Timeout) => {
            s.metrics.query(QueryOutcome::Timeout);
            return response::error(
                ErrorStatus::UploadTimeout,
                "query body was not received in time",
            );
        }
        Err(error) if error.over_limit() => return too_large(&s),
        Err(_) => {
            s.metrics.query(QueryOutcome::Invalid);
            return bad_request("query body could not be read");
        }
    };
    let header = match parse_request(s.module.product, &body) {
        Ok((header, _)) => header,
        Err(error) => return frame_error(&s, &error),
    };
    let session: SessionId = header.session_id;
    let (table, publication) = match s.publications.session(&session) {
        SessionLookup::Found { table, publication } => (table, publication),
        refusal => return not_found(&s, refusal),
    };
    let geometry = table.runtime.geometry();
    if let Err(error) = check_request_len(geometry, body.len()) {
        return frame_error(&s, &error);
    }
    if geometry != s.params.geometry() {
        s.metrics.query(QueryOutcome::Unavailable);
        return response::unavailable();
    }
    let Ok(permit) = s.queue.acquire(|| ()).await else {
        return overloaded(&s);
    };
    let state = s.clone();
    let answered = pir_control::admission::spawn_admitted(
        (permit, client, table, publication),
        move |(_, _, table, _)| table.runtime.answer(&state.params, &body[HEADER_BYTES..]),
    )
    .await;
    match s.publications.session(&session) {
        SessionLookup::Found { .. } => {}
        refusal => return not_found(&s, refusal),
    }
    match answered {
        Ok((Ok(reply), guards)) => {
            s.metrics.query(QueryOutcome::Answered);
            let bytes = encode_response(&header, &reply);
            response::ok(
                "application/octet-stream",
                NO_STORE,
                pir_control::http::retain(Body::from(bytes), guards),
            )
        }
        Ok((Err(tachyon_pir_table::Error::Query(_)), _)) => {
            s.metrics.query(QueryOutcome::Invalid);
            bad_request("the query payload is not a valid query for this table")
        }
        Ok((Err(error), _)) => {
            tracing::error!(product = s.module.product.name(), %error, "query evaluation failed");
            s.metrics.query(QueryOutcome::Unavailable);
            response::unavailable()
        }
        Err(_) => {
            tracing::error!(
                product = s.module.product.name(),
                "query evaluation task failed"
            );
            s.metrics.query(QueryOutcome::Unavailable);
            response::unavailable()
        }
    }
}
