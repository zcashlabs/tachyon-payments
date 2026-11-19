//! The write API (spec 9.3): `GET challenge` and `POST register` under
//! `/registry/v1` and its legacy alias `/v1/registry`, mounted with
//! `pir_control::PublicMount`.
//!
//! A registration passes, in order:
//!
//! 1. the client's concurrent slot (`per_client_concurrent`, `429`);
//! 2. the global cap and the client's rate (sliding 60 s windows, `429` with
//!    `Retry-After`); only a request admitted here counts;
//! 3. the body: exactly 1,256 bytes within `upload_ms` (`400` shorter, `413`
//!    longer, `408` late);
//! 4. validation in the spec's order: challenge authentic and not expired,
//!    proof of work, `pk` canonical, `ek` modulus check, `key16` nonzero
//!    (each `400 invalid_request`, the message naming the check);
//! 5. the durable log append; the `200` is sent only after it returned.
//!
//! The client key is `pir_control::admission::client_key`: the first
//! `X-Forwarded-For` address, else `X-Real-IP`, else the connection's peer
//! address when the server exposes it (`ConnectInfo`), else one shared
//! `unknown` key. Nothing a client sends is logged or used as a label.
use crate::{
    challenge::{ChallengeKey, now_unix},
    core::Core,
    limits::{Limited, RateLimits},
    metrics::Outcome,
    validate::check_address,
};
use axum::{
    Json, Router,
    extract::{ConnectInfo, Request, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{MethodRouter, get, post},
};
use pir_control::{
    admission::{BodyError, ClientSlots, client_key, read_body, spawn_admitted},
    public_routes::PublicMount,
};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tachyon_pir_protocol::{
    Bytes32, Challenge, ErrorBody, ErrorStatus, Product, REGISTER_BODY_BYTES, RegisterRequest,
    pow_hash, pow_meets,
};

/// The registry's public mount (spec 10.1).
pub const MOUNT: PublicMount =
    PublicMount::new(Product::Registry.mount(), Product::Registry.legacy_mount());

/// The write API's state.
pub(crate) struct WriteApi {
    pub core: Arc<Core>,
    pub key: ChallengeKey,
    pub difficulty_bits: u32,
    pub slots: ClientSlots,
    pub limits: RateLimits,
    pub upload: Duration,
}

/// Mount `challenge` and `register` at both prefixes.
pub(crate) fn mount(router: Router, api: Arc<WriteApi>) -> Router {
    let challenge: MethodRouter = get(challenge)
        .fallback(method_not_allowed)
        .with_state(api.clone());
    let register: MethodRouter = post(register).fallback(method_not_allowed).with_state(api);
    MOUNT.mount(router, [("/challenge", challenge), ("/register", register)])
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// An error body with its status (spec 10.5), `no-store`, and `Retry-After`
/// when given.
pub(crate) fn refusal(status: ErrorStatus, message: &str, retry_after: Option<u64>) -> Response {
    let code =
        StatusCode::from_u16(status.http_status()).expect("spec statuses are valid HTTP statuses");
    let mut response = (code, Json(ErrorBody::new(status, message))).into_response();
    if let Some(seconds) = retry_after {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(seconds.max(1)));
    }
    no_store(response)
}

pub(crate) async fn method_not_allowed() -> Response {
    let mut response = refusal(ErrorStatus::BadRequest, "method not allowed", None);
    *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
    response
}

pub(crate) async fn not_found() -> Response {
    let mut response = refusal(ErrorStatus::BadRequest, "unknown route", None);
    *response.status_mut() = StatusCode::NOT_FOUND;
    response
}

fn limited(limited: Limited) -> Response {
    let message = match limited {
        Limited::Global(_) => "the registry's global rate is exhausted",
        Limited::Client(_) => "this client's registration rate is exhausted",
    };
    refusal(
        ErrorStatus::Overloaded,
        message,
        Some(limited.retry_after()),
    )
}

/// `GET challenge`: a fresh stateless challenge with the configured
/// difficulty. Limited only by the global cap.
async fn challenge(State(api): State<Arc<WriteApi>>) -> Response {
    if let Err(refused) = api.limits.admit_global(Instant::now()) {
        return limited(refused);
    }
    let (challenge, expires_unix) = api.key.issue(now_unix());
    no_store(
        Json(Challenge {
            challenge: Bytes32(challenge),
            difficulty_bits: api.difficulty_bits,
            expires_unix,
        })
        .into_response(),
    )
}

/// `POST register`.
async fn register(State(api): State<Arc<WriteApi>>, request: Request) -> Response {
    let (response, outcome) = registration(&api, request).await;
    api.core.metrics.registration(outcome);
    tracing::debug!(result = outcome.label(), "registry registration");
    response
}

async fn registration(api: &WriteApi, request: Request) -> (Response, Outcome) {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip());
    let client = client_key(request.headers(), peer);
    let Some(slot) = api.slots.try_acquire(&client) else {
        return (
            refusal(
                ErrorStatus::Overloaded,
                "this client has too many registrations in flight",
                Some(1),
            ),
            Outcome::Overloaded,
        );
    };
    if let Err(refused) = api.limits.admit(&client, Instant::now()) {
        return (limited(refused), Outcome::RateLimited);
    }
    let body = match read_body(request.into_body(), REGISTER_BODY_BYTES, api.upload).await {
        Ok(body) => body,
        Err(BodyError::Timeout) => {
            return (
                refusal(
                    ErrorStatus::UploadTimeout,
                    "registration body not received in time",
                    None,
                ),
                Outcome::UploadTimeout,
            );
        }
        Err(error) if error.over_limit() => {
            return (
                refusal(
                    ErrorStatus::PayloadTooLarge,
                    "registration body is longer than 1256 bytes",
                    None,
                ),
                Outcome::TooLarge,
            );
        }
        Err(_) => {
            return (
                refusal(
                    ErrorStatus::BadRequest,
                    "registration body could not be read",
                    None,
                ),
                Outcome::Invalid,
            );
        }
    };
    let invalid = |message: &str| {
        (
            refusal(ErrorStatus::BadRequest, message, None),
            Outcome::Invalid,
        )
    };
    let Ok(request) = RegisterRequest::parse(&body) else {
        return invalid("registration body must be exactly 1256 bytes");
    };
    if let Err(refused) = api.key.verify(&request.challenge, now_unix()) {
        return invalid(&refused.to_string());
    }
    let digest = request.address.digest();
    if !pow_meets(
        &pow_hash(&request.challenge, &request.nonce, &digest),
        api.difficulty_bits,
    ) {
        return (
            refusal(
                ErrorStatus::BadRequest,
                &format!("proof of work does not meet {} bits", api.difficulty_bits),
                None,
            ),
            Outcome::PowRejected,
        );
    }
    let key16: [u8; 16] = digest[..16].try_into().expect("16 bytes");
    if let Err(refused) = check_address(&request.address, &key16) {
        return invalid(&refused.to_string());
    }
    // The slot moves into the blocking append, so a client that disconnects
    // still holds it until the append ends.
    let store = api.core.store.clone();
    let address = request.address;
    let appended = spawn_admitted(slot, move |_| store.register(&address)).await;
    let registration = match appended {
        Ok((Ok(registration), _slot)) => registration,
        Ok((Err(registry_store::Error::ZeroKey { .. }), _)) => {
            return invalid("key16 is all zero");
        }
        Ok((Err(error), _)) => {
            tracing::error!(%error, "registry log append failed");
            return unavailable();
        }
        Err(error) => {
            tracing::error!(%error, "registry log append did not finish");
            return unavailable();
        }
    };
    api.core.grew(registration.sequence + 1);
    let outcome = if registration.created {
        Outcome::Created
    } else {
        Outcome::Existing
    };
    (
        no_store(Json(registration.response()).into_response()),
        outcome,
    )
}

fn unavailable() -> (Response, Outcome) {
    (
        refusal(
            ErrorStatus::Unavailable,
            "the registry cannot record registrations now",
            Some(1),
        ),
        Outcome::Unavailable,
    )
}
