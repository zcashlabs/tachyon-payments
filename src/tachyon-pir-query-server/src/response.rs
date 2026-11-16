//! Response helpers: the error bodies of spec 10.5, exactly as
//! `tachyon_pir_protocol::ErrorBody` encodes them, and the cache headers of
//! spec 10.1. Messages are fixed strings: no client byte and no internal
//! error text is ever echoed.

use axum::{
    body::Body,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use tachyon_pir_protocol::{Bytes32, ErrorBody, ErrorStatus};

/// `Cache-Control` of everything but immutable objects.
pub const NO_STORE: &str = "no-store";
/// `Cache-Control` of digest-addressed objects: map chunks and public setups.
pub const IMMUTABLE: &str = "public, max-age=31536000, immutable";

fn json_error(status: ErrorStatus, body: &ErrorBody) -> Response {
    let bytes = serde_json::to_vec(body).expect("an error body serializes");
    let code = StatusCode::from_u16(status.http_status()).expect("spec 10.5 statuses are valid");
    let mut response = (
        code,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, NO_STORE),
        ],
        bytes,
    )
        .into_response();
    // 429 must and 503 may carry `Retry-After` (spec 10.5); both send 1 s.
    let retry = status
        .retry_after_seconds()
        .or((status == ErrorStatus::Unavailable).then_some(1));
    if let Some(seconds) = retry {
        response.headers_mut().insert(
            header::RETRY_AFTER,
            HeaderValue::from_str(&seconds.to_string()).expect("a number is a header value"),
        );
    }
    response
}

/// An error of `status` with a fixed message. Use [`stale`] for `409`.
pub fn error(status: ErrorStatus, message: &'static str) -> Response {
    debug_assert!(status != ErrorStatus::StaleRouting);
    json_error(status, &ErrorBody::new(status, message))
}

/// `409 stale_routing` naming the current map head digest.
pub fn stale(map_sha256: &str) -> Response {
    let mut digest = [0u8; 32];
    match hex::decode_to_slice(map_sha256, &mut digest) {
        Ok(()) => json_error(
            ErrorStatus::StaleRouting,
            &ErrorBody::stale_routing(
                "session is not in a map this server holds; refetch the map",
                Bytes32(digest),
            ),
        ),
        // Inventory digests are validated hex; this cannot happen.
        Err(_) => unavailable(),
    }
}

pub fn unavailable() -> Response {
    error(
        ErrorStatus::Unavailable,
        "this product is not serving a publication now; retry later",
    )
}

pub fn overloaded() -> Response {
    error(
        ErrorStatus::Overloaded,
        "query admission refused; retry later",
    )
}

pub fn revoked() -> Response {
    error(
        ErrorStatus::SessionUnavailable,
        "session was revoked by a reorg; refetch the map and re-accept its anchor",
    )
}

/// A successful response with `content_type` and `cache_control`.
pub fn ok(content_type: &'static str, cache_control: &'static str, body: Body) -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, cache_control),
        ],
        body,
    )
        .into_response()
}

/// Marks every response without a `Cache-Control` header `no-store` (spec
/// 10.1), and gives axum's own refusals (404 for an unknown path, 405 for a
/// wrong method) an error body, so every error a wallet sees carries one.
pub async fn default_no_store(mut response: Response) -> Response {
    let status = response.status();
    let has_json = response
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes().starts_with(b"application/json"));
    if (status.is_client_error() || status.is_server_error()) && !has_json {
        let body = ErrorBody::new(ErrorStatus::BadRequest, "no such route or method");
        let bytes = serde_json::to_vec(&body).expect("an error body serializes");
        let mut replaced =
            (status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response();
        for name in [header::ALLOW, header::RETRY_AFTER] {
            if let Some(value) = response.headers().get(&name) {
                replaced.headers_mut().insert(name, value.clone());
            }
        }
        response = replaced;
    }
    response
        .headers_mut()
        .entry(header::CACHE_CONTROL)
        .or_insert(HeaderValue::from_static(NO_STORE));
    response
}
