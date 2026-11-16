//! Everything a probe can fail with, and the mapping of the spec 10.5 error
//! responses onto typed variants.

use std::time::Duration;

use tachyon_pir_protocol::{
    Bytes32, ErrorBody, ErrorStatus, FrameError, MapError, PlacementError, RowError,
};

use crate::map_view::ScopeSelector;

/// The route a request went to, for error reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// `GET map`.
    Map,
    /// `GET map/{base}/{sha256}`.
    MapChunk,
    /// `GET public/{session_id}`.
    Public,
    /// `POST query`.
    Query,
    /// The node's JSON-RPC endpoint (anchor check).
    NodeRpc,
}

impl Route {
    pub const fn name(self) -> &'static str {
        match self {
            Route::Map => "map",
            Route::MapChunk => "map chunk",
            Route::Public => "public",
            Route::Query => "query",
            Route::NodeRpc => "node rpc",
        }
    }
}

impl std::fmt::Display for Route {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A probe failure. The variants for HTTP statuses follow the client actions
/// of spec 10.5; only `StaleRouting` is ever retried, once, by a lookup.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("origin {0:?} is not an http(s) URL")]
    Origin(String),
    #[error("CA certificate: {0}")]
    Certificate(String),
    #[error("{route}: transport: {source}")]
    Transport {
        route: Route,
        #[source]
        source: reqwest::Error,
    },
    /// 400 or 413 `invalid_request`: do not retry.
    #[error("{route}: invalid request (HTTP {status}): {}", body.message)]
    InvalidRequest {
        route: Route,
        status: u16,
        body: ErrorBody,
    },
    /// 408 `upload_timeout`.
    #[error("{route}: upload timeout: {}", body.message)]
    UploadTimeout { route: Route, body: ErrorBody },
    /// 409 `stale_routing`: the map is outdated. A lookup refetches the map
    /// and retries once; a second 409 is returned as this error.
    #[error("{route}: stale routing, current map {map_sha256}")]
    StaleRouting {
        route: Route,
        map_sha256: Bytes32,
        body: ErrorBody,
    },
    /// 410 `session_unavailable`: revoked by a reorg. Refetch the map and
    /// accept its anchor again before using it.
    #[error("{route}: session revoked: {}", body.message)]
    SessionRevoked { route: Route, body: ErrorBody },
    /// 429 `overloaded`: retry after the delay.
    #[error("{route}: overloaded, retry after {retry_after:?}")]
    Overloaded {
        route: Route,
        retry_after: Option<Duration>,
        body: ErrorBody,
    },
    /// 503 `temporarily_unavailable`: retry later.
    #[error("{route}: temporarily unavailable: {}", body.message)]
    Unavailable {
        route: Route,
        retry_after: Option<Duration>,
        body: ErrorBody,
    },
    /// A spec 10.5 status whose body is not the matching error body.
    #[error("{route}: HTTP {status} with a malformed error body: {reason}")]
    BadErrorBody {
        route: Route,
        status: u16,
        reason: String,
    },
    /// A status outside spec 10.5.
    #[error("{route}: unexpected HTTP {status}: {body}")]
    UnexpectedStatus {
        route: Route,
        status: u16,
        body: String,
    },
    #[error("{route}: response over {limit} bytes")]
    TooLarge { route: Route, limit: usize },
    /// `X-Map-Sha256` missing or not the SHA-256 of the body.
    #[error("map: X-Map-Sha256 {header:?} is not the body digest {actual}")]
    MapHeader {
        header: Option<String>,
        actual: Bytes32,
    },
    #[error("map: {0}")]
    Map(#[from] MapError),
    /// A map chunk whose bytes do not hash to the head's reference.
    #[error("map chunk {base}: digest mismatch")]
    ChunkDigest { base: u64 },
    /// A map document that is valid JSON but not the canonical encoding.
    #[error("{0}: not the canonical compact encoding")]
    NonCanonical(&'static str),
    /// A map naming another chain than the first map this client accepted.
    #[error("map names network {network} genesis {genesis}, not the chain first accepted")]
    ChainChanged { network: String, genesis: String },
    /// Public setup bytes of the wrong length.
    #[error("public {session_id}: {actual} bytes, expected {expected}")]
    PublicLength {
        session_id: Bytes32,
        expected: usize,
        actual: usize,
    },
    /// Public setup bytes whose SHA-256 is not the table's `public_sha256`.
    #[error("public {session_id}: sha256 {actual} is not {expected}")]
    PublicDigest {
        session_id: Bytes32,
        expected: Bytes32,
        actual: Bytes32,
    },
    /// A query response of the wrong length or without the echoed header.
    #[error("query response: {0}")]
    Frame(#[from] FrameError),
    /// The PIR engine refused to build or decode.
    #[error("pir: {0}")]
    Pir(String),
    /// A decoded row that is not canonical (spec 6.2, 8.4).
    #[error("row {row} of bucket {bucket} segment {segment}: {source}")]
    Row {
        bucket: u32,
        segment: u32,
        row: u32,
        #[source]
        source: RowError,
    },
    #[error("placement: {0}")]
    Placement(#[from] PlacementError),
    #[error("key is {actual} bytes, expected {expected}")]
    KeyLength { expected: usize, actual: usize },
    #[error("scope {0} is not listed in the map")]
    UnknownScope(ScopeSelector),
    /// Spec 11.5: no session exists for a scope below the floor.
    #[error("scope index {scope_index} is below this provider's floor {floor_scope}")]
    BelowFloor { scope_index: u64, floor_scope: u64 },
    /// A dummy query needs a table; an empty scope has none (spec 10.3).
    #[error("scope index {0} has no tables")]
    EmptyScope(u64),
    /// Spec 8.4: a tachygram is found at most once in its scope.
    #[error("tachygram found {0} times in one scope")]
    DuplicateTachygram(usize),
    #[error("registry maps carry no anchor")]
    NotChain,
    #[error("node rpc: {0}")]
    Rpc(String),
    /// The node's block hash at a height differs from the map's.
    #[error("node block {height} is {node}, map says {map}")]
    AnchorMismatch {
        what: &'static str,
        height: u32,
        map: String,
        node: String,
    },
}

impl ClientError {
    /// A short stable name for JSON reports.
    pub fn kind(&self) -> &'static str {
        match self {
            ClientError::Origin(_) => "origin",
            ClientError::Certificate(_) => "certificate",
            ClientError::Transport { .. } => "transport",
            ClientError::InvalidRequest { .. } => "invalid_request",
            ClientError::UploadTimeout { .. } => "upload_timeout",
            ClientError::StaleRouting { .. } => "stale_routing",
            ClientError::SessionRevoked { .. } => "session_revoked",
            ClientError::Overloaded { .. } => "overloaded",
            ClientError::Unavailable { .. } => "unavailable",
            ClientError::BadErrorBody { .. } => "bad_error_body",
            ClientError::UnexpectedStatus { .. } => "unexpected_status",
            ClientError::TooLarge { .. } => "too_large",
            ClientError::MapHeader { .. } => "map_header",
            ClientError::Map(_) => "map",
            ClientError::ChunkDigest { .. } => "chunk_digest",
            ClientError::NonCanonical(_) => "non_canonical",
            ClientError::ChainChanged { .. } => "chain_changed",
            ClientError::PublicLength { .. } => "public_length",
            ClientError::PublicDigest { .. } => "public_digest",
            ClientError::Frame(_) => "frame",
            ClientError::Pir(_) => "pir",
            ClientError::Row { .. } => "row",
            ClientError::Placement(_) => "placement",
            ClientError::KeyLength { .. } => "key_length",
            ClientError::UnknownScope(_) => "unknown_scope",
            ClientError::BelowFloor { .. } => "below_floor",
            ClientError::EmptyScope(_) => "empty_scope",
            ClientError::DuplicateTachygram(_) => "duplicate_tachygram",
            ClientError::NotChain => "not_chain",
            ClientError::Rpc(_) => "rpc",
            ClientError::AnchorMismatch { .. } => "anchor_mismatch",
        }
    }

    /// The server's `Retry-After`, for 429 and 503.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            ClientError::Overloaded { retry_after, .. }
            | ClientError::Unavailable { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// Error bodies are small; anything longer is not one.
pub(crate) const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

/// `Retry-After` as delay-seconds (the only form spec 10.5 uses).
pub fn parse_retry_after(value: Option<&str>) -> Option<Duration> {
    value
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

/// Maps a non-200 response to its typed error (spec 10.5). A status of the
/// spec's table must carry the matching `{"code","message"}` body, with
/// `map_sha256` exactly for 409; anything else is [`ClientError::BadErrorBody`].
pub fn status_error(
    route: Route,
    status: u16,
    retry_after: Option<&str>,
    body: &[u8],
) -> ClientError {
    let Some(expected) = ErrorStatus::from_http_status(status) else {
        return ClientError::UnexpectedStatus {
            route,
            status,
            body: String::from_utf8_lossy(&body[..body.len().min(512)]).into_owned(),
        };
    };
    let parsed: ErrorBody = match serde_json::from_slice(body) {
        Ok(parsed) => parsed,
        Err(e) => {
            return ClientError::BadErrorBody {
                route,
                status,
                reason: e.to_string(),
            };
        }
    };
    if parsed.code != expected.code() {
        return ClientError::BadErrorBody {
            route,
            status,
            reason: format!("code {} for HTTP {status}", parsed.code.as_str()),
        };
    }
    if parsed.validate().is_err() {
        return ClientError::BadErrorBody {
            route,
            status,
            reason: "map_sha256 must be present exactly for stale_routing".into(),
        };
    }
    let retry_after = parse_retry_after(retry_after);
    match expected {
        ErrorStatus::BadRequest | ErrorStatus::PayloadTooLarge => ClientError::InvalidRequest {
            route,
            status,
            body: parsed,
        },
        ErrorStatus::UploadTimeout => ClientError::UploadTimeout {
            route,
            body: parsed,
        },
        ErrorStatus::StaleRouting => ClientError::StaleRouting {
            route,
            map_sha256: parsed.map_sha256.expect("validated"),
            body: parsed,
        },
        ErrorStatus::SessionUnavailable => ClientError::SessionRevoked {
            route,
            body: parsed,
        },
        ErrorStatus::Overloaded => ClientError::Overloaded {
            route,
            retry_after,
            body: parsed,
        },
        ErrorStatus::Unavailable => ClientError::Unavailable {
            route,
            retry_after,
            body: parsed,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_pir_protocol::ErrorCode;

    fn body(code: &str, extra: &str) -> Vec<u8> {
        format!(r#"{{"code":"{code}","message":"m"{extra}}}"#).into_bytes()
    }

    #[test]
    fn spec_10_5_statuses_map_to_typed_errors() {
        let map = format!(r#","map_sha256":"{}""#, "ab".repeat(32));
        let cases: [(u16, &str, &str, &str); 7] = [
            (400, "invalid_request", "", "invalid_request"),
            (408, "upload_timeout", "", "upload_timeout"),
            (409, "stale_routing", &map, "stale_routing"),
            (410, "session_unavailable", "", "session_revoked"),
            (413, "invalid_request", "", "invalid_request"),
            (429, "overloaded", "", "overloaded"),
            (503, "temporarily_unavailable", "", "unavailable"),
        ];
        for (status, code, extra, kind) in cases {
            let error = status_error(Route::Query, status, Some("1"), &body(code, extra));
            assert_eq!(error.kind(), kind, "HTTP {status}");
        }
        match status_error(Route::Query, 409, None, &body("stale_routing", &map)) {
            ClientError::StaleRouting {
                map_sha256, body, ..
            } => {
                assert_eq!(map_sha256, Bytes32([0xab; 32]));
                assert_eq!(body.code, ErrorCode::StaleRouting);
            }
            other => panic!("{other:?}"),
        }
        match status_error(Route::Query, 413, None, &body("invalid_request", "")) {
            ClientError::InvalidRequest { status, .. } => assert_eq!(status, 413),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn retry_after_is_carried_for_429_and_503() {
        let e = status_error(Route::Query, 429, Some(" 1 "), &body("overloaded", ""));
        assert_eq!(e.retry_after(), Some(Duration::from_secs(1)));
        let e = status_error(
            Route::Map,
            503,
            Some("7"),
            &body("temporarily_unavailable", ""),
        );
        assert_eq!(e.retry_after(), Some(Duration::from_secs(7)));
        let e = status_error(Route::Query, 429, None, &body("overloaded", ""));
        assert!(matches!(
            e,
            ClientError::Overloaded {
                retry_after: None,
                ..
            }
        ));
        let e = status_error(
            Route::Query,
            429,
            Some("Wed, 21 Oct 2015 07:28:00 GMT"),
            &body("overloaded", ""),
        );
        assert_eq!(e.retry_after(), None);
    }

    #[test]
    fn malformed_error_bodies_are_refused() {
        // Code that does not match the status.
        let e = status_error(Route::Query, 410, None, &body("overloaded", ""));
        assert_eq!(e.kind(), "bad_error_body");
        // 409 without map_sha256.
        let e = status_error(Route::Query, 409, None, &body("stale_routing", ""));
        assert_eq!(e.kind(), "bad_error_body");
        // map_sha256 on a non-409.
        let extra = format!(r#","map_sha256":"{}""#, "00".repeat(32));
        let e = status_error(Route::Query, 400, None, &body("invalid_request", &extra));
        assert_eq!(e.kind(), "bad_error_body");
        // Unknown field, not JSON, empty.
        let e = status_error(
            Route::Query,
            503,
            None,
            &body("temporarily_unavailable", r#","retry":1"#),
        );
        assert_eq!(e.kind(), "bad_error_body");
        assert_eq!(
            status_error(Route::Query, 400, None, b"<html>").kind(),
            "bad_error_body"
        );
        assert_eq!(
            status_error(Route::Query, 400, None, b"").kind(),
            "bad_error_body"
        );
    }

    #[test]
    fn statuses_outside_the_table_are_unexpected() {
        for status in [301, 404, 500, 502] {
            match status_error(Route::Public, status, None, b"nope") {
                ClientError::UnexpectedStatus {
                    status: s, body, ..
                } => {
                    assert_eq!((s, body.as_str()), (status, "nope"));
                }
                other => panic!("{other:?}"),
            }
        }
    }
}
