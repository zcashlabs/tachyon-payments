//! Error bodies of the wallet-facing routes (spec 10.5), Enhance's vocabulary.
//!
//! JSON `{"code": "...", "message": "..."}`; a `409 stale_routing` body also
//! carries `map_sha256`, and only it does.

use serde::{Deserialize, Serialize};

use crate::bytes::Bytes32;

/// The `code` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidRequest,
    UploadTimeout,
    StaleRouting,
    SessionUnavailable,
    Overloaded,
    TemporarilyUnavailable,
}

impl ErrorCode {
    /// The wire string.
    pub const fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InvalidRequest => "invalid_request",
            ErrorCode::UploadTimeout => "upload_timeout",
            ErrorCode::StaleRouting => "stale_routing",
            ErrorCode::SessionUnavailable => "session_unavailable",
            ErrorCode::Overloaded => "overloaded",
            ErrorCode::TemporarilyUnavailable => "temporarily_unavailable",
        }
    }
}

/// One row of the spec 10.5 table: an HTTP status with its code. `400` and
/// `413` share `invalid_request`, so the status, not the code, is the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorStatus {
    /// 400: wrong length, bad magic, malformed body. Do not retry.
    BadRequest,
    /// 408: body not received in time. Retry.
    UploadTimeout,
    /// 409: session of a map this server no longer holds. Refetch the map,
    /// retry once.
    StaleRouting,
    /// 410: session revoked by a reorg. Refetch the map, re-accept the anchor.
    SessionUnavailable,
    /// 413: body over the limit. Do not retry.
    PayloadTooLarge,
    /// 429 with `Retry-After: 1`: admission refused. Retry later.
    Overloaded,
    /// 503: not ready or no serving authority. Retry later.
    Unavailable,
}

impl ErrorStatus {
    /// Every status, in table order.
    pub const ALL: [ErrorStatus; 7] = [
        ErrorStatus::BadRequest,
        ErrorStatus::UploadTimeout,
        ErrorStatus::StaleRouting,
        ErrorStatus::SessionUnavailable,
        ErrorStatus::PayloadTooLarge,
        ErrorStatus::Overloaded,
        ErrorStatus::Unavailable,
    ];

    /// The HTTP status code.
    pub const fn http_status(self) -> u16 {
        match self {
            ErrorStatus::BadRequest => 400,
            ErrorStatus::UploadTimeout => 408,
            ErrorStatus::StaleRouting => 409,
            ErrorStatus::SessionUnavailable => 410,
            ErrorStatus::PayloadTooLarge => 413,
            ErrorStatus::Overloaded => 429,
            ErrorStatus::Unavailable => 503,
        }
    }

    /// The body's `code`.
    pub const fn code(self) -> ErrorCode {
        match self {
            ErrorStatus::BadRequest | ErrorStatus::PayloadTooLarge => ErrorCode::InvalidRequest,
            ErrorStatus::UploadTimeout => ErrorCode::UploadTimeout,
            ErrorStatus::StaleRouting => ErrorCode::StaleRouting,
            ErrorStatus::SessionUnavailable => ErrorCode::SessionUnavailable,
            ErrorStatus::Overloaded => ErrorCode::Overloaded,
            ErrorStatus::Unavailable => ErrorCode::TemporarilyUnavailable,
        }
    }

    /// The `Retry-After` header value in seconds, where the spec sets one.
    pub const fn retry_after_seconds(self) -> Option<u32> {
        match self {
            ErrorStatus::Overloaded => Some(1),
            _ => None,
        }
    }

    /// The status for an HTTP status code, if it is one of the table's.
    pub fn from_http_status(status: u16) -> Option<ErrorStatus> {
        ErrorStatus::ALL
            .into_iter()
            .find(|s| s.http_status() == status)
    }
}

/// An error body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorBody {
    pub code: ErrorCode,
    /// Human-readable; never echoes client bytes or internal error text.
    pub message: String,
    /// The current map head's digest; present exactly for `stale_routing`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub map_sha256: Option<Bytes32>,
}

/// A body whose `map_sha256` presence does not match its code.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("map_sha256 must be present exactly for stale_routing")]
pub struct ErrorBodyMismatch;

impl ErrorBody {
    /// A body for `status`; use [`ErrorBody::stale_routing`] for 409.
    pub fn new(status: ErrorStatus, message: impl Into<String>) -> Self {
        Self {
            code: status.code(),
            message: message.into(),
            map_sha256: None,
        }
    }

    /// The 409 body naming the current map.
    pub fn stale_routing(message: impl Into<String>, map_sha256: Bytes32) -> Self {
        Self {
            code: ErrorCode::StaleRouting,
            message: message.into(),
            map_sha256: Some(map_sha256),
        }
    }

    /// Checks `map_sha256` is present exactly for `stale_routing`.
    pub fn validate(&self) -> Result<(), ErrorBodyMismatch> {
        if (self.code == ErrorCode::StaleRouting) == self.map_sha256.is_some() {
            Ok(())
        } else {
            Err(ErrorBodyMismatch)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_of_spec_10_5() {
        let expect = [
            (400, "invalid_request"),
            (408, "upload_timeout"),
            (409, "stale_routing"),
            (410, "session_unavailable"),
            (413, "invalid_request"),
            (429, "overloaded"),
            (503, "temporarily_unavailable"),
        ];
        for (status, (http, code)) in ErrorStatus::ALL.into_iter().zip(expect) {
            assert_eq!(status.http_status(), http);
            assert_eq!(status.code().as_str(), code);
            assert_eq!(
                serde_json::to_string(&status.code()).unwrap(),
                format!("\"{code}\"")
            );
            assert_eq!(ErrorStatus::from_http_status(http), Some(status));
        }
        assert_eq!(ErrorStatus::Overloaded.retry_after_seconds(), Some(1));
        assert_eq!(ErrorStatus::Unavailable.retry_after_seconds(), None);
        assert_eq!(ErrorStatus::from_http_status(500), None);
    }

    #[test]
    fn bodies_serialize_and_refuse_unknown_fields() {
        let body = ErrorBody::new(ErrorStatus::BadRequest, "wrong length");
        let json = serde_json::to_string(&body).unwrap();
        assert_eq!(
            json,
            r#"{"code":"invalid_request","message":"wrong length"}"#
        );
        assert_eq!(serde_json::from_str::<ErrorBody>(&json).unwrap(), body);
        body.validate().unwrap();

        let stale = ErrorBody::stale_routing("refetch the map", Bytes32([0xaa; 32]));
        let json = serde_json::to_string(&stale).unwrap();
        assert_eq!(
            json,
            format!(
                r#"{{"code":"stale_routing","message":"refetch the map","map_sha256":"{}"}}"#,
                "aa".repeat(32)
            )
        );
        assert_eq!(serde_json::from_str::<ErrorBody>(&json).unwrap(), stale);
        stale.validate().unwrap();

        assert!(
            serde_json::from_str::<ErrorBody>(
                r#"{"code":"invalid_request","message":"x","retry":1}"#
            )
            .is_err()
        );
        assert!(serde_json::from_str::<ErrorBody>(r#"{"code":"teapot","message":"x"}"#).is_err());
        let missing: ErrorBody =
            serde_json::from_str(r#"{"code":"stale_routing","message":"x"}"#).unwrap();
        assert_eq!(missing.validate(), Err(ErrorBodyMismatch));
        let extra = ErrorBody {
            map_sha256: Some(Bytes32([1; 32])),
            ..body
        };
        assert_eq!(extra.validate(), Err(ErrorBodyMismatch));
    }
}
