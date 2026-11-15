//! Query framing (spec 10.4), the Receiver `RPQ1` framing with per-product
//! magics.
//!
//! ```text
//! request  = magic(4) || session_id(32) || nonce(16) || payload
//! response = the request's 52-byte header || reply
//! ```
//!
//! No length fields: the payload and reply lengths are fixed by the table's
//! geometry ([`Geometry::payload_len`], [`Geometry::reply_len`]).

use crate::constants::{HEADER_BYTES, MAX_QUERY_BODY_BYTES};
use crate::error_body::ErrorStatus;
use crate::geometry::Geometry;
use crate::product::Product;

/// A malformed query or response body.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// Over [`MAX_QUERY_BODY_BYTES`]: HTTP 413.
    #[error("body of {0} bytes is over the 512 KiB limit")]
    TooLarge(usize),
    /// Shorter than the 52-byte header.
    #[error("body of {0} bytes is shorter than the header")]
    Short(usize),
    /// Not this product's magic.
    #[error("wrong query magic")]
    Magic,
    /// Not the exact length the table's geometry defines.
    #[error("body is {actual} bytes, expected {expected}")]
    Length { expected: usize, actual: usize },
    /// A response whose first 52 bytes are not the request's header.
    #[error("response does not echo the request header")]
    HeaderMismatch,
}

impl FrameError {
    /// The HTTP error a host answers with (spec 10.5): 413 for an oversized
    /// body, otherwise 400. A client-side [`FrameError::HeaderMismatch`] maps
    /// to 400 as well, though no host sends it.
    pub fn status(&self) -> ErrorStatus {
        match self {
            FrameError::TooLarge(_) => ErrorStatus::PayloadTooLarge,
            _ => ErrorStatus::BadRequest,
        }
    }
}

/// The 52-byte header of a query, echoed by its response.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryHeader {
    pub magic: [u8; 4],
    pub session_id: [u8; 32],
    /// 16 fresh random bytes per query, chosen by the client.
    pub nonce: [u8; 16],
}

impl QueryHeader {
    /// A header for `product` with its magic.
    pub fn new(product: Product, session_id: [u8; 32], nonce: [u8; 16]) -> Self {
        Self {
            magic: product.query_magic(),
            session_id,
            nonce,
        }
    }

    /// The header bytes.
    pub fn encode(&self) -> [u8; HEADER_BYTES] {
        let mut out = [0u8; HEADER_BYTES];
        out[..4].copy_from_slice(&self.magic);
        out[4..36].copy_from_slice(&self.session_id);
        out[36..].copy_from_slice(&self.nonce);
        out
    }

    /// The header at the start of `bytes`, which may continue past it. No
    /// magic check.
    pub fn parse(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < HEADER_BYTES {
            return Err(FrameError::Short(bytes.len()));
        }
        Ok(Self {
            magic: bytes[..4].try_into().expect("magic"),
            session_id: bytes[4..36].try_into().expect("session id"),
            nonce: bytes[36..HEADER_BYTES].try_into().expect("nonce"),
        })
    }
}

/// `header || payload`.
pub fn encode_request(header: &QueryHeader, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_BYTES + payload.len());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(payload);
    out
}

/// First host check, before the session is known: size limit, header present
/// and this product's magic. Returns the header and the payload.
pub fn parse_request(product: Product, body: &[u8]) -> Result<(QueryHeader, &[u8]), FrameError> {
    if body.len() > MAX_QUERY_BODY_BYTES {
        return Err(FrameError::TooLarge(body.len()));
    }
    let header = QueryHeader::parse(body)?;
    if header.magic != product.query_magic() {
        return Err(FrameError::Magic);
    }
    Ok((header, &body[HEADER_BYTES..]))
}

/// Second host check, once the session's table is known: the body is exactly
/// `52 + payload_len` for its geometry.
pub fn check_request_len(geometry: Geometry, body_len: usize) -> Result<(), FrameError> {
    if body_len == geometry.request_len() {
        Ok(())
    } else {
        Err(FrameError::Length {
            expected: geometry.request_len(),
            actual: body_len,
        })
    }
}

/// Both request checks at once.
pub fn validate_request(
    product: Product,
    geometry: Geometry,
    body: &[u8],
) -> Result<(QueryHeader, &[u8]), FrameError> {
    let parsed = parse_request(product, body)?;
    check_request_len(geometry, body.len())?;
    Ok(parsed)
}

/// `header || reply`, as a host answers.
pub fn encode_response(header: &QueryHeader, reply: &[u8]) -> Vec<u8> {
    encode_request(header, reply)
}

/// Client check of a response: exact length for the geometry and the
/// request's header echoed. Returns the reply.
pub fn validate_response<'a>(
    request: &QueryHeader,
    geometry: Geometry,
    body: &'a [u8],
) -> Result<&'a [u8], FrameError> {
    if body.len() != geometry.response_len() {
        return Err(FrameError::Length {
            expected: geometry.response_len(),
            actual: body.len(),
        });
    }
    if body[..HEADER_BYTES] != request.encode() {
        return Err(FrameError::HeaderMismatch);
    }
    Ok(&body[HEADER_BYTES..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> QueryHeader {
        QueryHeader::new(Product::Tachygram, [7; 32], [9; 16])
    }

    #[test]
    fn header_layout() {
        let bytes = header().encode();
        assert_eq!(&bytes[..4], b"TGQ1");
        assert_eq!(&bytes[4..36], &[7; 32]);
        assert_eq!(&bytes[36..], &[9; 16]);
        assert_eq!(QueryHeader::parse(&bytes), Ok(header()));
        assert_eq!(QueryHeader::parse(&bytes[..51]), Err(FrameError::Short(51)));
    }

    #[test]
    fn request_checks() {
        let g = Geometry::Tachyon4k8;
        let body = encode_request(&header(), &vec![1; g.payload_len()]);
        assert_eq!(body.len(), 28_212);
        let (h, payload) = validate_request(Product::Tachygram, g, &body).unwrap();
        assert_eq!((h, payload.len()), (header(), 28_160));

        assert_eq!(
            validate_request(Product::Memo, g, &body),
            Err(FrameError::Magic)
        );
        assert_eq!(
            validate_request(Product::Tachygram, g, &body[..body.len() - 1]),
            Err(FrameError::Length {
                expected: 28_212,
                actual: 28_211
            })
        );
        assert!(validate_request(Product::Tachygram, Geometry::Tachyon2k8, &body).is_err());
        assert_eq!(
            validate_request(Product::Tachygram, g, &body[..10]),
            Err(FrameError::Short(10))
        );
        let huge = encode_request(&header(), &vec![0; 512 * 1024]);
        let err = parse_request(Product::Tachygram, &huge).unwrap_err();
        assert_eq!(err, FrameError::TooLarge(512 * 1024 + 52));
        assert_eq!(err.status().http_status(), 413);
        assert_eq!(FrameError::Magic.status().http_status(), 400);
        let at_limit = vec![0u8; 512 * 1024];
        assert_eq!(
            parse_request(Product::Memo, &at_limit),
            Err(FrameError::Magic)
        );
    }

    #[test]
    fn response_checks() {
        let g = Geometry::Tachyon4k8;
        let body = encode_response(&header(), &vec![3; g.reply_len()]);
        assert_eq!(body.len(), 10_292);
        assert_eq!(
            validate_response(&header(), g, &body).unwrap().len(),
            10_240
        );
        let other = QueryHeader {
            nonce: [8; 16],
            ..header()
        };
        assert_eq!(
            validate_response(&other, g, &body),
            Err(FrameError::HeaderMismatch)
        );
        assert!(matches!(
            validate_response(&header(), g, &body[..10_291]),
            Err(FrameError::Length { .. })
        ));
    }
}
