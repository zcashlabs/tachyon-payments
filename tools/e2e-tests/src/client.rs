//! HTTP clients and request helpers for end-to-end tests. Every helper panics
//! with the URL, the status and the body when the answer is not what the test
//! expected, so a failure says what the server said.
use bytes::Bytes;
use reqwest::header::HeaderMap;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use std::time::Duration;

/// Per-request timeout of the clients built here.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// An HTTPS client that trusts `cert_pem` (a PEM certificate, such as the TLS
/// edge's) and no built-in root, and follows no redirect.
pub fn trusting(cert_pem: &[u8]) -> Client {
    let certificate = reqwest::Certificate::from_pem(cert_pem).expect("a PEM certificate");
    Client::builder()
        .tls_built_in_root_certs(false)
        .add_root_certificate(certificate)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()
        .expect("build the HTTPS client")
}

/// A plain-HTTP client (management and private listeners) that follows no
/// redirect.
pub fn plain() -> Client {
    Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()
        .expect("build the HTTP client")
}

/// A whole response.
#[derive(Clone, Debug)]
pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Reply {
    /// The body as text (lossy), for messages.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// The body parsed as JSON.
    pub fn json<T: DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.body).unwrap_or_else(|error| {
            panic!("body is not the expected JSON ({error}): {}", self.text())
        })
    }

    /// A header as text.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    /// Panics unless the status is `expected`.
    pub fn assert_status(&self, expected: StatusCode, url: &str) -> &Self {
        assert_eq!(
            self.status,
            expected,
            "{url} answered {} with body {:?}",
            self.status,
            self.text()
        );
        self
    }
}

async fn collect(url: &str, request: reqwest::RequestBuilder) -> Reply {
    let response = request
        .send()
        .await
        .unwrap_or_else(|error| panic!("request to {url} failed: {error:?}"));
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("reading the body from {url} failed: {error:?}"));
    Reply {
        status,
        headers,
        body,
    }
}

/// `GET url`, whatever the status.
pub async fn get(client: &Client, url: &str) -> Reply {
    collect(url, client.get(url)).await
}

/// `GET url`, asserting the status, returning the body.
pub async fn get_bytes(client: &Client, url: &str, expected: StatusCode) -> Bytes {
    let reply = get(client, url).await;
    reply.assert_status(expected, url);
    reply.body
}

/// `GET url`, asserting the status, returning the body parsed as JSON.
pub async fn get_json<T: DeserializeOwned>(client: &Client, url: &str, expected: StatusCode) -> T {
    let reply = get(client, url).await;
    reply.assert_status(expected, url);
    reply.json()
}

/// `POST url` with a raw body (a PIR query, a registration) and its content type,
/// whatever the status.
pub async fn post_bytes(
    client: &Client,
    url: &str,
    content_type: &str,
    body: impl Into<reqwest::Body>,
) -> Reply {
    let request = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(body);
    collect(url, request).await
}

/// `POST url` with an `application/octet-stream` body, whatever the status.
pub async fn post_octets(client: &Client, url: &str, body: impl Into<reqwest::Body>) -> Reply {
    post_bytes(client, url, "application/octet-stream", body).await
}

/// `POST url` with a JSON body, asserting the status, returning the body parsed
/// as JSON.
pub async fn post_json<T: DeserializeOwned>(
    client: &Client,
    url: &str,
    body: &serde_json::Value,
    expected: StatusCode,
) -> T {
    let reply = post_bytes(
        client,
        url,
        "application/json",
        serde_json::to_vec(body).expect("serialize the JSON body"),
    )
    .await;
    reply.assert_status(expected, url);
    reply.json()
}
