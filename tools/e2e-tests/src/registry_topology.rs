//! The end-to-end topology of the registry (spec 9, 13.1): the publisher
//! binary with the registry enabled, the TLS edge in front of it, the
//! query-server binary trusting the edge, the probe client as the wallet,
//! and the test itself as the registrant.
//!
//! ```text
//! test (registrant) ──HTTP + X-Forwarded-For──▶ registry listener ─┐
//!                                                                    tachyon-pir-publisher-test ──HTTP──▶ TlsEdge ──HTTPS──▶ tachyon-pir-query-server-test ◀── Client
//! NodeDouble ──RPC (start-up identity only)──────────────────────────┘
//! ```
//!
//! The registry has no chain source. Its log grows only through the write
//! API (spec 9.3), which the test calls directly on the registry listener,
//! **playing the reverse proxy** spec 9.3 requires: every request carries
//! an `X-Forwarded-For` the test sets, so a test chooses which client a
//! request comes from ([`WriteApi::as_client`]). The edge also routes
//! `/registry/v1/` to the registry listener (it overwrites
//! `X-Forwarded-For` with the peer address, as that proxy must), and
//! [`RegistryTopology::write_via_edge`] reaches the write API through it.
//! The node double serves a genesis-only chain: the publisher reads the
//! network identity from it once at start-up (spec 9.2 Identity) and the
//! registry follows no blocks.
use crate::budget::Budget;
use crate::client::{self, Reply};
use crate::node::start_node;
use crate::process::{Process, free_addrs};
use crate::registry_oracle::{Ack, OracleAddress, REGISTER_BODY_BYTES, REGISTRY_CHALLENGE_SECONDS};
use crate::tls_edge::TlsEdge;
use crate::topology::{Publication, describe, metric_value, wait_for, wait_limit};
use futures_util::StreamExt as _;
use futures_util::stream;
use reqwest::StatusCode;
use serde_json::{Map, Value, json};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tachyon_fixtures::{ChainConfig, NodeDouble, SyntheticChain, display};
use tachyon_pir_cli::{Client, MapHead, MapView};
use tachyon_pir_distribution::{Current, Inventory, Subject};
use tachyon_pir_protocol::{BlockHash, Product, RegistryMapChunk, RegistryMapHead, RegistryScope};

/// The registry as both hosts name it: configuration section, product.
pub const SECTION: &str = "registry";
pub const PRODUCT: Product = Product::Registry;

/// `registry.settings` of the publisher beyond `listen` and `state_dir`
/// (spec 9.2, 9.3, 11.7; `registry_publisher::RegistrySettings`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegistrySettings {
    pub difficulty_bits: u32,
    pub per_client_concurrent: usize,
    pub per_client_per_minute: u32,
    pub global_per_minute: u32,
    pub upload_ms: u64,
    pub publish_ms: u64,
}

impl RegistrySettings {
    /// For tests that register in bulk: a 4-bit proof of work (16 hashes on
    /// average), limits far above any test's volume, and a 100 ms
    /// publication cadence.
    pub fn bulk() -> Self {
        Self {
            difficulty_bits: 4,
            per_client_concurrent: 256,
            per_client_per_minute: 1_000_000,
            global_per_minute: 1_000_000,
            upload_ms: 15_000,
            publish_ms: 100,
        }
    }

    fn json(&self, listen: SocketAddr, state_dir: &Path) -> Value {
        json!({
            "listen": listen.to_string(),
            "state_dir": state_dir,
            "difficulty_bits": self.difficulty_bits,
            "per_client_concurrent": self.per_client_concurrent,
            "per_client_per_minute": self.per_client_per_minute,
            "global_per_minute": self.global_per_minute,
            "upload_ms": self.upload_ms,
            "publish_ms": self.publish_ms,
        })
    }
}

/// The network identity the registry is bound to (spec 9.2): the node's
/// chain name and genesis hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub network: String,
    /// Protocol order.
    pub genesis: [u8; 32],
}

impl Identity {
    pub fn of(chain: &SyntheticChain) -> Self {
        Self {
            network: chain.chain_name().to_owned(),
            genesis: chain.genesis_hash(),
        }
    }

    /// The genesis hash in display order, as the node prints it.
    pub fn genesis_display(&self) -> String {
        display(&self.genesis)
    }

    pub fn block_hash(&self) -> BlockHash {
        BlockHash::from_protocol_bytes(self.genesis)
    }
}

/// The genesis-only chain the node double serves: identity only.
pub fn identity_chain(seed: u64) -> SyntheticChain {
    SyntheticChain::new(ChainConfig::with_seed(seed))
}

// ---------------------------------------------------------------------------
// The write API.

/// A challenge as `GET /registry/v1/challenge` issued it, its format checked
/// (spec 9.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Issued {
    pub challenge: [u8; 32],
    pub difficulty_bits: u32,
    pub expires_unix: u64,
}

/// Seconds since the epoch.
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs()
}

/// `challenge(32) || nonce(8) || addr(1216)` (spec 9.3).
pub fn register_body(challenge: &[u8; 32], nonce: &[u8; 8], address: &OracleAddress) -> Vec<u8> {
    let mut body = Vec::with_capacity(REGISTER_BODY_BYTES);
    body.extend_from_slice(challenge);
    body.extend_from_slice(nonce);
    body.extend_from_slice(&address.bytes());
    assert_eq!(body.len(), REGISTER_BODY_BYTES);
    body
}

/// A nonce whose proof of work meets `bits` for `address` (the protocol
/// crate's client helper).
pub fn solve(challenge: &[u8; 32], address: &OracleAddress, bits: u32) -> [u8; 8] {
    tachyon_pir_protocol::registry::solve_pow(challenge, &address.digest(), bits)
        .expect("a solution")
}

/// The leading zero bits of the proof-of-work hash of spec 9.3, computed
/// here from the spec: `SHA-256("tachyon-pir/registry/v1/pow\0" ||
/// challenge || nonce || digest)`.
pub fn pow_bits(challenge: &[u8; 32], nonce: &[u8; 8], address: &OracleAddress) -> u32 {
    use sha2::Digest as _;
    let hash: [u8; 32] = sha2::Sha256::new()
        .chain_update(b"tachyon-pir/registry/v1/pow\0")
        .chain_update(challenge)
        .chain_update(nonce)
        .chain_update(address.digest())
        .finalize()
        .into();
    let mut bits = 0;
    for byte in hash {
        if byte == 0 {
            bits += 8;
        } else {
            return bits + byte.leading_zeros();
        }
    }
    bits
}

/// A nonce whose proof of work falls short of `bits` (which must be
/// positive).
pub fn unsolved(challenge: &[u8; 32], address: &OracleAddress, bits: u32) -> [u8; 8] {
    assert!(bits > 0, "every nonce meets 0 bits");
    (0u64..)
        .map(u64::to_le_bytes)
        .find(|nonce| pow_bits(challenge, nonce, address) < bits)
        .expect("a failing nonce")
}

/// Parses a `200` registration body (spec 9.3): exactly `chunk_id`,
/// `digest` (64 lowercase hex), `created` and `visible_after_sequence`.
pub fn parse_ack(reply: &Reply) -> Ack {
    let value: Value = reply.json();
    let object = value.as_object().expect("a JSON object");
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        ["chunk_id", "created", "digest", "visible_after_sequence"],
        "registration response fields: {}",
        reply.text()
    );
    let digest = object["digest"].as_str().expect("digest is a string");
    assert_eq!(digest.len(), 64);
    assert_eq!(digest, digest.to_lowercase(), "lowercase hex");
    Ack {
        chunk_id: object["chunk_id"].as_u64().expect("chunk_id"),
        digest: hex::decode(digest)
            .expect("hex digest")
            .try_into()
            .expect("32 bytes"),
        created: object["created"].as_bool().expect("created"),
        visible_after_sequence: object["visible_after_sequence"]
            .as_u64()
            .expect("visible_after_sequence"),
    }
}

/// Asserts an error answer of spec 10.5: `status`, the `code`, a message
/// containing `message`, and `Cache-Control: no-store` (spec 10.1).
pub fn assert_refusal(reply: &Reply, status: StatusCode, code: &str, message: &str) {
    assert_eq!(
        reply.status,
        status,
        "expected {status} {code} ({message}), got {} {}",
        reply.status,
        reply.text()
    );
    let body: Value = reply.json();
    assert_eq!(body["code"], code, "{}", reply.text());
    let text = body["message"].as_str().expect("a message");
    assert!(
        text.contains(message),
        "message {text:?} does not name {message:?}"
    );
    assert_eq!(reply.header("cache-control"), Some("no-store"));
}

/// The registry write API (spec 9.3) at one origin, as one client.
#[derive(Clone, Debug)]
pub struct WriteApi {
    http: reqwest::Client,
    /// `http://<registry listener>` or the edge's `https://` origin.
    origin: String,
    /// The `X-Forwarded-For` this client's requests carry; `None` through
    /// the edge, which sets it.
    forwarded_for: Option<String>,
}

impl WriteApi {
    /// Direct to the registry listener, as client `ip`: the test plays the
    /// reverse proxy and sets `X-Forwarded-For`.
    pub fn direct(listen: SocketAddr, ip: &str) -> Self {
        Self {
            http: client::plain(),
            origin: format!("http://{listen}"),
            forwarded_for: Some(ip.to_owned()),
        }
    }

    /// Through the TLS edge, which overwrites `X-Forwarded-For`.
    pub fn via_edge(edge: &TlsEdge) -> Self {
        Self {
            http: edge.client(),
            origin: edge.origin(),
            forwarded_for: None,
        }
    }

    /// The same origin as another client.
    pub fn as_client(&self, ip: &str) -> Self {
        Self {
            forwarded_for: Some(ip.to_owned()),
            ..self.clone()
        }
    }

    /// `<origin>/registry/v1/<route>` (the canonical mount, spec 10.1).
    pub fn url(&self, route: &str) -> String {
        format!("{}{}/{route}", self.origin, PRODUCT.mount())
    }

    fn request(&self, method: reqwest::Method, url: &str) -> reqwest::RequestBuilder {
        let request = self.http.request(method, url);
        match &self.forwarded_for {
            Some(ip) => request.header("x-forwarded-for", ip),
            None => request,
        }
    }

    async fn send(&self, url: &str, request: reqwest::RequestBuilder) -> Reply {
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

    /// `GET challenge`, whatever the status.
    pub async fn challenge_reply(&self) -> Reply {
        let url = self.url("challenge");
        self.send(&url, self.request(reqwest::Method::GET, &url))
            .await
    }

    /// `GET challenge`, its format checked against spec 9.3: exactly
    /// `challenge` (64 lowercase hex), `difficulty_bits` and
    /// `expires_unix`; the challenge's first 8 bytes are `expires_unix`
    /// little-endian; `expires_unix` is the issue time plus 600 s;
    /// `no-store`.
    pub async fn challenge(&self) -> Issued {
        let before = now_unix();
        let reply = self.challenge_reply().await;
        let after = now_unix();
        reply.assert_status(StatusCode::OK, &self.url("challenge"));
        assert_eq!(reply.header("cache-control"), Some("no-store"));
        let value: Value = reply.json();
        let object = value.as_object().expect("a JSON object");
        let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["challenge", "difficulty_bits", "expires_unix"]);
        let text = object["challenge"].as_str().expect("challenge hex");
        assert_eq!(text.len(), 64);
        assert_eq!(text, text.to_lowercase());
        let challenge: [u8; 32] = hex::decode(text).expect("hex").try_into().expect("32");
        let expires_unix = object["expires_unix"].as_u64().expect("expires_unix");
        assert_eq!(
            u64::from_le_bytes(challenge[..8].try_into().unwrap()),
            expires_unix,
            "the challenge begins with expires_unix (u64 LE)"
        );
        assert!(
            (before + REGISTRY_CHALLENGE_SECONDS..=after + REGISTRY_CHALLENGE_SECONDS)
                .contains(&expires_unix),
            "expires_unix {expires_unix} is not issue time + 600 ({before}..={after})"
        );
        Issued {
            challenge,
            difficulty_bits: u32::try_from(object["difficulty_bits"].as_u64().expect("bits"))
                .expect("u32"),
            expires_unix,
        }
    }

    /// `POST register` with `body`, whatever the status.
    pub async fn post(&self, body: impl Into<reqwest::Body>) -> Reply {
        let url = self.url("register");
        let request = self
            .request(reqwest::Method::POST, &url)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(body);
        self.send(&url, request).await
    }

    /// Registers `address` under `issued` with a solved proof of work,
    /// whatever the status.
    pub async fn register_reply(&self, issued: &Issued, address: &OracleAddress) -> Reply {
        let nonce = solve(&issued.challenge, address, issued.difficulty_bits);
        self.post(register_body(&issued.challenge, &nonce, address))
            .await
    }

    /// Registers `address`, asserting `200`, `no-store` and the response
    /// shape.
    pub async fn register(&self, issued: &Issued, address: &OracleAddress) -> Ack {
        let reply = self.register_reply(issued, address).await;
        reply.assert_status(StatusCode::OK, &self.url("register"));
        assert_eq!(reply.header("cache-control"), Some("no-store"));
        parse_ack(&reply)
    }

    /// Registers every address, `concurrency` at a time; acknowledgements in
    /// input order.
    pub async fn register_all(
        &self,
        issued: &Issued,
        addresses: &[OracleAddress],
        concurrency: usize,
    ) -> Vec<Ack> {
        stream::iter(addresses.iter())
            .map(|address| self.register(issued, address))
            .buffered(concurrency)
            .collect()
            .await
    }
}

/// A registration upload held open on a raw HTTP/1.1 connection to the
/// registry listener, for the concurrency and upload-deadline controls
/// (spec 9.3). The request carries `Expect: 100-continue`, so the server's
/// `100 Continue` shows that the handler has taken the client's slot, was
/// admitted by the rate windows, and is reading the body.
pub struct HeldUpload {
    stream: tokio::net::TcpStream,
    body: Vec<u8>,
}

impl HeldUpload {
    /// Sends the head of `POST /registry/v1/register` as client `ip` and
    /// waits (up to `timeout`) for `100 Continue`. No body byte is sent.
    pub async fn start(listen: SocketAddr, ip: &str, body: Vec<u8>, timeout: Duration) -> Self {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let mut stream = tokio::net::TcpStream::connect(listen)
            .await
            .expect("connect to the registry listener");
        let head = format!(
            "POST {}/register HTTP/1.1\r\nHost: {listen}\r\nX-Forwarded-For: {ip}\r\n\
             Content-Type: application/octet-stream\r\nContent-Length: {}\r\n\
             Expect: 100-continue\r\nConnection: close\r\n\r\n",
            PRODUCT.mount(),
            body.len()
        );
        stream
            .write_all(head.as_bytes())
            .await
            .expect("send the head");
        let mut seen = Vec::new();
        tokio::time::timeout(timeout, async {
            while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                let mut buf = [0u8; 256];
                let n = stream.read(&mut buf).await.expect("read 100 Continue");
                assert!(
                    n > 0,
                    "connection closed before 100 Continue: {:?}",
                    String::from_utf8_lossy(&seen)
                );
                seen.extend_from_slice(&buf[..n]);
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no 100 Continue within {timeout:?}"));
        let text = String::from_utf8_lossy(&seen);
        assert!(
            text.starts_with("HTTP/1.1 100"),
            "expected 100 Continue, got {text:?}"
        );
        Self { stream, body }
    }

    /// Sends the body and returns the response's status and body.
    pub async fn finish(mut self) -> (u16, String) {
        use tokio::io::AsyncWriteExt as _;
        let body = std::mem::take(&mut self.body);
        self.stream.write_all(&body).await.expect("send the body");
        self.response().await
    }

    /// Sends nothing more and returns the response the server gives on its
    /// own (the upload deadline).
    pub async fn response(mut self) -> (u16, String) {
        use tokio::io::AsyncReadExt as _;
        let mut raw = Vec::new();
        self.stream
            .read_to_end(&mut raw)
            .await
            .expect("read the response");
        let text = String::from_utf8_lossy(&raw).into_owned();
        let status = text
            .get(9..12)
            .and_then(|code| code.parse().ok())
            .unwrap_or_else(|| panic!("not an HTTP response: {text:?}"));
        let body = text
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_owned())
            .unwrap_or_default();
        (status, body)
    }
}

// ---------------------------------------------------------------------------
// The publisher.

/// The publisher binary with only the registry enabled.
pub struct RegistryPublisher {
    process: Process,
    management: SocketAddr,
    listen: SocketAddr,
    state_dir: PathBuf,
    settings: RegistrySettings,
}

impl RegistryPublisher {
    /// Configures (spec 11.7) and starts `binary` with the registry enabled
    /// and `settings`, reading its identity from `rpc_url`, and waits up to
    /// `timeout` for `/ready`.
    pub async fn start(
        binary: PathBuf,
        rpc_url: &str,
        settings: RegistrySettings,
        timeout: Duration,
    ) -> Self {
        let mut process = Process::new("publisher", binary);
        let ports = free_addrs(2);
        let (management, listen) = (ports[0], ports[1]);
        let state_dir = process.state_dir(SECTION);
        let config = json!({
            "management_listen": management.to_string(),
            // The registry reads only the identity from the chain section
            // (spec 11.7); `epoch_length` is required by the section's schema.
            "chain": {"rpc_url": rpc_url, "no_auth": true, "poll_ms": 1000, "epoch_length": 16},
            SECTION: {"enabled": true, "settings": settings.json(listen, &state_dir)},
        });
        process.config("--config", "publisher.json", &config);
        process.start();
        let mut host = Self {
            process,
            management,
            listen,
            state_dir,
            settings,
        };
        host.wait_ready(timeout).await;
        host
    }

    pub fn process(&mut self) -> &mut Process {
        &mut self.process
    }

    pub fn management(&self) -> SocketAddr {
        self.management
    }

    /// The registry listener: distribution, write API, `/metrics`.
    pub fn listen(&self) -> SocketAddr {
        self.listen
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn settings(&self) -> RegistrySettings {
        self.settings
    }

    /// The write API on the registry listener, as client `ip`.
    pub fn write(&self, ip: &str) -> WriteApi {
        WriteApi::direct(self.listen, ip)
    }

    pub async fn wait_ready(&mut self, timeout: Duration) {
        self.process
            .wait_ready(&self.management.to_string(), timeout)
            .await;
    }

    /// Crash (`SIGKILL` of the process group) and start again with the same
    /// configuration and state; waits for `/ready`.
    pub async fn crash_restart(&mut self, timeout: Duration) {
        self.process.restart();
        self.wait_ready(timeout).await;
    }

    fn url(&self, route: &str) -> String {
        format!(
            "http://{}{}{route}",
            self.listen,
            PRODUCT.distribution_prefix()
        )
    }

    /// `GET /provider/registry/v1/current`.
    pub async fn current(&self) -> Current {
        client::get_json(&client::plain(), &self.url("current"), StatusCode::OK).await
    }

    /// `GET /provider/registry/v1/objects/<sha256>`.
    pub async fn object(&self, sha256: &str) -> bytes::Bytes {
        let url = self.url(&format!("objects/{sha256}"));
        client::get_bytes(&client::plain(), &url, StatusCode::OK).await
    }

    /// The current publication; `None` while nothing is published.
    pub async fn inventory(&self) -> Option<Publication> {
        let current = self.current().await;
        let reference = current.publication?;
        let bytes = self.object(&reference.inventory.sha256).await;
        let inventory: Inventory = serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("the inventory object does not parse: {error}"));
        assert_eq!(inventory.descriptor(), reference.descriptor);
        Some(Publication {
            eligible: current.eligible,
            inventory_object: reference.inventory,
            inventory,
        })
    }

    /// The map head and whole listing of `publication`, each sealed map
    /// chunk fetched and checked against the head with the protocol crate's
    /// client checks (spec 10.3).
    pub async fn listing(
        &self,
        publication: &Publication,
    ) -> (RegistryMapHead, Vec<RegistryScope>) {
        let bytes = self.object(&publication.inventory.map_head.sha256).await;
        let head = RegistryMapHead::parse(&bytes).expect("the published registry head parses");
        head.validate()
            .expect("the published registry head is valid");
        assert_eq!(
            head.to_bytes().expect("encodes"),
            bytes.to_vec(),
            "the published head is canonically encoded"
        );
        let mut chunks: Vec<RegistryMapChunk> = Vec::new();
        for (position, reference) in head.sealed_chunks.iter().enumerate() {
            let bytes = self.object(&hex::encode(reference.sha256.0)).await;
            chunks.push(
                head.parse_chunk(position, &bytes)
                    .unwrap_or_else(|error| panic!("map chunk {position}: {error}")),
            );
        }
        head.check_listing(&chunks)
            .expect("the published listing is consistent");
        let mut scopes: Vec<RegistryScope> =
            chunks.into_iter().flat_map(|chunk| chunk.scopes).collect();
        scopes.extend(head.open_scopes.iter().cloned());
        (head, scopes)
    }

    /// Waits until the current publication satisfies `accept`.
    pub async fn wait_publication(
        &self,
        what: &str,
        timeout: Duration,
        accept: impl Fn(&Publication) -> bool,
    ) -> Publication {
        wait_for(what, timeout, || async {
            match self.inventory().await {
                Some(publication) if accept(&publication) => Ok(publication),
                Some(publication) => Err(format!(
                    "generation {} recovery epoch {} subject {:?}",
                    publication.inventory.generation,
                    publication.inventory.recovery_epoch,
                    publication.inventory.subject
                )),
                None => Err("nothing published".into()),
            }
        })
        .await
    }

    /// Waits for a publication whose subject is `log_sequence`.
    pub async fn wait_log_sequence(&self, log_sequence: u64, timeout: Duration) -> Publication {
        self.wait_publication(
            &format!("a registry publication at log_sequence {log_sequence}"),
            timeout,
            |p| subject_sequence(p) == log_sequence,
        )
        .await
    }

    /// `GET /metrics` on the registry listener.
    pub async fn metrics(&self) -> String {
        let url = format!("http://{}/metrics", self.listen);
        client::get(&client::plain(), &url)
            .await
            .assert_status(StatusCode::OK, &url)
            .text()
    }

    /// One series of `/metrics` (exact name with labels).
    pub async fn metric(&self, series: &str) -> f64 {
        let text = self.metrics().await;
        metric_value(&text, series).unwrap_or_else(|| panic!("no series {series} in\n{text}"))
    }
}

/// The `log_sequence` a registry publication's authority subject names.
pub fn subject_sequence(publication: &Publication) -> u64 {
    match publication.inventory.subject {
        Subject::Registry { log_sequence } => log_sequence,
        ref other => panic!("a registry publication has a registry subject: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The query host.

/// The query-server binary serving the registry (spec 11.7: no
/// `activation_height` or `epoch_length`).
pub struct RegistryQueryHost {
    process: Process,
    management: SocketAddr,
    public: SocketAddr,
    state_dir: PathBuf,
}

impl RegistryQueryHost {
    /// Writes the configuration for following `publisher` (an `https://`
    /// origin) trusting `publisher_ca`; `extra` is merged into the settings.
    /// Nothing runs until [`RegistryQueryHost::start`].
    pub fn configure(
        binary: PathBuf,
        publisher: &str,
        publisher_ca: &Path,
        identity: &Identity,
        extra: &Map<String, Value>,
    ) -> Self {
        let mut process = Process::new("query", binary);
        let ports = free_addrs(2);
        let (management, public) = (ports[0], ports[1]);
        let state_dir = process.state_dir(SECTION);
        let mut settings = json!({
            "publisher": publisher,
            "state_dir": state_dir,
            "publisher_ca": publisher_ca,
            "role": "combined",
            "publisher_network": identity.network,
            "publisher_genesis": identity.genesis_display(),
        });
        let object = settings.as_object_mut().expect("settings object");
        for (key, value) in extra {
            object.insert(key.clone(), value.clone());
        }
        let config = json!({
            "management_listen": management.to_string(),
            "public_listen": public.to_string(),
            SECTION: {"enabled": true, "settings": settings},
        });
        process.config("--config", "query.json", &config);
        Self {
            process,
            management,
            public,
            state_dir,
        }
    }

    pub fn process(&mut self) -> &mut Process {
        &mut self.process
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    /// `http://<public_listen>`.
    pub fn origin(&self) -> String {
        format!("http://{}", self.public)
    }

    pub fn start(&mut self) {
        self.process.start();
    }

    pub async fn wait_ready(&mut self, timeout: Duration) {
        self.process
            .wait_ready(&self.management.to_string(), timeout)
            .await;
    }

    pub async fn crash_restart(&mut self, timeout: Duration) {
        self.process.restart();
        self.wait_ready(timeout).await;
    }

    /// `GET <management>/<route>`, whatever the status.
    pub async fn management_get(&self, route: &str) -> Reply {
        client::get(
            &client::plain(),
            &format!("http://{}/{route}", self.management),
        )
        .await
    }

    /// `tachyon_registry_<series>` on the management `/metrics`.
    pub async fn metric(&self, series: &str) -> f64 {
        let reply = self.management_get("metrics").await;
        reply.assert_status(StatusCode::OK, "/metrics");
        let name = format!("tachyon_registry_{series}");
        metric_value(&reply.text(), &name)
            .unwrap_or_else(|| panic!("no series {name} in\n{}", reply.text()))
    }

    /// A probe client (the wallet) of the registry on its canonical mount.
    pub fn wallet(&self) -> Arc<Client> {
        Arc::new(Client::new(&self.origin(), PRODUCT, None).expect("a client"))
    }
}

/// The registry head of a fetched map.
pub fn registry_head(map: &MapView) -> &RegistryMapHead {
    match map.head() {
        MapHead::Registry(head) => head,
        MapHead::Chain(_) => panic!("a registry map has a registry head"),
    }
}

/// Fetches the map through `wallet` until `accept` holds, tolerating the
/// errors a host answers while it is not ready (`503`) or between maps.
pub async fn wait_map(
    wallet: &Client,
    what: &str,
    timeout: Duration,
    accept: impl Fn(&MapView) -> bool,
) -> Arc<MapView> {
    wait_for(what, timeout, || async {
        match wallet.fetch_map().await {
            Ok(map) if accept(&map) => Ok(map),
            Ok(map) => {
                let head = registry_head(&map);
                Err(format!(
                    "map generation {} recovery epoch {} log_sequence {}",
                    head.generation,
                    head.recovery_epoch.get(),
                    head.log_sequence
                ))
            }
            Err(error) => Err(describe(&error)),
        }
    })
    .await
}

/// Waits until the wallet's host serves a map at `log_sequence`.
pub async fn wait_sequence(wallet: &Client, log_sequence: u64, budget: &Budget) -> Arc<MapView> {
    wait_map(
        wallet,
        &format!("a served map at log_sequence {log_sequence}"),
        wait_limit(budget),
        |map| registry_head(map).log_sequence == log_sequence,
    )
    .await
}

// ---------------------------------------------------------------------------
// The whole topology.

/// What a registry test sets up.
#[derive(Clone, Debug)]
pub struct RegistryOptions {
    pub settings: RegistrySettings,
    /// Fields merged into the query host's `registry.settings`.
    pub query_settings: Map<String, Value>,
    /// Seeds the node double's genesis-only chain.
    pub chain_seed: u64,
}

impl RegistryOptions {
    /// Bulk write settings, fast query-host polling and admission wide
    /// enough for a test's parallel lookups.
    pub fn new(chain_seed: u64) -> Self {
        let mut query_settings = Map::new();
        query_settings.insert("poll_ms".into(), json!(100));
        query_settings.insert(
            "admission".into(),
            json!({"executing": 4, "waiting": 64, "wait_ms": 5000, "per_client": 16}),
        );
        Self {
            settings: RegistrySettings::bulk(),
            query_settings,
            chain_seed,
        }
    }

    pub fn query_setting(mut self, key: &str, value: Value) -> Self {
        self.query_settings.insert(key.into(), value);
        self
    }

    pub fn settings(mut self, settings: RegistrySettings) -> Self {
        self.settings = settings;
        self
    }
}

/// The topology of one registry test.
pub struct RegistryTopology {
    pub node: NodeDouble,
    pub identity: Identity,
    pub publisher: RegistryPublisher,
    pub edge: TlsEdge,
    /// Configured; started by [`RegistryTopology::start_query`].
    pub query: RegistryQueryHost,
}

impl RegistryTopology {
    /// Serves a genesis-only chain from an in-process node double, starts
    /// the publisher with the registry (waiting for `/ready`), puts the TLS
    /// edge in front of it (discovery to management; `/provider/registry/v1/`
    /// and the write API `/registry/v1/` to the registry listener) and
    /// configures the query host against the edge without starting it.
    pub async fn launch(
        options: RegistryOptions,
        publisher_binary: PathBuf,
        query_binary: PathBuf,
        budget: &Budget,
    ) -> Self {
        let chain = identity_chain(options.chain_seed);
        let identity = Identity::of(&chain);
        let node = start_node(chain).await;
        let publisher = RegistryPublisher::start(
            publisher_binary,
            &node.url(),
            options.settings,
            wait_limit(budget),
        )
        .await;
        let edge = TlsEdge::builder()
            .management(publisher.management().to_string())
            .product(PRODUCT.name(), publisher.listen().to_string())
            .prefix(
                format!("{}/", PRODUCT.mount()),
                publisher.listen().to_string(),
            )
            .start()
            .await
            .expect("start the TLS edge");
        let query = RegistryQueryHost::configure(
            query_binary,
            &edge.origin(),
            edge.ca_path(),
            &identity,
            &options.query_settings,
        );
        Self {
            node,
            identity,
            publisher,
            edge,
            query,
        }
    }

    /// [`RegistryTopology::launch`], then the query host up to `/ready`.
    pub async fn start(
        options: RegistryOptions,
        publisher_binary: PathBuf,
        query_binary: PathBuf,
        budget: &Budget,
    ) -> Self {
        let mut topology = Self::launch(options, publisher_binary, query_binary, budget).await;
        topology.start_query(budget).await;
        topology
    }

    pub async fn start_query(&mut self, budget: &Budget) {
        self.query.start();
        self.query.wait_ready(wait_limit(budget)).await;
    }

    /// The write API on the registry listener as client `ip`.
    pub fn write(&self, ip: &str) -> WriteApi {
        self.publisher.write(ip)
    }

    /// The write API through the TLS edge.
    pub fn write_via_edge(&self) -> WriteApi {
        WriteApi::via_edge(&self.edge)
    }

    /// `/provider/registry/v1/`, for edge paths.
    pub fn prefix(&self) -> &'static str {
        PRODUCT.distribution_prefix()
    }
}
