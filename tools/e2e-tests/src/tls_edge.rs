//! An in-process HTTPS reverse proxy in front of the publisher (spec sections 4.3
//! and 11.6).
//!
//! `pir-publisher` serves plain HTTP on private listeners, while
//! `pir_sync_manager::HttpsSource` accepts only `https://` origins, so the query
//! host reaches the publisher through this edge, as through a production proxy:
//!
//! - `/.well-known/pir-service-discovery` goes to the management upstream (the
//!   publisher serves discovery only there);
//! - `/provider/<product>/v1/...` goes to that product's upstream;
//! - extra prefixes (for example the registry write API, `/registry/v1/`) go to
//!   the upstream they were registered with;
//! - everything else is answered `404` by the edge itself.
//!
//! The edge listens on `127.0.0.1` with a freshly generated self-signed
//! certificate for `localhost` (and `127.0.0.1`). The certificate is its own trust
//! anchor; its PEM is written to [`TlsEdge::ca_path`] for the query host's
//! `publisher_ca`. Bodies are streamed in both directions; client connections
//! are HTTP/1.1 with keep-alive. The edge overwrites `X-Forwarded-For` with the
//! connecting peer's address, as spec section 9.3 requires of the proxy in front
//! of the registry write API.
//!
//! Faults are switched at run time with [`TlsEdge::inject`]; every request is
//! counted on arrival, faulted ones included.
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::header::{self, HeaderMap, HeaderName, HeaderValue};
use hyper::{Request, Response, StatusCode, Version};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, ready};
use std::time::Duration;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::TlsAcceptor;

/// Boxed error of the edge's bodies and services.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
type EdgeBody = BoxBody<Bytes, BoxError>;

/// The service discovery path the edge routes to the management upstream.
pub const DISCOVERY_PATH: &str = pir_service_discovery::PATH;

/// How long a client has to complete the TLS handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A self-signed certificate and its private key, both PEM.
#[derive(Clone, Debug)]
pub struct EdgeCertificate {
    pub cert_pem: String,
    pub key_pem: String,
}

impl EdgeCertificate {
    /// A fresh ECDSA P-256 certificate for `localhost` and `127.0.0.1`, with
    /// `basicConstraints` `CA:FALSE` like the openssl-made certificate in
    /// `wallet-pir`'s tests. The leaf is its own trust anchor.
    pub fn generate() -> Self {
        let mut params =
            rcgen::CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
                .expect("valid subject alternative names");
        let mut name = rcgen::DistinguishedName::new();
        name.push(rcgen::DnType::CommonName, "localhost");
        params.distinguished_name = name;
        params.is_ca = rcgen::IsCa::ExplicitNoCa;
        let key = rcgen::KeyPair::generate().expect("generate a P-256 key");
        let cert = params.self_signed(&key).expect("self-sign the certificate");
        Self {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
        }
    }

    /// A rustls server configuration serving this certificate over HTTP/1.1,
    /// with the ring provider.
    fn server_config(&self) -> io::Result<rustls::ServerConfig> {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let cert = CertificateDer::from_pem_slice(self.cert_pem.as_bytes()).map_err(invalid)?;
        let key = PrivateKeyDer::from_pem_slice(self.key_pem.as_bytes()).map_err(invalid)?;
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(invalid)?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(invalid)?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(config)
    }
}

fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

/// Which requests a fault applies to. Paths are matched without the query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// Every request.
    Any,
    /// `/.well-known/pir-service-discovery`.
    Discovery,
    /// Object bodies of any product: `/provider/<product>/v1/objects/...`.
    Objects,
    /// Everything under `/provider/<product>/v1/`.
    Product(String),
    /// Object bodies of one product.
    ProductObjects(String),
    /// Exactly this path.
    Path(String),
    /// Paths ending with this suffix (for example `/current` or
    /// `/authority/check`).
    Suffix(String),
}

impl Target {
    /// Whether a request for `path` is targeted.
    pub fn matches(&self, path: &str) -> bool {
        match self {
            Target::Any => true,
            Target::Discovery => path == DISCOVERY_PATH,
            Target::Objects => object_product(path).is_some(),
            Target::Product(product) => provider_product(path) == Some(product.as_str()),
            Target::ProductObjects(product) => object_product(path) == Some(product.as_str()),
            Target::Path(exact) => path == exact,
            Target::Suffix(suffix) => path.ends_with(suffix.as_str()),
        }
    }
}

/// The product of a distribution path `/provider/<product>/v1/...`.
pub fn provider_product(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/provider/")?;
    let (product, rest) = rest.split_once('/')?;
    (!product.is_empty() && rest.starts_with("v1/")).then_some(product)
}

/// The product of an object path `/provider/<product>/v1/objects/...`.
pub fn object_product(path: &str) -> Option<&str> {
    let product = provider_product(path)?;
    let rest = &path["/provider/".len() + product.len() + "/v1/".len()..];
    rest.starts_with("objects/").then_some(product)
}

/// What the edge does to a targeted request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Answer this status (with a short text body) without contacting the
    /// upstream: `404` to make an object missing, `502` or `503` to refuse.
    Status(u16),
    /// Abort the TCP connection with a reset before any response is sent.
    Reset,
    /// Wait this long, then continue (later faults still apply).
    Delay(Duration),
    /// Wait until this fault is removed ([`TlsEdge::remove`] or
    /// [`TlsEdge::clear_faults`]), then continue. [`TlsEdge::held`] counts the
    /// requests waiting.
    Hold,
    /// Forward, flipping the low bit of the first byte of the response body.
    CorruptBody,
    /// Forward, sending only the first half of the response body (none of a
    /// one-byte body) and then aborting the connection.
    TruncateBody,
    /// Answer `200` with a discovery document of the next major version.
    DiscoveryWrongMajor,
    /// Answer `200` with a body that is not a discovery document.
    DiscoveryMalformed,
}

impl Fault {
    /// `404` without contacting the upstream.
    pub fn not_found() -> Self {
        Fault::Status(404)
    }

    /// `502` without contacting the upstream.
    pub fn bad_gateway() -> Self {
        Fault::Status(502)
    }

    /// Whether the fault lets later faults of the same request apply.
    fn passes_through(&self) -> bool {
        matches!(self, Fault::Delay(_) | Fault::Hold)
    }
}

/// The discovery document a publisher of the next major contract version serves.
pub fn wrong_major_document() -> Vec<u8> {
    let next = pir_service_discovery::version().major + 1;
    pir_service_discovery::Document {
        service: pir_service_discovery::PUBLISHER_SERVICE.into(),
        version: format!("{next}.0.0"),
    }
    .to_json()
}

/// The body [`Fault::DiscoveryMalformed`] answers.
pub const MALFORMED_DISCOVERY: &[u8] = b"{\"service\": \"pir-publisher\", \"version\"";

/// Identifies an injected fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FaultId(u64);

struct Rule {
    id: FaultId,
    target: Target,
    fault: Fault,
    /// Matching requests left; `None` is unlimited. An exhausted rule stays
    /// listed (so a [`Fault::Hold`] keeps holding) but matches nothing.
    remaining: Option<usize>,
}

/// One request as it arrived at the edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
}

struct Shared {
    management: Option<String>,
    products: BTreeMap<String, String>,
    /// Longest prefix first.
    prefixes: Vec<(String, String)>,
    client: Client<HttpConnector, Incoming>,
    rules: Mutex<Vec<Rule>>,
    next_rule: AtomicU64,
    changed: watch::Sender<()>,
    held: AtomicUsize,
    requests: Mutex<Vec<RecordedRequest>>,
    connections: AtomicUsize,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Shared {
    fn upstream(&self, path: &str) -> Option<&str> {
        if path == DISCOVERY_PATH {
            return self.management.as_deref();
        }
        if let Some(product) = provider_product(path) {
            return self.products.get(product).map(String::as_str);
        }
        self.prefixes
            .iter()
            .find(|(prefix, _)| path.starts_with(prefix.as_str()))
            .map(|(_, upstream)| upstream.as_str())
    }

    /// The faults that apply to a request for `path`, in injection order, up to
    /// and including the first one that decides the response; each is consumed.
    fn take_faults(&self, path: &str) -> Vec<(FaultId, Fault)> {
        let mut rules = lock(&self.rules);
        let mut taken = Vec::new();
        for rule in rules.iter_mut() {
            if rule.remaining == Some(0) || !rule.target.matches(path) {
                continue;
            }
            if let Some(remaining) = &mut rule.remaining {
                *remaining -= 1;
            }
            taken.push((rule.id, rule.fault.clone()));
            if !rule.fault.passes_through() {
                break;
            }
        }
        taken
    }

    fn present(&self, id: FaultId) -> bool {
        lock(&self.rules).iter().any(|rule| rule.id == id)
    }

    async fn hold(&self, id: FaultId) {
        struct Held<'a>(&'a AtomicUsize);
        impl Drop for Held<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        self.held.fetch_add(1, Ordering::SeqCst);
        let _held = Held(&self.held);
        let mut changed = self.changed.subscribe();
        while self.present(id) {
            if changed.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Builds a [`TlsEdge`].
#[derive(Debug, Default)]
pub struct EdgeBuilder {
    management: Option<String>,
    products: BTreeMap<String, String>,
    prefixes: Vec<(String, String)>,
    listen: Option<SocketAddr>,
    cert_path: Option<PathBuf>,
    certificate: Option<EdgeCertificate>,
}

impl EdgeBuilder {
    /// The publisher's management listener (`host:port`), which serves discovery.
    pub fn management(mut self, upstream: impl Into<String>) -> Self {
        self.management = Some(upstream.into());
        self
    }

    /// The listener (`host:port`) serving `/provider/<product>/v1/`. Several
    /// products may share one listener (memo and first-contact do).
    pub fn product(mut self, product: impl Into<String>, upstream: impl Into<String>) -> Self {
        self.products.insert(product.into(), upstream.into());
        self
    }

    /// Routes every path starting with `prefix` (that no rule above claims) to
    /// `upstream`.
    pub fn prefix(mut self, prefix: impl Into<String>, upstream: impl Into<String>) -> Self {
        self.prefixes.push((prefix.into(), upstream.into()));
        self
    }

    /// Listen on this address instead of `127.0.0.1:0`.
    pub fn listen(mut self, addr: SocketAddr) -> Self {
        self.listen = Some(addr);
        self
    }

    /// Write the certificate PEM here instead of into a temporary directory the
    /// edge owns.
    pub fn cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.cert_path = Some(path.into());
        self
    }

    /// Serve this certificate instead of a fresh one (for a replacement edge
    /// that clients already trust).
    pub fn certificate(mut self, certificate: EdgeCertificate) -> Self {
        self.certificate = Some(certificate);
        self
    }

    /// Writes the certificate and starts listening.
    pub async fn start(self) -> io::Result<TlsEdge> {
        let certificate = self.certificate.unwrap_or_else(EdgeCertificate::generate);
        let acceptor = TlsAcceptor::from(Arc::new(certificate.server_config()?));
        let (dir, ca_path) = match self.cert_path {
            Some(path) => (None, path),
            None => {
                let dir = tempfile::Builder::new().prefix("tls-edge-").tempdir()?;
                let path = dir.path().join("edge-ca.pem");
                (Some(dir), path)
            }
        };
        std::fs::write(&ca_path, &certificate.cert_pem)?;
        let mut prefixes = self.prefixes;
        prefixes.sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(Some(Duration::from_secs(5)));
        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(5))
            .build(connector);
        let shared = Arc::new(Shared {
            management: self.management,
            products: self.products,
            prefixes,
            client,
            rules: Mutex::new(Vec::new()),
            next_rule: AtomicU64::new(0),
            changed: watch::Sender::new(()),
            held: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            connections: AtomicUsize::new(0),
        });
        let listener = bind(
            self.listen
                .unwrap_or(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
        )?;
        let addr = listener.local_addr()?;
        let edge = TlsEdge {
            shared,
            acceptor,
            addr,
            certificate,
            ca_path,
            _dir: dir,
            server: Mutex::new(None),
        };
        edge.launch(listener);
        Ok(edge)
    }
}

/// The accept loop of a listening edge.
struct Running {
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

/// A running TLS edge. Dropping it stops the listener and every connection.
pub struct TlsEdge {
    shared: Arc<Shared>,
    acceptor: TlsAcceptor,
    addr: SocketAddr,
    certificate: EdgeCertificate,
    ca_path: PathBuf,
    _dir: Option<tempfile::TempDir>,
    server: Mutex<Option<Running>>,
}

impl TlsEdge {
    pub fn builder() -> EdgeBuilder {
        EdgeBuilder::default()
    }

    /// The bound address; it stays the same across an outage.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// The origin clients use, `https://localhost:<port>` (the query host's
    /// `publisher` setting).
    pub fn origin(&self) -> String {
        format!("https://localhost:{}", self.addr.port())
    }

    /// `origin() + path`; `path` starts with `/`.
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.origin())
    }

    /// The certificate PEM file (the query host's `publisher_ca`).
    pub fn ca_path(&self) -> &Path {
        &self.ca_path
    }

    pub fn certificate(&self) -> &EdgeCertificate {
        &self.certificate
    }

    /// The certificate as a `reqwest` trust root.
    pub fn reqwest_certificate(&self) -> reqwest::Certificate {
        reqwest::Certificate::from_pem(self.certificate.cert_pem.as_bytes())
            .expect("the edge certificate parses")
    }

    /// A client that trusts this edge's certificate and nothing else.
    pub fn client(&self) -> reqwest::Client {
        crate::client::trusting(self.certificate.cert_pem.as_bytes())
    }

    /// Applies `fault` to every request `target` matches, until removed.
    pub fn inject(&self, target: Target, fault: Fault) -> FaultId {
        self.add_rule(target, fault, None)
    }

    /// Applies `fault` to the next `times` requests `target` matches.
    pub fn inject_times(&self, target: Target, fault: Fault, times: usize) -> FaultId {
        self.add_rule(target, fault, Some(times))
    }

    fn add_rule(&self, target: Target, fault: Fault, remaining: Option<usize>) -> FaultId {
        let id = FaultId(self.shared.next_rule.fetch_add(1, Ordering::SeqCst));
        lock(&self.shared.rules).push(Rule {
            id,
            target,
            fault,
            remaining,
        });
        self.shared.changed.send_replace(());
        id
    }

    /// Removes one fault, releasing the requests it holds.
    pub fn remove(&self, id: FaultId) {
        lock(&self.shared.rules).retain(|rule| rule.id != id);
        self.shared.changed.send_replace(());
    }

    /// Removes every fault (an outage is ended with [`TlsEdge::resume`]).
    pub fn clear_faults(&self) {
        lock(&self.shared.rules).clear();
        self.shared.changed.send_replace(());
    }

    /// Requests currently waiting on a [`Fault::Hold`].
    pub fn held(&self) -> usize {
        self.shared.held.load(Ordering::SeqCst)
    }

    /// Requests that arrived for exactly `path`.
    pub fn count(&self, path: &str) -> usize {
        lock(&self.shared.requests)
            .iter()
            .filter(|request| request.path == path)
            .count()
    }

    /// Requests that arrived for a path `target` matches.
    pub fn count_matching(&self, target: &Target) -> usize {
        lock(&self.shared.requests)
            .iter()
            .filter(|request| target.matches(&request.path))
            .count()
    }

    /// Request count per path.
    pub fn counts(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for request in lock(&self.shared.requests).iter() {
            *counts.entry(request.path.clone()).or_default() += 1;
        }
        counts
    }

    /// Every request, in arrival order.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        lock(&self.shared.requests).clone()
    }

    /// Forgets the recorded requests.
    pub fn reset_counts(&self) {
        lock(&self.shared.requests).clear();
    }

    /// TLS connections accepted so far (keep-alive reuses one).
    pub fn connections(&self) -> usize {
        self.shared.connections.load(Ordering::SeqCst)
    }

    /// Stops accepting and drops every open connection (held and in-flight
    /// requests included). Connecting then fails until [`TlsEdge::resume`].
    ///
    /// It returns once the listener is closed and every connection task has
    /// ended, so held requests are no longer counted by [`TlsEdge::held`].
    pub async fn outage(&self) {
        let running = lock(&self.server).take();
        if let Some(Running { stop, task }) = running {
            let _ = stop.send(());
            let _ = task.await;
        }
    }

    /// Listens again on the same address after an outage; a no-op when up.
    pub async fn resume(&self) -> io::Result<()> {
        if self.is_up() {
            return Ok(());
        }
        let listener = bind(self.addr)?;
        self.launch(listener);
        Ok(())
    }

    pub fn is_up(&self) -> bool {
        lock(&self.server).is_some()
    }

    fn launch(&self, listener: TcpListener) {
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(accept_loop(
            listener,
            self.acceptor.clone(),
            self.shared.clone(),
            stopped,
        ));
        *lock(&self.server) = Some(Running { stop, task });
    }
}

impl Drop for TlsEdge {
    fn drop(&mut self) {
        if let Some(running) = lock(&self.server).take() {
            running.task.abort();
        }
    }
}

fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(1024)
}

/// Accepts connections until `stop` fires (or its sender is dropped), then
/// closes the listener and aborts every connection, waiting for each to end.
/// Aborting this task instead (on drop) drops them without waiting.
async fn accept_loop(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    shared: Arc<Shared>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = &mut stop => {
                drop(listener);
                connections.shutdown().await;
                return;
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    connections.spawn(serve(stream, peer, acceptor.clone(), shared.clone()));
                }
                // Out of descriptors or a connection aborted before accept.
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

async fn serve(stream: TcpStream, peer: SocketAddr, acceptor: TlsAcceptor, shared: Arc<Shared>) {
    let _ = stream.set_nodelay(true);
    let fd = stream.as_raw_fd();
    let Ok(Ok(tls)) = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await else {
        return;
    };
    shared.connections.fetch_add(1, Ordering::SeqCst);
    let service = hyper::service::service_fn(move |request| {
        let shared = shared.clone();
        async move { handle(shared, peer, fd, request).await }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .keep_alive(true)
        .serve_connection(TokioIo::new(tls), service)
        .await;
}

async fn handle(
    shared: Arc<Shared>,
    peer: SocketAddr,
    fd: RawFd,
    request: Request<Incoming>,
) -> Result<Response<EdgeBody>, EdgeError> {
    let path = request.uri().path().to_owned();
    lock(&shared.requests).push(RecordedRequest {
        method: request.method().as_str().to_owned(),
        path: path.clone(),
    });
    let mut body_fault = BodyFault::None;
    for (id, fault) in shared.take_faults(&path) {
        match fault {
            Fault::Delay(delay) => tokio::time::sleep(delay).await,
            Fault::Hold => shared.hold(id).await,
            Fault::Status(code) => {
                let status = StatusCode::from_u16(code).map_err(EdgeError::from_display)?;
                return Ok(text(status, "fault injected by the TLS edge"));
            }
            Fault::Reset => {
                reset(fd);
                return Err(EdgeError("connection reset by the TLS edge".into()));
            }
            Fault::CorruptBody => body_fault = BodyFault::Corrupt,
            Fault::TruncateBody => body_fault = BodyFault::Truncate,
            Fault::DiscoveryWrongMajor => return Ok(json(wrong_major_document())),
            Fault::DiscoveryMalformed => return Ok(json(MALFORMED_DISCOVERY.to_vec())),
        }
    }
    let Some(upstream) = shared.upstream(&path).map(str::to_owned) else {
        return Ok(text(StatusCode::NOT_FOUND, "no route at the TLS edge"));
    };
    let (mut parts, body) = request.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", |value| value.as_str());
    parts.uri = format!("http://{upstream}{path_and_query}")
        .parse()
        .map_err(EdgeError::from_display)?;
    parts.version = Version::HTTP_11;
    strip_hop_by_hop(&mut parts.headers);
    parts.headers.insert(
        header::HOST,
        upstream.parse().map_err(EdgeError::from_display)?,
    );
    parts.headers.insert(
        HeaderName::from_static("x-forwarded-for"),
        peer.ip()
            .to_string()
            .parse()
            .map_err(EdgeError::from_display)?,
    );
    parts.headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static("https"),
    );
    let forwarded = Request::from_parts(parts, body);
    let response = match shared.client.request(forwarded).await {
        Ok(response) => response,
        Err(error) => {
            return Ok(text(
                StatusCode::BAD_GATEWAY,
                &format!("upstream {upstream} unavailable: {error}"),
            ));
        }
    };
    let (mut parts, body) = response.into_parts();
    strip_hop_by_hop(&mut parts.headers);
    let length = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok());
    let body = FaultBody {
        inner: body,
        fault: body_fault,
        limit: length.map(|length| length / 2),
        sent: 0,
        done: false,
        abort: None,
    };
    Ok(Response::from_parts(parts, body.boxed()))
}

/// Why the edge dropped a connection without answering.
#[derive(Debug)]
pub struct EdgeError(String);

impl EdgeError {
    fn from_display(error: impl std::fmt::Display) -> Self {
        Self(error.to_string())
    }
}

impl std::fmt::Display for EdgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EdgeError {}

/// Sets `SO_LINGER` to zero so closing the socket sends a TCP reset.
fn reset(fd: RawFd) {
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    // SAFETY: `fd` is the socket of the connection this request arrived on,
    // which hyper keeps open while the request's service future runs; the
    // option value is a properly sized `linger`.
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            (&linger as *const libc::linger).cast(),
            std::mem::size_of::<libc::linger>() as libc::socklen_t,
        );
    }
}

const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
];

fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let named: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .collect();
    for name in HOP_BY_HOP.iter().copied().chain(["upgrade"]) {
        headers.remove(name);
    }
    for name in named {
        headers.remove(name.as_str());
    }
}

fn full(body: impl Into<Bytes>) -> EdgeBody {
    Full::new(body.into())
        .map_err(|never| match never {})
        .boxed()
}

fn text(status: StatusCode, message: &str) -> Response<EdgeBody> {
    let mut response = Response::new(full(format!("{message}\n")));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

fn json(body: Vec<u8>) -> Response<EdgeBody> {
    let mut response = Response::new(full(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BodyFault {
    None,
    Corrupt,
    Truncate,
}

/// The upstream body, streamed frame by frame, with a body fault applied.
struct FaultBody {
    inner: Incoming,
    fault: BodyFault,
    /// Bytes a truncated body sends: half the declared length, or half the
    /// first frame when no length is declared.
    limit: Option<u64>,
    sent: u64,
    done: bool,
    /// Started once a truncated body has sent its bytes: the abort waits for
    /// it so hyper flushes the head and the partial body first.
    abort: Option<Pin<Box<tokio::time::Sleep>>>,
}

/// How long a truncated body lingers before the connection is aborted.
const TRUNCATE_FLUSH: Duration = Duration::from_millis(100);

impl FaultBody {
    /// Aborts a truncated body once the flush grace has passed.
    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let abort = self
            .abort
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(TRUNCATE_FLUSH)));
        ready!(abort.as_mut().poll(cx));
        self.done = true;
        Poll::Ready(Some(Err("body truncated by the TLS edge".into())))
    }
}

impl Body for FaultBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = &mut *self;
        if this.done {
            return Poll::Ready(None);
        }
        if this.fault == BodyFault::Truncate
            && (this.abort.is_some() || this.limit.is_some_and(|limit| this.sent >= limit))
        {
            return this.poll_abort(cx);
        }
        let frame = match ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
            None if this.fault == BodyFault::Truncate => return this.poll_abort(cx),
            None => {
                this.done = true;
                return Poll::Ready(None);
            }
            Some(Err(error)) => {
                this.done = true;
                return Poll::Ready(Some(Err(error.into())));
            }
            Some(Ok(frame)) => frame,
        };
        let mut data = match frame.into_data() {
            Ok(data) => data,
            Err(frame) => return Poll::Ready(Some(Ok(frame))),
        };
        match this.fault {
            BodyFault::None => {}
            BodyFault::Corrupt => {
                if this.sent == 0 && !data.is_empty() {
                    let mut bytes = data.to_vec();
                    bytes[0] ^= 1;
                    data = bytes.into();
                }
            }
            BodyFault::Truncate => {
                let limit = *this.limit.get_or_insert(data.len() as u64 / 2);
                let room = limit.saturating_sub(this.sent) as usize;
                if data.len() > room {
                    data.truncate(room);
                }
            }
        }
        this.sent += data.len() as u64;
        Poll::Ready(Some(Ok(Frame::data(data))))
    }

    fn is_end_stream(&self) -> bool {
        self.done || (self.fault != BodyFault::Truncate && self.inner.is_end_stream())
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
