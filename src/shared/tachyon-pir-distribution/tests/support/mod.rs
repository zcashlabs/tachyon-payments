//! Real-HTTP(S) harness: a publisher-side `Distribution` served by axum on
//! 127.0.0.1, a TLS terminator in front of it, and a query-host follower.
#![allow(dead_code)]
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::header,
    middleware::{self, Next},
    response::Response,
    routing,
};
use pir_pipeline::consumer::{ConsumerPass, polling::SyncManager};
use pir_publication::ObjectReference;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tachyon_pir_distribution::{
    BlockRef, Committed, Contents, Distribution, DistributionOptions, Engine, Error,
    FollowerConfig, ObjectLimits, Preparer, Product, Publications, ScopeBinding, ServingPolicy,
    ServingReport, SessionId, SessionLookup, Subject, TableObjects, VerifiedPublication,
    VerifiedTable, follow, publisher::BoxFuture,
};
use tokio::{net::TcpListener, task::JoinHandle};

pub const PRODUCT: Product = Product::Tachygram;
pub const ROWS: u64 = 64;
pub const PUBLIC: u64 = 16;
pub const LIMITS: ObjectLimits = ObjectLimits {
    rows_bytes: ROWS,
    public_bytes: PUBLIC,
    ..ObjectLimits::SPEC
};
pub const GENESIS: [u8; 32] = [7; 32];

pub fn binding() -> ScopeBinding {
    ScopeBinding::chain(GENESIS, 8, 64)
}

pub fn block(height: u64, fork: u8) -> BlockRef {
    let mut hash = [fork; 32];
    hash[..8].copy_from_slice(&height.to_le_bytes());
    BlockRef { height, hash }
}

/// A small table blob: rows from a seed, public setup derived from the rows,
/// and a session id over both, as the real session id commits to them.
#[derive(Clone, Copy, Debug)]
pub struct Blob {
    pub scope: u64,
    pub seed: u8,
}

impl Blob {
    pub fn rows(&self) -> Vec<u8> {
        (0..ROWS as usize)
            .map(|i| self.seed.wrapping_mul(31).wrapping_add(i as u8))
            .collect()
    }
    pub fn public(&self) -> Vec<u8> {
        public_for(&self.rows())
    }
    pub fn session(&self) -> SessionId {
        let mut hash = Sha256::new();
        hash.update(b"test-session");
        hash.update(self.scope.to_le_bytes());
        hash.update(self.rows());
        hash.update(self.public());
        hash.finalize().into()
    }
}

pub fn public_for(rows: &[u8]) -> Vec<u8> {
    Sha256::digest(rows)[..PUBLIC as usize].to_vec()
}

/// The test's map head: what a product's real map head binds that the
/// generic layer cannot read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Head {
    pub generation: u64,
    pub recovery_epoch: u64,
    pub sessions: Vec<String>,
}

pub struct Hooks {
    /// Height -> canonical hash at the publisher.
    pub canonical: Mutex<BTreeMap<u64, [u8; 32]>>,
    pub checks: AtomicUsize,
}

impl Hooks {
    pub fn canonical(&self, blocks: &[BlockRef]) {
        let mut canonical = self.canonical.lock().unwrap();
        for block in blocks {
            canonical.insert(block.height, block.hash);
        }
    }
}

impl tachyon_pir_distribution::PublisherHooks for Hooks {
    fn check_subjects(&self, subjects: Vec<Subject>) -> BoxFuture<Result<Vec<bool>, Error>> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        let canonical = self.canonical.lock().unwrap().clone();
        Box::pin(async move {
            Ok(subjects
                .iter()
                .map(|subject| match subject {
                    Subject::Chain { .. } => subject
                        .blocks()
                        .iter()
                        .all(|b| canonical.get(&b.height) == Some(&b.hash)),
                    Subject::Registry { .. } => true,
                })
                .collect())
        })
    }
    fn reissue_head(
        &self,
        head: &[u8],
        generation: u64,
        recovery_epoch: u64,
    ) -> Result<Vec<u8>, Error> {
        let mut head: Head = serde_json::from_slice(head)?;
        head.generation = generation;
        head.recovery_epoch = recovery_epoch;
        Ok(serde_json::to_vec(&head)?)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    None,
    Corrupt(String),
    Truncate(String),
}

/// What the publisher's listener saw and how it misbehaves.
#[derive(Default)]
pub struct Observed {
    pub object_gets: Mutex<Vec<String>>,
    pub fault: Mutex<Option<Fault>>,
}

impl Observed {
    pub fn take_gets(&self) -> Vec<String> {
        std::mem::take(&mut *self.object_gets.lock().unwrap())
    }
}

async fn observe(State(observed): State<Arc<Observed>>, request: Request, next: Next) -> Response {
    let path = request.uri().path().to_string();
    let object = path
        .strip_prefix(&format!("{}objects/", PRODUCT.prefix()))
        .map(str::to_string);
    if let Some(id) = &object {
        observed.object_gets.lock().unwrap().push(id.clone());
    }
    let response = next.run(request).await;
    let fault = observed.fault.lock().unwrap().clone();
    let target = match (&fault, &object) {
        (Some(Fault::Corrupt(sha)), Some(id)) | (Some(Fault::Truncate(sha)), Some(id))
            if sha == id =>
        {
            fault
        }
        _ => return response,
    };
    let (mut parts, body) = response.into_parts();
    let mut bytes = to_bytes(body, usize::MAX).await.unwrap().to_vec();
    match target {
        Some(Fault::Corrupt(_)) => bytes[0] ^= 1,
        Some(Fault::Truncate(_)) => bytes.truncate(bytes.len() / 2),
        _ => unreachable!(),
    }
    parts.headers.remove(header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(bytes))
}

/// The publisher side: a `Distribution` behind a plain HTTP listener.
pub struct Publisher {
    pub distribution: Distribution,
    pub hooks: Arc<Hooks>,
    pub observed: Arc<Observed>,
    pub addr: SocketAddr,
    pub state_dir: PathBuf,
    server: JoinHandle<()>,
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Publisher {
    pub async fn start(state_dir: &Path, retention: Duration) -> Self {
        Self::start_with(
            state_dir,
            retention,
            Arc::new(Hooks {
                canonical: Mutex::default(),
                checks: AtomicUsize::new(0),
            }),
        )
        .await
    }

    pub async fn start_with(state_dir: &Path, retention: Duration, hooks: Arc<Hooks>) -> Self {
        let distribution = Distribution::open(
            state_dir,
            PRODUCT,
            binding(),
            hooks.clone(),
            DistributionOptions {
                retention,
                limits: LIMITS,
            },
        )
        .unwrap();
        let observed = Arc::new(Observed::default());
        let app = distribution
            .mount(Router::new().route(
                pir_service_discovery::PATH,
                routing::get(|| async {
                    (
                        [(header::CONTENT_TYPE, "application/json")],
                        pir_service_discovery::Document::publisher().to_json(),
                    )
                }),
            ))
            .layer(middleware::from_fn_with_state(observed.clone(), observe));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            distribution,
            hooks,
            observed,
            addr,
            state_dir: state_dir.into(),
            server,
        }
    }

    /// Restart: stop serving, reopen the distribution in the same directory.
    pub async fn restart(self, retention: Duration) -> Self {
        let (state_dir, hooks) = (self.state_dir.clone(), self.hooks.clone());
        drop(self);
        Self::start_with(&state_dir, retention, hooks).await
    }

    /// Install and publish `tables` with one map chunk, anchored at `anchor`.
    pub fn publish(&self, tables: &[Blob], anchor: BlockRef) -> Result<Committed, Error> {
        let draft = self.distribution.begin()?;
        let contents = self.contents(&draft, tables, anchor)?;
        self.distribution.publish(draft, contents)
    }

    pub fn contents(
        &self,
        draft: &tachyon_pir_distribution::Draft,
        tables: &[Blob],
        anchor: BlockRef,
    ) -> Result<Contents, Error> {
        self.hooks.canonical(&[anchor]);
        let mut objects = Vec::new();
        for blob in tables {
            objects.push(TableObjects {
                scope_index: blob.scope,
                session_id: blob.session(),
                rows: self.distribution.install(&blob.rows())?,
                public: self.distribution.install(&blob.public())?,
            });
        }
        let head = Head {
            generation: draft.generation,
            recovery_epoch: draft.recovery_epoch,
            sessions: tables.iter().map(|b| hex::encode(b.session())).collect(),
        };
        let newest_scope = tables.iter().map(|b| b.scope).max().unwrap_or(0);
        Ok(Contents {
            map_head: self.distribution.install(&serde_json::to_vec(&head)?)?,
            map_chunks: vec![self.distribution.install(b"sealed map chunk 0")?],
            newest_scope,
            tables: objects,
            subject: Subject::Chain {
                anchor,
                open_scopes: vec![anchor],
            },
        })
    }

    pub fn object_path(&self, object: &ObjectReference) -> PathBuf {
        self.state_dir
            .join("distribution")
            .join(PRODUCT.name())
            .join("objects")
            .join(&object.sha256)
    }

    pub fn url(&self, suffix: &str) -> String {
        format!("http://{}{}{}", self.addr, PRODUCT.prefix(), suffix)
    }
}

/// A TLS terminator forwarding bytes to the publisher's plain listener.
pub struct Edge {
    pub origin: String,
    pub ca_pem: Vec<u8>,
    upstream: Arc<Mutex<SocketAddr>>,
    down: Arc<AtomicBool>,
    connections: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
    task: JoinHandle<()>,
}

impl Drop for Edge {
    fn drop(&mut self) {
        self.task.abort();
        self.sever();
    }
}

impl Edge {
    pub async fn start(upstream: SocketAddr) -> Self {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
        let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params.is_ca = rcgen::IsCa::ExplicitNoCa;
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(cert.der().to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!(
            "https://localhost:{}/",
            listener.local_addr().unwrap().port()
        );
        let upstream = Arc::new(Mutex::new(upstream));
        let down = Arc::new(AtomicBool::new(false));
        let connections: Arc<Mutex<Vec<tokio::task::AbortHandle>>> = Arc::default();
        let task = tokio::spawn({
            let (upstream, down, connections) =
                (upstream.clone(), down.clone(), connections.clone());
            async move {
                while let Ok((socket, _)) = listener.accept().await {
                    if down.load(Ordering::SeqCst) {
                        drop(socket);
                        continue;
                    }
                    let acceptor = acceptor.clone();
                    let target = *upstream.lock().unwrap();
                    let connection = tokio::spawn(async move {
                        let Ok(mut tls) = acceptor.accept(socket).await else {
                            return;
                        };
                        let Ok(mut plain) = tokio::net::TcpStream::connect(target).await else {
                            return;
                        };
                        let _ = tokio::io::copy_bidirectional(&mut tls, &mut plain).await;
                    });
                    connections.lock().unwrap().push(connection.abort_handle());
                }
            }
        });
        Self {
            origin,
            ca_pem: cert.pem().into_bytes(),
            upstream,
            down,
            connections,
            task,
        }
    }

    fn sever(&self) {
        for connection in self.connections.lock().unwrap().drain(..) {
            connection.abort();
        }
    }

    /// Refuse every connection (an outage), or accept again.
    pub fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
        self.sever();
    }

    pub fn set_upstream(&self, upstream: SocketAddr) {
        *self.upstream.lock().unwrap() = upstream;
        self.sever();
    }
}

pub type Runtime = Vec<u8>;

/// The test product's preparation: checks the map head against the
/// inventory and every prepared public setup against the published one.
#[derive(Default)]
pub struct Stats {
    pub prepared: Mutex<Vec<SessionId>>,
    pub refuse: AtomicBool,
}

pub struct TestPreparer(pub Arc<Stats>);

impl Preparer for TestPreparer {
    type Runtime = Runtime;
    fn check(&self, publication: &VerifiedPublication<'_>) -> Result<(), Error> {
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err("the product refuses this publication".into());
        }
        let head: Head = serde_json::from_slice(publication.map_head)?;
        let inventory = publication.inventory;
        let listed: BTreeSet<_> = inventory
            .tables
            .iter()
            .map(|t| hex::encode(t.session_id))
            .collect();
        if head.generation != inventory.generation
            || head.recovery_epoch != inventory.recovery_epoch
            || head.sessions.iter().cloned().collect::<BTreeSet<_>>() != listed
        {
            return Err("map head differs from the inventory".into());
        }
        Ok(())
    }
    fn prepare(&self, table: &VerifiedTable) -> Result<Runtime, Error> {
        let rows = table.read_rows()?;
        if public_for(&rows) != *table.public {
            return Err("prepared public setup differs from the published object".into());
        }
        self.0
            .prepared
            .lock()
            .unwrap()
            .push(table.objects.session_id);
        Ok(rows)
    }
}

/// The query-host side following the edge.
pub struct Host {
    pub manager: SyncManager<Engine<TestPreparer>>,
    pub publications: Publications<Runtime>,
    pub stats: Arc<Stats>,
    pub state_dir: PathBuf,
}

impl Host {
    pub fn start(
        edge: &Edge,
        state_dir: &Path,
        policy: ServingPolicy,
        retain_scopes: Option<u64>,
    ) -> Self {
        let stats = Arc::new(Stats::default());
        let publications = Publications::new(policy);
        let mut config =
            FollowerConfig::new(PRODUCT, binding(), edge.origin.clone(), state_dir.into());
        config.publisher_ca = Some(edge.ca_pem.clone());
        config.limits = LIMITS;
        config.retain_scopes = retain_scopes;
        let engine = follow(config, TestPreparer(stats.clone()), publications.clone()).unwrap();
        Self {
            manager: SyncManager::new(engine),
            publications,
            stats,
            state_dir: state_dir.into(),
        }
    }

    pub async fn poll(&mut self) -> Result<ConsumerPass<Option<ServingReport>>, Error> {
        self.manager.poll_once().await
    }

    /// Poll as `SyncManager` does across a publisher restart: a failure the
    /// engine classifies as transient (a connection the restart cut) is
    /// retried; any other failure is returned.
    pub async fn poll_reconnecting(
        &mut self,
    ) -> Result<ConsumerPass<Option<ServingReport>>, Error> {
        use pir_pipeline::consumer::polling::SyncTask;
        for _ in 0..5 {
            match self.poll().await {
                Err(error) if self.manager.task().transient(&error) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                result => return result,
            }
        }
        self.poll().await
    }

    /// Poll and require a commit.
    pub async fn commit(&mut self) -> ServingReport {
        match self.poll().await {
            Ok(ConsumerPass::Committed {
                report: Some(report),
                ..
            }) => report,
            other => panic!(
                "expected a commit, got {:?}",
                other.map(|p| p.into_report())
            ),
        }
    }

    pub fn status(&self, blob: &Blob) -> u16 {
        self.publications.session(&blob.session()).status()
    }

    pub fn found(&self, blob: &Blob) -> Vec<u8> {
        match self.publications.session(&blob.session()) {
            SessionLookup::Found { table, .. } => {
                assert_eq!(*table.public, *blob.public());
                (*table.runtime).clone()
            }
            other => panic!("session {blob:?} is not served: HTTP {}", other.status()),
        }
    }

    pub fn cached(&self, object: &ObjectReference) -> bool {
        self.state_dir.join("objects").join(&object.sha256).exists()
    }

    pub fn prepared(&self) -> Vec<SessionId> {
        std::mem::take(&mut *self.stats.prepared.lock().unwrap())
    }
}

pub async fn get(url: &str, if_none_match: Option<&str>) -> reqwest::Response {
    let mut request = reqwest::Client::new().get(url);
    if let Some(tag) = if_none_match {
        request = request.header(header::IF_NONE_MATCH.as_str(), tag);
    }
    request.send().await.unwrap()
}
