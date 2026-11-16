//! Real-HTTP(S) harness for the query host: a publisher-side `Distribution`
//! publishing synthetic tachygram publications (real `tachyon-4k8` tables
//! from `tachyon-pir-table`'s scope builder) on 127.0.0.1, a TLS terminator in
//! front of it, and the query host in-process on its own listeners.
#![allow(dead_code)]

use axum::{Router, http::header, routing};
use pir_pipeline::service::Shutdown;
use pir_publication::ObjectReference;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tachyon_pir_distribution::{
    BlockRef, Committed, Contents, Distribution, DistributionOptions, Error, PublisherHooks,
    ScopeBinding, Subject, TableObjects, publisher::BoxFuture,
};
use tachyon_pir_protocol::{
    BlockHash, Bytes32, ChainMapChunk, ChainMapHead, Entry, PROFILE, PirParams, Product,
    QueryHeader, RecoveryEpoch, Scope, ScopeState, SealedChunkRef, Table, TachygramEntry,
    map_sha256, query::encode_request,
};
use tokio::{net::TcpListener, task::JoinHandle};
use zakura_pir_client::native::{P8, ProfileClientExt as _};

pub const PRODUCT: Product = Product::Tachygram;
pub const NETWORK: &str = "synthetic";
pub const ACTIVATION: u32 = 8;
pub const EPOCH: u32 = 16;
/// The newest open scope is anchored this many blocks after its first block.
pub const ANCHOR_OFFSET: u32 = 10;
/// Entries of a scope fall in its first `ENTRY_SPAN` blocks.
pub const ENTRY_SPAN: u32 = 10;

pub fn genesis() -> BlockHash {
    BlockHash::from_protocol_bytes([0x5a; 32])
}

pub fn binding() -> ScopeBinding {
    ScopeBinding::chain(
        *genesis().protocol_bytes(),
        u64::from(ACTIVATION),
        u64::from(EPOCH),
    )
}

pub fn first_height(scope_id: u64) -> u32 {
    ACTIVATION + scope_id as u32 * EPOCH
}

pub fn block(height: u32, fork: u8) -> BlockRef {
    let mut hash = [fork; 32];
    hash[..4].copy_from_slice(&height.to_le_bytes());
    BlockRef {
        height: u64::from(height),
        hash,
    }
}

fn digest(parts: &[&[u8]]) -> [u8; 32] {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(part);
    }
    hash.finalize().into()
}

/// `count` distinct tachygram entries of scope `scope_id`, in ordinal order.
pub fn entries(scope_id: u64, seed: u8, count: u32) -> Vec<Entry> {
    let first = first_height(scope_id);
    (0..count)
        .map(|i| {
            Entry::Tachygram(TachygramEntry {
                tachygram: digest(&[
                    b"tachygram",
                    &[seed],
                    &scope_id.to_le_bytes(),
                    &i.to_le_bytes(),
                ]),
                blk_height: first + i % ENTRY_SPAN,
                stamp_ordinal: i,
            })
        })
        .collect()
}

/// A key no fixture holds.
pub fn missing_key(i: u32) -> [u8; 32] {
    digest(&[b"missing", &i.to_le_bytes()])
}

/// One published table: its map entry, rows and public setup.
#[derive(Clone)]
pub struct PublishedTable {
    pub table: Table,
    pub rows: Arc<Vec<u8>>,
    pub public: Arc<Vec<u8>>,
}

/// The tables of one scope and the entries they hold (the oracle).
pub struct Fixture {
    pub scope_id: u64,
    pub entries: Vec<Entry>,
    pub n_buckets: u32,
    pub tables: Vec<PublishedTable>,
}

impl Fixture {
    pub fn session(&self) -> [u8; 32] {
        self.tables[0].table.session_id.0
    }
    pub fn entry_count(&self) -> u64 {
        self.tables.iter().map(|t| t.table.entries).sum()
    }
}

type Cache = Mutex<HashMap<(u64, u8, u32, u8), Arc<Fixture>>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(Cache::default)
}

/// Variants of a scope's tables.
const PLAIN: u8 = 0;
const CORRUPT_ROW: u8 = 1;
const WRONG_PUBLIC: u8 = 2;

fn build(scope_id: u64, seed: u8, count: u32) -> Fixture {
    let entries = entries(scope_id, seed, count);
    let built = tachyon_pir_table::build_scope(PRODUCT, &genesis(), scope_id, &entries).unwrap();
    let tables = built
        .tables
        .iter()
        .map(|table| {
            let described = tachyon_pir_table::describe(PRODUCT, scope_id, table).unwrap();
            PublishedTable {
                table: described.table,
                rows: Arc::new(table.rows.clone()),
                public: Arc::new(described.public),
            }
        })
        .collect();
    Fixture {
        scope_id,
        entries,
        n_buckets: built.n_buckets,
        tables,
    }
}

/// A scope of `count` entries; built (about 1 s per table) once per process.
pub fn fixture(scope_id: u64, seed: u8, count: u32) -> Arc<Fixture> {
    variant(scope_id, seed, count, PLAIN)
}

/// The scope with one row made non-canonical (two entries swapped out of key
/// order), with digests, public setup and session id consistent with the bad
/// rows, as a buggy publisher would publish it.
pub fn corrupt_fixture(scope_id: u64, seed: u8, count: u32) -> Arc<Fixture> {
    variant(scope_id, seed, count, CORRUPT_ROW)
}

/// The scope with another table's public setup named as its own (digest and
/// session id recomputed), so only preparing the rows can tell.
pub fn wrong_public_fixture(scope_id: u64, seed: u8, count: u32) -> Arc<Fixture> {
    variant(scope_id, seed, count, WRONG_PUBLIC)
}

fn variant(scope_id: u64, seed: u8, count: u32, kind: u8) -> Arc<Fixture> {
    let key = (scope_id, seed, count, kind);
    if let Some(found) = cache().lock().unwrap().get(&key) {
        return found.clone();
    }
    let made = match kind {
        PLAIN => build(scope_id, seed, count),
        CORRUPT_ROW => corrupt(&variant(scope_id, seed, count, PLAIN)),
        WRONG_PUBLIC => {
            let plain = variant(scope_id, seed, count, PLAIN);
            let other = variant(scope_id, seed.wrapping_add(100), count, PLAIN);
            let mut table = plain.tables[0].clone();
            table.public = other.tables[0].public.clone();
            table.table.public_sha256 = Bytes32(tachyon_pir_table::public_sha256(&table.public));
            table.table.session_id = table.table.expected_session_id(PRODUCT, scope_id);
            Fixture {
                scope_id,
                entries: plain.entries.clone(),
                n_buckets: plain.n_buckets,
                tables: vec![table],
            }
        }
        _ => unreachable!(),
    };
    let made = Arc::new(made);
    cache().lock().unwrap().entry(key).or_insert(made).clone()
}

fn corrupt(plain: &Fixture) -> Fixture {
    let source = &plain.tables[0];
    let entry = PRODUCT.entry_bytes();
    let mut rows = (*source.rows).clone();
    // The first row holding two entries: swap them, breaking key order.
    let row = rows
        .chunks_exact(tachyon_pir_protocol::ROW_BYTES)
        .position(|r| r[entry..2 * entry].iter().any(|b| *b != 0))
        .expect("some row holds two entries");
    let at = row * tachyon_pir_protocol::ROW_BYTES;
    let (first, second) = rows[at..at + 2 * entry].split_at_mut(entry);
    first.swap_with_slice(second);
    let built = tachyon_pir_table::BuiltTable {
        bucket: source.table.bucket,
        segment: source.table.segment,
        geometry: source.table.geometry,
        seed: source.table.seed,
        entries: source.table.entries as u32,
        rows_sha256: tachyon_pir_table::rows_sha256(&rows),
        rows,
    };
    let described = tachyon_pir_table::describe(PRODUCT, plain.scope_id, &built).unwrap();
    Fixture {
        scope_id: plain.scope_id,
        entries: plain.entries.clone(),
        n_buckets: plain.n_buckets,
        tables: vec![PublishedTable {
            table: described.table,
            rows: Arc::new(built.rows),
            public: Arc::new(described.public),
        }],
    }
}

/// The publisher's hooks: canonical blocks and head reissue.
#[derive(Default)]
pub struct Hooks {
    pub canonical: Mutex<BTreeMap<u64, [u8; 32]>>,
}

impl PublisherHooks for Hooks {
    fn check_subjects(&self, subjects: Vec<Subject>) -> BoxFuture<Result<Vec<bool>, Error>> {
        let canonical = self.canonical.lock().unwrap().clone();
        Box::pin(async move {
            Ok(subjects
                .iter()
                .map(|subject| {
                    subject
                        .blocks()
                        .iter()
                        .all(|b| canonical.get(&b.height) == Some(&b.hash))
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
        let mut head = ChainMapHead::parse(head)?;
        head.generation = generation;
        head.recovery_epoch = RecoveryEpoch::new(recovery_epoch).ok_or("epoch zero")?;
        Ok(head.to_bytes()?)
    }
}

/// The publisher side: a `Distribution` and the discovery document behind a
/// plain HTTP listener.
pub struct Publisher {
    pub distribution: Distribution,
    pub hooks: Arc<Hooks>,
    pub addr: SocketAddr,
    pub discovery_version: Arc<Mutex<String>>,
    server: JoinHandle<()>,
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Publisher {
    pub async fn start(state_dir: &Path) -> Self {
        let hooks = Arc::new(Hooks::default());
        let distribution = Distribution::open(
            state_dir,
            tachyon_pir_distribution::Product::Tachygram,
            binding(),
            hooks.clone(),
            DistributionOptions::default(),
        )
        .unwrap();
        let discovery_version = Arc::new(Mutex::new(pir_service_discovery::VERSION.to_string()));
        let version = discovery_version.clone();
        let app = distribution.mount(Router::new().route(
            pir_service_discovery::PATH,
            routing::get(move || {
                let version = version.lock().unwrap().clone();
                async move {
                    let document = pir_service_discovery::Document {
                        service: pir_service_discovery::PUBLISHER_SERVICE.into(),
                        version,
                    };
                    (
                        [(header::CONTENT_TYPE, "application/json")],
                        document.to_json(),
                    )
                }
            }),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            distribution,
            hooks,
            addr,
            discovery_version,
            server,
        }
    }

    /// Publish scopes `0..scopes.len()`: every scope but the newest sealed,
    /// the newest open and anchored `ANCHOR_OFFSET` blocks into it. `None` is
    /// a scope without entries. `fork` varies the block hashes, as a reorg.
    pub fn publish(&self, scopes: &[Option<Arc<Fixture>>], fork: u8) -> Committed {
        assert!(!scopes.is_empty() && scopes.len() <= 33);
        let newest = scopes.len() as u64 - 1;
        let anchor_height = first_height(newest) + ANCHOR_OFFSET;
        let anchor = block(anchor_height, fork);
        self.hooks
            .canonical
            .lock()
            .unwrap()
            .insert(anchor.height, anchor.hash);
        let draft = self.distribution.begin().unwrap();
        let mut listed = Vec::new();
        let mut objects = Vec::new();
        for (index, fixture) in scopes.iter().enumerate() {
            let scope_id = index as u64;
            let open = scope_id == newest;
            let first = first_height(scope_id);
            let (entries, n_buckets, tables) = match fixture {
                Some(f) => {
                    assert_eq!(f.scope_id, scope_id);
                    (f.entry_count(), f.n_buckets, f.tables.clone())
                }
                None => (0, 1, Vec::new()),
            };
            for table in &tables {
                objects.push(TableObjects {
                    scope_index: scope_id,
                    session_id: table.table.session_id.0,
                    rows: self.distribution.install(&table.rows).unwrap(),
                    public: self.distribution.install(&table.public).unwrap(),
                });
            }
            listed.push(Scope {
                scope_id,
                state: if open {
                    ScopeState::Open
                } else {
                    ScopeState::Sealed
                },
                first_height: first,
                last_height: if open {
                    anchor_height
                } else {
                    first + EPOCH - 1
                },
                entries,
                n_buckets,
                tables: tables.into_iter().map(|t| t.table).collect(),
            });
        }
        let open_scopes = vec![listed.pop().unwrap()];
        let mut sealed_chunks = Vec::new();
        let mut map_chunks = Vec::new();
        if !listed.is_empty() {
            let chunk = ChainMapChunk {
                protocol: PRODUCT.protocol().into(),
                base: 0,
                scopes: listed,
            };
            let bytes = chunk.to_bytes();
            sealed_chunks.push(SealedChunkRef {
                base: 0,
                sha256: map_sha256(&bytes),
            });
            map_chunks.push(self.distribution.install(&bytes).unwrap());
        }
        let head = ChainMapHead {
            protocol: PRODUCT.protocol().into(),
            profile: PROFILE.into(),
            network: NETWORK.into(),
            genesis_hash: genesis(),
            activation_height: ACTIVATION,
            epoch_length: EPOCH,
            generation: draft.generation,
            recovery_epoch: RecoveryEpoch::new(draft.recovery_epoch).unwrap(),
            anchor_height,
            anchor_hash: BlockHash::from_protocol_bytes(anchor.hash),
            floor_scope: 0,
            sealed_chunks,
            open_scopes,
        };
        head.validate(PRODUCT).unwrap();
        let head = self
            .distribution
            .install(&head.to_bytes().unwrap())
            .unwrap();
        self.distribution
            .publish(
                draft,
                Contents {
                    map_head: head,
                    map_chunks,
                    newest_scope: newest,
                    tables: objects,
                    subject: Subject::Chain {
                        anchor,
                        open_scopes: vec![anchor],
                    },
                },
            )
            .unwrap()
    }

    pub fn head(&self) -> ObjectReference {
        self.distribution
            .current_inventory()
            .unwrap()
            .unwrap()
            .map_head
    }
}

/// A TLS terminator forwarding bytes to the publisher's plain listener.
pub struct Edge {
    pub origin: String,
    pub ca_pem: Vec<u8>,
    task: JoinHandle<()>,
}

impl Drop for Edge {
    fn drop(&mut self) {
        self.task.abort();
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
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(socket).await else {
                        return;
                    };
                    let Ok(mut plain) = tokio::net::TcpStream::connect(upstream).await else {
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut tls, &mut plain).await;
                });
            }
        });
        Self {
            origin,
            ca_pem: cert.pem().into_bytes(),
            task,
        }
    }
}

/// The settings section of a host following `edge`, as JSON.
pub fn settings(edge: &Edge, dir: &Path) -> serde_json::Value {
    let ca = dir.join("publisher-ca.pem");
    std::fs::write(&ca, &edge.ca_pem).unwrap();
    serde_json::json!({
        "publisher": edge.origin,
        "state_dir": dir.join("state"),
        "publisher_ca": ca,
        "role": "combined",
        "publisher_network": NETWORK,
        "publisher_genesis": genesis().display_hex(),
        "activation_height": ACTIVATION,
        "epoch_length": EPOCH,
        "poll_ms": 100,
    })
}

/// The query host, in-process on its own listeners.
pub struct Host {
    pub public: SocketAddr,
    pub management: SocketAddr,
    stop: Shutdown,
    task: Option<JoinHandle<Result<(), tachyon_pir_query_server::Error>>>,
}

impl Host {
    /// Start a host with `settings` (see [`settings`]).
    pub async fn start(settings: serde_json::Value) -> Self {
        let management = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (management_addr, public_addr) = (
            management.local_addr().unwrap(),
            public.local_addr().unwrap(),
        );
        let config = serde_json::json!({
            "management_listen": management_addr,
            "public_listen": public_addr,
            "tachygram": {"enabled": true, "settings": settings},
        });
        let config = tachyon_pir_query_server::Config::parse(config.to_string().as_bytes())
            .unwrap()
            .validate()
            .unwrap();
        let stop = Shutdown::default();
        let task = tokio::spawn(tachyon_pir_query_server::serve(
            config,
            tachyon_pir_query_server::Listeners { management, public },
            stop.clone(),
        ));
        Self {
            public: public_addr,
            management: management_addr,
            stop,
            task: Some(task),
        }
    }

    pub fn url(&self, suffix: &str) -> String {
        format!("http://{}{}{}", self.public, PRODUCT.mount(), suffix)
    }

    pub fn origin(&self) -> String {
        format!("http://{}", self.public)
    }

    pub async fn ready_status(&self) -> u16 {
        reqwest::get(format!("http://{}/ready", self.management))
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    pub async fn wait_ready(&self) {
        let started = Instant::now();
        while self.ready_status().await != 200 {
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "the host did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn metrics(&self) -> String {
        reqwest::get(format!("http://{}/metrics", self.management))
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    }

    /// The value of the metric line starting with `name` (labels included).
    pub async fn metric(&self, name: &str) -> Option<u64> {
        self.metrics()
            .await
            .lines()
            .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
    }

    /// Wait until `GET map` names `sha256`.
    pub async fn wait_map(&self, sha256: &str) {
        let started = Instant::now();
        loop {
            let response = reqwest::get(self.url("/map")).await.unwrap();
            if response
                .headers()
                .get("x-map-sha256")
                .is_some_and(|v| v.to_str().unwrap() == sha256)
            {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "the host did not install map {sha256}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Request a stop and require a clean exit.
    pub async fn stop(mut self) {
        self.stop.request();
        let result = tokio::time::timeout(Duration::from_secs(20), self.task.take().unwrap())
            .await
            .expect("the host stopped in time")
            .unwrap();
        result.unwrap();
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.stop.request();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// PIR parameters of the published geometry, derived once.
pub fn params() -> &'static PirParams {
    static PARAMS: OnceLock<PirParams> = OnceLock::new();
    PARAMS.get_or_init(|| PirParams::new(PRODUCT, tachyon_pir_protocol::PUBLISHED_GEOMETRY))
}

/// A real query body for `row` of session `session`, with its header.
pub fn query_body(session: [u8; 32], row: u32) -> (QueryHeader, Vec<u8>) {
    let p = params();
    let (_, upload) = P8
        .prepare(p.setup(), p.masks(), p.rows(), row as usize)
        .unwrap();
    let header = QueryHeader::new(PRODUCT, session, [7; 16]);
    let body = encode_request(&header, &upload);
    (header, body)
}

/// POST a query body.
pub async fn post_query(host: &Host, body: Vec<u8>, client: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(host.url("/query"))
        .header("x-forwarded-for", client)
        .body(body)
        .send()
        .await
        .unwrap()
}

/// Read an error body and check it is spec 10.5's for `status`.
pub async fn error_body(
    response: reqwest::Response,
    status: u16,
) -> tachyon_pir_protocol::ErrorBody {
    assert_eq!(response.status().as_u16(), status);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["content-type"], "application/json");
    let retry = response
        .headers()
        .get("retry-after")
        .map(|v| v.to_str().unwrap().to_string());
    let body: tachyon_pir_protocol::ErrorBody =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    body.validate().unwrap();
    let expected = tachyon_pir_protocol::ErrorStatus::from_http_status(status)
        .map(|s| s.code())
        .unwrap_or(tachyon_pir_protocol::ErrorCode::InvalidRequest);
    assert_eq!(body.code, expected);
    if matches!(status, 429 | 503) {
        assert_eq!(
            retry.as_deref(),
            Some("1"),
            "HTTP {status} carries Retry-After"
        );
    }
    body
}

/// The row context and session of `key` in `fixture`'s scope as `head` lists it.
pub fn target_row(fixture: &Fixture, key: &[u8]) -> u32 {
    let table = &fixture.tables[0].table;
    tachyon_pir_protocol::placement::row(
        PRODUCT,
        &genesis(),
        fixture.scope_id,
        table.seed,
        table.geometry.rows(),
        key,
    )
    .unwrap()
}

pub fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

pub fn path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().to_path_buf()
}
