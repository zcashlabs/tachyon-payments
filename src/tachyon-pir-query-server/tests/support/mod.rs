//! Real-HTTP(S) harness for the query host: a publisher-side `Distribution`
//! publishing synthetic publications of any chain product (real
//! `tachyon-4k8` tables from `tachyon-pir-table`'s scope builder) on
//! 127.0.0.1, a TLS terminator in front of it, and the query host in-process
//! on its own listeners.
#![allow(dead_code)]

use axum::{Router, http::header, routing};
use pir_pipeline::service::Shutdown;
use pir_publication::ObjectReference;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tachyon_pir_cli::{Client, LookupOutcome, ScopeSelector};
use tachyon_pir_distribution::{
    BlockRef, Committed, Contents, Distribution, DistributionOptions, Error, PublisherHooks,
    ScopeBinding, Subject, TableObjects, publisher::BoxFuture,
};
use tachyon_pir_protocol::{
    Address, BlockHash, Bytes32, ChainMapChunk, ChainMapHead, Entry, FirstContactEntry,
    KEM_CT_BYTES, KEM_EK_BYTES, MEMO_CT_BYTES, MemoEntry, PK_BYTES, PROFILE, PirParams, Product,
    QueryHeader, REGISTRY_CHUNK_ENTRIES, RecoveryEpoch, RegistryEntry, RegistryMapChunk,
    RegistryMapHead, RegistryScope, Scope, ScopeState, SealedChunkRef, Table, TachygramEntry,
    map_sha256, query::encode_request,
};
use tokio::{net::TcpListener, task::JoinHandle};
use zakura_pir_client::native::{P8, ProfileClientExt as _};

pub const NETWORK: &str = "synthetic";
pub const ACTIVATION: u32 = 8;
pub const EPOCH: u32 = 16;
/// The newest open scope is anchored this many blocks after its first block.
pub const ANCHOR_OFFSET: u32 = 10;
/// Tachygram entries of a scope fall in its first `ENTRY_SPAN` blocks.
pub const ENTRY_SPAN: u32 = 10;

pub fn genesis() -> BlockHash {
    BlockHash::from_protocol_bytes([0x5a; 32])
}

pub fn binding(product: Product) -> ScopeBinding {
    match product {
        Product::Registry => {
            ScopeBinding::registry(*genesis().protocol_bytes(), REGISTRY_CHUNK_ENTRIES)
        }
        _ => ScopeBinding::chain(
            *genesis().protocol_bytes(),
            u64::from(ACTIVATION),
            u64::from(EPOCH),
        ),
    }
}

/// The first block of the scope starting at epoch `scope_id`.
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

/// `N` bytes derived from `parts`.
fn bytes<const N: usize>(parts: &[&[u8]]) -> [u8; N] {
    let mut out = [0u8; N];
    for (i, chunk) in out.chunks_mut(32).enumerate() {
        let mut all: Vec<&[u8]> = parts.to_vec();
        let counter = (i as u32).to_le_bytes();
        all.push(&counter);
        let block = digest(&all);
        chunk.copy_from_slice(&block[..chunk.len()]);
    }
    out
}

/// One entry of `product` with the 32-byte `value` as its key (truncated to
/// `key16` where the product stores that) and payload derived from `salt`.
fn entry(product: Product, value: [u8; 32], salt: &[u8], height: u32, ordinal: u32) -> Entry {
    let key16: [u8; 16] = value[..16].try_into().unwrap();
    match product {
        Product::Memo => Entry::Memo(MemoEntry {
            key: key16,
            ciphertext: bytes::<MEMO_CT_BYTES>(&[b"ct", salt]),
        }),
        Product::FirstContact => Entry::FirstContact(Box::new(FirstContactEntry {
            key: key16,
            kem_ciphertext: bytes::<KEM_CT_BYTES>(&[b"kem", salt]),
            ciphertext: bytes::<MEMO_CT_BYTES>(&[b"ct", salt]),
        })),
        Product::Tachygram => Entry::Tachygram(TachygramEntry {
            tachygram: value,
            blk_height: height,
            stamp_ordinal: ordinal,
        }),
        Product::Registry => Entry::Registry(Box::new(RegistryEntry::new(Address {
            pk: bytes::<PK_BYTES>(&[b"pk", salt]),
            ek: bytes::<KEM_EK_BYTES>(&[b"ek", salt]),
        }))),
    }
}

/// What a scope's fixture holds: `count` entries with distinct keys and, for
/// products whose keys may repeat, `repeats` more entries sharing one key,
/// spread through the ordinal order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Spec {
    pub product: Product,
    pub scope_id: u64,
    pub seed: u8,
    pub count: u32,
    pub repeats: u32,
}

impl Spec {
    pub const fn new(product: Product, scope_id: u64, seed: u8, count: u32) -> Self {
        Self {
            product,
            scope_id,
            seed,
            count,
            repeats: 0,
        }
    }

    pub const fn repeating(self, repeats: u32) -> Self {
        Self { repeats, ..self }
    }

    /// The key every repeated entry carries.
    pub fn repeated_key(&self) -> Vec<u8> {
        let value = digest(&[b"repeat", &[self.seed], &self.scope_id.to_le_bytes()]);
        self.product.key_of(&value).to_vec()
    }

    /// The entries in ordinal order.
    pub fn entries(&self) -> Vec<Entry> {
        // Tachygrams are unique; a registry address is logged once.
        assert!(
            self.repeats == 0 || !matches!(self.product, Product::Tachygram | Product::Registry)
        );
        let first = first_height(self.scope_id);
        let repeated = digest(&[b"repeat", &[self.seed], &self.scope_id.to_le_bytes()]);
        let total = self.count + self.repeats;
        let every = total
            .checked_div(self.repeats)
            .map_or(u32::MAX, |n| n.max(1));
        let (mut unique, mut copies) = (0u32, 0u32);
        (0..total)
            .map(|ordinal| {
                let salt = [
                    &[self.seed][..],
                    &self.scope_id.to_le_bytes(),
                    &ordinal.to_le_bytes(),
                ]
                .concat();
                let height = first + ordinal % ENTRY_SPAN;
                let take_copy =
                    copies < self.repeats && (ordinal % every == 0 || unique >= self.count);
                if take_copy {
                    copies += 1;
                    entry(self.product, repeated, &salt, height, ordinal)
                } else {
                    unique += 1;
                    let value = digest(&[
                        b"entry",
                        &[self.seed],
                        &self.scope_id.to_le_bytes(),
                        &unique.to_le_bytes(),
                    ]);
                    entry(self.product, value, &salt, height, ordinal)
                }
            })
            .collect()
    }
}

/// A key no fixture holds, as the product stores keys.
pub fn missing_key(product: Product, i: u32) -> Vec<u8> {
    product
        .key_of(&digest(&[b"missing", &i.to_le_bytes()]))
        .to_vec()
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
    pub product: Product,
    pub scope_id: u64,
    pub entries: Vec<Entry>,
    pub n_buckets: u32,
    pub tables: Vec<PublishedTable>,
}

impl Fixture {
    /// The session of the scope's first table.
    pub fn session(&self) -> [u8; 32] {
        self.tables[0].table.session_id.0
    }
    pub fn entry_count(&self) -> u64 {
        self.tables.iter().map(|t| t.table.entries).sum()
    }
    /// Every entry with `key`, in ordinal order: what a lookup must return.
    pub fn expected(&self, key: &[u8]) -> Vec<Entry> {
        self.entries
            .iter()
            .filter(|e| e.key() == key)
            .cloned()
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Kind {
    Plain,
    CorruptRow,
    WrongPublic,
}

type Cache = Mutex<HashMap<(Spec, Kind), Arc<Fixture>>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(Cache::default)
}

fn build(spec: Spec) -> Fixture {
    let entries = spec.entries();
    let built =
        tachyon_pir_table::build_scope(spec.product, &genesis(), spec.scope_id, &entries).unwrap();
    let tables = built
        .tables
        .iter()
        .map(|table| {
            let described =
                tachyon_pir_table::describe(spec.product, spec.scope_id, table).unwrap();
            PublishedTable {
                table: described.table,
                rows: Arc::new(table.rows.clone()),
                public: Arc::new(described.public),
            }
        })
        .collect();
    Fixture {
        product: spec.product,
        scope_id: spec.scope_id,
        entries,
        n_buckets: built.n_buckets,
        tables,
    }
}

/// The scope's tables; built (about 1 s per table) once per process.
pub fn fixture(spec: Spec) -> Arc<Fixture> {
    variant(spec, Kind::Plain)
}

/// The scope with a nonzero byte in the zero tail of row 0 of its first
/// table, with digests, public setup and session id consistent with the bad
/// rows, as a buggy publisher would publish it.
pub fn corrupt_fixture(spec: Spec) -> Arc<Fixture> {
    variant(spec, Kind::CorruptRow)
}

/// The scope with another table's public setup named as its own (digest and
/// session id recomputed), so only preparing the rows can tell.
pub fn wrong_public_fixture(spec: Spec) -> Arc<Fixture> {
    variant(spec, Kind::WrongPublic)
}

fn variant(spec: Spec, kind: Kind) -> Arc<Fixture> {
    if let Some(found) = cache().lock().unwrap().get(&(spec, kind)) {
        return found.clone();
    }
    let made = match kind {
        Kind::Plain => build(spec),
        Kind::CorruptRow => corrupt(&variant(spec, Kind::Plain)),
        Kind::WrongPublic => {
            let plain = variant(spec, Kind::Plain);
            let other = variant(
                Spec {
                    seed: spec.seed.wrapping_add(100),
                    ..spec
                },
                Kind::Plain,
            );
            let mut table = plain.tables[0].clone();
            table.public = other.tables[0].public.clone();
            table.table.public_sha256 = Bytes32(tachyon_pir_table::public_sha256(&table.public));
            table.table.session_id = table.table.expected_session_id(spec.product, spec.scope_id);
            Fixture {
                product: spec.product,
                scope_id: spec.scope_id,
                entries: plain.entries.clone(),
                n_buckets: plain.n_buckets,
                tables: vec![table],
            }
        }
    };
    let made = Arc::new(made);
    cache()
        .lock()
        .unwrap()
        .entry((spec, kind))
        .or_insert(made)
        .clone()
}

fn corrupt(plain: &Fixture) -> Fixture {
    let source = &plain.tables[0];
    let mut rows = (*source.rows).clone();
    // The last byte of row 0 lies past every product's last slot.
    rows[tachyon_pir_protocol::ROW_BYTES - 1] = 1;
    let built = tachyon_pir_table::BuiltTable {
        bucket: source.table.bucket,
        segment: source.table.segment,
        geometry: source.table.geometry,
        seed: source.table.seed,
        entries: source.table.entries as u32,
        rows_sha256: tachyon_pir_table::rows_sha256(&rows),
        rows,
    };
    let described = tachyon_pir_table::describe(plain.product, plain.scope_id, &built).unwrap();
    Fixture {
        product: plain.product,
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
pub struct Hooks {
    pub product: Product,
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
        let epoch = RecoveryEpoch::new(recovery_epoch).ok_or("epoch zero")?;
        if self.product == Product::Registry {
            let mut head = RegistryMapHead::parse(head)?;
            head.generation = generation;
            head.recovery_epoch = epoch;
            return Ok(head.to_bytes()?);
        }
        let mut head = ChainMapHead::parse(head)?;
        head.generation = generation;
        head.recovery_epoch = epoch;
        Ok(head.to_bytes()?)
    }
}

/// One listed scope of a publication: where it starts (its first epoch, the
/// `scope_id`), how many epochs it covers, and its tables (`None` for a
/// scope without entries).
#[derive(Clone)]
pub struct Slot {
    pub scope_id: u64,
    pub epochs: u32,
    pub fixture: Option<Arc<Fixture>>,
}

impl Slot {
    pub fn epoch(scope_id: u64, fixture: Option<Arc<Fixture>>) -> Self {
        Self {
            scope_id,
            epochs: 1,
            fixture,
        }
    }
    pub fn span(scope_id: u64, epochs: u32, fixture: Option<Arc<Fixture>>) -> Self {
        Self {
            scope_id,
            epochs,
            fixture,
        }
    }
}

/// The publisher side: a `Distribution` and the discovery document behind a
/// plain HTTP listener.
pub struct Publisher {
    pub product: Product,
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
    pub async fn start(product: Product, state_dir: &Path) -> Self {
        let hooks = Arc::new(Hooks {
            product,
            canonical: Mutex::default(),
        });
        let distribution = Distribution::open(
            state_dir,
            tachyon_pir_distribution::Product::from_name(product.name()).unwrap(),
            binding(product),
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
            product,
            distribution,
            hooks,
            addr,
            discovery_version,
            server,
        }
    }

    /// Publish one-epoch scopes `0..scopes.len()` (see [`Publisher::publish_slots`]).
    pub fn publish(&self, scopes: &[Option<Arc<Fixture>>], fork: u8) -> Committed {
        let slots: Vec<Slot> = scopes
            .iter()
            .enumerate()
            .map(|(i, f)| Slot::epoch(i as u64, f.clone()))
            .collect();
        self.publish_slots(&slots, fork)
    }

    /// Publish consecutive scopes: every scope but the newest sealed, the
    /// newest open and anchored `ANCHOR_OFFSET` blocks into it. The scope
    /// index is the position in `slots`. `fork` varies the block hashes, as
    /// a reorg does.
    pub fn publish_slots(&self, slots: &[Slot], fork: u8) -> Committed {
        assert!(!slots.is_empty() && slots.len() <= 33);
        assert_eq!(slots[0].scope_id, 0);
        for pair in slots.windows(2) {
            assert_eq!(
                pair[1].scope_id,
                pair[0].scope_id + u64::from(pair[0].epochs)
            );
        }
        let newest = slots.len() as u64 - 1;
        let anchor_height = first_height(slots[slots.len() - 1].scope_id) + ANCHOR_OFFSET;
        let anchor = block(anchor_height, fork);
        self.hooks
            .canonical
            .lock()
            .unwrap()
            .insert(anchor.height, anchor.hash);
        let draft = self.distribution.begin().unwrap();
        let mut listed = Vec::new();
        let mut objects = Vec::new();
        for (index, slot) in slots.iter().enumerate() {
            let open = index as u64 == newest;
            let first = first_height(slot.scope_id);
            let (entries, n_buckets, tables) = match &slot.fixture {
                Some(f) => {
                    assert_eq!((f.product, f.scope_id), (self.product, slot.scope_id));
                    (f.entry_count(), f.n_buckets, f.tables.clone())
                }
                None => (0, 1, Vec::new()),
            };
            for table in &tables {
                objects.push(TableObjects {
                    scope_index: index as u64,
                    session_id: table.table.session_id.0,
                    rows: self.distribution.install(&table.rows).unwrap(),
                    public: self.distribution.install(&table.public).unwrap(),
                });
            }
            listed.push(Scope {
                scope_id: slot.scope_id,
                state: if open {
                    ScopeState::Open
                } else {
                    ScopeState::Sealed
                },
                first_height: first,
                last_height: if open {
                    anchor_height
                } else {
                    first + slot.epochs * EPOCH - 1
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
                protocol: self.product.protocol().into(),
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
            protocol: self.product.protocol().into(),
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
        head.validate(self.product).unwrap();
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

    /// Publish registry chunks `0..chunks.len()`: every chunk but the last
    /// sealed and full, the last open (`None`: no entries). `log_sequence`
    /// follows from the chunks (spec 10.3).
    pub fn publish_registry(&self, chunks: &[Option<Arc<Fixture>>]) -> Committed {
        assert_eq!(self.product, Product::Registry);
        assert!(!chunks.is_empty() && chunks.len() <= 33);
        let draft = self.distribution.begin().unwrap();
        let newest = chunks.len() as u64 - 1;
        let mut listed = Vec::new();
        let mut objects = Vec::new();
        let mut log_sequence = 0;
        for (index, fixture) in chunks.iter().enumerate() {
            let chunk_id = index as u64;
            let open = chunk_id == newest;
            let (entries, tables) = match fixture {
                Some(f) => {
                    assert_eq!(
                        (f.product, f.scope_id, f.n_buckets),
                        (self.product, chunk_id, 1)
                    );
                    (f.entry_count(), f.tables.clone())
                }
                None => (0, Vec::new()),
            };
            if !open {
                assert_eq!(entries, REGISTRY_CHUNK_ENTRIES, "sealed chunks are full");
            }
            log_sequence += entries;
            for table in &tables {
                objects.push(TableObjects {
                    scope_index: chunk_id,
                    session_id: table.table.session_id.0,
                    rows: self.distribution.install(&table.rows).unwrap(),
                    public: self.distribution.install(&table.public).unwrap(),
                });
            }
            listed.push(RegistryScope {
                scope_id: chunk_id,
                state: if open {
                    ScopeState::Open
                } else {
                    ScopeState::Sealed
                },
                entries,
                n_buckets: 1,
                tables: tables.into_iter().map(|t| t.table).collect(),
            });
        }
        let open_scopes = vec![listed.pop().unwrap()];
        let mut sealed_chunks = Vec::new();
        let mut map_chunks = Vec::new();
        if !listed.is_empty() {
            let chunk = RegistryMapChunk {
                protocol: self.product.protocol().into(),
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
        let head = RegistryMapHead {
            protocol: self.product.protocol().into(),
            profile: PROFILE.into(),
            network: NETWORK.into(),
            genesis_hash: genesis(),
            generation: draft.generation,
            recovery_epoch: RecoveryEpoch::new(draft.recovery_epoch).unwrap(),
            log_sequence,
            floor_scope: 0,
            sealed_chunks,
            open_scopes,
        };
        head.validate().unwrap();
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
                    subject: Subject::Registry { log_sequence },
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

/// The settings section of a registry host following `edge`: no chain
/// parameters.
pub fn registry_settings(edge: &Edge, dir: &Path) -> serde_json::Value {
    let mut value = settings(edge, dir);
    let object = value.as_object_mut().unwrap();
    object.remove("activation_height");
    object.remove("epoch_length");
    value
}

/// The settings section of a chain-product host following `edge`, as JSON.
pub fn settings(edge: &Edge, dir: &Path) -> serde_json::Value {
    std::fs::create_dir_all(dir).unwrap();
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

/// The configuration section name of `product`.
pub fn section(product: Product) -> &'static str {
    match product {
        Product::Memo => "memo",
        Product::FirstContact => "first_contact",
        Product::Tachygram => "tachygram",
        Product::Registry => "registry",
    }
}

/// The query host, in-process on its own listeners.
pub struct Host {
    pub product: Product,
    pub public: SocketAddr,
    pub management: SocketAddr,
    stop: Shutdown,
    task: Option<JoinHandle<Result<(), tachyon_pir_query_server::Error>>>,
}

impl Host {
    /// Start a host serving `product` with `settings` (see [`settings`]).
    pub async fn start(product: Product, settings: serde_json::Value) -> Self {
        Self::start_products(product, &[(product, settings)]).await
    }

    /// Start a host serving several products; `primary` is the one
    /// [`Host::url`] addresses.
    pub async fn start_products(
        primary: Product,
        products: &[(Product, serde_json::Value)],
    ) -> Self {
        let management = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (management_addr, public_addr) = (
            management.local_addr().unwrap(),
            public.local_addr().unwrap(),
        );
        let mut config = serde_json::json!({
            "management_listen": management_addr,
            "public_listen": public_addr,
        });
        for (product, settings) in products {
            config[section(*product)] = serde_json::json!({"enabled": true, "settings": settings});
        }
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
            product: primary,
            public: public_addr,
            management: management_addr,
            stop,
            task: Some(task),
        }
    }

    pub fn url(&self, suffix: &str) -> String {
        format!("http://{}{}{}", self.public, self.product.mount(), suffix)
    }

    pub fn origin(&self) -> String {
        format!("http://{}", self.public)
    }

    pub fn client(&self) -> Client {
        Client::new(&self.origin(), self.product, None).unwrap()
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

    /// Wait until the host has failed at least `n` synchronization passes of
    /// its primary product.
    pub async fn wait_failed_passes(&self, n: u64) {
        let name = format!(
            "tachyon_{}_sync_passes_total{{result=\"failed\"}}",
            self.product.name().replace('-', "_")
        );
        let started = Instant::now();
        while self.metric(&name).await.unwrap_or(0) < n {
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "no pass failed"
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

/// PIR parameters of the published geometry, derived once per product.
pub fn params(product: Product) -> Arc<PirParams> {
    type Params = Mutex<HashMap<Product, Arc<PirParams>>>;
    static PARAMS: OnceLock<Params> = OnceLock::new();
    PARAMS
        .get_or_init(Params::default)
        .lock()
        .unwrap()
        .entry(product)
        .or_insert_with(|| {
            Arc::new(PirParams::new(
                product,
                tachyon_pir_protocol::PUBLISHED_GEOMETRY,
            ))
        })
        .clone()
}

/// A real query body of `product` for `row` of session `session`.
pub fn query_body(product: Product, session: [u8; 32], row: u32) -> (QueryHeader, Vec<u8>) {
    let p = params(product);
    let (_, upload) = P8
        .prepare(p.setup(), p.masks(), p.rows(), row as usize)
        .unwrap();
    let header = QueryHeader::new(product, session, [7; 16]);
    let body = encode_request(&header, &upload);
    (header, body)
}

/// A real query of the first table of `fixture` for the row of its first key.
pub fn fixture_query(fixture: &Fixture) -> Vec<u8> {
    let row = target_row(fixture, fixture.entries[0].key());
    query_body(fixture.product, fixture.session(), row).1
}

/// POST a query body to the host's primary product.
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

/// The row of `key` in the first table of `fixture`.
pub fn target_row(fixture: &Fixture, key: &[u8]) -> u32 {
    let table = &fixture.tables[0].table;
    tachyon_pir_protocol::placement::row(
        fixture.product,
        &genesis(),
        fixture.scope_id,
        table.seed,
        table.geometry.rows(),
        key,
    )
    .unwrap()
}

/// Look up `hits` distinct keys of each `(scope index, fixture)` and as many
/// absent keys, and require exactly the oracle's answer: every entry of the
/// key in ordinal order, or absent.
pub async fn check_answers(client: &Client, scopes: &[(u64, &Fixture)], hits: usize) {
    let map = client.fetch_map().await.unwrap();
    for (index, fixture) in scopes {
        let selector = ScopeSelector::Index(*index);
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for entry in &fixture.entries {
            if !keys.iter().any(|k| k == entry.key()) {
                keys.push(entry.key().to_vec());
            }
        }
        let step = (keys.len() / hits).max(1);
        for key in keys.iter().step_by(step).take(hits) {
            match client.lookup(&map, selector, key).await.unwrap() {
                LookupOutcome::Found { entries, .. } => {
                    assert_eq!(entries, fixture.expected(key))
                }
                other => panic!("hit answered {other:?}"),
            }
        }
        for i in 0..hits as u32 {
            let key = missing_key(fixture.product, i);
            match client.lookup(&map, selector, &key).await.unwrap() {
                LookupOutcome::Absent { .. } => {}
                other => panic!("miss answered {other:?}"),
            }
        }
    }
}

pub fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}
