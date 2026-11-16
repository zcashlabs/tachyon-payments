//! Round trips against a small in-test query host (axum 0.7) serving real P8
//! runtimes from `tachyon-pir-table`: a memo map with a sealed scope whose
//! one bucket has two overflow segments and an open scope, and a tachygram
//! map with one open scope. Expected answers come from the input entry
//! lists, never from the tables.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tachyon_pir_cli::{Client, ClientError, LookupOutcome, MapView, ScopeSelector};
use tachyon_pir_protocol::query::{encode_response, validate_request};
use tachyon_pir_protocol::{
    BlockHash, Bytes32, ChainMapChunk, ChainMapHead, Entry, ErrorBody, ErrorStatus, FrameError,
    Geometry, MemoEntry, PROFILE, PirParams, Product, RecoveryEpoch, RowError, Scope, ScopeState,
    SealedChunkRef, TachygramEntry, map_sha256,
};
use tachyon_pir_table::{PreparedTable, build_scope, prepare};

const ACTIVATION: u32 = 8;
const EPOCH: u32 = 16;
const MEMO_ANCHOR: u32 = ACTIVATION + EPOCH + 6;
const TG_ANCHOR: u32 = ACTIVATION + 12;
/// Occurrences of the repeated memo key: more than two rows' worth of slots
/// (56), so it overflows under every seed into segments 1 and 2.
const REPEATS: usize = 130;

fn genesis() -> BlockHash {
    BlockHash::from_protocol_bytes(Sha256::digest(b"genesis").into())
}

fn anchor_hash(height: u32) -> BlockHash {
    BlockHash::from_protocol_bytes(Sha256::digest(height.to_le_bytes()).into())
}

fn key16(label: &str, i: usize) -> [u8; 16] {
    let digest = Sha256::new()
        .chain_update(label)
        .chain_update(i.to_le_bytes())
        .finalize();
    digest[..16].try_into().unwrap()
}

fn memo(key: [u8; 16], ordinal: usize) -> Entry {
    let mut ciphertext = [0u8; 128];
    ciphertext[..8].copy_from_slice(&(ordinal as u64).to_le_bytes());
    ciphertext[8..24].copy_from_slice(&key);
    ciphertext[127] = 0xee;
    Entry::Memo(MemoEntry { key, ciphertext })
}

/// Input lists, in ordinal order, per scope.
struct Inputs {
    memo0: Vec<Entry>,
    memo1: Vec<Entry>,
    tachygram0: Vec<Entry>,
    repeated: [u8; 16],
}

fn inputs() -> Inputs {
    let repeated = key16("repeated", 0);
    let mut memo0 = Vec::new();
    let mut ordinal = 0;
    // The repeated key interleaved with 24 other keys, some of them twice.
    for i in 0..REPEATS {
        memo0.push(memo(repeated, ordinal));
        ordinal += 1;
        if i % 5 == 0 {
            memo0.push(memo(key16("memo0", i / 5), ordinal));
            ordinal += 1;
        }
        if i % 15 == 0 {
            memo0.push(memo(key16("memo0", i / 5), ordinal));
            ordinal += 1;
        }
    }
    let mut memo1: Vec<Entry> = (0..12).map(|i| memo(key16("memo1", i), i)).collect();
    // A key of scope 0 also used in scope 1.
    memo1.push(memo(key16("memo0", 3), 12));
    let tachygram0 = (0..40u32)
        .map(|i| {
            Entry::Tachygram(TachygramEntry {
                tachygram: Sha256::digest(format!("tg{i}")).into(),
                blk_height: ACTIVATION + i % (TG_ANCHOR - ACTIVATION + 1),
                stamp_ordinal: i / 3,
            })
        })
        .collect();
    Inputs {
        memo0,
        memo1,
        tachygram0,
        repeated,
    }
}

/// One product's published state: map heads of two generations (the second
/// is served after a 409), chunks, and prepared tables by session id.
struct Published {
    product: Product,
    heads: [(Vec<u8>, Bytes32); 2],
    chunks: HashMap<(u64, Bytes32), Vec<u8>>,
    tables: HashMap<Bytes32, PreparedTable>,
    params: PirParams,
}

struct Fixture {
    inputs: Inputs,
    memo: Published,
    tachygram: Published,
}

fn build_tables(
    product: Product,
    scope_id: u64,
    entries: &[Entry],
    tables: &mut HashMap<Bytes32, PreparedTable>,
) -> (u32, Vec<tachyon_pir_protocol::Table>) {
    let built = build_scope(product, &genesis(), scope_id, entries).unwrap();
    let mut listed = Vec::new();
    for table in &built.tables {
        let prepared = prepare(product, scope_id, table).unwrap();
        listed.push(prepared.table.clone());
        tables.insert(prepared.table.session_id, prepared);
    }
    (built.n_buckets, listed)
}

fn chain_scope(
    scope_id: u64,
    state: ScopeState,
    last: u32,
    entries: &[Entry],
    built: (u32, Vec<tachyon_pir_protocol::Table>),
) -> Scope {
    Scope {
        scope_id,
        state,
        first_height: ACTIVATION + scope_id as u32 * EPOCH,
        last_height: last,
        entries: entries.len() as u64,
        n_buckets: built.0,
        tables: built.1,
    }
}

fn heads(head: ChainMapHead) -> [(Vec<u8>, Bytes32); 2] {
    let encode = |generation| {
        let bytes = ChainMapHead {
            generation,
            ..head.clone()
        }
        .to_bytes()
        .unwrap();
        let sha = map_sha256(&bytes);
        (bytes, sha)
    };
    [encode(1), encode(2)]
}

fn base_head(product: Product, anchor: u32) -> ChainMapHead {
    ChainMapHead {
        protocol: product.protocol().into(),
        profile: PROFILE.into(),
        network: "test".into(),
        genesis_hash: genesis(),
        activation_height: ACTIVATION,
        epoch_length: EPOCH,
        generation: 1,
        recovery_epoch: RecoveryEpoch::new(1).unwrap(),
        anchor_height: anchor,
        anchor_hash: anchor_hash(anchor),
        floor_scope: 0,
        sealed_chunks: Vec::new(),
        open_scopes: Vec::new(),
    }
}

static FIXTURE: LazyLock<Fixture> = LazyLock::new(|| {
    let inputs = inputs();

    let product = Product::Memo;
    let mut tables = HashMap::new();
    let built0 = build_tables(product, 0, &inputs.memo0, &mut tables);
    let scope0 = chain_scope(
        0,
        ScopeState::Sealed,
        ACTIVATION + EPOCH - 1,
        &inputs.memo0,
        built0,
    );
    let built1 = build_tables(product, 1, &inputs.memo1, &mut tables);
    let scope1 = chain_scope(1, ScopeState::Open, MEMO_ANCHOR, &inputs.memo1, built1);
    let chunk = ChainMapChunk {
        protocol: product.protocol().into(),
        base: 0,
        scopes: vec![scope0],
    };
    let chunk_bytes = chunk.to_bytes();
    let chunk_sha = map_sha256(&chunk_bytes);
    let mut head = base_head(product, MEMO_ANCHOR);
    head.sealed_chunks = vec![SealedChunkRef {
        base: 0,
        sha256: chunk_sha,
    }];
    head.open_scopes = vec![scope1];
    let memo = Published {
        product,
        heads: heads(head),
        chunks: HashMap::from([((0, chunk_sha), chunk_bytes)]),
        tables,
        params: PirParams::new(product, Geometry::Tachyon4k8),
    };

    let product = Product::Tachygram;
    let mut tables = HashMap::new();
    let built = build_tables(product, 0, &inputs.tachygram0, &mut tables);
    let scope = chain_scope(0, ScopeState::Open, TG_ANCHOR, &inputs.tachygram0, built);
    let mut head = base_head(product, TG_ANCHOR);
    head.open_scopes = vec![scope];
    let tachygram = Published {
        product,
        heads: heads(head),
        chunks: HashMap::new(),
        tables,
        params: PirParams::new(product, Geometry::Tachyon4k8),
    };
    Fixture {
        inputs,
        memo,
        tachygram,
    }
});

/// A fault the server injects into its next query answers, in order.
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// Answer with this spec 10.5 status. 409 also moves the server to the
    /// second map generation.
    Status(u16),
    /// Flip every bit of one byte of the response at this offset.
    Flip(usize),
    /// Drop the last byte.
    Truncate,
    /// Append a byte.
    Extend,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicFault {
    Flip,
    Truncate,
}

#[derive(Default)]
struct Counters {
    map_heads: AtomicU64,
    chunks: AtomicU64,
    publics: AtomicU64,
    queries: AtomicU64,
}

struct Server {
    fixture: &'static Fixture,
    generation: AtomicUsize,
    query_faults: Mutex<VecDeque<Fault>>,
    public_fault: Mutex<Option<PublicFault>>,
    /// Node JSON-RPC: block hashes by height, display order.
    blocks: Mutex<HashMap<u64, String>>,
    counters: Counters,
    /// `(session_id, body length)` of every query received.
    log: Mutex<Vec<(Bytes32, usize)>>,
}

impl Server {
    fn published(&self, product: &str) -> Option<&'static Published> {
        match product {
            "memo" => Some(&self.fixture.memo),
            "tachygram" => Some(&self.fixture.tachygram),
            _ => None,
        }
    }

    fn head(&self, published: &'static Published) -> &'static (Vec<u8>, Bytes32) {
        &published.heads[self.generation.load(Ordering::SeqCst)]
    }
}

fn error(status: ErrorStatus, body: ErrorBody) -> Response {
    let mut response = (
        StatusCode::from_u16(status.http_status()).unwrap(),
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_vec(&body).unwrap(),
    )
        .into_response();
    if let Some(seconds) = status.retry_after_seconds() {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
    }
    response
}

fn status_response(server: &Server, published: &'static Published, status: u16) -> Response {
    let status = ErrorStatus::from_http_status(status).unwrap();
    if status == ErrorStatus::StaleRouting {
        server.generation.store(1, Ordering::SeqCst);
        let current = server.head(published).1;
        return error(status, ErrorBody::stale_routing("refetch the map", current));
    }
    error(status, ErrorBody::new(status, "injected"))
}

fn octets(bytes: Vec<u8>) -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        bytes,
    )
        .into_response()
}

async fn map_head(State(server): State<Arc<Server>>, Path(product): Path<String>) -> Response {
    let Some(published) = server.published(&product) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    server.counters.map_heads.fetch_add(1, Ordering::SeqCst);
    let (bytes, sha) = server.head(published);
    let mut response = (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        bytes.clone(),
    )
        .into_response();
    response
        .headers_mut()
        .insert("x-map-sha256", HeaderValue::from_str(&sha.hex()).unwrap());
    response
}

async fn map_chunk(
    State(server): State<Arc<Server>>,
    Path((product, base, sha)): Path<(String, u64, String)>,
) -> Response {
    let Some(published) = server.published(&product) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    server.counters.chunks.fetch_add(1, Ordering::SeqCst);
    let found = sha
        .parse::<Bytes32>()
        .ok()
        .and_then(|sha| published.chunks.get(&(base, sha)));
    match found {
        Some(bytes) => octets(bytes.clone()),
        None => status_response(&server, published, 409),
    }
}

async fn public(
    State(server): State<Arc<Server>>,
    Path((product, session)): Path<(String, String)>,
) -> Response {
    let Some(published) = server.published(&product) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    server.counters.publics.fetch_add(1, Ordering::SeqCst);
    let Some(table) = session
        .parse::<Bytes32>()
        .ok()
        .and_then(|s| published.tables.get(&s))
    else {
        return status_response(&server, published, 409);
    };
    let mut bytes = table.public.clone();
    match *server.public_fault.lock().unwrap() {
        Some(PublicFault::Flip) => bytes[100] ^= 0x01,
        Some(PublicFault::Truncate) => {
            bytes.pop();
        }
        None => {}
    }
    octets(bytes)
}

async fn query(
    State(server): State<Arc<Server>>,
    Path(product): Path<String>,
    body: Bytes,
) -> Response {
    let Some(published) = server.published(&product) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    server.counters.queries.fetch_add(1, Ordering::SeqCst);
    let (header, payload) = match validate_request(published.product, Geometry::Tachyon4k8, &body) {
        Ok(parsed) => parsed,
        Err(e) => return error(e.status(), ErrorBody::new(e.status(), e.to_string())),
    };
    let session = Bytes32(header.session_id);
    server.lock_log().push((session, body.len()));
    let fault = server.query_faults.lock().unwrap().pop_front();
    if let Some(Fault::Status(status)) = fault {
        return status_response(&server, published, status);
    }
    let Some(table) = published.tables.get(&session) else {
        return status_response(&server, published, 409);
    };
    let payload = payload.to_vec();
    let reply = tokio::task::spawn_blocking(move || {
        table.runtime.answer(&published.params, &payload).unwrap()
    })
    .await
    .unwrap();
    let mut response = encode_response(&header, &reply);
    match fault {
        Some(Fault::Flip(at)) => response[at] ^= 0xff,
        Some(Fault::Truncate) => {
            response.pop();
        }
        Some(Fault::Extend) => response.push(0),
        Some(Fault::Status(_)) | None => {}
    }
    octets(response)
}

async fn rpc(State(server): State<Arc<Server>>, body: Bytes) -> Response {
    let request: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(request["method"], "getblockhash");
    let height = request["params"][0].as_u64().unwrap();
    let reply = match server.blocks.lock().unwrap().get(&height) {
        Some(hash) => json!({"result": hash, "error": null, "id": request["id"]}),
        None => {
            json!({"result": null, "error": {"code": -8, "message": "Block height out of range"}, "id": request["id"]})
        }
    };
    axum::Json(reply).into_response()
}

impl Server {
    fn lock_log(&self) -> std::sync::MutexGuard<'_, Vec<(Bytes32, usize)>> {
        self.log.lock().unwrap()
    }
}

/// A running in-test host and a client for `product` pointed at it.
struct Harness {
    server: Arc<Server>,
    origin: String,
}

impl Harness {
    async fn start() -> Harness {
        let fixture: &'static Fixture = &FIXTURE;
        let blocks = HashMap::from([
            (0, genesis().display_hex()),
            (MEMO_ANCHOR as u64, anchor_hash(MEMO_ANCHOR).display_hex()),
            (TG_ANCHOR as u64, anchor_hash(TG_ANCHOR).display_hex()),
        ]);
        let server = Arc::new(Server {
            fixture,
            generation: AtomicUsize::new(0),
            query_faults: Mutex::new(VecDeque::new()),
            public_fault: Mutex::new(None),
            blocks: Mutex::new(blocks),
            counters: Counters::default(),
            log: Mutex::new(Vec::new()),
        });
        let router = Router::new()
            .route("/:product/v1/map", get(map_head))
            .route("/:product/v1/map/:base/:sha", get(map_chunk))
            .route("/:product/v1/public/:session", get(public))
            .route("/:product/v1/query", post(query))
            .route("/rpc", post(rpc))
            .with_state(server.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Harness { server, origin }
    }

    fn client(&self, product: Product) -> Client {
        Client::new(&self.origin, product, None).unwrap()
    }

    fn fault(&self, faults: &[Fault]) {
        self.server
            .query_faults
            .lock()
            .unwrap()
            .extend(faults.iter().copied());
    }

    fn served(&self) -> (u64, u64, u64, u64) {
        let c = &self.server.counters;
        (
            c.map_heads.load(Ordering::SeqCst),
            c.chunks.load(Ordering::SeqCst),
            c.publics.load(Ordering::SeqCst),
            c.queries.load(Ordering::SeqCst),
        )
    }
}

/// The oracle: every input entry with `key`, in input (ordinal) order.
fn expected<'a>(inputs: &'a [Entry], key: &[u8]) -> Vec<&'a Entry> {
    inputs.iter().filter(|e| e.key() == key).collect()
}

fn distinct_keys(inputs: &[Entry]) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for entry in inputs {
        if !keys.iter().any(|k| k == entry.key()) {
            keys.push(entry.key().to_vec());
        }
    }
    keys
}

async fn check_every_key(
    client: &Client,
    map: &MapView,
    scope: u64,
    inputs: &[Entry],
    misses: usize,
) {
    for key in distinct_keys(inputs) {
        let outcome = client
            .lookup(map, ScopeSelector::Index(scope), &key)
            .await
            .unwrap();
        let LookupOutcome::Found { entries, answer } = &outcome else {
            panic!("key {} not found: {outcome:?}", hex::encode(&key));
        };
        let want = expected(inputs, &key);
        assert_eq!(entries.iter().collect::<Vec<_>>(), want, "scope {scope}");
        assert_eq!(answer.scope_index, scope);
        assert!(!answer.refetched_map);
    }
    for i in 0..misses {
        let key = &Sha256::digest(format!("miss{scope}-{i}"))[..client.product().key_len()];
        assert!(expected(inputs, key).is_empty());
        let outcome = client
            .lookup(map, ScopeSelector::Index(scope), key)
            .await
            .unwrap();
        assert!(
            matches!(outcome, LookupOutcome::Absent { .. }),
            "{outcome:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memo_hits_and_misses_match_the_input_lists() {
    let harness = Harness::start().await;
    let client = harness.client(Product::Memo);
    let map = client.fetch_map().await.unwrap();
    assert_eq!(map.scopes().len(), 2);
    let fixture: &Fixture = &FIXTURE;
    check_every_key(&client, &map, 0, &fixture.inputs.memo0, 4).await;
    check_every_key(&client, &map, 1, &fixture.inputs.memo1, 4).await;
    // One public setup per table, fetched once; one chunk fetch; one map.
    let tables = map
        .scopes()
        .iter()
        .map(|s| s.tables().tables().len() as u64)
        .sum::<u64>();
    let (heads, chunks, publics, queries) = harness.served();
    assert_eq!((heads, chunks, publics), (1, 1, tables));
    assert_eq!(client.stats().queries, queries);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_key_spans_three_segments_in_ordinal_order() {
    let harness = Harness::start().await;
    let client = harness.client(Product::Memo);
    let map = client.fetch_map().await.unwrap();
    let (_, scope) = map.scope(ScopeSelector::Index(0)).unwrap();
    let tables = scope.tables();
    assert_eq!(tables.n_buckets(), 1);
    assert_eq!(tables.segments(0).len(), 3, "two overflow segments");

    let fixture: &Fixture = &FIXTURE;
    let key = fixture.inputs.repeated;
    let outcome = client
        .lookup(&map, ScopeSelector::Index(0), &key)
        .await
        .unwrap();
    let LookupOutcome::Found { entries, answer } = outcome else {
        panic!("{outcome:?}");
    };
    let want = expected(&fixture.inputs.memo0, &key);
    assert_eq!(want.len(), REPEATS);
    assert_eq!(entries.iter().collect::<Vec<_>>(), want);
    // Ordinals (stored in the ciphertext) strictly increase across segments.
    let ordinals: Vec<u64> = entries
        .iter()
        .map(|e| match e {
            Entry::Memo(m) => u64::from_le_bytes(m.ciphertext[..8].try_into().unwrap()),
            other => panic!("{other:?}"),
        })
        .collect();
    assert!(ordinals.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(
        answer.queries.iter().map(|q| q.segment).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(answer.queries.iter().all(|q| q.matches > 0));
    assert_eq!(
        answer.queries.iter().map(|q| q.matches).sum::<usize>(),
        REPEATS
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tachygram_hits_and_misses_match_the_input_list() {
    let harness = Harness::start().await;
    let client = harness.client(Product::Tachygram);
    let map = client.fetch_map().await.unwrap();
    let fixture: &Fixture = &FIXTURE;
    check_every_key(&client, &map, 0, &fixture.inputs.tachygram0, 6).await;
    // The anchor matches the node; another anchor hash does not.
    let rpc = format!("{}/rpc", harness.origin);
    let accepted = client.accept_anchor(&map, &rpc).await.unwrap();
    assert_eq!(accepted.anchor_height, TG_ANCHOR);
    harness
        .server
        .blocks
        .lock()
        .unwrap()
        .insert(TG_ANCHOR as u64, "00".repeat(32));
    assert!(matches!(
        client.accept_anchor(&map, &rpc).await,
        Err(ClientError::AnchorMismatch { what: "anchor", .. })
    ));
    harness.server.blocks.lock().unwrap().remove(&0);
    assert!(matches!(
        client.accept_anchor(&map, &rpc).await,
        Err(ClientError::Rpc(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tampered_and_misframed_responses_are_refused() {
    let harness = Harness::start().await;
    let client = harness.client(Product::Memo);
    let map = client.fetch_map().await.unwrap();
    let key = key16("memo1", 0);
    let scope = ScopeSelector::Index(1);
    let response_len = Geometry::Tachyon4k8.response_len();

    // A reply byte in the row tail: the decoded row is not canonical.
    harness.fault(&[Fault::Flip(response_len - 3)]);
    match client.lookup(&map, scope, &key).await {
        Err(ClientError::Row {
            source: RowError::NonzeroTail,
            ..
        }) => {}
        other => panic!("{other:?}"),
    }
    // A reply byte in the empty slots after the row's entries.
    harness.fault(&[Fault::Flip(response_len - 1000)]);
    assert!(matches!(
        client.lookup(&map, scope, &key).await,
        Err(ClientError::Row { .. })
    ));
    // The echoed header.
    harness.fault(&[Fault::Flip(40)]);
    assert!(matches!(
        client.lookup(&map, scope, &key).await,
        Err(ClientError::Frame(FrameError::HeaderMismatch))
    ));
    // Wrong lengths.
    for fault in [Fault::Truncate, Fault::Extend] {
        harness.fault(&[fault]);
        assert!(matches!(
            client.lookup(&map, scope, &key).await,
            Err(ClientError::Frame(FrameError::Length { expected, .. })) if expected == response_len
        ));
    }
    // Nothing was retried, and an untampered lookup still succeeds.
    let (heads, _, _, queries) = harness.served();
    assert_eq!((heads, queries), (1, 5));
    assert!(matches!(
        client.lookup(&map, scope, &key).await.unwrap(),
        LookupOutcome::Found { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_routing_refetches_the_map_and_retries_once() {
    let harness = Harness::start().await;
    let client = harness.client(Product::Memo);
    let map = client.fetch_map().await.unwrap();
    let first_sha = map.map_sha256();
    let key = key16("memo1", 1);
    let scope = ScopeSelector::Index(1);

    harness.fault(&[Fault::Status(409)]);
    let outcome = client.lookup(&map, scope, &key).await.unwrap();
    let LookupOutcome::Found { answer, .. } = &outcome else {
        panic!("{outcome:?}");
    };
    assert!(answer.refetched_map);
    assert_eq!(answer.generation, 2);
    assert_ne!(answer.map_sha256, first_sha);
    assert_eq!(
        client.current_map().unwrap().map_sha256(),
        answer.map_sha256
    );
    // Exactly one map refetch (the chunk is cached by digest) and one retry.
    assert_eq!(harness.served(), (2, 1, 1, 2));

    // A second 409 on the retry is returned, not retried again.
    harness.fault(&[Fault::Status(409), Fault::Status(409)]);
    assert!(matches!(
        client.lookup(&map, scope, &key).await,
        Err(ClientError::StaleRouting { .. })
    ));
    assert_eq!(harness.served(), (3, 1, 1, 4));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoked_overloaded_and_unavailable_are_typed() {
    let harness = Harness::start().await;
    let client = harness.client(Product::Memo);
    let map = client.fetch_map().await.unwrap();
    let key = key16("memo1", 2);
    let scope = ScopeSelector::Index(1);

    harness.fault(&[Fault::Status(410)]);
    assert!(matches!(
        client.lookup(&map, scope, &key).await,
        Err(ClientError::SessionRevoked { .. })
    ));
    harness.fault(&[Fault::Status(429)]);
    match client.lookup(&map, scope, &key).await {
        Err(ClientError::Overloaded { retry_after, .. }) => {
            assert_eq!(retry_after, Some(Duration::from_secs(1)));
        }
        other => panic!("{other:?}"),
    }
    harness.fault(&[Fault::Status(503)]);
    assert!(matches!(
        client.lookup(&map, scope, &key).await,
        Err(ClientError::Unavailable { .. })
    ));
    harness.fault(&[Fault::Status(400)]);
    assert!(matches!(
        client.lookup(&map, scope, &key).await,
        Err(ClientError::InvalidRequest { status: 400, .. })
    ));
    harness.fault(&[Fault::Status(413)]);
    assert!(matches!(
        client.lookup(&map, scope, &key).await,
        Err(ClientError::InvalidRequest { status: 413, .. })
    ));
    // None of them refetched the map or retried.
    let (heads, _, _, queries) = harness.served();
    assert_eq!((heads, queries), (1, 5));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_setup_mismatch_is_refused() {
    let harness = Harness::start().await;
    let client = harness.client(Product::Tachygram);
    let map = client.fetch_map().await.unwrap();
    let fixture: &Fixture = &FIXTURE;
    let key = fixture.inputs.tachygram0[0].key().to_vec();
    let scope = ScopeSelector::Index(0);

    *harness.server.public_fault.lock().unwrap() = Some(PublicFault::Flip);
    assert!(matches!(
        client.lookup(&map, scope, &key).await,
        Err(ClientError::PublicDigest { .. })
    ));
    *harness.server.public_fault.lock().unwrap() = Some(PublicFault::Truncate);
    match client.lookup(&map, scope, &key).await {
        Err(ClientError::PublicLength {
            expected, actual, ..
        }) => assert_eq!((expected, actual), (45_056, 45_055)),
        other => panic!("{other:?}"),
    }
    // No query was sent with an unverified setup.
    assert_eq!(harness.served().3, 0);
    *harness.server.public_fault.lock().unwrap() = None;
    assert!(matches!(
        client.lookup(&map, scope, &key).await.unwrap(),
        LookupOutcome::Found { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dummy_query_sends_what_a_lookup_sends() {
    let harness = Harness::start().await;
    let client = harness.client(Product::Memo);
    let map = client.fetch_map().await.unwrap();
    let fixture: &Fixture = &FIXTURE;
    let scope = ScopeSelector::Index(0);

    client
        .lookup(&map, scope, &fixture.inputs.repeated)
        .await
        .unwrap();
    let real = std::mem::take(&mut *harness.server.lock_log());
    let answer = client.dummy_query(&map, scope).await.unwrap();
    let dummy = std::mem::take(&mut *harness.server.lock_log());
    // Scope 0 has one bucket, so the same sessions in the same order, with
    // bodies of the same length.
    assert_eq!(real.len(), 3);
    assert_eq!(dummy, real);
    assert_eq!(answer.queries.len(), 3);
    assert!(answer.queries.iter().all(|q| q.matches == 0));
    assert!(answer.queries.iter().all(|q| q.row < 4096));
    assert_eq!(dummy[0].1, Geometry::Tachyon4k8.request_len());
}

async fn cli(harness: &Harness, args: &[&str]) -> (bool, Value) {
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_tachyon-pir-cli"))
        .arg("--origin")
        .arg(&harness.origin)
        .args(args)
        .output()
        .await
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("{e}: {stdout:?} {:?}", output.stderr));
    (output.status.success(), value)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_prints_json_results() {
    let harness = Harness::start().await;
    let fixture: &Fixture = &FIXTURE;
    let rpc = format!("{}/rpc", harness.origin);

    let (ok, map) = cli(&harness, &["--product", "memo", "--node", &rpc, "map"]).await;
    assert!(ok, "{map}");
    assert_eq!(map["product"], "memo");
    assert_eq!(map["scopes"].as_array().unwrap().len(), 2);
    assert_eq!(map["scopes"][0]["tables"].as_array().unwrap().len(), 3);
    assert_eq!(map["anchor"]["anchor_height"], MEMO_ANCHOR);
    assert_eq!(map["requests"]["map_chunks"], 1);

    let tag = hex::encode(fixture.inputs.tachygram0[5].key());
    let (ok, found) = cli(
        &harness,
        &[
            "--product",
            "tachygram",
            "lookup",
            "--scope",
            "0",
            "--key",
            &tag,
        ],
    )
    .await;
    assert!(ok, "{found}");
    assert_eq!(found["result"], "found");
    let Entry::Tachygram(entry) = &fixture.inputs.tachygram0[5] else {
        panic!("tachygram inputs")
    };
    assert_eq!(found["entries"][0]["tachygram"], tag);
    assert_eq!(found["entries"][0]["blk_height"], entry.blk_height);
    assert_eq!(found["entries"][0]["stamp_ordinal"], entry.stamp_ordinal);
    assert_eq!(found["answer"]["coverage"]["last_height"], TG_ANCHOR);

    let (ok, absent) = cli(
        &harness,
        &[
            "--product",
            "memo",
            "lookup",
            "--scope-id",
            "1",
            "--key",
            &"42".repeat(32),
        ],
    )
    .await;
    assert!(ok, "{absent}");
    assert_eq!(absent["result"], "absent");
    assert_eq!(absent["answer"]["queries"].as_array().unwrap().len(), 1);

    let (ok, dummy) = cli(&harness, &["--product", "memo", "dummy", "--scope", "0"]).await;
    assert!(ok, "{dummy}");
    assert_eq!(dummy["result"], "dummy");
    assert_eq!(dummy["requests"]["queries"], 3);

    harness.fault(&[Fault::Status(429)]);
    let (ok, error) = cli(&harness, &["--product", "memo", "dummy", "--scope", "1"]).await;
    assert!(!ok);
    assert_eq!(error["error"]["kind"], "overloaded");
    assert_eq!(error["error"]["retry_after_seconds"], 1);
}
