//! The probe client: map fetch and validation, private lookups, dummy
//! queries and the anchor check, over one product's mount.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Poll;
use std::time::Duration;

use rand::{Rng, RngCore};
use reqwest::header::{CONTENT_TYPE, HeaderMap, RETRY_AFTER};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tachyon_pir_protocol::query::{encode_request, validate_response};
use tachyon_pir_protocol::{
    BlockHash, Bytes32, Entry, Geometry, HEADER_BYTES, MAX_MAP_HEAD_BYTES, PirParams, Product,
    QueryHeader, QueryTarget, RowContext, Table, lookup, validate_row,
};
use zakura_pir_client::native::{P8, ProfileClientExt as _};

use crate::error::{ClientError, MAX_ERROR_BODY_BYTES, Route, status_error};
use crate::map_view::{
    Coverage, MapChunkScopes, MapHead, MapView, ScopeSelector, ViewScope, parse_floor_header,
};

/// The largest map chunk accepted: 32 scopes of up to 512 tables of about
/// 330 bytes is 5.4 MB; the bound leaves room.
const MAX_MAP_CHUNK_BYTES: usize = 16 << 20;
/// Request timeout for every route.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Which of a product's two mounts (spec 10.1) the client uses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mount {
    /// `/<product>/v1`.
    #[default]
    Canonical,
    /// `/v1/<product>`.
    Legacy,
}

/// Requests sent, by route.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Stats {
    pub map_heads: u64,
    pub map_chunks: u64,
    pub publics: u64,
    pub queries: u64,
}

#[derive(Default)]
struct Counters {
    map_heads: AtomicU64,
    map_chunks: AtomicU64,
    publics: AtomicU64,
    queries: AtomicU64,
}

/// One table queried by a lookup or dummy query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct QueriedTable {
    pub bucket: u32,
    pub segment: u32,
    pub row: u32,
    pub session_id: Bytes32,
    /// Entries in the decoded row.
    pub row_entries: usize,
    /// Of those, entries with the looked-up key (zero for a dummy).
    pub matches: usize,
}

/// What a lookup or dummy query was answered under.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Answer {
    pub scope_index: u64,
    pub scope_id: u64,
    pub coverage: Coverage,
    pub map_sha256: Bytes32,
    pub generation: u64,
    /// The bucket queried; `None` for a scope with no tables.
    pub bucket: Option<u32>,
    /// Every table queried, in segment order.
    pub queries: Vec<QueriedTable>,
    /// Whether a 409 made this call refetch the map and retry.
    pub refetched_map: bool,
}

/// The result of a lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LookupOutcome {
    /// Every entry with the key, across the bucket's segments in segment
    /// order, which is ordinal order (spec 6.4).
    Found { entries: Vec<Entry>, answer: Answer },
    /// No entry with the key in canonical rows. Meaningful only for the
    /// scope's coverage (spec 8.4).
    Absent { answer: Answer },
    /// The scope is below this provider's floor; nothing was sent (spec 11.5).
    BelowFloor { scope_index: u64, floor_scope: u64 },
}

impl LookupOutcome {
    pub fn answer(&self) -> Option<&Answer> {
        match self {
            LookupOutcome::Found { answer, .. } | LookupOutcome::Absent { answer } => Some(answer),
            LookupOutcome::BelowFloor { .. } => None,
        }
    }

    fn mark_refetched(&mut self) {
        if let LookupOutcome::Found { answer, .. } | LookupOutcome::Absent { answer } = self {
            answer.refetched_map = true;
        }
    }
}

/// The anchor a map was accepted at (spec 12.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AnchorAccepted {
    pub anchor_height: u32,
    pub anchor_hash: String,
    pub genesis_hash: String,
}

/// PIR parameters are a pure function of product and geometry, shared by
/// every client in the process.
fn pir_params(product: Product, geometry: Geometry) -> Arc<PirParams> {
    type Cache = Mutex<HashMap<(Product, Geometry), Arc<PirParams>>>;
    static CACHE: OnceLock<Cache> = OnceLock::new();
    let cache = CACHE.get_or_init(Cache::default);
    if let Some(params) = cache
        .lock()
        .expect("params cache")
        .get(&(product, geometry))
    {
        return params.clone();
    }
    let params = Arc::new(PirParams::new(product, geometry));
    cache
        .lock()
        .expect("params cache")
        .entry((product, geometry))
        .or_insert(params)
        .clone()
}

/// The 52-byte header and request body of one query (spec 10.4):
/// `magic || session_id || nonce || payload`.
pub fn query_body(
    product: Product,
    session_id: Bytes32,
    nonce: [u8; 16],
    payload: &[u8],
) -> (QueryHeader, Vec<u8>) {
    let header = QueryHeader::new(product, session_id.0, nonce);
    let body = encode_request(&header, payload);
    (header, body)
}

/// Accepts a lookup key in hex: the stored key (`key16`, or the full
/// tachygram), or for the 16-byte products also the full 32-byte tag or
/// digest, whose first 16 bytes are the key (spec 6.3).
pub fn parse_key(product: Product, text: &str) -> Result<Vec<u8>, ClientError> {
    let bytes = hex::decode(text.trim()).map_err(|_| ClientError::KeyLength {
        expected: product.key_len(),
        actual: 0,
    })?;
    match bytes.len() {
        n if n == product.key_len() => Ok(bytes),
        32 => Ok(bytes[..product.key_len()].to_vec()),
        n => Err(ClientError::KeyLength {
            expected: product.key_len(),
            actual: n,
        }),
    }
}

/// A diagnostic client for one product of one query host.
pub struct Client {
    http: reqwest::Client,
    rpc: reqwest::Client,
    base: String,
    product: Product,
    /// The chain (network, genesis) of the first map accepted.
    chain: Mutex<Option<(String, BlockHash)>>,
    /// Parsed sealed chunks by digest; immutable.
    chunks: Mutex<HashMap<Bytes32, Arc<MapChunkScopes>>>,
    /// Public setup bytes by session id; the session id commits to their
    /// digest, so an entry is valid for every map naming the session.
    publics: Mutex<HashMap<Bytes32, Arc<Vec<u8>>>>,
    current: Mutex<Option<Arc<MapView>>>,
    counters: Counters,
}

impl Client {
    /// A client for `product` at `origin` (`http(s)://host[:port][/prefix]`).
    /// With `ca_pem`, TLS trusts that certificate only.
    pub fn new(origin: &str, product: Product, ca_pem: Option<&[u8]>) -> Result<Self, ClientError> {
        Self::with_mount(origin, product, ca_pem, Mount::Canonical)
    }

    /// As [`Client::new`], on the chosen mount.
    pub fn with_mount(
        origin: &str,
        product: Product,
        ca_pem: Option<&[u8]>,
        mount: Mount,
    ) -> Result<Self, ClientError> {
        let url =
            reqwest::Url::parse(origin).map_err(|_| ClientError::Origin(origin.to_owned()))?;
        if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
            return Err(ClientError::Origin(origin.to_owned()));
        }
        let prefix = match mount {
            Mount::Canonical => product.mount(),
            Mount::Legacy => product.legacy_mount(),
        };
        let base = format!("{}{prefix}", origin.trim_end_matches('/'));
        let mut builder = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(Duration::from_secs(10));
        if let Some(pem) = ca_pem {
            let certificates = reqwest::Certificate::from_pem_bundle(pem)
                .map_err(|e| ClientError::Certificate(e.to_string()))?;
            if certificates.is_empty() {
                return Err(ClientError::Certificate(
                    "no PEM certificate in the CA file".into(),
                ));
            }
            builder = builder.tls_built_in_root_certs(false);
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        let http = builder
            .build()
            .map_err(|e| ClientError::Certificate(e.to_string()))?;
        let rpc = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| ClientError::Certificate(e.to_string()))?;
        Ok(Self {
            http,
            rpc,
            base,
            product,
            chain: Mutex::new(None),
            chunks: Mutex::new(HashMap::new()),
            publics: Mutex::new(HashMap::new()),
            current: Mutex::new(None),
            counters: Counters::default(),
        })
    }

    pub fn product(&self) -> Product {
        self.product
    }

    /// `<origin><mount>`.
    pub fn base_url(&self) -> &str {
        &self.base
    }

    /// Requests sent so far.
    pub fn stats(&self) -> Stats {
        Stats {
            map_heads: self.counters.map_heads.load(Ordering::Relaxed),
            map_chunks: self.counters.map_chunks.load(Ordering::Relaxed),
            publics: self.counters.publics.load(Ordering::Relaxed),
            queries: self.counters.queries.load(Ordering::Relaxed),
        }
    }

    /// The map most recently fetched, including one a 409 retry fetched.
    pub fn current_map(&self) -> Option<Arc<MapView>> {
        self.current.lock().expect("current map").clone()
    }

    /// Fetches `GET map` and every sealed chunk, and validates the listing.
    /// The host's `X-Floor-Scope` header is required; the view's floor is the
    /// larger of it and the head's `floor_scope` (spec 11.5).
    /// Chunks are fetched only when not cached by digest. Sends no retries.
    pub async fn fetch_map(&self) -> Result<Arc<MapView>, ClientError> {
        self.counters.map_heads.fetch_add(1, Ordering::Relaxed);
        let response = self.get(Route::Map, &format!("{}/map", self.base)).await?;
        let header = response
            .headers()
            .get("x-map-sha256")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let floor = response
            .headers()
            .get("x-floor-scope")
            .map(|v| v.to_str().unwrap_or("\u{fffd}").to_owned());
        let body = read_ok(Route::Map, response, MAX_MAP_HEAD_BYTES).await?;
        let (head, sha256) = MapHead::from_response(self.product, header.as_deref(), &body)?;
        let host_floor = parse_floor_header(floor.as_deref())?;
        self.pin_chain(&head)?;

        let refs = head.sealed_chunks().to_vec();
        let mut chunks = Vec::with_capacity(refs.len());
        for (position, reference) in refs.iter().enumerate() {
            let cached = self
                .chunks
                .lock()
                .expect("chunk cache")
                .get(&reference.sha256)
                .cloned();
            let chunk = match cached {
                Some(chunk) => {
                    head.check_chunk(self.product, position, &chunk)?;
                    chunk
                }
                None => {
                    self.counters.map_chunks.fetch_add(1, Ordering::Relaxed);
                    let url = format!("{}/map/{}/{}", self.base, reference.base, reference.sha256);
                    let response = self.get(Route::MapChunk, &url).await?;
                    let bytes = read_ok(Route::MapChunk, response, MAX_MAP_CHUNK_BYTES).await?;
                    Arc::new(head.parse_chunk(self.product, position, &bytes)?)
                }
            };
            chunks.push(chunk);
        }
        let view = MapView::build(
            self.product,
            sha256,
            head,
            chunks.iter().map(|c| (**c).clone()).collect(),
            host_floor,
        )?;
        // Keep only the chunks this map names.
        let named: HashSet<Bytes32> = refs.iter().map(|r| r.sha256).collect();
        let mut cache = self.chunks.lock().expect("chunk cache");
        cache.retain(|digest, _| named.contains(digest));
        for (reference, chunk) in refs.iter().zip(chunks) {
            cache.insert(reference.sha256, chunk);
        }
        drop(cache);
        let view = Arc::new(view);
        *self.current.lock().expect("current map") = Some(view.clone());
        Ok(view)
    }

    /// A map naming another chain than the first one accepted is refused.
    fn pin_chain(&self, head: &MapHead) -> Result<(), ClientError> {
        let chain = (head.network().to_owned(), *head.genesis_hash());
        let mut pinned = self.chain.lock().expect("chain");
        match &*pinned {
            Some(known) if *known != chain => Err(ClientError::ChainChanged {
                network: chain.0,
                genesis: chain.1.display_hex(),
            }),
            _ => {
                *pinned = Some(chain);
                Ok(())
            }
        }
    }

    /// Looks up `key` (the stored key: `key16`, or the full tachygram) in a
    /// scope: a batch of one ([`Client::lookup_many`]), its queries sent one
    /// at a time in segment order. A 409 refetches the map and retries once;
    /// nothing else is retried.
    pub async fn lookup(
        &self,
        map: &MapView,
        scope: ScopeSelector,
        key: &[u8],
    ) -> Result<LookupOutcome, ClientError> {
        let mut outcomes = self.lookup_many(map, scope, &[key], 1).await?;
        Ok(outcomes.pop().expect("one outcome per key"))
    }

    /// Looks up every key of `keys` in one scope, answering each exactly as
    /// [`Client::lookup`] would, while querying each row once: the query plan
    /// of every key is computed, each distinct (table, row) of the plans is
    /// queried privately once, at most `parallel` at a time, and each key's
    /// outcome is built from the decoded rows of its own plan (filter, per
    /// segment [`QueriedTable`], tachygram duplicate check, found or absent).
    /// Outcomes are in `keys` order.
    ///
    /// The batch succeeds or fails as a whole: a key of the wrong length
    /// fails it before anything is sent, and the first failed query (or a
    /// duplicate tachygram) fails it. A 409 refetches the map and retries
    /// the whole batch once on the fresh map, and every outcome of the retry
    /// is marked [`Answer::refetched_map`]; nothing else is retried.
    ///
    /// Each query still hides its row, but the number of queries is revealed
    /// (spec 12.3): a batch sends one per distinct row, not one per key and
    /// segment, so the host learns how many rows its keys share. It is a
    /// diagnostic and test tool, not a wallet's lookup pattern.
    pub async fn lookup_many<K: AsRef<[u8]>>(
        &self,
        map: &MapView,
        scope: ScopeSelector,
        keys: &[K],
        parallel: usize,
    ) -> Result<Vec<LookupOutcome>, ClientError> {
        for key in keys {
            let key = key.as_ref();
            if key.len() != self.product.key_len() {
                return Err(ClientError::KeyLength {
                    expected: self.product.key_len(),
                    actual: key.len(),
                });
            }
        }
        match self.lookup_once(map, scope, keys, parallel).await {
            Err(ClientError::StaleRouting { .. }) => {
                let fresh = self.fetch_map().await?;
                let mut outcomes = self.lookup_once(&fresh, scope, keys, parallel).await?;
                for outcome in &mut outcomes {
                    outcome.mark_refetched();
                }
                Ok(outcomes)
            }
            other => other,
        }
    }

    /// One pass of [`Client::lookup_many`] under `map`, without the retry.
    async fn lookup_once<K: AsRef<[u8]>>(
        &self,
        map: &MapView,
        selector: ScopeSelector,
        keys: &[K],
        parallel: usize,
    ) -> Result<Vec<LookupOutcome>, ClientError> {
        let (index, scope) = map
            .scope(selector)
            .ok_or(ClientError::UnknownScope(selector))?;
        if index < map.floor_scope() {
            let below = LookupOutcome::BelowFloor {
                scope_index: index,
                floor_scope: map.floor_scope(),
            };
            return Ok(vec![below; keys.len()]);
        }
        // Every key's plan, and the distinct (session, row) targets of all of
        // them in first-use order (a batch of one: its plan, in segment
        // order). A session names one table of the scope, and the row context
        // is a function of the scope, the table and the row, so one decoded
        // row serves every key whose plan names it.
        let mut fetches: Vec<QueryTarget<'_>> = Vec::new();
        let mut fetch_of: HashMap<(Bytes32, u32), usize> = HashMap::new();
        let mut plans: Vec<Vec<(QueryTarget<'_>, usize)>> = Vec::with_capacity(keys.len());
        for key in keys {
            let plan = scope
                .tables()
                .query_plan(self.product, map.genesis_hash(), key.as_ref())?;
            let plan = plan
                .into_iter()
                .map(|target| {
                    let fetch = *fetch_of
                        .entry((target.table.session_id, target.row))
                        .or_insert_with(|| {
                            fetches.push(target);
                            fetches.len() - 1
                        });
                    (target, fetch)
                })
                .collect();
            plans.push(plan);
        }
        let rows = self.query_rows(&fetches, parallel).await?;
        keys.iter()
            .zip(plans)
            .map(|(key, plan)| {
                let key = key.as_ref();
                let mut answer = answer_for(map, index, scope);
                answer.bucket = plan.first().map(|(t, _)| t.table.bucket);
                let mut found = Vec::new();
                for (target, fetch) in &plan {
                    let entries = &rows[*fetch];
                    let matches: Vec<Entry> = lookup(entries, key).into_iter().cloned().collect();
                    answer.queries.push(QueriedTable {
                        bucket: target.table.bucket,
                        segment: target.table.segment,
                        row: target.row,
                        session_id: target.table.session_id,
                        row_entries: entries.len(),
                        matches: matches.len(),
                    });
                    found.extend(matches);
                }
                if self.product == Product::Tachygram && found.len() > 1 {
                    return Err(ClientError::DuplicateTachygram(found.len()));
                }
                Ok(if found.is_empty() {
                    LookupOutcome::Absent { answer }
                } else {
                    LookupOutcome::Found {
                        entries: found,
                        answer,
                    }
                })
            })
            .collect()
    }

    /// The validated entries of every target's row, in `targets` order.
    /// Each table's public setup is fetched (or found cached) first, in
    /// order, so concurrent queries never fetch one twice; then the rows are
    /// queried with at most `parallel` (at least one) in flight, started in
    /// order; with one in flight they go out one at a time in `targets`
    /// order and the first failure stops the rest. The first query to fail
    /// fails the call and drops the others in flight.
    async fn query_rows(
        &self,
        targets: &[QueryTarget<'_>],
        parallel: usize,
    ) -> Result<Vec<Vec<Entry>>, ClientError> {
        let mut seen = HashSet::new();
        for target in targets {
            if seen.insert(target.table.session_id) {
                self.public(target.table).await?;
            }
        }
        let parallel = parallel.max(1);
        let mut rows: Vec<Option<Vec<Entry>>> = vec![None; targets.len()];
        let mut queued = targets.iter().enumerate();
        let mut in_flight = Vec::with_capacity(parallel.min(targets.len()));
        loop {
            while in_flight.len() < parallel {
                let Some((i, target)) = queued.next() else {
                    break;
                };
                in_flight.push((i, Box::pin(self.query_row(target.table, &target.context))));
            }
            if in_flight.is_empty() {
                break;
            }
            let (i, row) = std::future::poll_fn(|cx| {
                let ready =
                    in_flight
                        .iter_mut()
                        .enumerate()
                        .find_map(|(k, (i, query))| match query.as_mut().poll(cx) {
                            Poll::Ready(row) => Some((k, *i, row)),
                            Poll::Pending => None,
                        });
                match ready {
                    Some((k, i, row)) => {
                        // The finished query; its row is returned.
                        let _done = in_flight.swap_remove(k);
                        Poll::Ready((i, row))
                    }
                    None => Poll::Pending,
                }
            })
            .await;
            rows[i] = Some(row?);
        }
        Ok(rows
            .into_iter()
            .map(|row| row.expect("every target queried"))
            .collect())
    }

    /// A dummy lookup (spec 12.3): a uniformly random bucket of the scope,
    /// then every segment of it at a uniformly random row, exactly the
    /// requests a real lookup of an unknown key sends. Rows are decoded and
    /// validated as a lookup's are. A 409 refetches the map and retries once.
    pub async fn dummy_query(
        &self,
        map: &MapView,
        scope: ScopeSelector,
    ) -> Result<Answer, ClientError> {
        match self.dummy_once(map, scope).await {
            Err(ClientError::StaleRouting { .. }) => {
                let fresh = self.fetch_map().await?;
                let mut answer = self.dummy_once(&fresh, scope).await?;
                answer.refetched_map = true;
                Ok(answer)
            }
            other => other,
        }
    }

    async fn dummy_once(
        &self,
        map: &MapView,
        selector: ScopeSelector,
    ) -> Result<Answer, ClientError> {
        let (index, scope) = map
            .scope(selector)
            .ok_or(ClientError::UnknownScope(selector))?;
        if index < map.floor_scope() {
            return Err(ClientError::BelowFloor {
                scope_index: index,
                floor_scope: map.floor_scope(),
            });
        }
        let tables = scope.tables();
        if tables.tables().is_empty() {
            return Err(ClientError::EmptyScope(index));
        }
        let (bucket, rows): (u32, Vec<u32>) = {
            let mut rng = rand::thread_rng();
            let bucket = rng.gen_range(0..tables.n_buckets());
            let rows = tables
                .segments(bucket)
                .iter()
                .map(|t| rng.gen_range(0..t.geometry.rows()))
                .collect();
            (bucket, rows)
        };
        let mut answer = answer_for(map, index, scope);
        answer.bucket = Some(bucket);
        for (table, row) in tables.segments(bucket).iter().zip(rows) {
            let context = tables.row_context(self.product, map.genesis_hash(), table, row);
            let entries = self.query_row(table, &context).await?;
            answer.queries.push(QueriedTable {
                bucket,
                segment: table.segment,
                row,
                session_id: table.session_id,
                row_entries: entries.len(),
                matches: 0,
            });
        }
        Ok(answer)
    }

    /// Queries `context.row` of `table` privately and returns the validated
    /// entries of the decoded row. Query preparation and reply decoding are
    /// CPU-bound and run on the blocking pool, so concurrent queries
    /// ([`Client::lookup_many`]) prepare and decode in parallel.
    async fn query_row(
        &self,
        table: &Table,
        context: &RowContext,
    ) -> Result<Vec<Entry>, ClientError> {
        let params = pir_params(self.product, table.geometry);
        let public = self.public(table).await?;
        let row = context.row as usize;
        let prepare_params = params.clone();
        let (secret, payload) = blocking(move || {
            let params = prepare_params;
            P8.prepare(params.setup(), params.masks(), params.rows(), row)
                .map_err(ClientError::Pir)
        })
        .await?;
        if payload.len() != params.payload_len() {
            return Err(ClientError::Pir(format!(
                "payload of {} bytes, expected {}",
                payload.len(),
                params.payload_len()
            )));
        }
        let mut nonce = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut nonce);
        let (header, body) = query_body(self.product, table.session_id, nonce, &payload);
        self.counters.queries.fetch_add(1, Ordering::Relaxed);
        let response = self
            .http
            .post(format!("{}/query", self.base))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await
            .map_err(|source| ClientError::Transport {
                route: Route::Query,
                source,
            })?;
        // Read one byte past the exact length so a longer reply is reported
        // as a length error.
        let bytes = read_ok(Route::Query, response, table.geometry.response_len() + 1).await;
        let bytes = match bytes {
            Err(ClientError::TooLarge { .. }) => {
                return Err(ClientError::Frame(
                    tachyon_pir_protocol::FrameError::Length {
                        expected: table.geometry.response_len(),
                        actual: table.geometry.response_len() + 1,
                    },
                ));
            }
            other => other?,
        };
        let (product, geometry, context) = (self.product, table.geometry, *context);
        let (bucket, segment) = (table.bucket, table.segment);
        blocking(move || {
            let reply = validate_response(&header, geometry, &bytes)?;
            debug_assert_eq!(reply.len() + HEADER_BYTES, bytes.len());
            let row = P8
                .decode_cols(&secret, &public, reply, params.cols())
                .map_err(ClientError::Pir)?;
            validate_row(product, &row, &context).map_err(|source| ClientError::Row {
                bucket,
                segment,
                row: context.row,
                source,
            })
        })
        .await
    }

    /// `GET public/{session_id}`, cached by session id, checked against the
    /// table's length and `public_sha256`.
    async fn public(&self, table: &Table) -> Result<Arc<Vec<u8>>, ClientError> {
        let cached = self
            .publics
            .lock()
            .expect("public cache")
            .get(&table.session_id)
            .cloned();
        if let Some(public) = cached {
            return Ok(public);
        }
        self.counters.publics.fetch_add(1, Ordering::Relaxed);
        let expected = table.geometry.public_len();
        let url = format!("{}/public/{}", self.base, table.session_id);
        let response = self.get(Route::Public, &url).await?;
        let bytes = match read_ok(Route::Public, response, expected + 1).await {
            Err(ClientError::TooLarge { .. }) => {
                return Err(ClientError::PublicLength {
                    session_id: table.session_id,
                    expected,
                    actual: expected + 1,
                });
            }
            other => other?,
        };
        if bytes.len() != expected {
            return Err(ClientError::PublicLength {
                session_id: table.session_id,
                expected,
                actual: bytes.len(),
            });
        }
        let actual = Bytes32(Sha256::digest(&bytes).into());
        if actual != table.public_sha256 {
            return Err(ClientError::PublicDigest {
                session_id: table.session_id,
                expected: table.public_sha256,
                actual,
            });
        }
        let public = Arc::new(bytes);
        self.publics
            .lock()
            .expect("public cache")
            .insert(table.session_id, public.clone());
        Ok(public)
    }

    /// Accepts a chain map's anchor against a node (spec 12.1): the node's
    /// `getblockhash 0` must be the map's `genesis_hash` and its
    /// `getblockhash anchor_height` the map's `anchor_hash`. `node_rpc_url`
    /// may carry basic-auth credentials.
    pub async fn accept_anchor(
        &self,
        map: &MapView,
        node_rpc_url: &str,
    ) -> Result<AnchorAccepted, ClientError> {
        let MapHead::Chain(head) = map.head() else {
            return Err(ClientError::NotChain);
        };
        let genesis = self.block_hash(node_rpc_url, 0).await?;
        if genesis != head.genesis_hash {
            return Err(ClientError::AnchorMismatch {
                what: "genesis",
                height: 0,
                map: head.genesis_hash.display_hex(),
                node: genesis.display_hex(),
            });
        }
        let anchor = self.block_hash(node_rpc_url, head.anchor_height).await?;
        if anchor != head.anchor_hash {
            return Err(ClientError::AnchorMismatch {
                what: "anchor",
                height: head.anchor_height,
                map: head.anchor_hash.display_hex(),
                node: anchor.display_hex(),
            });
        }
        Ok(AnchorAccepted {
            anchor_height: head.anchor_height,
            anchor_hash: head.anchor_hash.display_hex(),
            genesis_hash: head.genesis_hash.display_hex(),
        })
    }

    async fn block_hash(&self, url: &str, height: u32) -> Result<BlockHash, ClientError> {
        let request = json!({
            "jsonrpc": "1.0",
            "id": "tachyon-pir-cli",
            "method": "getblockhash",
            "params": [height],
        });
        let response = self
            .rpc
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(request.to_string())
            .send()
            .await
            .map_err(|source| ClientError::Transport {
                route: Route::NodeRpc,
                source,
            })?;
        let status = response.status().as_u16();
        let body = read_limited(Route::NodeRpc, response, MAX_ERROR_BODY_BYTES).await?;
        let reply: Value = serde_json::from_slice(&body)
            .map_err(|_| ClientError::Rpc(format!("HTTP {status}: not a JSON-RPC reply")))?;
        if let Some(error) = reply.get("error").filter(|e| !e.is_null()) {
            return Err(ClientError::Rpc(format!("getblockhash {height}: {error}")));
        }
        let hash = reply
            .get("result")
            .and_then(Value::as_str)
            .ok_or_else(|| ClientError::Rpc(format!("getblockhash {height}: no result")))?;
        BlockHash::parse_display(hash)
            .map_err(|_| ClientError::Rpc(format!("getblockhash {height}: {hash:?}")))
    }

    async fn get(&self, route: Route, url: &str) -> Result<reqwest::Response, ClientError> {
        self.http
            .get(url)
            .send()
            .await
            .map_err(|source| ClientError::Transport { route, source })
    }
}

/// Runs CPU-bound `work` on the blocking pool; a panic in it resumes here.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    match tokio::task::spawn_blocking(work).await {
        Ok(value) => value,
        Err(error) => match error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(error) => panic!("blocking task: {error}"),
        },
    }
}

fn answer_for(map: &MapView, index: u64, scope: &ViewScope) -> Answer {
    Answer {
        scope_index: index,
        scope_id: scope.scope_id(),
        coverage: scope.coverage(),
        map_sha256: map.map_sha256(),
        generation: map.head().generation(),
        bucket: None,
        queries: Vec::new(),
        refetched_map: false,
    }
}

fn retry_after(headers: &HeaderMap) -> Option<String> {
    headers
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

/// The body of a 200 response, at most `limit` bytes; any other status is
/// mapped by [`status_error`].
async fn read_ok(
    route: Route,
    response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, ClientError> {
    let status = response.status().as_u16();
    if status != 200 {
        let retry = retry_after(response.headers());
        let body = read_limited(route, response, MAX_ERROR_BODY_BYTES).await?;
        return Err(status_error(route, status, retry.as_deref(), &body));
    }
    read_limited(route, response, limit).await
}

async fn read_limited(
    route: Route,
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, ClientError> {
    let mut out = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|source| ClientError::Transport { route, source })?
    {
        if out.len() + chunk.len() > limit {
            return Err(ClientError::TooLarge { route, limit });
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}
