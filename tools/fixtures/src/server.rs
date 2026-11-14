//! The JSON-RPC node double: serves a [`SyntheticChain`] over HTTP as the Zakura
//! node does, with fault injection and request counting, plus control endpoints
//! under `/control/` on the same listener.
use crate::hash::{Hash32, display, parse_display};
use crate::json::{self, Placement};
use crate::model::{Block, SyntheticChain};
use crate::spec::{BlockJson, to_specs};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

/// JSON-RPC error codes the node uses.
pub mod codes {
    /// zcashd's `RPC_INVALID_ADDRESS_OR_KEY`: a block hash the node does not know.
    pub const BLOCK_NOT_FOUND: i64 = -5;
    /// zcashd's `RPC_INVALID_PARAMETER`: a height above the tip.
    pub const INVALID_PARAMETER: i64 = -8;
    pub const PARSE_ERROR: i64 = -32700;
    pub const INVALID_REQUEST: i64 = -32600;
    pub const METHOD_NOT_FOUND: i64 = -32601;
    pub const INVALID_PARAMS: i64 = -32602;
}

/// How `getblock` answers for the hash of a block that was reorged away.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StaleBlocks {
    /// RPC error -5 "Block not found", as for an unknown hash. The default, as in
    /// `wallet-pir`'s node double; the live node's behaviour is not established.
    #[default]
    NotFound,
    /// Serve the block with `confirmations: -1`, no `nextblockhash`, and
    /// `in_active_chain: false` / `confirmations: 0` on each transaction, as
    /// zcashd does for a block it still has on a side chain.
    Serve,
}

/// HTTP Basic credentials the double requires.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BasicAuth {
    pub user: String,
    pub password: String,
}

impl BasicAuth {
    /// The `Authorization` header value.
    pub fn header(&self) -> String {
        format!(
            "Basic {}",
            base64(format!("{}:{}", self.user, self.password).as_bytes())
        )
    }

    /// Writes these credentials as a node cookie file (`user:password`) at `path`.
    pub fn write_cookie(&self, path: &std::path::Path) -> io::Result<()> {
        std::fs::write(path, format!("{}:{}", self.user, self.password))
    }
}

/// Server settings.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeOptions {
    /// Required credentials; `None` accepts every request, as a node with RPC
    /// authentication disabled does.
    pub auth: Option<BasicAuth>,
    pub stale_blocks: StaleBlocks,
}

/// An injected failure of one JSON-RPC call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    /// The whole HTTP response has this status (a JSON-RPC error body with code
    /// -32603 is attached, as zcashd's 500s carry one).
    Http(u16),
    /// The call answers this JSON-RPC error with HTTP 200.
    Rpc { code: i64, message: String },
}

/// A deliberate inconsistency in a served `getblock` result, for the decoder's
/// integrity checks (spec section 11.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockMismatch {
    /// `hash` differs from the requested hash.
    Hash,
    /// `height` is one more than the block's.
    Height,
    /// `previousblockhash` names no real parent.
    PreviousHash,
    /// A different active block (the next one, or the previous at the tip) is
    /// served whole.
    OtherBlock,
    /// The first transaction's `blockhash` differs (verbosity 2 only).
    TxBlockHash,
    /// The first transaction's `height` is one more (verbosity 2 only).
    TxHeight,
    /// The last transaction's `txid` differs from the one authored.
    Txid,
}

/// One JSON-RPC call as received (a batch records each entry).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedCall {
    pub method: String,
    pub params: Value,
}

#[derive(Debug)]
struct FailRule {
    method: Option<String>,
    remaining: usize,
    failure: Failure,
}

#[derive(Debug, Default)]
struct Faults {
    default_delay: Duration,
    delays: HashMap<String, Duration>,
    failures: Vec<FailRule>,
    mismatch: Option<(BlockMismatch, usize)>,
}

impl Faults {
    fn delay_for(&self, method: &str) -> Duration {
        self.delays
            .get(method)
            .copied()
            .unwrap_or(self.default_delay)
    }

    fn take_failure(&mut self, method: &str) -> Option<Failure> {
        let rule = self.failures.iter_mut().find(|rule| {
            rule.remaining > 0 && rule.method.as_deref().is_none_or(|only| only == method)
        })?;
        rule.remaining -= 1;
        let failure = rule.failure.clone();
        self.failures.retain(|rule| rule.remaining > 0);
        Some(failure)
    }

    fn take_mismatch(&mut self) -> Option<BlockMismatch> {
        let (kind, remaining) = self.mismatch.as_mut()?;
        let kind = *kind;
        *remaining -= 1;
        if *remaining == 0 {
            self.mismatch = None;
        }
        Some(kind)
    }
}

struct Running {
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

struct Inner {
    addr: SocketAddr,
    options: NodeOptions,
    chain: Mutex<SyntheticChain>,
    faults: Mutex<Faults>,
    calls: Mutex<Vec<RecordedCall>>,
    server: Mutex<Option<Running>>,
    closed: AtomicBool,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A running node double. Dropping it stops the server.
pub struct NodeDouble {
    inner: Arc<Inner>,
}

impl NodeDouble {
    /// Serves `chain` on an ephemeral port of 127.0.0.1.
    pub async fn start(chain: SyntheticChain, options: NodeOptions) -> io::Result<Self> {
        Self::start_on(chain, options, SocketAddr::from(([127, 0, 0, 1], 0))).await
    }

    /// Serves `chain` on `addr`.
    pub async fn start_on(
        chain: SyntheticChain,
        options: NodeOptions,
        addr: SocketAddr,
    ) -> io::Result<Self> {
        let listener = bind(addr)?;
        let inner = Arc::new(Inner {
            addr: listener.local_addr()?,
            options,
            chain: Mutex::new(chain),
            faults: Mutex::new(Faults::default()),
            calls: Mutex::new(Vec::new()),
            server: Mutex::new(None),
            closed: AtomicBool::new(false),
        });
        inner.launch(listener);
        Ok(Self { inner })
    }

    /// The bound address; it stays the same across an outage.
    pub fn addr(&self) -> SocketAddr {
        self.inner.addr
    }

    /// The JSON-RPC URL, `http://<addr>`.
    pub fn url(&self) -> String {
        format!("http://{}", self.inner.addr)
    }

    /// The served chain. Edits (append, reorg) are visible to the next request;
    /// do not hold the guard across an `.await`.
    pub fn chain(&self) -> MutexGuard<'_, SyntheticChain> {
        lock(&self.inner.chain)
    }

    /// Delays every call of `method` (every method for `None`) by `delay` before
    /// it is answered. A per-method delay overrides the default one.
    pub fn set_delay(&self, method: Option<&str>, delay: Duration) {
        let mut faults = lock(&self.inner.faults);
        match method {
            Some(method) => {
                faults.delays.insert(method.to_owned(), delay);
            }
            None => faults.default_delay = delay,
        }
    }

    /// Fails the next `count` calls of `method` (of any method for `None`).
    pub fn fail_next(&self, method: Option<&str>, count: usize, failure: Failure) {
        if count > 0 {
            lock(&self.inner.faults).failures.push(FailRule {
                method: method.map(str::to_owned),
                remaining: count,
                failure,
            });
        }
    }

    /// Serves the next `count` successful `getblock` results (verbosity 1 or 2)
    /// with `kind` of inconsistency.
    pub fn mismatch_next(&self, count: usize, kind: BlockMismatch) {
        lock(&self.inner.faults).mismatch = (count > 0).then_some((kind, count));
    }

    /// Removes every delay, pending failure and mismatch.
    pub fn clear_faults(&self) {
        *lock(&self.inner.faults) = Faults::default();
    }

    /// Takes the server down: the listener is closed and open connections are
    /// dropped, so calls fail to connect. The chain and counters are kept.
    pub async fn outage(&self) {
        self.inner.outage().await;
    }

    /// Brings the server back up on the same address after [`Self::outage`].
    pub async fn restore(&self) -> io::Result<()> {
        self.inner.restore().await
    }

    /// Whether the server is accepting connections.
    pub fn is_up(&self) -> bool {
        lock(&self.inner.server).is_some()
    }

    /// Calls of `method` received so far, failed and delayed ones included.
    pub fn count(&self, method: &str) -> usize {
        lock(&self.inner.calls)
            .iter()
            .filter(|call| call.method == method)
            .count()
    }

    /// Calls received so far, per method.
    pub fn counts(&self) -> BTreeMap<String, usize> {
        counts(&lock(&self.inner.calls))
    }

    /// Every call received so far, in arrival order.
    pub fn calls(&self) -> Vec<RecordedCall> {
        lock(&self.inner.calls).clone()
    }

    /// Forgets the calls received so far.
    pub fn reset_calls(&self) {
        lock(&self.inner.calls).clear();
    }
}

impl Drop for NodeDouble {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        if let Some(running) = lock(&self.inner.server).take() {
            // Aborting the accept loop drops its connection set, which aborts
            // every connection task.
            running.task.abort();
        }
    }
}

fn counts(calls: &[RecordedCall]) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for call in calls {
        *out.entry(call.method.clone()).or_default() += 1;
    }
    out
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

impl Inner {
    fn launch(self: &Arc<Self>, listener: TcpListener) {
        let (shutdown, signal) = oneshot::channel();
        let task = tokio::spawn(accept_loop(self.clone(), listener, signal));
        *lock(&self.server) = Some(Running { shutdown, task });
    }

    async fn outage(&self) {
        let running = lock(&self.server).take();
        if let Some(running) = running {
            let _ = running.shutdown.send(());
            let _ = running.task.await;
        }
    }

    async fn restore(self: &Arc<Self>) -> io::Result<()> {
        if self.closed.load(Ordering::SeqCst) || lock(&self.server).is_some() {
            return Ok(());
        }
        let mut attempts = 0;
        let listener = loop {
            match bind(self.addr) {
                Ok(listener) => break listener,
                Err(error) if error.kind() == io::ErrorKind::AddrInUse && attempts < 100 => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => return Err(error),
            }
        };
        // Another restore may have won the race; then this listener is dropped.
        let up = lock(&self.server).is_some();
        if !up && !self.closed.load(Ordering::SeqCst) {
            self.launch(listener);
        }
        Ok(())
    }
}

async fn accept_loop(
    inner: Arc<Inner>,
    listener: TcpListener,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let inner = inner.clone();
                connections.spawn(async move {
                    let service = service_fn(move |request| {
                        let inner = inner.clone();
                        async move { Ok::<_, Infallible>(handle(inner, request).await) }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    drop(listener);
    connections.shutdown().await;
}

type HttpResponse = Response<Full<Bytes>>;

fn respond(status: StatusCode, body: &Value) -> HttpResponse {
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    response
}

async fn handle(inner: Arc<Inner>, request: Request<Incoming>) -> HttpResponse {
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => {
            return respond(
                StatusCode::BAD_REQUEST,
                &json!({"error": "unreadable body"}),
            );
        }
    };
    if let Some(action) = path.strip_prefix("/control/") {
        return control(&inner, &method, action, &body).await;
    }
    if let Some(auth) = &inner.options.auth
        && authorization.as_deref() != Some(auth.header().as_str())
    {
        return respond(StatusCode::UNAUTHORIZED, &json!(null));
    }
    if method != Method::POST {
        return respond(
            StatusCode::METHOD_NOT_ALLOWED,
            &json!({"error": "JSON-RPC requests are POSTed"}),
        );
    }
    rpc(&inner, &body).await
}

/// A JSON-RPC response envelope for `request`, shaped as the testnet node's:
/// `result`, `error` and the echoed `id` and `jsonrpc`.
fn envelope(request: &Value, outcome: Result<Value, (i64, String)>) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let mut out = match outcome {
        Ok(result) => json!({"result": result, "error": null, "id": id}),
        Err((code, message)) => {
            json!({"result": null, "error": {"code": code, "message": message}, "id": id})
        }
    };
    if let Some(version) = request.get("jsonrpc") {
        out["jsonrpc"] = version.clone();
    }
    out
}

async fn rpc(inner: &Arc<Inner>, body: &[u8]) -> HttpResponse {
    let parsed: Value = match serde_json::from_slice(body) {
        Ok(parsed) => parsed,
        Err(_) => {
            let error = envelope(
                &Value::Null,
                Err((codes::PARSE_ERROR, "Parse error".into())),
            );
            return respond(StatusCode::OK, &error);
        }
    };
    let (requests, batch) = match parsed {
        Value::Array(requests) => (requests, true),
        request => (vec![request], false),
    };
    if requests.is_empty() {
        let error = envelope(
            &Value::Null,
            Err((codes::INVALID_REQUEST, "Invalid Request".into())),
        );
        return respond(StatusCode::OK, &error);
    }
    let mut delay = Duration::ZERO;
    let mut http_failure = None;
    let mut injected = Vec::with_capacity(requests.len());
    {
        let mut faults = lock(&inner.faults);
        let mut calls = lock(&inner.calls);
        for request in &requests {
            let method = request.get("method").and_then(Value::as_str).unwrap_or("");
            calls.push(RecordedCall {
                method: method.to_owned(),
                params: request.get("params").cloned().unwrap_or(Value::Null),
            });
            delay = delay.max(faults.delay_for(method));
            injected.push(match faults.take_failure(method) {
                Some(Failure::Http(status)) => {
                    http_failure.get_or_insert(status);
                    None
                }
                Some(Failure::Rpc { code, message }) => Some((code, message)),
                None => None,
            });
        }
    }
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    if let Some(status) = http_failure {
        let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let error = envelope(
            &requests[0],
            Err((-32603, format!("injected HTTP {}", status.as_u16()))),
        );
        return respond(status, &error);
    }
    let answers: Vec<Value> = requests
        .iter()
        .zip(injected)
        .map(|(request, injected)| match injected {
            Some(error) => envelope(request, Err(error)),
            None => envelope(request, answer(inner, request)),
        })
        .collect();
    if batch {
        respond(StatusCode::OK, &Value::Array(answers))
    } else {
        respond(StatusCode::OK, &answers[0])
    }
}

type Outcome = Result<Value, (i64, String)>;

fn rpc_error(code: i64, message: &str) -> (i64, String) {
    (code, message.to_owned())
}

fn answer(inner: &Inner, request: &Value) -> Outcome {
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    let params: &[Value] = match request.get("params") {
        Some(Value::Array(params)) => params,
        None | Some(Value::Null) => &[],
        Some(_) => return Err(rpc_error(codes::INVALID_PARAMS, "Invalid params")),
    };
    let chain = lock(&inner.chain);
    match method {
        "getblockchaininfo" => Ok(json::blockchain_info(&chain)),
        "getblockcount" => Ok(json!(chain.tip_height())),
        "getbestblockhash" => Ok(json!(chain.tip().hash_display())),
        "getblockhash" => {
            let height = params
                .first()
                .and_then(Value::as_i64)
                .ok_or_else(|| rpc_error(codes::INVALID_PARAMS, "Invalid params"))?;
            u64::try_from(height)
                .ok()
                .and_then(|height| chain.block(height))
                .map(|block| json!(block.hash_display()))
                .ok_or_else(|| rpc_error(codes::INVALID_PARAMETER, "Block height out of range"))
        }
        "getblock" => getblock(inner, &chain, params),
        _ => Err(rpc_error(codes::METHOD_NOT_FOUND, "Method not found")),
    }
}

fn getblock(inner: &Inner, chain: &SyntheticChain, params: &[Value]) -> Outcome {
    let selector = params
        .first()
        .ok_or_else(|| rpc_error(codes::INVALID_PARAMS, "Invalid params"))?;
    let verbosity = match params.get(1) {
        None | Some(Value::Null) => 1,
        Some(Value::Bool(verbose)) => u64::from(*verbose),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| rpc_error(codes::INVALID_PARAMS, "Invalid params"))?,
    };
    let verbosity = u8::try_from(verbosity)
        .ok()
        .filter(|verbosity| *verbosity <= 2)
        .ok_or_else(|| {
            rpc_error(
                codes::INVALID_PARAMETER,
                "Verbosity must be in range from 0 to 2",
            )
        })?;
    let height = |height: u64| {
        chain
            .block(height)
            .map(|block| (block, true))
            .ok_or_else(|| rpc_error(codes::INVALID_PARAMETER, "Block height out of range"))
    };
    let (block, active) =
        match selector {
            Value::String(text) if text.len() == 64 => {
                let found = parse_display(text)
                    .and_then(|hash| chain.lookup(&hash))
                    .filter(|found| {
                        found.active || inner.options.stale_blocks == StaleBlocks::Serve
                    })
                    .ok_or_else(|| rpc_error(codes::BLOCK_NOT_FOUND, "Block not found"))?;
                (found.block, found.active)
            }
            Value::String(text) => height(text.parse().map_err(|_| {
                rpc_error(codes::INVALID_PARAMETER, "Invalid block height parameter")
            })?)?,
            Value::Number(number) => height(number.as_u64().ok_or_else(|| {
                rpc_error(codes::INVALID_PARAMETER, "Block height out of range")
            })?)?,
            _ => return Err(rpc_error(codes::INVALID_PARAMS, "Invalid params")),
        };
    let placement = Placement::of(chain, block, active);
    let served = json::block(block, placement, verbosity);
    if verbosity == 0 {
        return Ok(served);
    }
    let mismatch = lock(&inner.faults).take_mismatch();
    Ok(match mismatch {
        Some(kind) => mismatched(chain, block, verbosity, kind, served),
        None => served,
    })
}

/// `hash` with its first byte flipped.
fn altered(mut hash: Hash32) -> Hash32 {
    hash[0] ^= 0xff;
    hash
}

fn mismatched(
    chain: &SyntheticChain,
    block: &Block,
    verbosity: u8,
    kind: BlockMismatch,
    mut served: Value,
) -> Value {
    let wrong_hash = json!(display(&altered(block.hash())));
    match kind {
        BlockMismatch::Hash => served["hash"] = wrong_hash,
        BlockMismatch::Height => served["height"] = json!(block.height() + 1),
        BlockMismatch::PreviousHash => {
            served["previousblockhash"] = json!(display(&altered(block.prev())));
        }
        BlockMismatch::OtherBlock => {
            let other = chain
                .block(block.height() + 1)
                .or_else(|| block.height().checked_sub(1).and_then(|h| chain.block(h)));
            match other {
                Some(other) => {
                    let placement = Placement::of(chain, other, true);
                    return json::block(other, placement, verbosity);
                }
                None => served["hash"] = wrong_hash,
            }
        }
        BlockMismatch::TxBlockHash => {
            if let Some(tx) = served["tx"].get_mut(0).filter(|tx| tx.is_object()) {
                tx["blockhash"] = wrong_hash;
            }
        }
        BlockMismatch::TxHeight => {
            if let Some(tx) = served["tx"].get_mut(0).filter(|tx| tx.is_object()) {
                tx["height"] = json!(block.height() + 1);
            }
        }
        BlockMismatch::Txid => {
            let last = block.txs().last().expect("every block has a coinbase");
            let wrong = json!(display(&altered(last.txid())));
            if let Some(tx) = served["tx"].as_array_mut().and_then(|txs| txs.last_mut()) {
                match tx {
                    Value::Object(object) => {
                        object.insert("txid".into(), wrong);
                    }
                    other => *other = wrong,
                }
            }
        }
    }
    served
}

// Control endpoints.

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct AppendBody {
    random_blocks: u64,
    blocks: Vec<BlockJson>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReorgBody {
    fork_height: u64,
    #[serde(default)]
    random_blocks: u64,
    #[serde(default)]
    blocks: Vec<BlockJson>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutageBody {
    duration_ms: u64,
    #[serde(default = "default_outage_delay")]
    delay_ms: u64,
}

fn default_outage_delay() -> u64 {
    50
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FailBody {
    #[serde(default)]
    method: Option<String>,
    count: usize,
    #[serde(default)]
    http_status: Option<u16>,
    #[serde(default)]
    rpc_code: Option<i64>,
    #[serde(default)]
    rpc_message: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DelayBody {
    #[serde(default)]
    method: Option<String>,
    ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MismatchBody {
    kind: BlockMismatch,
    count: usize,
}

/// A refused control request: its status and message.
type Refusal = (StatusCode, String);

fn bad_request(message: impl std::fmt::Display) -> Refusal {
    (StatusCode::BAD_REQUEST, message.to_string())
}

fn parse<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, Refusal> {
    let body = if body.iter().all(u8::is_ascii_whitespace) {
        b"{}".as_slice()
    } else {
        body
    };
    serde_json::from_slice(body).map_err(bad_request)
}

fn state(inner: &Inner) -> Value {
    let chain = lock(&inner.chain);
    json!({
        "tip": chain.tip_height(),
        "tip_hash": chain.tip().hash_display(),
        "genesis_hash": display(&chain.genesis_hash()),
        "generation": chain.generation(),
        "up": lock(&inner.server).is_some(),
        "counts": counts(&lock(&inner.calls)),
    })
}

async fn control(inner: &Arc<Inner>, method: &Method, action: &str, body: &[u8]) -> HttpResponse {
    match control_action(inner, method, action, body) {
        Ok(()) => respond(StatusCode::OK, &state(inner)),
        Err((status, message)) => respond(status, &json!({"error": message})),
    }
}

fn control_action(
    inner: &Arc<Inner>,
    method: &Method,
    action: &str,
    body: &[u8],
) -> Result<(), Refusal> {
    if method == Method::GET && action == "state" {
        return Ok(());
    }
    if method != Method::POST {
        return Err((
            StatusCode::NOT_FOUND,
            format!("no control endpoint {method} /control/{action}"),
        ));
    }
    match action {
        "append" => {
            let body: AppendBody = parse(body)?;
            let specs = to_specs(&body.blocks).map_err(bad_request)?;
            let mut chain = lock(&inner.chain);
            for spec in specs {
                chain.append(spec);
            }
            chain.append_random(body.random_blocks);
        }
        "reorg" => {
            let body: ReorgBody = parse(body)?;
            let specs = to_specs(&body.blocks).map_err(bad_request)?;
            let mut chain = lock(&inner.chain);
            chain.reorg(body.fork_height, specs).map_err(bad_request)?;
            chain.append_random(body.random_blocks);
        }
        "outage" => {
            let body: OutageBody = parse(body)?;
            let inner = inner.clone();
            // The outage starts after `delay_ms`, so this response is delivered
            // before the connection carrying it is dropped.
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(body.delay_ms)).await;
                inner.outage().await;
                tokio::time::sleep(Duration::from_millis(body.duration_ms)).await;
                if let Err(error) = inner.restore().await {
                    eprintln!("node double: restore after outage failed: {error}");
                }
            });
        }
        "fail" => {
            let body: FailBody = parse(body)?;
            let failure = match (body.http_status, body.rpc_code) {
                (Some(status), None) => Failure::Http(status),
                (None, Some(code)) => Failure::Rpc {
                    code,
                    message: body.rpc_message.unwrap_or_else(|| "injected".into()),
                },
                _ => return Err(bad_request("give exactly one of http_status and rpc_code")),
            };
            if body.count > 0 {
                lock(&inner.faults).failures.push(FailRule {
                    method: body.method,
                    remaining: body.count,
                    failure,
                });
            }
        }
        "delay" => {
            let body: DelayBody = parse(body)?;
            let delay = Duration::from_millis(body.ms);
            let mut faults = lock(&inner.faults);
            match body.method {
                Some(method) => {
                    faults.delays.insert(method, delay);
                }
                None => faults.default_delay = delay,
            }
        }
        "mismatch" => {
            let body: MismatchBody = parse(body)?;
            lock(&inner.faults).mismatch = (body.count > 0).then_some((body.kind, body.count));
        }
        "clear-faults" => *lock(&inner.faults) = Faults::default(),
        "reset-calls" => lock(&inner.calls).clear(),
        _ => {
            return Err((
                StatusCode::NOT_FOUND,
                format!("no control endpoint POST /control/{action}"),
            ));
        }
    }
    Ok(())
}

/// Standard base64 with padding, for the `Authorization` header.
fn base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let bytes = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(bytes[0]) << 16) | (u32::from(bytes[1]) << 8) | u32::from(bytes[2]);
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(ALPHABET[((n >> shift) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_the_test_credential() {
        assert_eq!(base64(b"test:test"), "dGVzdDp0ZXN0");
        assert_eq!(base64(b"a"), "YQ==");
        assert_eq!(base64(b"ab"), "YWI=");
        assert_eq!(base64(b""), "");
    }
}
