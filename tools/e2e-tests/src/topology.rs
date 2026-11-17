//! The end-to-end topology of a chain product (spec 13.1): the in-process node
//! double serving the authored chain, the publisher binary, the TLS edge in
//! front of it and the query-server binary trusting the edge, each joined only
//! by real HTTP(S); the probe client is the wallet.
//!
//! ```text
//! NodeDouble ──RPC──▶ tachyon-pir-publisher-test ──HTTP──▶ TlsEdge ──HTTPS──▶ tachyon-pir-query-server-test ◀── Client
//! ```
//!
//! [`PublisherHost`] and [`QueryHost`] are the two spawned hosts with their
//! configurations (spec 11.7); [`Topology`] strings them together for one
//! product. Nothing here knows a product's tables: the helpers read the
//! publisher's distribution routes and the query host's public and management
//! listeners only.
use crate::budget::Budget;
use crate::client::{self, Reply};
use crate::node::start_node;
use crate::process::{Process, free_addrs};
use crate::tls_edge::TlsEdge;
use pir_publication::ObjectReference;
use reqwest::StatusCode;
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tachyon_fixtures::{NodeDouble, SyntheticChain, display};
use tachyon_pir_cli::{Client, ClientError, MapHead, MapView};
use tachyon_pir_distribution::{Current, Inventory, Subject};
use tachyon_pir_protocol::{ChainMapHead, Product};

/// How often the waits below poll.
pub const POLL: Duration = Duration::from_millis(50);

/// The deadline for one wait inside `budget`: what remains less a second, so
/// a wait that times out fails with its own message (and the processes' logs)
/// before the budget guard cancels the whole test.
pub fn wait_limit(budget: &Budget) -> Duration {
    budget.remaining().saturating_sub(Duration::from_secs(1))
}

/// Tests that spawn both hosts and prepare real tables are CPU- and
/// memory-heavy (spec 13.1: about 1 s per table on each side), so within one
/// test process they run one at a time: each takes this guard before its
/// budget starts. `cargo test` runs a binary's tests in parallel; without it
/// they would slow one another past their budgets.
pub async fn exclusive() -> tokio::sync::MutexGuard<'static, ()> {
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    SERIAL.lock().await
}

/// Polls `check` every [`POLL`] until it returns `Some`, panicking with
/// `what` and the last state `describe` reports after `timeout`.
pub async fn wait_for<T, F, Fut>(what: &str, timeout: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        let last = match check().await {
            Ok(value) => return value,
            Err(state) => state,
        };
        if Instant::now() >= deadline {
            panic!("timed out after {timeout:?} waiting for {what}; last state: {last}");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// A product as both hosts name it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProductNames {
    /// The configuration section (`tachygram`, `first_contact`, ..).
    pub section: &'static str,
    /// The wallet-facing product.
    pub product: Product,
}

impl ProductNames {
    pub const TACHYGRAM: ProductNames = ProductNames {
        section: "tachygram",
        product: Product::Tachygram,
    };

    /// `/provider/<product>/v1/` (spec 11.3).
    pub fn distribution_prefix(&self) -> &'static str {
        self.product.distribution_prefix()
    }
}

/// The publisher binary with one product enabled, on fixed reserved ports.
pub struct PublisherHost {
    process: Process,
    product: ProductNames,
    management: SocketAddr,
    listen: SocketAddr,
    state_dir: PathBuf,
}

impl PublisherHost {
    /// Configures (spec 11.7) and starts `binary` following `rpc_url` with
    /// `epoch_length`-block epochs and a `poll_ms` tip poll, and waits up to
    /// `timeout` for `/ready`.
    pub async fn start(
        binary: PathBuf,
        product: ProductNames,
        rpc_url: &str,
        epoch_length: u32,
        poll_ms: u64,
        timeout: Duration,
    ) -> Self {
        let mut process = Process::new("publisher", binary);
        let ports = free_addrs(2);
        let (management, listen) = (ports[0], ports[1]);
        let state_dir = process.state_dir(product.section);
        let mut config = Map::new();
        config.insert("management_listen".into(), json!(management.to_string()));
        config.insert(
            "chain".into(),
            json!({"rpc_url": rpc_url, "no_auth": true, "poll_ms": poll_ms,
                   "epoch_length": epoch_length}),
        );
        config.insert(
            product.section.into(),
            json!({"enabled": true,
                   "settings": {"listen": listen.to_string(), "state_dir": state_dir}}),
        );
        process.config("--config", "publisher.json", &Value::Object(config));
        process.start();
        let mut host = Self {
            process,
            product,
            management,
            listen,
            state_dir,
        };
        host.wait_ready(timeout).await;
        host
    }

    pub fn process(&mut self) -> &mut Process {
        &mut self.process
    }

    /// The management listener (`/health`, `/ready`, discovery).
    pub fn management(&self) -> SocketAddr {
        self.management
    }

    /// The product listener (distribution prefix and `/metrics`).
    pub fn listen(&self) -> SocketAddr {
        self.listen
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
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
            self.product.distribution_prefix()
        )
    }

    /// `GET <prefix>current`, read directly from the product listener (not
    /// through the edge, so edge counts stay the query host's).
    pub async fn current(&self) -> Current {
        client::get_json(&client::plain(), &self.url("current"), StatusCode::OK).await
    }

    /// `GET <prefix>objects/<sha256>`.
    pub async fn object(&self, sha256: &str) -> bytes::Bytes {
        let url = self.url(&format!("objects/{sha256}"));
        client::get_bytes(&client::plain(), &url, StatusCode::OK).await
    }

    /// The current publication's inventory object reference and inventory;
    /// `None` while nothing is published.
    pub async fn inventory(&self) -> Option<Publication> {
        let current = self.current().await;
        let reference = current.publication?;
        let bytes = self.object(&reference.inventory.sha256).await;
        let inventory: Inventory = serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("the inventory object does not parse: {error}"));
        assert_eq!(
            inventory.descriptor(),
            reference.descriptor,
            "the envelope's descriptor names another inventory"
        );
        Some(Publication {
            eligible: current.eligible,
            inventory_object: reference.inventory,
            inventory,
        })
    }

    /// The current publication's map head, for a chain product.
    pub async fn chain_map_head(&self) -> Option<ChainMapHead> {
        let publication = self.inventory().await?;
        let bytes = self.object(&publication.inventory.map_head.sha256).await;
        Some(ChainMapHead::parse(&bytes).expect("the published map head parses"))
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

    /// Waits until the current publication's authority subject names the
    /// anchor `height` with `hash` (protocol order): the publisher has caught
    /// up with a node whose tip is `height + CONFIRMATION_DEPTH`.
    pub async fn wait_anchor(&self, height: u64, hash: [u8; 32], timeout: Duration) -> Publication {
        self.wait_publication(
            &format!("the publisher to publish anchor {height}"),
            timeout,
            |p| {
                matches!(&p.inventory.subject,
                Subject::Chain { anchor, .. } if anchor.height == height && anchor.hash == hash)
            },
        )
        .await
    }

    /// `GET /metrics` on the product listener.
    pub async fn metrics(&self) -> String {
        let url = format!("http://{}/metrics", self.listen);
        client::get(&client::plain(), &url)
            .await
            .assert_status(StatusCode::OK, &url)
            .text()
    }
}

/// A publisher's current publication.
#[derive(Clone, Debug)]
pub struct Publication {
    /// Whether the envelope allows serving it.
    pub eligible: bool,
    /// The object holding the inventory.
    pub inventory_object: ObjectReference,
    pub inventory: Inventory,
}

impl Publication {
    /// Every object path of the publication under `prefix`
    /// (`<prefix>objects/<sha256>`): the inventory, the map head, the map
    /// chunks and every table's rows and public setup.
    pub fn object_paths(&self, prefix: &str) -> BTreeSet<String> {
        std::iter::once(&self.inventory_object)
            .chain(self.inventory.objects().iter())
            .map(|object| format!("{prefix}objects/{}", object.sha256))
            .collect()
    }

    /// The object paths of every table's rows and public setup.
    pub fn table_object_paths(&self, prefix: &str) -> BTreeSet<String> {
        self.inventory
            .tables
            .iter()
            .flat_map(|table| [&table.rows, &table.public])
            .map(|object| format!("{prefix}objects/{}", object.sha256))
            .collect()
    }
}

/// The chain identity the query host is configured to expect (spec 11.7).
#[derive(Clone, Debug)]
pub struct ChainIdentity {
    pub network: String,
    /// Display order, as the node prints it.
    pub genesis: String,
    pub activation_height: u64,
    pub epoch_length: u32,
}

impl ChainIdentity {
    pub fn of(chain: &SyntheticChain, epoch_length: u32) -> Self {
        Self {
            network: chain.chain_name().to_owned(),
            genesis: display(&chain.genesis_hash()),
            activation_height: chain
                .activation_height()
                .expect("a chain product needs NuTachyon"),
            epoch_length,
        }
    }
}

/// The query-server binary serving one product from a publisher origin.
pub struct QueryHost {
    process: Process,
    product: ProductNames,
    management: SocketAddr,
    public: SocketAddr,
    state_dir: PathBuf,
}

impl QueryHost {
    /// Writes the configuration (spec 11.7) for `product` following
    /// `publisher` (an `https://` origin) trusting `publisher_ca`; `extra`
    /// fields are merged into the product's `settings`. Nothing runs until
    /// [`QueryHost::start`].
    pub fn configure(
        binary: PathBuf,
        product: ProductNames,
        publisher: &str,
        publisher_ca: &Path,
        chain: &ChainIdentity,
        extra: &Map<String, Value>,
    ) -> Self {
        let mut process = Process::new("query", binary);
        let ports = free_addrs(2);
        let (management, public) = (ports[0], ports[1]);
        let state_dir = process.state_dir(product.section);
        let mut settings = json!({
            "publisher": publisher,
            "state_dir": state_dir,
            "publisher_ca": publisher_ca,
            "role": "combined",
            "publisher_network": chain.network,
            "publisher_genesis": chain.genesis,
            "activation_height": chain.activation_height,
            "epoch_length": chain.epoch_length,
        });
        let object = settings.as_object_mut().expect("settings object");
        for (key, value) in extra {
            object.insert(key.clone(), value.clone());
        }
        let mut config = Map::new();
        config.insert("management_listen".into(), json!(management.to_string()));
        config.insert("public_listen".into(), json!(public.to_string()));
        config.insert(
            product.section.into(),
            json!({"enabled": true, "settings": settings}),
        );
        process.config("--config", "query.json", &Value::Object(config));
        Self {
            process,
            product,
            management,
            public,
            state_dir,
        }
    }

    pub fn process(&mut self) -> &mut Process {
        &mut self.process
    }

    pub fn management(&self) -> SocketAddr {
        self.management
    }

    pub fn public(&self) -> SocketAddr {
        self.public
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    /// `http://<public_listen>`: the wallet's origin.
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

    /// Crash (`SIGKILL` of the process group) and start again with the same
    /// configuration and state; waits for `/ready`.
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

    /// `GET <public>/<mount>/<route>`, whatever the status.
    pub async fn public_get(&self, route: &str) -> Reply {
        client::get(
            &client::plain(),
            &format!("{}{}/{route}", self.origin(), self.product.product.mount()),
        )
        .await
    }

    /// One value of the management `/metrics` (exact series name with
    /// labels, e.g. `tachyon_tachygram_sync_passes_total{result="failed"}`).
    pub async fn metric(&self, series: &str) -> Option<f64> {
        let reply = self.management_get("metrics").await;
        reply.assert_status(StatusCode::OK, "/metrics");
        reply.text().lines().find_map(|line| {
            let (name, value) = line.rsplit_once(' ')?;
            (name == series).then(|| value.parse().ok()).flatten()
        })
    }

    /// A probe client (the wallet) on the canonical mount.
    pub fn wallet(&self) -> Arc<Client> {
        Arc::new(
            Client::new(&self.origin(), self.product.product, None)
                .expect("a client for the public listener"),
        )
    }
}

/// The chain map head of a fetched map.
pub fn chain_head(map: &MapView) -> &ChainMapHead {
    match map.head() {
        MapHead::Chain(head) => head,
        MapHead::Registry(_) => panic!("a chain product's map has a chain head"),
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
                let head = chain_head(&map);
                Err(format!(
                    "map generation {} recovery epoch {} anchor {}",
                    head.generation,
                    head.recovery_epoch.get(),
                    head.anchor_height
                ))
            }
            Err(error) => Err(describe(&error)),
        }
    })
    .await
}

/// A client error with its kind, for messages.
pub fn describe(error: &ClientError) -> String {
    format!("{} ({error})", error.kind())
}

/// What a product test sets up.
#[derive(Clone, Debug)]
pub struct TopologyOptions {
    pub product: ProductNames,
    pub epoch_length: u32,
    /// The publisher's tip poll interval.
    pub chain_poll_ms: u64,
    /// Fields merged into the query host's product `settings`
    /// (`poll_ms`, `max_unconfirmed_seconds`, `admission`, ..).
    pub query_settings: Map<String, Value>,
}

impl TopologyOptions {
    /// `product` with `epoch_length`-block epochs, fast polling on both sides
    /// and admission wide enough for a test's parallel lookups.
    pub fn new(product: ProductNames, epoch_length: u32) -> Self {
        let mut query_settings = Map::new();
        query_settings.insert("poll_ms".into(), json!(100));
        query_settings.insert(
            "admission".into(),
            json!({"executing": 4, "waiting": 64, "wait_ms": 5000, "per_client": 16}),
        );
        Self {
            product,
            epoch_length,
            chain_poll_ms: 50,
            query_settings,
        }
    }

    /// Sets one query-host product setting.
    pub fn query_setting(mut self, key: &str, value: Value) -> Self {
        self.query_settings.insert(key.into(), value);
        self
    }
}

/// The whole topology of one product test.
pub struct Topology {
    pub node: NodeDouble,
    pub publisher: PublisherHost,
    pub edge: TlsEdge,
    /// Configured; started by [`Topology::start_query`].
    pub query: QueryHost,
    pub options: TopologyOptions,
}

impl Topology {
    /// Serves `chain` from an in-process node double, starts the publisher
    /// (waiting for its `/ready`), puts the TLS edge in front of it
    /// (discovery to management, the product prefix to the product listener)
    /// and configures the query host against the edge. The query host is not
    /// started.
    pub async fn launch(
        chain: SyntheticChain,
        options: TopologyOptions,
        publisher_binary: PathBuf,
        query_binary: PathBuf,
        budget: &Budget,
    ) -> Self {
        let identity = ChainIdentity::of(&chain, options.epoch_length);
        let node = start_node(chain).await;
        let publisher = PublisherHost::start(
            publisher_binary,
            options.product,
            &node.url(),
            options.epoch_length,
            options.chain_poll_ms,
            wait_limit(budget),
        )
        .await;
        let edge = TlsEdge::builder()
            .management(publisher.management().to_string())
            .product(
                options.product.product.name(),
                publisher.listen().to_string(),
            )
            .start()
            .await
            .expect("start the TLS edge");
        let query = QueryHost::configure(
            query_binary,
            options.product,
            &edge.origin(),
            edge.ca_path(),
            &identity,
            &options.query_settings,
        );
        Self {
            node,
            publisher,
            edge,
            query,
            options,
        }
    }

    /// [`Topology::launch`], then starts the query host and waits for its
    /// `/ready`.
    pub async fn start(
        chain: SyntheticChain,
        options: TopologyOptions,
        publisher_binary: PathBuf,
        query_binary: PathBuf,
        budget: &Budget,
    ) -> Self {
        let mut topology =
            Self::launch(chain, options, publisher_binary, query_binary, budget).await;
        topology.start_query(budget).await;
        topology
    }

    pub async fn start_query(&mut self, budget: &Budget) {
        self.query.start();
        self.query.wait_ready(wait_limit(budget)).await;
    }

    /// The distribution prefix of the product, for edge paths.
    pub fn prefix(&self) -> &'static str {
        self.options.product.distribution_prefix()
    }
}
