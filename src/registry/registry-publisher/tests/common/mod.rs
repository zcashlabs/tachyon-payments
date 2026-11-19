//! Shared helpers: deterministic valid addresses, a registry served over
//! real HTTP on loopback, the write-API client side (challenge, proof of
//! work, body), and an independent reading of the published map and rows.
#![allow(dead_code)]

use pir_pipeline::{engine::PublisherEngine, service::Shutdown};
use registry_publisher::{
    Registry, RegistryConfig, RegistryIdentity, RegistrySettings, challenge::ChallengeKey,
};
use registry_store::{Binding, STORE_FILE, Store};
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tachyon_pir_distribution::{Current, Distribution, Inventory};
use tachyon_pir_protocol::{
    Address, BlockHash, Challenge, Entry, KEM_EK_BYTES, Product, RegisterRequest, RegistryMapChunk,
    RegistryMapHead, RegistryScope, ScopeTables, pow_hash, pow_meets, validate_row,
};

pub const PRODUCT: Product = Product::Registry;
pub const Q: u16 = 3329;

pub fn identity() -> RegistryIdentity {
    RegistryIdentity::new("test", BlockHash::from_protocol_bytes([0x5a; 32]))
}

pub fn binding() -> Binding {
    let identity = identity();
    Binding {
        chain: identity.chain,
        genesis_hash: identity.genesis_hash,
    }
}

/// Settings for tests: a low difficulty and generous limits unless a test
/// narrows them.
pub fn settings(listen: SocketAddr, state_dir: &Path) -> RegistrySettings {
    let mut settings = RegistrySettings::new(listen, state_dir.to_path_buf());
    settings.difficulty_bits = 6;
    settings.per_client_concurrent = 64;
    settings.per_client_per_minute = 10_000;
    settings.global_per_minute = 100_000;
    settings.publish_ms = 50;
    settings
}

fn stream(label: &[u8], index: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 32);
    let mut block = 0u64;
    while out.len() < len {
        out.extend_from_slice(
            &Sha256::new()
                .chain_update(label)
                .chain_update(index.to_le_bytes())
                .chain_update(block.to_le_bytes())
                .finalize(),
        );
        block += 1;
    }
    out.truncate(len);
    out
}

/// Pack 768 coefficients below 4096 as `ByteEncode12`.
pub fn encode12(coefficients: &[u16]) -> Vec<u8> {
    coefficients
        .chunks(2)
        .flat_map(|pair| {
            let (a, b) = (pair[0], pair[1]);
            [
                a as u8,
                ((a >> 8) as u8) | ((b as u8 & 0x0f) << 4),
                (b >> 4) as u8,
            ]
        })
        .collect()
}

/// A valid address, a pure function of `index`: `pk` below 2^254 (so below
/// `p`), every `ek` coefficient below `q`, a pseudorandom `ρ`.
pub fn address(index: u64) -> Address {
    let mut pk: [u8; 32] = stream(b"pk", index, 32).try_into().unwrap();
    pk[31] &= 0x3f;
    let coefficients: Vec<u16> = stream(b"ek", index, 1536)
        .chunks(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]) % Q)
        .collect();
    let mut ek = encode12(&coefficients);
    ek.extend_from_slice(&stream(b"rho", index, 32));
    Address {
        pk,
        ek: ek.try_into().unwrap(),
    }
}

/// Append `addresses` straight to the log in `state_dir` (bulk seeding; the
/// write API is exercised separately).
pub fn seed_log(state_dir: &Path, addresses: &[Address]) {
    let store = Store::open(state_dir.join(STORE_FILE), binding()).unwrap();
    for batch in addresses.chunks(512) {
        store.register_many(batch).unwrap();
    }
}

/// The first nonce meeting `bits` for `address` under `challenge`.
pub fn solve(challenge: &[u8; 32], address: &Address, bits: u32) -> [u8; 8] {
    let digest = address.digest();
    (0u64..)
        .map(u64::to_le_bytes)
        .find(|nonce| pow_meets(&pow_hash(challenge, nonce, &digest), bits))
        .unwrap()
}

pub fn body(challenge: [u8; 32], address: &Address, bits: u32) -> Vec<u8> {
    RegisterRequest {
        challenge,
        nonce: solve(&challenge, address, bits),
        address: address.clone(),
    }
    .encode()
}

/// The challenge key of `state_dir`, read from its secret file as an
/// independent party with the secret would.
pub fn key_of(state_dir: &Path) -> ChallengeKey {
    let secret = std::fs::read(state_dir.join("challenge-secret")).unwrap();
    ChallengeKey::from_secret(secret.try_into().unwrap())
}

/// One registry served on loopback, as the write API's reverse proxy reaches
/// it, with the connection's peer address available.
pub struct Server {
    pub addr: SocketAddr,
    pub dir: PathBuf,
    pub registry: Registry,
    pub shutdown: Shutdown,
    serving: tokio::task::JoinHandle<()>,
    publishing: Option<tokio::task::JoinHandle<()>>,
}

impl Server {
    /// Open the registry in `dir` with `change` applied to the test
    /// settings and serve it. The publication loop runs only if `publish`.
    pub async fn start(
        dir: &Path,
        publish: bool,
        change: impl FnOnce(&mut RegistrySettings),
    ) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut settings = settings(addr, dir);
        change(&mut settings);
        let shutdown = Shutdown::default();
        let mut registry =
            Registry::open(&RegistryConfig::new(settings, identity()), shutdown.clone()).unwrap();
        let app = registry.router();
        let stop = shutdown.clone();
        let serving = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move { stop.cancelled().await })
            .await
            .unwrap();
        });
        let publishing = publish.then(|| {
            let task = registry.take_publisher().unwrap();
            let stop = shutdown.clone();
            tokio::spawn(async move {
                use pir_pipeline::publisher::PublisherExecutor;
                PublisherEngine.run(task, stop).await.unwrap();
            })
        });
        Server {
            addr,
            dir: dir.to_path_buf(),
            registry,
            shutdown,
            serving,
            publishing,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// Publish now (restoring first on the first call), when the loop is not
    /// running.
    pub async fn publish(&mut self) -> Option<registry_publisher::Report> {
        self.registry
            .publisher()
            .publish_pending(&PublisherEngine)
            .await
            .unwrap()
    }

    pub async fn stop(self) {
        self.shutdown.request();
        tokio::time::timeout(Duration::from_secs(20), self.serving)
            .await
            .expect("the listener stops")
            .unwrap();
        if let Some(publishing) = self.publishing {
            tokio::time::timeout(Duration::from_secs(20), publishing)
                .await
                .expect("the publication loop stops")
                .unwrap();
        }
        drop(self.registry);
        // Connection tasks drop their router clones (and with them the state
        // directory lock) just after the listener stops; wait for that, as a
        // restarted process would find the lock released.
        let deadline = Instant::now() + Duration::from_secs(10);
        while pir_pipeline::service::lock(&self.dir).is_err() {
            assert!(
                Instant::now() < deadline,
                "the state directory stays locked"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

pub async fn challenge(client: &reqwest::Client, server: &Server) -> Challenge {
    let response = client
        .get(server.url("/registry/v1/challenge"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    response.json().await.unwrap()
}

/// `POST register` with `body`, from client `forwarded` (an
/// `X-Forwarded-For` value) when given.
pub async fn post(
    client: &reqwest::Client,
    server: &Server,
    body: Vec<u8>,
    forwarded: Option<&str>,
) -> reqwest::Response {
    let mut request = client.post(server.url("/registry/v1/register")).body(body);
    if let Some(forwarded) = forwarded {
        request = request.header("x-forwarded-for", forwarded);
    }
    request.send().await.unwrap()
}

/// Register `address` through a fresh challenge; the parsed `200` body.
pub async fn register(
    client: &reqwest::Client,
    server: &Server,
    address: &Address,
    bits: u32,
) -> tachyon_pir_protocol::Registration {
    let challenge = challenge(client, server).await;
    let response = post(
        client,
        server,
        body(challenge.challenge.0, address, bits),
        None,
    )
    .await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    response.json().await.unwrap()
}

/// An error response's status, `code`, `message` and `Retry-After`.
pub async fn refusal(response: reqwest::Response) -> (u16, String, String, Option<u64>) {
    let status = response.status().as_u16();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let retry = response
        .headers()
        .get("retry-after")
        .map(|v| v.to_str().unwrap().parse().unwrap());
    let body: tachyon_pir_protocol::ErrorBody = response.json().await.unwrap();
    (status, body.code.as_str().to_owned(), body.message, retry)
}

/// The current publication read back from the object store: envelope,
/// inventory, head and every sealed map chunk, checked as a client checks.
pub struct Published {
    pub current: Current,
    pub inventory: Inventory,
    pub head: RegistryMapHead,
    pub chunks: Vec<RegistryMapChunk>,
}

impl Published {
    pub fn read(distribution: &Distribution) -> Published {
        let current = distribution.current();
        assert!(current.eligible);
        let inventory = distribution.current_inventory().unwrap().unwrap();
        let head_bytes = distribution.read_object(&inventory.map_head).unwrap();
        let head = RegistryMapHead::parse(&head_bytes).unwrap();
        // Canonical encoding: the bytes are the compact serialization.
        assert_eq!(head.to_bytes().unwrap(), head_bytes);
        let chunks: Vec<RegistryMapChunk> = inventory
            .map_chunks
            .iter()
            .enumerate()
            .map(|(position, object)| {
                head.parse_chunk(position, &distribution.read_object(object).unwrap())
                    .unwrap()
            })
            .collect();
        head.check_listing(&chunks).unwrap();
        assert_eq!(
            inventory.subject,
            tachyon_pir_distribution::Subject::Registry {
                log_sequence: head.log_sequence
            }
        );
        Published {
            current,
            inventory,
            head,
            chunks,
        }
    }

    pub fn scopes(&self) -> Vec<&RegistryScope> {
        self.chunks
            .iter()
            .flat_map(|c| c.scopes.iter())
            .chain(self.head.open_scopes.iter())
            .collect()
    }

    pub fn scope(&self, chunk_id: u64) -> &RegistryScope {
        self.scopes()
            .into_iter()
            .find(|s| s.scope_id == chunk_id)
            .unwrap()
    }

    /// Every entry of `chunk_id`, decoded from its rows objects through
    /// `validate_row`, keyed by `(segment, row)`.
    pub fn rows(
        &self,
        distribution: &Distribution,
        chunk_id: u64,
    ) -> std::collections::HashMap<(u32, u32), Vec<Address>> {
        let scope = self.scope(chunk_id);
        let mut out = std::collections::HashMap::new();
        for table in &scope.tables {
            let objects = self
                .inventory
                .tables
                .iter()
                .find(|t| t.scope_index == chunk_id && t.session_id == table.session_id.0)
                .unwrap();
            let rows = distribution.read_object(&objects.rows).unwrap();
            assert_eq!(rows.len(), 4096 * 8192);
            let mut count = 0;
            for row in 0..4096u32 {
                let context = scope.row_context(PRODUCT, &self.head.genesis_hash, table, row);
                let slice = &rows[row as usize * 8192..(row as usize + 1) * 8192];
                let entries: Vec<Address> = validate_row(PRODUCT, slice, &context)
                    .unwrap()
                    .into_iter()
                    .map(|entry| match entry {
                        Entry::Registry(entry) => {
                            assert_eq!(entry.key, entry.address.key16());
                            entry.address
                        }
                        _ => panic!("not a registry entry"),
                    })
                    .collect();
                count += entries.len() as u64;
                if !entries.is_empty() {
                    out.insert((table.segment, row), entries);
                }
            }
            assert_eq!(count, table.entries);
        }
        out
    }

    /// Lookups of `address` in `chunk_id` as a wallet makes them: every
    /// segment of its bucket, at the row the query plan names.
    pub fn find(
        &self,
        rows: &std::collections::HashMap<(u32, u32), Vec<Address>>,
        chunk_id: u64,
        address: &Address,
    ) -> usize {
        let scope = self.scope(chunk_id);
        scope
            .query_plan(PRODUCT, &self.head.genesis_hash, &address.key16())
            .unwrap()
            .iter()
            .map(|target| {
                rows.get(&(target.table.segment, target.row))
                    .map_or(0, |entries| {
                        entries.iter().filter(|a| *a == address).count()
                    })
            })
            .sum()
    }
}

/// Wait until `check` holds, polling every 20 ms for at most `seconds`.
pub async fn eventually(seconds: u64, what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// An `ek` with coefficient `index` set to `value` and the rest valid.
pub fn ek_with(base: &Address, index: usize, value: u16) -> [u8; KEM_EK_BYTES] {
    let mut ek = base.ek;
    let group = index / 2 * 3;
    if index.is_multiple_of(2) {
        ek[group] = value as u8;
        ek[group + 1] = (ek[group + 1] & 0xf0) | (value >> 8) as u8;
    } else {
        ek[group + 1] = (ek[group + 1] & 0x0f) | ((value & 0x0f) << 4) as u8;
        ek[group + 2] = (value >> 4) as u8;
    }
    ek
}
