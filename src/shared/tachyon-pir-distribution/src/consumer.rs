//! The query-host side: a `pir_sync_manager::publication::PublicationAdapter`
//! that follows one product's publications over `HttpsSource` and
//! `ConsumerEngine`, and the serving registry ([`Publications`]) the host's
//! routes read.
//!
//! Serving rules (spec 10.5, 11.6):
//! - A publication serves only after every object it needs was fetched,
//!   verified against its digest and length, checked by the product and
//!   prepared, and the publisher confirmed it live. A refused publication
//!   leaves the previous one serving.
//! - A publication displaced by a newer one of the same recovery epoch keeps
//!   serving for the retention window (default 600 s).
//! - Negative evidence (a withdrawal, a new recovery epoch, a refuted subject,
//!   a failed floor write) stops serving at once: lookups answer
//!   [`SessionLookup::Unavailable`] until the next publication is installed.
//! - Sessions of a publication displaced because the recovery epoch increased,
//!   and absent from the new one, are revoked for the retention window
//!   ([`SessionLookup::Revoked`], `410`). Any other unknown session is
//!   [`SessionLookup::Unknown`] (`409`).
//! - While the publisher is unreachable the host keeps serving what it last
//!   confirmed for at most `max_unconfirmed`, then answers `Unavailable`
//!   until a pass confirms again. Nothing persisted locally ever restores
//!   serving after a host restart; the host follows the publisher again.
use crate::{
    Error,
    contract::{
        Descriptor, INVENTORY_LIMIT, Inventory, ObjectLimits, Product, ScopeBinding, SessionId,
        Subject, TableObjects,
    },
};
use pir_pipeline::consumer::{
    ConsumerEngine, ConsumerPass, DeferralReason, PeerHandover, PeerReservation,
    reservations::NoPeers,
};
use pir_publication::{
    AuthorityFloor, ObjectCache, ObjectReference, PublicationReference, PublicationScope,
    SourceBinding, VerifiedObjectFile, VerifiedObjects, ViewIdentity,
};
use pir_sync_manager::{
    HttpsSource,
    publication::{
        Envelope, EnvelopeConsumer, ObjectFetcher, PreparedCommit, PublicationAdapter,
        PublicationPlan, ViewRequirement,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::File,
    future::Future,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

/// Default time a displaced publication keeps serving (spec 11.6), and a
/// revoked session keeps answering `410`.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(600);
/// Default time the host keeps serving without a successful authority check.
pub const DEFAULT_MAX_UNCONFIRMED: Duration = Duration::from_secs(600);
/// Default bound on body bytes one synchronization pass may download.
pub const DEFAULT_DOWNLOAD_BUDGET: u64 = 256 << 30;
/// Displaced publications kept at most, oldest dropped first.
pub const MAX_DISPLACED: usize = 16;
/// Blocks per authority-check request, which keeps the echoed response well
/// inside `pir-sync-manager`'s 64 KiB bound.
const BLOCKS_PER_CHECK: usize = 128;

/// The product's preparation of a publication's runtimes.
pub trait Preparer: Send + Sync + 'static {
    /// The PIR runtime of one table, supplied by the query host.
    type Runtime: Send + Sync + 'static;

    /// Check the publication as a whole before any table is prepared: parse
    /// the map head and chunks and check them against the inventory (map
    /// `generation` and `recovery_epoch`, every listed table's digests and
    /// session id). An error refuses the publication.
    fn check(&self, publication: &VerifiedPublication<'_>) -> Result<(), Error>;

    /// Prepare one table from verified bytes, and check the prepared public
    /// setup equals the published one (spec 6.6). Called only for tables no
    /// held runtime already covers. Runs on a blocking thread. An error
    /// refuses the publication.
    fn prepare(&self, table: &VerifiedTable) -> Result<Self::Runtime, Error>;
}

/// One table's verified objects.
pub struct VerifiedTable {
    pub objects: TableObjects,
    pub public: Arc<[u8]>,
    rows: Arc<VerifiedObjectFile>,
}

impl VerifiedTable {
    /// The verified rows bytes.
    pub fn read_rows(&self) -> io::Result<Vec<u8>> {
        self.rows.read_bytes()
    }
    /// The verified rows file, for products that map it instead of reading it.
    pub fn rows_file(&self) -> &VerifiedObjectFile {
        &self.rows
    }
}

/// A publication's verified object set, handed to [`Preparer::check`].
pub struct VerifiedPublication<'a> {
    pub inventory: &'a Inventory,
    /// The oldest scope this host serves; tables below it were not fetched.
    pub floor_scope: u64,
    pub map_head: &'a [u8],
    pub map_chunks: &'a [(ObjectReference, Arc<[u8]>)],
    pub tables: &'a [VerifiedTable],
}

/// One served table.
pub struct Table<T> {
    pub objects: TableObjects,
    pub public: Arc<[u8]>,
    pub runtime: Arc<T>,
}

impl<T> Clone for Table<T> {
    fn clone(&self) -> Self {
        Self {
            objects: self.objects.clone(),
            public: self.public.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

/// One installed publication.
pub struct Publication<T> {
    identity: ViewIdentity,
    inventory: Inventory,
    floor_scope: u64,
    map_head: Arc<[u8]>,
    map_chunks: BTreeMap<String, Arc<[u8]>>,
    tables: BTreeMap<SessionId, Table<T>>,
}

impl<T> Publication<T> {
    pub fn identity(&self) -> ViewIdentity {
        self.identity
    }
    pub fn inventory(&self) -> &Inventory {
        &self.inventory
    }
    pub fn generation(&self) -> u64 {
        self.inventory.generation
    }
    pub fn recovery_epoch(&self) -> u64 {
        self.inventory.recovery_epoch
    }
    pub fn floor_scope(&self) -> u64 {
        self.floor_scope
    }
    /// The map head bytes, served at `GET map`.
    pub fn map_head(&self) -> &Arc<[u8]> {
        &self.map_head
    }
    /// The map head's digest, for `X-Map-Sha256` and `409` bodies.
    pub fn map_sha256(&self) -> &str {
        &self.inventory.map_head.sha256
    }
    /// A map chunk by digest, served at `GET map/{base}/{sha256}`.
    pub fn map_chunk(&self, sha256: &str) -> Option<&Arc<[u8]>> {
        self.map_chunks.get(sha256)
    }
    pub fn table(&self, session: &SessionId) -> Option<&Table<T>> {
        self.tables.get(session)
    }
    pub fn tables(&self) -> impl Iterator<Item = &Table<T>> {
        self.tables.values()
    }
}

/// How a query host answers for one session id.
pub enum SessionLookup<T> {
    /// Served by the current publication or a displaced one in its window.
    Found {
        table: Table<T>,
        publication: Arc<Publication<T>>,
    },
    /// Revoked by a recovery (`410 session_unavailable`).
    Revoked,
    /// Not held (`409 stale_routing` with the current map digest).
    Unknown { map_sha256: String },
    /// Nothing serves now: not ready, fenced, or unconfirmed for too long
    /// (`503 temporarily_unavailable`).
    Unavailable,
}

impl<T> SessionLookup<T> {
    /// The HTTP status spec 10.5 assigns, `200` for a found session.
    pub fn status(&self) -> u16 {
        match self {
            Self::Found { .. } => 200,
            Self::Revoked => 410,
            Self::Unknown { .. } => 409,
            Self::Unavailable => 503,
        }
    }
}

/// Serving windows of one product.
#[derive(Clone, Copy, Debug)]
pub struct ServingPolicy {
    /// Displaced publications serve, and revoked sessions answer `410`, this long.
    pub retention: Duration,
    /// Serving stops this long after the last successful authority check.
    pub max_unconfirmed: Duration,
}

impl Default for ServingPolicy {
    fn default() -> Self {
        Self {
            retention: DEFAULT_RETENTION,
            max_unconfirmed: DEFAULT_MAX_UNCONFIRMED,
        }
    }
}

struct State<T> {
    current: Option<Arc<Publication<T>>>,
    displaced: Vec<(Arc<Publication<T>>, SystemTime)>,
    /// Publications fenced since the last install: no longer served, kept to
    /// decide revocation and to reuse their runtimes.
    fenced: Vec<Arc<Publication<T>>>,
    revoked: HashMap<SessionId, SystemTime>,
    confirmed_at: Option<SystemTime>,
}

impl<T> State<T> {
    fn expire(&mut self, now: SystemTime) {
        self.displaced.retain(|(_, until)| *until > now);
        self.revoked.retain(|_, until| *until > now);
    }
    fn serving(&self) -> impl Iterator<Item = &Arc<Publication<T>>> {
        self.current
            .iter()
            .chain(self.displaced.iter().map(|(publication, _)| publication))
    }
    fn fresh(&self, now: SystemTime, policy: &ServingPolicy) -> bool {
        self.confirmed_at.is_some_and(|at| {
            now.duration_since(at)
                .is_ok_and(|age| age <= policy.max_unconfirmed)
                || at > now
        })
    }
}

/// The serving registry of one product, shared by the follower and the
/// host's routes. Clones share state.
pub struct Publications<T> {
    state: Arc<Mutex<State<T>>>,
    policy: ServingPolicy,
}

impl<T> Clone for Publications<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            policy: self.policy,
        }
    }
}

/// What a committed or deferred pass reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServingReport {
    pub identity: ViewIdentity,
    pub generation: u64,
    pub recovery_epoch: u64,
}

impl<T> Publications<T> {
    pub fn new(policy: ServingPolicy) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                current: None,
                displaced: Vec::new(),
                fenced: Vec::new(),
                revoked: HashMap::new(),
                confirmed_at: None,
            })),
            policy,
        }
    }

    pub fn policy(&self) -> ServingPolicy {
        self.policy
    }

    /// Classify `session` (spec 10.5). A host evaluating a query looks the
    /// session up again before answering; if it is no longer found, the
    /// answer is discarded in favour of the new classification.
    pub fn session(&self, session: &SessionId) -> SessionLookup<T> {
        let now = SystemTime::now();
        let mut state = self.state.lock().unwrap();
        state.expire(now);
        let fresh = state.current.is_some() && state.fresh(now, &self.policy);
        if fresh {
            for publication in state.serving() {
                if let Some(table) = publication.tables.get(session) {
                    return SessionLookup::Found {
                        table: table.clone(),
                        publication: publication.clone(),
                    };
                }
            }
        }
        if state.revoked.contains_key(session) {
            return SessionLookup::Revoked;
        }
        match state.current.as_ref().filter(|_| fresh) {
            Some(current) => SessionLookup::Unknown {
                map_sha256: current.map_sha256().to_string(),
            },
            None => SessionLookup::Unavailable,
        }
    }

    /// The publication serving the map, if anything serves now.
    pub fn current(&self) -> Option<Arc<Publication<T>>> {
        let now = SystemTime::now();
        let state = self.state.lock().unwrap();
        state
            .current
            .clone()
            .filter(|_| state.fresh(now, &self.policy))
    }

    /// Whether the product is ready: a publication serves now.
    pub fn is_ready(&self) -> bool {
        self.current().is_some()
    }

    fn current_identity(&self) -> Option<ViewIdentity> {
        self.state
            .lock()
            .unwrap()
            .current
            .as_ref()
            .map(|p| p.identity)
    }

    fn report(&self) -> Option<ServingReport> {
        self.current().map(|p| report(&p))
    }

    fn confirm(&self, at: SystemTime) {
        let mut state = self.state.lock().unwrap();
        state.confirmed_at = Some(state.confirmed_at.map_or(at, |prior| prior.max(at)));
    }

    /// Stop serving everything; returns the views that were serving.
    fn fence(&self) -> Vec<ViewIdentity> {
        let now = SystemTime::now();
        let mut state = self.state.lock().unwrap();
        state.expire(now);
        let mut fenced: Vec<_> = state.current.take().into_iter().collect();
        fenced.extend(
            state
                .displaced
                .drain(..)
                .map(|(publication, _)| publication),
        );
        let identities = fenced.iter().map(|p| p.identity).collect();
        state.fenced.extend(fenced);
        identities
    }

    /// Stop serving exactly `views`.
    fn invalidate(&self, views: &[ViewIdentity]) {
        let mut state = self.state.lock().unwrap();
        if let Some(current) = state.current.take_if(|p| views.contains(&p.identity)) {
            state.fenced.push(current);
        }
        let (dropped, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut state.displaced)
            .into_iter()
            .partition(|(p, _)| views.contains(&p.identity));
        state.displaced = kept;
        state.fenced.extend(dropped.into_iter().map(|(p, _)| p));
    }

    fn install(
        &self,
        publication: Arc<Publication<T>>,
        confirmed_at: Option<SystemTime>,
    ) -> ServingReport {
        let now = SystemTime::now();
        let until = now + self.policy.retention;
        let epoch = publication.recovery_epoch();
        let mut state = self.state.lock().unwrap();
        state.expire(now);
        let mut displaced = std::mem::take(&mut state.displaced);
        if let Some(previous) = state.current.take() {
            displaced.push((previous, until));
        }
        let mut superseded = std::mem::take(&mut state.fenced);
        for (previous, until) in displaced {
            if previous.recovery_epoch() < epoch {
                superseded.push(previous);
            } else {
                state.displaced.push((previous, until));
            }
        }
        for previous in superseded.iter().filter(|p| p.recovery_epoch() < epoch) {
            for session in previous.tables.keys() {
                if !publication.tables.contains_key(session) {
                    state.revoked.insert(*session, until);
                }
            }
        }
        for session in publication.tables.keys() {
            state.revoked.remove(session);
        }
        let excess = state.displaced.len().saturating_sub(MAX_DISPLACED);
        state.displaced.drain(..excess);
        if let Some(at) = confirmed_at {
            state.confirmed_at = Some(state.confirmed_at.map_or(at, |prior| prior.max(at)));
        }
        let report = report(&publication);
        state.current = Some(publication);
        report
    }

    fn serving_views(&self) -> Vec<(ViewIdentity, Subject)> {
        let now = SystemTime::now();
        let mut state = self.state.lock().unwrap();
        state.expire(now);
        state
            .serving()
            .map(|p| (p.identity, p.inventory.subject.clone()))
            .collect()
    }

    fn retained_views(&self) -> Vec<(ViewIdentity, ViewRequirement)> {
        let now = SystemTime::now();
        let mut state = self.state.lock().unwrap();
        state.expire(now);
        state
            .current
            .iter()
            .map(|p| (p.identity, ViewRequirement::Serving))
            .chain(
                state
                    .displaced
                    .iter()
                    .map(|(p, until)| (p.identity, ViewRequirement::HeldUntil(*until))),
            )
            .collect()
    }

    /// A held runtime prepared from exactly `objects`.
    fn runtime(&self, objects: &TableObjects) -> Option<Arc<T>> {
        let state = self.state.lock().unwrap();
        state
            .serving()
            .chain(state.fenced.iter())
            .filter_map(|p| p.tables.get(&objects.session_id))
            .find(|table| table.objects == *objects)
            .map(|table| table.runtime.clone())
    }

    fn held_sessions(&self) -> BTreeSet<SessionId> {
        let state = self.state.lock().unwrap();
        state
            .serving()
            .chain(state.fenced.iter())
            .flat_map(|p| p.tables.keys().copied())
            .collect()
    }
}

fn report<T>(publication: &Publication<T>) -> ServingReport {
    ServingReport {
        identity: publication.identity,
        generation: publication.generation(),
        recovery_epoch: publication.recovery_epoch(),
    }
}

/// How a query host follows one product's publisher.
#[derive(Clone, Debug)]
pub struct FollowerConfig {
    pub product: Product,
    pub binding: ScopeBinding,
    /// The publisher's HTTPS origin, `https://host[:port]/`.
    pub publisher: String,
    /// PEM trust roots for the publisher (`publisher_ca`); none uses the
    /// system roots.
    pub publisher_ca: Option<Vec<u8>>,
    /// This product's own state directory (locked while following).
    pub state_dir: PathBuf,
    /// Serve the newest `retain_scopes` scopes (spec 11.5); `None` serves all.
    pub retain_scopes: Option<u64>,
    /// Object lengths; production keeps [`ObjectLimits::SPEC`].
    pub limits: ObjectLimits,
    /// Body bytes one pass may download.
    pub download_budget: u64,
}

impl FollowerConfig {
    pub fn new(
        product: Product,
        binding: ScopeBinding,
        publisher: String,
        state_dir: PathBuf,
    ) -> Self {
        Self {
            product,
            binding,
            publisher,
            publisher_ca: None,
            state_dir,
            retain_scopes: None,
            limits: ObjectLimits::SPEC,
            download_budget: DEFAULT_DOWNLOAD_BUDGET,
        }
    }
}

/// The engine a query host runs per product, typically through
/// `pir_pipeline::consumer::polling::SyncManager`.
pub type Engine<P> = ConsumerEngine<EnvelopeConsumer<Follower<P>>>;

/// Open the product's floor and object cache under `state_dir` and build its
/// consumer engine over `HttpsSource`.
pub fn follow<P: Preparer>(
    config: FollowerConfig,
    preparer: P,
    publications: Publications<P::Runtime>,
) -> Result<Engine<P>, Error> {
    let owner = Arc::new(pir_pipeline::service::lock(&config.state_dir)?);
    let roots = match &config.publisher_ca {
        Some(pem) => vec![reqwest::Certificate::from_pem(pem)?],
        None => Vec::new(),
    };
    let source = HttpsSource::new(
        &config.publisher,
        config.product.prefix(),
        config.download_budget,
        &roots,
    )?;
    let floor = AuthorityFloor::open(
        config.state_dir.join("authority-floor.json"),
        SourceBinding {
            publisher_origin: source.origin().to_string(),
            product_namespace: config.product.prefix().into(),
            chain: config.binding.genesis,
        },
    )?;
    let cache = ObjectCache::open(config.state_dir.join("objects"))?;
    let follower = Follower {
        scope: config.product.scope(&config.binding),
        product: config.product,
        limits: config.limits,
        retain_scopes: config.retain_scopes,
        state_dir: config.state_dir,
        _owner: owner,
        preparer: Arc::new(preparer),
        publications,
        prepared: HashMap::new(),
        check_started: None,
    };
    EnvelopeConsumer::open(follower, source, floor, cache)
}

/// The product adapter the consumer engine drives.
pub struct Follower<P: Preparer> {
    product: Product,
    scope: PublicationScope,
    limits: ObjectLimits,
    retain_scopes: Option<u64>,
    state_dir: PathBuf,
    _owner: Arc<File>,
    preparer: Arc<P>,
    publications: Publications<P::Runtime>,
    /// Runtimes of loads that have not been installed yet, reused when a
    /// pass deferred after loading.
    prepared: HashMap<SessionId, (TableObjects, Arc<P::Runtime>)>,
    /// When the latest authority check of serving views started.
    check_started: Option<SystemTime>,
}

impl<P: Preparer> Follower<P> {
    pub fn publications(&self) -> &Publications<P::Runtime> {
        &self.publications
    }
}

pub struct Candidate {
    inventory: Inventory,
    reference: PublicationReference<Descriptor>,
    identity: ViewIdentity,
    floor_scope: u64,
    required: Vec<ObjectReference>,
}

pub struct Loaded<T> {
    reference: PublicationReference<Descriptor>,
    required: Vec<ObjectReference>,
    publication: Arc<Publication<T>>,
}

fn load_publication<P: Preparer>(
    preparer: &P,
    candidate: Candidate,
    objects: &VerifiedObjects,
    reuse: &HashMap<SessionId, Arc<P::Runtime>>,
) -> Result<Publication<P::Runtime>, Error> {
    let inventory = &candidate.inventory;
    let map_head: Arc<[u8]> = objects.read(&inventory.map_head)?.into();
    let map_chunks = inventory
        .map_chunks
        .iter()
        .map(|chunk| Ok((chunk.clone(), objects.read(chunk)?.into())))
        .collect::<io::Result<Vec<(ObjectReference, Arc<[u8]>)>>>()?;
    let tables = inventory
        .tables
        .iter()
        .filter(|table| table.scope_index >= candidate.floor_scope)
        .map(|table| {
            Ok(VerifiedTable {
                objects: table.clone(),
                public: objects.read(&table.public)?.into(),
                rows: objects.get(&table.rows)?,
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    preparer.check(&VerifiedPublication {
        inventory,
        floor_scope: candidate.floor_scope,
        map_head: &map_head,
        map_chunks: &map_chunks,
        tables: &tables,
    })?;
    let mut prepared = BTreeMap::new();
    for table in tables {
        let runtime = match reuse.get(&table.objects.session_id) {
            Some(runtime) => runtime.clone(),
            None => Arc::new(preparer.prepare(&table)?),
        };
        prepared.insert(
            table.objects.session_id,
            Table {
                objects: table.objects,
                public: table.public,
                runtime,
            },
        );
    }
    Ok(Publication {
        identity: candidate.identity,
        floor_scope: candidate.floor_scope,
        map_head,
        map_chunks: map_chunks
            .into_iter()
            .map(|(object, bytes)| (object.sha256, bytes))
            .collect(),
        tables: prepared,
        inventory: candidate.inventory,
    })
}

impl<P: Preparer> PublicationAdapter for Follower<P> {
    type Descriptor = Descriptor;
    type AuthoritySubject = Subject;
    type InstalledView = ViewIdentity;
    type Candidate = Candidate;
    type Loaded = Loaded<P::Runtime>;
    type Report = Option<ServingReport>;

    fn validate_envelope(&self, envelope: &Envelope<Self>) -> Result<(), Error> {
        if envelope.scope != self.scope {
            return Err(format!(
                "{} publisher scope differs from the configured network or source",
                self.product.name()
            )
            .into());
        }
        let epoch_differs = envelope
            .publication
            .as_ref()
            .is_some_and(|p| p.descriptor.recovery_epoch != envelope.fence.recovery_epoch);
        if envelope.eligible && epoch_differs {
            return Err("eligible publication was made under another recovery epoch".into());
        }
        Ok(())
    }

    fn fence_serving(&mut self) -> impl Future<Output = Vec<ViewIdentity>> + Send + 'static {
        let publications = self.publications.clone();
        async move { publications.fence() }
    }

    fn invalidate_views(
        &mut self,
        views: Vec<ViewIdentity>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let publications = self.publications.clone();
        async move { publications.invalidate(&views) }
    }

    fn close_admission(&mut self) -> impl Future<Output = ()> + Send + 'static {
        let publications = self.publications.clone();
        async move {
            publications.fence();
        }
    }

    async fn serving_subjects(&mut self, _: &Envelope<Self>) -> Vec<(ViewIdentity, Subject)> {
        self.check_started = Some(SystemTime::now());
        self.publications.serving_views()
    }

    fn authority_batches(
        &self,
        subjects: Vec<(ViewIdentity, Subject)>,
    ) -> Vec<Vec<(ViewIdentity, Subject)>> {
        let mut batches = Vec::new();
        let mut batch = Vec::new();
        let mut blocks = 0;
        for (identity, subject) in subjects {
            let weight = subject.weight();
            if !batch.is_empty() && blocks + weight > BLOCKS_PER_CHECK {
                batches.push(std::mem::take(&mut batch));
                blocks = 0;
            }
            blocks += weight;
            batch.push((identity, subject));
        }
        if !batch.is_empty() {
            batches.push(batch);
        }
        batches
    }

    async fn plan(
        &mut self,
        envelope: &Envelope<Self>,
        objects: &mut ObjectFetcher<'_>,
    ) -> Result<PublicationPlan<Candidate>, Error> {
        // The engine plans only after every serving view was confirmed.
        if let Some(at) = self.check_started.take() {
            self.publications.confirm(at);
        }
        let reference = envelope
            .publication
            .as_ref()
            .ok_or("eligible envelope names no publication")?;
        let identity = ViewIdentity(reference.publication);
        if self.publications.current_identity() == Some(identity) {
            return Ok(PublicationPlan::Deferred);
        }
        let bytes = objects
            .fetch_metadata(&reference.inventory, INVENTORY_LIMIT)
            .await?;
        let inventory: Inventory = serde_json::from_slice(&bytes)?;
        inventory.validate(self.product, &self.limits)?;
        if !inventory.describes(&reference.inventory, reference) {
            return Err("inventory does not describe the authorized publication".into());
        }
        if inventory.recovery_epoch != envelope.fence.recovery_epoch {
            return Err("inventory was made under another recovery epoch".into());
        }
        let floor_scope = inventory.floor_scope(self.retain_scopes);
        let required = inventory.objects_from(floor_scope);
        Ok(PublicationPlan::Candidate {
            identity,
            required_objects: required.clone(),
            candidate: Candidate {
                inventory,
                reference: reference.clone(),
                identity,
                floor_scope,
                required,
            },
        })
    }

    async fn load(
        &mut self,
        candidate: Candidate,
        objects: VerifiedObjects,
    ) -> Result<Self::Loaded, Error> {
        let mut reuse = HashMap::new();
        for table in &candidate.inventory.tables {
            let held = self.publications.runtime(table).or_else(|| {
                self.prepared
                    .get(&table.session_id)
                    .filter(|(objects, _)| objects == table)
                    .map(|(_, runtime)| runtime.clone())
            });
            if let Some(runtime) = held {
                reuse.insert(table.session_id, runtime);
            }
        }
        let preparer = self.preparer.clone();
        let reference = candidate.reference.clone();
        let required = candidate.required.clone();
        let publication = tokio::task::spawn_blocking(move || {
            load_publication(preparer.as_ref(), candidate, &objects, &reuse)
        })
        .await??;
        for table in publication.tables() {
            self.prepared.insert(
                table.objects.session_id,
                (table.objects.clone(), table.runtime.clone()),
            );
        }
        Ok(Loaded {
            reference,
            required,
            publication: Arc::new(publication),
        })
    }

    async fn rebind(
        &mut self,
        _: &Envelope<Self>,
        loaded: Self::Loaded,
        _: &mut ObjectFetcher<'_>,
    ) -> Result<Self::Loaded, Error> {
        Ok(loaded)
    }

    fn revalidate(
        &self,
        envelope: &Envelope<Self>,
        loaded: &Self::Loaded,
    ) -> Result<(ViewIdentity, Subject), Error> {
        if envelope.publication.as_ref() != Some(&loaded.reference) {
            return Err("publication advanced during loading".into());
        }
        Ok((
            loaded.publication.identity,
            loaded.publication.inventory.subject.clone(),
        ))
    }

    async fn reserve_peer(&mut self, _: &Self::Loaded) -> Result<PeerReservation, Error> {
        Ok(NoPeers.reserve())
    }

    fn view_objects(&self, loaded: &Self::Loaded) -> BTreeSet<String> {
        std::iter::once(&loaded.reference.inventory)
            .chain(&loaded.required)
            .map(|object| object.sha256.clone())
            .collect()
    }

    async fn retained_views(&mut self) -> Vec<(ViewIdentity, ViewRequirement)> {
        self.publications.retained_views()
    }

    fn prepare_commit(
        &mut self,
        loaded: Self::Loaded,
    ) -> Result<(PreparedCommit<Self::Report>, ViewIdentity), Error> {
        let identity = loaded.publication.identity;
        let record = serde_json::to_vec(&ActiveRecord {
            publication: hex::encode(identity.0),
            inventory: loaded.reference.inventory.clone(),
        })?;
        let root = self.state_dir.clone();
        let publications = self.publications.clone();
        let confirmed = self.check_started.take();
        let publication = loaded.publication;
        let revoke = self.publications.clone();
        Ok((
            PreparedCommit {
                record: Box::new(move || {
                    pir_control::durable::write_atomic(&root, "active.json", |file| {
                        file.write_all(&record)
                    })?;
                    pir_control::durable::sync_dir(&root)
                }),
                install: Box::pin(
                    async move { Ok(Some(publications.install(publication, confirmed))) },
                ),
                revoke_after_failed_record: Box::pin(async move {
                    revoke.fence();
                }),
            },
            identity,
        ))
    }

    fn installed(&mut self, _: ViewIdentity) {
        let held = self.publications.held_sessions();
        self.prepared.retain(|session, _| held.contains(session));
    }

    async fn release_peer(&mut self, _: &Self::Report) -> PeerHandover {
        NoPeers.complete()
    }

    async fn serving_report(&mut self) -> Self::Report {
        self.publications.report()
    }

    async fn deferred(&mut self, reason: DeferralReason) {
        tracing::debug!(
            product = self.product.name(),
            ?reason,
            "publication pass deferred"
        );
    }

    async fn pass_finished(&mut self, result: &Result<ConsumerPass<Self::Report>, Error>) {
        match result {
            Err(error) => {
                tracing::warn!(product = self.product.name(), %error, "publication synchronization failed")
            }
            Ok(ConsumerPass::Committed { report, .. }) => {
                tracing::info!(product = self.product.name(), serving = ?report, "publication committed")
            }
            Ok(ConsumerPass::Deferred { report }) => {
                tracing::debug!(product = self.product.name(), serving = ?report, "publication pass deferred")
            }
        }
    }
}

/// The durable record of the last installed publication (`active.json`).
/// It is informational: serving after a restart always follows the publisher.
#[derive(serde::Serialize)]
struct ActiveRecord {
    publication: String,
    inventory: ObjectReference,
}
