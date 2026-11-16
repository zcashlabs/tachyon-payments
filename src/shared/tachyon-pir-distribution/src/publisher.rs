//! The publisher side: one product's object store and durable authority, its
//! durable generation counter, and the `current`, `objects/:id` and
//! `authority/check` routes.
use crate::{
    Error,
    contract::{
        Current, Descriptor, INVENTORY_LIMIT, Inventory, MAX_CHECK_BLOCKS, ObjectLimits, Product,
        ScopeBinding, Subject, TableObjects,
    },
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use pir_publication::{
    AuthorityCheckRefusal, AuthorityCheckRequest, AuthorityCheckResponse, AuthorityTransition,
    ObjectReference, PublicationAuthority, PublicationAuthorityError, PublicationReference,
    PublishedObjectStore, SubjectAnswer, open_object_file,
};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    io::Read,
    path::{Path as FilePath, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// Default time a displaced publication's objects stay served, so a query
/// host that observed it can finish fetching.
pub const DEFAULT_PUBLISHER_RETENTION: Duration = Duration::from_secs(600);

const GENERATION_FILE: &str = "generation.json";
const IMMUTABLE: &str = "public, max-age=31536000, immutable";

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// What the generic distribution needs from the product.
pub trait PublisherHooks: Send + Sync + 'static {
    /// Whether each subject is still authoritative at the publisher's source,
    /// in order: for chain products every named block is canonical, for the
    /// registry the log still begins with `log_sequence` entries. An error
    /// answers `503`.
    fn check_subjects(&self, subjects: Vec<Subject>) -> BoxFuture<Result<Vec<bool>, Error>>;

    /// The map head `head` re-issued with `generation` and `recovery_epoch`,
    /// every scope, chunk and table unchanged. Used to restore a publication
    /// under a restarted publisher's epoch, and to publish a candidate again
    /// after an uncertain authority write forced a recovery.
    fn reissue_head(
        &self,
        head: &[u8],
        generation: u64,
        recovery_epoch: u64,
    ) -> Result<Vec<u8>, Error>;
}

#[derive(Clone, Copy, Debug)]
pub struct DistributionOptions {
    /// How long a displaced publication's objects stay served.
    pub retention: Duration,
    /// Object lengths; production keeps [`ObjectLimits::SPEC`].
    pub limits: ObjectLimits,
}

impl Default for DistributionOptions {
    fn default() -> Self {
        Self {
            retention: DEFAULT_PUBLISHER_RETENTION,
            limits: ObjectLimits::SPEC,
        }
    }
}

/// A publication in preparation: its reserved generation and the fence it
/// started under. A commit is refused once the fence moved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Draft {
    pub generation: u64,
    pub recovery_epoch: u64,
    latest_withdrawal: u64,
}

/// What a product publishes, apart from what the draft fixes.
#[derive(Clone, Debug)]
pub struct Contents {
    pub map_head: ObjectReference,
    pub map_chunks: Vec<ObjectReference>,
    pub newest_scope: u64,
    pub tables: Vec<TableObjects>,
    pub subject: Subject,
}

/// An assembled publication: its inventory is stored and validated, nothing
/// is retained or published yet.
pub struct Candidate {
    draft: Draft,
    inventory: Inventory,
    reference: PublicationReference<Descriptor>,
    objects: Vec<ObjectReference>,
}

impl Candidate {
    pub fn inventory(&self) -> &Inventory {
        &self.inventory
    }
    pub fn reference(&self) -> &PublicationReference<Descriptor> {
        &self.reference
    }
}

/// A publication that became current.
#[derive(Clone, Debug)]
pub struct Committed {
    pub sequence: u64,
    pub generation: u64,
    pub recovery_epoch: u64,
    pub reference: PublicationReference<Descriptor>,
}

#[derive(Debug)]
pub enum CommitRefusal {
    /// A withdrawal or recovery happened after the draft began; rebuild.
    FenceMoved,
    /// A publication with this or a later generation is already current.
    Superseded,
    /// An object the inventory names is not installed.
    MissingObject(String),
}
impl std::fmt::Display for CommitRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FenceMoved => {
                f.write_str("publication authority was fenced after the draft began")
            }
            Self::Superseded => f.write_str("a newer publication is already current"),
            Self::MissingObject(sha) => write!(f, "publication object {sha} is not installed"),
        }
    }
}
impl std::error::Error for CommitRefusal {}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationRecord {
    generation: u64,
}

/// One product's distribution on the publisher.
#[derive(Clone)]
pub struct Distribution {
    inner: Arc<Inner>,
}

struct Inner {
    product: Product,
    root: PathBuf,
    authority: Mutex<PublicationAuthority<Descriptor>>,
    objects: PublishedObjectStore,
    generation: Mutex<u64>,
    restorable: AtomicBool,
    hooks: Arc<dyn PublisherHooks>,
    retention_ms: u64,
    limits: ObjectLimits,
}

impl Distribution {
    /// Open the product's distribution under `state_dir/distribution/<product>`.
    /// The authority starts a new, withdrawn recovery epoch on every open
    /// (spec 11.4); [`Self::restore`] republishes the previous publication.
    pub fn open(
        state_dir: &FilePath,
        product: Product,
        binding: ScopeBinding,
        hooks: Arc<dyn PublisherHooks>,
        options: DistributionOptions,
    ) -> Result<Self, Error> {
        let root = state_dir.join("distribution").join(product.name());
        std::fs::create_dir_all(&root)?;
        let authority: PublicationAuthority<Descriptor> =
            PublicationAuthority::open(root.join("authority.json"), product.scope(&binding))?;
        let objects = PublishedObjectStore::open(root.join("objects"))?;
        let saved = match std::fs::read(root.join(GENERATION_FILE)) {
            Ok(bytes) => serde_json::from_slice::<GenerationRecord>(&bytes)?.generation,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let published = authority
            .current()
            .publication
            .as_ref()
            .map_or(0, |p| p.descriptor.generation);
        Ok(Self {
            inner: Arc::new(Inner {
                product,
                root,
                authority: Mutex::new(authority),
                objects,
                generation: Mutex::new(saved.max(published)),
                restorable: AtomicBool::new(true),
                hooks,
                retention_ms: u64::try_from(options.retention.as_millis())
                    .map_err(|_| "publisher retention overflows")?,
                limits: options.limits,
            }),
        })
    }

    pub fn product(&self) -> Product {
        self.inner.product
    }

    /// The envelope `current` serves.
    pub fn current(&self) -> Current {
        self.inner.authority.lock().unwrap().current().clone()
    }

    /// The last generation reserved (published or burned by a failed attempt).
    pub fn last_generation(&self) -> u64 {
        *self.inner.generation.lock().unwrap()
    }

    /// Install immutable bytes as an object. Installing grants no serving:
    /// only a committed publication retains and serves its objects, and an
    /// object no publication references is collected at the next commit.
    pub fn install(&self, bytes: &[u8]) -> Result<ObjectReference, Error> {
        Ok(self.inner.objects.store(bytes)?)
    }

    pub fn install_stream(&self, input: impl Read) -> Result<ObjectReference, Error> {
        Ok(self.inner.objects.store_stream(input)?)
    }

    /// Read a retained object, for example a previous map chunk to reissue.
    pub fn read_object(&self, object: &ObjectReference) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        self.inner
            .objects
            .open_retained(&object.sha256)?
            .read_to_end(&mut bytes)?;
        object.verify(&bytes)?;
        Ok(bytes)
    }

    /// The inventory of the envelope's publication, eligible or not.
    pub fn current_inventory(&self) -> Result<Option<Inventory>, Error> {
        let Some(reference) = self.current().publication else {
            return Ok(None);
        };
        let inventory: Inventory =
            serde_json::from_slice(&self.read_object(&reference.inventory)?)?;
        if !inventory.describes(&reference.inventory, &reference) {
            return Err("stored inventory does not describe the current publication".into());
        }
        Ok(Some(inventory))
    }

    /// Reserve the next generation durably and capture the fence. Every
    /// publication takes a new generation; one that fails burns it.
    pub fn begin(&self) -> Result<Draft, Error> {
        let mut generation = self.inner.generation.lock().unwrap();
        let next = generation.checked_add(1).ok_or("generation exhausted")?;
        let bytes = serde_json::to_vec(&GenerationRecord { generation: next })?;
        pir_control::durable::write_atomic(&self.inner.root, GENERATION_FILE, |file| {
            file.write_all(&bytes)
        })?;
        pir_control::durable::sync_dir(&self.inner.root)?;
        *generation = next;
        let fence = self.inner.authority.lock().unwrap().current().fence;
        Ok(Draft {
            generation: next,
            recovery_epoch: fence.recovery_epoch,
            latest_withdrawal: fence.latest_withdrawal,
        })
    }

    /// Validate the publication and store its inventory as an object.
    pub fn assemble(&self, draft: Draft, contents: Contents) -> Result<Candidate, Error> {
        let inventory = Inventory {
            generation: draft.generation,
            recovery_epoch: draft.recovery_epoch,
            map_head: contents.map_head,
            map_chunks: contents.map_chunks,
            newest_scope: contents.newest_scope,
            tables: contents.tables,
            subject: contents.subject,
        };
        self.candidate(draft, inventory)
    }

    fn candidate(&self, draft: Draft, inventory: Inventory) -> Result<Candidate, Error> {
        inventory.validate(self.inner.product, &self.inner.limits)?;
        let bytes = serde_json::to_vec(&inventory)?;
        if bytes.len() as u64 > INVENTORY_LIMIT {
            return Err("inventory exceeds its bound".into());
        }
        let object = self.inner.objects.store(&bytes)?;
        let reference = inventory.reference(object.clone());
        let mut objects = inventory.objects();
        objects.push(object);
        Ok(Candidate {
            draft,
            inventory,
            reference,
            objects,
        })
    }

    /// Make `candidate` current: refuse it if the fence moved or a newer
    /// generation is current, retain its objects (and the displaced
    /// publication's for the retention window, collecting everything else),
    /// then publish. An uncertain authority recovers first and publishes the
    /// candidate again under the new epoch with a re-issued map head.
    pub fn commit(&self, candidate: Candidate) -> Result<Committed, Error> {
        let mut authority = self.inner.authority.lock().unwrap();
        let current = authority.current();
        if current.fence.recovery_epoch != candidate.draft.recovery_epoch
            || current.fence.latest_withdrawal != candidate.draft.latest_withdrawal
        {
            return Err(CommitRefusal::FenceMoved.into());
        }
        if current
            .publication
            .as_ref()
            .is_some_and(|p| p.descriptor.generation >= candidate.draft.generation)
        {
            return Err(CommitRefusal::Superseded.into());
        }
        self.retain(&candidate.objects)?;
        let published =
            match authority.apply(AuthorityTransition::Publish(candidate.reference.clone())) {
                Err(PublicationAuthorityError::RecoveryRequired) => {
                    // A previous write may have reached disk: recover with a
                    // durable negative fence, then publish under the new epoch.
                    authority.apply(AuthorityTransition::Recover)?;
                    let recovery_epoch = authority.current().fence.recovery_epoch;
                    let candidate = self.reissue(candidate, recovery_epoch)?;
                    self.retain(&candidate.objects)?;
                    authority
                        .apply(AuthorityTransition::Publish(candidate.reference))?
                        .clone()
                }
                result => result?.clone(),
            };
        self.inner.restorable.store(false, Ordering::SeqCst);
        let reference = published.publication.expect("a publication was published");
        Ok(Committed {
            sequence: published.sequence,
            generation: reference.descriptor.generation,
            recovery_epoch: reference.descriptor.recovery_epoch,
            reference,
        })
    }

    /// [`Self::assemble`] then [`Self::commit`].
    pub fn publish(&self, draft: Draft, contents: Contents) -> Result<Committed, Error> {
        self.commit(self.assemble(draft, contents)?)
    }

    fn reissue(&self, candidate: Candidate, recovery_epoch: u64) -> Result<Candidate, Error> {
        let head = self.read_installed(&candidate.inventory.map_head)?;
        let head =
            self.inner
                .hooks
                .reissue_head(&head, candidate.draft.generation, recovery_epoch)?;
        let map_head = self.install(&head)?;
        let draft = Draft {
            recovery_epoch,
            ..candidate.draft
        };
        self.candidate(
            draft,
            Inventory {
                recovery_epoch,
                map_head,
                ..candidate.inventory
            },
        )
    }

    fn read_installed(&self, object: &ObjectReference) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        open_object_file(&self.object_path(object)?, Some(object.bytes))?
            .read_to_end(&mut bytes)?;
        object.verify(&bytes)?;
        Ok(bytes)
    }

    fn object_path(&self, object: &ObjectReference) -> Result<PathBuf, Error> {
        if !pir_publication::is_object_digest(&object.sha256) {
            return Err("not an object digest".into());
        }
        Ok(self.inner.root.join("objects").join(&object.sha256))
    }

    /// Retention is established before references become visible. Called
    /// with the authority held, so no other retention collects in between.
    fn retain(&self, objects: &[ObjectReference]) -> Result<(), Error> {
        for object in objects {
            if open_object_file(&self.object_path(object)?, Some(object.bytes)).is_err() {
                return Err(CommitRefusal::MissingObject(object.sha256.clone()).into());
            }
        }
        let now = now_ms();
        self.inner.objects.retain_replacement(
            objects.iter().cloned(),
            now.saturating_add(self.inner.retention_ms),
            now,
        )?;
        Ok(())
    }

    /// Withdraw the current publication (spec 11.4: a rewind inside open
    /// scopes whose kept block is below the anchor). Durable on return;
    /// on error the envelope is already withdrawn in memory and a later
    /// publication recovers first.
    pub fn withdraw(&self) -> Result<(), Error> {
        self.inner.restorable.store(false, Ordering::SeqCst);
        self.inner
            .authority
            .lock()
            .unwrap()
            .apply(AuthorityTransition::Withdraw)?;
        Ok(())
    }

    /// Withdraw if the current publication is eligible and its anchor is above
    /// `kept_height` (`None`: nothing kept). Returns whether it withdrew.
    pub fn withdraw_above(&self, kept_height: Option<u64>) -> Result<bool, Error> {
        if !self.current().eligible {
            return Ok(false);
        }
        let above = match self.current_inventory()?.map(|i| i.subject) {
            Some(Subject::Chain { anchor, .. }) => {
                kept_height.is_none_or(|kept| anchor.height > kept)
            }
            _ => false,
        };
        if above {
            self.withdraw()?;
        }
        Ok(above)
    }

    /// Start a new recovery epoch, withdrawn (spec 11.4: a rewind reaching a
    /// sealed scope). Query hosts revoke every session of the previous epoch
    /// that the next publication does not carry. Durable on return.
    pub fn recover(&self) -> Result<(), Error> {
        self.inner.restorable.store(false, Ordering::SeqCst);
        self.inner
            .authority
            .lock()
            .unwrap()
            .apply(AuthorityTransition::Recover)?;
        Ok(())
    }

    /// After open, publish the previous process's last publication again
    /// under this process's recovery epoch and a new generation, with the map
    /// head re-issued, so query hosts keep serving its tables while the
    /// product rebuilds. Only once per open, before any other transition, and
    /// only if every object is still retained and its subject is still
    /// authoritative. Returns whether it published.
    pub async fn restore(&self) -> Result<bool, Error> {
        if !self.inner.restorable.swap(false, Ordering::SeqCst) {
            return Ok(false);
        }
        let current = self.current();
        let Some(previous) = current.publication.clone().filter(|p| {
            !current.eligible && p.descriptor.recovery_epoch < current.fence.recovery_epoch
        }) else {
            return Ok(false);
        };
        let inventory: Inventory = serde_json::from_slice(&self.read_object(&previous.inventory)?)?;
        if !inventory.describes(&previous.inventory, &previous) {
            return Err("stored inventory does not describe the previous publication".into());
        }
        inventory.validate(self.inner.product, &self.inner.limits)?;
        for object in inventory.objects() {
            if self.inner.objects.open_retained(&object.sha256).is_err() {
                return Ok(false);
            }
        }
        let answers = self
            .inner
            .hooks
            .check_subjects(vec![inventory.subject.clone()])
            .await?;
        if answers != [true] {
            return Ok(false);
        }
        let draft = self.begin()?;
        let head = self.read_object(&inventory.map_head)?;
        let head = self
            .inner
            .hooks
            .reissue_head(&head, draft.generation, draft.recovery_epoch)?;
        let contents = Contents {
            map_head: self.install(&head)?,
            map_chunks: inventory.map_chunks,
            newest_scope: inventory.newest_scope,
            tables: inventory.tables,
            subject: inventory.subject,
        };
        match self.publish(draft, contents) {
            Ok(_) => Ok(true),
            Err(error) if error.is::<CommitRefusal>() => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// `GET current`, `GET objects/:id` and `POST authority/check`, to be
    /// nested at [`Product::mount_path`].
    pub fn router(&self) -> Router {
        Router::new()
            .route("/current", get(current))
            .route("/objects/:id", get(object))
            .route("/authority/check", post(authority_check))
            .with_state(self.clone())
    }

    /// `router` with this distribution nested at its product's prefix.
    pub fn mount(&self, router: Router) -> Router {
        router.nest(self.inner.product.mount_path(), self.router())
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn current(State(s): State<Distribution>, headers: HeaderMap) -> Response {
    let envelope = s.current();
    let token = pir_publication::conditional_token(&envelope);
    let unchanged = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        == Some(token.as_str());
    let mut response = if unchanged {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        Json(envelope).into_response()
    };
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&token).expect("tokens are ASCII"),
    );
    no_store(response)
}

async fn object(State(s): State<Distribution>, Path(id): Path<String>) -> Response {
    let Ok(file) = s.inner.objects.open_retained(&id) else {
        return no_store(StatusCode::NOT_FOUND.into_response());
    };
    let Ok(metadata) = file.metadata() else {
        return no_store(StatusCode::INTERNAL_SERVER_ERROR.into_response());
    };
    let body = Body::from_stream(tokio_util::io::ReaderStream::new(
        tokio::fs::File::from_std(file),
    ));
    let mut response = body.into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(metadata.len()));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(IMMUTABLE));
    response
}

async fn authority_check(State(s): State<Distribution>, body: Bytes) -> Response {
    match check(&s, &body).await {
        Ok(response) => no_store(Json(response).into_response()),
        Err(status) => no_store(status.into_response()),
    }
}

async fn check(
    s: &Distribution,
    body: &[u8],
) -> Result<AuthorityCheckResponse<Subject>, StatusCode> {
    let request: AuthorityCheckRequest<Subject> =
        serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let admit = || {
        request
            .admit(s.inner.authority.lock().unwrap().current())
            .map_err(|refusal| match refusal {
                AuthorityCheckRefusal::MalformedRequest => StatusCode::BAD_REQUEST,
                AuthorityCheckRefusal::AuthorityAdvanced => StatusCode::CONFLICT,
            })
    };
    admit()?;
    let mut blocks = 0usize;
    for subject in &request.subjects {
        subject
            .validate(s.inner.product)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        blocks += subject.weight();
    }
    if blocks > MAX_CHECK_BLOCKS {
        return Err(StatusCode::BAD_REQUEST);
    }
    let verdicts = s
        .inner
        .hooks
        .check_subjects(request.subjects.clone())
        .await
        .map_err(|error| {
            tracing::warn!(product = s.inner.product.name(), %error, "authority subject check failed");
            StatusCode::SERVICE_UNAVAILABLE
        })?;
    if verdicts.len() != request.subjects.len() {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    admit()?;
    Ok(AuthorityCheckResponse {
        authority: request.authority,
        answers: request
            .subjects
            .iter()
            .cloned()
            .zip(verdicts)
            .map(|(subject, still_authoritative)| SubjectAnswer {
                subject,
                still_authoritative,
            })
            .collect(),
    })
}
