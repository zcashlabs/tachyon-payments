//! Publication (spec 9.2, 10.3, 11.3): the registration log at one pinned
//! `log_sequence` becomes one publication through `PublisherExecutor` and the
//! phases `check`, `build`, `store`, `verify`, `assemble`, `recheck` and
//! `commit`.
//!
//! [`PublicationTask`] is the `pir_pipeline::publisher::Publisher` loop the
//! registry runs instead of a chain pass (spec 9.2 Cadence): `acquire` offers
//! new material when the log grew since the last publication (or the last
//! attempt failed), `wait` sleeps until the next registration and then until
//! `publish_ms` after the previous start. Registrations arriving while a
//! publication is built only advance the log sequence the next `acquire`
//! reads: one active and one pending publication. Nothing waits for a
//! consumer (scenario 1).
use crate::{
    core::{
        BuiltScope, Core, Memo, OpenKey, PRODUCT, PublishedTable, chunk_entries, entries_digest,
        figures, open_chunk,
    },
    error::{Error, Result},
    metrics::PublicationFigures,
};
use pir_pipeline::{
    FailurePolicy,
    publisher::{Control, Publication, PublicationInput, Publisher, PublisherExecutor},
    service::Shutdown,
};
use pir_publication::{AuthorityFence, ObjectReference};
use registry_store::REGISTRY_CHUNK_ENTRIES;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tachyon_pir_distribution::{
    Candidate, CommitRefusal, Committed, Contents, Subject, TableObjects,
};
use tachyon_pir_protocol::{
    Bytes32, Entry, MAP_CHUNK_SCOPES, MapChunk, PROFILE, RecoveryEpoch, RegistryMapChunk,
    RegistryMapHead, RegistryScope, ScopeState, SealedChunkRef, map_sha256,
};
use tachyon_pir_table::{DescribedTable, ScopeTables as BuiltTables};
use tokio::sync::watch;

/// One chunk of a snapshot.
pub(crate) struct ScopeWork {
    pub chunk_id: u64,
    pub state: ScopeState,
    pub content: Content,
    /// Set for the open chunk with entries: the key its tables are kept by.
    pub open_key: Option<OpenKey>,
}

pub(crate) enum Content {
    /// Built before from the identical entry list.
    Reused(Arc<BuiltScope>),
    /// To build, from these entries in sequence order.
    Build(Vec<Entry>),
}

/// Everything one publication is made from: the log read at one pinned
/// `log_sequence` (spec 9.2 Consistency).
pub(crate) struct Work {
    pub log_sequence: u64,
    pub scopes: Vec<ScopeWork>,
}

/// Read the log as of `log_sequence`: sealed chunks `0 .. open` (each exactly
/// 3,072 entries; reused once built) and the open chunk `log_sequence / 3072`
/// holding `log_sequence % 3072` entries, possibly none (spec 10.3).
pub(crate) fn snapshot(core: &Core, log_sequence: u64) -> Result<Work> {
    let open = open_chunk(log_sequence);
    let memo = core.memo.lock().unwrap();
    let mut scopes = Vec::with_capacity(open as usize + 1);
    for chunk_id in 0..open {
        let content = match memo.sealed.get(&chunk_id) {
            Some(built) => Content::Reused(built.clone()),
            None => {
                let entries = core.store.chunk_before(chunk_id, log_sequence)?;
                if entries.len() as u64 != REGISTRY_CHUNK_ENTRIES {
                    return Err(Error::other(format!(
                        "sealed chunk {chunk_id} holds {} entries, not {REGISTRY_CHUNK_ENTRIES}",
                        entries.len()
                    )));
                }
                Content::Build(chunk_entries(entries))
            }
        };
        scopes.push(ScopeWork {
            chunk_id,
            state: ScopeState::Sealed,
            content,
            open_key: None,
        });
    }
    let entries = chunk_entries(core.store.chunk_before(open, log_sequence)?);
    let expected = log_sequence % REGISTRY_CHUNK_ENTRIES;
    if entries.len() as u64 != expected {
        return Err(Error::other(format!(
            "open chunk {open} holds {} entries below {log_sequence}, expected {expected}",
            entries.len()
        )));
    }
    let (content, open_key) = if entries.is_empty() {
        (Content::Reused(BuiltScope::empty()), None)
    } else {
        let key = OpenKey {
            chunk_id: open,
            entries: expected,
            digest: entries_digest(open, &entries),
        };
        match &memo.open {
            Some((known, built)) if *known == key => (Content::Reused(built.clone()), Some(key)),
            _ => (Content::Build(entries), Some(key)),
        }
    };
    scopes.push(ScopeWork {
        chunk_id: open,
        state: ScopeState::Open,
        content,
        open_key,
    });
    Ok(Work {
        log_sequence,
        scopes,
    })
}

/// What a committed publication reports.
#[derive(Clone, Debug)]
pub struct Report {
    pub committed: Committed,
    /// The map's `log_sequence`.
    pub log_sequence: u64,
    /// Tables built (not reused) for this publication.
    pub tables_built: usize,
}

/// A chunk built in this publication: its tables (rows in memory until
/// `verify`) and each table's map entry and public setup.
pub struct NewScope {
    position: usize,
    built: BuiltTables,
    described: Vec<DescribedTable>,
}

pub struct Material {
    work: Arc<Work>,
    new: Vec<NewScope>,
}

pub struct Stored {
    work: Arc<Work>,
    new: Vec<(NewScope, Vec<PublishedTable>)>,
}

pub struct Verified {
    work: Arc<Work>,
    scopes: Vec<Arc<BuiltScope>>,
}

pub struct Prepared {
    candidate: Candidate,
    work: Arc<Work>,
    built: Vec<Arc<BuiltScope>>,
    figures: PublicationFigures,
}

/// One publication of the log at a pinned `log_sequence`.
pub struct Export {
    core: Arc<Core>,
    log_sequence: u64,
    fence: AuthorityFence,
    shutdown: Shutdown,
    started: Instant,
    tables_built: usize,
}

impl Export {
    pub(crate) fn new(core: Arc<Core>, log_sequence: u64, shutdown: Shutdown) -> Self {
        let fence = core.distribution.current().fence;
        Self {
            core,
            log_sequence,
            fence,
            shutdown,
            started: Instant::now(),
            tables_built: 0,
        }
    }
}

fn check_installed(object: &ObjectReference, digest: &Bytes32, bytes: u64) -> Result<()> {
    if object.sha256 != digest.hex() || object.bytes != bytes {
        return Err(Error::other(format!(
            "installed object {} does not match the table's digest {digest}",
            object.sha256
        )));
    }
    Ok(())
}

impl Publication for Export {
    type Error = Error;
    type Material = Material;
    type Stored = Stored;
    type Verified = Verified;
    type Candidate = Prepared;
    type Report = Report;

    /// Refuse work a shutdown stopped or a withdrawal or recovery overtook.
    fn check(&self) -> Result<()> {
        if self.shutdown.requested() {
            return Err(Error::other("registry publication stopped by shutdown"));
        }
        if self.core.distribution.current().fence != self.fence {
            return Err(Error::other(
                "registry publication invalidated by a withdrawal or recovery",
            ));
        }
        Ok(())
    }

    /// Read the log at the pinned sequence and build the tables of every
    /// chunk with no built tables yet, with their public setup (spec 6.4,
    /// 6.6).
    async fn build(&mut self) -> Result<Material> {
        let core = self.core.clone();
        let log_sequence = self.log_sequence;
        let material = tokio::task::spawn_blocking(move || -> Result<Material> {
            let work = Arc::new(snapshot(&core, log_sequence)?);
            let genesis = core.genesis();
            let mut new = Vec::new();
            for (position, scope) in work.scopes.iter().enumerate() {
                let Content::Build(entries) = &scope.content else {
                    continue;
                };
                let built = tachyon_pir_table::build_scope(PRODUCT, &genesis, scope.chunk_id, entries)
                    .map_err(|error| {
                        tracing::error!(chunk = scope.chunk_id, %error, "registry chunk does not build; the previous publication keeps serving");
                        Error::other(error)
                    })?;
                let described = built
                    .tables
                    .iter()
                    .map(|table| tachyon_pir_table::describe(PRODUCT, scope.chunk_id, table))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Error::other)?;
                new.push(NewScope {
                    position,
                    built,
                    described,
                });
            }
            Ok(Material { work, new })
        })
        .await
        .map_err(Error::other)??;
        self.tables_built = material.new.iter().map(|s| s.described.len()).sum();
        self.core
            .stats
            .tables_built
            .fetch_add(self.tables_built as u64, Ordering::Relaxed);
        Ok(material)
    }

    /// Install every new table's rows and public setup as objects. Nothing is
    /// served until commit.
    async fn store(&mut self, material: Material) -> Result<Stored> {
        let distribution = self.core.distribution.clone();
        tokio::task::spawn_blocking(move || -> Result<Stored> {
            let Material { work, new: scopes } = material;
            let mut new = Vec::with_capacity(scopes.len());
            for scope in scopes {
                let scope_index = work.scopes[scope.position].chunk_id;
                let mut tables = Vec::with_capacity(scope.described.len());
                for (built, described) in scope.built.tables.iter().zip(&scope.described) {
                    let rows = distribution.install(&built.rows)?;
                    let public = distribution.install(&described.public)?;
                    tables.push(PublishedTable {
                        table: described.table.clone(),
                        objects: TableObjects {
                            scope_index,
                            session_id: described.table.session_id.0,
                            rows,
                            public,
                        },
                    });
                }
                new.push((scope, tables));
            }
            Ok(Stored { work, new })
        })
        .await
        .map_err(Error::other)?
    }

    /// Validate every row of every new table (spec 6.2) and the table rules,
    /// the entry counts against the log, digests and session ids against the
    /// bytes, each installed object against its map entry, and the whole map
    /// listing as a client checks it (spec 10.3).
    async fn verify(&mut self, stored: Stored) -> Result<Verified> {
        let genesis = self.core.genesis();
        let Stored { work, new } = stored;
        let checked = work.clone();
        let new = tokio::task::spawn_blocking(move || -> Result<Vec<(usize, Arc<BuiltScope>)>> {
            let mut verified = Vec::with_capacity(new.len());
            for (scope, tables) in new {
                let meta = &checked.scopes[scope.position];
                let count = tachyon_pir_table::validate_scope(
                    PRODUCT,
                    &genesis,
                    meta.chunk_id,
                    None,
                    &scope.built,
                )
                .map_err(Error::other)?;
                let Content::Build(entries) = &meta.content else {
                    return Err(Error::other("a built chunk has no entries"));
                };
                if count != entries.len() as u64 {
                    return Err(Error::other(format!(
                        "chunk {}: tables hold {count} entries, the log {}",
                        meta.chunk_id,
                        entries.len()
                    )));
                }
                if scope.built.n_buckets != 1 {
                    return Err(Error::other(format!(
                        "chunk {}: {} buckets, the registry has one",
                        meta.chunk_id, scope.built.n_buckets
                    )));
                }
                for ((built, described), published) in
                    scope.built.tables.iter().zip(&scope.described).zip(&tables)
                {
                    let table = &published.table;
                    if table.rows_sha256.0 != built.rows_sha256
                        || tachyon_pir_table::rows_sha256(&built.rows) != built.rows_sha256
                        || tachyon_pir_table::public_sha256(&described.public)
                            != table.public_sha256.0
                        || table.session_id != table.expected_session_id(PRODUCT, meta.chunk_id)
                    {
                        return Err(Error::other(format!(
                            "chunk {}: table digests or session id do not match its bytes",
                            meta.chunk_id
                        )));
                    }
                    check_installed(
                        &published.objects.rows,
                        &table.rows_sha256,
                        built.rows.len() as u64,
                    )?;
                    check_installed(
                        &published.objects.public,
                        &table.public_sha256,
                        described.public.len() as u64,
                    )?;
                }
                verified.push((
                    scope.position,
                    Arc::new(BuiltScope {
                        entries: count,
                        n_buckets: scope.built.n_buckets,
                        tables,
                    }),
                ));
            }
            Ok(verified)
        })
        .await
        .map_err(Error::other)??;
        let mut new: HashMap<usize, Arc<BuiltScope>> = new.into_iter().collect();
        let scopes = work
            .scopes
            .iter()
            .enumerate()
            .map(|(position, scope)| match &scope.content {
                Content::Reused(built) => Ok(built.clone()),
                Content::Build(_) => new
                    .remove(&position)
                    .ok_or_else(|| Error::other("a chunk to build was not built")),
            })
            .collect::<Result<Vec<_>>>()?;
        // The listing a client will check, under a provisional generation.
        let (head, chunks) = self.listing(&work, &scopes, 1, self.fence.recovery_epoch)?;
        head.check_listing(&chunks).map_err(Error::other)?;
        Ok(Verified { work, scopes })
    }

    /// Reserve the generation, install the map chunks and head, and store the
    /// inventory with the subject `log_sequence` (spec 11.3).
    async fn assemble(&mut self, verified: Verified) -> Result<Prepared> {
        let distribution = &self.core.distribution;
        let draft = distribution.begin()?;
        let Verified { work, scopes } = verified;
        let (head, chunks) =
            self.listing(&work, &scopes, draft.generation, draft.recovery_epoch)?;
        let mut map_chunks = Vec::with_capacity(chunks.len());
        for chunk in &chunks {
            map_chunks.push(distribution.install(&chunk.to_bytes())?);
        }
        let map_head = distribution.install(&head.to_bytes().map_err(Error::other)?)?;
        let tables = scopes
            .iter()
            .flat_map(|scope| scope.tables.iter().map(|t| t.objects.clone()))
            .collect();
        let contents = Contents {
            map_head,
            map_chunks,
            newest_scope: open_chunk(work.log_sequence),
            tables,
            subject: Subject::Registry {
                log_sequence: work.log_sequence,
            },
        };
        let candidate = distribution.assemble(draft, contents)?;
        let figures = figures(
            chunks
                .iter()
                .flat_map(|c| c.scopes.iter())
                .chain(head.open_scopes.iter()),
        );
        Ok(Prepared {
            candidate,
            work,
            built: scopes,
            figures,
        })
    }

    /// The log still begins with the publication's entries: it holds at
    /// least `log_sequence` of them (it is append-only).
    async fn recheck(&mut self, candidate: &mut Prepared) -> Result<()> {
        let store = self.core.store.clone();
        let stored = tokio::task::spawn_blocking(move || store.log_sequence())
            .await
            .map_err(Error::other)??;
        if stored < candidate.work.log_sequence {
            return Err(Error::other(format!(
                "the log holds {stored} entries, fewer than the publication's {}",
                candidate.work.log_sequence
            )));
        }
        Ok(())
    }

    /// Commit, then keep the built chunks this publication carries for the
    /// next one.
    async fn commit(self, prepared: Prepared) -> Result<Report> {
        let core = &self.core;
        let committed = match core.distribution.commit(prepared.candidate) {
            Ok(committed) => committed,
            Err(error) => {
                // A reused table whose objects were collected: forget every
                // reused table, so the next attempt rebuilds what it needs.
                if matches!(
                    error.downcast_ref::<CommitRefusal>(),
                    Some(CommitRefusal::MissingObject(_))
                ) {
                    *core.memo.lock().unwrap() = Memo::default();
                }
                return Err(Error::Other(error));
            }
        };
        {
            let mut memo = core.memo.lock().unwrap();
            let mut sealed = HashMap::new();
            let mut open = None;
            for (work, built) in prepared.work.scopes.iter().zip(&prepared.built) {
                match work.state {
                    ScopeState::Sealed => {
                        sealed.insert(work.chunk_id, built.clone());
                    }
                    ScopeState::Open => {
                        open = work.open_key.map(|key| (key, built.clone()));
                    }
                }
            }
            memo.sealed = sealed;
            memo.open = open;
        }
        let log_sequence = prepared.work.log_sequence;
        *core.served.lock().unwrap() = Some(log_sequence);
        core.stats.publications.fetch_add(1, Ordering::Relaxed);
        core.metrics
            .published(prepared.figures, Some(self.started.elapsed().as_secs_f64()));
        tracing::info!(
            generation = committed.generation,
            recovery_epoch = committed.recovery_epoch,
            log_sequence,
            tables_built = self.tables_built,
            "registry publication committed"
        );
        Ok(Report {
            committed,
            log_sequence,
            tables_built: self.tables_built,
        })
    }
}

impl Export {
    /// The map head and sealed map chunks of `scopes` (spec 10.3): fields in
    /// the registry head's order, `floor_scope` 0, exactly one open chunk.
    fn listing(
        &self,
        work: &Work,
        scopes: &[Arc<BuiltScope>],
        generation: u64,
        recovery_epoch: u64,
    ) -> Result<(RegistryMapHead, Vec<RegistryMapChunk>)> {
        let mut sealed = Vec::new();
        let mut open = Vec::new();
        for (meta, built) in work.scopes.iter().zip(scopes) {
            let scope = RegistryScope {
                scope_id: meta.chunk_id,
                state: meta.state,
                entries: built.entries,
                n_buckets: built.n_buckets,
                tables: built.tables.iter().map(|t| t.table.clone()).collect(),
            };
            match scope.state {
                ScopeState::Sealed if open.is_empty() => sealed.push(scope),
                ScopeState::Sealed => {
                    return Err(Error::other("a sealed chunk follows the open one"));
                }
                ScopeState::Open => open.push(scope),
            }
        }
        let chunks: Vec<RegistryMapChunk> = sealed
            .chunks(MAP_CHUNK_SCOPES as usize)
            .enumerate()
            .map(|(k, scopes)| MapChunk {
                protocol: PRODUCT.protocol().into(),
                base: k as u64 * MAP_CHUNK_SCOPES,
                scopes: scopes.to_vec(),
            })
            .collect();
        let head = RegistryMapHead {
            protocol: PRODUCT.protocol().into(),
            profile: PROFILE.into(),
            network: self.core.identity.chain.clone(),
            genesis_hash: self.core.genesis(),
            generation,
            recovery_epoch: RecoveryEpoch::new(recovery_epoch)
                .ok_or_else(|| Error::other("recovery epoch zero"))?,
            log_sequence: work.log_sequence,
            floor_scope: 0,
            sealed_chunks: chunks
                .iter()
                .map(|chunk| SealedChunkRef {
                    base: chunk.base,
                    sha256: map_sha256(&chunk.to_bytes()),
                })
                .collect(),
            open_scopes: open,
        };
        Ok((head, chunks))
    }
}

/// The registry's publication loop. Run it with `PublisherExecutor::run`;
/// tests may drive it with [`PublicationTask::publish_pending`].
pub struct PublicationTask {
    pub(crate) core: Arc<Core>,
    pub(crate) log: watch::Receiver<u64>,
    pub(crate) shutdown: Shutdown,
    pub(crate) interval: Duration,
    /// Restore the previous process's publication before anything else.
    pub(crate) restore: bool,
    pub(crate) failed: bool,
    pub(crate) closed: bool,
    pub(crate) last_start: Option<Instant>,
}

/// Least delay before retrying a failed publication.
const RETRY_DELAY: Duration = Duration::from_secs(1);

impl PublicationTask {
    /// Once, before the first publication: republish the previous process's
    /// last publication under this process's recovery epoch (spec 9.2
    /// Restart, 11.4).
    async fn restore_once(&mut self) {
        if !std::mem::take(&mut self.restore) {
            return;
        }
        match self.core.distribution.restore().await {
            Ok(true) => match crate::core::Loaded::read(&self.core.distribution) {
                Ok(Some(loaded)) => {
                    *self.core.served.lock().unwrap() = Some(loaded.head.log_sequence);
                    self.core.metrics.published(loaded.figures(), None);
                    tracing::info!(
                        log_sequence = loaded.head.log_sequence,
                        "registry restored its previous publication"
                    );
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(%error, "registry restored publication unreadable"),
            },
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(%error, "registry previous publication not restored")
            }
        }
    }

    /// Whether the log or the envelope calls for a publication.
    fn due(&self, log_sequence: u64) -> bool {
        self.failed
            || *self.core.served.lock().unwrap() != Some(log_sequence)
            || !self.core.distribution.current().eligible
    }

    async fn take(&mut self) -> Result<Option<Export>> {
        self.restore_once().await;
        self.log.borrow_and_update();
        let store = self.core.store.clone();
        let log_sequence = tokio::task::spawn_blocking(move || store.log_sequence())
            .await
            .map_err(Error::other)??;
        self.core.grew(log_sequence);
        if !self.due(log_sequence) {
            return Ok(None);
        }
        self.failed = false;
        self.last_start = Some(Instant::now());
        Ok(Some(Export::new(
            self.core.clone(),
            log_sequence,
            self.shutdown.clone(),
        )))
    }

    fn mark_failed(&mut self, error: &dyn std::fmt::Display) {
        tracing::warn!(%error, "registry publication failed; the previous publication keeps serving");
        self.failed = true;
    }

    /// Restore (first call only), then publish the log if it grew since the
    /// last publication. `Ok(None)` when nothing was due.
    pub async fn publish_pending<E: PublisherExecutor>(
        &mut self,
        executor: &E,
    ) -> Result<Option<Report>> {
        let Some(export) = self.take().await? else {
            return Ok(None);
        };
        match executor.publish(export).await {
            Ok(report) => Ok(Some(report)),
            Err(error) => {
                self.mark_failed(&error);
                Err(error)
            }
        }
    }
}

impl Publisher for PublicationTask {
    type Error = Error;
    type Report = Report;
    type Publication<'a> = Export;

    async fn acquire(&mut self) -> Result<PublicationInput<Export>> {
        Ok(match self.take().await? {
            Some(export) => PublicationInput::NewMaterial(export),
            None => PublicationInput::Unchanged,
        })
    }

    fn failed(&mut self, error: &Error) -> FailurePolicy {
        self.mark_failed(error);
        error.policy()
    }

    fn settled(&mut self) -> Result<Control> {
        Ok(if self.closed {
            Control::Complete
        } else {
            Control::Continue
        })
    }

    /// Until a registration grew the log past the last publication (or a
    /// failed attempt is to be retried), then until `publish_ms` after the
    /// last start. Cancel-safe.
    async fn wait(&mut self) -> Result<()> {
        loop {
            let known = *self.log.borrow_and_update();
            if self.due(known) {
                break;
            }
            if self.log.changed().await.is_err() {
                self.closed = true;
                return Ok(());
            }
        }
        let spacing = if self.failed {
            self.interval.max(RETRY_DELAY)
        } else {
            self.interval
        };
        if let Some(start) = self.last_start {
            tokio::time::sleep_until((start + spacing).into()).await;
        }
        Ok(())
    }
}
