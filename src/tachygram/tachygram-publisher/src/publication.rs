//! Publication (spec 6.5, 10.3, 11.3): one snapshot of the journal, taken by
//! `caught_up`, becomes one publication through `PublisherExecutor::publish`
//! and the phases `check`, `build`, `store`, `verify`, `assemble`, `recheck`
//! and `commit`.
//!
//! Coalescing: `caught_up` puts its snapshot in a one-slot watch channel and
//! returns, so ingestion never waits for preparation. One task publishes the
//! latest snapshot at a time; snapshots offered meanwhile replace each other
//! in the slot. That is one active and one pending publication (spec 11.3).
//! Nothing in either path waits for a consumer (scenario 1).
use crate::{
    core::{
        BuiltScope, Core, Memo, OfferKey, PRODUCT, PublishedTable, block_ref, entries_digest,
        epoch_entries, figures,
    },
    error::{Error, Result},
};
use pir_pipeline::{
    FailurePolicy,
    publisher::{Publication, PublicationInput, Publisher, PublisherExecutor},
    service::Shutdown,
};
use pir_publication::ObjectReference;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
use tachygram_store::Store;
use tachyon_chain::{BlockId, Hash};
use tachyon_pir_distribution::{
    Candidate, CommitRefusal, Committed, Contents, Subject, TableObjects,
};
use tachyon_pir_protocol::{
    BlockHash, Bytes32, ChainMapChunk, ChainMapHead, Entry, MAP_CHUNK_SCOPES, MapChunk, PROFILE,
    RecoveryEpoch, Scope, ScopeState, SealedChunkRef, map_sha256,
};
use tachyon_pir_table::{DescribedTable, ScopeTables as BuiltTables};
use tokio::sync::watch;

/// One scope of a snapshot.
pub(crate) struct ScopeWork {
    pub epoch: u32,
    pub state: ScopeState,
    pub first_height: u32,
    /// The map's `last_height`: the anchor for the newest scope, the epoch's
    /// last block for every older one (spec 10.3).
    pub last_height: u32,
    /// The journal's hash at `last_height`.
    pub last_block: Hash,
    pub digest: [u8; 32],
    pub content: Content,
}

pub(crate) enum Content {
    /// Built before from the identical entry list.
    Reused(Arc<BuiltScope>),
    /// To build, from these entries in ordinal order.
    Build(Vec<Entry>),
}

/// Everything one publication is made from, read from the journal in one
/// writer operation.
pub(crate) struct Work {
    pub key: OfferKey,
    pub scopes: Vec<ScopeWork>,
}

impl Work {
    pub fn anchor(&self) -> BlockId {
        self.key.anchor
    }
}

/// Read the journal at its tip into a snapshot. Every epoch from 0 to the
/// anchor's must be stored from its first block (spec 7.3), or nothing is
/// published.
pub(crate) fn snapshot(core: &Core, store: &Store, key: OfferKey) -> Result<Work> {
    let anchor = key.anchor;
    let newest = core
        .schedule
        .epoch_of(anchor.height)
        .ok_or_else(|| Error::other("the journal tip is below the activation height"))?;
    let memo = core.memo.lock().unwrap();
    let mut scopes = Vec::with_capacity(newest as usize + 1);
    for epoch in 0..=newest {
        let (first_height, epoch_last) = core.epoch_bounds(epoch)?;
        let summary = store
            .epoch_summary(epoch)?
            .ok_or_else(|| Error::other(format!("epoch {epoch} has no stored block")))?;
        if summary.first_height != first_height {
            return Err(Error::other(format!(
                "epoch {epoch} is stored from {}, not its first height {first_height}",
                summary.first_height
            )));
        }
        let last_height = if epoch == newest {
            anchor.height
        } else {
            epoch_last
        };
        if summary.last_height != last_height {
            return Err(Error::other(format!(
                "epoch {epoch} is stored to {}, expected {last_height}",
                summary.last_height
            )));
        }
        let last_block = store
            .block_hash(last_height)?
            .ok_or_else(|| Error::other(format!("no stored block at {last_height}")))?;
        let state = if core.sealed(epoch, anchor.height) {
            ScopeState::Sealed
        } else {
            ScopeState::Open
        };
        let known = (state == ScopeState::Sealed)
            .then(|| memo.sealed.get(&(epoch, last_block)))
            .flatten()
            .and_then(|digest| {
                memo.scopes
                    .get(&(epoch, *digest))
                    .map(|built| (*digest, built.clone()))
            });
        let (digest, content) = match known {
            Some((digest, built)) => (digest, Content::Reused(built)),
            None if summary.entries == 0 => ([0; 32], Content::Reused(BuiltScope::empty())),
            None => {
                let entries = epoch_entries(store, epoch)?;
                if entries.len() as u64 != summary.entries {
                    return Err(Error::other(format!(
                        "epoch {epoch} lists {} entries, its summary {}",
                        entries.len(),
                        summary.entries
                    )));
                }
                let digest = entries_digest(epoch, &entries);
                match memo.scopes.get(&(epoch, digest)) {
                    Some(built) => (digest, Content::Reused(built.clone())),
                    None => (
                        digest,
                        Content::Build(entries.into_iter().map(Entry::Tachygram).collect()),
                    ),
                }
            }
        };
        scopes.push(ScopeWork {
            epoch,
            state,
            first_height,
            last_height,
            last_block,
            digest,
            content,
        });
    }
    Ok(Work { key, scopes })
}

/// What a committed publication reports.
#[derive(Clone, Debug)]
pub struct Report {
    pub committed: Committed,
    pub anchor: BlockId,
    /// Tables built (not reused) for this publication.
    pub tables_built: usize,
}

/// A scope built in this publication: the built tables (rows in memory until
/// `verify`) and each table's map entry and public setup.
pub struct NewScope {
    position: usize,
    built: BuiltTables,
    described: Vec<DescribedTable>,
}

pub struct Stored {
    new: Vec<(NewScope, Vec<PublishedTable>)>,
}

pub struct Verified {
    scopes: Vec<Arc<BuiltScope>>,
}

pub struct Prepared {
    candidate: Candidate,
    built: Vec<Arc<BuiltScope>>,
    figures: crate::metrics::PublicationFigures,
}

/// One publication of one snapshot.
pub struct Export {
    core: Arc<Core>,
    work: Arc<Work>,
    shutdown: Shutdown,
    started: Instant,
    tables_built: usize,
}

impl Export {
    pub(crate) fn new(core: Arc<Core>, work: Arc<Work>, shutdown: Shutdown) -> Self {
        Self {
            core,
            work,
            shutdown,
            started: Instant::now(),
            tables_built: 0,
        }
    }

    fn scope(&self, position: usize, built: &BuiltScope) -> Scope {
        let work = &self.work.scopes[position];
        Scope {
            scope_id: u64::from(work.epoch),
            state: work.state,
            first_height: work.first_height,
            last_height: work.last_height,
            entries: built.entries,
            n_buckets: built.n_buckets,
            tables: built.tables.iter().map(|t| t.table.clone()).collect(),
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
    type Material = Vec<NewScope>;
    type Stored = Stored;
    type Verified = Verified;
    type Candidate = Prepared;
    type Report = Report;

    /// Refuse work whose snapshot a withdrawal, recovery or rewind has
    /// overtaken, or that a shutdown stopped.
    fn check(&self) -> Result<()> {
        if self.shutdown.requested() {
            return Err(Error::other("tachygram publication stopped by shutdown"));
        }
        if self.core.distribution.current().fence != self.work.key.fence
            || self.core.rewinds() != self.work.key.rewinds
        {
            return Err(Error::other(
                "tachygram publication invalidated by a withdrawal or rewind",
            ));
        }
        Ok(())
    }

    /// Build the tables of every scope whose entry list has no built tables
    /// yet, and their public setup (spec 6.4, 6.6).
    async fn build(&mut self) -> Result<Vec<NewScope>> {
        let work = self.work.clone();
        let genesis = self.core.genesis();
        let new = tokio::task::spawn_blocking(move || -> Result<Vec<NewScope>> {
            let mut new = Vec::new();
            for (position, scope) in work.scopes.iter().enumerate() {
                let Content::Build(entries) = &scope.content else {
                    continue;
                };
                let scope_id = u64::from(scope.epoch);
                let built = tachyon_pir_table::build_scope(PRODUCT, &genesis, scope_id, entries)
                    .map_err(|error| {
                        tracing::error!(epoch = scope.epoch, %error, "tachygram scope does not build; the previous publication keeps serving");
                        Error::other(error)
                    })?;
                let described = built
                    .tables
                    .iter()
                    .map(|table| tachyon_pir_table::describe(PRODUCT, scope_id, table))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Error::other)?;
                new.push(NewScope {
                    position,
                    built,
                    described,
                });
            }
            Ok(new)
        })
        .await
        .map_err(Error::other)??;
        self.tables_built = new.iter().map(|scope| scope.described.len()).sum();
        self.core
            .stats
            .tables_built
            .fetch_add(self.tables_built as u64, Ordering::Relaxed);
        Ok(new)
    }

    /// Install every new table's rows and public setup as objects. Nothing is
    /// served until commit.
    async fn store(&mut self, material: Vec<NewScope>) -> Result<Stored> {
        let distribution = self.core.distribution.clone();
        let epochs: Vec<u64> = material
            .iter()
            .map(|scope| u64::from(self.work.scopes[scope.position].epoch))
            .collect();
        tokio::task::spawn_blocking(move || -> Result<Stored> {
            let mut new = Vec::with_capacity(material.len());
            for (scope, scope_index) in material.into_iter().zip(epochs) {
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
            Ok(Stored { new })
        })
        .await
        .map_err(Error::other)?
    }

    /// Validate every row of every new table (spec 6.2, 8.4) and the table
    /// rules, check that each installed object is exactly the bytes its map
    /// entry names, and check the whole map listing as a client would
    /// (spec 10.3).
    async fn verify(&mut self, stored: Stored) -> Result<Verified> {
        let work = self.work.clone();
        let genesis = self.core.genesis();
        let new = tokio::task::spawn_blocking(move || -> Result<Vec<(usize, Arc<BuiltScope>)>> {
            let mut verified = Vec::with_capacity(stored.new.len());
            for (scope, tables) in stored.new {
                let meta = &work.scopes[scope.position];
                let scope_id = u64::from(meta.epoch);
                let count = tachyon_pir_table::validate_scope(
                    PRODUCT,
                    &genesis,
                    scope_id,
                    Some((meta.first_height, meta.last_height)),
                    &scope.built,
                )
                .map_err(Error::other)?;
                let Content::Build(entries) = &meta.content else {
                    return Err(Error::other("a built scope has no entries"));
                };
                if count != entries.len() as u64 {
                    return Err(Error::other(format!(
                        "epoch {}: tables hold {count} entries, the journal {}",
                        meta.epoch,
                        entries.len()
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
                        || table.session_id != table.expected_session_id(PRODUCT, scope_id)
                    {
                        return Err(Error::other(format!(
                            "epoch {}: table digests or session id do not match its bytes",
                            meta.epoch
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
        let scopes = self
            .work
            .scopes
            .iter()
            .enumerate()
            .map(|(position, scope)| match &scope.content {
                Content::Reused(built) => Ok(built.clone()),
                Content::Build(_) => new
                    .remove(&position)
                    .ok_or_else(|| Error::other("a scope to build was not built")),
            })
            .collect::<Result<Vec<_>>>()?;
        // The listing a client will check, under a provisional generation.
        let (head, chunks) = self.listing(&scopes, 1, self.work.key.fence.recovery_epoch)?;
        head.check_listing(PRODUCT, &chunks).map_err(Error::other)?;
        Ok(Verified { scopes })
    }

    /// Reserve the generation, install the map chunks and head, and store the
    /// inventory.
    async fn assemble(&mut self, verified: Verified) -> Result<Prepared> {
        let distribution = &self.core.distribution;
        let draft = distribution.begin()?;
        let (head, chunks) =
            self.listing(&verified.scopes, draft.generation, draft.recovery_epoch)?;
        let mut map_chunks = Vec::with_capacity(chunks.len());
        for chunk in &chunks {
            map_chunks.push(distribution.install(&chunk.to_bytes())?);
        }
        let map_head = distribution.install(&head.to_bytes().map_err(Error::other)?)?;
        let anchor = block_ref(&self.work.anchor());
        let open_scopes = self
            .work
            .scopes
            .iter()
            .filter(|scope| scope.state == ScopeState::Open)
            .map(|scope| tachyon_pir_distribution::BlockRef {
                height: u64::from(scope.last_height),
                hash: *scope.last_block.protocol_bytes(),
            })
            .collect();
        let tables = verified
            .scopes
            .iter()
            .flat_map(|scope| scope.tables.iter().map(|t| t.objects.clone()))
            .collect();
        let contents = Contents {
            map_head,
            map_chunks,
            newest_scope: u64::from(self.work.scopes.last().map_or(0, |s| s.epoch)),
            tables,
            subject: Subject::Chain {
                anchor,
                open_scopes,
            },
        };
        let candidate = distribution.assemble(draft, contents)?;
        let figures = figures(
            self.work.anchor().height,
            chunks
                .iter()
                .flat_map(|c| c.scopes.iter())
                .chain(head.open_scopes.iter()),
        );
        Ok(Prepared {
            candidate,
            built: verified.scopes,
            figures,
        })
    }

    /// The network is still the one bound, and the anchor (hence every open
    /// scope's last block, its ancestor) is still the node's block.
    async fn recheck(&mut self, _candidate: &mut Prepared) -> Result<()> {
        let identity = self.core.node.network_identity().await?;
        if identity != self.core.identity {
            return Err(Error::Network(format!(
                "the node now reports {identity:?}, not {:?}",
                self.core.identity
            )));
        }
        let anchor = self.work.anchor();
        if self.core.node.block_hash(anchor.height).await? != anchor.hash {
            return Err(Error::other(format!(
                "anchor {} {} is no longer canonical",
                anchor.height, anchor.hash
            )));
        }
        Ok(())
    }

    /// Commit under the rewind gate, then keep the built scopes this
    /// publication carries for the next one.
    async fn commit(self, prepared: Prepared) -> Result<Report> {
        let core = &self.core;
        let committed = {
            let rewinds = core.rewinds.lock().unwrap();
            if *rewinds != self.work.key.rewinds {
                return Err(Error::other("tachygram publication overtaken by a rewind"));
            }
            match core.distribution.commit(prepared.candidate) {
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
            }
        };
        {
            let mut memo = core.memo.lock().unwrap();
            let mut scopes = HashMap::new();
            let mut sealed = HashMap::new();
            for (work, built) in self.work.scopes.iter().zip(&prepared.built) {
                if built.entries == 0 {
                    continue;
                }
                scopes.insert((work.epoch, work.digest), built.clone());
                if work.state == ScopeState::Sealed {
                    sealed.insert((work.epoch, work.last_block), work.digest);
                }
            }
            memo.scopes = scopes;
            memo.sealed = sealed;
        }
        *core.served.lock().unwrap() = Some(block_ref(&self.work.anchor()));
        core.stats.publications.fetch_add(1, Ordering::Relaxed);
        core.metrics
            .published(prepared.figures, Some(self.started.elapsed().as_secs_f64()));
        tracing::info!(
            generation = committed.generation,
            recovery_epoch = committed.recovery_epoch,
            anchor = self.work.anchor().height,
            tables_built = self.tables_built,
            "tachygram publication committed"
        );
        Ok(Report {
            committed,
            anchor: self.work.anchor(),
            tables_built: self.tables_built,
        })
    }
}

impl Export {
    /// The map head and sealed chunks of `scopes` (spec 10.3).
    fn listing(
        &self,
        scopes: &[Arc<BuiltScope>],
        generation: u64,
        recovery_epoch: u64,
    ) -> Result<(ChainMapHead, Vec<ChainMapChunk>)> {
        let mut sealed = Vec::new();
        let mut open = Vec::new();
        for (position, built) in scopes.iter().enumerate() {
            let scope = self.scope(position, built);
            match scope.state {
                ScopeState::Sealed if open.is_empty() => sealed.push(scope),
                ScopeState::Sealed => {
                    return Err(Error::other("a sealed scope follows an open one"));
                }
                ScopeState::Open => open.push(scope),
            }
        }
        let chunks: Vec<ChainMapChunk> = sealed
            .chunks(MAP_CHUNK_SCOPES as usize)
            .enumerate()
            .map(|(k, scopes)| MapChunk {
                protocol: PRODUCT.protocol().into(),
                base: k as u64 * MAP_CHUNK_SCOPES,
                scopes: scopes.to_vec(),
            })
            .collect();
        let anchor = self.work.anchor();
        let identity = &self.core.identity;
        let head = ChainMapHead {
            protocol: PRODUCT.protocol().into(),
            profile: PROFILE.into(),
            network: identity.chain.clone(),
            genesis_hash: self.core.genesis(),
            activation_height: identity.activation_height,
            epoch_length: self.core.schedule.epoch_length(),
            generation,
            recovery_epoch: RecoveryEpoch::new(recovery_epoch)
                .ok_or_else(|| Error::other("recovery epoch zero"))?,
            anchor_height: anchor.height,
            anchor_hash: BlockHash::from_protocol_bytes(*anchor.hash.protocol_bytes()),
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

/// The publication task: publishes the latest offered snapshot, one at a
/// time. Run it with `PublisherExecutor::run`; tests call
/// [`PublicationTask::publish_pending`].
pub struct PublicationTask {
    pub(crate) core: Arc<Core>,
    pub(crate) pending: watch::Receiver<Option<Arc<Work>>>,
    pub(crate) shutdown: Shutdown,
    pub(crate) closed: bool,
    pub(crate) woken: bool,
    pub(crate) attempt: Option<OfferKey>,
}

impl PublicationTask {
    /// The snapshot offered since the last take, if any.
    fn take(&mut self) -> Option<Arc<Work>> {
        // `wait` already marked the change it woke for as seen.
        let woken = std::mem::take(&mut self.woken);
        if !woken && !self.pending.has_changed().unwrap_or(false) {
            return None;
        }
        let work = self.pending.borrow_and_update().clone();
        self.attempt = work.as_ref().map(|work| work.key);
        work
    }

    fn mark_failed(&self, error: &dyn std::fmt::Display) {
        tracing::warn!(%error, "tachygram publication failed; the previous publication keeps serving");
        let mut offer = self.core.offer.lock().unwrap();
        if offer.key == self.attempt {
            offer.failed = true;
        }
    }

    /// Publish the offered snapshot, if one is pending. `Ok(None)` when none
    /// is. A failure leaves the previous publication current and makes the
    /// next `caught_up` offer again.
    pub async fn publish_pending<E: PublisherExecutor>(
        &mut self,
        executor: &E,
    ) -> Result<Option<Report>> {
        let Some(work) = self.take() else {
            return Ok(None);
        };
        let export = Export::new(self.core.clone(), work, self.shutdown.clone());
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
        Ok(match self.take() {
            Some(work) => PublicationInput::NewMaterial(Export::new(
                self.core.clone(),
                work,
                self.shutdown.clone(),
            )),
            None => PublicationInput::Unchanged,
        })
    }

    fn failed(&mut self, error: &Error) -> FailurePolicy {
        self.mark_failed(error);
        FailurePolicy::NextPoll
    }

    fn settled(&mut self) -> Result<pir_pipeline::publisher::Control> {
        Ok(if self.closed {
            pir_pipeline::publisher::Control::Complete
        } else {
            pir_pipeline::publisher::Control::Continue
        })
    }

    async fn wait(&mut self) -> Result<()> {
        let changed = self.pending.changed().await;
        self.closed = changed.is_err();
        self.woken = changed.is_ok();
        Ok(())
    }
}
