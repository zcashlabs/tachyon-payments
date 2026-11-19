//! State shared by the write API, the publication task, the distribution
//! hooks and the routes of one registry publisher.
use crate::{
    config::RegistryIdentity,
    error::{Error, Result},
    metrics::{Metrics, PublicationFigures},
};
use registry_store::{REGISTRY_CHUNK_ENTRIES, Store};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tachyon_pir_distribution::{Distribution, Inventory, Subject, TableObjects};
use tachyon_pir_protocol::{
    Address, BlockHash, Entry, Product, RegistryEntry, RegistryMapChunk, RegistryMapHead,
    RegistryScope, ScopeState, Table,
};
use tokio::sync::watch;

pub(crate) const PRODUCT: Product = Product::Registry;

/// One built table as a publication names it: its map entry and its objects.
#[derive(Clone, Debug)]
pub(crate) struct PublishedTable {
    pub table: Table,
    pub objects: TableObjects,
}

/// The tables of one chunk, built from one exact entry list. Table bytes are
/// a pure function of `(product, genesis, chunk_id, entries)` (spec 6.5).
#[derive(Debug)]
pub(crate) struct BuiltScope {
    pub entries: u64,
    pub n_buckets: u32,
    pub tables: Vec<PublishedTable>,
}

impl BuiltScope {
    pub fn empty() -> Arc<Self> {
        Arc::new(Self {
            entries: 0,
            n_buckets: 1,
            tables: Vec::new(),
        })
    }
}

/// The open chunk as last built: its id, entry count and entry digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OpenKey {
    pub chunk_id: u64,
    pub entries: u64,
    pub digest: [u8; 32],
}

/// Built chunks the current publication carries, so later publications reuse
/// them: every sealed chunk is built once (spec 9.2), and the open chunk is
/// rebuilt only when its entry list changed.
#[derive(Default)]
pub(crate) struct Memo {
    pub sealed: HashMap<u64, Arc<BuiltScope>>,
    pub open: Option<(OpenKey, Arc<BuiltScope>)>,
}

/// Counters for tests and operators.
#[derive(Default)]
pub(crate) struct Stats {
    pub tables_built: AtomicU64,
    pub publications: AtomicU64,
}

pub(crate) struct Core {
    pub store: Arc<Store>,
    pub distribution: Distribution,
    pub identity: RegistryIdentity,
    pub metrics: Arc<Metrics>,
    pub memo: Mutex<Memo>,
    /// The newest log sequence the write API produced; the publication task
    /// wakes on it.
    pub log: watch::Sender<u64>,
    /// `log_sequence` of the eligible publication this process made or
    /// restored.
    pub served: Mutex<Option<u64>>,
    pub stats: Stats,
    pub _lock: std::fs::File,
}

impl Core {
    pub fn genesis(&self) -> BlockHash {
        self.identity.genesis_hash
    }

    pub fn tables_built(&self) -> u64 {
        self.stats.tables_built.load(Ordering::Relaxed)
    }

    /// Record that the log holds at least `log_sequence` entries.
    pub fn grew(&self, log_sequence: u64) {
        self.log.send_if_modified(|known| {
            let grew = log_sequence > *known;
            *known = (*known).max(log_sequence);
            grew
        });
    }
}

/// The table entries of a chunk, in sequence order (the registry's ordinal).
pub(crate) fn chunk_entries(entries: Vec<(u64, Address)>) -> Vec<Entry> {
    entries
        .into_iter()
        .map(|(_, address)| Entry::Registry(Box::new(RegistryEntry::new(address))))
        .collect()
}

/// A digest of a chunk's exact entry list (internal; not a wire value).
pub(crate) fn entries_digest(chunk_id: u64, entries: &[Entry]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"registry-publisher/v1/entries\0");
    hash.update(chunk_id.to_le_bytes());
    hash.update((entries.len() as u64).to_le_bytes());
    for entry in entries {
        hash.update(entry.to_bytes());
    }
    hash.finalize().into()
}

/// The current envelope's publication, eligible or not, read back from the
/// object store: inventory, map head and every sealed map chunk.
pub(crate) struct Loaded {
    pub inventory: Inventory,
    pub head: RegistryMapHead,
    pub chunks: Vec<RegistryMapChunk>,
}

impl Loaded {
    pub fn read(distribution: &Distribution) -> Result<Option<Self>> {
        let Some(inventory) = distribution.current_inventory()? else {
            return Ok(None);
        };
        let head = RegistryMapHead::parse(&distribution.read_object(&inventory.map_head)?)
            .map_err(Error::other)?;
        let mut chunks = Vec::with_capacity(inventory.map_chunks.len());
        for (position, object) in inventory.map_chunks.iter().enumerate() {
            let bytes = distribution.read_object(object)?;
            chunks.push(head.parse_chunk(position, &bytes).map_err(Error::other)?);
        }
        head.check_listing(&chunks).map_err(Error::other)?;
        if inventory.subject
            != (Subject::Registry {
                log_sequence: head.log_sequence,
            })
        {
            return Err(Error::other(
                "the inventory's subject is not the map's log_sequence",
            ));
        }
        Ok(Some(Self {
            inventory,
            head,
            chunks,
        }))
    }

    /// Every listed chunk, in index order.
    pub fn scopes(&self) -> impl Iterator<Item = &RegistryScope> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.scopes.iter())
            .chain(self.head.open_scopes.iter())
    }

    pub fn figures(&self) -> PublicationFigures {
        figures(self.scopes())
    }

    /// The scope's tables with their objects, matched by session id.
    pub fn built(&self, scope: &RegistryScope) -> Result<BuiltScope> {
        let tables = scope
            .tables
            .iter()
            .map(|table| {
                let objects = self
                    .inventory
                    .tables
                    .iter()
                    .find(|t| t.scope_index == scope.scope_id && t.session_id == table.session_id.0)
                    .ok_or_else(|| {
                        Error::other(format!(
                            "inventory lacks a table of chunk {}",
                            scope.scope_id
                        ))
                    })?;
                Ok(PublishedTable {
                    table: table.clone(),
                    objects: objects.clone(),
                })
            })
            .collect::<Result<_>>()?;
        Ok(BuiltScope {
            entries: scope.entries,
            n_buckets: scope.n_buckets,
            tables,
        })
    }
}

/// Gauge values of a publication listing `scopes`.
pub(crate) fn figures<'a>(scopes: impl Iterator<Item = &'a RegistryScope>) -> PublicationFigures {
    let mut figures = PublicationFigures::default();
    for scope in scopes {
        figures.entries += scope.entries;
        if scope.state == ScopeState::Open {
            figures.open_scope_entries += scope.entries;
        }
        figures.overflow_segments += scope.tables.iter().filter(|t| t.segment > 0).count() as u64;
    }
    figures
}

/// After a restart, take the previous publication's built chunks into the
/// memo so neither the restored publication nor the next one rebuilds them
/// (spec 9.2 Restart, 11.4). Each chunk's rows are rebuilt from the log
/// (cheap; no PIR preparation) and must match the published `rows_sha256`
/// of every table, so a chunk is reused only for the log it was built from.
/// Returns the number of chunks taken.
pub(crate) fn seed_memo(core: &Core) -> Result<usize> {
    let Some(loaded) = Loaded::read(&core.distribution)? else {
        return Ok(0);
    };
    let log_sequence = loaded.head.log_sequence;
    if core.store.log_sequence()? < log_sequence {
        return Err(Error::other(format!(
            "the log holds fewer entries than the previous publication's {log_sequence}"
        )));
    }
    let genesis = core.genesis();
    let mut memo = Memo::default();
    for scope in loaded.scopes() {
        if scope.entries == 0 {
            continue;
        }
        let entries = chunk_entries(core.store.chunk_before(scope.scope_id, log_sequence)?);
        if entries.len() as u64 != scope.entries {
            return Err(Error::other(format!(
                "chunk {} holds {} entries in the log, {} in the publication",
                scope.scope_id,
                entries.len(),
                scope.entries
            )));
        }
        let rebuilt = tachyon_pir_table::build_scope(PRODUCT, &genesis, scope.scope_id, &entries)
            .map_err(Error::other)?;
        let same = rebuilt.tables.len() == scope.tables.len()
            && rebuilt
                .tables
                .iter()
                .zip(&scope.tables)
                .all(|(built, table)| {
                    built.rows_sha256 == table.rows_sha256.0
                        && (built.bucket, built.segment, built.seed)
                            == (table.bucket, table.segment, table.seed)
                });
        if !same {
            return Err(Error::other(format!(
                "chunk {} does not rebuild to its published rows",
                scope.scope_id
            )));
        }
        let built = Arc::new(loaded.built(scope)?);
        match scope.state {
            ScopeState::Sealed => {
                memo.sealed.insert(scope.scope_id, built);
            }
            ScopeState::Open => {
                memo.open = Some((
                    OpenKey {
                        chunk_id: scope.scope_id,
                        entries: scope.entries,
                        digest: entries_digest(scope.scope_id, &entries),
                    },
                    built,
                ));
            }
        }
    }
    let seeded = memo.sealed.len() + usize::from(memo.open.is_some());
    *core.memo.lock().unwrap() = memo;
    Ok(seeded)
}

/// The open chunk of a log holding `log_sequence` entries (spec 10.3).
pub(crate) const fn open_chunk(log_sequence: u64) -> u64 {
    log_sequence / REGISTRY_CHUNK_ENTRIES
}
