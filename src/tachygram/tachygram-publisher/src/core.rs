//! State shared by the writer, the publication task, the canonical guard and
//! the routes of one tachygram publisher.
use crate::{
    error::{Error, Result},
    metrics::{Metrics, PublicationFigures},
};
use pir_publication::AuthorityFence;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tachygram_store::{STORE_FILE, Store};
use tachyon_chain::{BlockId, EpochSchedule, Hash, NetworkIdentity, NodeClient};
use tachyon_pir_distribution::{BlockRef, Distribution, Inventory, Subject, TableObjects};
use tachyon_pir_protocol::{
    BlockHash, ChainMapChunk, ChainMapHead, Entry, Product, SEAL_DEPTH, Scope, ScopeState, Table,
    TachygramEntry,
};

pub(crate) const PRODUCT: Product = Product::Tachygram;

/// One built table as a publication names it: its map entry and its objects.
#[derive(Clone, Debug)]
pub(crate) struct PublishedTable {
    pub table: Table,
    pub objects: TableObjects,
}

/// The tables of one scope, built from one exact entry list. Table bytes are
/// a pure function of `(product, genesis, scope_id, entries)` (spec 6.5), so a
/// scope whose entry list is unchanged reuses them.
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

/// Built scopes the current publication carries, so later publications reuse
/// them without rebuilding.
#[derive(Default)]
pub(crate) struct Memo {
    /// By `(epoch, entry-list digest)`.
    pub scopes: HashMap<(u32, [u8; 32]), Arc<BuiltScope>>,
    /// Sealed scopes by `(epoch, hash of its last block)`: a sealed epoch with
    /// the same last block has the same entries, so its journal records need
    /// not be read again.
    pub sealed: HashMap<(u32, Hash), [u8; 32]>,
}

/// What `caught_up` last offered for publication, so an unchanged pass offers
/// nothing and a failed attempt is offered again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OfferKey {
    pub anchor: BlockId,
    pub fence: AuthorityFence,
    pub rewinds: u64,
}

#[derive(Default)]
pub(crate) struct Offer {
    pub key: Option<OfferKey>,
    pub failed: bool,
}

/// Counters for tests and operators.
#[derive(Default)]
pub(crate) struct Stats {
    pub tables_built: AtomicU64,
    pub publications: AtomicU64,
}

pub(crate) struct Core {
    pub distribution: Distribution,
    pub node: NodeClient,
    pub identity: NetworkIdentity,
    pub schedule: EpochSchedule,
    pub metrics: Arc<Metrics>,
    pub memo: Mutex<Memo>,
    /// Count of journal rewinds. Held across a commit and across each
    /// `withdraw_above`, so a publication prepared before a rewind can never
    /// commit after it.
    pub rewinds: Mutex<u64>,
    pub offer: Mutex<Offer>,
    /// Anchor of the eligible publication this process made or restored.
    pub served: Mutex<Option<BlockRef>>,
    pub stats: Stats,
    pub _lock: std::fs::File,
}

impl Core {
    pub fn genesis(&self) -> BlockHash {
        BlockHash::from_protocol_bytes(*self.identity.genesis_hash.protocol_bytes())
    }

    pub fn rewinds(&self) -> u64 {
        *self.rewinds.lock().unwrap()
    }

    /// The epoch's first and last heights.
    pub fn epoch_bounds(&self, epoch: u32) -> Result<(u32, u32)> {
        let first = self.schedule.first_height(epoch);
        let last = self.schedule.last_height(epoch);
        first
            .zip(last)
            .ok_or_else(|| Error::other(format!("epoch {epoch} is beyond u32 heights")))
    }

    /// Whether `epoch` is sealed under `anchor`: its last block is at least
    /// `SEAL_DEPTH` blocks below the anchor (spec 6.5).
    pub fn sealed(&self, epoch: u32, anchor: u32) -> bool {
        self.schedule.last_height(epoch).is_some_and(|last| {
            last.checked_add(SEAL_DEPTH)
                .is_some_and(|reach| reach <= anchor)
        })
    }

    pub fn tables_built(&self) -> u64 {
        self.stats.tables_built.load(Ordering::Relaxed)
    }
}

pub(crate) fn block_ref(id: &BlockId) -> BlockRef {
    BlockRef {
        height: u64::from(id.height),
        hash: *id.hash.protocol_bytes(),
    }
}

/// The entries of `epoch` in ordinal order: stamp order (block, then
/// transaction), then each stamp's own tachygram order. This is spec 5.2's
/// `(height, tx index in block, entry index)` order applied to tachygrams; it
/// decides which entries spill to overflow segments (spec 6.4).
pub(crate) fn epoch_entries(store: &Store, epoch: u32) -> Result<Vec<TachygramEntry>> {
    let stamps = store.stamps(epoch)?;
    let mut heights = Vec::with_capacity(stamps.len());
    for (index, stamp) in stamps.iter().enumerate() {
        if stamp.ordinal.get() as usize != index {
            return Err(Error::other(format!(
                "epoch {epoch}: stamp ordinals are not contiguous from 0"
            )));
        }
        heights.push(stamp.height);
    }
    store
        .epoch_tachygrams(epoch)?
        .map(|(ordinal, tachygram)| {
            let blk_height = *heights.get(ordinal.get() as usize).ok_or_else(|| {
                Error::other(format!("epoch {epoch}: tachygram of an unknown stamp"))
            })?;
            Ok(TachygramEntry {
                tachygram,
                blk_height,
                stamp_ordinal: ordinal.get(),
            })
        })
        .collect()
}

/// A digest of an exact entry list in ordinal order (internal; not a wire
/// value).
pub(crate) fn entries_digest(epoch: u32, entries: &[TachygramEntry]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"tachygram-publisher/v1/entries\0");
    hash.update(epoch.to_le_bytes());
    hash.update((entries.len() as u64).to_le_bytes());
    for entry in entries {
        hash.update(Entry::Tachygram(*entry).to_bytes());
    }
    hash.finalize().into()
}

/// The current envelope's publication, eligible or not, read back from the
/// object store: inventory, map head and every sealed chunk.
pub(crate) struct Loaded {
    pub inventory: Inventory,
    pub head: ChainMapHead,
    pub chunks: Vec<ChainMapChunk>,
}

impl Loaded {
    pub fn read(distribution: &Distribution) -> Result<Option<Self>> {
        let Some(inventory) = distribution.current_inventory()? else {
            return Ok(None);
        };
        let head = ChainMapHead::parse(&distribution.read_object(&inventory.map_head)?)
            .map_err(Error::other)?;
        let mut chunks = Vec::with_capacity(inventory.map_chunks.len());
        for (position, object) in inventory.map_chunks.iter().enumerate() {
            let bytes = distribution.read_object(object)?;
            chunks.push(
                head.parse_chunk(PRODUCT, position, &bytes)
                    .map_err(Error::other)?,
            );
        }
        head.check_listing(PRODUCT, &chunks).map_err(Error::other)?;
        Ok(Some(Self {
            inventory,
            head,
            chunks,
        }))
    }

    pub fn anchor(&self) -> Option<BlockRef> {
        match &self.inventory.subject {
            Subject::Chain { anchor, .. } => Some(*anchor),
            Subject::Registry { .. } => None,
        }
    }

    /// Every listed scope, in index order.
    pub fn scopes(&self) -> impl Iterator<Item = &Scope> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.scopes.iter())
            .chain(self.head.open_scopes.iter())
    }

    pub fn figures(&self) -> PublicationFigures {
        figures(self.head.anchor_height, self.scopes())
    }

    /// The scope's tables with their objects, matched by session id.
    pub fn built(&self, scope: &Scope) -> Result<BuiltScope> {
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
                            "inventory lacks a table of scope {}",
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

/// Gauge values of a publication with `scopes` anchored at `anchor_height`.
pub(crate) fn figures<'a>(
    anchor_height: u32,
    scopes: impl Iterator<Item = &'a Scope>,
) -> PublicationFigures {
    let mut figures = PublicationFigures {
        anchor_height,
        ..PublicationFigures::default()
    };
    for scope in scopes {
        figures.entries += scope.entries;
        if scope.state == ScopeState::Open {
            figures.open_scope_entries += scope.entries;
        }
        figures.overflow_segments += scope.tables.iter().filter(|t| t.segment > 0).count() as u64;
    }
    figures
}

/// After a restart, take the previous publication's built scopes into the
/// memo so they are not rebuilt (spec 11.4: table objects and sessions are
/// unchanged). Only when the journal still holds the publication's anchor,
/// which makes its blocks up to the anchor those the publication was built
/// from; each scope's entry count is checked against the journal again.
pub(crate) fn seed_memo(core: &Core, store: &Store) -> Result<usize> {
    let Some(loaded) = Loaded::read(&core.distribution)? else {
        return Ok(0);
    };
    let Some(anchor) = loaded.anchor() else {
        return Ok(0);
    };
    let Ok(height) = u32::try_from(anchor.height) else {
        return Ok(0);
    };
    if store.block_hash(height)?.map(|h| *h.protocol_bytes()) != Some(anchor.hash) {
        return Ok(0);
    }
    let mut memo = core.memo.lock().unwrap();
    let mut seeded = 0;
    for scope in loaded.scopes() {
        if scope.entries == 0 {
            continue;
        }
        let epoch = u32::try_from(scope.scope_id).map_err(Error::other)?;
        let entries: Vec<_> = epoch_entries(store, epoch)?
            .into_iter()
            .filter(|entry| entry.blk_height <= scope.last_height)
            .collect();
        if entries.len() as u64 != scope.entries {
            return Err(Error::other(format!(
                "journal epoch {epoch} holds {} entries up to {}, the publication {}",
                entries.len(),
                scope.last_height,
                scope.entries
            )));
        }
        let digest = entries_digest(epoch, &entries);
        memo.scopes
            .insert((epoch, digest), Arc::new(loaded.built(scope)?));
        if scope.state == ScopeState::Sealed
            && let Some(last) = store.block_hash(scope.last_height)?
        {
            memo.sealed.insert((epoch, last), digest);
        }
        seeded += 1;
    }
    Ok(seeded)
}

/// The journal's path under the state directory.
pub(crate) fn store_path(state_dir: &std::path::Path) -> std::path::PathBuf {
    state_dir.join(STORE_FILE)
}
