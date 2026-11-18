//! State shared by the writer, the two publication tasks, the canonical
//! guards and the routes of one memo publisher.
//!
//! One journal and one chain follower feed two publications (spec 11.7):
//! memo, whose scopes are epochs, and first-contact, whose scopes are spans.
//! Each product has its own [`Side`]: distribution, built-table cache, offer
//! state and counters. The journal's rewind counter is shared, because a
//! rewind invalidates what was prepared for either product.
use crate::{
    error::{Error, Result},
    metrics::{Metrics, PublicationFigures},
};
use memo_store::{STORE_FILE, Store};
use pir_publication::AuthorityFence;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tachyon_chain::{BlockId, EpochSchedule, Hash, NetworkIdentity, NodeClient};
use tachyon_pir_distribution::{BlockRef, Distribution, Inventory, Subject, TableObjects};
use tachyon_pir_protocol::{
    BlockHash, ChainMapChunk, ChainMapHead, Entry, Product, Scope, ScopeState, Table,
};

/// The distribution contract's name for a protocol product.
pub(crate) fn distribution_product(product: Product) -> tachyon_pir_distribution::Product {
    match product {
        Product::Memo => tachyon_pir_distribution::Product::Memo,
        Product::FirstContact => tachyon_pir_distribution::Product::FirstContact,
        Product::Tachygram => tachyon_pir_distribution::Product::Tachygram,
        Product::Registry => tachyon_pir_distribution::Product::Registry,
    }
}

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
pub(crate) struct Built {
    /// By `(scope_id, entry-list digest)`.
    pub scopes: HashMap<(u64, [u8; 32]), Arc<BuiltScope>>,
    /// Sealed scopes by `(scope_id, hash of its last block)`: a sealed epoch
    /// or span with the same last block has the same entries, so its journal
    /// records need not be read again.
    pub sealed: HashMap<(u64, Hash), [u8; 32]>,
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

/// One product's publication state.
pub(crate) struct Side {
    pub product: Product,
    pub distribution: Distribution,
    pub built: Mutex<Built>,
    pub offer: Mutex<Offer>,
    /// Anchor of the eligible publication this process made or restored.
    pub served: Mutex<Option<BlockRef>>,
    pub tables_built: AtomicU64,
    pub publications: AtomicU64,
}

impl Side {
    pub fn new(product: Product, distribution: Distribution) -> Self {
        Self {
            product,
            distribution,
            built: Mutex::new(Built::default()),
            offer: Mutex::new(Offer::default()),
            served: Mutex::new(None),
            tables_built: AtomicU64::new(0),
            publications: AtomicU64::new(0),
        }
    }

    pub fn tables_built(&self) -> u64 {
        self.tables_built.load(Ordering::Relaxed)
    }
}

pub(crate) struct Core {
    pub node: NodeClient,
    pub identity: NetworkIdentity,
    pub schedule: EpochSchedule,
    pub metrics: Arc<Metrics>,
    pub memo: Arc<Side>,
    pub first_contact: Option<Arc<Side>>,
    /// Count of journal rewinds. Held across a commit and across each
    /// `withdraw_above`, so a publication of either product prepared before a
    /// rewind can never commit after it.
    pub rewinds: Mutex<u64>,
    pub _lock: std::fs::File,
}

impl Core {
    pub fn genesis(&self) -> BlockHash {
        BlockHash::from_protocol_bytes(*self.identity.genesis_hash.protocol_bytes())
    }

    pub fn rewinds(&self) -> u64 {
        *self.rewinds.lock().unwrap()
    }

    /// Every published product, memo first.
    pub fn sides(&self) -> impl Iterator<Item = &Arc<Side>> {
        std::iter::once(&self.memo).chain(self.first_contact.iter())
    }

    pub fn side(&self, product: Product) -> Option<&Arc<Side>> {
        self.sides().find(|side| side.product == product)
    }

    /// The epoch's first and last heights.
    pub fn epoch_bounds(&self, epoch: u32) -> Result<(u32, u32)> {
        let first = self.schedule.first_height(epoch);
        let last = self.schedule.last_height(epoch);
        first
            .zip(last)
            .ok_or_else(|| Error::other(format!("epoch {epoch} is beyond u32 heights")))
    }
}

pub(crate) fn block_ref(id: &BlockId) -> BlockRef {
    BlockRef {
        height: u64::from(id.height),
        hash: *id.hash.protocol_bytes(),
    }
}

/// The entries of one scope of `product` in ordinal order, every entry of a
/// repeated tag included (spec 5.3): a memo epoch (`scope_id` = epoch) or a
/// first-contact span (`scope_id` = its first epoch). With `through`, only
/// entries at or below that height.
pub(crate) fn scope_entries(
    store: &Store,
    product: Product,
    scope_id: u64,
    through: Option<u32>,
) -> Result<Vec<Entry>> {
    let scope = u32::try_from(scope_id).map_err(Error::other)?;
    let within = |height: u32| through.is_none_or(|through| height <= through);
    Ok(match product {
        Product::Memo => store
            .memo_entries(scope)?
            .into_iter()
            .filter(|record| within(record.ordinal.height))
            .map(|record| Entry::Memo(record.entry()))
            .collect(),
        Product::FirstContact => store
            .first_contact_entries(scope)?
            .into_iter()
            .filter(|record| within(record.ordinal.height))
            .map(|record| Entry::FirstContact(Box::new(record.entry())))
            .collect(),
        other => {
            return Err(Error::other(format!(
                "{other} is not a memo-family product"
            )));
        }
    })
}

/// A digest of an exact entry list in ordinal order (internal; not a wire
/// value).
pub(crate) fn entries_digest(product: Product, scope_id: u64, entries: &[Entry]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"memo-publisher/v1/entries\0");
    hash.update(product.name().as_bytes());
    hash.update([0]);
    hash.update(scope_id.to_le_bytes());
    hash.update((entries.len() as u64).to_le_bytes());
    for entry in entries {
        hash.update(entry.to_bytes());
    }
    hash.finalize().into()
}

/// A product's current envelope publication, eligible or not, read back from
/// the object store: inventory, map head and every sealed chunk.
pub(crate) struct Loaded {
    pub inventory: Inventory,
    pub head: ChainMapHead,
    pub chunks: Vec<ChainMapChunk>,
}

impl Loaded {
    pub fn read(distribution: &Distribution, product: Product) -> Result<Option<Self>> {
        let Some(inventory) = distribution.current_inventory()? else {
            return Ok(None);
        };
        let head = ChainMapHead::parse(&distribution.read_object(&inventory.map_head)?)
            .map_err(Error::other)?;
        let mut chunks = Vec::with_capacity(inventory.map_chunks.len());
        for (position, object) in inventory.map_chunks.iter().enumerate() {
            let bytes = distribution.read_object(object)?;
            chunks.push(
                head.parse_chunk(product, position, &bytes)
                    .map_err(Error::other)?,
            );
        }
        head.check_listing(product, &chunks).map_err(Error::other)?;
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

    /// Every listed scope with its scope index, in index order.
    pub fn indexed_scopes(&self) -> impl Iterator<Item = (u64, &Scope)> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.scopes.iter())
            .chain(self.head.open_scopes.iter())
            .enumerate()
            .map(|(index, scope)| (index as u64, scope))
    }

    /// Every sealed scope, in index order.
    pub fn sealed_scopes(&self) -> impl Iterator<Item = &Scope> {
        self.chunks.iter().flat_map(|chunk| chunk.scopes.iter())
    }

    pub fn figures(&self) -> PublicationFigures {
        figures(
            self.head.anchor_height,
            self.indexed_scopes().map(|(_, scope)| scope),
        )
    }

    /// The scope's tables with their objects, matched by session id.
    pub fn built(&self, index: u64, scope: &Scope) -> Result<BuiltScope> {
        let tables = scope
            .tables
            .iter()
            .map(|table| {
                let objects = self
                    .inventory
                    .tables
                    .iter()
                    .find(|t| t.scope_index == index && t.session_id == table.session_id.0)
                    .ok_or_else(|| {
                        Error::other(format!("inventory lacks a table of scope index {index}"))
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

/// After a restart, take a product's previous publication's built scopes
/// into its cache so they are not rebuilt (spec 11.4: table objects and
/// sessions are unchanged). Only when the journal still holds the
/// publication's anchor, which makes its blocks up to the anchor those the
/// publication was built from; each scope's entries are read from the journal
/// again and their count checked.
pub(crate) fn seed_built(side: &Side, store: &Store) -> Result<usize> {
    let Some(loaded) = Loaded::read(&side.distribution, side.product)? else {
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
    let mut built = side.built.lock().unwrap();
    let mut seeded = 0;
    for (index, scope) in loaded.indexed_scopes() {
        if scope.entries == 0 {
            continue;
        }
        let entries = scope_entries(store, side.product, scope.scope_id, Some(scope.last_height))?;
        if entries.len() as u64 != scope.entries {
            return Err(Error::other(format!(
                "journal {} scope {} holds {} entries up to {}, the publication {}",
                side.product,
                scope.scope_id,
                entries.len(),
                scope.last_height,
                scope.entries
            )));
        }
        let digest = entries_digest(side.product, scope.scope_id, &entries);
        built.scopes.insert(
            (scope.scope_id, digest),
            Arc::new(loaded.built(index, scope)?),
        );
        if scope.state == ScopeState::Sealed
            && let Some(last) = store.block_hash(scope.last_height)?
        {
            built.sealed.insert((scope.scope_id, last), digest);
        }
        seeded += 1;
    }
    Ok(seeded)
}

/// The journal's path under the state directory.
pub(crate) fn store_path(state_dir: &std::path::Path) -> std::path::PathBuf {
    state_dir.join(STORE_FILE)
}
