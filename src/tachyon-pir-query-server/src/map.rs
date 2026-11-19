//! Reading a publication's map into the form the shared core checks and
//! serves, per [`MapFamily`]. The chain family parses `ChainMapHead` and its
//! chunks, the registry family `RegistryMapHead` and its chunks, each with
//! the protocol crate's own client-side checks.

use crate::{Error, product::MapFamily};
use pir_publication::ObjectReference;
use std::{collections::HashMap, sync::Arc};
use tachyon_pir_protocol::{
    BlockHash, ChainMapChunk, ChainMapHead, Product, RegistryMapChunk, RegistryMapHead, RowContext,
    ScopeState, ScopeTables, Table,
};

/// The chain facts a chain map states.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainFacts {
    pub activation_height: u32,
    pub epoch_length: u32,
    pub anchor_height: u32,
    pub anchor_hash: BlockHash,
    /// `last_height` of every open scope, in listing order.
    pub open_last_heights: Vec<u32>,
}

/// One table as the map describes it, with the context its rows are
/// validated under (`row` is 0; the validator sets each row).
#[derive(Clone, Debug)]
pub struct ListedTable {
    pub scope_index: u64,
    pub scope_id: u64,
    pub table: Table,
    pub template: RowContext,
}

/// A parsed, checked map.
#[derive(Clone, Debug)]
pub struct Listing {
    pub network: String,
    pub genesis: BlockHash,
    pub generation: u64,
    pub recovery_epoch: u64,
    pub floor_scope: u64,
    pub chain: Option<ChainFacts>,
    /// The registry's `log_sequence`.
    pub log_sequence: Option<u64>,
    /// Number of scopes listed (sealed and open); the newest index is one less.
    pub scopes: u64,
    pub entries_total: u64,
    pub open_scope_entries: u64,
    /// Sealed chunk digests with their `base`, in listing order.
    pub chunks: Vec<(String, u64)>,
    pub tables: Vec<ListedTable>,
}

/// What the routes and metrics need of a served map, kept per map digest.
#[derive(Clone, Debug, Default)]
pub struct MapInfo {
    pub chunk_bases: HashMap<String, u64>,
    pub floor_scope: u64,
    pub anchor_height: Option<u32>,
    pub log_sequence: Option<u64>,
    pub newest_scope: Option<u64>,
    pub entries_total: u64,
    pub open_scope_entries: u64,
}

impl Listing {
    pub fn info(&self) -> MapInfo {
        MapInfo {
            chunk_bases: self.chunks.iter().cloned().collect(),
            floor_scope: self.floor_scope,
            anchor_height: self.chain.as_ref().map(|c| c.anchor_height),
            log_sequence: self.log_sequence,
            newest_scope: self.scopes.checked_sub(1),
            entries_total: self.entries_total,
            open_scope_entries: self.open_scope_entries,
        }
    }
}

/// Parse and check a map head and its chunks (in the inventory's order,
/// which must be the head's `sealed_chunks` order).
pub fn read(
    family: MapFamily,
    product: Product,
    head: &[u8],
    chunks: &[(ObjectReference, Arc<[u8]>)],
) -> Result<Listing, Error> {
    match family {
        MapFamily::Chain => read_chain(product, head, chunks),
        MapFamily::Registry => read_registry(product, head, chunks),
    }
}

/// Every scope's entries and tables, in listing order: the scope index is
/// the position.
fn list_scopes<'a, S: ScopeTables + 'a>(
    listing: &mut Listing,
    product: Product,
    scopes: impl Iterator<Item = &'a S>,
) {
    let genesis = listing.genesis;
    for (index, scope) in scopes.enumerate() {
        let index = index as u64;
        listing.scopes += 1;
        listing.entries_total += scope.entries();
        if scope.state() == ScopeState::Open {
            listing.open_scope_entries += scope.entries();
        }
        for table in scope.tables() {
            listing.tables.push(ListedTable {
                scope_index: index,
                scope_id: scope.scope_id(),
                table: table.clone(),
                template: scope.row_context(product, &genesis, table, 0),
            });
        }
    }
}

fn read_registry(
    product: Product,
    head_bytes: &[u8],
    chunk_objects: &[(ObjectReference, Arc<[u8]>)],
) -> Result<Listing, Error> {
    let head = RegistryMapHead::parse(head_bytes)?;
    if head.to_bytes()? != head_bytes {
        return Err("the map head is not canonically encoded".into());
    }
    if head.protocol != product.protocol() {
        return Err("the map is not this product's".into());
    }
    head.validate()?;
    if chunk_objects.len() != head.sealed_chunks.len() {
        return Err("the inventory's map chunks are not the head's sealed chunks".into());
    }
    let mut chunks: Vec<RegistryMapChunk> = Vec::with_capacity(chunk_objects.len());
    for (position, ((object, bytes), reference)) in
        chunk_objects.iter().zip(&head.sealed_chunks).enumerate()
    {
        if object.sha256 != reference.sha256.hex() {
            return Err(format!("map chunk {position} is not the head's sealed chunk").into());
        }
        let chunk = head.parse_chunk(position, bytes)?;
        if chunk.to_bytes() != bytes[..] {
            return Err(format!("map chunk {position} is not canonically encoded").into());
        }
        chunks.push(chunk);
    }
    head.check_listing(&chunks)?;
    let mut listing = Listing {
        network: head.network.clone(),
        genesis: head.genesis_hash,
        generation: head.generation,
        recovery_epoch: head.recovery_epoch.get(),
        floor_scope: head.floor_scope,
        chain: None,
        log_sequence: Some(head.log_sequence),
        scopes: 0,
        entries_total: 0,
        open_scope_entries: 0,
        chunks: head
            .sealed_chunks
            .iter()
            .map(|c| (c.sha256.hex(), c.base))
            .collect(),
        tables: Vec::new(),
    };
    let scopes = chunks
        .iter()
        .flat_map(|chunk| chunk.scopes.iter())
        .chain(head.open_scopes.iter());
    list_scopes(&mut listing, product, scopes);
    Ok(listing)
}

fn read_chain(
    product: Product,
    head_bytes: &[u8],
    chunk_objects: &[(ObjectReference, Arc<[u8]>)],
) -> Result<Listing, Error> {
    let head = ChainMapHead::parse(head_bytes)?;
    // Clients refuse a non-canonical map (spec 10.3); never serve one.
    if head.to_bytes()? != head_bytes {
        return Err("the map head is not canonically encoded".into());
    }
    head.validate(product)?;
    if chunk_objects.len() != head.sealed_chunks.len() {
        return Err("the inventory's map chunks are not the head's sealed chunks".into());
    }
    let mut chunks: Vec<ChainMapChunk> = Vec::with_capacity(chunk_objects.len());
    for (position, ((object, bytes), reference)) in
        chunk_objects.iter().zip(&head.sealed_chunks).enumerate()
    {
        if object.sha256 != reference.sha256.hex() {
            return Err(format!("map chunk {position} is not the head's sealed chunk").into());
        }
        let chunk = head.parse_chunk(product, position, bytes)?;
        if chunk.to_bytes() != bytes[..] {
            return Err(format!("map chunk {position} is not canonically encoded").into());
        }
        chunks.push(chunk);
    }
    head.check_listing(product, &chunks)?;
    let genesis = head.genesis_hash;
    let mut listing = Listing {
        network: head.network.clone(),
        genesis,
        generation: head.generation,
        recovery_epoch: head.recovery_epoch.get(),
        floor_scope: head.floor_scope,
        chain: Some(ChainFacts {
            activation_height: head.activation_height,
            epoch_length: head.epoch_length,
            anchor_height: head.anchor_height,
            anchor_hash: head.anchor_hash,
            open_last_heights: head.open_scopes.iter().map(|s| s.last_height).collect(),
        }),
        log_sequence: None,
        scopes: 0,
        entries_total: 0,
        open_scope_entries: 0,
        chunks: head
            .sealed_chunks
            .iter()
            .map(|c| (c.sha256.hex(), c.base))
            .collect(),
        tables: Vec::new(),
    };
    let scopes = chunks
        .iter()
        .flat_map(|chunk| chunk.scopes.iter())
        .chain(head.open_scopes.iter());
    list_scopes(&mut listing, product, scopes);
    Ok(listing)
}
