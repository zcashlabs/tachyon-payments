//! A fetched and validated map (spec 10.3): the head, every sealed chunk and
//! every scope by index and by `scope_id`.

use std::collections::HashMap;
use std::fmt;

use serde::Serialize;
use tachyon_pir_protocol::{
    BlockHash, Bytes32, ChainMapChunk, ChainMapHead, MapError, Product, REGISTRY_CHUNK_ENTRIES,
    RegistryMapChunk, RegistryMapHead, RegistryScope, Scope, ScopeState, ScopeTables,
    SealedChunkRef, map_sha256,
};

use crate::error::ClientError;

/// How a caller names a scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScopeSelector {
    /// The scope index of spec 10.3: the epoch for memo and tachygram, the
    /// span position for first-contact, the chunk id for registry.
    Index(u64),
    /// The scope's `scope_id`.
    Id(u64),
}

impl fmt::Display for ScopeSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScopeSelector::Index(i) => write!(f, "index {i}"),
            ScopeSelector::Id(id) => write!(f, "id {id}"),
        }
    }
}

/// The map head, in the form of its product.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapHead {
    Chain(ChainMapHead),
    Registry(RegistryMapHead),
}

impl MapHead {
    /// Checks a `GET map` response (spec 10.2, 10.3): `X-Map-Sha256` equal to
    /// the body's SHA-256, the head form of `product`, the head-only rules,
    /// and the canonical compact encoding.
    pub fn from_response(
        product: Product,
        header: Option<&str>,
        body: &[u8],
    ) -> Result<(MapHead, Bytes32), ClientError> {
        let actual = map_sha256(body);
        if header.map(str::trim) != Some(actual.hex().as_str()) {
            return Err(ClientError::MapHeader {
                header: header.map(str::to_owned),
                actual,
            });
        }
        let head = if product.is_chain() {
            let head = ChainMapHead::parse(body)?;
            head.validate(product)?;
            if head.to_bytes()? != body {
                return Err(ClientError::NonCanonical("map head"));
            }
            MapHead::Chain(head)
        } else {
            let head = RegistryMapHead::parse(body)?;
            head.validate()?;
            if head.to_bytes()? != body {
                return Err(ClientError::NonCanonical("map head"));
            }
            MapHead::Registry(head)
        };
        Ok((head, actual))
    }

    pub fn network(&self) -> &str {
        match self {
            MapHead::Chain(h) => &h.network,
            MapHead::Registry(h) => &h.network,
        }
    }

    pub fn genesis_hash(&self) -> &BlockHash {
        match self {
            MapHead::Chain(h) => &h.genesis_hash,
            MapHead::Registry(h) => &h.genesis_hash,
        }
    }

    pub fn generation(&self) -> u64 {
        match self {
            MapHead::Chain(h) => h.generation,
            MapHead::Registry(h) => h.generation,
        }
    }

    pub fn recovery_epoch(&self) -> u64 {
        match self {
            MapHead::Chain(h) => h.recovery_epoch.get(),
            MapHead::Registry(h) => h.recovery_epoch.get(),
        }
    }

    pub fn floor_scope(&self) -> u64 {
        match self {
            MapHead::Chain(h) => h.floor_scope,
            MapHead::Registry(h) => h.floor_scope,
        }
    }

    pub fn sealed_chunks(&self) -> &[SealedChunkRef] {
        match self {
            MapHead::Chain(h) => &h.sealed_chunks,
            MapHead::Registry(h) => &h.sealed_chunks,
        }
    }

    /// Verifies, parses and checks the sealed chunk at `position` (digest,
    /// canonical encoding, frame and scopes against this head).
    pub fn parse_chunk(
        &self,
        product: Product,
        position: usize,
        bytes: &[u8],
    ) -> Result<MapChunkScopes, ClientError> {
        let base = self.sealed_chunks().get(position).map_or(0, |r| r.base);
        let digest = |e: MapError| match e {
            MapError::Digest => ClientError::ChunkDigest { base },
            e => ClientError::Map(e),
        };
        let chunk = match self {
            MapHead::Chain(h) => {
                let chunk = h.parse_chunk(product, position, bytes).map_err(digest)?;
                if chunk.to_bytes() != bytes {
                    return Err(ClientError::NonCanonical("map chunk"));
                }
                MapChunkScopes::Chain(chunk)
            }
            MapHead::Registry(h) => {
                let chunk = h.parse_chunk(position, bytes).map_err(digest)?;
                if chunk.to_bytes() != bytes {
                    return Err(ClientError::NonCanonical("map chunk"));
                }
                MapChunkScopes::Registry(chunk)
            }
        };
        Ok(chunk)
    }

    /// Checks an already parsed (cached) chunk at `position` of this head: a
    /// chunk is immutable, but may sit at another position of a newer map.
    pub fn check_chunk(
        &self,
        product: Product,
        position: usize,
        chunk: &MapChunkScopes,
    ) -> Result<(), ClientError> {
        match (self, chunk) {
            (MapHead::Chain(h), MapChunkScopes::Chain(c)) => h.check_chunk(product, position, c)?,
            (MapHead::Registry(h), MapChunkScopes::Registry(c)) => h.check_chunk(position, c)?,
            _ => return Err(MapError::Invalid("chunk form does not match the head".into()).into()),
        }
        Ok(())
    }
}

/// A sealed map chunk of either form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapChunkScopes {
    Chain(ChainMapChunk),
    Registry(RegistryMapChunk),
}

/// One listed scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViewScope {
    Chain(Scope),
    Registry(RegistryScope),
}

impl ViewScope {
    /// The tables and placement of this scope.
    pub fn tables(&self) -> &dyn ScopeTables {
        match self {
            ViewScope::Chain(s) => s,
            ViewScope::Registry(s) => s,
        }
    }

    pub fn scope_id(&self) -> u64 {
        self.tables().scope_id()
    }

    /// What the scope covers, so a caller knows what an absence means
    /// (spec 8.4).
    pub fn coverage(&self) -> Coverage {
        match self {
            ViewScope::Chain(s) => Coverage::Heights {
                state: s.state,
                first_height: s.first_height,
                last_height: s.last_height,
                entries: s.entries,
            },
            ViewScope::Registry(s) => Coverage::Sequences {
                state: s.state,
                first_sequence: s.scope_id * REGISTRY_CHUNK_ENTRIES,
                entries: s.entries,
            },
        }
    }
}

/// What a scope covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Coverage {
    /// A chain scope: blocks `first_height..=last_height`. For the newest
    /// open scope `last_height` is the anchor.
    Heights {
        state: ScopeState,
        first_height: u32,
        last_height: u32,
        entries: u64,
    },
    /// A registry chunk: log sequences `first_sequence..first_sequence + entries`.
    Sequences {
        state: ScopeState,
        first_sequence: u64,
        entries: u64,
    },
}

/// A map head with every sealed chunk, validated as a whole listing.
#[derive(Clone, Debug)]
pub struct MapView {
    product: Product,
    map_sha256: Bytes32,
    head: MapHead,
    scopes: Vec<ViewScope>,
    by_id: HashMap<u64, usize>,
}

impl MapView {
    /// Assembles a view from a checked head and its chunks in position order,
    /// running the whole-listing check (`check_listing`).
    pub fn build(
        product: Product,
        map_sha256: Bytes32,
        head: MapHead,
        chunks: Vec<MapChunkScopes>,
    ) -> Result<MapView, ClientError> {
        let mut scopes = Vec::new();
        match &head {
            MapHead::Chain(h) => {
                let chunks: Vec<ChainMapChunk> = chunks
                    .into_iter()
                    .map(|c| match c {
                        MapChunkScopes::Chain(c) => Ok(c),
                        MapChunkScopes::Registry(_) => Err(form_mismatch()),
                    })
                    .collect::<Result<_, _>>()?;
                h.check_listing(product, &chunks)?;
                scopes.extend(
                    chunks
                        .into_iter()
                        .flat_map(|c| c.scopes)
                        .chain(h.open_scopes.iter().cloned())
                        .map(ViewScope::Chain),
                );
            }
            MapHead::Registry(h) => {
                let chunks: Vec<RegistryMapChunk> = chunks
                    .into_iter()
                    .map(|c| match c {
                        MapChunkScopes::Registry(c) => Ok(c),
                        MapChunkScopes::Chain(_) => Err(form_mismatch()),
                    })
                    .collect::<Result<_, _>>()?;
                h.check_listing(&chunks)?;
                scopes.extend(
                    chunks
                        .into_iter()
                        .flat_map(|c| c.scopes)
                        .chain(h.open_scopes.iter().cloned())
                        .map(ViewScope::Registry),
                );
            }
        }
        let mut by_id = HashMap::with_capacity(scopes.len());
        for (index, scope) in scopes.iter().enumerate() {
            if by_id.insert(scope.scope_id(), index).is_some() {
                return Err(MapError::Invalid(format!(
                    "scope_id {} listed twice",
                    scope.scope_id()
                ))
                .into());
            }
            if product.index_is_scope_id() && scope.scope_id() != index as u64 {
                return Err(MapError::Invalid(format!(
                    "scope {} listed at index {index}",
                    scope.scope_id()
                ))
                .into());
            }
        }
        Ok(MapView {
            product,
            map_sha256,
            head,
            scopes,
            by_id,
        })
    }

    pub fn product(&self) -> Product {
        self.product
    }

    /// SHA-256 of the head bytes, as `X-Map-Sha256` carried it.
    pub fn map_sha256(&self) -> Bytes32 {
        self.map_sha256
    }

    pub fn head(&self) -> &MapHead {
        &self.head
    }

    pub fn floor_scope(&self) -> u64 {
        self.head.floor_scope()
    }

    pub fn genesis_hash(&self) -> &BlockHash {
        self.head.genesis_hash()
    }

    /// Every scope, by index.
    pub fn scopes(&self) -> &[ViewScope] {
        &self.scopes
    }

    /// The scope a selector names, with its index.
    pub fn scope(&self, selector: ScopeSelector) -> Option<(u64, &ViewScope)> {
        let index = match selector {
            ScopeSelector::Index(i) => usize::try_from(i).ok()?,
            ScopeSelector::Id(id) => *self.by_id.get(&id)?,
        };
        self.scopes.get(index).map(|s| (index as u64, s))
    }
}

fn form_mismatch() -> ClientError {
    MapError::Invalid("chunk form does not match the head".into()).into()
}
