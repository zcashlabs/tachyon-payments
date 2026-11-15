//! The map (spec 10.3): head, chunks, scopes and tables, with the checks a
//! client runs before it queries, and the (table, row) plan for one key.
//!
//! Every struct refuses unknown fields. Block hashes (`genesis_hash`,
//! `anchor_hash`) are display-order hex; digests and session ids plain hex
//! (spec 3). `recovery_epoch` is a decimal string (see [`RecoveryEpoch`]).
//! The canonical encoding the publisher serves is compact JSON in declaration
//! order ([`ChainMapHead::to_bytes`]); clients hash the bytes they received.
//!
//! Validation is split by what a client holds:
//!
//! - `validate`: the head alone. Open scopes, chunk references, and every
//!   relation between them that the head determines.
//! - `check_chunk` / `parse_chunk`: one sealed chunk against the head.
//! - `check_listing`: the head with every chunk, which adds the relations
//!   across chunk boundaries and the floor for first-contact.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::bytes::{BlockHash, Bytes32, RecoveryEpoch};
use crate::constants::{
    FC_SPAN_MAX_EPOCHS, MAP_CHUNK_SCOPES, MAX_MAP_HEAD_BYTES, MAX_SEEDS, MAX_SEGMENTS, PROFILE,
    PUBLISHED_GEOMETRY, REGISTRY_CHUNK_ENTRIES,
};
use crate::geometry::Geometry;
use crate::placement::{self, PlacementError, valid_n_buckets};
use crate::product::Product;
use crate::row::RowContext;
use crate::session::{SessionParts, session_id};

/// A map document that cannot be used.
#[derive(Debug, thiserror::Error)]
pub enum MapError {
    /// A head over [`MAX_MAP_HEAD_BYTES`].
    #[error("map head of {0} bytes is over the 1 MiB limit")]
    TooLarge(usize),
    /// Not the JSON shape, including unknown fields.
    #[error("map JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// A chunk whose bytes do not hash to its reference.
    #[error("map chunk digest mismatch")]
    Digest,
    /// Well-formed JSON that breaks a rule of spec 6 or 10.3.
    #[error("invalid map: {0}")]
    Invalid(String),
}

fn invalid<T>(reason: impl Into<String>) -> Result<T, MapError> {
    Err(MapError::Invalid(reason.into()))
}

/// SHA-256 of a map document's bytes, as `X-Map-Sha256` and chunk references
/// carry it.
pub fn map_sha256(bytes: &[u8]) -> Bytes32 {
    Bytes32(Sha256::digest(bytes).into())
}

/// Whether a scope can still change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScopeState {
    Open,
    Sealed,
}

/// One table: a (bucket, segment) of a scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub bucket: u32,
    pub segment: u32,
    pub geometry: Geometry,
    pub seed: u32,
    pub entries: u64,
    pub rows_sha256: Bytes32,
    pub public_sha256: Bytes32,
    pub session_id: Bytes32,
}

impl Table {
    /// The session id inputs of this table in scope `scope_id`.
    pub fn session_parts(&self, scope_id: u64) -> SessionParts {
        SessionParts {
            scope_id,
            bucket: self.bucket,
            segment: self.segment,
            rows: self.geometry.rows(),
            seed: self.seed,
            rows_sha256: self.rows_sha256.0,
            public_sha256: self.public_sha256.0,
        }
    }

    /// The session id this table must carry.
    pub fn expected_session_id(&self, product: Product, scope_id: u64) -> Bytes32 {
        Bytes32(session_id(product, &self.session_parts(scope_id)))
    }
}

/// A chain scope (memo, first-contact, tachygram).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub scope_id: u64,
    pub state: ScopeState,
    pub first_height: u32,
    /// The scope's last block when sealed; for the newest open scope the
    /// anchor height.
    pub last_height: u32,
    pub entries: u64,
    pub n_buckets: u32,
    pub tables: Vec<Table>,
}

/// A registry chunk scope: no heights.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryScope {
    pub scope_id: u64,
    pub state: ScopeState,
    pub entries: u64,
    pub n_buckets: u32,
    pub tables: Vec<Table>,
}

/// A sealed map chunk as the head references it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedChunkRef {
    pub base: u64,
    pub sha256: Bytes32,
}

/// `GET map/{base}/{sha256}`: the sealed scopes with index in
/// `base .. base + 32`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapChunk<S> {
    pub protocol: String,
    pub base: u64,
    pub scopes: Vec<S>,
}

/// A memo, first-contact or tachygram chunk.
pub type ChainMapChunk = MapChunk<Scope>;
/// A registry chunk.
pub type RegistryMapChunk = MapChunk<RegistryScope>;

impl<S: Serialize> MapChunk<S> {
    /// Canonical bytes: compact JSON.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a map chunk serializes")
    }

    /// SHA-256 of the canonical bytes.
    pub fn sha256(&self) -> Bytes32 {
        map_sha256(&self.to_bytes())
    }
}

/// The map head of memo, first-contact and tachygram.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainMapHead {
    pub protocol: String,
    pub profile: String,
    pub network: String,
    pub genesis_hash: BlockHash,
    pub activation_height: u32,
    pub epoch_length: u32,
    pub generation: u64,
    pub recovery_epoch: RecoveryEpoch,
    pub anchor_height: u32,
    pub anchor_hash: BlockHash,
    pub floor_scope: u64,
    pub sealed_chunks: Vec<SealedChunkRef>,
    pub open_scopes: Vec<Scope>,
}

/// The registry map head: no chain parameters or anchor; `log_sequence`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryMapHead {
    pub protocol: String,
    pub profile: String,
    pub network: String,
    pub genesis_hash: BlockHash,
    pub generation: u64,
    pub recovery_epoch: RecoveryEpoch,
    /// Log entries this publication contains; the next registration's
    /// sequence.
    pub log_sequence: u64,
    pub floor_scope: u64,
    pub sealed_chunks: Vec<SealedChunkRef>,
    pub open_scopes: Vec<RegistryScope>,
}

/// One table to query for a key, the row in it, and the context its decoded
/// row is validated under.
#[derive(Clone, Copy, Debug)]
pub struct QueryTarget<'a> {
    pub table: &'a Table,
    pub row: u32,
    pub context: RowContext,
}

/// What chain and registry scopes share: the tables and how keys reach them.
pub trait ScopeTables {
    fn scope_id(&self) -> u64;
    fn state(&self) -> ScopeState;
    fn entries(&self) -> u64;
    fn n_buckets(&self) -> u32;
    fn tables(&self) -> &[Table];
    /// `[first_height, last_height]`, for chain scopes.
    fn heights(&self) -> Option<(u32, u32)>;

    /// The seed of segment 0, equal in every bucket; `None` for an empty
    /// scope. Assumes a validated scope, whose first table is bucket 0
    /// segment 0.
    fn scope_seed(&self) -> Option<u32> {
        self.tables().first().map(|t| t.seed)
    }

    /// The tables of `bucket`, in segment order (tables are listed in
    /// ascending `(bucket, segment)` order).
    fn segments(&self, bucket: u32) -> &[Table] {
        let tables = self.tables();
        let start = tables.partition_point(|t| t.bucket < bucket);
        let end = tables.partition_point(|t| t.bucket <= bucket);
        &tables[start..end]
    }

    /// The row context for `row` of `table` in this scope.
    fn row_context(
        &self,
        product: Product,
        genesis: &BlockHash,
        table: &Table,
        row: u32,
    ) -> RowContext {
        RowContext {
            product,
            genesis: *genesis,
            scope_id: self.scope_id(),
            n_buckets: self.n_buckets(),
            scope_seed: self.scope_seed().unwrap_or(table.seed),
            bucket: table.bucket,
            seed: table.seed,
            rows: table.geometry.rows(),
            row,
            heights: self.heights(),
        }
    }

    /// The tables and rows a lookup of `key` (the stored key) must query:
    /// the bucket from the scope seed, then every segment of that bucket with
    /// the row under its own seed, in segment order. Empty for a scope with
    /// no tables, where every lookup is absent. Assumes a validated scope.
    fn query_plan(
        &self,
        product: Product,
        genesis: &BlockHash,
        key: &[u8],
    ) -> Result<Vec<QueryTarget<'_>>, PlacementError> {
        let Some(scope_seed) = self.scope_seed() else {
            return Ok(Vec::new());
        };
        let scope_id = self.scope_id();
        let bucket = placement::bucket(
            product,
            genesis,
            scope_id,
            scope_seed,
            self.n_buckets(),
            key,
        )?;
        self.segments(bucket)
            .iter()
            .map(|table| {
                let row = placement::row(
                    product,
                    genesis,
                    scope_id,
                    table.seed,
                    table.geometry.rows(),
                    key,
                )?;
                Ok(QueryTarget {
                    table,
                    row,
                    context: self.row_context(product, genesis, table, row),
                })
            })
            .collect()
    }

    /// The table rules of spec 6.3, 6.4 and 10.3 for one scope.
    fn check_tables(&self, product: Product) -> Result<(), MapError> {
        let id = self.scope_id();
        let n = self.n_buckets();
        let tables = self.tables();
        if !valid_n_buckets(n) {
            return invalid(format!("scope {id}: n_buckets {n}"));
        }
        if product == Product::Registry && n != 1 {
            return invalid(format!("registry scope {id}: n_buckets must be 1"));
        }
        if self.entries() == 0 {
            if n != 1 || !tables.is_empty() {
                return invalid(format!(
                    "empty scope {id} must have one bucket and no tables"
                ));
            }
            return Ok(());
        }
        if tables
            .windows(2)
            .any(|w| (w[0].bucket, w[0].segment) >= (w[1].bucket, w[1].segment))
        {
            return invalid(format!("scope {id}: tables not in (bucket, segment) order"));
        }
        if tables.last().is_none_or(|t| t.bucket >= n) {
            return invalid(format!("scope {id}: table bucket out of range"));
        }
        let scope_seed = tables[0].seed;
        let capacity = product.slots() as u64 * PUBLISHED_GEOMETRY.rows() as u64;
        let mut total = 0u64;
        for bucket in 0..n {
            let segments = self.segments(bucket);
            if segments.is_empty() || segments.len() > MAX_SEGMENTS as usize {
                return invalid(format!(
                    "scope {id} bucket {bucket}: {} segments",
                    segments.len()
                ));
            }
            if scope_seed != 0 && segments.len() != 1 {
                return invalid(format!(
                    "scope {id}: a nonzero scope seed allows no overflow segments"
                ));
            }
            for (k, table) in segments.iter().enumerate() {
                if table.segment as usize != k {
                    return invalid(format!(
                        "scope {id} bucket {bucket}: segments not contiguous from 0"
                    ));
                }
                if table.geometry != PUBLISHED_GEOMETRY {
                    return invalid(format!(
                        "scope {id}: geometry {} is not published",
                        table.geometry
                    ));
                }
                if table.seed >= MAX_SEEDS {
                    return invalid(format!("scope {id}: seed {} out of range", table.seed));
                }
                if k == 0 && table.seed != scope_seed {
                    return invalid(format!(
                        "scope {id}: segment-0 seeds differ between buckets"
                    ));
                }
                if k > 0 && table.entries == 0 {
                    return invalid(format!(
                        "scope {id} bucket {bucket}: empty overflow segment"
                    ));
                }
                if table.entries > capacity {
                    return invalid(format!("scope {id}: table over its slot capacity"));
                }
                if table.session_id != table.expected_session_id(product, id) {
                    return invalid(format!(
                        "scope {id} bucket {bucket} segment {k}: session id"
                    ));
                }
                total = total.saturating_add(table.entries);
            }
        }
        if total != self.entries() {
            return invalid(format!(
                "scope {id}: table entries sum to {total}, not {}",
                self.entries()
            ));
        }
        Ok(())
    }
}

impl ScopeTables for Scope {
    fn scope_id(&self) -> u64 {
        self.scope_id
    }
    fn state(&self) -> ScopeState {
        self.state
    }
    fn entries(&self) -> u64 {
        self.entries
    }
    fn n_buckets(&self) -> u32 {
        self.n_buckets
    }
    fn tables(&self) -> &[Table] {
        &self.tables
    }
    fn heights(&self) -> Option<(u32, u32)> {
        Some((self.first_height, self.last_height))
    }
}

impl ScopeTables for RegistryScope {
    fn scope_id(&self) -> u64 {
        self.scope_id
    }
    fn state(&self) -> ScopeState {
        self.state
    }
    fn entries(&self) -> u64 {
        self.entries
    }
    fn n_buckets(&self) -> u32 {
        self.n_buckets
    }
    fn tables(&self) -> &[Table] {
        &self.tables
    }
    fn heights(&self) -> Option<(u32, u32)> {
        None
    }
}

fn check_header(protocol: &str, profile: &str, product: Product) -> Result<(), MapError> {
    if protocol != product.protocol() {
        return invalid(format!(
            "protocol {protocol:?} is not {}",
            product.protocol()
        ));
    }
    if profile != PROFILE {
        return invalid(format!("profile {profile:?} is not {PROFILE}"));
    }
    Ok(())
}

/// Chunk `k` has `base = 32k`, in ascending order.
fn check_chunk_refs(refs: &[SealedChunkRef]) -> Result<(), MapError> {
    for (k, r) in refs.iter().enumerate() {
        if Some(r.base) != (k as u64).checked_mul(MAP_CHUNK_SCOPES) {
            return invalid(format!("sealed chunk {k} has base {}", r.base));
        }
    }
    Ok(())
}

/// Chunks needed for `sealed` sealed scopes.
fn chunks_for(sealed: u64) -> u64 {
    sealed.div_ceil(MAP_CHUNK_SCOPES)
}

/// Frame checks shared by both chunk kinds: position, protocol, base, every
/// scope sealed, and the scope count (32 except in the last chunk).
fn check_chunk_frame<S: ScopeTables>(
    product: Product,
    refs: &[SealedChunkRef],
    position: usize,
    chunk: &MapChunk<S>,
) -> Result<(), MapError> {
    let Some(reference) = refs.get(position) else {
        return invalid(format!("no sealed chunk at position {position}"));
    };
    if chunk.protocol != product.protocol() {
        return invalid("chunk protocol");
    }
    if chunk.base != reference.base {
        return invalid(format!(
            "chunk base {} is not {}",
            chunk.base, reference.base
        ));
    }
    let count = chunk.scopes.len() as u64;
    let last = position + 1 == refs.len();
    if count == 0 || count > MAP_CHUNK_SCOPES || (!last && count != MAP_CHUNK_SCOPES) {
        return invalid(format!("chunk {} holds {count} scopes", chunk.base));
    }
    if chunk.scopes.iter().any(|s| s.state() != ScopeState::Sealed) {
        return invalid(format!("chunk {} lists an open scope", chunk.base));
    }
    for scope in &chunk.scopes {
        scope.check_tables(product)?;
    }
    Ok(())
}

impl ChainMapHead {
    /// Parses a head, refusing one over 1 MiB before reading it.
    pub fn parse(bytes: &[u8]) -> Result<Self, MapError> {
        if bytes.len() > MAX_MAP_HEAD_BYTES {
            return Err(MapError::TooLarge(bytes.len()));
        }
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Canonical bytes (compact JSON), refusing a head over 1 MiB.
    pub fn to_bytes(&self) -> Result<Vec<u8>, MapError> {
        let bytes = serde_json::to_vec(self)?;
        if bytes.len() > MAX_MAP_HEAD_BYTES {
            return Err(MapError::TooLarge(bytes.len()));
        }
        Ok(bytes)
    }

    /// The product its `protocol` names.
    pub fn product(&self) -> Option<Product> {
        Product::from_protocol(&self.protocol).filter(|p| p.is_chain())
    }

    fn check_scope(&self, product: Product, scope: &Scope, newest: bool) -> Result<(), MapError> {
        let id = scope.scope_id;
        let activation = u64::from(self.activation_height);
        let length = u64::from(self.epoch_length);
        let first = u64::from(scope.first_height);
        let last = u64::from(scope.last_height);
        let expected_first = id
            .checked_mul(length)
            .and_then(|h| h.checked_add(activation));
        if expected_first != Some(first) {
            return invalid(format!(
                "scope {id}: first_height {first} is not its epoch start"
            ));
        }
        if last < first || last > u64::from(self.anchor_height) {
            return invalid(format!(
                "scope {id}: last_height {last} outside first..=anchor"
            ));
        }
        let blocks = last - first + 1;
        let max_epochs = match product {
            Product::FirstContact => u64::from(FC_SPAN_MAX_EPOCHS),
            _ => 1,
        };
        if blocks > max_epochs * length {
            return invalid(format!("scope {id}: covers more than {max_epochs} epochs"));
        }
        let complete = scope.state == ScopeState::Sealed || !newest;
        if complete && !blocks.is_multiple_of(length) {
            return invalid(format!("scope {id}: does not end at an epoch boundary"));
        }
        if newest && last != u64::from(self.anchor_height) {
            return invalid(format!(
                "newest scope {id}: last_height is not the anchor height"
            ));
        }
        scope.check_tables(product)
    }

    fn check_successor(previous: &Scope, next: &Scope) -> Result<(), MapError> {
        if u64::from(previous.last_height) + 1 != u64::from(next.first_height) {
            return invalid(format!(
                "scope {} does not follow scope {}",
                next.scope_id, previous.scope_id
            ));
        }
        Ok(())
    }

    fn check_product(&self, product: Product) -> Result<(), MapError> {
        if !product.is_chain() {
            return invalid("registry maps use RegistryMapHead");
        }
        check_header(&self.protocol, &self.profile, product)?;
        if self.epoch_length == 0 {
            return invalid("epoch_length is zero");
        }
        Ok(())
    }

    /// Checks the head alone: protocol, profile, geometry and tables of every
    /// open scope, scope order and heights, the newest scope ending at the
    /// anchor, chunk bases, and the chunk count against the open scopes.
    pub fn validate(&self, product: Product) -> Result<(), MapError> {
        self.check_product(product)?;
        check_chunk_refs(&self.sealed_chunks)?;
        if self.anchor_height < self.activation_height {
            if !self.sealed_chunks.is_empty()
                || !self.open_scopes.is_empty()
                || self.floor_scope != 0
            {
                return invalid("a map anchored below activation lists no scopes");
            }
            return Ok(());
        }
        let count = self.open_scopes.len();
        for (i, scope) in self.open_scopes.iter().enumerate() {
            if scope.state != ScopeState::Open {
                return invalid(format!("open_scopes lists sealed scope {}", scope.scope_id));
            }
            self.check_scope(product, scope, i + 1 == count)?;
            if i > 0 {
                Self::check_successor(&self.open_scopes[i - 1], scope)?;
            }
        }
        let chunks = self.sealed_chunks.len() as u64;
        let Some(first_open) = self.open_scopes.first() else {
            if chunks == 0 {
                return invalid("a map anchored at or above activation lists no scopes");
            }
            return Ok(());
        };
        if product.index_is_scope_id() {
            if chunks_for(first_open.scope_id) != chunks {
                return invalid("sealed chunk count does not match the first open scope");
            }
            let newest = self.open_scopes.last().map_or(0, |s| s.scope_id);
            if self.floor_scope > newest {
                return invalid("floor_scope is above the newest scope");
            }
        } else {
            // First-contact: s sealed spans precede the first open span, each
            // covering 1 to 64 epochs, and fill exactly `chunks` chunks.
            let epochs = first_open.scope_id;
            let fewest = epochs.div_ceil(u64::from(FC_SPAN_MAX_EPOCHS));
            let lowest = (chunks.saturating_sub(1) * MAP_CHUNK_SCOPES + 1).max(fewest);
            let highest = (chunks * MAP_CHUNK_SCOPES).min(epochs);
            let possible = if chunks == 0 {
                epochs == 0
            } else {
                lowest <= highest
            };
            if !possible {
                return invalid("sealed chunk count does not match the first open span");
            }
        }
        Ok(())
    }

    /// Checks sealed chunk `position` against this (validated) head: frame,
    /// scopes and tables, scope ids and heights in sequence, and, for the
    /// last chunk, its join with the open scopes.
    pub fn check_chunk(
        &self,
        product: Product,
        position: usize,
        chunk: &ChainMapChunk,
    ) -> Result<(), MapError> {
        self.check_product(product)?;
        check_chunk_frame(product, &self.sealed_chunks, position, chunk)?;
        let last_chunk = position + 1 == self.sealed_chunks.len();
        let count = chunk.scopes.len();
        for (i, scope) in chunk.scopes.iter().enumerate() {
            let newest = last_chunk && i + 1 == count && self.open_scopes.is_empty();
            self.check_scope(product, scope, newest)?;
            if i > 0 {
                Self::check_successor(&chunk.scopes[i - 1], scope)?;
            }
            if product.index_is_scope_id() && scope.scope_id != chunk.base + i as u64 {
                return invalid(format!(
                    "chunk {}: scope {} at index {}",
                    chunk.base,
                    scope.scope_id,
                    chunk.base + i as u64
                ));
            }
        }
        if position == 0 && chunk.scopes[0].scope_id != 0 {
            return invalid("the first chunk does not start at scope 0");
        }
        if last_chunk && let Some(first_open) = self.open_scopes.first() {
            Self::check_successor(&chunk.scopes[count - 1], first_open)?;
            if product.index_is_scope_id() && first_open.scope_id != chunk.base + count as u64 {
                return invalid("the last chunk does not end before the first open scope");
            }
        }
        Ok(())
    }

    /// Verifies a fetched chunk's digest against its reference, parses it and
    /// checks it ([`ChainMapHead::check_chunk`]).
    pub fn parse_chunk(
        &self,
        product: Product,
        position: usize,
        bytes: &[u8],
    ) -> Result<ChainMapChunk, MapError> {
        let Some(reference) = self.sealed_chunks.get(position) else {
            return invalid(format!("no sealed chunk at position {position}"));
        };
        if map_sha256(bytes) != reference.sha256 {
            return Err(MapError::Digest);
        }
        let chunk: ChainMapChunk = serde_json::from_slice(bytes)?;
        self.check_chunk(product, position, &chunk)?;
        Ok(chunk)
    }

    /// Checks the whole listing: the head, every chunk in position order and
    /// the joins between chunks; every scope from index 0 to the newest
    /// listed once, and `floor_scope` at most the newest index.
    pub fn check_listing(
        &self,
        product: Product,
        chunks: &[ChainMapChunk],
    ) -> Result<(), MapError> {
        self.validate(product)?;
        if chunks.len() != self.sealed_chunks.len() {
            return invalid("listing needs every sealed chunk");
        }
        for (position, chunk) in chunks.iter().enumerate() {
            self.check_chunk(product, position, chunk)?;
            if position > 0 {
                let previous = chunks[position - 1].scopes.last().expect("validated chunk");
                Self::check_successor(previous, &chunk.scopes[0])?;
            }
        }
        let scopes = chunks.iter().map(|c| c.scopes.len() as u64).sum::<u64>()
            + self.open_scopes.len() as u64;
        if scopes > 0 && self.floor_scope >= scopes {
            return invalid("floor_scope is above the newest scope");
        }
        Ok(())
    }

    /// The listed open scope with `scope_id`.
    pub fn open_scope(&self, scope_id: u64) -> Option<&Scope> {
        self.open_scopes.iter().find(|s| s.scope_id == scope_id)
    }
}

impl RegistryMapHead {
    /// Parses a head, refusing one over 1 MiB before reading it.
    pub fn parse(bytes: &[u8]) -> Result<Self, MapError> {
        if bytes.len() > MAX_MAP_HEAD_BYTES {
            return Err(MapError::TooLarge(bytes.len()));
        }
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Canonical bytes (compact JSON), refusing a head over 1 MiB.
    pub fn to_bytes(&self) -> Result<Vec<u8>, MapError> {
        let bytes = serde_json::to_vec(self)?;
        if bytes.len() > MAX_MAP_HEAD_BYTES {
            return Err(MapError::TooLarge(bytes.len()));
        }
        Ok(bytes)
    }

    /// The open chunk: the one the next registration lands in.
    pub fn open_chunk(&self) -> Option<&RegistryScope> {
        self.open_scopes.first()
    }

    /// Checks the head alone: protocol and profile; exactly one open chunk,
    /// `log_sequence / 3072`, holding `log_sequence % 3072` entries, with
    /// valid tables; chunk bases and count; `floor_scope`.
    pub fn validate(&self) -> Result<(), MapError> {
        check_header(&self.protocol, &self.profile, Product::Registry)?;
        check_chunk_refs(&self.sealed_chunks)?;
        let [open] = self.open_scopes.as_slice() else {
            return invalid("a registry map lists exactly one open chunk");
        };
        if open.state != ScopeState::Open {
            return invalid("open_scopes lists a sealed chunk");
        }
        if open.scope_id != self.log_sequence / REGISTRY_CHUNK_ENTRIES
            || open.entries != self.log_sequence % REGISTRY_CHUNK_ENTRIES
        {
            return invalid("open chunk does not match log_sequence");
        }
        open.check_tables(Product::Registry)?;
        if chunks_for(open.scope_id) != self.sealed_chunks.len() as u64 {
            return invalid("sealed chunk count does not match the open chunk");
        }
        if self.floor_scope > open.scope_id {
            return invalid("floor_scope is above the open chunk");
        }
        Ok(())
    }

    /// Checks sealed chunk `position` against this (validated) head: frame,
    /// consecutive chunk ids, 3,072 entries each, tables.
    pub fn check_chunk(&self, position: usize, chunk: &RegistryMapChunk) -> Result<(), MapError> {
        check_header(&self.protocol, &self.profile, Product::Registry)?;
        check_chunk_frame(Product::Registry, &self.sealed_chunks, position, chunk)?;
        for (i, scope) in chunk.scopes.iter().enumerate() {
            if scope.scope_id != chunk.base + i as u64 {
                return invalid(format!(
                    "registry chunk {} at index {}",
                    scope.scope_id,
                    chunk.base + i as u64
                ));
            }
            if scope.entries != REGISTRY_CHUNK_ENTRIES {
                return invalid(format!(
                    "sealed registry chunk {} is not full",
                    scope.scope_id
                ));
            }
        }
        let next = chunk.base + chunk.scopes.len() as u64;
        if position + 1 == self.sealed_chunks.len()
            && self.open_chunk().is_some_and(|o| o.scope_id != next)
        {
            return invalid("the last chunk does not end before the open chunk");
        }
        Ok(())
    }

    /// Verifies a fetched chunk's digest, parses and checks it.
    pub fn parse_chunk(&self, position: usize, bytes: &[u8]) -> Result<RegistryMapChunk, MapError> {
        let Some(reference) = self.sealed_chunks.get(position) else {
            return invalid(format!("no sealed chunk at position {position}"));
        };
        if map_sha256(bytes) != reference.sha256 {
            return Err(MapError::Digest);
        }
        let chunk: RegistryMapChunk = serde_json::from_slice(bytes)?;
        self.check_chunk(position, &chunk)?;
        Ok(chunk)
    }

    /// Checks the head and every chunk.
    pub fn check_listing(&self, chunks: &[RegistryMapChunk]) -> Result<(), MapError> {
        self.validate()?;
        if chunks.len() != self.sealed_chunks.len() {
            return invalid("listing needs every sealed chunk");
        }
        for (position, chunk) in chunks.iter().enumerate() {
            self.check_chunk(position, chunk)?;
        }
        Ok(())
    }
}
