//! Wallet-facing contract of the Tachyon payment PIR services (spec
//! `docs/spec/spec-tachyon-payment-pir.md`, section 4.2): constants, row
//! layouts, placement hashing, PIR parameters, session ids, map and header
//! types, error bodies and the registry wire types, for all four products.
//!
//! No server, chain or async dependencies: a wallet, the probe client, the
//! publisher and the query host all build on this crate. It defines its own
//! 32-byte types ([`BlockHash`], [`Bytes32`]) rather than depending on
//! `tachyon-chain`.
//!
//! Byte order (spec 3): block hashes are held in protocol order and written
//! in JSON in display order; every other 32-byte value is plain hex. Integers
//! are little-endian.

pub mod bytes;
pub mod constants;
pub mod entry;
pub mod error_body;
pub mod geometry;
pub mod map;
pub mod params;
pub mod placement;
pub mod product;
pub mod query;
pub mod registry;
pub mod row;
pub mod session;

pub use bytes::{BlockHash, Bytes32, HexError, RecoveryEpoch};
pub use constants::*;
pub use entry::{Entry, FirstContactEntry, MemoEntry, RegistryEntry, TachygramEntry};
pub use error_body::{ErrorBody, ErrorCode, ErrorStatus};
pub use geometry::Geometry;
pub use map::{
    ChainMapChunk, ChainMapHead, MapChunk, MapError, QueryTarget, RegistryMapChunk,
    RegistryMapHead, RegistryScope, Scope, ScopeState, ScopeTables, SealedChunkRef, Table,
    map_sha256,
};
pub use params::{PirParams, SeedPurpose, seed};
pub use placement::{PlacementError, bucket, n_buckets_for, placement_hash, row};
pub use product::Product;
pub use query::{FrameError, QueryHeader};
pub use registry::{Address, Challenge, RegisterRequest, Registration, pow_hash, pow_meets};
pub use row::{RowContext, RowError, encode_row, lookup, validate_row};
pub use session::{SessionParts, session_id};
