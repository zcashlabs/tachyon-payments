//! Constants of spec section 15, and the fixed sizes of sections 6, 9 and 10.
//!
//! Values marked provisional in the spec are still the contract until the spec
//! changes them.

use crate::geometry::Geometry;

/// Default epoch length in blocks. Not final in the draft ZIPs: deployments
/// read it from configuration per network and publish it in the map
/// (`epoch_length`), and clients use the map's value.
pub const EPOCH_LENGTH: u32 = 4096;
/// Bytes per table row (spec 6.1).
pub const ROW_BYTES: usize = 8192;
/// Follow-up memo ciphertext (provisional, D1).
pub const MEMO_CT_BYTES: usize = 128;
/// ML-KEM-768 ciphertext (provisional, D1).
pub const KEM_CT_BYTES: usize = 1088;
/// ML-KEM-768 encapsulation key (provisional, D1).
pub const KEM_EK_BYTES: usize = 1184;
/// Entries in one on-chain envelope (provisional, D1).
pub const MAX_ENVELOPE_ENTRIES: usize = 16;
/// Wallet-state ciphertext bound (provisional, D1).
pub const MAX_STATE_BYTES: usize = 1024;
/// Largest `n_buckets` of a scope; buckets are powers of two up to this.
pub const MAX_BUCKETS: u32 = 64;
/// Seeds are searched in `0..MAX_SEEDS`; every published seed is below it.
pub const MAX_SEEDS: u32 = 64;
/// Segments per bucket, segment 0 included.
pub const MAX_SEGMENTS: u32 = 8;
/// Blocks below the node tip that the publisher ingests to.
pub const CONFIRMATION_DEPTH: u32 = 2;
/// A scope seals once its last block is this many blocks below the tip
/// (provisional).
pub const SEAL_DEPTH: u32 = 100;
/// A first-contact span closes at the first epoch boundary with at least this
/// many entries (provisional).
pub const FC_SPAN_TARGET: u64 = 3072;
/// A first-contact span closes after at most this many epochs (provisional).
pub const FC_SPAN_MAX_EPOCHS: u32 = 64;
/// Registrations per registry chunk (provisional, D3).
pub const REGISTRY_CHUNK_ENTRIES: u64 = 3072;
/// Lifetime of a registry challenge (provisional).
pub const REGISTRY_CHALLENGE_SECONDS: u64 = 600;
/// Default registry proof-of-work difficulty (spec 9.3).
pub const REGISTRY_DEFAULT_DIFFICULTY_BITS: u32 = 20;
/// Sealed scopes per map chunk.
pub const MAP_CHUNK_SCOPES: u64 = 32;
/// The only geometry version 1 publishes (spec 6.1).
pub const PUBLISHED_GEOMETRY: Geometry = Geometry::Tachyon4k8;

/// The PIR profile every table is served at (spec 6.6).
pub const PROFILE: &str = "reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1";
/// Query and response header: `magic(4) || session_id(32) || nonce(16)`.
pub const HEADER_BYTES: usize = 52;
/// Largest query body a host reads (spec 10.4).
pub const MAX_QUERY_BODY_BYTES: usize = 512 * 1024;
/// Deadline for receiving a query or registration body, in seconds.
pub const UPLOAD_DEADLINE_SECONDS: u64 = 15;
/// Largest map head, in bytes (spec 10.3): two open scopes of 64 buckets x 8
/// segments must fit.
pub const MAX_MAP_HEAD_BYTES: usize = 1024 * 1024;
/// How long a query host keeps a displaced publication, in seconds (spec 10.5,
/// 11.6).
pub const DISPLACED_RETENTION_SECONDS: u64 = 600;

/// Registry public key `pk` (spec 9.1).
pub const PK_BYTES: usize = 32;
/// Registry address `pk || ek` (spec 9.1).
pub const ADDR_BYTES: usize = PK_BYTES + KEM_EK_BYTES;
/// Proof-of-work nonce in a registration body.
pub const POW_NONCE_BYTES: usize = 8;
/// Registration body `challenge(32) || nonce(8) || addr(1216)` (spec 9.3).
pub const REGISTER_BODY_BYTES: usize = 32 + POW_NONCE_BYTES + ADDR_BYTES;

const _: () = assert!(ADDR_BYTES == 1216);
const _: () = assert!(REGISTER_BODY_BYTES == 1256);
