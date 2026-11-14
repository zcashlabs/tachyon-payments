# Integrating with wallet-pir

Status: research guide, 2026-11-14. Nothing in this document is implemented.
It describes `../wallet-pir` at revision `40b0ba6a` (`40b0ba6a26f87fe0aaa694602879f105abcd218f`)
and the crates.io packages it pins (`zakura-pir-*` `=0.0.1-rc5`, `reinspiring`
`=0.1.3-rc.1` patched to `valargroup/ipir-sp` rev `5ea080a3`).

The contract is [`docs/spec/spec-tachyon-payment-pir.md`](../spec/spec-tachyon-payment-pir.md)
(called "the spec" below). This guide tells you how to build the spec's crates on
wallet-pir's libraries without reading wallet-pir. Section 8 lists the places
where the spec disagrees with wallet-pir and proposes corrections.

Path conventions:

- `wp/...` is a path in `../wallet-pir` (relative to `/home/ryqford/dev/zcash/wallet-pir`).
- `reg/<crate>/...` is a path in
  `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/<crate>-0.0.1-rc5/`.
- `ipir/...` is a path in `~/.cargo/git/checkouts/ipir-sp-ecbe94d2bf94a34c/5ea080a/`.

Line numbers refer to those exact revisions.

## Contents

1. Native PIR engine for arbitrary 8,192-byte rows
2. Publication model (`pir-publication`)
3. Publisher host (`pir-publisher`, `pir-pipeline`)
4. Query host
5. Row validation (Status `decode_row`, Receiver, txid)
6. End-to-end test harness
7. Recommended design for this workspace
8. Spec statements that conflict with wallet-pir

## 1. Native PIR engine for arbitrary 8,192-byte rows

### 1.1 Who does what

There is no "build a P8 table from bytes" function in a single call. Both of
wallet-pir's 8-bit products assemble it from the same five public primitives:

| Step | Side | Function | Crate (feature) |
|---|---|---|---|
| Derive query masks | both | `Profile::public_query_masks(&self, seed: [u8; 32], rows: usize, cols: usize) -> Result<Vec<Vec<u64>>, String>` | `zakura-pir-protocol` (`native`), `reg/zakura-pir-protocol/src/native.rs:109` |
| Derive packing setup | both | `NativeSetup::new(params: NativeParams, seed: [u8; 32]) -> Self` with `P8.params()` | `reinspiring`, re-exported as `zakura_pir_protocol::native::NativeSetup` (`ipir/reinspiring/src/native.rs:180`) |
| Public hint | server | `hint<'a, T: Entry + 'a>(masks: &[Vec<u64>], rows: usize, cols: usize, column: impl Fn(usize) -> &'a [T] + Sync) -> Result<Vec<Vec<Vec<u64>>>, String>` | `zakura-pir-server-core`, `reg/zakura-pir-server-core/src/native.rs:45` |
| Two-mask preprocessing | server | `preprocess(setup: &NativeSetup, hint: &[Vec<Vec<u64>>]) -> Result<Vec<NativePreprocessed>, String>` | `zakura-pir-server-core`, `native.rs:82` |
| Public setup bytes | server | `ProfilePublicationExt::publish<B: Borrow<NativePreprocessed>>(&self, blocks: &[B]) -> Result<Vec<u8>, String>` | `zakura-pir-server-core`, `native.rs:8-33` |
| Parse upload | server | `ProfileQueryExt::parse_accepted(&self, setup: &NativeSetup, bytes: &[u8], rows: usize) -> Result<(NativeKeys, Vec<u64>), String>` | `zakura-pir-query-server`, `reg/zakura-pir-query-server/src/native.rs:24-29,91-102` |
| Scan | server | `PreparedU16Query::new(words: &[u64], q: u64)`, `PreparedU16Query::interleave_u8_columns(db: &mut [u8], rows: usize)`, `multiply_interleaved_u8(&self, db: &[u8], out: &mut [u64])` | `reinspiring::native_kernel`, `ipir/reinspiring/src/native_kernel.rs:19,64,79` |
| Pack reply | server | `ProfileQueryExt::pack<B: Borrow<NativePreprocessed> + Sync>(&self, blocks: &[B], keys: &NativeKeys, intermediate: &[u64]) -> Result<Vec<u8>, String>` | `zakura-pir-query-server`, `native.rs:41-46,126-153` |
| Build query | client | `ProfileClientExt::prepare(&self, setup: &NativeSetup, masks: &[Vec<u64>], rows: usize, target: usize) -> Result<(NativeSecret, Vec<u8>), String>` | `zakura-pir-client` (`native`), `reg/zakura-pir-client/src/native.rs:13-19,52-67` |
| Decode reply | client | `ProfileClientExt::decode_cols(&self, secret: &NativeSecret, public: &[u8], body: &[u8], cols: usize) -> Result<Vec<u8>, String>` | `zakura-pir-client` (`native`), `native.rs:40-45,111-158` |

All of them are methods on, or take, `zakura_pir_protocol::native::P8`
(`reg/zakura-pir-protocol/src/native.rs:77-85`):

```rust
pub const P8: Profile = Profile {
    p_bits: 8, gadget_bits: 27, ell: 1, mask_bits: 22,
    nearest_bits: None, dithered_bits: 28, response_bits: 10,
};
```

with `D = 2048` and `Q = 1 << 54` (`native.rs:9,12`). This is the profile named
`reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1`; the string constant
`P8_PROFILE` lives in `zakura_pir_protocol::transparent::native`
(`reg/zakura-pir-protocol/src/transparent/native.rs:14`), behind the
`transparent` feature. Define the string in `tachyon-pir-protocol` rather than
enabling `transparent` just for it.

How the two wallet-pir products use these:

- **txid** wraps them in `transparent_shared::runtime::TableRuntime`
  (`wp/src/transparent/transparent-shared/src/runtime/table_runtime.rs:73-228`):
  `TableRuntime::build(shared: &SharedParams, rows: &[u8]) -> Result<Self, String>` (line 89),
  `evaluate(&self, shared: &SharedParams, binding: [u8; 8], body: &[u8]) -> Result<Vec<u8>, String>` (line 150),
  `answer(&self, binding: [u8; 8], query: &ParsedQuery)` (line 207),
  `self_check(&self, shared, rows) -> Result<usize, String>` (line 169).
  Its parameters come from `TableProfile::new(schema, geometry, table, rows, row_bytes)`
  (`reg/zakura-pir-protocol/src/transparent/native.rs:138-202`).
- **Receiver** wraps them in `zakura_pir_query_server::receiver::server::Server<M>`
  (`reg/zakura-pir-query-server/src/receiver/server.rs:15-77,126-196`), with
  parameters from `zakura_pir_protocol::receiver::native::profile(kind, rows)`
  (`reg/zakura-pir-protocol/src/receiver/native.rs:36-65`).

Neither wrapper is usable for our tables:

- `TableProfile::new` chooses the profile with `profile_for(table)`
  (`transparent/native.rs:18-23`), which returns `P8` only for the table names
  `"directory"`, `"pages"` and `"txdirectory"` and `P16` for anything else. A
  table named `"memo"` silently becomes a 16-bit table with different sizes.
  `transparent-shared` also pulls in axum, sysinfo, the runtime cache and
  txid's `Geometry`/`Table` enums.
- `receiver::server::Server` only accepts Receiver `DirectoryTable` /
  `CompanionTable` manifests with 4,096-byte rows.

**Write our own ~120-line runtime in `tachyon-pir-table` on the free
primitives, exactly as Receiver's `Prepared` does** (`receiver/server.rs:29-77`).
The scratch program in 1.4 is a complete, tested version of it.

### 1.2 Code sketch (compiled and run, see 1.4)

```rust
use rayon::prelude::*;
use reinspiring::native_kernel::PreparedU16Query;
use zakura_pir_client::native::ProfileClientExt as _;            // client only
use zakura_pir_protocol::native::{D, NativeSetup, P8, Q};
use zakura_pir_query_server::native::ProfileQueryExt as _;       // parse_accepted, pack
use zakura_pir_server_core::native::{
    hint, preprocess, NativePreprocessed, ProfilePublicationExt as _,
};

pub const ROW_BYTES: usize = 8192;

/// Data-independent parameters both sides derive from public seeds.
pub struct Shared { pub rows: usize, pub cols: usize, pub setup: NativeSetup, pub masks: Vec<Vec<u64>> }

impl Shared {
    pub fn new(rows: usize, mask_seed: [u8; 32], packing_seed: [u8; 32]) -> Self {
        let cols = ROW_BYTES / P8.entry_bytes(); // 8192: one byte per coefficient
        Self {
            rows, cols,
            setup: NativeSetup::new(P8.params(), packing_seed),
            masks: P8.public_query_masks(mask_seed, rows, cols).unwrap(),
        }
    }
}

pub struct Runtime { pre: Vec<NativePreprocessed>, db: Vec<u8>, pub public: Vec<u8> }

impl Runtime {
    /// `rows` is `shared.rows * ROW_BYTES` row-major bytes, already validated.
    pub fn build(shared: &Shared, rows: &[u8]) -> Self {
        let n = shared.rows;
        // Column-major: entry c of row r at db[c * n + r].
        let mut db = vec![0u8; n * ROW_BYTES];
        db.par_chunks_mut(n).enumerate().for_each(|(col, column)| {
            for (row, e) in column.iter_mut().enumerate() { *e = rows[row * ROW_BYTES + col]; }
        });
        let h = hint(&shared.masks, n, shared.cols, |c| &db[c * n..(c + 1) * n]).unwrap();
        let pre = preprocess(&shared.setup, &h).unwrap();
        drop(h);
        db.par_chunks_mut(n * 16).for_each(|band| PreparedU16Query::interleave_u8_columns(band, n));
        let public = P8.publish(&pre).unwrap();          // 45,056 bytes
        Self { pre, db, public }
    }

    /// `payload` is the bytes after our 52-byte header. Returns the packed reply body.
    pub fn answer(&self, shared: &Shared, payload: &[u8]) -> Result<Vec<u8>, String> {
        let (keys, query) = P8.parse_accepted(&shared.setup, payload, shared.rows)?; // length-checked
        let prepared = PreparedU16Query::new(&query, Q).map_err(|e| e.to_string())?;
        let mut inter = vec![0u64; self.pre.len() * D];
        inter.par_chunks_mut(16)
            .zip(self.db.par_chunks(shared.rows * 16))
            .for_each(|(out, band)| prepared.multiply_interleaved_u8(band, out));
        P8.pack(&self.pre, &keys, &inter)                // 10,240 bytes
    }
}

// Client: query row `target`, decode its 8,192 bytes.
let (secret, upload) = P8.prepare(&shared.setup, &shared.masks, shared.rows, target)?;
let body = /* POST header || upload, strip the 52-byte echoed header */;
let row: Vec<u8> = P8.decode_cols(&secret, &public_setup_bytes, &body, shared.cols)?; // 8,192 bytes
```

The scan is the same one txid uses (`table_runtime.rs:207-227`). Receiver uses
a plain scalar fold instead (`receiver/server.rs:108-124`); both give identical
replies. `PreparedU16Query::supports_interleaved()` reports whether the
AVX-512 VNNI path is in use; the portable path is chosen automatically.

### 1.3 Exact sizes and the "single length"

`P8` has no nearest-rounded width (`nearest_bits: None`), so a full-row query
has exactly one accepted length:

```rust
// reg/zakura-pir-protocol/src/native.rs:190-216
pub const fn request_len_bits(&self, rows: usize, bits: usize) -> usize {
    self.key_bytes() + (rows * bits).div_ceil(8)
}
pub const fn accepted_query_bits(&self, rows: usize, len: usize) -> Option<usize> { ... }
```

Obtain it in code as `P8.request_len_bits(rows, P8.dithered_bits)` and check
it with `P8.accepted_query_bits(rows, payload.len()).is_some()`, or simply let
`parse_accepted` refuse any other length. Receiver does exactly this, header
included: `query_bytes(rows) = HEADER_BYTES + P8.request_len_bits(rows as usize, P8.dithered_bits)`
and `response_bytes() = HEADER_BYTES + P8.response_len(COLS)`
(`reg/zakura-pir-protocol/src/receiver/native.rs:72-81`). Do not use
`parse_selection`: it also accepts a shorter prefix selection (Receiver
companions only).

| Quantity | Formula | `tachyon-2k8` (2,048 x 8,192) | `tachyon-4k8` (4,096 x 8,192) |
|---|---|---:|---:|
| Packing key `K_g` | `P8.key_bytes()` = `1 * 2048 * 54 / 8` | 13,824 | 13,824 |
| Selection | `rows * 28 / 8` | 7,168 | 14,336 |
| **Payload** (after our header) | `P8.request_len_bits(rows, 28)` | **20,992** | **28,160** |
| Request body with 52-byte header | `52 + payload` | 21,044 | 28,212 |
| **Reply body** | `P8.response_len(8192)` = `8192 * 10 / 8` | **10,240** | **10,240** |
| Response with echoed 52-byte header | `52 + 10,240` | 10,292 | 10,292 |
| **Public setup** (raw) | `P8.public_len(8192)` = `2 * 8192 * 22 / 8` | **45,056** | **45,056** |
| Decoded row | `cols * P8.entry_bytes()` | 8,192 | 8,192 |
| Packing blocks per row | `8192 / P8.instance_bytes()` (2,048) | 4 | 4 |

wallet-pir's published figures (28,168 B upload, 10,256 B reply, "60.1 KB"
setup, `wp/docs/txid/txid-display.md:200-204`) are txid's framing: an 8-byte
binding before the payload (`SharedParams::query_bytes() = 8 + request_bytes`,
`runtime/parameters.rs:138-140`), a 16-byte binding-plus-epoch header before
the reply (`response_bytes() = 16 + response_bytes`, line 187-189), and the
setup carried base64 inside JSON (`public_params: BASE64_STANDARD.encode(&built.public_params)`,
`wp/src/pir-query-server/src/transparent/tx_detail/service.rs:798`).

### 1.4 Measurements (this workspace's machine, 2026-11-14)

Scratch crate:
`/tmp/claude-1000/-home-ryqford-dev-zcash-tachyon-payments/7496c098-abf7-4614-b111-1ac45df7b727/scratchpad/p8bench`
(`src/main.rs`, depends only on the four `zakura-pir-*` crates, `reinspiring`,
`rayon`, `sha2`; built with `--profile release-fast`, the same profile as our
workspace; wallet-pir's `Cargo.lock` copied for identical versions). Host: AMD
Ryzen 9 9950X3D (16 cores / 32 threads, AVX-512 VNNI and VBMI, interleaved
kernel active). Rows: pseudo-random bytes in the first 5,000 bytes of each row,
zero after (roughly a 61 %-full bucket row). Every round trip decoded the exact
row.

| | 2k8, 32 threads | 4k8, 32 threads | 2k8, 4 threads | 4k8, 4 threads | 2k8, 1 thread | 4k8, 1 thread |
|---|---:|---:|---:|---:|---:|---:|
| Prepare runtime (hint + preprocess + interleave + publish) | 0.85-1.15 s | 0.84-0.88 s | 1.19-1.22 s | 1.39 s | 4.26-4.28 s | 5.01 s |
| Client `prepare` (query) | 4.3 ms | 5.4 ms | 4.0 ms | 5.4 ms | 4.0 ms | 5.2 ms |
| Server answer (parse + scan + pack) | 2.2 ms | 2.9 ms | 3.3 ms | 3.0 ms | 4.3 ms | 4.6 ms |
| Client `decode_cols` | 4.2 ms | 4.2 ms | 4.1 ms | 4.2 ms | 4.3 ms | 4.1 ms |

Deriving `Shared` (masks + packing setup) takes about 0.4 ms.

Memory per table:

| | 2k8 | 4k8 |
|---|---:|---:|
| Held by the runtime, measured (`db + public + 16 + Σ(40 + coefficient_bytes)`, the formula of `TableRuntime::held_bytes`, `table_runtime.rs:111-123`) | 131,772,600 B = **125.67 MiB** | 148,549,816 B = **141.67 MiB** |
| wallet-pir's planning bound (`reserved_bytes`, 8-byte matrix words) | 151,171,248 B = 144.17 MiB | 160.17 MiB |
| Process RSS growth during the first build (transient hint, column copy, allocator) | +403 / +501 MiB | +212 / +272 MiB |

The held size on this CPU is below wallet-pir's bound because the compiled
matrix is stored packed at 27 bits on AVX-512 VBMI hosts
(`wp/src/transparent/transparent-shared/src/runtime/cache/tests.rs:348-353`).
Plan with the bound (144.17 / 160.17 MiB) plus a transient build peak of a
few hundred MiB. **Only the 16-32 MiB database term depends on rows**; the four
compiled matrices (about 32 MiB each) depend on the row width
(`runtime/parameters.rs:4-38`; `cache/tests.rs:338-346`: "the database doubles,
the compiled matrices do not"). A `2k8` table therefore costs about 90 % of a
`4k8` one.

### 1.5 Determinism

- `masks` and `setup` depend only on the two 32-byte seeds and the shape. Both
  wallet-pir products derive the seeds from a domain string and the geometry,
  never from data: Receiver hashes `SCHEMA || "/native-p8/" || purpose || "\0" || kind || "\0" || rows_le`
  (`receiver/native.rs:45-56`); txid hashes
  `schema || "/native-reinspiring-p8-g27x1/" || purpose || "\0" || geometry || "\0" || table`
  with purposes `query-masks` and `packing-setup` (`transparent/native.rs:47-70`).
  So every table of one geometry shares masks and setup, and a client derives
  them once per geometry.
- The public setup bytes are a deterministic function of the seeds and the row
  bytes. Verified by the scratch program: two builds from the same rows give
  identical `public` and identical database bytes; flipping one row byte
  changes `public`.
- Only the client query is random (`prepare` draws from `OsRng`,
  `zakura-pir-client/src/native.rs:59-61`).

So the spec's "public setup material is deterministic per table" holds, and
`public_sha256` is a pure function of `rows_sha256` and the geometry. The
consequence is that a publisher must run a full preparation (about 1 s per
table on a multi-core host, 4-5 s single-threaded) to learn `public_sha256`
for the session id (section 8).

Recommended seeds for this workspace, mirroring Receiver:

```rust
fn seed(product: &str /* "memo" ... */, geometry: &str /* "tachyon-4k8" */, purpose: &[u8]) -> [u8; 32] {
    Sha256::new()
        .chain_update(b"tachyon-pir/v1/native-p8/")
        .chain_update(product).chain_update(b"\0")
        .chain_update(geometry).chain_update(b"\0")
        .chain_update(purpose)            // b"query-masks" | b"packing-setup"
        .finalize().into()
}
```

### 1.6 Thread safety

`NativeSetup`, `NativePreprocessed`, `NativeSecret` and a `Runtime` holding
them are `Send + Sync` (all fields are plain `Vec`s; checked by a compile-time
`assert_send_sync::<T>()` in the scratch program). `answer` takes `&self`, so
one `Arc<Runtime>` serves concurrent queries; each call parallelises
internally on the global rayon pool (Receiver and txid both rely on this).
Admission therefore matters more than locking: two concurrent answers already
use the whole pool. Prepare runtimes on a separate pool or `spawn_blocking`
so builds do not starve queries (txid uses `build_pool`/`validation_pool`,
`runtime/scheduling.rs`).

### 1.7 How txid chooses `txid-2k8` or `txid-4k8`

There is no per-table rule. The geometry is fixed per publication root, chosen
by the operator when the root is bootstrapped:

- Both geometries are defined in `zakura_pir_protocol::transparent::shard::display`
  (`reg/zakura-pir-protocol/src/transparent/shard/display/geometry.rs:16-37`):
  `TXID_2K8` (2,048 x 8,192) and `TXID_4K8` (4,096 x 8,192), both 72 entries of
  113 bytes per row plus 56 zero bytes (`txid.rs:14-17,444`).
- `DisplayRoot { geometry: String, seal: DisplaySealParams, ... }`
  (`wp/src/transparent/transparent-publisher/src/tx_detail/publisher.rs:63-75`)
  records it; `display_by_name` validates it (line 79-82). The CLI default is
  `--geometry txid-2k8` with `--archive-target 90000` and
  `--recent-window 90000` (`commands/detail.rs:27-41`).
- Deployment switched the whole service to a fresh `txid-4k8` root on
  2026-10-13 with 180,000-entry archives and window (61 % fill either way),
  because each lookup then hides among twice the entries for 7 KB more upload,
  and the fleet needed about half as many runtimes (15.2 GiB instead of
  26.9 GiB) (`wp/docs/transparent/remaining-work.md:193-216`,
  `wp/docs/txid/CHANGELOG.md:24-40`). `txid-2k8` "stays in the registry" for
  rollback only.

Recommendation for us (proposed spec text in section 8, item C3): **use
`tachyon-4k8` for every table in version 1** and keep `tachyon-2k8` only as a
defined-but-unused geometry. The saving from 2k8 is 7,168 bytes of upload,
about 16 MiB of 160 MiB, and about 15 % of build time; the cost is a second
code path, a second set of seeds, and half the anonymity set per table. If a
rule is wanted, make it a pure function of public counts: a scope with
`n_buckets = 1` whose entry count is at most `target_load * 2048` uses
`tachyon-2k8`; everything else `tachyon-4k8`.

## 2. Publication model (`pir-publication`)

Crate: `wp/src/shared/pir-publication` (no features except `test-support`, which
only injects record-write faults: `records.rs:15-26`). It does no network I/O
and has **no HTTP handlers**: each product writes its own three routes on top of
it. Receiver and Status use the full envelope contract; **txid does not** (it
has an older Transparent envelope, `transparent_shared::publication::Current`,
`contracts.rs:14`, routes `current`, `inventories/<sha>`, `artifacts/<sha>`,
`anchors/<h>/<hash>`, and always `recovery_epoch: 0`). Mirror **Receiver** for
publication and txid only for the table layout.

### 2.1 Objects (content addressing, install)

```rust
// object_reference.rs
pub const OBJECTS_PATH: &str = "objects/";                       // :8
pub fn object_digest(bytes: &[u8]) -> String                      // :11 lowercase hex SHA-256
pub fn is_object_digest(name: &str) -> bool                       // :17 exactly 64 [0-9a-f]
#[serde(deny_unknown_fields)]
pub struct ObjectReference { pub sha256: String, pub bytes: u64 } // :28
impl ObjectReference {
    pub fn for_bytes(bytes: &[u8]) -> Self                         // :59
    pub fn validate(&self, max_bytes: u64) -> Result<(), ObjectReferenceError> // :67
    pub fn verify(&self, bytes: &[u8]) -> Result<(), ObjectReferenceError>     // :74 length, then digest
}
// published_objects.rs (publisher side)
pub struct PublishedObjectStore { .. }                                                 // :26
impl PublishedObjectStore {
    pub fn open(directory: PathBuf) -> io::Result<Self>                                // :33
    pub fn store(&self, bytes: &[u8]) -> io::Result<ObjectReference>                   // :48
    pub fn store_stream(&self, input: impl io::Read) -> io::Result<ObjectReference>    // :52
    pub fn retain_publication(&self, objects: impl IntoIterator<Item = ObjectReference>,
        activatable_until_ms: u64, now_ms: u64) -> io::Result<()>                      // :62
    pub fn retain_replacement(&self, objects: impl IntoIterator<Item = ObjectReference>,
        predecessor_until_ms: u64, now_ms: u64) -> io::Result<()>                      // :74
    pub fn open_retained(&self, name: &str) -> io::Result<fs::File>                    // :135
}
// object_cache.rs (query-host side)
pub struct ObjectCache { .. }                                                          // :25
// open (:126), object_path (:153), open_verified(&ObjectReference) (:165),
// open_received (:199, length only), begin_partial (:214), collect_unreferenced (:241)
```

Install is atomic (`object_installation.rs:64-194`): write and hash into a
private `.install-<rand>/object` (mode 0700), seal 0444, `sync_all`, re-check
file identity, `hard_link` to `<dir>/<sha256>` (an existing file must re-hash
equal), fsync the directory. Objects are never modified. `retain_*` persists
`retained-publications.json` and deletes every digest-named file no retained
publication references (`published_objects.rs:82-132`); storing does not
authorise serving.

### 2.2 Envelope, descriptor, fence

```rust
// publication_envelope.rs
pub const CURRENT_PATH: &str = "current";                                          // :14
#[serde(deny_unknown_fields)]
pub struct PublicationScope { pub product: String, pub chain: [u8; 32], pub source: [u8; 32] } // :21
pub struct AuthorityFence { pub recovery_epoch: u64, pub latest_withdrawal: u64 }   // :33
pub struct PublicationReference<D> {                                                // :55
    pub publication: PublicationIdentity,   // SHA-256(json(&(material, inventory, descriptor))), :94-102
    pub material: MaterialIdentity,         // [u8; 32]
    pub inventory: ObjectReference,         // the inventory JSON, itself an object
    pub descriptor: D,
}
impl<D: Serialize> PublicationReference<D> {
    pub fn new(material: MaterialIdentity, inventory: ObjectReference, descriptor: D) -> Self // :75
}
pub struct PublicationEnvelope<D> {                                                 // :109
    pub scope: PublicationScope, pub sequence: u64, pub eligible: bool,
    pub fence: AuthorityFence, pub publication: Option<PublicationReference<D>>,
}
pub fn conditional_token<D: Serialize>(envelope: &PublicationEnvelope<D>) -> String  // :242 ETag
```

- The **descriptor** `D` is the product's own type; the product schema string
  goes in `scope.product`. Receiver: `PublicationScope { product: "receiver-prepared-distribution-v1", chain: genesis, source: SHA256(activation_le) }`
  (`wp/src/receiver/receiver-pir-server/src/distribution.rs:80-86`).
- `validate` requires `sequence > 0`, `recovery_epoch > 0`,
  `latest_withdrawal <= sequence`, an eligible envelope to carry a publication,
  and a verifying identity (`:142`). `EnvelopeSuccession::classify` (`:202`)
  refuses another scope, an older sequence, a regressed fence or the same
  sequence with different bytes; views are fenced when `!next.eligible` or the
  fence changed.

### 2.3 Authority (publisher) and floor (consumer)

```rust
// publication_authority.rs
pub enum AuthorityTransition<D> { Publish(PublicationReference<D>), Withdraw, Recover }   // :12
pub enum PublicationAuthorityError { WrongScope, RecoveryRequired, Exhausted, Corrupt(serde_json::Error), Io(io::Error) } // :24
impl<D: Clone + Serialize + DeserializeOwned> PublicationAuthority<D> {
    pub fn open(path: PathBuf, scope: PublicationScope) -> Result<Self, PublicationAuthorityError> // :76
    pub fn current(&self) -> &PublicationEnvelope<D>                                             // :118
    pub fn apply(&mut self, transition: AuthorityTransition<D>) -> Result<&PublicationEnvelope<D>, PublicationAuthorityError> // :139
}
// authority_check.rs
pub const AUTHORITY_CHECK_PATH: &str = "authority/check";   // :9
pub const MAX_AUTHORITY_SUBJECTS: usize = 256;              // :12
pub struct AuthorityCheckRequest<S> { pub authority: AuthorityBinding, pub subjects: Vec<S> }          // :18
pub struct AuthorityCheckResponse<S> { pub authority: AuthorityBinding, pub answers: Vec<SubjectAnswer<S>> } // :28
pub struct SubjectAnswer<S> { pub subject: S, pub still_authoritative: bool }                          // :38
impl<S> AuthorityCheckRequest<S> {
    pub fn admit<D>(&self, current: &PublicationEnvelope<D>) -> Result<(), AuthorityCheckRefusal>       // :80
}
// authority_floor.rs (query host)
pub struct SourceBinding { pub publisher_origin: String, pub product_namespace: String, pub chain: [u8; 32] } // :21
impl<D: Clone + PartialEq + Serialize + DeserializeOwned> AuthorityFloor<D> {
    pub fn open(path: PathBuf, source: SourceBinding) -> Result<Self, AuthorityFloorError>               // :131
    // classify (:182), record (:197), invalidate (:214), pending_write (:227), confirm_durable (:243)
}
```

- **Recovery epoch.** `PublicationAuthority::open` on every process start
  reserves `saved.sequence + 2`, applies `Recover` (`recovery_epoch += 1`,
  `eligible = false`, `latest_withdrawal = sequence`) and writes durably
  (`:76-116`). A restarted publisher is therefore withdrawn in a new epoch and
  must `Publish` again. Receiver re-publishes its previous inventory under the
  new epoch before the first rebuild (`Distribution::restore`,
  `wp/src/receiver/receiver-indexer/src/distribution.rs:98-154`) and puts
  `recovery_epoch` into its descriptor so the republished identity is new
  (`receiver-pir-server/src/distribution.rs:21-29`).
- A failed write marks the authority uncertain; then `Publish` returns
  `RecoveryRequired`. The commit idiom (`receiver-indexer/src/distribution.rs:514-524`):

  ```rust
  match authority.apply(AuthorityTransition::Publish(candidate.reference.clone())) {
      Err(pir_publication::PublicationAuthorityError::RecoveryRequired) => {
          authority.apply(AuthorityTransition::Recover)?;
          authority.apply(AuthorityTransition::Publish(candidate.reference))?;
      }
      result => { result?; }
  }
  ```
- **Generation** is not a `pir-publication` concept. It is Status's descriptor
  field (`StatusDescriptor { generation: u64 }`,
  `wp/src/status/status-shared/src/publication.rs:29`) from its own durable
  counter (`status-publisher/src/authority.rs`). Our map's `generation` should
  be ours, kept in the descriptor.
- **Consumer floor** is `AuthorityFloor`: it only moves forward, and a floor
  not yet durable permits no positive progression.

### 2.4 The five normative rules

From `wp/docs/spec/spec-publisher-ingest-model.md:15-21` (status: draft), verbatim:

```
The common model is:

1. Store immutable bytes once, under an identity derived from their contents.
2. Publish references to those bytes when the source advances.
3. Reuse verification of unchanged files; recheck changed or replaced files.
4. Make a publication current only after preparation succeeds.
5. Delete stored bytes only when nothing still needs them.
```

Sections 1-5 of that document (`:58-193`) expand them: SHA-256 plus exact
length, private staging and directory sync before a reference; references
never imply serving; verification evidence binds digest, size,
dev/inode/size/mtime/ctime and semantic context and never survives a restart;
cancellation, reorg and withdrawal checks run after every await and
immediately before activation, retention is established before references
become visible, consumers never block preparation; collection accounts for
every retained publication, candidate and in-flight response.

### 2.5 How Receiver and txid describe their publication

Receiver (`wp/src/receiver/receiver-pir-server/src/distribution.rs`):

```rust
pub const PREFIX: &str = "/provider/receiver/v1/";                 // :11
pub const SCHEMA: &str = "receiver-prepared-distribution-v1";      // :12
pub const METADATA_LIMIT: u64 = 128 * 1024 * 1024;                 // :13
pub type Current = PublicationEnvelope<Descriptor>;                // :15
pub type Subject = Vec<(u32, Hash)>;                               // :17 every anchor a view depends on
pub struct Descriptor { pub history_revision: u64, pub directory: Option<Hash>,
    pub filters: Option<blocks::Head>, pub recovery_epoch: u64 }   // :21
pub struct Segment { pub publication: receiver::Publication, pub directory: ObjectReference,
    pub directory_public: ObjectReference, pub choice: ObjectReference,
    pub companion: ObjectReference, pub companion_public: ObjectReference } // :35
pub struct Inventory { pub genesis: Hash, pub history_revision: u64, pub directory: Option<Directory>,
    pub filters: Option<Filters>, pub recovery_epoch: u64 }        // :69
```

- Objects: row bytes and **public setup bytes are both objects** (`*_public`),
  prepared by the publisher; `Inventory::validate` binds each object's length
  and digest to its session commitment (rows = `DIRECTORY_ROWS * ROW_BYTES`,
  public = `public_bytes()`) (`:87-237`). Material identity =
  SHA-256(inventory JSON). The inventory JSON is itself an object and the
  envelope's `inventory` points at it.
- Our equivalent: `Descriptor { generation, recovery_epoch, map_head: Hash }`,
  `Inventory { map_head: ObjectReference, map_chunks: Vec<ObjectReference>, tables: Vec<TableObjects { rows, public }> , anchors... }`,
  `SCHEMA = "tachyon-<product>-prepared-distribution-v1"`,
  `PREFIX = "/provider/<product>/v1/"`, `Subject = Vec<(u32, Hash)>` (the
  anchor plus each open scope's last block).

txid layout facts we copy (all in `reg/zakura-pir-protocol/src/transparent/shard/`):
`MAX_BUCKETS = 64`, `MAX_PLACEMENT_SEEDS = 64` (`display/mod.rs:47,75`);
placement `u64_le(SHA256("txid-display/row/v3" ‖ seed_le4 ‖ shard_id_le8 ‖ bucket_le4 ‖ tag16)[..8]) % rows`
(`display/mod.rs:71-90`); seed search 0..64 then overflow segments
(`reg/zakura-pir-publisher/src/transparent/display/tables.rs:36-135`); map head
`DisplayRecentMap` with `chunks: Vec<DisplayChunkRef { start_height, sha256 }>`
and chunks `DisplayIndexChunk { schema, base_shard_id, shards }` of
`INDEX_CHUNK_SHARDS = 32` (`display/split.rs:37,54,63,88`).

## 3. Publisher host (`pir-publisher` library, `pir-pipeline`)

### 3.1 Entry point

```rust
// wp/src/pir-publisher/src/host.rs:19-50 (re-exported as pir_publisher::run_registered, lib.rs:7)
pub async fn run_registered(
    management_listen: SocketAddr,
    publishers: Vec<PublisherDefinition<axum::Router>>,
    chain: Option<pir_pipeline::ingestion::ChainSettings>,
    shutdown: Shutdown,                       // pir_pipeline::service::Shutdown
) -> Result<()>;                              // Box<dyn Error + Send + Sync>
```

What it does, in order (`host.rs:19-50`):
1. Refuses chain-following definitions without `chain`.
2. `validate_resources` (`config/validation.rs:7-31`, `config/layout.rs`):
   management listener must be **loopback**; every product listener must be a
   **fixed (non-zero port) loopback or private (RFC 1918 / ULA) address**;
   no two definitions may share a listener or port-with-unspecified-IP;
   definition names must be unique; writable paths must be absolute and must
   **not overlap between different definitions**.
3. Binds every listener up front (`host/listeners.rs:18-36`).
4. Serves management (`host/management.rs:67-113`) as **plain HTTP**:
   - `GET /health` -> 200, `{"ready": bool, "products": {"<name>": {"phase": "running|failed|stopped", "ready": bool}}}`
   - `GET /ready` -> same body, 200 or 503. A product is ready when running
     and every `ReadinessProbe` passes (`HttpSuccess` or `Body(fn(&[u8]) -> bool)`,
     body read up to 64 KiB, 2 s timeout, `http://{address}{path}`).
   - `GET /.well-known/pir-service-discovery` -> `pir_service_discovery::Document::publisher().to_json()`,
     i.e. `{"service":"pir-publisher","version":"1.0.0"}`, `content-type: application/json`,
     `cache-control: no-store`.
   - **There is no `/metrics`.**
5. Starts one `ChainFollower` over `pir_node_rpc::RpcClient` (`getblockcount`,
   `getblockhash`) if any definition follows the chain (`host.rs:53-75`;
   `ingestion.rs:1-16`). Requires the crate feature **`chain`**; without it
   `follow` returns "this build has no chain-following publisher" (`host.rs:76-83`).
6. Starts each definition with a `ServiceContext` and supervises it
   (`host/products.rs`, `host/supervision.rs`). A product whose future returns
   is marked `failed` and **is not restarted**; a panic stops the whole host.

Our binary can call `run_registered` unchanged with our own definitions. No
patches are needed. The constraints above are the price (section 8 lists the
spec consequences): no TLS, no metrics route, discovery only on the
management listener, private listeners only, one listener and one disjoint
state directory per definition.

`pir_publisher::config::Config` is wallet-pir's product list and is not
reusable; write our own config struct and build `ChainSettings` directly
(its fields are pub, `pir-pipeline/src/ingestion.rs:37-44`):

```rust
pub struct ChainSettings { pub rpc_url: String, pub rpc_cookie: Option<PathBuf>, pub poll: Duration }
```

### 3.2 Definition and context

```rust
// wp/src/shared/pir-pipeline/src/publisher/registration.rs
pub enum ReadinessRule { HttpSuccess, Body(fn(&[u8]) -> bool) }                       // :8
pub struct ReadinessProbe { pub address: SocketAddr, pub path: &'static str, pub rule: ReadinessRule } // :13
#[derive(Default)]
pub struct PublisherResources { pub listeners: Vec<SocketAddr>, pub writable_paths: Vec<PathBuf>, pub probes: Vec<ReadinessProbe> } // :21
pub struct ServiceContext<R> { pub http: Http<R>, pub shutdown: Shutdown, pub chain: Option<crate::ingestion::ChainFeed> } // :28
impl<R> PublisherDefinition<R> {
    pub fn new<F, Fut>(name: &'static str, resources: PublisherResources, start: F) -> Self
    where F: FnOnce(ServiceContext<R>) -> Fut + Send + 'static,
          Fut: Future<Output = Result<()>> + Send + 'static;                          // :46
    pub fn following_chain(mut self) -> Self;                                          // :60
}
// publisher/runtime.rs:5
pub async fn dedicated_runtime<F, Fut>(name: &'static str, start: F) -> super::Result<()>
where F: FnOnce() -> Fut + Send + 'static, Fut: Future<Output = super::Result<()>>;
// service.rs
impl<R> Http<R> { pub fn serve(&self, addr: SocketAddr, router: R) -> HttpFuture }   // :48
pub fn lock(root: &Path) -> io::Result<File>                                           // :72, root/publisher.lock
```

`ServiceContext.http.serve(addr, router)` serves on a listener the definition
declared; each socket can be taken once.

### 3.3 Ingestion trait and call order

```rust
// wp/src/shared/pir-pipeline/src/ingestion.rs:63-118
pub trait Ingestion: Send {
    type Error: std::fmt::Display + Send;
    fn target_height(&self, tip: u64) -> u64 { tip }
    fn start_height(&self, tip: u64) -> u64;
    fn observed(&mut self, _observation: &ChainObservation) -> impl Future<Output = Result<(), Self::Error>> + Send { async { Ok(()) } }
    fn committed(&self) -> Option<BlockId>;
    fn checkpoint_at_or_below(&self, height: u64) -> impl Future<Output = Option<BlockId>> + Send;
    fn withdraw_above(&mut self, keep: Option<&BlockId>) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn rewind(&mut self, keep: Option<&BlockId>) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn ingest(&mut self, from: u64, through: u64) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn caught_up(&mut self, _observation: &ChainObservation) -> impl Future<Output = Result<(), Self::Error>> + Send { async { Ok(()) } }
    fn failed(&mut self, _error: &Self::Error) -> FailurePolicy { FailurePolicy::NextPoll }
}
pub struct BlockId { pub height: u64, pub hash: String }                     // :22, hash in RPC DISPLAY order
pub struct ChainObservation { pub genesis: String, pub tip: BlockId, pub pass: u64 } // :31
pub struct ChainFeed { pub chain: Arc<dyn CanonicalChain>, pub observations: watch::Receiver<Option<ChainObservation>> } // :56
pub trait IngestionExecutor: Clone + Send + Sync + 'static {
    fn ingest<'a, I: Ingestion + 'a>(&'a self, ingestion: I, feed: ChainFeed, shutdown: Shutdown) -> IngestionRun<'a, I::Error>;
} // implemented by PublisherEngine (:134-143)
```

There is **no `append` and no `publish` in this trait.** Per observation the
live scheduler (`ingestion/schedule.rs:226-292`) does, holding the writer lock:

1. `observed(&obs)` (network policy; a different genesis is never reconciled).
2. `reconcile` (`:38-91`): if the node no longer reports `obs.tip`, stop
   (Stale). Walk `checkpoint_at_or_below(tip)` newest-first against
   `chain.hash_at(h)`; if the newest checkpoint is not canonical,
   `withdraw_above(keep)` **then** `rewind(keep)`.
3. Compare `committed()` with `min(target_height(tip), tip)`:
   - Current -> `caught_up(&obs)`. **Publish here.**
   - Missing by fewer than `LIVE_BLOCKS = 8` -> `ingest(from, through)`, accept
     only if the newest committed block is still canonical (`anchored`,
     `:153-171`), reconcile again, and if Current call `caught_up`.
   - Missing by more -> hand the observation to the backfill scheduler, which
     calls `ingest` in batches of `BACKFILL_BLOCKS = 64` (`:294-352`).
4. Errors go through `failed()`: `NextPoll` logs and retries, `Stop` ends the
   product.

`ingest(from, through)` must fetch and decode the blocks itself (the feed only
carries tips and canonical hashes), e.g. through `tachyon-chain` over its own
`RpcClient` built from `ChainSettings`. For tests and maintenance,
`pir_pipeline::ingestion::catch_up(writer, chain, observation)` (`schedule.rs:119-147`)
runs the same steps in the caller's task.

### 3.4 Publication / Publisher traits and `PublisherEngine`

```rust
// wp/src/shared/pir-pipeline/src/publisher/operations.rs:15-58
pub trait Publication: Send {
    type Error: Send; type Material: Send; type Stored: Send;
    type Verified: Send; type Candidate: Send; type Report: Send;
    fn check(&self) -> Result<(), Self::Error>;
    fn build(&mut self) -> impl Future<Output = Result<Self::Material, Self::Error>> + Send;
    fn store(&mut self, material: Self::Material) -> impl Future<Output = Result<Self::Stored, Self::Error>> + Send;
    fn verify(&mut self, stored: Self::Stored) -> impl Future<Output = Result<Self::Verified, Self::Error>> + Send;
    fn assemble(&mut self, verified: Self::Verified) -> impl Future<Output = Result<Self::Candidate, Self::Error>> + Send;
    fn recheck(&mut self, candidate: &mut Self::Candidate) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn commit(self, candidate: Self::Candidate) -> impl Future<Output = Result<Self::Report, Self::Error>> + Send;
}
pub trait Publisher: Send { /* background, acquire -> PublicationInput<..>, completed, failed, settled, wait, finish */ } // :72-102
pub trait PublisherExecutor: Clone + Send + Sync + 'static {
    fn publish<P: Publication>(&self, publication: P) -> impl Future<Output = Result<P::Report, P::Error>> + Send;
    fn publish_input<P: Publication>(&self, input: PublicationInput<P>) -> ...;
    fn run<'a, P: Publisher + 'a>(&'a self, publisher: P, shutdown: Shutdown) -> PublisherRun<'a, P::Error>;
}
// engine.rs:10-53
#[derive(Clone, Copy, Default)] pub struct PublisherEngine;
// prepare(): check, build, check, store, check, verify, check, assemble, check
// PreparedPublication::commit(): check, recheck, check, commit
```

`Publisher` is the polling loop for products without a chain (Status uses it
with time-based waits; our **registry** product fits it: `acquire` returns
`NewMaterial` when the registration log grew, `wait` sleeps until the next
registration or tick). Chain products (Receiver; our memo, first-contact,
tachygram) instead call `executor.publish(Export { .. })` from `caught_up`
(`receiver-indexer/src/managed.rs:418-437`, `distribution.rs:153-193`).

Receiver's `Export` (`receiver-indexer/src/distribution.rs:446-538`) is the
template:

| Phase | Receiver does | We do |
|---|---|---|
| `check` | fail if the in-memory serving epoch moved | same (epoch bumped by every withdrawal/revoke) |
| `build` | `spawn_blocking`: store each table's files as objects, build `Inventory` | build rows for changed scopes, prepare runtimes to get `public`, store rows, public, map chunks |
| `store` | identity | identity (objects already stored) |
| `verify` | `inventory.validate(...)` | validate every row (section 5) and that each `public` matches a fresh preparation |
| `assemble` | store inventory JSON as an object, `PublicationReference::new` | same |
| `recheck` | `check_network`, re-read every anchor hash; on mismatch revoke and fail | same |
| `commit` | inside `in_epoch`: `retain_replacement(objects, now + retention)`, `apply(Publish)`, `RecoveryRequired` idiom | same |

### 3.5 How a product plugs in (sketch, after `receiver-indexer/src/managed.rs:104-130`)

```rust
pub fn configure<E: PublisherExecutor + IngestionExecutor>(
    config: TachygramConfig,
    chain: Option<&ChainSettings>,
    executor: E,
) -> pir_pipeline::publisher::Result<PublisherDefinition<axum::Router>> {
    let chain = chain.cloned().ok_or("tachygram publisher needs the root chain settings")?;
    let resources = PublisherResources {
        listeners: vec![config.listen],                 // private address, fixed port
        writable_paths: vec![config.state_dir.clone()], // absolute, disjoint from other products
        probes: vec![ReadinessProbe {
            address: config.listen,
            path: "/provider/tachygram/v1/current",
            rule: ReadinessRule::Body(distribution_ready), // eligible && publication.is_some()
        }],
    };
    Ok(PublisherDefinition::new("tachygram", resources, move |cx| async move {
        let feed = cx.chain.ok_or("started without the root chain")?;
        pir_pipeline::publisher::dedicated_runtime("tachygram-publisher", move || async move {
            run(config, chain, feed, cx.http, cx.shutdown, executor).await
        }).await
    }).following_chain())
}

async fn run<E: PublisherExecutor + IngestionExecutor>(..) -> Result<()> {
    let _lock = pir_pipeline::service::lock(&config.state_dir)?;
    let distribution = Distribution::open(..)?;          // PublishedObjectStore + PublicationAuthority (new epoch)
    let app = axum::Router::new().nest("/provider/tachygram/v1", distribution.router()); // current, objects/:id, authority/check
    // spawn http.serve(config.listen, app); spawn canonical guard over feed (managed.rs:279-316);
    executor.ingest(writer, feed, stop.clone()).await     // Writer: Ingestion; caught_up -> executor.publish(Export)
}

// Our binary:
let defs = vec![tachygram_publisher::configure(cfg.tachygram, Some(&chain), PublisherEngine)?, /* ... */];
pir_publisher::run_registered(cfg.management_listen, defs, Some(chain), shutdown).await
```

The distribution routes (`receiver-indexer/src/distribution.rs:68-74,340-430`):

- `GET current` -> `Json(authority.current())`, `ETag: conditional_token(&envelope)`,
  304 on matching `If-None-Match`, `cache-control: no-store`.
- `GET objects/:id` -> stream `objects.open_retained(&id)`; 404 if not retained.
- `POST authority/check` -> `request.admit(current)` (400 malformed, 409
  advanced), bound the total anchors (`MAX_CHECK_ANCHORS = 4096`), check
  network, look up each anchor's hash once, answer `still_authoritative`, then
  `admit` again.

### 3.6 TLS

`pir-publisher` speaks plain HTTP only (`axum::serve(listener, router)`,
`host/listeners.rs:40-56`), while the query side's `HttpsSource` refuses
anything but `https://` (`pir-sync-manager/src/transport.rs:20-29`,
`https_only(true)`). wallet-pir's tests put a TLS terminator in front:

- `wp/src/pir-publisher/tests/support/distribution_tls.py` (227 lines), a
  Python `ThreadingHTTPServer` in `ssl.SSLContext(PROTOCOL_TLS_SERVER)`, binds
  `:0`, prints its port, proxies to the product listener, sends
  `/.well-known/pir-service-discovery` to `$DISCOVERY_UPSTREAM` (the
  management listener), and reads a fault-control file on each request
  (`refused-body`, `corrupt`, `truncated`, `missing`, `redirect`, `pause-*`,
  `discovery-*`).
- Certificates: `openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost -addext basicConstraints=critical,CA:FALSE`
  (`wp/tools/pir-e2e-tests/src/enhance_fixture.rs:86-166`); the leaf itself is
  the trust anchor, origin `https://localhost:<port>`. No rcgen anywhere.
- `pir-sync-manager`'s unit tests instead run an in-process `tokio-rustls`
  server over the same openssl-made cert (`pir-sync-manager/src/tests.rs:15-80`).

In production a reverse proxy does the same job, and must route the discovery
path to the management listener.

## 4. Query host

wallet-pir's `pir-query-server` is a library (`src/lib.rs`), but everything a
host needs is private (`host`, `hold`, `supervision`, `public::forward`,
`receiver::sync`, `Config::source`) or Receiver-specific
(`receiver_pir_server::Publications`). Write our own host; copy the patterns
below. Receiver's sync (`wp/src/pir-query-server/src/receiver/sync.rs`, 545
lines) and `receiver_pir_server::publication` (`publication.rs:290-830`) are
the two files to read most closely.

### 4.1 Consumer stack per product

```rust
// pir-sync-manager/src/transport.rs:14
pub fn new(origin: &str, prefix: &str, budget: u64, roots: &[reqwest::Certificate]) -> Result<HttpsSource, Error>;
// origin: bare https://host[:port]/, prefix like "/provider/tachygram/v1/"; https_only, no redirects,
// connect 10 s, request 120 s, extra roots added to webpki roots, budget counts every body byte.

// pir-sync-manager/src/publication.rs:311,346
impl<A: PublicationAdapter> EnvelopeConsumer<A> {
    pub fn new(adapter: A, source: HttpsSource, floor: AuthorityFloor<A::Descriptor>, cache: ObjectCache) -> Result<Self, Error>;
    pub fn open(adapter: A, source: HttpsSource, floor: AuthorityFloor<A::Descriptor>, cache: ObjectCache) -> Result<ConsumerEngine<Self>, Error>;
}
// pir-pipeline/src/consumer/polling.rs:57-105
SyncManager::new(task).with_success_interval(interval).run_until(stop.cancelled()).await -> Option<Error>
```

Wiring, as Receiver does (`receiver/sync.rs:22-52`, `receiver/mod.rs:82-116`):

```rust
let _lock = pir_pipeline::service::lock(&state_dir)?;
let roots = match &cfg.publisher_ca { Some(p) => vec![reqwest::Certificate::from_pem(&std::fs::read(p)?)?], None => vec![] };
let source = HttpsSource::new(&cfg.publisher, "/provider/tachygram/v1/", cfg.bootstrap_max_bytes, &roots)?;
let floor = AuthorityFloor::open(state_dir.join("authority-floor.json"),
    SourceBinding { publisher_origin: cfg.publisher.clone(), product_namespace: PREFIX.into(), chain: genesis })?;
let cache = ObjectCache::open(state_dir.join("objects"))?;
let engine = EnvelopeConsumer::open(TachygramAdapter { .. }, source, floor, cache)?;
let stop_sync = SyncManager::new(engine).with_success_interval(Duration::from_secs(cfg.poll_seconds))
    .run_until(stop.cancelled());
```

`PublicationAdapter` (`publication.rs:57-214`) is the trait we implement:
`validate_envelope`, `fence_serving`, `invalidate_views`, `close_admission`,
`serving_subjects`, `plan` (returns `PublicationPlan::Candidate { candidate, identity: ViewIdentity, required_objects }`),
`load` (prepare runtimes here, off the async threads), `rebind`, `revalidate`,
`reserve_peer`, `view_objects`, `retained_views`, `prepare_commit`,
`installed`, `release_peer`, `serving_report`. The engine's order per pass
(`pir-pipeline/src/consumer/engine.rs:41-110`): settle (runs **discovery every
pass**) -> observe (`GET current`, `If-None-Match`, 64 KiB limit, classify
against the floor, fence serving on withdrawal or new epoch, persist floor) ->
eligible -> check_serving -> select/plan -> obtain (`ObjectFetcher::fetch`, every
object SHA-256-verified before it is renamed into the cache,
`pir-sync-manager/src/objects.rs:92`) -> load -> observe again -> rebind ->
revalidate -> authorize (`POST authority/check`; 409 re-observes and defers) ->
confirm -> commit (durable record, fence check, install, collect).

**Prepare before switch** falls out of this: fetch, load and preparation run
while the old view serves; only `commit` swaps.

### 4.2 Service discovery

`HttpsSource::discover(&self) -> Result<Version, DiscoveryError>`
(`pir-sync-manager/src/discovery.rs:94`): `GET origin + /.well-known/pir-service-discovery`,
10 s deadline, then `pir_service_discovery::check_publisher(body)` (service must
be `"pir-publisher"`, same major version as this build's `1.0.0`). Run it once
before binding listeners (`pir-query-server/src/host.rs:196-219`); the engine
runs it again in every `settle`. `EnvelopeConsumer::terminal` returns
`pir_sync_manager::is_incompatible(..)`, so `run_until` returns the error and
the host exits non-zero when `error.downcast_ref::<pir_sync_manager::DiscoveryError>().is_some()`
(`host.rs:461-465`); other product errors only mark that product failed.

### 4.3 Displaced publication for 600 s, revocation

Receiver keeps it in `receiver_pir_server::Publications`
(`wp/src/receiver/receiver-pir-server/src/publication.rs`):
`DEFAULT_RETENTION = 600 s` (:290), `MAX_RETAINED = 16` (:296); on install
the displaced current is pushed with `until = now + retention` (:519-567);
`select(&session_id)` (:784) searches current plus retained and returns
`StatusCode::GONE` when nothing matches (:827); `revoke()` (:764) bumps an
in-memory epoch and drops current and retained; `revoke_serving(epoch, anchor)`
(:773); `in_epoch(expected, f)` (:442). The adapter calls them from
`fence_serving` and `invalidate_views` (`receiver/sync.rs:133-143,186-238`).
Configure as `retention_seconds` (default 600, 1..=86400,
`pir-query-server/src/receiver/mod.rs:28-40`). Reimplement this (about 300
lines) in our host.

### 4.4 Public listener, 503 until ready, management

- `public::Slot` starts `Starting`; requests wait up to `STARTING_WAIT = 15 s`
  then get 503 `"query service is not serving this product\n"`
  (`pir-query-server/src/public.rs:26,58-77`). Receiver additionally holds a
  503 inventory for up to 15 s (`hold.rs:18-68`). For us: 503 with
  `{"code":"temporarily_unavailable",...}` until the product's first commit.
- Readiness: `Readiness::register(|| async { publications.current_anchor().is_some() })`
  (`readiness.rs:17-31`, `receiver/mod.rs:95`).
- Management (loopback only, `config.rs:239`; `host.rs:243-283`): `GET /health`
  200 `{"ready": bool, "products": {"<name>": {"phase": "running|stopping|stopped|failed", "ready": bool}}}`;
  `GET /ready` same body, 200 or 503. Same shape as the publisher.
- Shutdown (`host.rs:119-150`): mark stopping, withdraw slots, drain the public
  listener (`PUBLIC_DRAIN = 60 s`), stop products.

### 4.5 `pir_control` (feature `admission`)

```rust
// pir-control/src/public_routes.rs
pub struct PublicMount { pub canonical: &'static str, pub legacy: &'static str }   // :17
impl PublicMount {
    pub const fn new(canonical: &'static str, legacy: &'static str) -> Self;      // :27
    pub const fn prefixes(&self) -> [&'static str; 2];                             // :32
    pub fn mount<S>(self, router: Router<S>, operations: impl IntoIterator<Item = (&'static str, MethodRouter<S>)>) -> Router<S>
        where S: Clone + Send + Sync + 'static;                                    // :41 registers "{prefix}{suffix}" for both
    pub fn operation<'a>(&self, path: &'a str) -> Option<&'a str>;                // :60 strips either prefix (use for metric labels)
}
// pir-control/src/admission.rs
pub enum Refusal { Full, Deadline, Closed }                                                       // :19
impl Queue {
    pub fn new(executing: usize, waiting: usize, wait: Duration, fast_path: bool) -> Self;       // :46
    pub async fn acquire<G>(&self, on_wait: impl FnOnce() -> G) -> Result<OwnedSemaphorePermit, Refusal>; // :59
    pub fn executing_available(&self) -> usize; pub fn waiting_available(&self) -> usize; pub async fn drained(&self);
}
pub enum BodyError { Timeout, Read(axum::Error) }  impl BodyError { pub fn over_limit(&self) -> bool }   // :101,111
pub async fn read_body(body: Body, limit: usize, deadline: Duration) -> Result<Bytes, BodyError>;       // :126
pub fn spawn_admitted<G, R, F>(guard: G, work: F) -> tokio::task::JoinHandle<(R, G)>
    where G: Send + 'static, R: Send + 'static, F: FnOnce(&G) -> R + Send + 'static;                    // :136
pub fn client_key(headers: &HeaderMap, peer: Option<IpAddr>) -> String;                                 // :152
impl ClientSlots { pub fn new(cap: usize) -> Self; pub fn try_acquire(&self, key: &str) -> Option<ClientSlot>; pub fn tracked(&self) -> usize } // :184-209
// pir-control/src/http.rs:54
pub fn retain<G: Send + Unpin + 'static>(body: Body, guard: G) -> Body;   // keeps permits alive until the body is sent
```

- `client_key` takes the first `X-Forwarded-For` entry, then `X-Real-IP`, then
  `peer`, then `"unknown"`, **trusting headers as sent**. Receiver passes
  `peer = None`. Behind our proxy, configure which source to trust; never
  label metrics with it.
- Overflow handling is the caller's: Receiver maps any `Refusal` to
  `429` + `Retry-After: 1` (`receiver/routes.rs:410-412,446-448`). Its values:
  `Queue::new(2, 8, Duration::from_secs(2), true)`, `ClientSlots::new(2)`,
  upload deadline 15 s (`routes.rs:76-85`).
- Receiver does **not** use `PublicMount`; it nests twice
  (`/receiver/v1`, `/v1/receiver`). txid uses `PublicMount::new("/transparent/v1", "/v2")`.
  Use `PublicMount::new("/tachygram/v1", "/v1/tachygram")` etc.
- Other: `pir_control::Identity::process()` (`lib.rs:38-49`, binary sha256 +
  incarnation, put it in `/health`), `durable::{write_atomic, replace_durable, sync_dir}`
  (`durable.rs:39-117`).

### 4.6 `pir_observability` (feature `http`)

```rust
// lib.rs:78-101
impl HttpMetrics { pub fn initialize(&self, endpoints: &[&'static str]); pub fn render(&self) -> String }
// http.rs:121 (a handler, not a Layer)
pub async fn observe(metrics: HttpMetrics, endpoint: Option<&'static str>, request: Request, next: Next) -> Response;
```

Receiver (`receiver/routes.rs:22-161`): `fn endpoint(path) -> Option<&'static str>`
maps both prefixes to fixed labels; `metrics.initialize(&ENDPOINTS)`;
`.route("/metrics", get(|| ([(CONTENT_TYPE, "text/plain")], metrics.render())))`;
layers `.layer(map_response(no_store_errors)).layer(from_fn_with_state(metrics, observe_wrapper))`.
Emitted names: `pir_http_{arrivals_total,inflight,cancelled_total,request_body_bytes_total,response_body_bytes_total,body_errors_total,incomplete_responses_total}{endpoint}`,
`pir_http_responses_total{endpoint,code="Nxx"}`, `pir_http_duration_seconds_*`.

### 4.7 Error bodies and Cache-Control

- Receiver: bare status codes, empty bodies, `Cache-Control: no-store` added to
  every 4xx/5xx by `no_store_errors` (`routes.rs:338-346`), success
  `application/json` or `application/octet-stream` with `no-store`
  (`:375-407`). Nothing immutable.
- txid: `{"error": msg, "retry"?: ..., "map_sha256"?: ...}`
  (`pir-query-server/src/transparent/request.rs:25-132`): 400, 408, 411, 421,
  409 stale (`"refresh the txid map"`), 503 + `retry-after: 1`. Map head
  `no-cache`, map chunks and manifests `public, max-age=31536000, immutable`
  (`tx_detail/service.rs:653,668,703,732`).
- Enhance (the vocabulary our spec adopts): `{"code","message"}` with codes
  `stale_routing` (409), `session_unavailable` (410), `overloaded` (429 +
  `retry-after: 1`), `temporarily_unavailable` (503/502), else
  `invalid_request`; server-error messages never echo internal text
  (`pir-query-server/src/enhance/query_serving.rs:183-222`).

### 4.8 Receiver `RPQ1` serving

Header (`reg/zakura-pir-protocol/src/receiver/session.rs:5`, `native.rs:14`):
`[0..4] "RPQ1" | [4..36] session_id | [36..52] nonce (16 random bytes)`, then
the P8 payload; no length fields. The client builds `MAGIC ++ id ++ OsRng[16] ++ upload`
and checks `response.len() == response_bytes()` and that the first 52 bytes
equal its request (`reg/zakura-pir-client/src/receiver/client.rs:65-85`).
The evaluator echoes `body[..52]` then the packed answer, refusing wrong magic
(Malformed) or a different session (Revision) (`reg/zakura-pir-query-server/src/receiver/server.rs:54-77`).

Handler `query` (`wp/src/pir-query-server/src/receiver/routes.rs:419-479`):
1. `clients.try_acquire(&client_key(headers, None))` else 429.
2. `read_body(body, query_bytes(MAX_COMPANION_ROWS), 15 s)`: timeout 408,
   over limit 413, else 400.
3. Under 52 bytes or wrong magic: 400.
4. `publications.select(&body[4..36])` (current + retained): miss -> **410**.
5. Length neither full nor trimmed: 400 (413 if larger than full).
6. `queue.acquire` else 429 + `Retry-After: 1`.
7. `spawn_admitted((permit, slot, publication), || table.respond(&body))`.
8. After evaluation, if `publications.epoch()` moved or authorisation was
   cleared: **410** (revoked mid-flight).
9. `Ok` -> octet-stream wrapped in `pir_control::http::retain`, `no-store`.

Public setup: `GET {prefix}/public/:session` -> hex-decode (400), `select`
(410), `binary(table.public())` octet-stream, `no-store` (`routes.rs:207-210,387-395`).
Receiver has no 409.

### 4.9 txid map head and chunks

`tx_detail/service.rs:486-526`, mounted under `/transparent/v1` and `/v2`:

| Route | Response |
|---|---|
| `GET /txid/map` | `DisplayRecentMap` bytes, `x-txid-map-sha256: <sha>`, `cache-control: no-cache` |
| `GET /txid/map/:base/:digest` | `DisplayIndexChunk` bytes, `public, max-age=31536000, immutable`; unknown digest 409, base mismatch 400 |
| `GET /txid/{archive,recent}/shards/:id/revisions/:digest/setup/:table/:segment` | JSON with base64 `public_params` |
| `POST /txid/{archive,recent}/shards/:id/revisions/:digest/query/:table` | octet-stream, all segments' answers |

A txid query requires `Content-Length` (411), checks the exact length before
reading (400 after draining), admits, reads with a 10 s deadline and answers
every segment (`request.rs:221-400`). A reorg during evaluation becomes 409
(`live.rs:1038-1041`).

## 5. Row validation to mirror

### 5.1 Status `decode_row` (`wp/src/status/status-shared/src/protocol.rs:297-339`)

```rust
pub fn decode_row(manifest: &Manifest, txid: &Hash, earliest: Option<u32>, row: &[u8]) -> Result<Observation, Error>
```

Checks, in order: row length exactly `ROW_BYTES` (12,288); bytes after
`SLOTS * SLOT_BYTES` all zero; each 40-byte slot decodes (`Record::decode`,
`:95-112`: all-zero = empty, reserved bytes zero, tag/height consistency);
no record after an empty slot; keys strictly ascending; every record's bucket
equals the queried key's bucket; every record's height inside
`[coverage_start, anchor_height]`. A hit returns it; a miss is `NotFound`
only if the queried coverage lies inside the manifest's, else
`CoverageIncomplete`. Before it, the client checks query age, manifest
freshness, exact response length and the 52-byte header echo
(`:445-466,524-547`), and at session start `SHA256(public) == manifest.public_digest`.

### 5.2 txid client (`reg/zakura-pir-client/src/transparent/txid_client/client.rs:405-447`)

Recomputes the row from the key, queries that row in every segment, then
`txid::find_entry` (`reg/zakura-pir-protocol/src/transparent/shard/txid.rs:497`)
runs `row_entries` (canonical: decodable entries, tags strictly ascending,
empty slots only after entries, zero padding) on each copy, and **recomputes
placement for every entry in the row**, refusing "entry outside its row" and a
tag found in two segments. The server and publisher run the whole-table
equivalent, `display::verify_rows` (`tables.rs:85-135`): shape, seeds < 64,
canonical rows, every entry in its bucket and row, no duplicate tag, no empty
overflow segment, counts equal to the manifest.

### 5.3 Receiver client

`segment::decode_row(bytes) -> Result<Vec<Entry>, Error>`
(`reg/zakura-pir-protocol/src/receiver/segment.rs:228-244`): exact length,
zero tail, ascending tags, empties only at the end, `count != 0`. It does
**not** recompute placement of the other entries (only the full-table
`DirectoryTable::validate`, run by publisher and server, does: `table.rs:399-434`).

### 5.4 What our `validate_row` should check (all three sides)

1. `row.len() == 8192`; bytes after `SLOTS * ENTRY_BYTES` are zero.
2. Occupied entries first; after the first all-zero entry, all entries zero.
3. No occupied entry has an all-zero key.
4. Keys non-decreasing (memo, first-contact, registry: duplicates allowed);
   strictly increasing for tachygram (uniqueness, spec 8.3).
5. Every entry's placement recomputes to this `(bucket, row)` under the
   table's seed (txid's rule, stronger than Receiver's client).
6. Product range checks: tachygram `blk_height` inside the scope's
   `[first_height, last_height]`, as Status checks heights.
7. On the client, before decoding: exact response length and header echo; at
   session start `SHA256(public) == public_sha256` and the session id
   recomputes from the map's table entry.

## 6. End-to-end test harness (`wp/tools/pir-e2e-tests`)

### 6.1 Binaries

`CARGO_BIN_EXE_<name>` is set only for bins of the test's own package, so the
e2e crate re-declares the hosts' entrypoints as its own bins
(`tools/pir-e2e-tests/Cargo.toml:7-8,136-147`):

```toml
autotests = false
autobins = false
[[bin]]
name = "pir-publisher-test"
path = "../../src/pir-publisher/src/main.rs"
required-features = ["publisher-host"]
test = false
[[bin]]
name = "pir-query-server-test"
path = "../../src/pir-query-server/src/main.rs"
required-features = ["query-host"]
test = false
```

The `#[cfg(feature = ...)]` inside those `main.rs` files then resolve against
the **e2e crate's** features, so it re-declares `receiver`, `status`, etc.
(`Cargo.toml:12-28`). Spawned with an override
(`tests/support/query_host.rs:24-38`):

```rust
let child = tokio::process::Command::new(
    std::env::var_os("WALLET_PIR_QUERY_SERVER_BIN")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_pir-query-server-test").into()),
)
.arg("--config").arg(&config)
.env("RUST_LOG", std::env::var("RUST_LOG").unwrap_or("info".into()))
.kill_on_drop(true)
.stdout(file.try_clone().unwrap()).stderr(file)
.spawn().unwrap();
```

No build.rs, no `cargo build` inside tests.

### 6.2 Config, ports, readiness, lifecycle

- Configs are `serde_json::json!` documents written to `<tempdir>/<name>.json`;
  state under `tempfile::tempdir()`; all paths absolute (host configs use
  `deny_unknown_fields`). Receiver example (`tests/support/receiver.rs:141-153,205-215`):
  publisher `{"management_listen", "chain": {"rpc_url", "no_auth": true, "poll_ms": 100}, "receiver": {"enabled": true, "settings": {"data_dir", "bind", ...}}}`;
  query server `{"public_listen", "management_listen", "receiver": {"enabled": true, "settings": {"publisher": tls.origin, "publisher_ca": tls.certificate, "state_dir", "poll_seconds": 1, "retention_seconds": 600, ...}}}`.
- Ports: bind `127.0.0.1:0`, read the port, drop, pass it on (a tolerated race):
  `pub fn free() -> String` (`query_host.rs:3-9`). (`run_registered` refuses
  port 0, so this is required for the publisher.)
- Readiness: poll `GET http://{management}/ready` every 50 ms, up to 1,200
  times, and `child.try_wait()` each iteration so a crash fails fast with the
  log (`query_host.rs:79-101`).
- Lifecycle: `kill_on_drop(true)`; graceful stop with `SIGTERM` and a 90 s wait
  (`query_host.rs:106-115`); stdout and stderr to one log file read back into
  the panic message.

### 6.3 Node double and oracle

- wallet-pir's node double is in-process axum (`wp/src/shared/pir-chain/src/testing.rs:310-537`,
  feature `testing-node`): `getblockcount`, `getblockhash`, `getblock` 0/2,
  JSON-RPC 1.0 singles and 2.0 batches, cookie auth, fault hooks. We already
  have `tools/fixtures` (`tachyon-node-double`).
- Oracles never share the builder: frozen byte fixtures assembled "by direct
  byte slicing", hand-computed rules, synthetic records decoded by the external
  crates.io client, and an independent full-tree rebuild for Receiver
  (`zakura_pir_publisher::receiver::witness::payloads`, `tests/support/receiver.rs:303-314`).

### 6.4 The 30 s budget

wallet-pir runs no process-spawning e2e test inside its 30 s gates. The
per-case deadline is `WALLET_PIR_CASE_SECONDS` (default 30) with
`--test-threads=1` (`tools/ci/test_budget.py:67-78`); for `pir-e2e-tests`,
`tools/ci/integration-tests.json:84-105` lists only `ci_hint` (no binaries) as
fast. Everything spawning binaries is "full", `#[ignore]` or behind
`qualification-tests` (e.g. `reorg_recovery.rs:1053-1056`, budgets of 240 s
preparation and 90 s advance). No nextest. Our spec asks for more (section 8,
C13).

What makes our e2e feasible in 30 s: one table costs about 1 s to prepare on
a multi-core host (section 1.4) and our fixtures need only a handful of
tables; with `poll_ms: 100` on the publisher and a 1 s query-host poll, a
two-table cold bootstrap is a few seconds. Budget memory at about 0.5 GiB per
host process per two tables.

### 6.5 Helpers worth copying (as patterns; our code, not vendored crates)

| Path | Lines | Purpose |
|---|---:|---|
| `wp/tools/pir-e2e-tests/tests/support/query_host.rs` | 116 | `free()`, `Host` (spawn, log, `/ready` poll, `assert_running`, SIGTERM). Copy first |
| `wp/tools/pir-e2e-tests/tests/support/receiver.rs` | 321 | Full publisher + node double + TLS edge + query host; restart with config mutation; outage via fault file. Best template |
| `wp/src/pir-publisher/tests/support/distribution_tls.py` | 227 | TLS edge with fault injection and discovery routing |
| `wp/src/pir-query-server/tests/support/status_tls.rs` | 88 | openssl cert + edge spawn owning its `TempDir` |
| `wp/tools/pir-e2e-tests/src/enhance_fixture.rs:86-166` | ~80 | `tls_with_discovery` returning a trusting `reqwest::Client` |
| `wp/src/shared/pir-sync-manager/src/tests.rs:15-80` | ~65 | In-process `tokio-rustls` TLS server (no Python) |
| `wp/tools/pir-e2e-tests/tests/rpc.rs:31-37` | 7 | std `Process` kill-on-drop guard |
| `wp/tools/pir-e2e-tests/tests/reorg_recovery.rs:217-262` | ~45 | Node outage switch in front of the double |

## 7. Recommended design for this workspace

Rule: wallet-pir crates are git dependencies at `40b0ba6a`, used unchanged.
Everything wallet-pir keeps private or product-specific is rewritten in our
crates, following the cited files.

| Crate | Depends on (wallet-pir / zakura) | Features | Owns |
|---|---|---|---|
| `tachyon-pir-protocol` | `zakura-pir-protocol` | `native` | Constants (`P8_PROFILE` string, geometry names, `SLOTS`, `ENTRY_BYTES`), placement hash, session id, header framing, map/scope/table JSON types (`deny_unknown_fields`), `Shared::new(product, geometry)` (masks + setup from our seeds), exact lengths via `P8.request_len_bits`/`response_len`/`public_len`, `validate_row`. No async, no HTTP. |
| `tachyon-pir-table` | `zakura-pir-server-core`, `zakura-pir-query-server`, `reinspiring`, `rayon` (feature `runtime`) | none on zakura crates | Row building (seed search, segments), table digests, `Runtime::{build, answer, self_check, held_bytes}` (section 1.2). Used by publishers and the query host; never by the client. |
| `tachyon-chain` | `pir-node-rpc` | none | `NodeClient` over `RpcClient::new(url, Credentials, id, HttpPolicy::SHARED)`; impl `pir_pipeline::ingestion::{CanonicalChain, ChainSource}` if needed for tests (the host supplies its own follower) |
| `tachygram-store` | (none from wallet-pir; `pir-pipeline` only for `BlockId` if wanted) | | SQLite journal (`rusqlite = { version = "0.37", features = ["bundled"] }`, as `receiver-store`) |
| `tachygram-publisher` | `pir-pipeline`, `pir-publication`, `pir-control`, `tachyon-chain`, `tachyon-pir-table` (`runtime`), `axum = "0.7"`, `tokio` | | `configure(..) -> PublisherDefinition<axum::Router>`, `Writer: Ingestion`, `Export: Publication`, `Distribution` (store + authority + 3 routes), canonical guard |
| `memo-publisher`, `registry-publisher` | same | | as above; registry uses `Publisher` + `PublisherEngine::run` instead of `Ingestion` |
| `tachyon-pir-publisher` (bin) | `pir-publisher` **`features = ["chain"]`**, `pir-pipeline`, `pir-service-discovery`, product publishers | product features | Config (`deny_unknown_fields`), `--config`, `--check`, signals, `run_registered`; optional `--contract-info` via `ContractInfo::new("tachyon-pir-publisher", PRODUCTS, &[])` |
| `tachyon-pir-query-server` (bin + lib) | `pir-pipeline`, `pir-sync-manager`, `pir-publication`, `pir-service-discovery`, `pir-control` (`admission`), `pir-observability` (`http`), `tachyon-pir-protocol`, `tachyon-pir-table` (`runtime`), `axum = "0.7"`, `reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"] }` (for `reqwest::Certificate`), `tokio` | product features | Host (management, public listener with per-product slots and 503, drain), discovery preflight, per-product `PublicationAdapter` + `Publications` (current + 600 s retained, epoch, revoke), routes, admission, metrics |
| `tachyon-pir-cli` | `zakura-pir-client` (`native`), `tachyon-pir-protocol`, `reqwest` 0.12 rustls | | Map fetch, `P8.prepare`, POST, header check, `P8.decode_cols`, `validate_row`, lookup |
| `tachyon-pir-e2e-tests` | `pir-service-discovery`, `pir-publication`, `tachyon-*`, `tachyon-fixtures`, `reqwest`, `tempfile`, `libc`; TLS edge deps (see below) | mirror the hosts' product features | `[[bin]]` re-declarations of both hosts' `main.rs`, process helpers, TLS edge, independent oracle |

Root manifest notes:

- Keep the `[patch.crates-io]` block (patches do not propagate through git
  dependencies). Already present.
- Use **axum 0.7** everywhere a router crosses into wallet-pir
  (`PublisherDefinition<axum::Router>`, `PublicMount::mount`,
  `pir_observability::observe`); route parameters are `:param` / `*rest`.
  Locked versions in wallet-pir: axum 0.7.9, hyper 1.9.0, tokio 1.51.1, tower
  0.5.3, reqwest 0.12.28, rustls 0.23.37 (ring provider), tokio-rustls 0.26.4.
  Copy wallet-pir's `Cargo.lock` entries when first resolving, to land on the
  same versions.
- `zakura-pir-*` need no product features (`transparent`, `receiver`) for our
  use; enable only `native` on `zakura-pir-protocol` and `zakura-pir-client`.
- Do **not** depend on `transparent-shared`, `receiver-pir-server` or
  `pir-query-server`: they are product-specific, outside the reuse list, and
  the first one's `TableProfile` silently selects P16 for our table names.

Obstacles and workarounds:

| Obstacle | Workaround within the rule |
|---|---|
| `run_registered` serves no TLS and no `/metrics`; discovery only on management | TLS proxy in front (tests: our own edge); each product serves `/metrics` on its listener, or add a tiny `metrics` `PublisherDefinition` with its own listener |
| Listeners must be private with fixed ports; no shared listener; no overlapping writable paths | One listener and one state dir per definition; memo + first-contact in **one** definition (they share `memo-v1.sqlite`) |
| A failed product is never restarted | Treat product errors as `FailurePolicy::NextPoll` where safe; rely on systemd for restarts |
| `HttpsSource` is HTTPS-only | Tests run a TLS edge; pass the test cert as `publisher_ca` |
| Receiver's `Publications`, `public::Slot`, `hold`, `supervise`, `receiver::sync` are private or Receiver-specific | Reimplement in `tachyon-pir-query-server` (about 1,000 lines total) following the cited files |
| `TableProfile`/`profile_for` choose P16 for unknown table names | Use the free `P8` primitives (section 1) |
| `P8_PROFILE` is behind `zakura-pir-protocol/transparent` | Define the string in `tachyon-pir-protocol` and assert the derived `Profile` equals `P8` |
| `pir-publication/test-support` only fakes record writes | Enable it as a dev-dependency for durability fault tests only |
| `distribution_tls.py` is Python + openssl | Prefer our own in-process Rust edge (`tokio-rustls` + `rcgen` for the cert, or the openssl CLI as wallet-pir does) with the same fault modes and discovery routing |

## 8. Spec statements that conflict with wallet-pir

| # | Spec text | What wallet-pir actually does | Proposed correction |
|---|---|---|---|
| C1 | 6.6: "Reference sizes ... for `4k8`: upload 28,168 bytes, reply 10,256 bytes" | Those include txid's 8-byte binding and 16-byte reply header. The P8 payload is 28,160 B (4k8) / 20,992 B (2k8); the reply body is 10,240 B | "Payload 28,160 B (`tachyon-4k8`) or 20,992 B (`tachyon-2k8`); request body 52 + payload = 28,212 / 21,044 B; response 52 + 10,240 = 10,292 B" |
| C2 | 6.6: "public setup about 60 KB" | 60.1 KB is txid's base64-in-JSON. Raw is `P8.public_len(8192)` = 45,056 B; spec 10.2 serves it as octet-stream | "Public setup 45,056 bytes" |
| C3 | 6.1: two geometries, no rule for choosing | txid fixes one geometry per root by operator choice; deployed `txid-4k8` | Add: "Version 1 publishes every table at `tachyon-4k8`. `tachyon-2k8` is reserved." (or the pure-count rule of 1.7) |
| C4 | 6.6 / 11.5 / 17: "resident runtime about 160 MiB per table" | 160.17 MiB is the planning bound for 4k8; 2k8 is 144.17 MiB (not half); measured held 141.67 / 125.67 MiB on AVX-512 VBMI; build peaks add 200-500 MiB | State both bounds and the build transient; note 2k8 does not halve memory |
| C5 | 6.2: entries "sorted ascending by `(key, ordinal)`"; client "sorted order" check | The ordinal is not stored in memo/first-contact/registry entries, so no reader can verify it | Publisher sorts by `(key, ordinal)`; readers check keys non-decreasing (strictly increasing for tachygram) |
| C6 | 6.3: placement hashes `key`, while `key16` is what is stored | txid recomputes placement for every entry from stored bytes; with a 32-byte tag input, other entries' placement cannot be recomputed | Define the placement input as the stored key: `key16` (memo, first-contact, registry), full `tg` (tachygram) |
| C7 | 6.6 / 10.4: `public_sha256` in session id | Correct, but implies the publisher fully prepares every table it publishes (about 1 s each on many cores, 4-5 s single-threaded); open-epoch rebuilds at up to 64 buckets x segments per publication | Add a cost note and require coalescing (one active, one pending) per product; consider publishing open scopes at a lower cadence than blocks |
| C8 | 11.2: "`target_height`, checkpoints, `withdraw_above`, `rewind`" and the user's "append, publish" | Trait is `target_height`, `start_height`, `observed`, `committed`, `checkpoint_at_or_below`, `withdraw_above`, `rewind`, `ingest(from, through)`, `caught_up`, `failed`; publication is `PublisherExecutor::publish(Publication)` from `caught_up` with phases `check/build/store/verify/assemble/recheck/commit` | Name the real methods; say publication runs from `caught_up` |
| C9 | 11.2 / 3: protocol-order hashes | `pir_pipeline::ingestion::BlockId.hash` and `ChainObservation.genesis` are display-order strings | Note the conversion at the `Ingestion` boundary |
| C10 | 11.7 publisher config: no distribution listen addresses; `first_contact` shares memo `state_dir` | Each `PublisherDefinition` must declare its own private, fixed-port listener; writable paths of different definitions must not overlap | Add `listen` per product section; make memo and first-contact one definition (one listener, both prefixes) or give them separate dirs |
| C11 | 11.8: publisher management `/metrics` | `run_registered`'s management has only `/health`, `/ready`, discovery | Publisher product metrics on each product listener at `/metrics` (or a metrics definition); query server keeps `/metrics` on management |
| C12 | 9.3: registry write API on "its own public listener" `127.0.0.1:9410` | `run_registered` refuses non-private listeners, so public exposure needs a proxy; `client_key` trusts `X-Forwarded-For` as sent | Say the write API listens privately behind a reverse proxy, and specify which client-address header the proxy sets and the service trusts |
| C13 | 13.1: "End to end: Publisher behind TLS, separately spawned query host ... each under 30 s" | wallet-pir runs no process-spawning test under 30 s; publisher TLS is a separate terminator; `HttpsSource` requires HTTPS | Keep the target but state the harness: a TLS edge (our code), small fixtures, `--test-threads=1`, release-fast, real geometry (no smaller table exists: rows must be a multiple of 2,048) |
| C14 | 11.6: "checks `/.well-known/pir-service-discovery` before start and before each sync pass" | Correct (preflight plus every `settle`), but the publisher serves it only on the loopback management listener | Require the TLS edge/proxy to route that path to the management listener |
| C15 | 10.5: 409 `stale_routing` vs 410 `session_unavailable` | Receiver answers 410 for every unknown session; txid uses 409 for retired revisions; telling "revoked by reorg" from "unknown" requires remembering revoked ids | Keep a revoked-session set for the retention window: in it -> 410; otherwise unknown -> 409 with `map_sha256` |
| C16 | 10.1 mounts "with `pir_control::PublicMount`, as in wallet-pir" | Receiver does not use it (nests twice); txid/Status/Enhance do | Keep `PublicMount`; drop "as Receiver does" if implied |
| C17 | 11.3: "uses `pir_publication` unchanged ... distribution prefixes ... `current`, `objects/<sha256>`, `authority/check`" | `pir-publication` has no routes; each product writes them (Receiver `distribution.rs:68-74,340-430`) | "Each publisher serves the three routes over `pir_publication` types, as Receiver does" |
| C18 | 11.4: "A rewind that reaches a sealed scope increments `recovery_epoch`" | `recovery_epoch` (the `AuthorityFence`) increases only on publisher restart or an explicit `Recover`; Receiver's reorg revocation bumps an in-memory serving epoch and withdraws (`latest_withdrawal`) | Either call `Recover` explicitly on a sealed-scope reorg (Status calls `recover()` after every withdrawal) or say the map's `recovery_epoch` is our own counter kept in the descriptor |
| C19 | 4.3: crates.io list omits `zakura-pir-server-core`, `zakura-pir-client` and direct `reinspiring` | `hint`/`preprocess`/`publish` are in server-core; `prepare`/`decode_cols` in client; the scan kernel in `reinspiring::native_kernel` | List all five plus `reinspiring` with their features (`native` on protocol and client) |
| C20 | 6.4: "`MAX_SEGMENTS = 8` from Receiver" | Receiver's 8 is `inventory::MAX_SEGMENTS` (segments of a shard), not overflow segments; txid has no overflow-segment cap | Keep 8 as our own constant; cite it as ours |
| C21 | 6.6: "the native ReinspiRING ... exactly as Receiver and transaction details use it" | True of the profile, but the public setup also depends on our seeds; txid's `TableProfile` would select P16 for our table names | Specify our seed derivation (1.5) in the spec so clients derive masks and setup independently |
