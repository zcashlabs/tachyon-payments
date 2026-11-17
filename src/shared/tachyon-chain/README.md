# tachyon-chain

Reads a Zakura node for the Tachyon payment-protocol PIR services: decodes
`getblock <hash> 2` results into `TachyonBlock`, does epoch arithmetic, and reads
the network identity. Spec sections 3, 4.2 and 11.1
(`docs/spec/spec-tachyon-payment-pir.md`).

## API

```rust
// Types
pub struct Hash(/* [u8; 32], protocol order */);
impl Hash {
    pub const ZERO: Hash;
    pub const fn from_protocol_bytes(bytes: [u8; 32]) -> Hash;
    pub const fn protocol_bytes(&self) -> &[u8; 32];
    pub fn parse_display(displayed: &str) -> Result<Hash, HashParseError>;
    pub fn display_hex(&self) -> String;
}   // also Display / FromStr / Debug in display order
pub struct BlockId { pub height: u32, pub hash: Hash }
pub struct NetworkIdentity { pub chain: String, pub genesis_hash: Hash, pub activation_height: u32 }
pub struct TachyonBlock { pub id: BlockId, pub prev: Hash, pub txs: Vec<TachyonTx> }
pub struct TachyonTx { pub tx_index: u32, pub txid: Hash, pub memo: Vec<u8>, pub own_actions: u32, pub stamp: Stamp }
pub enum Stamp {
    Proof { anchor: [u8; 32], tachygram_set: [u8; 32], tachygrams: Vec<[u8; 32]> },
    Pointer { aggregate_id: [u8; 64] },
}

// Decoding (pure)
pub struct DecodeOptions { pub strict_zip_rules: bool }   // Default: off
pub fn decode_block(result: &serde_json::Value, requested_hash: &Hash,
                    requested_height: Option<u32>, options: DecodeOptions)
    -> Result<TachyonBlock, DecodeError>;
impl NetworkIdentity {
    pub fn from_blockchaininfo(info: &serde_json::Value, genesis_hash: Hash)
        -> Result<NetworkIdentity, IdentityError>;
}

// Epochs
pub const EPOCH_LENGTH: u32 = 4096;        // default only; configure per network
pub const CONFIRMATION_DEPTH: u32 = 2;
pub const fn target_height(tip: u32) -> u32;  // tip - 2, saturating
pub struct EpochSchedule;  // new(activation_height, epoch_length) -> Result<_, EpochError>
//   pool_height(h), epoch_of(h) -> Option<u32>   (None below activation)
//   first_height(e), last_height(e) -> Option<u32> (None beyond u32 heights)

// Node reads (async, over pir_node_rpc::RpcClient)
impl NodeClient {
    pub fn new(rpc: RpcClient, options: DecodeOptions) -> NodeClient;
    pub fn connect(url, credentials: Credentials, options) -> Result<NodeClient, NodeError>;
    pub async fn network_identity(&self) -> Result<NetworkIdentity, NodeError>;
    pub async fn tip(&self) -> Result<BlockId, NodeError>;
    pub async fn block_hash(&self, height: u32) -> Result<Hash, NodeError>;
    pub async fn block(&self, hash: &Hash) -> Result<TachyonBlock, NodeError>;
    pub async fn block_at(&self, id: &BlockId) -> Result<TachyonBlock, NodeError>;
}
```

Only `getblockchaininfo`, `getblockcount`, `getblockhash` and `getblock` at
verbosity 2 are called.

### Decoding rules

- The block's `hash` must equal the requested hash, and its `height` the
  requested height when one is given (`block_at`). Every transaction's
  `blockhash` and `height`, where present, must name the same block; `nTx`,
  where present, must equal the number of listed transactions.
- A block whose `confirmations` is negative is refused as
  `DecodeError::Stale`: the node serves a block reorged away by hash with
  `confirmations: -1` (spec section 3), and it is not canonical. A missing
  `confirmations` is accepted, as is zero or any positive count.
- `previousblockhash` is required except at height 0, where `prev` is
  `Hash::ZERO` if it is absent (Zakura prints the zero hash for genesis;
  zcashd omits the field).
- Every txid must be a 64-digit hex hash, unique within the block.
- Only transactions with a `tachyon` member become `TachyonTx`. `tx_index` is
  the transaction's index among **all** transactions of the block (coinbase is
  0), the "tx index in block" of the entry ordinal (spec 5.2).
- `own_actions` is the length of `tachyon.actions`. `memo` is `tachyon.memo`
  as hex (`""` is an empty memo). Both are required.
- `stamp.type` is `"proof"` (requires `anchor`, `tachygramSet`, `tachygrams`;
  32 bytes each) or `"pointer"` (requires a 64-byte `aggregateId`). A stamp
  carrying the other kind's fields, or any other type, is refused. Other fields
  (`proof`, `coverage`, `feeZat`, ...) are ignored.
- Tachygram counts are read from the data; nothing assumes two per action.
- With `strict_zip_rules`: at most 4,095 actions per bundle; 2 to 8,190
  tachygrams per proof stamp, strictly ascending; no tachygram twice in the
  block. Off by default; the alpha testnet does not follow the draft ZIPs.

## Byte orders

- `Hash` (block hashes, txids, `prev`, `genesis_hash`) is held in protocol
  order, the reverse of the node's hex. `Display`/`FromStr` use display order,
  the hex the node and explorer print.
- Tachygrams, anchors, tachygram sets, aggregate ids and memos are the node's
  hex decoded left to right, not reversed.
- On the alpha testnet every sampled `aggregateId` is the covering proof
  transaction's txid followed by its `authdigest`, both in protocol order
  (each 32-byte half is the reverse of the node's hex for that field), in the
  same block. The decoder does not rely on this; the golden expectations
  record it as `pointers_name_a_proof_in_block`.

## Golden fixtures

`tests/golden/` holds blocks frozen from the alpha testnet node
(`http://tachyscan.cnode.phd:18232`) by `tests/golden/capture.py`:

- `block-<height>.json`: the verbatim `result` of `getblock <hash> 2`;
- `getblockchaininfo.json`: the verbatim `getblockchaininfo` result;
- `manifest.json`: capture date, node URL, tip, genesis hash, and each block's
  height, hash, size, SHA-256 and the reason it was chosen;
- `expectations.json`: the expected decoder output, computed by the capture
  script in Python straight from the raw JSON. It is the independent oracle of
  `tests/golden.rs` and is never produced by the Rust decoder.

### How the frozen set was chosen

Captured 2026-11-14 (tip 40,295). Heights were chosen from a read-only scan of
17,739 blocks (heights 108–13,646 and 36,000–40,199) plus every 200th block.
In that scan:

- every block has zero or one proof stamp; no block has several proof stamps,
  and none has only pointer stamps (every pointer names a proof in its block);
- every `tachyon.memo` is empty, so no frozen block has a non-empty memo (the
  memo decoding test edits one in);
- proof stamps carry 3 to 112 tachygrams, about half of them odd, always
  strictly ascending; the count is 2 × covered actions + 1 to 16;
- every bundle has 1 to 3 actions; 2,094 blocks have no Tachyon transaction.

The set: genesis (0), activation (8), the first Tachyon block (109, a
3-tachygram stamp), a block with a transparent transaction between the
coinbase and the Tachyon ones (148), the epoch boundary (4103, 4104), an odd
17-tachygram stamp with pointers (40180), a 17-transaction block (40182), a
post-activation block without Tachyon transactions (40192) and a largest
(112-tachygram) stamp (40086). `manifest.json` records each reason.

`tests/golden.rs` decodes every frozen block against the expectations, tests
mismatch, hex, duplicate-txid, stamp-shape and strict-rule refusals on edited
copies, and runs `NodeClient` over real HTTP against an in-test JSON-RPC
responder serving the fixtures. One NodeClient test also runs against
`tachyon_fixtures::NodeDouble` (dev-dependency) with
`StaleBlocks::Serve`, fetching a block after it is reorged away.

### Refreshing

```sh
cd src/shared/tachyon-chain/tests/golden
python3 -I capture.py                       # default height set, live node
python3 -I capture.py --heights 8,4104,...  # another set
python3 -I capture.py --expectations-only   # recompute expectations offline
cargo test --locked -p tachyon-chain --profile release-fast
```

The script refuses any method but the four read methods above and calls
`getblock` only at verbosity 2. It rechecks every height's hash after the
capture so a reorg during capture fails it. Keep the directory under about
3 MB. A refresh that changes `expectations.json` is a change in node output
and must be reviewed as one; `tests/golden.rs` names some heights directly
(40180 and 40182 as the edited block and its substitute), so a new height set
must keep those or update the tests.
