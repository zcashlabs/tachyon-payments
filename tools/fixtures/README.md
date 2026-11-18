# tachyon-fixtures

The node double, synthetic chains and the synthetic envelope generator for the
Tachyon PIR services' fast tests (spec sections 4.2, 5 and 13.1).

The crate does **not** depend on `tachyon-chain`, on the envelope parser in
`tachyon-pir-protocol`, or on any other decoder of the node's JSON or of memos.
It is the independent oracle the decoder and the products are checked against,
so it must stay that way. (`tachyon-pir-protocol` is a dev-dependency used only
by `tests/envelope.rs` to cross-check the parser against the authored truth.)

## Synthetic chain model (`SyntheticChain`)

A chain is a pure function of a `ChainConfig` and the sequence of builder calls
made on it. The same seed and the same calls give the same blocks, hashes
included, in any process.

- **Network identity**: `chain` (default `"test"`), the NuTachyon
  `activation_height` (default 8, as on the alpha testnet; `None` leaves the
  upgrade out of `getblockchaininfo`), and the genesis hash, which is derived
  from the seed. Block times are `genesis_time + height * block_interval`
  (defaults: the sampled testnet time, 9 s).
- **Blocks** run from height 0 (genesis, coinbase only) to the tip. Every block
  starts with a transparent coinbase (no `tachyon` key) paying the subsidy plus
  the block's fees; Tachyon transactions follow.
- **Tachyon transactions** carry N own actions (`cv`, `rk`, `spendAuthSig`
  filler), a memo of arbitrary bytes, a value balance, and either a **proof
  stamp** (anchor, tachygramSet, an arbitrary list of 32-byte tachygrams,
  proof, coverage, feeZat) or a **pointer stamp** (64-byte aggregateId,
  feeZat).
- **Builder API**: `append(BlockSpec)`, `append_random(n)`, `reorg(fork, specs)`,
  `reorg_random(fork, n)`. `TxSpec::tachyon(n)` with `.memo(..)`, `.proof(..)`
  (explicit tachygrams, stored exactly as given, so a test may author disorder
  or duplicates), `.random_proof(count)`, `.pointer()`, `.stamp(..)`,
  `.with_actions(..)`, `.fee(..)`, `.value_balance(..)`. `TxSpec::from_tx` and
  `BlockSpec::from_block` re-author existing content so it keeps its txids
  (for example on the other side of a reorg).
- **Random content** comes from `TxMix`: Tachyon transactions per block, actions
  per transaction, proof-stamp and memo percentages, tachygram counts (odd counts
  occur) and memo lengths. Random tachygrams are strictly ascending within a
  stamp and distinct across the whole chain, every branch included;
  `fresh_tachygrams(n)` hands out more such tachygrams for explicit use. No
  Tachyon transaction is generated below the activation height (an explicit
  `BlockSpec` may still place one there).
- **Hashes**: held in protocol order, printed in display (reversed) hex, exactly
  as the node does (spec section 3). A transaction's bytes (the `hex` field) are
  a fixture encoding, **not** the node's wire format; its txid is
  `SHA256d("txid" || bytes)`, so equal content has an equal txid. A block hash is
  `SHA256d(header)` over the previous hash, the Merkle root of the txids, height,
  time and a **fork salt** (the chain's reorg generation), so a replacement block
  differs from the one it replaces even when its content is identical.
  Tachygrams, anchors and other values are printed unreversed.
- **Reorgs**: `reorg(fork_height, new_blocks)` moves every block above
  `fork_height` to a stale set (still retrievable with `lookup(hash)`) and
  appends the new ones. It may shorten the chain; a fork above the tip is
  refused.
- **Ground truth**: `blocks()`, `block(h)`, `lookup(hash)`, `Block::txs()`,
  `Block::tachyon_txs()` (index in block, txid, own actions, memo bytes, the
  authored memo, stamp), `Block::tachygrams()`, `active_tachygrams()`. These
  return the values as authored and never parse the served JSON. The envelope
  ground truth is below.

## Synthetic envelopes (`tachyon_fixtures::envelope`)

The generator writes `vMemoTachyon` envelopes itself, straight from spec
section 5.1, and the chain keeps every memo **as authored**. Nothing in the crate
parses an envelope, so the memo and first-contact oracle of an end-to-end test
shares no code with the products' parser.

- **Authoring**: `AuthoredEnvelope { entries }` with
  `AuthoredEntry::FollowUp { tag, ciphertext: [u8; 128] }`,
  `FirstContact { tag, kem_ciphertext: [u8; 1088], ciphertext: [u8; 128] }` and
  `WalletState { tag, ciphertext: Vec<u8> }`. `to_bytes()` is
  `"TPM1" || u8 count || (kind || tag(32) || body)*`, a wallet-state body being
  `len (u16 LE) || ciphertext`. `check()` / `is_valid()` apply the spec's rules to
  the authored values (1 to 16 entries, wallet state at most 1024 bytes, nonzero
  `key16`; the generator also never authors an all-zero wallet-state key, which
  spec 6.2 leaves unspecified).
- **Malformed envelopes** (scenario 7): `Malformation::author(rng)` or
  `AuthoredEnvelope::malformed(kind, rng)`. Each breaks exactly the rule it names
  and keeps every other one:

  | `Malformation` (JSON name) | Bytes |
  |---|---|
  | `BadMagic` (`bad_magic`) | a valid envelope with another 4-byte magic |
  | `MissingCount` (`missing_count`) | exactly `TPM1` |
  | `CountZero` (`count_zero`) | `TPM1 00` |
  | `CountAboveMax` (`count_above_max`) | count 17, 17 well-formed entries |
  | `CountExceedsEntries` (`count_exceeds_entries`) | count `n + 1` (at most 16), `n` whole entries |
  | `UnknownKind` (`unknown_kind`) | one entry with kind `0x00` or `0x04..=0xff`, a tag and 128 body bytes |
  | `TruncatedBody` (`truncated_body`) | the last entry's body cut by 1 byte up to all of it |
  | `TrailingBytes` (`trailing_bytes`) | a valid envelope plus 1 to 32 bytes |
  | `StateTooLong` (`state_too_long`) | one wallet state with `len` 1025 to 1100 and `len` bytes |
  | `FollowUpZeroKey` (`follow_up_zero_key`) | one follow-up whose `key16` is all zero |
  | `FirstContactZeroKey` (`first_contact_zero_key`) | one first contact whose `key16` is all zero |
  | `RandomGarbage` (`random_garbage`) | 1 to 300 random bytes not starting with `TPM1` |

- **Generators** (all deterministic from a `Rng`; `chain.rng()` is the chain's
  own stream): `random_tag` (nonzero `key16`), `tag_with_key16` (a test's ground
  key plus 16 random bytes, scenario 6), `AuthoredEntry::random`,
  `random_with_tag`, `random_sized`, `AuthoredEnvelope::random(rng, n, m, k)`,
  and `pack(rng, entries)`, which splits entries in order into envelopes of 1 to
  16. On the chain: `envelope_block(n, m, k)` (that many follow-up,
  first-contact and wallet-state entries, shuffled and packed one envelope per
  transaction) and `entries_block(entries)` (exactly these entries, in this
  order: repeated tags for scenario 5, ground keys for scenario 6).
- **Transactions**: `TxSpec::envelope(&envelope)` (panics on an invalid one),
  `TxSpec::malformed_memo(Malformation)` (bytes drawn from the chain's stream at
  build time), `TxSpec::malformed_bytes(MalformedMemo)`, and
  `BlockSpec::envelopes(&[..])` (one transaction per envelope). `TxSpec::memo`
  still takes raw bytes, recorded as unclassified.
- **Random envelopes**: `ChainConfig::envelopes = Some(EnvelopeMix { .. })` (the
  chain spec's `envelopes`) gives generated Tachyon transactions an envelope with
  chance `envelope_percent` (else an empty memo), malformed with chance
  `malformed_percent` (a `Malformation` drawn uniformly), else valid with
  `follow_ups`, `first_contacts`, `wallet_states` entries (inclusive ranges,
  shuffled; at least one entry, at most 16) and `wallet_state_len` bytes per
  wallet state. With chance `repeat_tag_percent` an entry reuses a tag already
  drawn for its kind on the chain, earlier entries of the same envelope included
  (scenario 5); with chance `key16_percent` its tag takes one of `key16s`
  (scenario 6). Without a mix (`None`, the default), generated memos are the
  `TxMix`'s random bytes as before; they never start with `TPM1` and are
  recorded as `RandomGarbage`.
- **Ground truth**: `Tx::authored_memo()` and `TachyonTxTruth::authored_memo`
  give each Tachyon transaction's `AuthoredMemo`: `Empty`, `Envelope(..)`,
  `Malformed(MalformedMemo { malformation, bytes })` or `Raw(bytes)` (set by a
  test; not classified). From it, never from bytes:
  - `Block::follow_ups()`, `chain.follow_ups(heights)`: every follow-up entry of
    a valid envelope as `FollowUpTruth { height, tx_index, entry_index, tag,
    ciphertext }`, in ordinal order `(height, tx index in block, entry index)`
    (spec 5.2). `tx_index` is the position in the block's `tx` list, the
    coinbase being 0; `entry_index` counts every entry of the envelope.
  - `Block::first_contacts()`, `chain.first_contacts(heights)`:
    `FirstContactTruth { .., tag, kem_ciphertext, ciphertext }`, likewise.
  - `Block::envelope_counts()`, `chain.envelope_counts(heights)`:
    `EnvelopeCounts { empty, valid, malformed, raw, follow_ups, first_contacts,
    wallet_states }` (entries of valid envelopes only).

  These read the active chain; a block re-authored with `BlockSpec::from_block`
  keeps its envelopes and their truth.

## JSON-RPC server (`NodeDouble`)

`NodeDouble::start(chain, options)` binds `127.0.0.1:0` (or `start_on(addr)`)
and serves HTTP/1.1 JSON-RPC on any path except `/control/...`. The chain sits
behind a lock: `node.chain()` returns a guard, and edits made through it (append,
reorg) are visible to the next request.

| Method | Answer |
|---|---|
| `getblockchaininfo` | `chain`, `blocks`, `bestblockhash`, `upgrades` (the testnet's eleven entries, each capped at the NuTachyon height, plus `NuTachyon` under branch id `fffffffc` at the configured height), and every other field of the sample |
| `getblockcount` | the tip height |
| `getbestblockhash` | the tip hash |
| `getblockhash <h>` | the active hash at `h`; -8 `Block height out of range` above the tip |
| `getblock <hash\|height> [verbosity]` | verbosity 2 (transaction objects), 1 (txids, the default) or 0 (hex of the fixture block encoding); -5 `Block not found` for an unknown hash; -8 above the tip or for another verbosity |
| anything else | -32601 `Method not found` |

Also -32700 for unparseable JSON and -32602 for missing or wrongly typed
params. JSON-RPC errors are answered with HTTP 200 and a
`{"result": null, "error": {"code", "message"}, "id"}` body, echoing `jsonrpc`
and `id`, which is the envelope `pir-node-rpc` reads (it refuses non-2xx
statuses before parsing). JSON-RPC 2.0 batches are answered entry by entry.

**Shape conformance.** `getblock <hash> 2` and `getblockchaininfo` have exactly
the fields, nesting, JSON types (integers and floats told apart) and fixed hex
lengths of the responses saved from the testnet in `tests/data/`
(`tests/conformance.rs` checks every object kind). Block fields: `hash`,
`confirmations`, `size`, `height`, `version`, `merkleroot`, roots,
`nTx`, `tx`, `time`, `nonce`, `solution`, `bits`, `difficulty`, `chainSupply`,
`valuePools`, `trees`, `previousblockhash` (absent at genesis), `nextblockhash`
(absent at the tip). Transaction fields: `txid`, `height`, `blockhash`,
`in_active_chain`, `confirmations`, `hex`, `vin`, `vout`, ..., and for Tachyon
transactions `tachyon` = `{actions[{cv, rk, spendAuthSig}], valueBalance,
valueBalanceZat, cvFeeMax, bindingSig, memo, stamp}` with
`stamp.type = "proof"` or `"pointer"`. `memo` is hex, `""` when empty. Values the
services do not read (roots, solution, scripts) are well-formed filler.

**Blocks reorged away** (`NodeOptions::stale_blocks`):

- `StaleBlocks::NotFound` (default): `getblock` by the old hash answers -5
  `Block not found`, as for any unknown hash (and as `wallet-pir`'s node double
  does).
- `StaleBlocks::Serve`: the old block is served with `confirmations: -1`, no
  `nextblockhash`, and `in_active_chain: false`, `confirmations: 0` on each
  transaction, as zcashd does for a side-chain block it still holds.

Neither the saved samples nor `wallet-pir`'s double show which the live Zakura
node does, so both are offered and consumers must cope with both.

**Authentication.** `NodeOptions::auth = Some(BasicAuth { user, password })`
requires HTTP Basic credentials (401 otherwise); `BasicAuth::write_cookie`
writes the matching cookie file for `pir_node_rpc::Credentials::CookieFile`.
`None` accepts every request.

### Fault injection

| Call | Effect |
|---|---|
| `set_delay(Some(method) \| None, d)` | delay that method (or every method) by `d` before answering |
| `fail_next(Some(method) \| None, n, Failure::Http(500))` | the next `n` matching calls get that HTTP status (with a JSON-RPC error body) |
| `fail_next(.., n, Failure::Rpc { code, message })` | the next `n` matching calls get that JSON-RPC error |
| `mismatch_next(n, BlockMismatch::..)` | the next `n` successful `getblock` results (verbosity 1 or 2) are inconsistent: `Hash`, `Height`, `PreviousHash`, `OtherBlock` (another block served whole), `TxBlockHash`, `TxHeight` (verbosity 2 only), `Txid` |
| `outage().await` / `restore().await` | close the listener and drop open connections (calls fail to connect), then listen again on the same address; the chain and counters are kept |
| `clear_faults()` | remove all of the above except an outage |

### Request counting

Every call is recorded on arrival, failed and delayed ones included (a batch
records each entry): `count(method)`, `counts()`, `calls()` (method and
params, in order, so a test can prove that blocks are fetched by hash), and
`reset_calls()`.

## The `tachyon-node-double` binary

```text
tachyon-node-double --spec <chain-spec.json> [--listen 127.0.0.1:0]
                    [--auth user:password] [--cookie <path>]
                    [--stale-blocks not_found|serve]
```

It builds the chain from the spec, listens, and prints exactly one line on
stdout: the JSON-RPC URL, `http://<ip>:<port>`. It runs until killed or
interrupted. `--cookie` writes the `--auth` credentials as a cookie file. Errors
go to stderr with a non-zero exit.

Chain spec file (every field optional; unknown fields are refused):

```json
{
  "seed": 7,
  "chain": "test",
  "activation_height": 8,
  "genesis_time": 1512758833,
  "block_interval": 9,
  "mix": {"tachyon_txs": [0, 4], "actions": [1, 3], "proof_percent": 30,
          "tachygrams": [5, 21], "memo_percent": 50, "memo_len": [1, 120]},
  "random_blocks": 40,
  "explicit_blocks": [
    {"txs": [
      {"actions": 2, "memo_hex": "54504d31", "stamp": {"type": "proof", "tachygrams": ["<64 hex>", "..."]}},
      {"actions": 1, "stamp": {"type": "random_proof", "count": 7}},
      {"actions": 3, "stamp": {"type": "pointer"}, "fee_zat": 50000, "value_balance_zat": 0},
      {"envelope": {"entries": [
        {"kind": "follow_up", "tag": "<64 hex>", "ciphertext": "<256 hex>"},
        {"kind": "first_contact", "tag": "<64 hex>", "kem_ciphertext": "<2176 hex>", "ciphertext": "<256 hex>"},
        {"kind": "wallet_state", "tag": "<64 hex>", "ciphertext": "<up to 2048 hex>"}
      ]}},
      {"malformed": "trailing_bytes"}
    ]}
  ],
  "envelopes": {"envelope_percent": 80, "malformed_percent": 10,
                "follow_ups": [0, 4], "first_contacts": [0, 1], "wallet_states": [0, 1],
                "wallet_state_len": [0, 64], "repeat_tag_percent": 10,
                "key16s": ["<32 hex>"], "key16_percent": 0}
}
```

A transaction takes at most one of `memo_hex`, `envelope` (refused unless valid;
`TxJson::with_envelope(&envelope)` writes one from an `AuthoredEnvelope`) and
`malformed` (a `Malformation` name; the bytes are drawn from the chain's stream,
so an in-process build draws the same ones). `envelopes` (shown with its
defaults) applies to `random_blocks` and to random blocks appended or reorged in
through the control endpoints; omitted, generated memos are the mix's random
bytes.

`random_blocks` generated blocks follow genesis, then `explicit_blocks` in
order. `"activation_height": null` leaves NuTachyon out.

The same spec built in-process with `ChainSpec::build` gives the same chain, so a
test process holds the ground truth of the separate binary; applying the same
control edits to its local copy (`reorg_random`, `append_random`, `append`) keeps
it in step (`tests/binary.rs` relies on this).

### Control endpoints

On the same listener, under `/control/`, without RPC authentication. POST bodies
are JSON; each success answers the state below, a bad request answers 400 with
`{"error": ...}` and changes nothing.

| Endpoint | Body | Effect |
|---|---|---|
| `GET /control/state` | | `{"tip", "tip_hash", "genesis_hash", "generation", "up", "counts"}` |
| `POST /control/append` | `{"blocks": [<block>...], "random_blocks": n}` | append explicit blocks, then `n` random ones |
| `POST /control/reorg` | `{"fork_height": h, "blocks": [...], "random_blocks": n}` | replace everything above `h` with the explicit blocks, then `n` random ones |
| `POST /control/outage` | `{"duration_ms": d, "delay_ms": 50}` | after `delay_ms` (so this response is delivered first) take the server down for `d` ms, then bring it back |
| `POST /control/fail` | `{"method": "getblock" \| null, "count": n, "http_status": 500}` or `{..., "rpc_code": -28, "rpc_message": "..."}` | fail the next `n` calls |
| `POST /control/delay` | `{"method": "getblock" \| null, "ms": d}` | per-method (or default) delay |
| `POST /control/mismatch` | `{"kind": "hash" \| "height" \| "previous_hash" \| "other_block" \| "tx_block_hash" \| "tx_height" \| "txid", "count": n}` | mismatch injection |
| `POST /control/clear-faults` | `{}` | clear delays, failures and mismatches |
| `POST /control/reset-calls` | `{}` | forget the request counts |

A `<block>` is `{"txs": [<tx>...], "time": t}` with `<tx>` as in the spec file.

## Tests

`cargo test -p tachyon-fixtures --profile release-fast`:

- `conformance`: served shapes against the saved testnet samples; NuTachyon
  follows the configured activation or is absent.
- `ground_truth`: every served block round-trips the authored model through
  `pir_node_rpc::RpcClient`; blocks are fetched by hash; cookie and fixed
  credentials, batches, and the node's error codes.
- `reorg`: hashes above the fork change and those below do not, while readers
  are calling; stale-block policies; refused reorgs.
- `faults`: delays, HTTP and JSON-RPC failures, every mismatch kind, outage and
  restore on the same address, control endpoints, malformed requests.
- `binary`: the binary prints its URL, serves the spec's chain (equal to an
  in-process build), authenticates with a cookie, and obeys control reorgs and
  appends; a bad spec is refused.
- `envelope`: each kind's bytes against hand-written bytes; every
  `Malformation` breaks exactly its rule (a structural walk written in the test,
  with that one rule switched off or repaired the bytes are valid); the node
  double serves the authored memo hex; ordinal order of the ground truth;
  repeated tags across blocks and within one envelope; ground keys; envelope
  specs for the binary (including control appends) and their refusals;
  determinism from the seed; and a cross-check that `tachyon-pir-protocol`'s
  parser accepts every authored envelope with equal content and refuses every
  malformation with the matching error.
