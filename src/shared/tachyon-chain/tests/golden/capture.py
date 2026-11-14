#!/usr/bin/env python3
"""Freeze Tachyon testnet blocks as golden fixtures for `tachyon-chain`.

Reads a Zakura node READ-ONLY. The only methods this script can send are
`getblockchaininfo`, `getblockcount`, `getblockhash` and `getblock` at
verbosity 2 (enforced by `Node.call`); it never sends or submits anything.

Usage (from any directory):

    python3 -I capture.py [--url URL] [--out DIR] [--heights H,H,...]
    python3 -I capture.py --expectations-only [--out DIR]

A capture writes into DIR (default: this script's directory):

- `getblockchaininfo.json`: the node's `getblockchaininfo` result, verbatim.
- `block-<height>.json`: each block's `getblock <hash> 2` result, verbatim
  (the exact bytes of the `result` member of the node's response).
- `manifest.json`: capture date, node URL, tip, genesis hash and every frozen
  block's height, hash, file size, SHA-256 and the reason it was chosen.
- `expectations.json`: what the decoder must produce for each block, computed
  here from the raw JSON with Python. It is the independent oracle of the golden
  tests: it shares no code with the Rust decoder.

`--expectations-only` recomputes `expectations.json` from the frozen files
without contacting any node.
"""

import argparse
import datetime
import hashlib
import json
import os
import sys
import urllib.request

DEFAULT_URL = "http://tachyscan.cnode.phd:18232"
REQUEST_ID = "tachyon-chain-capture"
ALLOWED_METHODS = {"getblockchaininfo", "getblockcount", "getblockhash", "getblock"}
TACHYON_UPGRADE = "NuTachyon"
EPOCH_LENGTH = 4096

# Heights frozen by default, each with the reason it is in the set. The scan
# that chose them is described in ../../README.md.
DEFAULT_BLOCKS = [
    (0, "genesis: no previousblockhash; its hash is the network's genesis_hash"),
    (8, "NuTachyon activation block (pool height 0, epoch 0); no Tachyon transactions"),
    (109, "first block with Tachyon transactions; smallest proof stamp seen (3 tachygrams, odd)"),
    (148, "a non-Tachyon transaction at index 1 precedes the Tachyon ones: tx_index counts all transactions"),
    (4103, "last block of epoch 0"),
    (4104, "first block of epoch 1"),
    (40180, "proof stamp with an odd tachygram count (17) and two pointer stamps"),
    (40182, "most transactions per block seen (17): one proof stamp, 15 pointer stamps"),
    (40192, "post-activation block with no Tachyon transactions"),
    (40086, "largest proof stamp seen (112 tachygrams, even)"),
]


class Node:
    """A read-only JSON-RPC client for a Zakura node."""

    def __init__(self, url):
        self.url = url

    def call_raw(self, method, params):
        """The raw response body of one call, and its parsed `result`."""
        if method not in ALLOWED_METHODS:
            raise ValueError(f"refusing to call {method}: this script is read-only")
        if method == "getblock" and (len(params) != 2 or params[1] != 2):
            raise ValueError("getblock is only called at verbosity 2")
        body = json.dumps(
            {"jsonrpc": "1.0", "id": REQUEST_ID, "method": method, "params": params}
        ).encode()
        request = urllib.request.Request(
            self.url, data=body, headers={"content-type": "application/json"}
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            raw = response.read().decode("utf-8")
        envelope = json.loads(raw)
        if envelope.get("error") is not None:
            raise RuntimeError(f"{method} {params}: {envelope['error']}")
        if "result" not in envelope:
            raise RuntimeError(f"{method} {params}: response has no result")
        return raw, envelope["result"]

    def call(self, method, params):
        return self.call_raw(method, params)[1]

    def call_verbatim(self, method, params):
        """The exact text of the response's `result` member, checked to parse to
        the same value as the whole response's result."""
        raw, result = self.call_raw(method, params)
        decoder = json.JSONDecoder()
        # Find the top-level "result" member: walk the envelope's members in order.
        position = raw.index("{") + 1
        while True:
            position = skip_space(raw, position)
            key, position = decoder.raw_decode(raw, position)
            position = skip_space(raw, position)
            assert raw[position] == ":", "malformed response envelope"
            start = skip_space(raw, position + 1)
            value, end = decoder.raw_decode(raw, start)
            if key == "result":
                assert value == result, "verbatim result does not match"
                return raw[start:end]
            position = skip_space(raw, end)
            assert raw[position] == ",", "response envelope has no result member"
            position += 1


def skip_space(text, position):
    while text[position] in " \t\r\n":
        position += 1
    return position


def sha256_file(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def protocol_hex(display_hex):
    """Display-order hash hex to protocol-order hex (bytes reversed)."""
    return bytes.fromhex(display_hex)[::-1].hex()


def capture(node, out, heights):
    os.makedirs(out, exist_ok=True)
    captured_at = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    reasons = dict(heights)

    info_text = node.call_verbatim("getblockchaininfo", [])
    with open(os.path.join(out, "getblockchaininfo.json"), "w", encoding="utf-8") as f:
        f.write(info_text)
    tip_height = node.call("getblockcount", [])
    tip_hash = node.call("getblockhash", [tip_height])
    genesis_hash = node.call("getblockhash", [0])

    blocks = []
    for height, reason in heights:
        block_hash = node.call("getblockhash", [height])
        text = node.call_verbatim("getblock", [block_hash, 2])
        block = json.loads(text)
        if block["hash"] != block_hash or block["height"] != height:
            raise RuntimeError(f"node answered block {block['hash']} for {height}")
        name = f"block-{height}.json"
        path = os.path.join(out, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        blocks.append(
            {
                "height": height,
                "hash": block_hash,
                "file": name,
                "bytes": os.path.getsize(path),
                "sha256": sha256_file(path),
                "reason": reasons[height],
            }
        )
    # The chain must not have moved under the capture.
    for entry in blocks:
        if node.call("getblockhash", [entry["height"]]) != entry["hash"]:
            raise RuntimeError(f"block {entry['height']} changed during capture")

    manifest = {
        "captured_at": captured_at.isoformat().replace("+00:00", "Z"),
        "node_url": node.url,
        "methods": sorted(ALLOWED_METHODS),
        "tip": {"height": tip_height, "hash": tip_hash},
        "genesis_hash": genesis_hash,
        "getblockchaininfo": {
            "file": "getblockchaininfo.json",
            "sha256": sha256_file(os.path.join(out, "getblockchaininfo.json")),
        },
        "blocks": blocks,
    }
    write_json(os.path.join(out, "manifest.json"), manifest)


def write_json(path, value):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(value, f, indent=2, sort_keys=False)
        f.write("\n")


def expect_tx(index, tx):
    bundle = tx["tachyon"]
    stamp = bundle["stamp"]
    memo = bundle["memo"]
    expected = {
        "tx_index": index,
        "txid": tx["txid"],
        "txid_protocol_hex": protocol_hex(tx["txid"]),
        "own_actions": len(bundle["actions"]),
        "memo_hex": memo,
        "memo_len": len(bytes.fromhex(memo)),
        "stamp": stamp["type"],
    }
    if stamp["type"] == "proof":
        tachygrams = stamp["tachygrams"]
        expected.update(
            {
                "anchor": stamp["anchor"],
                "tachygram_set": stamp["tachygramSet"],
                "tachygram_count": len(tachygrams),
                "first_tachygram": tachygrams[0] if tachygrams else None,
                "last_tachygram": tachygrams[-1] if tachygrams else None,
                # SHA-256 over the tachygrams' bytes, concatenated in node order.
                "tachygrams_sha256": hashlib.sha256(
                    b"".join(bytes.fromhex(t) for t in tachygrams)
                ).hexdigest(),
                "tachygrams_strictly_ascending": all(
                    bytes.fromhex(a) < bytes.fromhex(b)
                    for a, b in zip(tachygrams, tachygrams[1:])
                ),
            }
        )
    elif stamp["type"] == "pointer":
        aggregate = stamp["aggregateId"]
        expected.update(
            {
                "aggregate_id": aggregate,
                "aggregate_id_len": len(bytes.fromhex(aggregate)),
            }
        )
    else:
        raise RuntimeError(f"unknown stamp type {stamp['type']!r}")
    return expected


def expectations(out):
    with open(os.path.join(out, "manifest.json"), encoding="utf-8") as f:
        manifest = json.load(f)
    with open(os.path.join(out, "getblockchaininfo.json"), encoding="utf-8") as f:
        info = json.load(f)
    tachyon = [u for u in info["upgrades"].values() if u["name"] == TACHYON_UPGRADE]
    if len(tachyon) != 1:
        raise RuntimeError(f"expected one {TACHYON_UPGRADE} upgrade, found {len(tachyon)}")
    activation = tachyon[0]["activationheight"]
    genesis = manifest["genesis_hash"]

    blocks = []
    for entry in manifest["blocks"]:
        path = os.path.join(out, entry["file"])
        if sha256_file(path) != entry["sha256"]:
            raise RuntimeError(f"{entry['file']} does not match the manifest")
        with open(path, encoding="utf-8") as f:
            block = json.load(f)
        prev = block.get("previousblockhash", "00" * 32)
        txs = [expect_tx(i, tx) for i, tx in enumerate(block["tx"]) if "tachyon" in tx]
        proofs = {t["txid_protocol_hex"]: t for t in txs if t["stamp"] == "proof"}
        all_tachygrams = [
            t
            for tx in block["tx"]
            if "tachyon" in tx and tx["tachyon"]["stamp"]["type"] == "proof"
            for t in tx["tachyon"]["stamp"]["tachygrams"]
        ]
        height = block["height"]
        blocks.append(
            {
                "file": entry["file"],
                "height": height,
                "hash": block["hash"],
                "hash_protocol_hex": protocol_hex(block["hash"]),
                "prev": prev,
                "prev_protocol_hex": protocol_hex(prev),
                "tx_count": len(block["tx"]),
                "epoch": (height - activation) // EPOCH_LENGTH if height >= activation else None,
                "proof_stamps": len(proofs),
                "pointer_stamps": sum(t["stamp"] == "pointer" for t in txs),
                "tachygrams_distinct_in_block": len(set(all_tachygrams)) == len(all_tachygrams),
                # Observed chain fact, not used by the decoder: each pointer's
                # aggregateId is a proof transaction's txid || authdigest, both in
                # protocol order, in the same block.
                "pointers_name_a_proof_in_block": all(
                    t["aggregate_id"][:64] in proofs
                    and t["aggregate_id"][64:]
                    == protocol_hex(
                        next(
                            tx["authdigest"]
                            for tx in block["tx"]
                            if tx["txid"] == proofs[t["aggregate_id"][:64]]["txid"]
                        )
                    )
                    for t in txs
                    if t["stamp"] == "pointer"
                ),
                "tachyon_txs": txs,
            }
        )
    result = {
        "generated_by": "capture.py --expectations-only (Python, independent of the Rust decoder)",
        "network": {
            "chain": info["chain"],
            "genesis_hash": genesis,
            "genesis_protocol_hex": protocol_hex(genesis),
            "activation_height": activation,
        },
        "blocks": blocks,
    }
    write_json(os.path.join(out, "expectations.json"), result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--out", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument(
        "--heights",
        help="comma-separated heights to freeze instead of the default set",
    )
    parser.add_argument("--expectations-only", action="store_true")
    args = parser.parse_args()
    if not args.expectations_only:
        heights = DEFAULT_BLOCKS
        if args.heights:
            heights = [(int(h), "requested on the command line") for h in args.heights.split(",")]
        capture(Node(args.url), args.out, heights)
    result = expectations(args.out)
    total = sum(
        os.path.getsize(os.path.join(args.out, name)) for name in os.listdir(args.out)
    )
    print(
        f"{len(result['blocks'])} blocks; fixtures directory is {total} bytes",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
