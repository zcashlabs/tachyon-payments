#!/usr/bin/env python3
"""Known-answer vectors for tachyon-pir-protocol.

Computed with Python's hashlib and struct straight from the spec
(docs/spec/spec-tachyon-payment-pir.md), sharing no code with the Rust crate:

- 6.3  placement hash, bucket and row; n_buckets rule
- 6.6  PIR parameter seeds
- 6.2, 8.1, 9.1  entry layouts and canonical rows (keys ground into one row)
- 9.1, 9.3  address digest, proof of work, registration body
- 10.4 session id

Run from anywhere: `python3 -I generate.py` rewrites vectors.json beside this
script. The output is deterministic; `git diff` after a run must be empty.
"""
from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path

OUT = Path(__file__).resolve().parent / 'vectors.json'

# The alpha testnet genesis block, as the node prints it (display order). Hash
# inputs use protocol order: the bytes reversed (spec section 3).
GENESIS_DISPLAY = '05a60a92d99d85997cce3b87616c089f6124d7342af37106edc76126334a2c38'
GENESIS = bytes.fromhex(GENESIS_DISPLAY)[::-1]

ROW_BYTES = 8192
PRODUCTS = {
    'memo': {'protocol': 'tachyon-memo-pir-v1', 'key': 16, 'entry': 144, 'target': 34},
    'first-contact': {'protocol': 'tachyon-first-contact-pir-v1', 'key': 16, 'entry': 1232, 'target': 1},
    'tachygram': {'protocol': 'tachyon-tachygram-pir-v1', 'key': 32, 'entry': 40, 'target': 124},
    'registry': {'protocol': 'tachyon-registry-pir-v1', 'key': 16, 'entry': 1232, 'target': 1},
}
GEOMETRIES = {'tachyon-4k8': 4096, 'tachyon-2k8': 2048}


def sha(*parts: bytes) -> bytes:
    h = hashlib.sha256()
    for part in parts:
        h.update(part)
    return h.digest()


def domain(product: str) -> bytes:
    return b'tachyon-pir/' + product.encode() + b'/v1/place\0'


def placement_hash(product: str, scope_id: int, seed: int, key: bytes) -> bytes:
    assert len(key) == PRODUCTS[product]['key']
    return sha(domain(product), GENESIS, struct.pack('<Q', scope_id), struct.pack('<I', seed), key)


def bucket_of(h: bytes, n_buckets: int) -> int:
    return struct.unpack('<I', h[4:8])[0] % n_buckets


def row_of(h: bytes, rows: int) -> int:
    return struct.unpack('<I', h[0:4])[0] % rows


def pir_seed(product: str, geometry: str, purpose: str) -> bytes:
    return sha(b'tachyon-pir/v1/native-p8/', product.encode(), b'\0', geometry.encode(), b'\0',
               purpose.encode())


def session_id(protocol: str, scope_id: int, bucket: int, segment: int, rows: int, seed: int,
               rows_sha256: bytes, public_sha256: bytes) -> bytes:
    p = protocol.encode()
    return sha(b'tachyon-pir/v1/session\0', bytes([len(p)]), p,
               struct.pack('<QIIII', scope_id, bucket, segment, rows, seed), rows_sha256, public_sha256)


def n_buckets_for(product: str, rows: int, entries: int) -> int:
    target = PRODUCTS[product]['target']
    n = 1
    while n < 64 and entries > target * n * rows:
        n *= 2
    return n


def stream(label: str, length: int) -> bytes:
    """Deterministic filler bytes."""
    out = b''
    counter = 0
    while len(out) < length:
        out += sha(label.encode(), struct.pack('<I', counter))
        counter += 1
    return out[:length]


def addr_digest(addr: bytes) -> bytes:
    assert len(addr) == 1216
    return sha(b'tachyon-pir/registry/v1/addr\0', addr)


def pow_hash(challenge: bytes, nonce: bytes, digest: bytes) -> bytes:
    return sha(b'tachyon-pir/registry/v1/pow\0', challenge, nonce, digest)


def leading_zero_bits(h: bytes) -> int:
    value = int.from_bytes(h, 'big')
    return 256 - value.bit_length()


def placement_vectors() -> list[dict]:
    cases = [
        ('memo', 0, 0, 1, 'tachyon-4k8'),
        ('memo', 9, 3, 2, 'tachyon-4k8'),
        ('memo', 2**40 + 7, 63, 64, 'tachyon-4k8'),
        ('first-contact', 0, 0, 1, 'tachyon-4k8'),
        ('first-contact', 12, 17, 4, 'tachyon-2k8'),
        ('tachygram', 5, 0, 1, 'tachyon-4k8'),
        ('tachygram', 5, 7, 8, 'tachyon-4k8'),
        ('tachygram', 2**64 - 1, 2**32 - 1, 32, 'tachyon-2k8'),
        ('registry', 0, 0, 1, 'tachyon-4k8'),
        ('registry', 3, 1, 1, 'tachyon-4k8'),
    ]
    out = []
    for i, (product, scope_id, seed, n_buckets, geometry) in enumerate(cases):
        key = stream(f'placement-key-{i}', PRODUCTS[product]['key'])
        h = placement_hash(product, scope_id, seed, key)
        rows = GEOMETRIES[geometry]
        out.append({
            'product': product,
            'scope_id': scope_id,
            'seed': seed,
            'key': key.hex(),
            'hash': h.hex(),
            'n_buckets': n_buckets,
            'bucket': bucket_of(h, n_buckets),
            'geometry': geometry,
            'row': row_of(h, rows),
        })
    return out


def seed_vectors() -> list[dict]:
    return [
        {'product': product, 'geometry': geometry, 'purpose': purpose,
         'seed': pir_seed(product, geometry, purpose).hex()}
        for product in PRODUCTS for geometry in GEOMETRIES
        for purpose in ('query-masks', 'packing-setup')
    ]


def session_vectors() -> list[dict]:
    cases = [
        ('memo', 9, 0, 0, 4096, 0),
        ('first-contact', 3, 1, 2, 4096, 5),
        ('tachygram', 2**33, 63, 7, 4096, 63),
        ('registry', 12, 0, 0, 2048, 1),
    ]
    out = []
    for i, (product, scope_id, bucket, segment, rows, seed) in enumerate(cases):
        rows_sha = sha(b'rows', bytes([i]))
        public_sha = sha(b'public', bytes([i]))
        out.append({
            'product': product, 'scope_id': scope_id, 'bucket': bucket, 'segment': segment,
            'rows': rows, 'seed': seed, 'rows_sha256': rows_sha.hex(),
            'public_sha256': public_sha.hex(),
            'session_id': session_id(PRODUCTS[product]['protocol'], scope_id, bucket, segment, rows,
                                     seed, rows_sha, public_sha).hex(),
        })
    return out


def n_bucket_vectors() -> list[dict]:
    out = []
    for product in PRODUCTS:
        target = PRODUCTS[product]['target']
        for entries in (0, 1, target * 4096, target * 4096 + 1, target * 4096 * 2,
                        target * 4096 * 2 + 1, target * 4096 * 33, target * 4096 * 64,
                        target * 4096 * 64 + 1, 10**9):
            out.append({'product': product, 'geometry': 'tachyon-4k8', 'entries': entries,
                        'n_buckets': n_buckets_for(product, 4096, entries)})
        out.append({'product': product, 'geometry': 'tachyon-2k8', 'entries': target * 2048 + 1,
                    'n_buckets': n_buckets_for(product, 2048, target * 2048 + 1)})
    return out


def grind(product: str, scope_id: int, n_buckets: int, scope_seed: int, seed: int, bucket: int,
          row: int, count: int, label: str, key_of=None):
    """`count` distinct candidates whose key lands in (bucket, row); key_of maps
    a candidate's bytes to its key (identity by default)."""
    found = []
    counter = 0
    while len(found) < count:
        material = stream(f'{label}-{counter}', 1216 if key_of else PRODUCTS[product]['key'])
        counter += 1
        key = key_of(material) if key_of else material
        if bucket_of(placement_hash(product, scope_id, scope_seed, key), n_buckets) != bucket:
            continue
        if row_of(placement_hash(product, scope_id, seed, key), 4096) != row:
            continue
        found.append((key, material))
    return found


def finish_row(entries: list[bytes]) -> bytes:
    row = b''.join(entries)
    assert len(row) <= ROW_BYTES
    return row + bytes(ROW_BYTES - len(row))


def row_vectors() -> list[dict]:
    out = []

    # Memo: two buckets, segment 0 (seed equals the scope seed), one key twice.
    ctx = {'product': 'memo', 'scope_id': 9, 'n_buckets': 2, 'scope_seed': 3, 'seed': 3,
           'bucket': 1, 'row': 1234, 'first_height': None, 'last_height': None}
    keys = sorted(k for k, _ in grind('memo', 9, 2, 3, 3, 1, 1234, 3, 'memo-row'))
    entries = []
    for i, key in enumerate([keys[0], keys[1], keys[1], keys[2]]):
        entries.append({'key': key.hex(), 'ciphertext': stream(f'memo-ct-{i}', 128).hex()})
    row = finish_row([bytes.fromhex(e['key']) + bytes.fromhex(e['ciphertext']) for e in entries])
    out.append({**ctx, 'entries': entries, 'row_sha256': sha(row).hex()})

    # First contact: one bucket, a span scope, segment 0 under seed 0.
    ctx = {'product': 'first-contact', 'scope_id': 0, 'n_buckets': 1, 'scope_seed': 0, 'seed': 0,
           'bucket': 0, 'row': 4095, 'first_height': None, 'last_height': None}
    keys = sorted(k for k, _ in grind('first-contact', 0, 1, 0, 0, 0, 4095, 2, 'fc-row'))
    entries = []
    for i, key in enumerate(keys):
        entries.append({'key': key.hex(), 'kem_ciphertext': stream(f'fc-kem-{i}', 1088).hex(),
                        'ciphertext': stream(f'fc-ct-{i}', 128).hex()})
    row = finish_row([bytes.fromhex(e['key']) + bytes.fromhex(e['kem_ciphertext'])
                      + bytes.fromhex(e['ciphertext']) for e in entries])
    out.append({**ctx, 'entries': entries, 'row_sha256': sha(row).hex()})

    # Tachygram: four buckets, overflow segment 1 (seed 7) of bucket 2 under
    # scope seed 0. Epoch 5 with activation 8 and epoch length 4096.
    first, last = 8 + 5 * 4096, 8 + 6 * 4096 - 1
    ctx = {'product': 'tachygram', 'scope_id': 5, 'n_buckets': 4, 'scope_seed': 0, 'seed': 7,
           'bucket': 2, 'row': 77, 'first_height': first, 'last_height': last}
    keys = sorted(k for k, _ in grind('tachygram', 5, 4, 0, 7, 2, 77, 4, 'tg-row'))
    heights = [first, first + 100, last - 1, last]
    entries = [{'tachygram': key.hex(), 'blk_height': heights[i], 'stamp_ordinal': 3 * i + 1}
               for i, key in enumerate(keys)]
    row = finish_row([bytes.fromhex(e['tachygram']) + struct.pack('<II', e['blk_height'],
                                                                   e['stamp_ordinal'])
                      for e in entries])
    out.append({**ctx, 'entries': entries, 'row_sha256': sha(row).hex()})

    # Registry: chunk 3, one bucket, seed 1; each key is its address digest prefix.
    ctx = {'product': 'registry', 'scope_id': 3, 'n_buckets': 1, 'scope_seed': 1, 'seed': 1,
           'bucket': 0, 'row': 600, 'first_height': None, 'last_height': None}
    found = grind('registry', 3, 1, 1, 1, 0, 600, 2, 'reg-row', key_of=lambda a: addr_digest(a)[:16])
    found.sort()
    entries = [{'key': key.hex(), 'pk': addr[:32].hex(), 'ek': addr[32:].hex()} for key, addr in found]
    row = finish_row([bytes.fromhex(e['key']) + bytes.fromhex(e['pk']) + bytes.fromhex(e['ek'])
                      for e in entries])
    out.append({**ctx, 'entries': entries, 'row_sha256': sha(row).hex()})
    return out


def registry_vectors() -> dict:
    addr = stream('registry-addr', 1216)
    digest = addr_digest(addr)
    challenge = sha(b'registry-challenge')
    nonce = bytes(range(8))
    h = pow_hash(challenge, nonce, digest)
    # The first nonce, counting up from 0 as a little-endian u64, that meets 12 bits.
    difficulty = 12
    counter = 0
    while leading_zero_bits(pow_hash(challenge, struct.pack('<Q', counter), digest)) < difficulty:
        counter += 1
    solved = struct.pack('<Q', counter)
    body = challenge + nonce + addr
    assert len(body) == 1256
    return {
        'addr': addr.hex(),
        'digest': digest.hex(),
        'key16': digest[:16].hex(),
        'challenge': challenge.hex(),
        'nonce': nonce.hex(),
        'pow_hash': h.hex(),
        'pow_leading_zero_bits': leading_zero_bits(h),
        'difficulty_bits': difficulty,
        'solved_nonce': solved.hex(),
        'solved_hash': pow_hash(challenge, solved, digest).hex(),
        'body_sha256': sha(body).hex(),
    }


def main() -> None:
    vectors = {
        'comment': 'Generated by generate.py with Python hashlib; do not edit by hand.',
        'genesis_display': GENESIS_DISPLAY,
        'genesis_protocol': GENESIS.hex(),
        'seeds': seed_vectors(),
        'placement': placement_vectors(),
        'n_buckets': n_bucket_vectors(),
        'sessions': session_vectors(),
        'rows': row_vectors(),
        'registry': registry_vectors(),
    }
    OUT.write_text(json.dumps(vectors, indent=1) + '\n')


if __name__ == '__main__':
    main()
