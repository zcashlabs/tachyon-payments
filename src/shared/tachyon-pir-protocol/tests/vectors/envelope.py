#!/usr/bin/env python3
"""Known-answer vectors for the on-chain envelope (spec section 5, 6.2).

Builds envelope bytes with struct and hashlib straight from the spec
(docs/spec/spec-tachyon-payment-pir.md), sharing no code with the Rust crate:

    envelope    = "TPM1" || entry_count(u8, 1..=16) || entry*
    entry       = kind(u8) || tag(32) || body
    0x01 body   = ciphertext(128)
    0x02 body   = kem_ciphertext(1088) || ciphertext(128)
    0x03 body   = len(u16 LE) || ciphertext(len), len <= 1024

Valid cases list each entry's fields; invalid cases name the error the
parser reports (the first failing check in byte order; see the Rust module
docs). Run from anywhere: `python3 -I envelope.py` rewrites envelope.json
beside this script. The output is deterministic.
"""
from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path

OUT = Path(__file__).resolve().parent / 'envelope.json'

MAGIC = b'TPM1'
MEMO_CT = 128
KEM_CT = 1088
MAX_ENTRIES = 16
MAX_STATE = 1024
FOLLOW_UP, FIRST_CONTACT, WALLET_STATE = 0x01, 0x02, 0x03


def sha(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def stream(label: str, length: int) -> bytes:
    """Deterministic filler bytes."""
    out = b''
    counter = 0
    while len(out) < length:
        out += hashlib.sha256(label.encode() + struct.pack('<I', counter)).digest()
        counter += 1
    return out[:length]


def follow_up(label: str, tag: bytes | None = None) -> dict:
    return {'kind': FOLLOW_UP, 'tag': tag if tag is not None else stream(label + '/tag', 32),
            'ct': stream(label + '/ct', MEMO_CT)}


def first_contact(label: str, tag: bytes | None = None) -> dict:
    return {'kind': FIRST_CONTACT, 'tag': tag if tag is not None else stream(label + '/tag', 32),
            'kem': stream(label + '/kem', KEM_CT), 'ct': stream(label + '/ct', MEMO_CT)}


def wallet_state(label: str, length: int, tag: bytes | None = None) -> dict:
    return {'kind': WALLET_STATE, 'tag': tag if tag is not None else stream(label + '/tag', 32),
            'state': stream(label + '/state', length)}


def entry_bytes(e: dict) -> bytes:
    head = struct.pack('<B', e['kind']) + e['tag']
    if e['kind'] == FOLLOW_UP:
        return head + e['ct']
    if e['kind'] == FIRST_CONTACT:
        return head + e['kem'] + e['ct']
    return head + struct.pack('<H', len(e['state'])) + e['state']


def envelope(entries: list[dict], count: int | None = None) -> bytes:
    n = len(entries) if count is None else count
    return MAGIC + struct.pack('<B', n) + b''.join(entry_bytes(e) for e in entries)


def describe(e: dict) -> dict:
    out = {'kind': e['kind'], 'tag': e['tag'].hex()}
    key16 = e['tag'][:16]
    if e['kind'] == FOLLOW_UP:
        out['ciphertext_sha256'] = sha(e['ct']).hex()
        # The memo-table entry key16 || ciphertext (spec 6.2).
        out['table_entry_sha256'] = sha(key16 + e['ct']).hex()
    elif e['kind'] == FIRST_CONTACT:
        out['kem_ciphertext_sha256'] = sha(e['kem']).hex()
        out['ciphertext_sha256'] = sha(e['ct']).hex()
        # The first-contact-table entry key16 || kem_ct || ciphertext (spec 6.2).
        out['table_entry_sha256'] = sha(key16 + e['kem'] + e['ct']).hex()
    else:
        out['state_len'] = len(e['state'])
        out['state_sha256'] = sha(e['state']).hex()
    return out


def valid_vectors() -> list[dict]:
    cases = [
        ('one follow-up', [follow_up('v1')]),
        ('one first contact', [first_contact('v2')]),
        ('one empty wallet state', [wallet_state('v3', 0)]),
        ('one full wallet state', [wallet_state('v4', MAX_STATE)]),
        # Wallet-state tags are not keys: an all-zero tag is valid.
        ('wallet state with zero tag', [wallet_state('v5', 7, tag=bytes(32))]),
        # key16 is tag[..16]: one nonzero byte there is enough, the rest may be zero.
        ('follow-up key16 nonzero only in byte 15',
         [follow_up('v6', tag=bytes(15) + b'\x01' + bytes(16))]),
        ('first contact key16 nonzero only in byte 0',
         [first_contact('v7', tag=b'\x80' + bytes(31))]),
        ('two follow-ups with one tag', [follow_up('v8a', tag=b'\x11' * 32),
                                         follow_up('v8b', tag=b'\x11' * 32)]),
    ]
    mixed = []
    for i in range(MAX_ENTRIES):
        label = f'mixed/{i}'
        if i % 3 == 0:
            mixed.append(follow_up(label))
        elif i % 3 == 1:
            mixed.append(wallet_state(label, (i * 67) % (MAX_STATE + 1)))
        elif i in (2, 11):
            mixed.append(first_contact(label))
        else:
            mixed.append(follow_up(label))
    cases.append(('sixteen mixed entries', mixed))
    out = []
    for name, entries in cases:
        data = envelope(entries)
        out.append({'name': name, 'hex': data.hex(), 'len': len(data), 'sha256': sha(data).hex(),
                    'entries': [describe(e) for e in entries]})
    return out


def truncated(field: str, index: int | None, offset: int, needed: int, available: int) -> dict:
    return {'type': 'Truncated', 'field': field, 'index': index, 'offset': offset,
            'needed': needed, 'available': available}


def invalid_vectors() -> list[dict]:
    fu = follow_up('i/fu')
    fc = first_contact('i/fc')
    ws = wallet_state('i/ws', 40)
    one_fu = envelope([fu])
    one_fc = envelope([fc])
    one_ws = envelope([ws])
    e0 = 5  # offset of entry 0
    cases = [
        ('empty', b'', truncated('Magic', None, 0, 4, 0)),
        ('three bytes of magic', b'TPM', truncated('Magic', None, 0, 4, 3)),
        ('one non-magic byte', b'\x00', truncated('Magic', None, 0, 4, 1)),
        ('wrong magic', b'TPM2' + one_fu[4:], {'type': 'BadMagic', 'magic': 'TPM2'.encode().hex()}),
        ('lowercase magic', b'tpm1' + one_fu[4:], {'type': 'BadMagic', 'magic': b'tpm1'.hex()}),
        ('magic only', MAGIC, truncated('EntryCount', None, 4, 1, 0)),
        ('zero entries', MAGIC + b'\x00', {'type': 'EntryCount', 'count': 0}),
        ('zero entries with an entry', envelope([fu], count=0), {'type': 'EntryCount', 'count': 0}),
        ('seventeen entries',
         envelope([follow_up(f'i/17/{i}') for i in range(17)]), {'type': 'EntryCount', 'count': 17}),
        ('255 entries', MAGIC + b'\xff', {'type': 'EntryCount', 'count': 255}),
        ('count 2, one entry', envelope([fu], count=2), truncated('Kind', 1, len(one_fu), 1, 0)),
        ('kind 0x00', MAGIC + b'\x01\x00' + fu['tag'] + fu['ct'],
         {'type': 'UnknownKind', 'index': 0, 'kind': 0}),
        ('kind 0x04 at index 1', envelope([fu], count=2) + b'\x04' + fu['tag'] + fu['ct'],
         {'type': 'UnknownKind', 'index': 1, 'kind': 4}),
        ('kind 0xff', MAGIC + b'\x01\xff', {'type': 'UnknownKind', 'index': 0, 'kind': 255}),
        ('tag cut', one_fu[:e0 + 1 + 31], truncated('Tag', 0, e0 + 1, 32, 31)),
        ('follow-up ciphertext cut', one_fu[:-1], truncated('Ciphertext', 0, e0 + 33, MEMO_CT, MEMO_CT - 1)),
        ('kem ciphertext cut', one_fc[:e0 + 33 + 1000],
         truncated('KemCiphertext', 0, e0 + 33, KEM_CT, 1000)),
        ('first-contact ciphertext cut', one_fc[:-5],
         truncated('Ciphertext', 0, e0 + 33 + KEM_CT, MEMO_CT, MEMO_CT - 5)),
        ('state length cut', one_ws[:e0 + 33 + 1], truncated('StateLen', 0, e0 + 33, 2, 1)),
        ('state ciphertext cut', one_ws[:-1], truncated('StateCiphertext', 0, e0 + 35, 40, 39)),
        ('state length 1025', MAGIC + b'\x01\x03' + ws['tag'] + struct.pack('<H', 1025) + bytes(1025),
         {'type': 'StateTooLong', 'index': 0, 'len': 1025}),
        ('state length 0xffff, no body', MAGIC + b'\x01\x03' + ws['tag'] + b'\xff\xff',
         {'type': 'StateTooLong', 'index': 0, 'len': 65535}),
        ('zero tag follow-up', envelope([follow_up('i/z', tag=bytes(32))]),
         {'type': 'ZeroKey', 'index': 0}),
        ('zero key16 follow-up, nonzero tag tail',
         envelope([follow_up('i/z', tag=bytes(16) + b'\xff' * 16)]), {'type': 'ZeroKey', 'index': 0}),
        ('zero key16 first contact at index 2',
         envelope([fu, ws, first_contact('i/z', tag=bytes(16) + b'\x01' * 16)]),
         {'type': 'ZeroKey', 'index': 2}),
        # The key is checked when the tag is read, before the body.
        ('zero key16 follow-up, body missing', MAGIC + b'\x01\x01' + bytes(32),
         {'type': 'ZeroKey', 'index': 0}),
        ('one trailing byte', one_fu + b'\x00', {'type': 'TrailingBytes', 'offset': len(one_fu), 'count': 1}),
        ('a second envelope appended', one_ws + one_fu,
         {'type': 'TrailingBytes', 'offset': len(one_ws), 'count': len(one_fu)}),
    ]
    return [{'name': name, 'hex': data.hex(), 'error': error} for name, data, error in cases]


def main() -> None:
    vectors = {
        'comment': 'Generated by envelope.py with Python struct and hashlib; do not edit by hand.',
        'valid': valid_vectors(),
        'invalid': invalid_vectors(),
    }
    OUT.write_text(json.dumps(vectors, indent=1) + '\n')


if __name__ == '__main__':
    main()
