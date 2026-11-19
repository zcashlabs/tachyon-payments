#!/usr/bin/env python3
"""Known-answer cases for the registry address checks of spec 9.1.

Independent of the Rust implementation: pk is compared as a Python integer
with the Pallas base modulus, and ek is decoded bit by bit as FIPS 203
algorithm 6 (ByteDecode_12) states it, then checked as FIPS 203 section 7.2
states the modulus check: ByteEncode_12(ByteDecode_12(ek[0:1152]) mod q)
must equal ek[0:1152].

Run from this directory to regenerate address_checks.json:

    python3 address_checks.py > address_checks.json
"""
import json
import random

P = 0x40000000000000000000000000000000224698FC094CF91B992D30ED00000001
Q = 3329
POLY_BYTES = 1152


def bits_of(data):
    # FIPS 203 BytesToBits: bit j of byte i is b[8i + j].
    return [(byte >> j) & 1 for byte in data for j in range(8)]


def bits_to_bytes(bits):
    return bytes(sum(bits[8 * i + j] << j for j in range(8)) for i in range(len(bits) // 8))


def byte_decode12(data):
    bits = bits_of(data)
    return [sum(bits[12 * i + j] << j for j in range(12)) for i in range(256 * len(data) // 384)]


def byte_encode12(coefficients):
    bits = [(c >> j) & 1 for c in coefficients for j in range(12)]
    return bits_to_bytes(bits)


def ek_passes(ek):
    poly = ek[:POLY_BYTES]
    return byte_encode12([c % Q for c in byte_decode12(poly)]) == poly


def first_bad(ek):
    for index, c in enumerate(byte_decode12(ek[:POLY_BYTES])):
        if c >= Q:
            return index
    return None


def ek_from(coefficients, rho):
    return byte_encode12(coefficients) + rho


def main():
    rng = random.Random(20261114)
    pks = []
    for name, value in [
        ("zero", 0),
        ("one", 1),
        ("p_minus_1", P - 1),
        ("p", P),
        ("p_plus_1", P + 1),
        ("two_254", 1 << 254),
        ("two_255", 1 << 255),
        ("all_ones", (1 << 256) - 1),
        ("random_below", rng.randrange(P)),
        ("random_above", rng.randrange(P, 1 << 256)),
    ]:
        pk = value.to_bytes(32, "little")
        pks.append({"name": name, "pk": pk.hex(), "canonical": int.from_bytes(pk, "little") < P})

    def valid():
        return [rng.randrange(Q) for _ in range(768)]

    rho = bytes(rng.randrange(256) for _ in range(32))
    cases = [("zero", [0] * 768, bytes(32)), ("random_valid", valid(), rho),
             ("all_q_minus_1", [Q - 1] * 768, bytes([0xFF]) * 32)]
    for index in (0, 1, 383, 767):
        coefficients = valid()
        coefficients[index] = Q
        cases.append((f"q_at_{index}", coefficients, rho))
    coefficients = valid()
    coefficients[rng.randrange(768)] = 0xFFF
    cases.append(("fff_at_random", coefficients, rho))
    coefficients = valid()
    for index in rng.sample(range(768), 5):
        coefficients[index] = rng.randrange(Q, 4096)
    cases.append(("several_random_above_q", coefficients, rho))
    eks = []
    for name, coefficients, seed in cases:
        ek = ek_from(coefficients, seed)
        assert len(ek) == 1184
        assert byte_decode12(ek[:POLY_BYTES]) == coefficients
        eks.append({"name": name, "ek": ek.hex(), "passes": ek_passes(ek), "first_bad": first_bad(ek)})
    print(json.dumps({"pk": pks, "ek": eks}, indent=1))


if __name__ == "__main__":
    main()
