//! The independent oracle of the registry end-to-end tests (spec 13.1:
//! "oracles are independent").
//!
//! The registry has no chain: its source is the registrations the test itself
//! sends to the publisher's write API. The oracle is the test's own record of
//! them: every address it registered (the `pk` and `ek` bytes it generated),
//! the address digest computed here with `sha2` straight from spec 9.1, and
//! the sequence and chunk the publisher acknowledged. Expected chunk
//! structure and lookup answers follow from that record and the spec's
//! arithmetic alone. This module shares no code with the protocol crate, the
//! registry store, the table builder or the publisher: it depends on none of
//! `tachyon-pir-protocol`, `tachyon-pir-table`, `registry-store` or
//! `registry-publisher`.
//!
//! Restated from the spec in this file:
//!
//! - the address and its digest, `SHA-256("tachyon-pir/registry/v1/addr\0" ||
//!   pk || ek)`, and `key16 = digest[0..16]` (spec 9.1);
//! - `pk` canonical: its little-endian integer below the Pallas base modulus
//!   `p` (spec 9.1);
//! - the `ek` modulus check: every 12-bit coefficient of `ByteEncode12(t̂)`,
//!   read little-endian in order, below `q = 3,329` (spec 9.1, FIPS 203 7.2);
//! - chunks: `chunk = sequence / 3,072`; a map at `log_sequence` lists sealed
//!   chunks `0 .. log_sequence / 3,072`, each exactly 3,072 entries, and one
//!   open chunk `log_sequence / 3,072` holding `log_sequence % 3,072` (spec
//!   9.2, 10.3);
//! - lookups: a key is present in chunk `c` of a map at `log_sequence` exactly
//!   when an address with that `key16` was acknowledged with chunk `c` and a
//!   sequence below `log_sequence` (spec 9.3 "visible_after_sequence").
//!
//! The acknowledgement itself is checked against the record: the digest is
//! the oracle's, the chunk is the sequence's, a repeated address keeps its
//! sequence with `created: false`, and the sequences acknowledged for
//! distinct addresses are distinct (and, once a batch is complete, exactly
//! `0 .. n`, [`RegistryOracle::assert_contiguous`]).
use crate::oracle::State;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// `REGISTRY_CHUNK_ENTRIES` (spec 9.2, 15).
pub const REGISTRY_CHUNK_ENTRIES: u64 = 3072;
/// The address digest domain (spec 9.1).
pub const ADDR_DOMAIN: &[u8] = b"tachyon-pir/registry/v1/addr\0";
/// `pk` bytes (spec 9.1).
pub const PK_BYTES: usize = 32;
/// ML-KEM-768 encapsulation key bytes, `ByteEncode12(t̂) || ρ` (spec 9.1).
pub const EK_BYTES: usize = 1184;
/// The `ByteEncode12(t̂)` part of `ek`: 768 coefficients of 12 bits.
pub const EK_T_BYTES: usize = 1152;
/// Coefficients in `t̂` (k = 3 polynomials of 256).
pub const EK_COEFFICIENTS: usize = 768;
/// `addr = pk || ek` (spec 9.1).
pub const ADDR_BYTES: usize = PK_BYTES + EK_BYTES;
/// The registration body, `challenge(32) || nonce(8) || addr(1216)` (spec 9.3).
pub const REGISTER_BODY_BYTES: usize = 32 + 8 + ADDR_BYTES;
/// The ML-KEM modulus `q` (FIPS 203).
pub const ML_KEM_Q: u16 = 3329;
/// The Pallas base field modulus `p` (spec 9.1), big-endian hex as the spec
/// prints it.
pub const PALLAS_P_HEX: &str = "40000000000000000000000000000000224698fc094cf91b992d30ed00000001";
/// `REGISTRY_CHALLENGE_SECONDS` (spec 9.3, 15).
pub const REGISTRY_CHALLENGE_SECONDS: u64 = 600;

/// `p` as 32 little-endian bytes, the `pk` encoding (spec 9.1).
pub fn pallas_p_le() -> [u8; 32] {
    let mut bytes: [u8; 32] = hex::decode(PALLAS_P_HEX)
        .expect("hex")
        .try_into()
        .expect("32 bytes");
    bytes.reverse();
    bytes
}

/// Whether `pk`'s little-endian integer is below `p` (spec 9.1).
pub fn pk_canonical(pk: &[u8; PK_BYTES]) -> bool {
    let p = pallas_p_le();
    // Compare from the most significant byte down.
    for i in (0..PK_BYTES).rev() {
        if pk[i] != p[i] {
            return pk[i] < p[i];
        }
    }
    false // equal to p
}

/// The 768 coefficients of `ek[0..1152]`: each 3-byte group `b0 b1 b2` holds
/// `c0 = b0 | (b1 & 0x0f) << 8` and `c1 = b1 >> 4 | b2 << 4` (12 bits
/// little-endian in order, FIPS 203 `ByteDecode12` before the reduction).
pub fn ek_coefficients(ek: &[u8]) -> Vec<u16> {
    assert!(ek.len() >= EK_T_BYTES);
    ek[..EK_T_BYTES]
        .chunks_exact(3)
        .flat_map(|g| {
            let (b0, b1, b2) = (u16::from(g[0]), u16::from(g[1]), u16::from(g[2]));
            [b0 | ((b1 & 0x0f) << 8), (b1 >> 4) | (b2 << 4)]
        })
        .collect()
}

/// `ByteEncode12` of 768 coefficients, each below 4,096.
pub fn encode12(coefficients: &[u16]) -> [u8; EK_T_BYTES] {
    assert_eq!(coefficients.len(), EK_COEFFICIENTS);
    let mut out = [0u8; EK_T_BYTES];
    for (pair, group) in coefficients.chunks_exact(2).zip(out.chunks_exact_mut(3)) {
        let (c0, c1) = (pair[0], pair[1]);
        assert!(c0 < 4096 && c1 < 4096, "12-bit coefficients");
        group[0] = (c0 & 0xff) as u8;
        group[1] = ((c0 >> 8) | ((c1 & 0x0f) << 4)) as u8;
        group[2] = (c1 >> 4) as u8;
    }
    out
}

/// The FIPS 203 7.2 modulus check: every coefficient below `q`.
pub fn ek_modulus_ok(ek: &[u8]) -> bool {
    ek_coefficients(ek).iter().all(|c| *c < ML_KEM_Q)
}

/// One registry address, `pk(32) || ek(1184)`, as the test generated it.
#[derive(Clone, PartialEq, Eq)]
pub struct OracleAddress {
    pub pk: [u8; PK_BYTES],
    pub ek: Box<[u8; EK_BYTES]>,
}

impl std::fmt::Debug for OracleAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OracleAddress(digest {})", hex::encode(self.digest()))
    }
}

impl OracleAddress {
    /// `pk || ek`.
    pub fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(ADDR_BYTES);
        out.extend_from_slice(&self.pk);
        out.extend_from_slice(&self.ek[..]);
        out
    }

    /// `SHA-256("tachyon-pir/registry/v1/addr\0" || pk || ek)` (spec 9.1).
    pub fn digest(&self) -> [u8; 32] {
        Sha256::new()
            .chain_update(ADDR_DOMAIN)
            .chain_update(self.pk)
            .chain_update(&self.ek[..])
            .finalize()
            .into()
    }

    /// `digest[0..16]`, the lookup key.
    pub fn key16(&self) -> [u8; 16] {
        self.digest()[..16].try_into().expect("16 bytes")
    }

    /// Whether the publisher must accept it (spec 9.1, 9.3): `pk` canonical,
    /// the `ek` modulus check, `key16` not all zero.
    pub fn valid(&self) -> bool {
        pk_canonical(&self.pk) && ek_modulus_ok(&self.ek[..]) && self.key16() != [0; 16]
    }
}

/// A deterministic byte stream, `SHA-256(domain || seed || counter)` blocks.
#[derive(Clone, Debug)]
pub struct Stream {
    seed: [u8; 32],
    counter: u64,
    buffer: Vec<u8>,
}

impl Stream {
    pub fn new(label: &str, seed: u64) -> Self {
        Self {
            seed: Sha256::new()
                .chain_update(b"tachyon-e2e/registry-oracle\0")
                .chain_update(label.as_bytes())
                .chain_update([0])
                .chain_update(seed.to_le_bytes())
                .finalize()
                .into(),
            counter: 0,
            buffer: Vec::new(),
        }
    }

    pub fn fill(&mut self, out: &mut [u8]) {
        for byte in out {
            if self.buffer.is_empty() {
                let block: [u8; 32] = Sha256::new()
                    .chain_update(self.seed)
                    .chain_update(self.counter.to_le_bytes())
                    .finalize()
                    .into();
                self.counter += 1;
                self.buffer = block.iter().rev().copied().collect();
            }
            *byte = self.buffer.pop().expect("refilled");
        }
    }

    pub fn u16(&mut self) -> u16 {
        let mut b = [0u8; 2];
        self.fill(&mut b);
        u16::from_le_bytes(b)
    }
}

/// Generates valid addresses (and invalid variants) from a seed.
#[derive(Clone, Debug)]
pub struct AddressGen {
    stream: Stream,
}

impl AddressGen {
    pub fn new(seed: u64) -> Self {
        Self {
            stream: Stream::new("addresses", seed),
        }
    }

    /// A uniformly random `pk` below `2^254`, which is below `p` (`p > 2^254`).
    pub fn pk(&mut self) -> [u8; PK_BYTES] {
        let mut pk = [0u8; PK_BYTES];
        self.stream.fill(&mut pk);
        pk[31] &= 0x3f;
        debug_assert!(pk_canonical(&pk));
        pk
    }

    /// An `ek` whose 768 coefficients are uniform in `0..q`, with a random
    /// `ρ`.
    pub fn ek(&mut self) -> Box<[u8; EK_BYTES]> {
        let coefficients: Vec<u16> = (0..EK_COEFFICIENTS)
            .map(|_| {
                loop {
                    let c = self.stream.u16() & 0x0fff;
                    if c < ML_KEM_Q {
                        break c;
                    }
                }
            })
            .collect();
        let mut ek = Box::new([0u8; EK_BYTES]);
        ek[..EK_T_BYTES].copy_from_slice(&encode12(&coefficients));
        self.stream.fill(&mut ek[EK_T_BYTES..]);
        ek
    }

    /// A valid address.
    pub fn address(&mut self) -> OracleAddress {
        let address = OracleAddress {
            pk: self.pk(),
            ek: self.ek(),
        };
        assert!(address.valid(), "generated addresses are valid");
        address
    }

    /// `n` valid addresses.
    pub fn addresses(&mut self, n: usize) -> Vec<OracleAddress> {
        (0..n).map(|_| self.address()).collect()
    }
}

/// Invalid variants of a valid address, each failing exactly one check of
/// spec 9.1.
pub mod invalid {
    use super::*;

    /// `pk = p`: the smallest non-canonical value.
    pub fn pk_equal_p(base: &OracleAddress) -> OracleAddress {
        let address = OracleAddress {
            pk: pallas_p_le(),
            ek: base.ek.clone(),
        };
        assert!(!pk_canonical(&address.pk) && ek_modulus_ok(&address.ek[..]));
        address
    }

    /// `pk = 2^256 - 1`.
    pub fn pk_all_ones(base: &OracleAddress) -> OracleAddress {
        OracleAddress {
            pk: [0xff; PK_BYTES],
            ek: base.ek.clone(),
        }
    }

    /// `pk = p - 1`: canonical (the largest canonical value), for the
    /// boundary's other side.
    pub fn pk_p_minus_one(base: &OracleAddress) -> OracleAddress {
        let mut pk = pallas_p_le();
        pk[0] -= 1; // p ends in 0x01
        assert!(pk_canonical(&pk));
        OracleAddress {
            pk,
            ek: base.ek.clone(),
        }
    }

    /// `base` with coefficient `index` of `t̂` set to `value` (12 bits).
    pub fn ek_coefficient(base: &OracleAddress, index: usize, value: u16) -> OracleAddress {
        let mut coefficients = ek_coefficients(&base.ek[..]);
        coefficients[index] = value;
        let mut ek = base.ek.clone();
        ek[..EK_T_BYTES].copy_from_slice(&encode12(&coefficients));
        OracleAddress { pk: base.pk, ek }
    }
}

/// What the publisher acknowledged for one registration (the `200` body of
/// spec 9.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ack {
    pub chunk_id: u64,
    pub digest: [u8; 32],
    pub created: bool,
    /// The entry's log sequence.
    pub visible_after_sequence: u64,
}

/// One registered address in the oracle's record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registered {
    pub address: OracleAddress,
    pub digest: [u8; 32],
    pub key16: [u8; 16],
    pub sequence: u64,
    pub chunk: u64,
}

/// One chunk a registry map at some `log_sequence` must list (spec 10.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpectedChunk {
    /// The scope index, `chunk_id`, which is also the `scope_id`.
    pub index: u64,
    pub state: State,
    pub entries: u64,
    /// The first log sequence of the chunk.
    pub first_sequence: u64,
}

/// The test's record of what it registered.
#[derive(Clone, Debug, Default)]
pub struct RegistryOracle {
    entries: Vec<Registered>,
    by_digest: HashMap<[u8; 32], usize>,
    by_key16: HashMap<[u8; 16], usize>,
    by_sequence: HashMap<u64, usize>,
}

impl RegistryOracle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the acknowledgement of `address`, checking it: the digest is
    /// the oracle's, the chunk is `sequence / 3,072`, a new address is
    /// `created` at a sequence no other address holds, and a repeated one is
    /// not created and keeps its sequence and chunk (spec 9.3 idempotence).
    pub fn record(&mut self, address: &OracleAddress, ack: Ack) -> &Registered {
        let digest = address.digest();
        assert_eq!(
            hex::encode(ack.digest),
            hex::encode(digest),
            "the acknowledged digest is the address's (spec 9.1)"
        );
        assert_eq!(
            ack.chunk_id,
            ack.visible_after_sequence / REGISTRY_CHUNK_ENTRIES,
            "chunk = sequence / 3072 (spec 9.4)"
        );
        if let Some(&i) = self.by_digest.get(&digest) {
            let known = &self.entries[i];
            assert!(!ack.created, "a repeated address is not created again");
            assert_eq!(
                (ack.visible_after_sequence, ack.chunk_id),
                (known.sequence, known.chunk),
                "a repeated address keeps its sequence and chunk"
            );
            return &self.entries[i];
        }
        assert!(ack.created, "a new address is created: {ack:?}");
        assert!(
            !self.by_sequence.contains_key(&ack.visible_after_sequence),
            "sequence {} acknowledged for two addresses",
            ack.visible_after_sequence
        );
        let i = self.entries.len();
        let key16 = address.key16();
        self.entries.push(Registered {
            address: address.clone(),
            digest,
            key16,
            sequence: ack.visible_after_sequence,
            chunk: ack.chunk_id,
        });
        self.by_digest.insert(digest, i);
        self.by_key16.insert(key16, i);
        self.by_sequence.insert(ack.visible_after_sequence, i);
        &self.entries[i]
    }

    /// The sequences acknowledged so far are exactly `0 .. len`: the log has
    /// no gap (spec 9.4), so the oracle knows the log's whole content.
    pub fn assert_contiguous(&self) {
        let n = self.entries.len() as u64;
        for s in 0..n {
            assert!(
                self.by_sequence.contains_key(&s),
                "sequence {s} of 0..{n} was never acknowledged"
            );
        }
    }

    /// Registered addresses.
    pub fn len(&self) -> u64 {
        self.entries.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Every registered address, in the order the test recorded them.
    pub fn entries(&self) -> &[Registered] {
        &self.entries
    }

    /// The record of `address`, if registered.
    pub fn get(&self, address: &OracleAddress) -> Option<&Registered> {
        self.by_digest
            .get(&address.digest())
            .map(|&i| &self.entries[i])
    }

    /// The entry at `sequence`.
    pub fn at(&self, sequence: u64) -> Option<&Registered> {
        self.by_sequence.get(&sequence).map(|&i| &self.entries[i])
    }

    /// The chunks a map at `log_sequence` lists, by index: sealed chunks of
    /// exactly 3,072 entries, then one open chunk (spec 10.3).
    pub fn chunks(&self, log_sequence: u64) -> Vec<ExpectedChunk> {
        let open = log_sequence / REGISTRY_CHUNK_ENTRIES;
        (0..=open)
            .map(|index| ExpectedChunk {
                index,
                state: if index < open {
                    State::Sealed
                } else {
                    State::Open
                },
                entries: if index < open {
                    REGISTRY_CHUNK_ENTRIES
                } else {
                    log_sequence % REGISTRY_CHUNK_ENTRIES
                },
                first_sequence: index * REGISTRY_CHUNK_ENTRIES,
            })
            .collect()
    }

    /// The entries of `chunk` in a map at `log_sequence`, in sequence order.
    pub fn in_chunk(&self, chunk: u64, log_sequence: u64) -> Vec<&Registered> {
        let first = chunk * REGISTRY_CHUNK_ENTRIES;
        let end = (first + REGISTRY_CHUNK_ENTRIES).min(log_sequence);
        (first..end)
            .map(|s| {
                self.at(s)
                    .unwrap_or_else(|| panic!("sequence {s} is not in the oracle"))
            })
            .collect()
    }

    /// The address a lookup of `key16` in chunk `chunk` of a map at
    /// `log_sequence` must return; `None` means absent.
    pub fn expected(&self, chunk: u64, key16: &[u8; 16], log_sequence: u64) -> Option<&Registered> {
        let entry = &self.entries[*self.by_key16.get(key16)?];
        (entry.chunk == chunk && entry.sequence < log_sequence).then_some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pk_boundary() {
        let p = pallas_p_le();
        assert_eq!(p[0], 1);
        assert_eq!(p[31], 0x40);
        assert!(!pk_canonical(&p));
        let mut below = p;
        below[0] = 0;
        assert!(pk_canonical(&below));
        assert!(pk_canonical(&[0; 32]));
        assert!(!pk_canonical(&[0xff; 32]));
    }

    #[test]
    fn encode12_round_trips() {
        let mut generator = AddressGen::new(1);
        let address = generator.address();
        let coefficients = ek_coefficients(&address.ek[..]);
        assert_eq!(coefficients.len(), EK_COEFFICIENTS);
        assert_eq!(encode12(&coefficients)[..], address.ek[..EK_T_BYTES]);
        let bad = invalid::ek_coefficient(&address, 767, ML_KEM_Q);
        assert!(!ek_modulus_ok(&bad.ek[..]));
        let edge = invalid::ek_coefficient(&address, 0, ML_KEM_Q - 1);
        assert!(ek_modulus_ok(&edge.ek[..]));
    }

    #[test]
    fn chunk_listing() {
        let oracle = RegistryOracle::new();
        let chunks = oracle.chunks(3072);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].state, State::Sealed);
        assert_eq!(chunks[0].entries, 3072);
        assert_eq!(chunks[1].state, State::Open);
        assert_eq!(chunks[1].entries, 0);
        assert_eq!(oracle.chunks(0).len(), 1);
    }
}
