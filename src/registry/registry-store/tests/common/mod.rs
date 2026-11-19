//! Shared helpers: a binding and deterministic, valid-looking addresses.
#![allow(dead_code)]

use registry_store::{Binding, STORE_FILE, Store};
use std::path::{Path, PathBuf};
use tachyon_pir_protocol::BlockHash;
use tachyon_pir_protocol::constants::{KEM_EK_BYTES, PK_BYTES};
use tachyon_pir_protocol::registry::Address;

pub fn binding() -> Binding {
    Binding {
        chain: "test".to_owned(),
        genesis_hash: BlockHash::from_protocol_bytes([0x5a; 32]),
    }
}

pub fn store_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join(STORE_FILE)
}

pub fn open(path: &Path) -> Store {
    Store::open(path, binding()).unwrap()
}

/// SplitMix64.
fn next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn fill(state: &mut u64, out: &mut [u8]) {
    for chunk in out.chunks_mut(8) {
        let word = next(state).to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
}

/// Address `index` of family `seed`: random bytes, with `pk`'s top byte
/// masked below the Pallas modulus' (`0x40`) so it is canonical. The store
/// validates neither key, so `ek` is left random.
pub fn address(seed: u64, index: u64) -> Address {
    let mut state = seed.wrapping_mul(0x1000_0000_01b3) ^ index;
    let mut pk = [0u8; PK_BYTES];
    let mut ek = [0u8; KEM_EK_BYTES];
    fill(&mut state, &mut pk);
    pk[PK_BYTES - 1] &= 0x3f;
    fill(&mut state, &mut ek);
    Address { pk, ek }
}

pub fn addresses(seed: u64, range: std::ops::Range<u64>) -> Vec<Address> {
    range.map(|index| address(seed, index)).collect()
}
