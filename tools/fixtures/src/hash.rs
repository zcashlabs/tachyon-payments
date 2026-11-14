//! Hashing and byte-order helpers.
//!
//! Block hashes and transaction ids are held in protocol order and printed in
//! display order (reversed hex), as the node's RPC prints them (spec section 3,
//! "Byte order of 32-byte values"). Tachygrams, anchors and every other 32-byte
//! value are printed left to right, unreversed.
use sha2::{Digest, Sha256};

/// A 32-byte value.
pub type Hash32 = [u8; 32];

/// The display-order hex of a protocol-order block hash or transaction id.
pub fn display(hash: &Hash32) -> String {
    let mut bytes = *hash;
    bytes.reverse();
    hex::encode(bytes)
}

/// The protocol-order bytes of a display-order hex hash, or `None` if `displayed`
/// is not 64 hex digits.
pub fn parse_display(displayed: &str) -> Option<Hash32> {
    let mut bytes: Hash32 = hex::decode(displayed).ok()?.try_into().ok()?;
    bytes.reverse();
    Some(bytes)
}

/// SHA-256 of the concatenation of `parts`.
pub fn sha256(parts: &[&[u8]]) -> Hash32 {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

/// Double SHA-256 of the concatenation of `parts`, as block and transaction hashes use.
pub fn sha256d(parts: &[&[u8]]) -> Hash32 {
    sha256(&[&sha256(parts)])
}

/// The Bitcoin-style Merkle root of `leaves` (protocol order), the last leaf of an
/// odd level paired with itself; all zeros for no leaves.
pub fn merkle_root(leaves: &[Hash32]) -> Hash32 {
    if leaves.is_empty() {
        return [0; 32];
    }
    let mut level = leaves.to_vec();
    while level.len() > 1 {
        level = level
            .chunks(2)
            .map(|pair| {
                let right = pair.get(1).unwrap_or(&pair[0]);
                sha256d(&[&pair[0], right])
            })
            .collect();
    }
    level[0]
}

/// Base58Check of `payload`, as transparent addresses are encoded.
pub fn base58check(payload: &[u8]) -> String {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let checksum = sha256d(&[payload]);
    let mut data = payload.to_vec();
    data.extend_from_slice(&checksum[..4]);
    let zeros = data.iter().take_while(|byte| **byte == 0).count();
    // Base-58 digits, least significant first.
    let mut digits: Vec<u8> = Vec::new();
    for byte in &data {
        let mut carry = u32::from(*byte);
        for digit in digits.iter_mut() {
            carry += u32::from(*digit) << 8;
            *digit = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut out = String::with_capacity(zeros + digits.len());
    out.extend(std::iter::repeat_n('1', zeros));
    out.extend(
        digits
            .iter()
            .rev()
            .map(|digit| ALPHABET[*digit as usize] as char),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_reverses_protocol_order() {
        let mut hash = [0u8; 32];
        hash[0] = 0xab;
        hash[31] = 0x01;
        let shown = display(&hash);
        assert!(shown.starts_with("01") && shown.ends_with("ab"));
        assert_eq!(parse_display(&shown), Some(hash));
        assert_eq!(parse_display("zz"), None);
        assert_eq!(parse_display(&shown[2..]), None);
    }

    #[test]
    fn base58check_matches_a_known_testnet_address() {
        // The coinbase output of the saved testnet block 39,000.
        let mut payload = vec![0x1d, 0x25];
        payload.extend(hex::decode("f8234adbf175c19064e3c49d91c7b138c1ee130e").unwrap());
        assert_eq!(base58check(&payload), "tmYLPBuVJnUzQ3BShCDExSijwXxjyM7V3qA");
    }

    #[test]
    fn merkle_root_of_one_leaf_is_the_leaf() {
        let leaf = [7u8; 32];
        assert_eq!(merkle_root(&[leaf]), leaf);
        assert_ne!(merkle_root(&[leaf, leaf]), leaf);
        assert_eq!(merkle_root(&[]), [0; 32]);
    }
}
