//! Fixed-size row entries of the four products (spec 6.2, 8.1, 9.1).
//!
//! | Product | Layout | Bytes |
//! |---|---|---:|
//! | memo | `key16 \|\| ciphertext(128)` | 144 |
//! | first-contact | `key16 \|\| kem_ct(1088) \|\| ciphertext(128)` | 1,232 |
//! | tachygram | `tg(32) \|\| blk_height(u32 LE) \|\| stamp_ordinal(u32 LE)` | 40 |
//! | registry | `key16 \|\| pk(32) \|\| ek(1184)` | 1,232 |

use crate::constants::{KEM_CT_BYTES, MEMO_CT_BYTES};
use crate::product::Product;
use crate::registry::Address;

/// A memo entry: `key16 || ciphertext`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoEntry {
    pub key: [u8; 16],
    pub ciphertext: [u8; MEMO_CT_BYTES],
}

/// A first-contact entry: `key16 || kem_ciphertext || ciphertext`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirstContactEntry {
    pub key: [u8; 16],
    pub kem_ciphertext: [u8; KEM_CT_BYTES],
    pub ciphertext: [u8; MEMO_CT_BYTES],
}

/// A tachygram record (spec 8.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TachygramEntry {
    pub tachygram: [u8; 32],
    /// Height of the block containing the creation stamp.
    pub blk_height: u32,
    /// Zero-based ordinal of the stamp among proof stamps in its epoch.
    pub stamp_ordinal: u32,
}

/// A registry entry: `key16 || pk || ek`, where `key16` is the address
/// digest's prefix (spec 9.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryEntry {
    pub key: [u8; 16],
    pub address: Address,
}

impl RegistryEntry {
    /// The entry for `address`, keyed by its digest prefix.
    pub fn new(address: Address) -> Self {
        Self {
            key: address.key16(),
            address,
        }
    }
}

/// One entry of any product. The two 1,232-byte layouts are boxed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    Memo(MemoEntry),
    FirstContact(Box<FirstContactEntry>),
    Tachygram(TachygramEntry),
    Registry(Box<RegistryEntry>),
}

impl Entry {
    /// The product whose layout this entry has.
    pub fn product(&self) -> Product {
        match self {
            Entry::Memo(_) => Product::Memo,
            Entry::FirstContact(_) => Product::FirstContact,
            Entry::Tachygram(_) => Product::Tachygram,
            Entry::Registry(_) => Product::Registry,
        }
    }

    /// The stored key: `key16`, or the full tachygram.
    pub fn key(&self) -> &[u8] {
        match self {
            Entry::Memo(e) => &e.key,
            Entry::FirstContact(e) => &e.key,
            Entry::Tachygram(e) => &e.tachygram,
            Entry::Registry(e) => &e.key,
        }
    }

    /// Writes the entry into `out`, which is exactly `ENTRY_BYTES` long.
    pub fn encode_into(&self, out: &mut [u8]) {
        assert_eq!(out.len(), self.product().entry_bytes(), "entry slot length");
        match self {
            Entry::Memo(e) => {
                out[..16].copy_from_slice(&e.key);
                out[16..].copy_from_slice(&e.ciphertext);
            }
            Entry::FirstContact(e) => {
                out[..16].copy_from_slice(&e.key);
                out[16..16 + KEM_CT_BYTES].copy_from_slice(&e.kem_ciphertext);
                out[16 + KEM_CT_BYTES..].copy_from_slice(&e.ciphertext);
            }
            Entry::Tachygram(e) => {
                out[..32].copy_from_slice(&e.tachygram);
                out[32..36].copy_from_slice(&e.blk_height.to_le_bytes());
                out[36..40].copy_from_slice(&e.stamp_ordinal.to_le_bytes());
            }
            Entry::Registry(e) => {
                out[..16].copy_from_slice(&e.key);
                out[16..].copy_from_slice(&e.address.to_bytes());
            }
        }
    }

    /// The entry's bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![0; self.product().entry_bytes()];
        self.encode_into(&mut out);
        out
    }

    /// Decodes one `ENTRY_BYTES`-long slot of `product`. Layout only: the
    /// row codec checks keys, order and placement. `None` if the length is
    /// wrong.
    pub fn decode(product: Product, bytes: &[u8]) -> Option<Entry> {
        if bytes.len() != product.entry_bytes() {
            return None;
        }
        let key16 = || -> [u8; 16] { bytes[..16].try_into().expect("16 bytes") };
        Some(match product {
            Product::Memo => Entry::Memo(MemoEntry {
                key: key16(),
                ciphertext: bytes[16..].try_into().expect("ciphertext"),
            }),
            Product::FirstContact => Entry::FirstContact(Box::new(FirstContactEntry {
                key: key16(),
                kem_ciphertext: bytes[16..16 + KEM_CT_BYTES].try_into().expect("kem"),
                ciphertext: bytes[16 + KEM_CT_BYTES..].try_into().expect("ciphertext"),
            })),
            Product::Tachygram => Entry::Tachygram(TachygramEntry {
                tachygram: bytes[..32].try_into().expect("tachygram"),
                blk_height: u32::from_le_bytes(bytes[32..36].try_into().expect("u32")),
                stamp_ordinal: u32::from_le_bytes(bytes[36..40].try_into().expect("u32")),
            }),
            Product::Registry => Entry::Registry(Box::new(RegistryEntry {
                key: key16(),
                address: Address::from_bytes(&bytes[16..]).expect("address length"),
            })),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layouts_round_trip_at_spec_offsets() {
        let tg = Entry::Tachygram(TachygramEntry {
            tachygram: [0xab; 32],
            blk_height: 0x0102_0304,
            stamp_ordinal: 0x0a0b_0c0d,
        });
        let bytes = tg.to_bytes();
        assert_eq!(bytes.len(), 40);
        assert_eq!(&bytes[32..], &[4, 3, 2, 1, 0x0d, 0x0c, 0x0b, 0x0a]);
        assert_eq!(Entry::decode(Product::Tachygram, &bytes), Some(tg));

        let memo = Entry::Memo(MemoEntry {
            key: [1; 16],
            ciphertext: [2; 128],
        });
        let bytes = memo.to_bytes();
        assert_eq!((bytes.len(), bytes[15], bytes[16]), (144, 1, 2));
        assert_eq!(Entry::decode(Product::Memo, &bytes), Some(memo));

        let fc = Entry::FirstContact(Box::new(FirstContactEntry {
            key: [1; 16],
            kem_ciphertext: [2; 1088],
            ciphertext: [3; 128],
        }));
        let bytes = fc.to_bytes();
        assert_eq!(
            (bytes.len(), bytes[16], bytes[1103], bytes[1104]),
            (1232, 2, 2, 3)
        );
        assert_eq!(Entry::decode(Product::FirstContact, &bytes), Some(fc));

        let reg = Entry::Registry(Box::new(RegistryEntry::new(Address {
            pk: [4; 32],
            ek: [5; 1184],
        })));
        let bytes = reg.to_bytes();
        assert_eq!(
            (bytes.len(), bytes[16], bytes[47], bytes[48]),
            (1232, 4, 4, 5)
        );
        assert_eq!(Entry::decode(Product::Registry, &bytes), Some(reg));

        assert_eq!(Entry::decode(Product::Memo, &[0; 143]), None);
    }
}
