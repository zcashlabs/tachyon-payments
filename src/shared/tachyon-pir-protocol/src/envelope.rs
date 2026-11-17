//! The on-chain payment envelope carried in `vMemoTachyon` (spec 5,
//! provisional: decision D1).
//!
//! ```text
//! envelope    = magic entry_count entry*
//! magic       = "TPM1"                       4 bytes
//! entry_count = u8                           1..=16
//! entry       = kind(u8) tag(32) body
//! ```
//!
//! | `kind` | Name | `body` | Body bytes |
//! |---|---|---|---:|
//! | `0x01` | follow-up | `ciphertext(128)` | 128 |
//! | `0x02` | first contact | `kem_ciphertext(1088) \|\| ciphertext(128)` | 1,216 |
//! | `0x03` | wallet state | `len(u16 LE) \|\| ciphertext(len)`, `len <= 1024` | 2 + `len` |
//!
//! [`parse`] reads attacker-controlled bytes: it never panics, and it
//! allocates at most one `Vec` of [`MAX_ENVELOPE_ENTRIES`] entries plus each
//! entry's body, only once the bytes for that body are present. Fields are
//! read in byte order and the first failing check is the error:
//!
//! - a field that runs past the end of the input is [`EnvelopeError::Truncated`]
//!   (so the empty input is `Truncated` at the magic, as is any 1-3 byte
//!   input; an empty memo is not an envelope and callers skip it first,
//!   spec 5.2);
//! - a magic other than `TPM1` is [`EnvelopeError::BadMagic`];
//! - an `entry_count` of 0 or over 16 is [`EnvelopeError::EntryCount`];
//! - a `kind` other than 1, 2, 3 is [`EnvelopeError::UnknownKind`], read before
//!   the tag;
//! - a follow-up or first-contact tag whose `key16` (`tag[..16]`) is all zero is
//!   [`EnvelopeError::ZeroKey`] (spec 6.2), checked as soon as the tag is read,
//!   before the body; wallet-state tags are not keys and are not checked;
//! - a wallet-state `len` over [`MAX_STATE_BYTES`] is
//!   [`EnvelopeError::StateTooLong`], checked before its ciphertext is read;
//! - bytes after the last entry are [`EnvelopeError::TrailingBytes`].
//!
//! [`Envelope::encode`] refuses exactly what [`parse`] refuses, so for every
//! `x`, `parse(x)` succeeding implies `encode(parse(x)) == x`, and every
//! successful `encode` parses back to the same envelope.

use crate::constants::{KEM_CT_BYTES, MAX_ENVELOPE_ENTRIES, MAX_STATE_BYTES, MEMO_CT_BYTES};
use crate::entry::{FirstContactEntry, MemoEntry};

/// The envelope magic.
pub const MAGIC: [u8; 4] = *b"TPM1";
/// A follow-up memo entry; goes to the memo database (spec 5.2, 7.1).
pub const KIND_FOLLOW_UP: u8 = 0x01;
/// A first-contact entry; goes to the first-contact database (spec 5.2, 7.2).
pub const KIND_FIRST_CONTACT: u8 = 0x02;
/// A wallet-state entry; counted and otherwise ignored (spec 5.2).
pub const KIND_WALLET_STATE: u8 = 0x03;

/// Bytes of an entry's `kind` and `tag`.
pub const ENTRY_HEADER_BYTES: usize = 1 + 32;
/// Body bytes of a follow-up entry.
pub const FOLLOW_UP_BODY_BYTES: usize = MEMO_CT_BYTES;
/// Body bytes of a first-contact entry.
pub const FIRST_CONTACT_BODY_BYTES: usize = KEM_CT_BYTES + MEMO_CT_BYTES;
/// The largest valid envelope: 16 first-contact entries (19,989 bytes). A
/// longer input never parses.
pub const MAX_ENVELOPE_BYTES: usize =
    MAGIC.len() + 1 + MAX_ENVELOPE_ENTRIES * (ENTRY_HEADER_BYTES + FIRST_CONTACT_BODY_BYTES);

const _: () = assert!(FIRST_CONTACT_BODY_BYTES == 1216);
const _: () = assert!(MAX_ENVELOPE_BYTES == 19_989);
const _: () = assert!(MAX_ENVELOPE_ENTRIES <= u8::MAX as usize);
const _: () = assert!(MAX_STATE_BYTES <= u16::MAX as usize);

/// The field of an envelope that ran past the end of the input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Field {
    Magic,
    EntryCount,
    Kind,
    Tag,
    /// The 128-byte ciphertext of a follow-up or first-contact entry.
    Ciphertext,
    KemCiphertext,
    /// The `u16` length of a wallet-state ciphertext.
    StateLen,
    StateCiphertext,
}

/// An invalid envelope (spec 5.1, 5.2, 6.2). `index` is the zero-based entry
/// index and `offset` a byte offset into the envelope.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    /// The first four bytes are not `TPM1`.
    #[error("bad envelope magic {0:02x?}")]
    BadMagic([u8; 4]),
    /// The input ends inside `field`, which starts at `offset` and needs
    /// `needed` bytes of which `available` remain. `index` is `None` for the
    /// magic and the entry count.
    #[error(
        "envelope truncated in {field:?} of entry {index:?} at offset {offset}: needs {needed} bytes, {available} remain"
    )]
    Truncated {
        field: Field,
        index: Option<usize>,
        offset: usize,
        needed: usize,
        available: usize,
    },
    /// An entry count of 0 or over [`MAX_ENVELOPE_ENTRIES`].
    #[error("envelope entry count {0} is outside 1..=16")]
    EntryCount(usize),
    /// A kind other than 0x01, 0x02 or 0x03.
    #[error("entry {index} has unknown kind {kind:#04x}")]
    UnknownKind { index: usize, kind: u8 },
    /// A wallet-state ciphertext longer than [`MAX_STATE_BYTES`].
    #[error("entry {index} has a wallet-state ciphertext of {len} bytes, over 1024")]
    StateTooLong { index: usize, len: usize },
    /// A follow-up or first-contact entry whose `key16` is all zero.
    #[error("entry {index} has an all-zero key16")]
    ZeroKey { index: usize },
    /// `count` bytes follow the last entry, starting at `offset`.
    #[error("{count} trailing bytes after the last entry at offset {offset}")]
    TrailingBytes { offset: usize, count: usize },
}

/// One entry of an envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvelopeEntry {
    /// Kind 0x01.
    FollowUp {
        tag: [u8; 32],
        ciphertext: Box<[u8; MEMO_CT_BYTES]>,
    },
    /// Kind 0x02.
    FirstContact {
        tag: [u8; 32],
        kem_ciphertext: Box<[u8; KEM_CT_BYTES]>,
        ciphertext: Box<[u8; MEMO_CT_BYTES]>,
    },
    /// Kind 0x03. A valid entry has `ciphertext.len() <= MAX_STATE_BYTES`;
    /// [`Envelope::encode`] refuses a longer one.
    WalletState { tag: [u8; 32], ciphertext: Vec<u8> },
}

impl EnvelopeEntry {
    /// The entry's kind byte.
    pub fn kind(&self) -> u8 {
        match self {
            EnvelopeEntry::FollowUp { .. } => KIND_FOLLOW_UP,
            EnvelopeEntry::FirstContact { .. } => KIND_FIRST_CONTACT,
            EnvelopeEntry::WalletState { .. } => KIND_WALLET_STATE,
        }
    }

    /// The entry's 32-byte tag.
    pub fn tag(&self) -> &[u8; 32] {
        match self {
            EnvelopeEntry::FollowUp { tag, .. }
            | EnvelopeEntry::FirstContact { tag, .. }
            | EnvelopeEntry::WalletState { tag, .. } => tag,
        }
    }

    /// The tag's first 16 bytes: the memo and first-contact table key.
    pub fn key16(&self) -> [u8; 16] {
        key16(self.tag())
    }

    /// The memo-table entry (`key16 = tag[..16]`) for a follow-up entry, else
    /// `None`.
    pub fn memo_entry(&self) -> Option<MemoEntry> {
        match self {
            EnvelopeEntry::FollowUp { tag, ciphertext } => Some(MemoEntry {
                key: key16(tag),
                ciphertext: **ciphertext,
            }),
            _ => None,
        }
    }

    /// The first-contact-table entry (`key16 = tag[..16]`) for a
    /// first-contact entry, else `None`.
    pub fn first_contact_entry(&self) -> Option<FirstContactEntry> {
        match self {
            EnvelopeEntry::FirstContact {
                tag,
                kem_ciphertext,
                ciphertext,
            } => Some(FirstContactEntry {
                key: key16(tag),
                kem_ciphertext: **kem_ciphertext,
                ciphertext: **ciphertext,
            }),
            _ => None,
        }
    }

    /// The entry's encoded length: kind, tag and body.
    pub fn encoded_len(&self) -> usize {
        ENTRY_HEADER_BYTES
            + match self {
                EnvelopeEntry::FollowUp { .. } => FOLLOW_UP_BODY_BYTES,
                EnvelopeEntry::FirstContact { .. } => FIRST_CONTACT_BODY_BYTES,
                EnvelopeEntry::WalletState { ciphertext, .. } => 2 + ciphertext.len(),
            }
    }

    /// The check [`parse`] applies to the entry at `index` once its fields are
    /// read, in the same order.
    fn check(&self, index: usize) -> Result<(), EnvelopeError> {
        match self {
            EnvelopeEntry::FollowUp { tag, .. } | EnvelopeEntry::FirstContact { tag, .. } => {
                check_key(index, tag)
            }
            EnvelopeEntry::WalletState { ciphertext, .. } => {
                check_state_len(index, ciphertext.len())
            }
        }
    }
}

/// A parsed envelope: 1 to 16 entries in envelope order (the entry index of
/// the ordinal, spec 5.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub entries: Vec<EnvelopeEntry>,
}

impl Envelope {
    /// The envelope's bytes. Refuses an envelope [`parse`] would refuse: an
    /// entry count outside 1..=16, an all-zero `key16` on a follow-up or
    /// first-contact entry, a wallet-state ciphertext over 1,024 bytes. The
    /// first failing check in envelope order is the error, as in [`parse`].
    pub fn encode(&self) -> Result<Vec<u8>, EnvelopeError> {
        let count = self.entries.len();
        if !(1..=MAX_ENVELOPE_ENTRIES).contains(&count) {
            return Err(EnvelopeError::EntryCount(count));
        }
        for (index, entry) in self.entries.iter().enumerate() {
            entry.check(index)?;
        }
        let len = MAGIC.len()
            + 1
            + self
                .entries
                .iter()
                .map(EnvelopeEntry::encoded_len)
                .sum::<usize>();
        let mut out = Vec::with_capacity(len);
        out.extend_from_slice(&MAGIC);
        out.push(u8::try_from(count).expect("count checked"));
        for entry in &self.entries {
            out.push(entry.kind());
            out.extend_from_slice(entry.tag());
            match entry {
                EnvelopeEntry::FollowUp { ciphertext, .. } => {
                    out.extend_from_slice(&ciphertext[..])
                }
                EnvelopeEntry::FirstContact {
                    kem_ciphertext,
                    ciphertext,
                    ..
                } => {
                    out.extend_from_slice(&kem_ciphertext[..]);
                    out.extend_from_slice(&ciphertext[..]);
                }
                EnvelopeEntry::WalletState { ciphertext, .. } => {
                    let n = u16::try_from(ciphertext.len()).expect("length checked");
                    out.extend_from_slice(&n.to_le_bytes());
                    out.extend_from_slice(ciphertext);
                }
            }
        }
        debug_assert_eq!(out.len(), len);
        Ok(out)
    }
}

/// Parses a nonempty `vMemoTachyon` as an envelope; see the module docs for
/// the order of checks. The empty input is
/// `Truncated { field: Field::Magic, .. }`.
pub fn parse(bytes: &[u8]) -> Result<Envelope, EnvelopeError> {
    let mut r = Reader { bytes, offset: 0 };
    let magic: [u8; 4] = r.array(Field::Magic, None)?;
    if magic != MAGIC {
        return Err(EnvelopeError::BadMagic(magic));
    }
    let [count] = r.array(Field::EntryCount, None)?;
    let count = usize::from(count);
    if !(1..=MAX_ENVELOPE_ENTRIES).contains(&count) {
        return Err(EnvelopeError::EntryCount(count));
    }
    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let i = Some(index);
        let [kind] = r.array(Field::Kind, i)?;
        if !matches!(
            kind,
            KIND_FOLLOW_UP | KIND_FIRST_CONTACT | KIND_WALLET_STATE
        ) {
            return Err(EnvelopeError::UnknownKind { index, kind });
        }
        let tag: [u8; 32] = r.array(Field::Tag, i)?;
        let entry = match kind {
            KIND_FOLLOW_UP => {
                check_key(index, &tag)?;
                EnvelopeEntry::FollowUp {
                    tag,
                    ciphertext: Box::new(r.array(Field::Ciphertext, i)?),
                }
            }
            KIND_FIRST_CONTACT => {
                check_key(index, &tag)?;
                EnvelopeEntry::FirstContact {
                    tag,
                    kem_ciphertext: Box::new(r.array(Field::KemCiphertext, i)?),
                    ciphertext: Box::new(r.array(Field::Ciphertext, i)?),
                }
            }
            _ => {
                let len = usize::from(u16::from_le_bytes(r.array(Field::StateLen, i)?));
                check_state_len(index, len)?;
                EnvelopeEntry::WalletState {
                    tag,
                    ciphertext: r.take(Field::StateCiphertext, i, len)?.to_vec(),
                }
            }
        };
        entries.push(entry);
    }
    let rest = bytes.len() - r.offset;
    if rest != 0 {
        return Err(EnvelopeError::TrailingBytes {
            offset: r.offset,
            count: rest,
        });
    }
    Ok(Envelope { entries })
}

fn key16(tag: &[u8; 32]) -> [u8; 16] {
    let mut key = [0u8; 16];
    key.copy_from_slice(&tag[..16]);
    key
}

fn check_key(index: usize, tag: &[u8; 32]) -> Result<(), EnvelopeError> {
    if tag[..16].iter().all(|&b| b == 0) {
        Err(EnvelopeError::ZeroKey { index })
    } else {
        Ok(())
    }
}

fn check_state_len(index: usize, len: usize) -> Result<(), EnvelopeError> {
    if len > MAX_STATE_BYTES {
        Err(EnvelopeError::StateTooLong { index, len })
    } else {
        Ok(())
    }
}

/// A cursor that reports a short read as [`EnvelopeError::Truncated`].
struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn take(
        &mut self,
        field: Field,
        index: Option<usize>,
        needed: usize,
    ) -> Result<&'a [u8], EnvelopeError> {
        let available = self.bytes.len() - self.offset;
        if available < needed {
            return Err(EnvelopeError::Truncated {
                field,
                index,
                offset: self.offset,
                needed,
                available,
            });
        }
        let out = &self.bytes[self.offset..self.offset + needed];
        self.offset += needed;
        Ok(out)
    }

    fn array<const N: usize>(
        &mut self,
        field: Field,
        index: Option<usize>,
    ) -> Result<[u8; N], EnvelopeError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(field, index, N)?);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_and_offsets() {
        let env = Envelope {
            entries: vec![
                EnvelopeEntry::FollowUp {
                    tag: [1; 32],
                    ciphertext: Box::new([2; 128]),
                },
                EnvelopeEntry::WalletState {
                    tag: [0; 32],
                    ciphertext: vec![3; 5],
                },
            ],
        };
        let bytes = env.encode().unwrap();
        assert_eq!(bytes.len(), 5 + 33 + 128 + 33 + 2 + 5);
        assert_eq!(&bytes[..5], b"TPM1\x02");
        assert_eq!((bytes[5], bytes[6], bytes[38], bytes[165]), (1, 1, 2, 2));
        assert_eq!((bytes[166], bytes[167], bytes[198]), (3, 0, 0));
        assert_eq!((bytes[199], bytes[200], bytes[201]), (5, 0, 3));
        assert_eq!(parse(&bytes).unwrap(), env);
    }

    #[test]
    fn empty_input_is_truncated_at_the_magic() {
        assert_eq!(
            parse(&[]),
            Err(EnvelopeError::Truncated {
                field: Field::Magic,
                index: None,
                offset: 0,
                needed: 4,
                available: 0,
            })
        );
    }
}
