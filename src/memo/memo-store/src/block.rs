//! A block's envelope content, ready to append (spec sections 5.1, 5.2, 7.1
//! and 7.2).
use std::fmt;
use tachyon_chain::{BlockId, EpochSchedule, Hash, TachyonBlock};
use tachyon_pir_protocol::constants::{KEM_CT_BYTES, MEMO_CT_BYTES};
use tachyon_pir_protocol::envelope::{self, EnvelopeEntry, EnvelopeError};
use tachyon_pir_protocol::{FirstContactEntry, MemoEntry};

/// An entry's ordinal, `(height, tx index in block, entry index)` (spec 5.2):
/// the deterministic order of entries wherever they are sorted. The derived
/// order is that lexicographic order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ordinal {
    /// The height of the block carrying the entry.
    pub height: u32,
    /// The carrying transaction's index among all transactions of the block,
    /// coinbase included (spec 11.1).
    pub tx_index: u32,
    /// The entry's zero-based index in its envelope.
    pub entry_index: u8,
}

impl fmt::Display for Ordinal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}, {}, {})",
            self.height, self.tx_index, self.entry_index
        )
    }
}

/// The two stored entry kinds: follow-up entries (kind `0x01`) go to the memo
/// tables, first-contact entries (kind `0x02`) to the first-contact tables.
/// Wallet-state entries (kind `0x03`) are counted and not stored (spec 5.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    Memo,
    FirstContact,
}

/// One follow-up entry (kind `0x01`), as the journal stores it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoRecord {
    pub ordinal: Ordinal,
    /// The full 32-byte tag; the table key is its first 16 bytes.
    pub tag: [u8; 32],
    pub ciphertext: [u8; MEMO_CT_BYTES],
}

impl MemoRecord {
    /// The table entry `key16 || ciphertext` (spec 6.2).
    pub fn entry(&self) -> MemoEntry {
        MemoEntry {
            key: key16(&self.tag),
            ciphertext: self.ciphertext,
        }
    }
}

/// One first-contact entry (kind `0x02`), as the journal stores it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirstContactRecord {
    pub ordinal: Ordinal,
    /// The full 32-byte tag; the table key is its first 16 bytes.
    pub tag: [u8; 32],
    pub kem_ciphertext: Box<[u8; KEM_CT_BYTES]>,
    pub ciphertext: [u8; MEMO_CT_BYTES],
}

impl FirstContactRecord {
    /// The table entry `key16 || kem_ciphertext || ciphertext` (spec 6.2).
    pub fn entry(&self) -> FirstContactEntry {
        FirstContactEntry {
            key: key16(&self.tag),
            kem_ciphertext: *self.kem_ciphertext,
            ciphertext: self.ciphertext,
        }
    }
}

/// A non-empty memo that is not a valid envelope. It is skipped whole and
/// counted (spec 5.2); the store keeps only the count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidEnvelope {
    pub tx_index: u32,
    pub txid: Hash,
    pub error: EnvelopeError,
}

/// Envelope and entry counts of a block, an epoch or the whole journal: what
/// the memo metrics of spec 11.8 report.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// Non-empty memos that parsed as envelopes.
    pub envelopes_valid: u64,
    /// Non-empty memos that did not (`envelopes_invalid_total`). Empty memos
    /// are neither.
    pub envelopes_invalid: u64,
    /// Follow-up entries (kind `0x01`) of valid envelopes.
    pub memo_entries: u64,
    /// First-contact entries (kind `0x02`) of valid envelopes.
    pub first_contact_entries: u64,
    /// Wallet-state entries (kind `0x03`) of valid envelopes
    /// (`state_entries_total`).
    pub state_entries: u64,
}

/// A block as the journal stores it: its id, parent and epoch, the entries of
/// its valid envelopes with their ordinals, and its envelope counts.
///
/// Build it with [`IndexedBlock::new`]. The fields are public so that a caller
/// (or a test) can hand-build one, so [`crate::Store::append`] checks them
/// again: the epoch against the store's schedule, every ordinal and every key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedBlock {
    pub id: BlockId,
    /// The parent block's hash, protocol order.
    pub prev: Hash,
    /// The block's epoch, `None` below the activation height.
    pub epoch: Option<u32>,
    /// Follow-up entries in ordinal order.
    pub memo: Vec<MemoRecord>,
    /// First-contact entries in ordinal order.
    pub first_contact: Vec<FirstContactRecord>,
    /// Non-empty memos that parsed.
    pub envelopes_valid: u32,
    /// Non-empty memos that did not parse, in transaction order.
    pub invalid: Vec<InvalidEnvelope>,
    /// Wallet-state entries of the valid envelopes.
    pub state_entries: u32,
}

impl IndexedBlock {
    /// The envelope content of `block` on a network with `schedule`.
    ///
    /// For each Tachyon transaction in block order: an empty memo is skipped;
    /// a memo that does not parse is recorded in [`IndexedBlock::invalid`] and
    /// contributes nothing else; a valid envelope contributes each follow-up
    /// and first-contact entry with ordinal `(height, tx_index, entry index)`,
    /// and counts its wallet-state entries (spec 5.2). Nothing here fails: the
    /// memo is attacker-controlled data.
    pub fn new(block: &TachyonBlock, schedule: &EpochSchedule) -> Self {
        let height = block.id.height;
        let mut indexed = Self {
            id: block.id,
            prev: block.prev,
            epoch: schedule.epoch_of(height),
            memo: Vec::new(),
            first_contact: Vec::new(),
            envelopes_valid: 0,
            invalid: Vec::new(),
            state_entries: 0,
        };
        for tx in &block.txs {
            if tx.memo.is_empty() {
                continue;
            }
            let parsed = match envelope::parse(&tx.memo) {
                Ok(parsed) => parsed,
                Err(error) => {
                    indexed.invalid.push(InvalidEnvelope {
                        tx_index: tx.tx_index,
                        txid: tx.txid,
                        error,
                    });
                    continue;
                }
            };
            indexed.envelopes_valid += 1;
            for (index, entry) in parsed.entries.into_iter().enumerate() {
                let ordinal = Ordinal {
                    height,
                    tx_index: tx.tx_index,
                    // At most MAX_ENVELOPE_ENTRIES (16) entries parse.
                    entry_index: u8::try_from(index).expect("at most 16 entries"),
                };
                match entry {
                    EnvelopeEntry::FollowUp { tag, ciphertext } => {
                        indexed.memo.push(MemoRecord {
                            ordinal,
                            tag,
                            ciphertext: *ciphertext,
                        });
                    }
                    EnvelopeEntry::FirstContact {
                        tag,
                        kem_ciphertext,
                        ciphertext,
                    } => indexed.first_contact.push(FirstContactRecord {
                        ordinal,
                        tag,
                        kem_ciphertext,
                        ciphertext: *ciphertext,
                    }),
                    EnvelopeEntry::WalletState { .. } => indexed.state_entries += 1,
                }
            }
        }
        indexed
    }

    /// The block's envelope counts.
    pub fn counts(&self) -> Counts {
        Counts {
            envelopes_valid: u64::from(self.envelopes_valid),
            envelopes_invalid: self.invalid.len() as u64,
            memo_entries: self.memo.len() as u64,
            first_contact_entries: self.first_contact.len() as u64,
            state_entries: u64::from(self.state_entries),
        }
    }
}

/// The table key: the tag's first 16 bytes (spec 6.3).
pub(crate) fn key16(tag: &[u8; 32]) -> [u8; 16] {
    let mut key = [0u8; 16];
    key.copy_from_slice(&tag[..16]);
    key
}
