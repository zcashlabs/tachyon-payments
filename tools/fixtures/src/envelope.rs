//! The synthetic envelope generator (spec sections 4.2 and 5).
//!
//! Envelopes are **authored** here, as values, and encoded into `vMemoTachyon`
//! bytes by this module's own writer, straight from spec section 5.1. Nothing
//! in this crate parses an envelope: the ground truth a chain keeps
//! ([`AuthoredMemo`], and the ordinal lists built from it by
//! [`crate::Block::follow_ups`] and friends) is what was authored, so an
//! end-to-end test can compute expected memo and first-contact answers without
//! sharing any code with the products' parser.
//!
//! ```text
//! envelope    = "TPM1" entry_count(u8, 1..=16) entry*
//! entry       = kind(u8) tag(32) body
//! follow-up     0x01  ciphertext(128)
//! first contact 0x02  kem_ciphertext(1088) || ciphertext(128)
//! wallet state  0x03  len(u16 LE) || ciphertext(len), len <= 1024
//! ```
//!
//! An envelope is invalid when its magic, count, a kind or a length is wrong,
//! when bytes are missing or left over, or when a follow-up or first-contact
//! entry's `key16` (the tag's first 16 bytes) is all zero (spec 6.2).
//! [`Malformation`] authors one envelope for each of those rules, breaking that
//! rule and no other.
use crate::model::FixtureError;
use crate::rng::Rng;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The envelope magic (spec 5.1).
pub const MAGIC: [u8; 4] = *b"TPM1";
/// `MEMO_CT_BYTES` (spec 15).
pub const MEMO_CT_BYTES: usize = 128;
/// `KEM_CT_BYTES` (spec 15).
pub const KEM_CT_BYTES: usize = 1088;
/// `MAX_ENVELOPE_ENTRIES` (spec 15).
pub const MAX_ENVELOPE_ENTRIES: usize = 16;
/// `MAX_STATE_BYTES` (spec 15).
pub const MAX_STATE_BYTES: usize = 1024;
/// Kind byte of a follow-up entry.
pub const KIND_FOLLOW_UP: u8 = 0x01;
/// Kind byte of a first-contact entry.
pub const KIND_FIRST_CONTACT: u8 = 0x02;
/// Kind byte of a wallet-state entry.
pub const KIND_WALLET_STATE: u8 = 0x03;

/// The kind of an envelope entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    FollowUp,
    FirstContact,
    WalletState,
}

impl EntryKind {
    /// Every kind, in kind-byte order.
    pub const ALL: [EntryKind; 3] = [
        EntryKind::FollowUp,
        EntryKind::FirstContact,
        EntryKind::WalletState,
    ];

    /// The kind byte (spec 5.1).
    pub fn byte(self) -> u8 {
        match self {
            EntryKind::FollowUp => KIND_FOLLOW_UP,
            EntryKind::FirstContact => KIND_FIRST_CONTACT,
            EntryKind::WalletState => KIND_WALLET_STATE,
        }
    }

    fn index(self) -> usize {
        match self {
            EntryKind::FollowUp => 0,
            EntryKind::FirstContact => 1,
            EntryKind::WalletState => 2,
        }
    }
}

/// One authored envelope entry.
// Fixture data: the first-contact variant is 1.2 KB, and plain arrays keep the
// authoring type exactly the shape of the wire entry.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AuthoredEntry {
    /// Kind `0x01`: goes to the memo database.
    FollowUp {
        tag: [u8; 32],
        ciphertext: [u8; MEMO_CT_BYTES],
    },
    /// Kind `0x02`: goes to the first-contact database.
    FirstContact {
        tag: [u8; 32],
        kem_ciphertext: [u8; KEM_CT_BYTES],
        ciphertext: [u8; MEMO_CT_BYTES],
    },
    /// Kind `0x03`: counted and otherwise ignored. A valid entry has at most
    /// [`MAX_STATE_BYTES`] of ciphertext.
    WalletState { tag: [u8; 32], ciphertext: Vec<u8> },
}

impl AuthoredEntry {
    pub fn kind(&self) -> EntryKind {
        match self {
            AuthoredEntry::FollowUp { .. } => EntryKind::FollowUp,
            AuthoredEntry::FirstContact { .. } => EntryKind::FirstContact,
            AuthoredEntry::WalletState { .. } => EntryKind::WalletState,
        }
    }

    pub fn tag(&self) -> &[u8; 32] {
        match self {
            AuthoredEntry::FollowUp { tag, .. }
            | AuthoredEntry::FirstContact { tag, .. }
            | AuthoredEntry::WalletState { tag, .. } => tag,
        }
    }

    /// The tag's first 16 bytes, the table key (spec 6.3).
    pub fn key16(&self) -> [u8; 16] {
        key16(self.tag())
    }

    /// The entry's encoded length: kind, tag and body.
    pub fn encoded_len(&self) -> usize {
        1 + 32
            + match self {
                AuthoredEntry::FollowUp { .. } => MEMO_CT_BYTES,
                AuthoredEntry::FirstContact { .. } => KEM_CT_BYTES + MEMO_CT_BYTES,
                AuthoredEntry::WalletState { ciphertext, .. } => 2 + ciphertext.len(),
            }
    }

    /// A random entry of `kind` with a random tag (nonzero `key16`). A
    /// wallet-state entry gets 0 to 64 bytes of ciphertext.
    pub fn random(rng: &mut Rng, kind: EntryKind) -> Self {
        let tag = random_tag(rng);
        Self::random_with_tag(rng, kind, tag)
    }

    /// A random entry of `kind` with exactly `tag` (so a test can repeat tags or
    /// place ground keys). A wallet-state entry gets 0 to 64 bytes.
    pub fn random_with_tag(rng: &mut Rng, kind: EntryKind, tag: [u8; 32]) -> Self {
        let len = rng.range(0, 64) as usize;
        Self::random_sized(rng, kind, tag, len)
    }

    /// As [`Self::random_with_tag`], with `state_len` bytes of wallet-state
    /// ciphertext (ignored for the other kinds).
    pub fn random_sized(rng: &mut Rng, kind: EntryKind, tag: [u8; 32], state_len: usize) -> Self {
        match kind {
            EntryKind::FollowUp => AuthoredEntry::FollowUp {
                tag,
                ciphertext: rng.array(),
            },
            EntryKind::FirstContact => AuthoredEntry::FirstContact {
                tag,
                kem_ciphertext: rng.array(),
                ciphertext: rng.array(),
            },
            EntryKind::WalletState => AuthoredEntry::WalletState {
                tag,
                ciphertext: rng.bytes(state_len),
            },
        }
    }

    fn write(&self, out: &mut Vec<u8>) {
        out.push(self.kind().byte());
        out.extend_from_slice(self.tag());
        match self {
            AuthoredEntry::FollowUp { ciphertext, .. } => out.extend_from_slice(ciphertext),
            AuthoredEntry::FirstContact {
                kem_ciphertext,
                ciphertext,
                ..
            } => {
                out.extend_from_slice(kem_ciphertext);
                out.extend_from_slice(ciphertext);
            }
            AuthoredEntry::WalletState { ciphertext, .. } => {
                let len = u16::try_from(ciphertext.len())
                    .expect("a wallet-state ciphertext longer than u16::MAX cannot be encoded");
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(ciphertext);
            }
        }
    }
}

/// The tag's first 16 bytes.
pub fn key16(tag: &[u8; 32]) -> [u8; 16] {
    let mut out = [0; 16];
    out.copy_from_slice(&tag[..16]);
    out
}

/// A random tag whose `key16` is not all zero.
pub fn random_tag(rng: &mut Rng) -> [u8; 32] {
    loop {
        let tag: [u8; 32] = rng.array();
        if tag[..16].iter().any(|&byte| byte != 0) {
            return tag;
        }
    }
}

/// A tag whose `key16` is exactly `key16` and whose other 16 bytes are random:
/// how a test's ground keys (scenario 6) become tags.
pub fn tag_with_key16(rng: &mut Rng, key16: [u8; 16]) -> [u8; 32] {
    let mut tag = [0; 32];
    tag[..16].copy_from_slice(&key16);
    rng.fill(&mut tag[16..]);
    tag
}

/// Why an authored envelope is not valid.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidEnvelope {
    #[error("{0} entries; a valid envelope has 1 to 16")]
    EntryCount(usize),
    #[error("entry {index} has {len} bytes of wallet state; at most 1024 are valid")]
    StateTooLong { index: usize, len: usize },
    #[error("entry {index} has an all-zero key16")]
    ZeroKey { index: usize },
}

/// An authored envelope: the content of one `vMemoTachyon`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct AuthoredEnvelope {
    pub entries: Vec<AuthoredEntry>,
}

impl AuthoredEnvelope {
    pub fn new(entries: Vec<AuthoredEntry>) -> Self {
        Self { entries }
    }

    /// The spec 5.1 encoding: `"TPM1" || u8 count || (kind || tag || body)*`,
    /// wallet-state bodies as `len (u16 LE) || ciphertext`.
    ///
    /// It encodes invalid envelopes too ([`Malformation`] uses that), but panics
    /// on one that cannot be written at all: more than 255 entries, or a
    /// wallet-state ciphertext longer than `u16::MAX`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let count = u8::try_from(self.entries.len())
            .expect("an envelope of more than 255 entries cannot be encoded");
        let mut out = Vec::with_capacity(self.encoded_len());
        out.extend_from_slice(&MAGIC);
        out.push(count);
        for entry in &self.entries {
            entry.write(&mut out);
        }
        out
    }

    /// The length of [`Self::to_bytes`].
    pub fn encoded_len(&self) -> usize {
        MAGIC.len()
            + 1
            + self
                .entries
                .iter()
                .map(AuthoredEntry::encoded_len)
                .sum::<usize>()
    }

    /// Whether the envelope is valid (spec 5.1, 5.2 and 6.2), checked on the
    /// authored values. Every tag's `key16` must be nonzero, wallet-state tags
    /// included: spec 6.2 names only memo and first-contact keys, so the
    /// generator never authors an all-zero wallet-state key and does not claim
    /// such an envelope is valid.
    pub fn check(&self) -> Result<(), InvalidEnvelope> {
        if !(1..=MAX_ENVELOPE_ENTRIES).contains(&self.entries.len()) {
            return Err(InvalidEnvelope::EntryCount(self.entries.len()));
        }
        for (index, entry) in self.entries.iter().enumerate() {
            if entry.key16() == [0; 16] {
                return Err(InvalidEnvelope::ZeroKey { index });
            }
            if let AuthoredEntry::WalletState { ciphertext, .. } = entry
                && ciphertext.len() > MAX_STATE_BYTES
            {
                return Err(InvalidEnvelope::StateTooLong {
                    index,
                    len: ciphertext.len(),
                });
            }
        }
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        self.check().is_ok()
    }

    /// The bytes of an envelope malformed as `malformation` names (scenario 7),
    /// drawn from `rng`.
    pub fn malformed(malformation: Malformation, rng: &mut Rng) -> Vec<u8> {
        malformation.author(rng).bytes
    }

    /// `follow_ups` follow-up, `first_contacts` first-contact and
    /// `wallet_states` wallet-state entries with random tags and content, in a
    /// random order. Panics unless the total is 1 to 16.
    pub fn random(
        rng: &mut Rng,
        follow_ups: usize,
        first_contacts: usize,
        wallet_states: usize,
    ) -> Self {
        let mut kinds = kinds(follow_ups, first_contacts, wallet_states);
        assert!(
            (1..=MAX_ENVELOPE_ENTRIES).contains(&kinds.len()),
            "a valid envelope has 1 to 16 entries, not {}",
            kinds.len()
        );
        shuffle(rng, &mut kinds);
        Self::new(
            kinds
                .into_iter()
                .map(|kind| AuthoredEntry::random(rng, kind))
                .collect(),
        )
    }

    /// The entry of `kind` at each position of the envelope, with its entry
    /// index.
    pub fn entries_of(&self, kind: EntryKind) -> impl Iterator<Item = (usize, &AuthoredEntry)> {
        self.entries
            .iter()
            .enumerate()
            .filter(move |(_, entry)| entry.kind() == kind)
    }
}

/// Splits `entries`, in order, into consecutive envelopes of 1 to 16 entries
/// each (sizes drawn from `rng`), so their ordinals keep the given order. Panics
/// if an entry would make its envelope invalid.
pub fn pack(rng: &mut Rng, entries: Vec<AuthoredEntry>) -> Vec<AuthoredEnvelope> {
    let mut out = Vec::new();
    let mut rest = entries.into_iter().peekable();
    while rest.peek().is_some() {
        let size = rng.range(1, MAX_ENVELOPE_ENTRIES as u64) as usize;
        let envelope = AuthoredEnvelope::new(rest.by_ref().take(size).collect());
        if let Err(error) = envelope.check() {
            panic!("pack: an entry makes its envelope invalid: {error}");
        }
        out.push(envelope);
    }
    out
}

fn kinds(follow_ups: usize, first_contacts: usize, wallet_states: usize) -> Vec<EntryKind> {
    let mut out = Vec::with_capacity(follow_ups + first_contacts + wallet_states);
    out.extend(std::iter::repeat_n(EntryKind::FollowUp, follow_ups));
    out.extend(std::iter::repeat_n(EntryKind::FirstContact, first_contacts));
    out.extend(std::iter::repeat_n(EntryKind::WalletState, wallet_states));
    out
}

/// A deterministic Fisher-Yates shuffle.
pub(crate) fn shuffle<T>(rng: &mut Rng, items: &mut [T]) {
    for i in (1..items.len()).rev() {
        let j = rng.range(0, i as u64) as usize;
        items.swap(i, j);
    }
}

/// One way an envelope can be invalid (spec 5.1, 5.2, 6.2; scenario 7). Each
/// variant authors bytes that break exactly the rule it names and keep every
/// other rule: the other entries are well formed, counts match unless the count
/// is the broken rule, and every other key is nonzero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Malformation {
    /// A valid envelope whose first four bytes are not `TPM1`.
    BadMagic,
    /// Exactly `TPM1`: the input ends before the entry count.
    MissingCount,
    /// `TPM1 00` and nothing after it.
    CountZero,
    /// A count of 17 followed by 17 well-formed entries.
    CountAboveMax,
    /// A count of `n + 1` (at most 16) followed by `n` whole entries: the input
    /// ends where the last counted entry would start.
    CountExceedsEntries,
    /// One entry whose kind is not 1, 2 or 3; it carries a tag and 128 bytes of
    /// body, so it would be a valid follow-up with kind `0x01`.
    UnknownKind,
    /// The last entry's body is cut short (by 1 byte up to all of it).
    TruncatedBody,
    /// A valid envelope followed by 1 to 32 extra bytes.
    TrailingBytes,
    /// One wallet-state entry with `len` 1025 to 1100 and exactly `len` bytes
    /// of ciphertext.
    StateTooLong,
    /// One follow-up entry whose `key16` is all zero (the tag's other 16 bytes
    /// are random).
    FollowUpZeroKey,
    /// One first-contact entry whose `key16` is all zero.
    FirstContactZeroKey,
    /// 1 to 300 random bytes that do not start with `TPM1`.
    RandomGarbage,
}

impl Malformation {
    /// Every malformation.
    pub const ALL: [Malformation; 12] = [
        Malformation::BadMagic,
        Malformation::MissingCount,
        Malformation::CountZero,
        Malformation::CountAboveMax,
        Malformation::CountExceedsEntries,
        Malformation::UnknownKind,
        Malformation::TruncatedBody,
        Malformation::TrailingBytes,
        Malformation::StateTooLong,
        Malformation::FollowUpZeroKey,
        Malformation::FirstContactZeroKey,
        Malformation::RandomGarbage,
    ];

    /// Authors a malformed envelope from `rng`.
    pub fn author(self, rng: &mut Rng) -> MalformedMemo {
        let bytes = match self {
            Malformation::BadMagic => {
                let mut bytes = base(rng, 1, 4).to_bytes();
                let mut magic: [u8; 4] = rng.array();
                if magic == MAGIC {
                    magic[0] ^= 0x80;
                }
                bytes[..4].copy_from_slice(&magic);
                bytes
            }
            Malformation::MissingCount => MAGIC.to_vec(),
            Malformation::CountZero => {
                let mut bytes = MAGIC.to_vec();
                bytes.push(0);
                bytes
            }
            Malformation::CountAboveMax => {
                // Follow-ups and wallet states only, to keep the memo small.
                let entries = (0..=MAX_ENVELOPE_ENTRIES)
                    .map(|_| {
                        let kind = if rng.percent(50) {
                            EntryKind::FollowUp
                        } else {
                            EntryKind::WalletState
                        };
                        AuthoredEntry::random(rng, kind)
                    })
                    .collect();
                AuthoredEnvelope::new(entries).to_bytes()
            }
            Malformation::CountExceedsEntries => {
                let envelope = base(rng, 1, MAX_ENVELOPE_ENTRIES - 1);
                let mut bytes = envelope.to_bytes();
                bytes[4] = envelope.entries.len() as u8 + 1;
                bytes
            }
            Malformation::UnknownKind => {
                let envelope = base(rng, 1, 4);
                let at = rng.range(0, envelope.entries.len() as u64 - 1) as usize;
                // 0x00 or 0x04..=0xff.
                let kind = match rng.range(0, 252) as u8 {
                    0 => 0,
                    other => other + 3,
                };
                let mut bytes = MAGIC.to_vec();
                bytes.push(envelope.entries.len() as u8);
                for (index, entry) in envelope.entries.iter().enumerate() {
                    if index == at {
                        bytes.push(kind);
                        bytes.extend_from_slice(&random_tag(rng));
                        bytes.extend_from_slice(&rng.bytes(MEMO_CT_BYTES));
                    } else {
                        entry.write(&mut bytes);
                    }
                }
                bytes
            }
            Malformation::TruncatedBody => {
                let envelope = base(rng, 1, 4);
                let last = envelope.entries.last().expect("base has an entry");
                let body = last.encoded_len() - 33;
                let cut = rng.range(1, body as u64) as usize;
                let mut bytes = envelope.to_bytes();
                bytes.truncate(bytes.len() - cut);
                bytes
            }
            Malformation::TrailingBytes => {
                let mut bytes = base(rng, 1, 4).to_bytes();
                let extra = rng.range(1, 32) as usize;
                bytes.extend_from_slice(&rng.bytes(extra));
                bytes
            }
            Malformation::StateTooLong => {
                let len = rng.range(MAX_STATE_BYTES as u64 + 1, 1100) as usize;
                let tag = random_tag(rng);
                let entry = AuthoredEntry::random_sized(rng, EntryKind::WalletState, tag, len);
                among_valid(rng, entry).to_bytes()
            }
            Malformation::FollowUpZeroKey => {
                let tag = zero_key(rng);
                let entry = AuthoredEntry::random_with_tag(rng, EntryKind::FollowUp, tag);
                among_valid(rng, entry).to_bytes()
            }
            Malformation::FirstContactZeroKey => {
                let tag = zero_key(rng);
                let entry = AuthoredEntry::random_with_tag(rng, EntryKind::FirstContact, tag);
                among_valid(rng, entry).to_bytes()
            }
            Malformation::RandomGarbage => {
                let len = rng.range(1, 300) as usize;
                not_an_envelope(rng.bytes(len))
            }
        };
        MalformedMemo {
            malformation: self,
            bytes,
        }
    }
}

/// A valid envelope of `min..=max` entries of random kinds.
fn base(rng: &mut Rng, min: usize, max: usize) -> AuthoredEnvelope {
    let count = rng.range(min as u64, max as u64) as usize;
    let entries = (0..count)
        .map(|_| {
            let kind = EntryKind::ALL[rng.range(0, 2) as usize];
            AuthoredEntry::random(rng, kind)
        })
        .collect();
    AuthoredEnvelope::new(entries)
}

/// `entry` at a random position among 0 to 3 valid random entries.
fn among_valid(rng: &mut Rng, entry: AuthoredEntry) -> AuthoredEnvelope {
    let mut envelope = base(rng, 0, 3);
    let at = rng.range(0, envelope.entries.len() as u64) as usize;
    envelope.entries.insert(at, entry);
    envelope
}

/// A tag with an all-zero `key16` and random remaining bytes.
fn zero_key(rng: &mut Rng) -> [u8; 32] {
    tag_with_key16(rng, [0; 16])
}

/// `bytes` changed, if needed, so that they do not start with the magic: random
/// memos are then certainly not envelopes, without drawing more randomness.
pub(crate) fn not_an_envelope(mut bytes: Vec<u8>) -> Vec<u8> {
    if bytes.starts_with(&MAGIC) {
        bytes[0] ^= 0x80;
    }
    bytes
}

/// The bytes of a malformed envelope and the rule they break.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MalformedMemo {
    pub malformation: Malformation,
    pub bytes: Vec<u8>,
}

/// A transaction's memo as authored: the ground truth the oracle reads.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum AuthoredMemo {
    /// No memo (`""` on the wire); skipped by ingestion.
    #[default]
    Empty,
    /// A valid envelope.
    Envelope(AuthoredEnvelope),
    /// Bytes that are not a valid envelope, and why. Random memos of a
    /// [`crate::TxMix`] (no envelope mix) are [`Malformation::RandomGarbage`]:
    /// the generator guarantees they do not start with `TPM1`.
    Malformed(MalformedMemo),
    /// Non-empty bytes a test set with [`crate::TxSpec::memo`]; not classified.
    Raw(Vec<u8>),
}

impl AuthoredMemo {
    /// [`Self::Empty`] for no bytes, else [`Self::Raw`].
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            AuthoredMemo::Empty
        } else {
            AuthoredMemo::Raw(bytes)
        }
    }

    /// Random memo bytes known not to be an envelope.
    pub(crate) fn garbage(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            AuthoredMemo::Empty
        } else {
            AuthoredMemo::Malformed(MalformedMemo {
                malformation: Malformation::RandomGarbage,
                bytes: not_an_envelope(bytes),
            })
        }
    }

    /// The `vMemoTachyon` bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            AuthoredMemo::Empty => Vec::new(),
            AuthoredMemo::Envelope(envelope) => envelope.to_bytes(),
            AuthoredMemo::Malformed(malformed) => malformed.bytes.clone(),
            AuthoredMemo::Raw(bytes) => bytes.clone(),
        }
    }

    /// The valid envelope, if this is one.
    pub fn envelope(&self) -> Option<&AuthoredEnvelope> {
        match self {
            AuthoredMemo::Envelope(envelope) => Some(envelope),
            _ => None,
        }
    }

    /// The malformation, if the memo is a malformed envelope.
    pub fn malformation(&self) -> Option<Malformation> {
        match self {
            AuthoredMemo::Malformed(malformed) => Some(malformed.malformation),
            _ => None,
        }
    }
}

/// How a [`crate::TxSpec`]'s memo is authored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemoSpec {
    /// Exactly this memo.
    Authored(AuthoredMemo),
    /// An envelope malformed this way, drawn from the chain's stream when the
    /// transaction is built.
    Malformed(Malformation),
}

impl Default for MemoSpec {
    fn default() -> Self {
        MemoSpec::Authored(AuthoredMemo::Empty)
    }
}

/// A follow-up entry of the active chain as authored, with its ordinal
/// `(height, tx_index, entry_index)` (spec 5.2).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FollowUpTruth {
    pub height: u64,
    /// Position in the block's `tx` list (the coinbase is 0).
    pub tx_index: usize,
    /// Position in the envelope's entry list, all kinds counted.
    pub entry_index: usize,
    pub tag: [u8; 32],
    pub ciphertext: [u8; MEMO_CT_BYTES],
}

impl FollowUpTruth {
    pub fn ordinal(&self) -> (u64, usize, usize) {
        (self.height, self.tx_index, self.entry_index)
    }

    pub fn key16(&self) -> [u8; 16] {
        key16(&self.tag)
    }
}

/// A first-contact entry of the active chain as authored, with its ordinal.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FirstContactTruth {
    pub height: u64,
    /// Position in the block's `tx` list (the coinbase is 0).
    pub tx_index: usize,
    /// Position in the envelope's entry list, all kinds counted.
    pub entry_index: usize,
    pub tag: [u8; 32],
    pub kem_ciphertext: [u8; KEM_CT_BYTES],
    pub ciphertext: [u8; MEMO_CT_BYTES],
}

impl FirstContactTruth {
    pub fn ordinal(&self) -> (u64, usize, usize) {
        (self.height, self.tx_index, self.entry_index)
    }

    pub fn key16(&self) -> [u8; 16] {
        key16(&self.tag)
    }
}

/// What a block (or a range of blocks) carries in its Tachyon memos, as
/// authored. `follow_ups`, `first_contacts` and `wallet_states` count entries
/// of valid envelopes only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct EnvelopeCounts {
    /// Tachyon transactions with an empty memo.
    pub empty: usize,
    /// Valid envelopes.
    pub valid: usize,
    /// Malformed envelopes (each counted by `tachyon_memo_envelopes_invalid_total`).
    pub malformed: usize,
    /// Unclassified memos set as raw bytes by a test.
    pub raw: usize,
    pub follow_ups: usize,
    pub first_contacts: usize,
    pub wallet_states: usize,
}

impl EnvelopeCounts {
    pub(crate) fn add_memo(&mut self, memo: &AuthoredMemo) {
        match memo {
            AuthoredMemo::Empty => self.empty += 1,
            AuthoredMemo::Malformed(_) => self.malformed += 1,
            AuthoredMemo::Raw(_) => self.raw += 1,
            AuthoredMemo::Envelope(envelope) => {
                self.valid += 1;
                for entry in &envelope.entries {
                    match entry.kind() {
                        EntryKind::FollowUp => self.follow_ups += 1,
                        EntryKind::FirstContact => self.first_contacts += 1,
                        EntryKind::WalletState => self.wallet_states += 1,
                    }
                }
            }
        }
    }
}

impl std::ops::AddAssign for EnvelopeCounts {
    fn add_assign(&mut self, other: Self) {
        self.empty += other.empty;
        self.valid += other.valid;
        self.malformed += other.malformed;
        self.raw += other.raw;
        self.follow_ups += other.follow_ups;
        self.first_contacts += other.first_contacts;
        self.wallet_states += other.wallet_states;
    }
}

/// Random envelopes for generated blocks ([`crate::ChainConfig::envelopes`],
/// the chain spec's `envelopes`). Ranges are inclusive `[min, max]`. When set,
/// it replaces the [`crate::TxMix`]'s random memos.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EnvelopeMix {
    /// Chance, in percent, that a generated Tachyon transaction carries an
    /// envelope; the others have an empty memo.
    pub envelope_percent: u8,
    /// Chance, in percent, that a carried envelope is malformed, with a
    /// [`Malformation`] drawn uniformly.
    pub malformed_percent: u8,
    /// Follow-up entries per valid envelope.
    pub follow_ups: [u32; 2],
    /// First-contact entries per valid envelope.
    pub first_contacts: [u32; 2],
    /// Wallet-state entries per valid envelope.
    pub wallet_states: [u32; 2],
    /// Wallet-state ciphertext bytes; at most 1024.
    pub wallet_state_len: [u32; 2],
    /// Chance, in percent, that an entry reuses a tag already drawn for an
    /// entry of the same kind on this chain (any branch, an earlier entry of the
    /// same envelope included): scenario 5.
    pub repeat_tag_percent: u8,
    /// Ground keys (32 hex digits each in JSON) for scenario 6: an entry's tag
    /// is one of these `key16`s plus 16 random bytes with chance
    /// `key16_percent`.
    #[serde(with = "hex_keys")]
    pub key16s: Vec<[u8; 16]>,
    pub key16_percent: u8,
}

impl Default for EnvelopeMix {
    fn default() -> Self {
        Self {
            envelope_percent: 80,
            malformed_percent: 10,
            follow_ups: [0, 4],
            first_contacts: [0, 1],
            wallet_states: [0, 1],
            wallet_state_len: [0, 64],
            repeat_tag_percent: 10,
            key16s: Vec::new(),
            key16_percent: 0,
        }
    }
}

impl EnvelopeMix {
    /// Refuses a mix whose envelopes would not be valid.
    pub fn check(&self) -> Result<(), FixtureError> {
        let error = |message: String| Err(FixtureError::Spec(format!("envelopes: {message}")));
        for (name, percent) in [
            ("envelope_percent", self.envelope_percent),
            ("malformed_percent", self.malformed_percent),
            ("repeat_tag_percent", self.repeat_tag_percent),
            ("key16_percent", self.key16_percent),
        ] {
            if percent > 100 {
                return error(format!("{name} {percent} is over 100"));
            }
        }
        if self.wallet_state_len[1] as usize > MAX_STATE_BYTES {
            return error(format!(
                "wallet_state_len {} is over {MAX_STATE_BYTES}",
                self.wallet_state_len[1]
            ));
        }
        if self.key16s.contains(&[0; 16]) {
            return error("an all-zero key16 makes its envelope invalid".to_owned());
        }
        Ok(())
    }
}

/// Tags drawn so far per kind, for [`EnvelopeMix::repeat_tag_percent`].
#[derive(Clone, Debug, Default)]
pub(crate) struct TagPool {
    tags: [Vec<[u8; 32]>; 3],
    seen: [std::collections::HashSet<[u8; 32]>; 3],
}

impl TagPool {
    pub(crate) fn insert(&mut self, kind: EntryKind, tag: [u8; 32]) {
        if self.seen[kind.index()].insert(tag) {
            self.tags[kind.index()].push(tag);
        }
    }

    fn pick(&self, rng: &mut Rng, kind: EntryKind) -> Option<[u8; 32]> {
        let tags = &self.tags[kind.index()];
        if tags.is_empty() {
            return None;
        }
        Some(tags[rng.range(0, tags.len() as u64 - 1) as usize])
    }
}

/// Draws one generated transaction's memo from `mix`.
pub(crate) fn draw_memo(rng: &mut Rng, mix: &EnvelopeMix, pool: &mut TagPool) -> MemoSpec {
    if !rng.percent(mix.envelope_percent) {
        return MemoSpec::Authored(AuthoredMemo::Empty);
    }
    if rng.percent(mix.malformed_percent) {
        let malformation =
            Malformation::ALL[rng.range(0, Malformation::ALL.len() as u64 - 1) as usize];
        return MemoSpec::Malformed(malformation);
    }
    let draw = |rng: &mut Rng, [low, high]: [u32; 2]| rng.range(low.into(), high.into()) as usize;
    let follow_ups = draw(rng, mix.follow_ups);
    let first_contacts = draw(rng, mix.first_contacts);
    let wallet_states = draw(rng, mix.wallet_states);
    let mut kinds = kinds(follow_ups, first_contacts, wallet_states);
    if kinds.is_empty() {
        kinds.push(EntryKind::FollowUp);
    }
    shuffle(rng, &mut kinds);
    kinds.truncate(MAX_ENVELOPE_ENTRIES);
    let entries = kinds
        .into_iter()
        .map(|kind| {
            let tag = if let Some(tag) = rng
                .percent(mix.repeat_tag_percent)
                .then(|| pool.pick(rng, kind))
                .flatten()
            {
                tag
            } else if !mix.key16s.is_empty() && rng.percent(mix.key16_percent) {
                let key = mix.key16s[rng.range(0, mix.key16s.len() as u64 - 1) as usize];
                tag_with_key16(rng, key)
            } else {
                random_tag(rng)
            };
            pool.insert(kind, tag);
            let len = draw(rng, mix.wallet_state_len).min(MAX_STATE_BYTES);
            AuthoredEntry::random_sized(rng, kind, tag, len)
        })
        .collect();
    MemoSpec::Authored(AuthoredMemo::Envelope(AuthoredEnvelope::new(entries)))
}

/// An envelope in a chain spec: every value as hex, unreversed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvelopeJson {
    pub entries: Vec<EntryJson>,
}

/// An envelope entry in a chain spec.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EntryJson {
    FollowUp {
        tag: String,
        ciphertext: String,
    },
    FirstContact {
        tag: String,
        kem_ciphertext: String,
        ciphertext: String,
    },
    WalletState {
        tag: String,
        ciphertext: String,
    },
}

impl From<&AuthoredEnvelope> for EnvelopeJson {
    fn from(envelope: &AuthoredEnvelope) -> Self {
        let entries = envelope
            .entries
            .iter()
            .map(|entry| match entry {
                AuthoredEntry::FollowUp { tag, ciphertext } => EntryJson::FollowUp {
                    tag: hex::encode(tag),
                    ciphertext: hex::encode(ciphertext),
                },
                AuthoredEntry::FirstContact {
                    tag,
                    kem_ciphertext,
                    ciphertext,
                } => EntryJson::FirstContact {
                    tag: hex::encode(tag),
                    kem_ciphertext: hex::encode(kem_ciphertext),
                    ciphertext: hex::encode(ciphertext),
                },
                AuthoredEntry::WalletState { tag, ciphertext } => EntryJson::WalletState {
                    tag: hex::encode(tag),
                    ciphertext: hex::encode(ciphertext),
                },
            })
            .collect();
        Self { entries }
    }
}

impl EnvelopeJson {
    /// The authored envelope; refused unless it is valid.
    pub fn to_envelope(&self) -> Result<AuthoredEnvelope, FixtureError> {
        let entries = self
            .entries
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let field = |name: &str| format!("envelope entry {index}: {name}");
                Ok(match entry {
                    EntryJson::FollowUp { tag, ciphertext } => AuthoredEntry::FollowUp {
                        tag: fixed(tag, &field("tag"))?,
                        ciphertext: fixed(ciphertext, &field("ciphertext"))?,
                    },
                    EntryJson::FirstContact {
                        tag,
                        kem_ciphertext,
                        ciphertext,
                    } => AuthoredEntry::FirstContact {
                        tag: fixed(tag, &field("tag"))?,
                        kem_ciphertext: fixed(kem_ciphertext, &field("kem_ciphertext"))?,
                        ciphertext: fixed(ciphertext, &field("ciphertext"))?,
                    },
                    EntryJson::WalletState { tag, ciphertext } => AuthoredEntry::WalletState {
                        tag: fixed(tag, &field("tag"))?,
                        ciphertext: hex::decode(ciphertext).map_err(|error| {
                            FixtureError::Spec(format!("{}: {error}", field("ciphertext")))
                        })?,
                    },
                })
            })
            .collect::<Result<Vec<_>, FixtureError>>()?;
        let envelope = AuthoredEnvelope::new(entries);
        envelope.check().map_err(|error| {
            FixtureError::Spec(format!("envelope: {error} (author it as `malformed`)"))
        })?;
        Ok(envelope)
    }
}

fn fixed<const N: usize>(hex_value: &str, field: &str) -> Result<[u8; N], FixtureError> {
    hex::decode(hex_value)
        .ok()
        .and_then(|bytes| <[u8; N]>::try_from(bytes).ok())
        .ok_or_else(|| FixtureError::Spec(format!("{field} is not {N} hex bytes")))
}

mod hex_keys {
    use super::*;

    pub fn serialize<S: Serializer>(keys: &[[u8; 16]], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(keys.iter().map(hex::encode))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<[u8; 16]>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .iter()
            .map(|key| {
                hex::decode(key)
                    .ok()
                    .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                    .ok_or_else(|| {
                        serde::de::Error::custom(format!("key16 {key:?} is not 16 hex bytes"))
                    })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_random_mix_memo_is_valid_or_named() {
        let mut rng = Rng::new("envelope-test", 1);
        let mut pool = TagPool::default();
        let mix = EnvelopeMix {
            follow_ups: [0, 20],
            ..EnvelopeMix::default()
        };
        for _ in 0..500 {
            match draw_memo(&mut rng, &mix, &mut pool) {
                MemoSpec::Authored(AuthoredMemo::Envelope(envelope)) => {
                    assert_eq!(envelope.check(), Ok(()));
                }
                MemoSpec::Authored(AuthoredMemo::Empty) | MemoSpec::Malformed(_) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn garbage_never_starts_with_the_magic() {
        assert_eq!(
            not_an_envelope(b"TPM1xyz".to_vec())[..4],
            [b'T' ^ 0x80, b'P', b'M', b'1']
        );
        assert_eq!(not_an_envelope(b"TPM".to_vec()), b"TPM");
        assert_eq!(AuthoredMemo::garbage(Vec::new()), AuthoredMemo::Empty);
    }

    #[test]
    fn mix_check_refuses_invalid_settings() {
        assert!(EnvelopeMix::default().check().is_ok());
        for mix in [
            EnvelopeMix {
                malformed_percent: 101,
                ..EnvelopeMix::default()
            },
            EnvelopeMix {
                wallet_state_len: [0, 1025],
                ..EnvelopeMix::default()
            },
            EnvelopeMix {
                key16s: vec![[0; 16]],
                ..EnvelopeMix::default()
            },
        ] {
            assert!(mix.check().is_err(), "{mix:?}");
        }
    }
}
