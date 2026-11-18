//! The SQLite journal `memo-v1.sqlite` (spec section 7.3).
use crate::block::{Counts, FirstContactRecord, IndexedBlock, Kind, MemoRecord, Ordinal, key16};
use crate::error::{EntryFault, Error};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use tachyon_chain::{BlockId, EpochSchedule, Hash, NetworkIdentity};
use tachyon_pir_protocol::Entry;
use tachyon_pir_protocol::constants::{FC_SPAN_MAX_EPOCHS, FC_SPAN_TARGET};

/// The store's file name in a data directory.
pub const STORE_FILE: &str = "memo-v1.sqlite";

/// The schema version, kept in `PRAGMA user_version`.
pub const SCHEMA_VERSION: u32 = 1;

/// How long a write waits for another connection's lock.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// The schema. Every table is keyed for the reads the publisher makes:
///
/// - `blocks` by height, with `(epoch, height)` for epoch summaries and span
///   counts. Each block row carries the block's envelope counts (spec 11.8).
/// - `memo_entries` by `(epoch, height, tx_index, entry_index)`: an epoch's
///   entries in ordinal order are one clustered range scan.
/// - `first_contact_entries` by the same key, as a unique index on a rowid
///   table: its 1.2 KB rows are too large to cluster well in a
///   `WITHOUT ROWID` b-tree, and rows are only ever appended in ordinal order
///   (rewinds remove a tail), so the rowid order is the ordinal order too.
/// - `span_boundaries` by span index; each row was created by the append of
///   `decided_height`, the last block of `last_epoch`, and cascades from it.
///
/// Entry tables carry no foreign key: SQLite would look up the parent block
/// for every deleted row. [`Store::rewind`] deletes them explicitly by range,
/// and [`Store::epoch_summary`] checks them against their blocks' counts.
const SCHEMA: &str = "
CREATE TABLE binding (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    chain TEXT NOT NULL,
    genesis_hash BLOB NOT NULL CHECK (length(genesis_hash) = 32),
    activation_height INTEGER NOT NULL CHECK (activation_height BETWEEN 0 AND 4294967295),
    epoch_length INTEGER NOT NULL CHECK (epoch_length BETWEEN 1 AND 4294967295),
    fc_span_target INTEGER NOT NULL CHECK (fc_span_target >= 1),
    fc_span_max_epochs INTEGER NOT NULL CHECK (fc_span_max_epochs BETWEEN 1 AND 4294967295)
);
CREATE TABLE blocks (
    height INTEGER PRIMARY KEY CHECK (height BETWEEN 0 AND 4294967295),
    hash BLOB NOT NULL CHECK (length(hash) = 32),
    epoch INTEGER NOT NULL CHECK (epoch BETWEEN 0 AND 4294967295),
    envelopes_valid INTEGER NOT NULL CHECK (envelopes_valid >= 0),
    envelopes_invalid INTEGER NOT NULL CHECK (envelopes_invalid >= 0),
    memo_entries INTEGER NOT NULL CHECK (memo_entries >= 0),
    first_contact_entries INTEGER NOT NULL CHECK (first_contact_entries >= 0),
    state_entries INTEGER NOT NULL CHECK (state_entries >= 0)
);
CREATE INDEX blocks_epoch ON blocks (epoch, height);
CREATE TABLE memo_entries (
    epoch INTEGER NOT NULL,
    height INTEGER NOT NULL,
    tx_index INTEGER NOT NULL CHECK (tx_index BETWEEN 0 AND 4294967295),
    entry_index INTEGER NOT NULL CHECK (entry_index BETWEEN 0 AND 255),
    tag BLOB NOT NULL CHECK (length(tag) = 32 AND substr(tag, 1, 16) != zeroblob(16)),
    ciphertext BLOB NOT NULL CHECK (length(ciphertext) = 128),
    PRIMARY KEY (epoch, height, tx_index, entry_index)
) WITHOUT ROWID;
CREATE TABLE first_contact_entries (
    epoch INTEGER NOT NULL,
    height INTEGER NOT NULL,
    tx_index INTEGER NOT NULL CHECK (tx_index BETWEEN 0 AND 4294967295),
    entry_index INTEGER NOT NULL CHECK (entry_index BETWEEN 0 AND 255),
    tag BLOB NOT NULL CHECK (length(tag) = 32 AND substr(tag, 1, 16) != zeroblob(16)),
    kem_ciphertext BLOB NOT NULL CHECK (length(kem_ciphertext) = 1088),
    ciphertext BLOB NOT NULL CHECK (length(ciphertext) = 128)
);
CREATE UNIQUE INDEX first_contact_order
    ON first_contact_entries (epoch, height, tx_index, entry_index);
CREATE TABLE span_boundaries (
    span_index INTEGER PRIMARY KEY CHECK (span_index BETWEEN 0 AND 4294967295),
    first_epoch INTEGER NOT NULL UNIQUE,
    last_epoch INTEGER NOT NULL UNIQUE CHECK (last_epoch BETWEEN first_epoch AND 4294967295),
    entries INTEGER NOT NULL CHECK (entries >= 0),
    decided_height INTEGER NOT NULL UNIQUE REFERENCES blocks (height) ON DELETE CASCADE
);
";

/// The network and epoch schedule a database is bound to. Every stored epoch,
/// ordinal and span depends on all four fields, so a store is never reused
/// across a change in any of them.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Binding {
    /// `getblockchaininfo.chain`, for example `"test"`.
    pub chain: String,
    /// Block 0's hash, protocol order.
    pub genesis_hash: Hash,
    /// The `NuTachyon` activation height: pool height zero, the start of
    /// epoch zero, and the first block of every journal.
    pub activation_height: u32,
    /// Blocks per epoch, from configuration (spec 15).
    pub epoch_length: u32,
}

impl Binding {
    /// The binding of the network `identity` with epochs of `epoch_length`
    /// blocks.
    pub fn new(identity: &NetworkIdentity, epoch_length: u32) -> Self {
        Self {
            chain: identity.chain.clone(),
            genesis_hash: identity.genesis_hash,
            activation_height: identity.activation_height,
            epoch_length,
        }
    }

    /// The epoch schedule this binding names.
    pub fn schedule(&self) -> Result<EpochSchedule, Error> {
        Ok(EpochSchedule::new(
            self.activation_height,
            self.epoch_length,
        )?)
    }
}

/// What is stored of one epoch: the memo scope of spec 7.1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochSummary {
    pub epoch: u32,
    /// The first stored block height in the epoch. Every journal starts at
    /// the activation height, so this is always the epoch's first height.
    pub first_height: u32,
    /// The last stored block height in the epoch; the epoch is complete when
    /// this is its last height.
    pub last_height: u32,
    /// The epoch's envelope and entry counts.
    pub counts: Counts,
}

/// A first-contact span (spec 7.2): consecutive whole epochs, the scope of the
/// first-contact tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    /// Zero-based position among spans; span 0 starts at epoch 0.
    pub index: u32,
    /// The span's first epoch, its `scope_id`.
    pub first_epoch: u32,
    /// The span's last epoch once a boundary closed it; `None` while open.
    pub last_epoch: Option<u32>,
    /// First-contact entries stored in the span.
    pub entries: u64,
    /// The block whose append decided the boundary: the last block of
    /// `last_epoch`. `None` while open.
    pub closed_by: Option<BlockId>,
    /// The last stored block height in the span: `closed_by`'s height once
    /// closed, the journal tip while open.
    pub last_height: u32,
}

impl Span {
    /// The span's `scope_id` (spec 7.2): its first epoch.
    pub fn scope_id(&self) -> u64 {
        u64::from(self.first_epoch)
    }

    /// Whether a boundary has closed the span. A closed span never changes
    /// unless a rewind removes the block that closed it.
    pub fn is_closed(&self) -> bool {
        self.last_epoch.is_some()
    }

    /// Whether `epoch` is in the span (an open span extends without bound).
    pub fn contains(&self, epoch: u32) -> bool {
        epoch >= self.first_epoch && self.last_epoch.is_none_or(|last| epoch <= last)
    }
}

/// One stored entry of either kind, with its ordinal and full tag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Memo(MemoRecord),
    FirstContact(FirstContactRecord),
}

impl Record {
    pub fn ordinal(&self) -> Ordinal {
        match self {
            Record::Memo(r) => r.ordinal,
            Record::FirstContact(r) => r.ordinal,
        }
    }

    pub fn tag(&self) -> &[u8; 32] {
        match self {
            Record::Memo(r) => &r.tag,
            Record::FirstContact(r) => &r.tag,
        }
    }

    /// The table entry, in the protocol crate's layout (spec 6.2).
    pub fn entry(&self) -> Entry {
        match self {
            Record::Memo(r) => Entry::Memo(r.entry()),
            Record::FirstContact(r) => Entry::FirstContact(Box::new(r.entry())),
        }
    }
}

/// Whether a span holding `entries` first-contact entries over `epochs`
/// epochs closes at the boundary after its last epoch (spec 7.2).
fn closes(entries: u64, epochs: u64) -> bool {
    entries >= FC_SPAN_TARGET || epochs >= u64::from(FC_SPAN_MAX_EPOCHS)
}

/// The memo and first-contact journal: a contiguous run of blocks from the
/// activation height with their entries, counts and span boundaries, bound to
/// one network.
///
/// One `Store` owns the file: [`Store::committed`] is cached in memory, and
/// [`Store::append`] reads the tip again inside its write transaction, so a
/// second writer cannot make it append out of order but can make
/// [`Store::committed`] stale.
pub struct Store {
    db: Connection,
    binding: Binding,
    schedule: EpochSchedule,
    tip: Option<BlockId>,
}

impl Store {
    /// Opens or creates the journal at `path`, bound to `binding`.
    ///
    /// A new file is bound to `binding` and to this build's span constants;
    /// an existing one must have been created with an equal binding
    /// ([`Error::BindingMismatch`]) and the same constants
    /// ([`Error::SpanRuleMismatch`]), or nothing changes. The database runs in
    /// WAL mode with `synchronous=FULL`, as `receiver-store` does, so every
    /// committed append or rewind survives a crash.
    pub fn open(path: impl AsRef<Path>, binding: Binding) -> Result<Self, Error> {
        let schedule = binding.schedule()?;
        let mut db = Connection::open(path)?;
        db.busy_timeout(BUSY_TIMEOUT)?;
        db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;")?;
        let mode: String = db.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::JournalMode(mode));
        }
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version: u32 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        match version {
            0 => {
                let tables: bool =
                    tx.query_row("SELECT EXISTS (SELECT 1 FROM sqlite_master)", [], |r| {
                        r.get(0)
                    })?;
                if tables {
                    return Err(Error::NotAStore);
                }
                tx.execute_batch(SCHEMA)?;
                tx.execute(
                    "INSERT INTO binding VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        binding.chain,
                        binding.genesis_hash.protocol_bytes(),
                        binding.activation_height,
                        binding.epoch_length,
                        FC_SPAN_TARGET,
                        FC_SPAN_MAX_EPOCHS
                    ],
                )?;
                tx.execute_batch(&format!("PRAGMA user_version={SCHEMA_VERSION};"))?;
            }
            SCHEMA_VERSION => {}
            other => return Err(Error::UnsupportedSchema(other)),
        }
        let (stored, target, max_epochs) = tx.query_row(
            "SELECT chain, genesis_hash, activation_height, epoch_length,
                    fc_span_target, fc_span_max_epochs
             FROM binding WHERE id = 1",
            [],
            |r| {
                Ok((
                    Binding {
                        chain: r.get(0)?,
                        genesis_hash: Hash::from_protocol_bytes(r.get(1)?),
                        activation_height: r.get(2)?,
                        epoch_length: r.get(3)?,
                    },
                    r.get::<_, u64>(4)?,
                    r.get::<_, u32>(5)?,
                ))
            },
        )?;
        if stored != binding {
            return Err(Error::BindingMismatch {
                stored: Box::new(stored),
                requested: Box::new(binding),
            });
        }
        if (target, max_epochs) != (FC_SPAN_TARGET, FC_SPAN_MAX_EPOCHS) {
            return Err(Error::SpanRuleMismatch {
                stored_target: target,
                stored_max_epochs: max_epochs,
                target: FC_SPAN_TARGET,
                max_epochs: FC_SPAN_MAX_EPOCHS,
            });
        }
        let tip = read_tip(&tx)?;
        tx.commit()?;
        Ok(Self {
            db,
            binding,
            schedule,
            tip,
        })
    }

    /// The network and schedule this store is bound to.
    pub fn binding(&self) -> &Binding {
        &self.binding
    }

    /// The epoch schedule of the binding.
    pub fn schedule(&self) -> EpochSchedule {
        self.schedule
    }

    /// The newest stored block, `None` when the store is empty.
    pub fn committed(&self) -> Option<BlockId> {
        self.tip
    }

    /// Appends `block` atomically and durably: all of it is committed or none.
    ///
    /// - The first block of an empty store must be the activation block
    ///   ([`Error::NotAtActivation`]): every journal starts at the activation
    ///   height (spec 7.3), so every epoch is stored from its first block and
    ///   every span from epoch 0.
    /// - Every later block must follow the tip: `height == tip.height + 1` and
    ///   `prev == tip.hash` ([`Error::HeightGap`], [`Error::ParentMismatch`]).
    /// - `block.epoch` must be the epoch the store's schedule gives the height
    ///   ([`Error::EpochMismatch`]). Each entry's ordinal must name this block
    ///   and the entries of each kind must be in strictly ascending ordinal
    ///   order, with no ordinal used twice, and no key may be all zero
    ///   ([`Error::Entry`]).
    /// - Every follow-up and first-contact entry is stored, duplicate tags
    ///   included (spec 5.3), with the block's envelope counts.
    /// - When the block is the last of its epoch, the open first-contact span
    ///   is closed at this epoch if it now holds at least `FC_SPAN_TARGET`
    ///   entries or covers `FC_SPAN_MAX_EPOCHS` epochs (spec 7.2); the
    ///   boundary is a row created in the same transaction.
    pub fn append(&mut self, block: &IndexedBlock) -> Result<(), Error> {
        let height = block.id.height;
        let expected = self.schedule.epoch_of(height);
        if block.epoch != expected {
            return Err(Error::EpochMismatch {
                height,
                claimed: block.epoch,
                expected,
            });
        }
        check_entries(block)?;
        let activation = self.schedule.activation_height();
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        match read_tip(&tx)? {
            Some(tip) => {
                if tip.height.checked_add(1) != Some(height) {
                    return Err(Error::HeightGap { tip, height });
                }
                if tip.hash != block.prev {
                    return Err(Error::ParentMismatch {
                        tip,
                        height,
                        prev: block.prev,
                    });
                }
            }
            None if height != activation => {
                return Err(Error::NotAtActivation { height, activation });
            }
            None => {}
        }
        // At or above activation: the first block is the activation block and
        // every later one follows it.
        let epoch = expected.ok_or(Error::Corrupt("a stored block is below activation"))?;
        let counts = block.counts();
        tx.execute(
            "INSERT INTO blocks (height, hash, epoch, envelopes_valid, envelopes_invalid,
                                 memo_entries, first_contact_entries, state_entries)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                height,
                block.id.hash.protocol_bytes(),
                epoch,
                counts.envelopes_valid,
                counts.envelopes_invalid,
                counts.memo_entries,
                counts.first_contact_entries,
                counts.state_entries
            ],
        )?;
        {
            let mut insert = tx.prepare(
                "INSERT INTO memo_entries (epoch, height, tx_index, entry_index, tag, ciphertext)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for record in &block.memo {
                insert.execute(params![
                    epoch,
                    height,
                    record.ordinal.tx_index,
                    record.ordinal.entry_index,
                    record.tag,
                    record.ciphertext
                ])?;
            }
            let mut insert = tx.prepare(
                "INSERT INTO first_contact_entries
                     (epoch, height, tx_index, entry_index, tag, kem_ciphertext, ciphertext)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for record in &block.first_contact {
                insert.execute(params![
                    epoch,
                    height,
                    record.ordinal.tx_index,
                    record.ordinal.entry_index,
                    record.tag,
                    &record.kem_ciphertext[..],
                    record.ciphertext
                ])?;
            }
        }
        if self.schedule.last_height(epoch) == Some(height) {
            decide_span(&tx, epoch, height)?;
        }
        tx.commit()?;
        self.tip = Some(block.id);
        Ok(())
    }

    /// Removes every block above `keep`, with its entries and every span
    /// boundary it decided; with `None`, removes every block. `keep` must be a
    /// stored block, height and hash ([`Error::UnknownCheckpoint`]). Atomic and
    /// durable, like [`Store::append`]. Re-appending the same blocks afterwards
    /// reproduces the same entries and boundaries, because both depend only on
    /// the stored blocks.
    pub fn rewind(&mut self, keep: Option<&BlockId>) -> Result<(), Error> {
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Every height is above -1, so `None` removes everything. Removed
        // entries are in `keep`'s epoch or later, which bounds the range scan.
        let (above, from_epoch) = match keep {
            None => (-1, 0),
            Some(keep) => {
                if read_hash(&tx, keep.height)? != Some(keep.hash) {
                    return Err(Error::UnknownCheckpoint(*keep));
                }
                let epoch = self
                    .schedule
                    .epoch_of(keep.height)
                    .ok_or(Error::Corrupt("a stored block is below activation"))?;
                (i64::from(keep.height), epoch)
            }
        };
        tx.execute(
            "DELETE FROM span_boundaries WHERE decided_height > ?1",
            [above],
        )?;
        tx.execute(
            "DELETE FROM memo_entries WHERE epoch >= ?1 AND height > ?2",
            params![from_epoch, above],
        )?;
        tx.execute(
            "DELETE FROM first_contact_entries WHERE epoch >= ?1 AND height > ?2",
            params![from_epoch, above],
        )?;
        tx.execute("DELETE FROM blocks WHERE height > ?1", [above])?;
        let tip = read_tip(&tx)?;
        tx.commit()?;
        self.tip = tip;
        Ok(())
    }

    /// The newest stored block at or below `height`. Every stored block is a
    /// checkpoint.
    pub fn checkpoint_at_or_below(&self, height: u32) -> Result<Option<BlockId>, Error> {
        Ok(self
            .db
            .query_row(
                "SELECT height, hash FROM blocks WHERE height <= ?1 ORDER BY height DESC LIMIT 1",
                [height],
                block_id,
            )
            .optional()?)
    }

    /// The hash of the stored block at `height`, if any.
    pub fn block_hash(&self, height: u32) -> Result<Option<Hash>, Error> {
        read_hash(&self.db, height)
    }

    /// The epochs with at least one stored block, ascending.
    pub fn epochs(&self) -> Result<Vec<u32>, Error> {
        let mut query = self
            .db
            .prepare("SELECT DISTINCT epoch FROM blocks ORDER BY epoch")?;
        let epochs = query
            .query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        Ok(epochs)
    }

    /// What is stored of `epoch`, or `None` when no block of it is stored.
    /// The stored entries are checked against their blocks' counts
    /// ([`Error::Corrupt`] otherwise).
    pub fn epoch_summary(&self, epoch: u32) -> Result<Option<EpochSummary>, Error> {
        // One read transaction, so the counts describe one state.
        let tx = self.db.unchecked_transaction()?;
        let row: (Option<u32>, Option<u32>, Counts) = tx.query_row(
            "SELECT MIN(height), MAX(height),
                    COALESCE(SUM(envelopes_valid), 0), COALESCE(SUM(envelopes_invalid), 0),
                    COALESCE(SUM(memo_entries), 0), COALESCE(SUM(first_contact_entries), 0),
                    COALESCE(SUM(state_entries), 0)
             FROM blocks WHERE epoch = ?1",
            [epoch],
            |r| Ok((r.get(0)?, r.get(1)?, counts_at(r, 2)?)),
        )?;
        let (Some(first_height), Some(last_height), counts) = row else {
            return Ok(None);
        };
        let memo: u64 = tx.query_row(
            "SELECT COUNT(*) FROM memo_entries WHERE epoch = ?1",
            [epoch],
            |r| r.get(0),
        )?;
        let first_contact: u64 = tx.query_row(
            "SELECT COUNT(*) FROM first_contact_entries WHERE epoch = ?1",
            [epoch],
            |r| r.get(0),
        )?;
        tx.commit()?;
        if memo != counts.memo_entries || first_contact != counts.first_contact_entries {
            return Err(Error::Corrupt("entries differ from their blocks' counts"));
        }
        Ok(Some(EpochSummary {
            epoch,
            first_height,
            last_height,
            counts,
        }))
    }

    /// The counts over every stored block: the source of the memo metrics
    /// `envelopes_invalid_total` and `state_entries_total` (spec 11.8) and of
    /// entry totals per kind. They describe the stored chain, so a rewind
    /// lowers them.
    pub fn totals(&self) -> Result<Counts, Error> {
        Ok(self.db.query_row(
            "SELECT COALESCE(SUM(envelopes_valid), 0), COALESCE(SUM(envelopes_invalid), 0),
                    COALESCE(SUM(memo_entries), 0), COALESCE(SUM(first_contact_entries), 0),
                    COALESCE(SUM(state_entries), 0)
             FROM blocks",
            [],
            |r| counts_at(r, 0),
        )?)
    }

    /// Non-empty memos of the stored blocks that were not valid envelopes
    /// (`tachyon_memo_envelopes_invalid_total`, spec 5.2 and 11.8).
    pub fn envelopes_invalid_total(&self) -> Result<u64, Error> {
        Ok(self.totals()?.envelopes_invalid)
    }

    /// Wallet-state entries of the stored blocks
    /// (`tachyon_memo_state_entries_total`, spec 11.8).
    pub fn state_entries_total(&self) -> Result<u64, Error> {
        Ok(self.totals()?.state_entries)
    }

    /// The follow-up entries of memo scope `epoch` (spec 7.1), in ordinal
    /// order, every entry of a repeated tag included (spec 5.3). Empty when no
    /// block of the epoch is stored.
    pub fn memo_entries(&self, epoch: u32) -> Result<Vec<MemoRecord>, Error> {
        let mut query = self.db.prepare(
            "SELECT height, tx_index, entry_index, tag, ciphertext FROM memo_entries
             WHERE epoch = ?1 ORDER BY epoch, height, tx_index, entry_index",
        )?;
        let records = query
            .query_map([epoch], |r| {
                Ok(MemoRecord {
                    ordinal: ordinal_at(r, 0)?,
                    tag: r.get(3)?,
                    ciphertext: r.get(4)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(records)
    }

    /// The first-contact entries of the span whose `scope_id` (first epoch) is
    /// `scope_id` (spec 7.2), in ordinal order, every entry of a repeated tag
    /// included. `scope_id` must be a closed span's first epoch or the open
    /// span's ([`Error::UnknownSpan`]); the open span's entries are those
    /// stored so far, none before its first block is stored.
    pub fn first_contact_entries(&self, scope_id: u32) -> Result<Vec<FirstContactRecord>, Error> {
        let tx = self.db.unchecked_transaction()?;
        let closed: Option<u32> = tx
            .query_row(
                "SELECT last_epoch FROM span_boundaries WHERE first_epoch = ?1",
                [scope_id],
                |r| r.get(0),
            )
            .optional()?;
        let last_epoch = match closed {
            Some(last) => last,
            None if open_span_start(&tx)?.1 == scope_id => u32::MAX,
            None => return Err(Error::UnknownSpan(scope_id)),
        };
        let mut query = tx.prepare(
            "SELECT height, tx_index, entry_index, tag, kem_ciphertext, ciphertext
             FROM first_contact_entries WHERE epoch BETWEEN ?1 AND ?2
             ORDER BY epoch, height, tx_index, entry_index",
        )?;
        let records = query
            .query_map(params![scope_id, last_epoch], |r| {
                let kem: Vec<u8> = r.get(4)?;
                Ok((ordinal_at(r, 0)?, r.get(3)?, kem, r.get(5)?))
            })?
            .map(|row| {
                let (ordinal, tag, kem, ciphertext) = row?;
                let kem_ciphertext = Box::<[u8]>::from(kem)
                    .try_into()
                    .map_err(|_| Error::Corrupt("kem ciphertext length"))?;
                Ok(FirstContactRecord {
                    ordinal,
                    tag,
                    kem_ciphertext,
                    ciphertext,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        drop(query);
        tx.commit()?;
        Ok(records)
    }

    /// The entries of `kind` in one scope, in ordinal order (spec 7.3
    /// `entries(kind, scope)`): for [`Kind::Memo`] `scope_id` is the epoch,
    /// for [`Kind::FirstContact`] the span's first epoch. See
    /// [`Store::memo_entries`] and [`Store::first_contact_entries`].
    pub fn entries(
        &self,
        kind: Kind,
        scope_id: u32,
    ) -> Result<impl ExactSizeIterator<Item = Record> + use<>, Error> {
        let records: Vec<Record> = match kind {
            Kind::Memo => self
                .memo_entries(scope_id)?
                .into_iter()
                .map(Record::Memo)
                .collect(),
            Kind::FirstContact => self
                .first_contact_entries(scope_id)?
                .into_iter()
                .map(Record::FirstContact)
                .collect(),
        };
        Ok(records.into_iter())
    }

    /// Every first-contact span with at least one stored block, in order:
    /// the closed spans, then the open span once a block of its first epoch
    /// is stored (spec 7.2). The stored boundaries are checked against the
    /// span rule and the stored counts ([`Error::Corrupt`] otherwise).
    pub fn spans(&self) -> Result<Vec<Span>, Error> {
        let tx = self.db.unchecked_transaction()?;
        let per_epoch: BTreeMap<u32, u64> = {
            let mut query = tx.prepare(
                "SELECT epoch, SUM(first_contact_entries) FROM blocks GROUP BY epoch ORDER BY epoch",
            )?;
            query
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<Result<_, _>>()?
        };
        type BoundaryRow = (u32, u32, u32, u64, u32, Option<Hash>);
        let boundaries: Vec<BoundaryRow> = {
            let mut query = tx.prepare(
                "SELECT s.span_index, s.first_epoch, s.last_epoch, s.entries, s.decided_height,
                        b.hash
                 FROM span_boundaries s LEFT JOIN blocks b ON b.height = s.decided_height
                 ORDER BY s.span_index",
            )?;
            query
                .query_map([], |r| {
                    let hash: Option<[u8; 32]> = r.get(5)?;
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        hash.map(Hash::from_protocol_bytes),
                    ))
                })?
                .collect::<Result<_, _>>()?
        };
        let tip = read_tip(&tx)?;
        tx.commit()?;

        let epoch_entries = |epoch: u32| per_epoch.get(&epoch).copied().unwrap_or(0);
        let mut spans = Vec::with_capacity(boundaries.len() + 1);
        let mut next_first = Some(0u32);
        for (position, (index, first, last, entries, decided, hash)) in
            boundaries.into_iter().enumerate()
        {
            if Some(first) != next_first || index as usize != position {
                return Err(Error::Corrupt("span boundaries are not consecutive"));
            }
            let hash = hash.ok_or(Error::Corrupt("a span boundary's block is missing"))?;
            if self.schedule.last_height(last) != Some(decided) {
                return Err(Error::Corrupt(
                    "a span boundary is not at its epoch's last block",
                ));
            }
            // The boundary must be the first epoch boundary where the rule
            // holds, over the stored counts.
            let mut sum = 0u64;
            for epoch in first..=last {
                sum += epoch_entries(epoch);
                let closing = closes(sum, u64::from(epoch - first) + 1);
                if closing != (epoch == last) {
                    return Err(Error::Corrupt("a span boundary contradicts the span rule"));
                }
            }
            if sum != entries {
                return Err(Error::Corrupt("a span's entries differ from its blocks"));
            }
            spans.push(Span {
                index,
                first_epoch: first,
                last_epoch: Some(last),
                entries,
                closed_by: Some(BlockId {
                    height: decided,
                    hash,
                }),
                last_height: decided,
            });
            next_first = last.checked_add(1);
        }
        let (Some(tip), Some(first)) = (tip, next_first) else {
            return Ok(spans);
        };
        let tip_epoch = self
            .schedule
            .epoch_of(tip.height)
            .ok_or(Error::Corrupt("a stored block is below activation"))?;
        if tip_epoch < first {
            return Ok(spans);
        }
        // No complete epoch of the open span may satisfy the rule: its
        // boundary would be missing.
        let mut entries = 0u64;
        for epoch in first..=tip_epoch {
            entries += epoch_entries(epoch);
            let complete = self
                .schedule
                .last_height(epoch)
                .is_some_and(|last| last <= tip.height);
            if complete && closes(entries, u64::from(epoch - first) + 1) {
                return Err(Error::Corrupt("a span boundary is missing"));
            }
        }
        spans.push(Span {
            index: u32::try_from(spans.len()).map_err(|_| Error::Corrupt("span index"))?,
            first_epoch: first,
            last_epoch: None,
            entries,
            closed_by: None,
            last_height: tip.height,
        });
        Ok(spans)
    }
}

/// Checks a block's entries before anything is written: each ordinal names
/// the block, each kind is in strictly ascending ordinal order, no ordinal is
/// shared between kinds, and no key is all zero.
fn check_entries(block: &IndexedBlock) -> Result<(), Error> {
    let height = block.id.height;
    let fault = |kind, ordinal, fault| Error::Entry {
        height,
        kind,
        ordinal,
        fault,
    };
    let memo = block.memo.iter().map(|r| (Kind::Memo, r.ordinal, &r.tag));
    let first_contact = block
        .first_contact
        .iter()
        .map(|r| (Kind::FirstContact, r.ordinal, &r.tag));
    let mut ordinals = Vec::with_capacity(block.memo.len() + block.first_contact.len());
    let mut previous: Option<(Kind, Ordinal)> = None;
    for (kind, ordinal, tag) in memo.chain(first_contact) {
        if ordinal.height != height {
            return Err(fault(kind, ordinal, EntryFault::ForeignHeight));
        }
        if block.epoch.is_none() {
            return Err(fault(kind, ordinal, EntryFault::BelowActivation));
        }
        if previous.is_some_and(|(k, p)| k == kind && ordinal <= p) {
            return Err(fault(kind, ordinal, EntryFault::Order));
        }
        if key16(tag) == [0; 16] {
            return Err(fault(kind, ordinal, EntryFault::ZeroKey));
        }
        previous = Some((kind, ordinal));
        ordinals.push((ordinal, kind));
    }
    // Stable, so of two entries sharing an ordinal the first-contact one is
    // reported.
    ordinals.sort_by_key(|(ordinal, _)| *ordinal);
    if let Some(pair) = ordinals.windows(2).find(|w| w[0].0 == w[1].0) {
        return Err(fault(pair[1].1, pair[1].0, EntryFault::Order));
    }
    Ok(())
}

/// Closes the open span at `epoch` if the span rule holds there (spec 7.2).
/// Runs inside the append of `height`, the last block of `epoch`, after the
/// block's row is written.
fn decide_span(tx: &Transaction<'_>, epoch: u32, height: u32) -> Result<(), Error> {
    let (index, first) = open_span_start(tx)?;
    if first > epoch {
        return Err(Error::Corrupt("the open span starts after the tip"));
    }
    let entries: u64 = tx.query_row(
        "SELECT COALESCE(SUM(first_contact_entries), 0) FROM blocks WHERE epoch BETWEEN ?1 AND ?2",
        params![first, epoch],
        |r| r.get(0),
    )?;
    if closes(entries, u64::from(epoch - first) + 1) {
        tx.execute(
            "INSERT INTO span_boundaries (span_index, first_epoch, last_epoch, entries, decided_height)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![index, first, epoch, entries, height],
        )?;
    }
    Ok(())
}

/// The open span's index and first epoch: the span after the last boundary,
/// or span 0 at epoch 0.
fn open_span_start(db: &Connection) -> Result<(u32, u32), Error> {
    let last: Option<(u32, u32)> = db
        .query_row(
            "SELECT span_index, last_epoch FROM span_boundaries ORDER BY span_index DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    match last {
        None => Ok((0, 0)),
        Some((index, last)) => Ok((
            index
                .checked_add(1)
                .ok_or(Error::Corrupt("span index overflow"))?,
            last.checked_add(1)
                .ok_or(Error::Corrupt("span epoch overflow"))?,
        )),
    }
}

/// An [`Ordinal`] from `(height, tx_index, entry_index)` columns at `at`.
fn ordinal_at(r: &rusqlite::Row<'_>, at: usize) -> rusqlite::Result<Ordinal> {
    Ok(Ordinal {
        height: r.get(at)?,
        tx_index: r.get(at + 1)?,
        entry_index: r.get(at + 2)?,
    })
}

/// [`Counts`] from five columns at `at`, in field order.
fn counts_at(r: &rusqlite::Row<'_>, at: usize) -> rusqlite::Result<Counts> {
    Ok(Counts {
        envelopes_valid: r.get(at)?,
        envelopes_invalid: r.get(at + 1)?,
        memo_entries: r.get(at + 2)?,
        first_contact_entries: r.get(at + 3)?,
        state_entries: r.get(at + 4)?,
    })
}

/// A [`BlockId`] from a `(height, hash)` row.
fn block_id(r: &rusqlite::Row<'_>) -> rusqlite::Result<BlockId> {
    Ok(BlockId {
        height: r.get(0)?,
        hash: Hash::from_protocol_bytes(r.get(1)?),
    })
}

/// The newest stored block.
fn read_tip(db: &Connection) -> Result<Option<BlockId>, Error> {
    Ok(db
        .query_row(
            "SELECT height, hash FROM blocks ORDER BY height DESC LIMIT 1",
            [],
            block_id,
        )
        .optional()?)
}

/// The hash of the stored block at `height`.
fn read_hash(db: &Connection, height: u32) -> Result<Option<Hash>, Error> {
    Ok(db
        .query_row("SELECT hash FROM blocks WHERE height = ?1", [height], |r| {
            Ok(Hash::from_protocol_bytes(r.get(0)?))
        })
        .optional()?)
}
