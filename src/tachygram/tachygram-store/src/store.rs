//! The SQLite journal `tachygram-v1.sqlite` (spec section 8.5).
use crate::block::{IndexedBlock, StampOrdinal};
use crate::error::{Error, IntegrityFault};
use rusqlite::{Connection, ErrorCode, OptionalExtension, TransactionBehavior, params};
use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;
use tachyon_chain::{BlockId, EpochSchedule, Hash, NetworkIdentity};

/// The store's file name in a data directory.
pub const STORE_FILE: &str = "tachygram-v1.sqlite";

/// The schema version, kept in `PRAGMA user_version`.
pub const SCHEMA_VERSION: u32 = 1;

/// How long a write waits for another connection's lock.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// The schema. Every table is keyed for the reads the publisher makes:
///
/// - `blocks` by height, with `(epoch, height)` for epoch summaries;
/// - `stamps` by `(epoch, ordinal)`, the stamp order, with `(height, tx_index)`
///   for the cascade from `blocks`;
/// - `tachygrams` by `(epoch, tg)`, the record order of spec 6.2 and the
///   uniqueness of spec 8.3, with `(epoch, stamp_ordinal, position)` for stamp
///   order (spec 8.6) and for rewinds.
///
/// `stamps` rows reference their block and cascade from it. `tachygrams` rows
/// carry no foreign key: with one, SQLite looks up the parent stamp for every
/// deleted row, which made a rewind over 100,000 tachygrams take a second
/// instead of a tenth of one. [`Store::rewind`] deletes them explicitly, and
/// [`Store::epoch_summary`] checks that they match their stamps' counts.
const SCHEMA: &str = "
CREATE TABLE binding (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    chain TEXT NOT NULL,
    genesis_hash BLOB NOT NULL CHECK (length(genesis_hash) = 32),
    activation_height INTEGER NOT NULL CHECK (activation_height BETWEEN 0 AND 4294967295),
    epoch_length INTEGER NOT NULL CHECK (epoch_length BETWEEN 1 AND 4294967295)
);
CREATE TABLE blocks (
    height INTEGER PRIMARY KEY CHECK (height BETWEEN 0 AND 4294967295),
    hash BLOB NOT NULL CHECK (length(hash) = 32),
    epoch INTEGER CHECK (epoch BETWEEN 0 AND 4294967295)
);
CREATE INDEX blocks_epoch ON blocks (epoch, height);
CREATE TABLE stamps (
    epoch INTEGER NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 4294967295),
    height INTEGER NOT NULL REFERENCES blocks (height) ON DELETE CASCADE,
    tx_index INTEGER NOT NULL CHECK (tx_index BETWEEN 0 AND 4294967295),
    txid BLOB NOT NULL CHECK (length(txid) = 32),
    tachygram_count INTEGER NOT NULL CHECK (tachygram_count BETWEEN 0 AND 4294967295),
    PRIMARY KEY (epoch, ordinal),
    UNIQUE (height, tx_index)
) WITHOUT ROWID;
CREATE TABLE tachygrams (
    epoch INTEGER NOT NULL,
    tg BLOB NOT NULL CHECK (length(tg) = 32
        AND tg != x'0000000000000000000000000000000000000000000000000000000000000000'),
    height INTEGER NOT NULL,
    stamp_ordinal INTEGER NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (epoch, tg)
) WITHOUT ROWID;
CREATE UNIQUE INDEX tachygrams_stamp_order ON tachygrams (epoch, stamp_ordinal, position);
";

/// The network and epoch schedule a database is bound to. Every stored epoch
/// and ordinal depends on all four fields, so a store is never reused across a
/// change in any of them.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Binding {
    /// `getblockchaininfo.chain`, for example `"test"`.
    pub chain: String,
    /// Block 0's hash, protocol order.
    pub genesis_hash: Hash,
    /// The `NuTachyon` activation height: pool height zero, the start of
    /// epoch zero.
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

/// One stored proof stamp (spec 8.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StampRecord {
    pub ordinal: StampOrdinal,
    /// The height of the block carrying the stamp.
    pub height: u32,
    /// The carrying transaction's index among all transactions of the block.
    pub tx_index: u32,
    /// The carrying transaction's id, protocol order.
    pub txid: Hash,
    /// How many tachygrams the stamp lists, as the data says.
    pub tachygram_count: u32,
}

/// The fields of one 40-byte tachygram record (spec 8.1):
/// `tg || blk_height (u32 LE) || stamp_ordinal (u32 LE)`. Encoding is the
/// protocol crate's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TachygramRecord {
    pub tachygram: [u8; 32],
    /// The height of the block containing the creation stamp.
    pub blk_height: u32,
    pub stamp_ordinal: StampOrdinal,
}

/// What is stored of one epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochSummary {
    pub epoch: u32,
    /// Tachygram records stored in the epoch.
    pub entries: u64,
    /// Proof stamps stored in the epoch.
    pub stamps: u32,
    /// The first stored block height in the epoch. It is the epoch's first
    /// height unless the store began inside the epoch.
    pub first_height: u32,
    /// The last stored block height in the epoch.
    pub last_height: u32,
}

/// The tachygram journal: a contiguous run of blocks with their proof stamps
/// and tachygrams, bound to one network.
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
    /// A new file is bound to `binding`; an existing one must have been created
    /// with an equal binding, or [`Error::BindingMismatch`] is returned and
    /// nothing changes. The database runs in WAL mode with
    /// `synchronous=FULL`, as `receiver-store` does, so every committed append
    /// or rewind survives a crash.
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
                    "INSERT INTO binding VALUES (1, ?1, ?2, ?3, ?4)",
                    params![
                        binding.chain,
                        binding.genesis_hash.protocol_bytes(),
                        binding.activation_height,
                        binding.epoch_length
                    ],
                )?;
                tx.execute_batch(&format!("PRAGMA user_version={SCHEMA_VERSION};"))?;
            }
            SCHEMA_VERSION => {}
            other => return Err(Error::UnsupportedSchema(other)),
        }
        let stored = tx.query_row(
            "SELECT chain, genesis_hash, activation_height, epoch_length FROM binding WHERE id = 1",
            [],
            |r| {
                Ok(Binding {
                    chain: r.get(0)?,
                    genesis_hash: Hash::from_protocol_bytes(r.get(1)?),
                    activation_height: r.get(2)?,
                    epoch_length: r.get(3)?,
                })
            },
        )?;
        if stored != binding {
            return Err(Error::BindingMismatch {
                stored: Box::new(stored),
                requested: Box::new(binding),
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
    /// - The block must follow the tip: `height == tip.height + 1` and
    ///   `prev == tip.hash` ([`Error::HeightGap`], [`Error::ParentMismatch`]).
    ///   The first block of an empty store may have any height and parent; the
    ///   publisher starts at the activation height, which makes every epoch
    ///   complete from its first block. A store that starts inside an epoch
    ///   numbers that epoch's stamps from its first stored block, which is not
    ///   the epoch's canonical numbering.
    /// - `block.epoch` must be the epoch the store's schedule gives the height
    ///   ([`Error::EpochMismatch`]), and its stamps must be in strictly
    ///   ascending transaction order ([`Error::StampOrder`]).
    /// - Each proof stamp is given the next `stamp_ordinal` of its epoch:
    ///   zero for the epoch's first stored stamp, in block order then
    ///   transaction order (spec 8.1).
    /// - A tachygram already stored in the same epoch, repeated within the
    ///   block, or all zero, and any proof stamp below the activation height,
    ///   is an [`IntegrityFault`] (spec 8.3, 6.2). The same tachygram in a
    ///   different epoch is accepted: consensus forbids reuse in the current
    ///   and previous epoch, but spec 8.3 makes only a duplicate within one
    ///   epoch the journal's fault, and each epoch is its own scope (spec 8.2).
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
        check_stamps(block)?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(tip) = read_tip(&tx)? {
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
        tx.execute(
            "INSERT INTO blocks (height, hash, epoch) VALUES (?1, ?2, ?3)",
            params![height, block.id.hash.protocol_bytes(), block.epoch],
        )?;
        if let Some(epoch) = block.epoch {
            let last: Option<u32> = tx.query_row(
                "SELECT MAX(ordinal) FROM stamps WHERE epoch = ?1",
                [epoch],
                |r| r.get(0),
            )?;
            let mut ordinal = match last {
                None => 0,
                Some(last) => last
                    .checked_add(1)
                    .ok_or(Error::Corrupt("stamp ordinal overflow"))?,
            };
            let mut insert_stamp = tx.prepare(
                "INSERT INTO stamps (epoch, ordinal, height, tx_index, txid, tachygram_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            let mut insert_tachygram = tx.prepare(
                "INSERT INTO tachygrams (epoch, tg, height, stamp_ordinal, position)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for stamp in &block.stamps {
                let count = u32::try_from(stamp.tachygrams.len())
                    .map_err(|_| Error::Corrupt("tachygram count beyond u32"))?;
                insert_stamp.execute(params![
                    epoch,
                    ordinal,
                    height,
                    stamp.tx_index,
                    stamp.txid.protocol_bytes(),
                    count
                ])?;
                for (position, tachygram) in stamp.tachygrams.iter().enumerate() {
                    let inserted = insert_tachygram.execute(params![
                        epoch,
                        tachygram,
                        height,
                        ordinal,
                        position as u64
                    ]);
                    match inserted {
                        Ok(_) => {}
                        Err(error) if is_primary_key_violation(&error) => {
                            let (first_height, first_ordinal) = tx.query_row(
                                "SELECT height, stamp_ordinal FROM tachygrams
                                 WHERE epoch = ?1 AND tg = ?2",
                                params![epoch, tachygram],
                                |r| Ok((r.get(0)?, r.get(1)?)),
                            )?;
                            return Err(IntegrityFault::DuplicateInEpoch {
                                epoch,
                                height,
                                tachygram: *tachygram,
                                first_height,
                                first_stamp_ordinal: StampOrdinal(first_ordinal),
                            }
                            .into());
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or(Error::Corrupt("stamp ordinal overflow"))?;
            }
        }
        tx.commit()?;
        self.tip = Some(block.id);
        Ok(())
    }

    /// Removes every block above `keep`, with its stamps and tachygrams; with
    /// `None`, removes every block. `keep` must be a stored block, height and
    /// hash ([`Error::UnknownCheckpoint`]). Atomic and durable, like
    /// [`Store::append`]. Re-appending the same blocks afterwards assigns the
    /// same ordinals, because ordinals depend only on the stored blocks below.
    pub fn rewind(&mut self, keep: Option<&BlockId>) -> Result<(), Error> {
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Every height is above -1, so `None` removes everything.
        let above = match keep {
            None => -1,
            Some(keep) => {
                if read_hash(&tx, keep.height)? != Some(keep.hash) {
                    return Err(Error::UnknownCheckpoint(*keep));
                }
                i64::from(keep.height)
            }
        };
        // Ordinals grow with height within an epoch, so the removed stamps of
        // each epoch are those from its first removed ordinal on, and their
        // tachygrams are one range of the stamp-order index. Stamps cascade
        // from their blocks; they are deleted explicitly all the same.
        let firsts: Vec<(u32, u32)> = {
            let mut query = tx.prepare(
                "SELECT epoch, MIN(ordinal) FROM stamps WHERE height > ?1 GROUP BY epoch",
            )?;
            query
                .query_map([above], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<Result<_, _>>()?
        };
        for (epoch, first) in firsts {
            tx.execute(
                "DELETE FROM tachygrams WHERE epoch = ?1 AND stamp_ordinal >= ?2",
                params![epoch, first],
            )?;
        }
        tx.execute("DELETE FROM stamps WHERE height > ?1", [above])?;
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

    /// The epochs with at least one stored block, ascending. Blocks below the
    /// activation height are in no epoch.
    pub fn epochs(&self) -> Result<Vec<u32>, Error> {
        let mut query = self
            .db
            .prepare("SELECT DISTINCT epoch FROM blocks WHERE epoch IS NOT NULL ORDER BY epoch")?;
        let epochs = query
            .query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        Ok(epochs)
    }

    /// What is stored of `epoch`, or `None` when no block of it is stored.
    pub fn epoch_summary(&self, epoch: u32) -> Result<Option<EpochSummary>, Error> {
        // One read transaction, so the three counts describe one state.
        let tx = self.db.unchecked_transaction()?;
        let heights: (Option<u32>, Option<u32>) = tx.query_row(
            "SELECT MIN(height), MAX(height) FROM blocks WHERE epoch = ?1",
            [epoch],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let (Some(first_height), Some(last_height)) = heights else {
            return Ok(None);
        };
        let (stamps, listed): (u32, u64) = tx.query_row(
            "SELECT COUNT(*), COALESCE(SUM(tachygram_count), 0) FROM stamps WHERE epoch = ?1",
            [epoch],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let entries: u64 = tx.query_row(
            "SELECT COUNT(*) FROM tachygrams WHERE epoch = ?1",
            [epoch],
            |r| r.get(0),
        )?;
        tx.commit()?;
        if entries != listed {
            return Err(Error::Corrupt(
                "tachygrams differ from their stamps' counts",
            ));
        }
        Ok(Some(EpochSummary {
            epoch,
            entries,
            stamps,
            first_height,
            last_height,
        }))
    }

    /// The proof stamps stored in `epoch`, in ordinal order (spec 8.5).
    pub fn stamps(&self, epoch: u32) -> Result<Vec<StampRecord>, Error> {
        let mut query = self.db.prepare(
            "SELECT ordinal, height, tx_index, txid, tachygram_count FROM stamps
             WHERE epoch = ?1 ORDER BY ordinal",
        )?;
        let stamps = query
            .query_map([epoch], |r| {
                Ok(StampRecord {
                    ordinal: StampOrdinal(r.get(0)?),
                    height: r.get(1)?,
                    tx_index: r.get(2)?,
                    txid: Hash::from_protocol_bytes(r.get(3)?),
                    tachygram_count: r.get(4)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(stamps)
    }

    /// The record fields of every tachygram stored in `epoch`, ascending by
    /// tachygram bytes, the order entries take in a row (spec 6.2). Tachygrams
    /// are unique within an epoch, so the order is strict.
    pub fn epoch_records(&self, epoch: u32) -> Result<Vec<TachygramRecord>, Error> {
        let mut query = self.db.prepare(
            "SELECT tg, height, stamp_ordinal FROM tachygrams WHERE epoch = ?1 ORDER BY tg",
        )?;
        let records = query
            .query_map([epoch], |r| {
                Ok(TachygramRecord {
                    tachygram: r.get(0)?,
                    blk_height: r.get(1)?,
                    stamp_ordinal: StampOrdinal(r.get(2)?),
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(records)
    }

    /// Every tachygram stored in `epoch` with its stamp's ordinal, in stamp
    /// order and, within a stamp, in the stamp's own order: the input an OSS
    /// routes into buckets (spec 8.6).
    pub fn epoch_tachygrams(
        &self,
        epoch: u32,
    ) -> Result<impl ExactSizeIterator<Item = (StampOrdinal, [u8; 32])> + use<>, Error> {
        let mut query = self.db.prepare(
            "SELECT stamp_ordinal, tg FROM tachygrams WHERE epoch = ?1
             ORDER BY stamp_ordinal, position",
        )?;
        let tachygrams: Vec<(StampOrdinal, [u8; 32])> = query
            .query_map([epoch], |r| Ok((StampOrdinal(r.get(0)?), r.get(1)?)))?
            .collect::<Result<_, _>>()?;
        Ok(tachygrams.into_iter())
    }
}

/// Checks a block's stamps before anything is written: transaction order,
/// activation, all-zero tachygrams and tachygrams repeated within the block.
fn check_stamps(block: &IndexedBlock) -> Result<(), Error> {
    let height = block.id.height;
    let mut previous: Option<u32> = None;
    let mut seen = HashSet::with_capacity(block.tachygram_count());
    for stamp in &block.stamps {
        if previous.is_some_and(|previous| stamp.tx_index <= previous) {
            return Err(Error::StampOrder {
                height,
                tx_index: stamp.tx_index,
            });
        }
        previous = Some(stamp.tx_index);
        if block.epoch.is_none() {
            return Err(IntegrityFault::StampBelowActivation {
                height,
                tx_index: stamp.tx_index,
            }
            .into());
        }
        for tachygram in &stamp.tachygrams {
            if *tachygram == [0; 32] {
                return Err(IntegrityFault::ZeroTachygram {
                    height,
                    tx_index: stamp.tx_index,
                }
                .into());
            }
            if !seen.insert(tachygram) {
                return Err(IntegrityFault::DuplicateInBlock {
                    height,
                    tachygram: *tachygram,
                }
                .into());
            }
        }
    }
    Ok(())
}

/// Whether `error` is a primary-key violation: in `tachygrams`, a tachygram
/// already stored in the epoch.
fn is_primary_key_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation
                && failure.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
    )
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
