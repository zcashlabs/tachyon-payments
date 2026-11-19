//! The SQLite registration log `registry-v1.sqlite` (spec section 9.4).
use crate::error::Error;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;
use tachyon_pir_protocol::BlockHash;
use tachyon_pir_protocol::constants::{ADDR_BYTES, REGISTRY_CHUNK_ENTRIES};
use tachyon_pir_protocol::registry::{Address, Registration as WireRegistration};

/// The store's file name in a data directory.
pub const STORE_FILE: &str = "registry-v1.sqlite";

/// The schema version, kept in `PRAGMA user_version`.
pub const SCHEMA_VERSION: u32 = 1;

/// How long a write or checkpoint waits for another connection's lock.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// The schema.
///
/// - `binding` holds the network identity and the chunk size the log was
///   created under (spec 9.2 Identity).
/// - `registrations` is the log: `sequence` is the rowid, so a chunk is one
///   clustered range scan and the log sequence is `MAX(sequence) + 1`;
///   `registrations_digest` makes each address appear once (spec 9.4).
/// - The triggers make the file itself append-only and gap-free, whoever
///   writes to it: an update or delete is aborted, and so is an insert at any
///   sequence but the next.
const SCHEMA: &str = "
CREATE TABLE binding (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    chain TEXT NOT NULL,
    genesis_hash BLOB NOT NULL CHECK (typeof(genesis_hash) = 'blob' AND length(genesis_hash) = 32),
    chunk_entries INTEGER NOT NULL CHECK (chunk_entries > 0)
);
CREATE TABLE registrations (
    sequence INTEGER PRIMARY KEY CHECK (sequence >= 0),
    digest BLOB NOT NULL CHECK (typeof(digest) = 'blob' AND length(digest) = 32
        AND substr(digest, 1, 16) != zeroblob(16)),
    addr BLOB NOT NULL CHECK (typeof(addr) = 'blob' AND length(addr) = 1216)
);
CREATE UNIQUE INDEX registrations_digest ON registrations (digest);
CREATE TRIGGER registrations_gap_free BEFORE INSERT ON registrations
WHEN NEW.sequence IS NOT (SELECT COALESCE(MAX(sequence) + 1, 0) FROM registrations)
BEGIN SELECT RAISE(ABORT, 'registry log sequences are gap-free'); END;
CREATE TRIGGER registrations_never_changed BEFORE UPDATE ON registrations
BEGIN SELECT RAISE(ABORT, 'registry log entries are never changed'); END;
CREATE TRIGGER registrations_never_deleted BEFORE DELETE ON registrations
BEGIN SELECT RAISE(ABORT, 'registry log entries are never deleted'); END;
";

const _: () = assert!(ADDR_BYTES == 1216);

/// The network identity a log is bound to (spec 9.2 Identity): the registry
/// follows no blocks, but its map's `network` and `genesis_hash` and its
/// placement hash use this identity, so a log is never reused across networks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Binding {
    /// `getblockchaininfo.chain`, for example `"test"`.
    pub chain: String,
    /// Block 0's hash, protocol order.
    pub genesis_hash: BlockHash,
}

/// Where an address sits in the log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Registration {
    /// The entry's log sequence, counting from 0; the response's
    /// `visible_after_sequence` (spec 9.3).
    pub sequence: u64,
    /// `sequence / REGISTRY_CHUNK_ENTRIES`; never changes (spec 9.2).
    pub chunk_id: u64,
    /// The address digest (spec 9.1).
    pub digest: [u8; 32],
    /// Whether this call appended the entry. False when the address was
    /// already in the log, and always false from [`Store::lookup`].
    pub created: bool,
}

impl Registration {
    fn new(sequence: u64, digest: [u8; 32], created: bool) -> Self {
        Self {
            sequence,
            chunk_id: chunk_of(sequence),
            digest,
            created,
        }
    }

    /// The `200` body of `POST /registry/v1/register` (spec 9.3).
    pub fn response(&self) -> WireRegistration {
        WireRegistration {
            chunk_id: self.chunk_id,
            digest: self.digest.into(),
            created: self.created,
            visible_after_sequence: self.sequence,
        }
    }
}

/// The chunk holding log sequence `sequence` (spec 9.4).
pub const fn chunk_of(sequence: u64) -> u64 {
    sequence / REGISTRY_CHUNK_ENTRIES
}

/// The first log sequence of chunk `chunk_id`, saturating at `u64::MAX`.
pub const fn chunk_start(chunk_id: u64) -> u64 {
    chunk_id.saturating_mul(REGISTRY_CHUNK_ENTRIES)
}

/// The registration log: an append-only, gap-free sequence of addresses bound
/// to one network.
///
/// `Store` is `Send + Sync`; share it behind an `Arc`. It holds two
/// connections, each behind its own mutex:
///
/// - the **writer**, which every registration and checkpoint locks for its
///   whole `IMMEDIATE` transaction. Writes are therefore serialised within
///   the process, and the next sequence is read inside the transaction that
///   inserts it, so a second process writing the same file waits on SQLite's
///   lock (up to five seconds) and still cannot produce a duplicate or a gap;
/// - a **reader** (`query_only`), used by every read. In WAL mode it reads the
///   last committed state without blocking or being blocked by the writer, so
///   the publisher's chunk reads do not delay registrations.
///
/// Methods block on SQLite and on fsync; call them from a blocking context
/// (for example `tokio::task::spawn_blocking`) in an async server.
pub struct Store {
    writer: Mutex<Connection>,
    reader: Mutex<Connection>,
    binding: Binding,
}

impl Store {
    /// Opens or creates the log at `path`, bound to `binding`.
    ///
    /// A new file records `binding` and [`REGISTRY_CHUNK_ENTRIES`]. An existing
    /// file must have been created with an equal binding
    /// ([`Error::BindingMismatch`]) and the same chunk size
    /// ([`Error::ChunkRuleMismatch`]); a SQLite file holding anything else is
    /// [`Error::NotAStore`], and another schema version
    /// [`Error::UnsupportedSchema`]. A refused open changes nothing. The
    /// database runs in WAL mode with `synchronous=FULL`, so every committed
    /// registration survives a crash.
    pub fn open(path: impl AsRef<Path>, binding: Binding) -> Result<Self, Error> {
        let path = path.as_ref();
        let mut writer = Connection::open(path)?;
        writer.busy_timeout(BUSY_TIMEOUT)?;
        writer.execute_batch("PRAGMA synchronous=FULL;")?;
        // Refuse a foreign file before switching it to WAL, which would
        // change it. The check is repeated inside the transaction below.
        let version: u32 = writer.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version == 0 && has_tables(&writer)? {
            return Err(Error::NotAStore);
        }
        let mode: String = writer.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::JournalMode(mode));
        }
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version: u32 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        match version {
            0 => {
                if has_tables(&tx)? {
                    return Err(Error::NotAStore);
                }
                tx.execute_batch(SCHEMA)?;
                tx.execute(
                    "INSERT INTO binding VALUES (1, ?1, ?2, ?3)",
                    params![
                        binding.chain,
                        binding.genesis_hash.protocol_bytes(),
                        to_sql(REGISTRY_CHUNK_ENTRIES)?
                    ],
                )?;
                tx.execute_batch(&format!("PRAGMA user_version={SCHEMA_VERSION};"))?;
            }
            SCHEMA_VERSION => {}
            other => return Err(Error::UnsupportedSchema(other)),
        }
        let (stored, chunk_entries) = tx.query_row(
            "SELECT chain, genesis_hash, chunk_entries FROM binding WHERE id = 1",
            [],
            |r| {
                Ok((
                    Binding {
                        chain: r.get(0)?,
                        genesis_hash: BlockHash::from_protocol_bytes(r.get(1)?),
                    },
                    r.get::<_, i64>(2)?,
                ))
            },
        )?;
        if stored != binding {
            return Err(Error::BindingMismatch {
                stored: Box::new(stored),
                requested: Box::new(binding),
            });
        }
        let chunk_entries = from_sql(chunk_entries)?;
        if chunk_entries != REGISTRY_CHUNK_ENTRIES {
            return Err(Error::ChunkRuleMismatch {
                stored: chunk_entries,
                build: REGISTRY_CHUNK_ENTRIES,
            });
        }
        tx.commit()?;
        let reader = Connection::open(path)?;
        reader.busy_timeout(BUSY_TIMEOUT)?;
        reader.execute_batch("PRAGMA query_only=ON;")?;
        Ok(Self {
            writer: Mutex::new(writer),
            reader: Mutex::new(reader),
            binding,
        })
    }

    /// The network identity this log is bound to.
    pub fn binding(&self) -> &Binding {
        &self.binding
    }

    /// Registers `address` (spec 9.3, 9.4).
    ///
    /// An address already in the log returns its existing sequence and chunk
    /// with `created: false` and writes nothing. Otherwise it is appended at
    /// the next sequence in one `IMMEDIATE` transaction, and this returns only
    /// after that commit is durable (WAL fsync, `synchronous=FULL`): the
    /// caller may acknowledge as soon as it returns. Concurrent calls are
    /// serialised and never share or skip a sequence.
    ///
    /// The store checks only its own integrity: an all-zero `key16` is
    /// [`Error::ZeroKey`]. The `pk` and `ek` checks of spec 9.1 are the
    /// caller's.
    pub fn register(&self, address: &Address) -> Result<Registration, Error> {
        let mut registrations = self.register_many(std::slice::from_ref(address))?;
        registrations
            .pop()
            .ok_or(Error::Corrupt("registration produced no result"))
    }

    /// Registers each of `addresses` in order, in one transaction with one
    /// fsync: a group commit for a caller that batches concurrent requests.
    /// The result has one [`Registration`] per input, as [`Store::register`]
    /// would give it; an address repeated within the batch is appended once
    /// and its repeats have `created: false`. All or nothing: if any address
    /// is refused, nothing is written.
    pub fn register_many(&self, addresses: &[Address]) -> Result<Vec<Registration>, Error> {
        let digests: Vec<[u8; 32]> = addresses.iter().map(Address::digest).collect();
        if let Some(digest) = digests.iter().find(|d| d[..16] == [0; 16]) {
            return Err(Error::ZeroKey { digest: *digest });
        }
        let mut writer = lock(&self.writer);
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut next = read_log_sequence(&tx)?;
        let mut registrations = Vec::with_capacity(addresses.len());
        {
            let mut find =
                tx.prepare_cached("SELECT sequence, addr FROM registrations WHERE digest = ?1")?;
            let mut insert = tx.prepare_cached(
                "INSERT INTO registrations (sequence, digest, addr) VALUES (?1, ?2, ?3)",
            )?;
            for (address, digest) in addresses.iter().zip(digests) {
                let bytes = address.to_bytes();
                let existing: Option<(i64, Vec<u8>)> = find
                    .query_row([digest.as_slice()], |r| Ok((r.get(0)?, r.get(1)?)))
                    .optional()?;
                match existing {
                    Some((sequence, stored)) => {
                        if stored != bytes {
                            return Err(Error::Corrupt(
                                "a stored address differs from another with its digest",
                            ));
                        }
                        registrations.push(Registration::new(from_sql(sequence)?, digest, false));
                    }
                    None => {
                        insert.execute(params![to_sql(next)?, digest.as_slice(), &bytes[..]])?;
                        registrations.push(Registration::new(next, digest, true));
                        next = next
                            .checked_add(1)
                            .ok_or(Error::Corrupt("log sequence overflow"))?;
                    }
                }
            }
        }
        tx.commit()?;
        Ok(registrations)
    }

    /// The number of entries in the log, which is also the sequence the next
    /// registration will receive: the registry map's `log_sequence` (spec
    /// 10.3).
    pub fn log_sequence(&self) -> Result<u64, Error> {
        read_log_sequence(&lock(&self.reader))
    }

    /// The number of chunks the map lists: every sealed chunk and the one open
    /// chunk, which may be empty (spec 10.3). That is
    /// `log_sequence / REGISTRY_CHUNK_ENTRIES + 1`.
    pub fn chunk_count(&self) -> Result<u64, Error> {
        Ok(chunk_of(self.log_sequence()?) + 1)
    }

    /// The entries of chunk `chunk_id` currently in the log, as
    /// `(sequence, address)` in sequence order, which is the entry ordinal of
    /// the registry table. A sealed chunk has exactly
    /// [`REGISTRY_CHUNK_ENTRIES`]; the open one fewer; a chunk beyond it none.
    /// Each entry is checked against its digest ([`Error::Corrupt`]).
    pub fn chunk(&self, chunk_id: u64) -> Result<Vec<(u64, Address)>, Error> {
        self.chunk_before(chunk_id, u64::MAX)
    }

    /// The entries of chunk `chunk_id` with a sequence below `log_sequence`:
    /// the chunk as a publication whose map has that `log_sequence` contains
    /// it. The publisher reads [`Store::log_sequence`] once and then every
    /// chunk it rebuilds with this, so registrations that land meanwhile do
    /// not make the chunks disagree with the map.
    pub fn chunk_before(
        &self,
        chunk_id: u64,
        log_sequence: u64,
    ) -> Result<Vec<(u64, Address)>, Error> {
        let start = chunk_start(chunk_id);
        // No stored sequence exceeds SQLite's integer range.
        let end = start
            .saturating_add(REGISTRY_CHUNK_ENTRIES)
            .min(log_sequence)
            .min(i64::MAX as u64);
        if start >= end {
            return Ok(Vec::new());
        }
        let reader = lock(&self.reader);
        let mut query = reader.prepare_cached(
            "SELECT sequence, digest, addr FROM registrations
             WHERE sequence >= ?1 AND sequence < ?2 ORDER BY sequence",
        )?;
        let rows = query.query_map(params![to_sql(start)?, to_sql(end)?], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        let mut entries = Vec::new();
        for row in rows {
            let (sequence, digest, addr) = row?;
            let sequence = from_sql(sequence)?;
            if sequence != start + entries.len() as u64 {
                return Err(Error::Corrupt("a gap in log sequences"));
            }
            entries.push((sequence, checked_address(&digest, &addr)?));
        }
        Ok(entries)
    }

    /// Where the address with `digest` sits in the log, with `created: false`;
    /// `None` when it is not registered.
    pub fn lookup(&self, digest: &[u8; 32]) -> Result<Option<Registration>, Error> {
        let reader = lock(&self.reader);
        let row: Option<(i64, Vec<u8>)> = reader
            .prepare_cached("SELECT sequence, addr FROM registrations WHERE digest = ?1")?
            .query_row([digest.as_slice()], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        let Some((sequence, addr)) = row else {
            return Ok(None);
        };
        checked_address(digest, &addr)?;
        Ok(Some(Registration::new(from_sql(sequence)?, *digest, false)))
    }

    /// Copies every committed entry from the write-ahead log into the main
    /// file and truncates the log (`wal_checkpoint(TRUNCATE)`), waiting up to
    /// five seconds for readers. Afterwards, until the next registration, the
    /// main file alone is a complete backup of the registry (spec 9.4).
    /// [`Error::CheckpointBusy`] if readers kept it from completing.
    pub fn checkpoint(&self) -> Result<(), Error> {
        let writer = lock(&self.writer);
        let busy: i64 = writer.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
        if busy != 0 {
            return Err(Error::CheckpointBusy);
        }
        Ok(())
    }
}

/// Locks a connection. A thread that panicked while holding it left no
/// transaction open (a dropped transaction rolls back), so a poisoned lock is
/// taken over.
fn lock(connection: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    connection
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Whether the database holds any table, index or trigger.
fn has_tables(db: &Connection) -> Result<bool, Error> {
    Ok(
        db.query_row("SELECT EXISTS (SELECT 1 FROM sqlite_master)", [], |r| {
            r.get(0)
        })?,
    )
}

/// `MAX(sequence) + 1`, or 0 for an empty log.
fn read_log_sequence(db: &Connection) -> Result<u64, Error> {
    let last: Option<i64> =
        db.query_row("SELECT MAX(sequence) FROM registrations", [], |r| r.get(0))?;
    match last {
        None => Ok(0),
        Some(last) => from_sql(last)?
            .checked_add(1)
            .ok_or(Error::Corrupt("log sequence overflow")),
    }
}

/// A stored address, checked against its stored digest.
fn checked_address(digest: &[u8], addr: &[u8]) -> Result<Address, Error> {
    let address = Address::from_bytes(addr)
        .map_err(|_| Error::Corrupt("a stored address of wrong length"))?;
    if address.digest()[..] != *digest {
        return Err(Error::Corrupt(
            "a stored address does not hash to its digest",
        ));
    }
    Ok(address)
}

fn to_sql(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error::Corrupt("value beyond SQLite's integer range"))
}

fn from_sql(value: i64) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error::Corrupt("negative stored value"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The writer commits with `synchronous=FULL` in WAL mode, and the reader
    /// cannot write.
    #[test]
    fn connections_are_configured_for_durability() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(
            dir.path().join(STORE_FILE),
            Binding {
                chain: "test".to_owned(),
                genesis_hash: BlockHash::from_protocol_bytes([7; 32]),
            },
        )
        .unwrap();
        let writer = lock(&store.writer);
        let synchronous: i64 = writer
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .unwrap();
        assert_eq!(synchronous, 2, "synchronous=FULL");
        let mode: String = writer
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        drop(writer);
        let reader = lock(&store.reader);
        let query_only: i64 = reader
            .query_row("PRAGMA query_only", [], |r| r.get(0))
            .unwrap();
        assert_eq!(query_only, 1);
        assert!(
            reader
                .execute("INSERT INTO binding VALUES (2, 'x', zeroblob(32), 1)", [])
                .is_err()
        );
    }
}
