//! The registration log against authored addresses (spec sections 9.2 to 9.4,
//! 10.3 and scenario 14 of 13.2).
mod common;

use common::*;
use registry_store::{
    Binding, Error, REGISTRY_CHUNK_ENTRIES, Registration, SCHEMA_VERSION, STORE_FILE, Store,
    chunk_of, chunk_start,
};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Barrier;
use std::time::Instant;
use tachyon_pir_protocol::BlockHash;
use tachyon_pir_protocol::registry::Address;

/// Every entry of the log, chunk by chunk, checked to be gap-free.
fn all_entries(store: &Store) -> Vec<(u64, Address)> {
    let log_sequence = store.log_sequence().unwrap();
    let mut entries = Vec::new();
    for chunk_id in 0..store.chunk_count().unwrap() {
        let chunk = store.chunk(chunk_id).unwrap();
        assert!(chunk.len() as u64 <= REGISTRY_CHUNK_ENTRIES);
        entries.extend(chunk);
    }
    assert_eq!(entries.len() as u64, log_sequence);
    for (position, (sequence, _)) in entries.iter().enumerate() {
        assert_eq!(*sequence, position as u64);
    }
    entries
}

/// The log holds exactly `expected`, in order, and each is found by digest.
fn assert_log(store: &Store, expected: &[Address]) {
    let entries = all_entries(store);
    assert_eq!(entries.len(), expected.len());
    for ((sequence, stored), authored) in entries.iter().zip(expected) {
        assert_eq!(stored, authored, "entry {sequence}");
        assert_eq!(
            store.lookup(&authored.digest()).unwrap(),
            Some(Registration {
                sequence: *sequence,
                chunk_id: sequence / REGISTRY_CHUNK_ENTRIES,
                digest: authored.digest(),
                created: false,
            })
        );
    }
}

/// A repeated address returns its first registration with `created: false`
/// and writes nothing (spec 9.3).
#[test]
fn registration_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&store_path(&dir));
    assert_eq!(store.log_sequence().unwrap(), 0);
    assert_eq!(store.chunk_count().unwrap(), 1);
    assert!(store.chunk(0).unwrap().is_empty());
    let [a, b] = [address(1, 0), address(1, 1)];
    assert_eq!(store.lookup(&a.digest()).unwrap(), None);

    let first = store.register(&a).unwrap();
    assert_eq!(
        first,
        Registration {
            sequence: 0,
            chunk_id: 0,
            digest: a.digest(),
            created: true
        }
    );
    let response = first.response();
    assert_eq!(response.chunk_id, 0);
    assert_eq!(response.digest.bytes(), &a.digest());
    assert!(response.created);
    assert_eq!(response.visible_after_sequence, 0);

    let again = store.register(&a).unwrap();
    assert_eq!(
        again,
        Registration {
            created: false,
            ..first
        }
    );
    assert!(!again.response().created);
    assert_eq!(store.log_sequence().unwrap(), 1);

    let second = store.register(&b).unwrap();
    assert_eq!((second.sequence, second.created), (1, true));
    assert_eq!(store.register(&a).unwrap(), again);
    assert_eq!(
        store.register(&b).unwrap(),
        Registration {
            created: false,
            ..second
        }
    );
    assert_eq!(store.log_sequence().unwrap(), 2);
    assert_log(&store, &[a.clone(), b.clone()]);

    // In one batch: a repeat within the batch and repeats of stored entries.
    let c = address(1, 2);
    let batch = store
        .register_many(&[c.clone(), a.clone(), c.clone(), b.clone()])
        .unwrap();
    assert_eq!(
        batch
            .iter()
            .map(|r| (r.sequence, r.created))
            .collect::<Vec<_>>(),
        vec![(2, true), (0, false), (2, false), (1, false)]
    );
    assert_eq!(store.log_sequence().unwrap(), 3);
    assert_eq!(store.register_many(&[]).unwrap(), vec![]);
    assert_log(&store, &[a, b, c]);
}

/// Sequences count from 0 without gaps; chunk `c` holds sequences
/// `3072c .. 3072(c + 1)`, and a chunk that has just filled leaves an empty
/// open chunk after it (spec 9.2, 9.4, 10.3).
#[test]
fn sequences_and_chunks_cross_a_chunk_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&store_path(&dir));
    let total = REGISTRY_CHUNK_ENTRIES + 9;
    let authored = addresses(2, 0..total);
    let below = (REGISTRY_CHUNK_ENTRIES - 3) as usize;
    let batch = store.register_many(&authored[..below]).unwrap();
    for (sequence, registration) in batch.iter().enumerate() {
        assert_eq!(registration.sequence, sequence as u64);
        assert_eq!(registration.chunk_id, 0);
        assert!(registration.created);
    }
    // The last entries of chunk 0 and the first of chunk 1 one at a time.
    for (index, address) in authored.iter().enumerate().skip(below) {
        let sequence = index as u64;
        let registration = store.register(address).unwrap();
        assert_eq!(registration.sequence, sequence);
        assert_eq!(registration.chunk_id, chunk_of(sequence));
        assert_eq!(
            registration.chunk_id,
            if sequence < REGISTRY_CHUNK_ENTRIES {
                0
            } else {
                1
            }
        );
        assert!(registration.created);
        assert_eq!(store.log_sequence().unwrap(), sequence + 1);
        // One open chunk at log_sequence / 3072, so filling chunk 0 opens an
        // empty chunk 1.
        let expected_chunks = (sequence + 1) / REGISTRY_CHUNK_ENTRIES + 1;
        assert_eq!(store.chunk_count().unwrap(), expected_chunks);
        if sequence + 1 == REGISTRY_CHUNK_ENTRIES {
            assert_eq!(store.chunk_count().unwrap(), 2);
            assert!(store.chunk(1).unwrap().is_empty());
            assert_eq!(store.chunk(0).unwrap().len() as u64, REGISTRY_CHUNK_ENTRIES);
        }
    }
    assert_eq!(store.log_sequence().unwrap(), total);
    assert_eq!(store.chunk_count().unwrap(), 2);
    let sealed = store.chunk(0).unwrap();
    assert_eq!(sealed.len() as u64, REGISTRY_CHUNK_ENTRIES);
    assert_eq!(sealed.last().unwrap().0, REGISTRY_CHUNK_ENTRIES - 1);
    let open_chunk = store.chunk(1).unwrap();
    assert_eq!(open_chunk.len(), 9);
    assert_eq!(open_chunk[0].0, chunk_start(1));
    assert_eq!(open_chunk[0].1, authored[REGISTRY_CHUNK_ENTRIES as usize]);
    assert!(store.chunk(2).unwrap().is_empty());
    assert!(store.chunk(u64::MAX).unwrap().is_empty());
    assert!(store.chunk(1 << 52).unwrap().is_empty());
    // A chunk as of an earlier log sequence.
    assert_eq!(
        store.chunk_before(1, REGISTRY_CHUNK_ENTRIES + 4).unwrap(),
        open_chunk[..4].to_vec()
    );
    assert!(
        store
            .chunk_before(1, REGISTRY_CHUNK_ENTRIES)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.chunk_before(0, 5).unwrap(), sealed[..5].to_vec());
    assert_eq!(store.chunk_before(0, total).unwrap(), sealed);
    // Re-registering across the boundary changes nothing.
    for index in [
        0,
        REGISTRY_CHUNK_ENTRIES - 1,
        REGISTRY_CHUNK_ENTRIES,
        total - 1,
    ] {
        let registration = store.register(&authored[index as usize]).unwrap();
        assert_eq!(registration.sequence, index);
        assert_eq!(registration.chunk_id, chunk_of(index));
        assert!(!registration.created);
    }
    assert_eq!(store.log_sequence().unwrap(), total);
    assert_log(&store, &authored);
}

/// Every registration survives dropping and reopening the store, and the log
/// continues at the next sequence.
#[test]
fn registrations_are_durable_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let authored = addresses(3, 0..40);
    let store = open(&path);
    for address in &authored[..25] {
        store.register(address).unwrap();
    }
    drop(store);
    let store = open(&path);
    assert_eq!(store.binding(), &binding());
    assert_log(&store, &authored[..25]);
    assert_eq!(store.register(&authored[3]).unwrap().sequence, 3);
    assert_eq!(
        store.register_many(&authored[25..]).unwrap()[0].sequence,
        25
    );
    drop(store);
    assert_log(&open(&path), &authored);
}

/// A store that is never closed (its connections are leaked, as a crash
/// would leave them) has its acknowledged registrations in the files on disk:
/// both the files in place and a copy of them, taken while it is still open,
/// reopen with every entry.
#[test]
fn an_unclosed_store_leaves_every_registration_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let authored = addresses(4, 0..60);
    let store = open(&path);
    for address in &authored {
        store.register(address).unwrap();
    }
    std::mem::forget(store);
    let wal = dir.path().join(format!("{STORE_FILE}-wal"));
    assert!(
        wal.metadata().unwrap().len() > 0,
        "the WAL was never checkpointed"
    );
    let image = tempfile::tempdir().unwrap();
    std::fs::copy(&path, store_path(&image)).unwrap();
    std::fs::copy(&wal, image.path().join(format!("{STORE_FILE}-wal"))).unwrap();
    assert_log(&open(&store_path(&image)), &authored);
    let reopened = open(&path);
    assert_log(&reopened, &authored);
    assert_eq!(reopened.register(&address(4, 60)).unwrap().sequence, 60);
}

const CRASH_CHILD: &str = "REGISTRY_STORE_CRASH_CHILD";
const CRASH_TEST: &str = "acknowledged_registrations_survive_a_process_crash";
/// The child stops by itself after this many registrations if never killed.
const CRASH_CHILD_LIMIT: u64 = 50_000;

/// The child half of the crash test: registers address `i` of family 5 for
/// `i = 0, 1, ...` and prints `ack i sequence` after each `register` returns,
/// that is when the registration could be acknowledged.
fn crash_child(path: &Path) {
    let store = open(path);
    for index in 0..CRASH_CHILD_LIMIT {
        let registration = store.register(&address(5, index)).unwrap();
        println!("ack {index} {}", registration.sequence);
    }
}

/// A process killed (SIGKILL) while registering loses no acknowledged
/// registration: on reopen every acknowledged address is at its acknowledged
/// sequence, and the log holds nothing else but the child's next addresses in
/// order (spec 9.3 "durable (fsync)", scenario 14).
#[test]
fn acknowledged_registrations_survive_a_process_crash() {
    if let Some(path) = std::env::var_os(CRASH_CHILD) {
        crash_child(Path::new(&path));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([CRASH_TEST, "--exact", "--nocapture", "--test-threads=1"])
        .env(CRASH_CHILD, &path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut acknowledged: Vec<(u64, u64)> = Vec::new();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    for line in lines.by_ref() {
        if let Some(ack) = parse_ack(&line.unwrap()) {
            acknowledged.push(ack);
            if acknowledged.len() == 300 {
                child.kill().unwrap();
                break;
            }
        }
    }
    // Acknowledgements already in the pipe when the child died count too; a
    // line cut short by the kill does not parse and is not one.
    for line in lines {
        let Ok(line) = line else { break };
        if let Some(ack) = parse_ack(&line) {
            acknowledged.push(ack);
        }
    }
    let status = child.wait().unwrap();
    assert!(!status.success(), "the child was killed, not finished");
    assert!(acknowledged.len() >= 300);
    assert!(
        dir.path().join(format!("{STORE_FILE}-wal")).exists(),
        "the child never closed the store"
    );

    let store = open(&path);
    let log_sequence = store.log_sequence().unwrap();
    for (index, sequence) in &acknowledged {
        assert_eq!(index, sequence);
        let registration = store.lookup(&address(5, *index).digest()).unwrap();
        assert_eq!(registration.map(|r| r.sequence), Some(*sequence));
    }
    assert!(log_sequence >= acknowledged.len() as u64);
    // At most the one registration in flight when the kill landed is in the
    // log without an acknowledgement.
    assert!(log_sequence <= acknowledged.len() as u64 + 1);
    assert_log(&store, &addresses(5, 0..log_sequence));
}

fn parse_ack(line: &str) -> Option<(u64, u64)> {
    let mut words = line.strip_prefix("ack ")?.split(' ');
    let index = words.next()?.parse().ok()?;
    let sequence = words.next()?.parse().ok()?;
    words.next().is_none().then_some((index, sequence))
}

/// Many threads registering at once, each its own addresses and all of them
/// one shared set in different orders, produce distinct gap-free sequences,
/// one entry per distinct address, and exactly one `created: true` for each.
#[test]
fn concurrent_registrations_are_gap_free_and_unique() {
    const THREADS: u64 = 16;
    const OWN: u64 = 40;
    const SHARED: u64 = 30;
    let dir = tempfile::tempdir().unwrap();
    let store = open(&store_path(&dir));
    let shared = addresses(6, 0..SHARED);
    let barrier = Barrier::new(THREADS as usize);
    let results: Vec<Vec<(Address, Registration)>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|thread| {
                let (store, shared, barrier) = (&store, &shared, &barrier);
                scope.spawn(move || {
                    let own = addresses(100 + thread, 0..OWN);
                    let mut work: Vec<Address> = Vec::new();
                    for (index, address) in own.into_iter().enumerate() {
                        work.push(address);
                        // The shared set, rotated by thread, interleaved.
                        if index < SHARED as usize {
                            let rotated = (index + 7 * thread as usize) % SHARED as usize;
                            work.push(shared[rotated].clone());
                        }
                    }
                    barrier.wait();
                    let mut results = Vec::new();
                    if thread % 4 == 3 {
                        // Some threads in small group commits.
                        for batch in work.chunks(5) {
                            let registrations = store.register_many(batch).unwrap();
                            results.extend(batch.iter().cloned().zip(registrations));
                        }
                    } else {
                        for address in work {
                            let registration = store.register(&address).unwrap();
                            results.push((address, registration));
                        }
                    }
                    results
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let distinct = THREADS * OWN + SHARED;
    assert_eq!(store.log_sequence().unwrap(), distinct);
    let mut created = HashSet::new();
    let mut by_digest: HashMap<[u8; 32], (u64, u32)> = HashMap::new();
    for (address, registration) in results.iter().flatten() {
        assert_eq!(registration.digest, address.digest());
        assert_eq!(registration.chunk_id, chunk_of(registration.sequence));
        if registration.created {
            assert!(created.insert(registration.sequence), "sequence reused");
        }
        let seen = by_digest
            .entry(registration.digest)
            .or_insert((registration.sequence, 0));
        assert_eq!(seen.0, registration.sequence, "one address, two sequences");
        seen.1 += u32::from(registration.created);
    }
    assert_eq!(created, (0..distinct).collect::<HashSet<_>>());
    assert_eq!(by_digest.len() as u64, distinct);
    assert!(by_digest.values().all(|(_, created)| *created == 1));
    // Each shared address was registered by every thread.
    for address in &shared {
        let calls = results
            .iter()
            .flatten()
            .filter(|(a, _)| a == address)
            .count();
        assert_eq!(calls as u64, THREADS);
    }
    let entries = all_entries(&store);
    let mut stored = HashSet::new();
    for (sequence, address) in &entries {
        assert_eq!(by_digest[&address.digest()].0, *sequence);
        assert!(stored.insert(address.digest()));
    }
}

/// A log is bound to its network identity and chunk size: reopening under
/// another identity, or a file whose chunk size differs from this build's, is
/// refused and changes nothing (spec 9.2 Identity).
#[test]
fn a_binding_mismatch_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let authored = addresses(7, 0..5);
    open(&path).register_many(&authored).unwrap();
    let changes: [fn(&mut Binding); 3] = [
        |b| b.chain = "main".to_owned(),
        |b| b.genesis_hash = BlockHash::from_protocol_bytes([0x5b; 32]),
        |b| b.chain = "Test".to_owned(),
    ];
    for change in changes {
        let mut wrong = binding();
        change(&mut wrong);
        match Store::open(&path, wrong.clone()) {
            Err(Error::BindingMismatch { stored, requested }) => {
                assert_eq!(*stored, binding());
                assert_eq!(*requested, wrong);
            }
            Err(other) => panic!("{other:?}"),
            Ok(_) => panic!("opened with {wrong:?}"),
        }
    }
    assert_log(&open(&path), &authored);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("UPDATE binding SET chunk_entries = 1024;")
        .unwrap();
    assert!(matches!(
        Store::open(&path, binding()),
        Err(Error::ChunkRuleMismatch {
            stored: 1024,
            build: 3072
        })
    ));
}

/// Another SQLite database, a file that is not SQLite, and a future schema
/// version are all refused.
#[test]
fn foreign_files_and_unknown_schemas_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let foreign = dir.path().join("foreign.sqlite");
    rusqlite::Connection::open(&foreign)
        .unwrap()
        .execute_batch("CREATE TABLE registrations (x INTEGER);")
        .unwrap();
    assert!(matches!(
        Store::open(&foreign, binding()),
        Err(Error::NotAStore)
    ));
    // The foreign file is left as it was, not switched to WAL.
    let mode: String = rusqlite::Connection::open(&foreign)
        .unwrap()
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "delete");
    let garbage = dir.path().join("garbage.sqlite");
    std::fs::write(&garbage, vec![0x42; 8192]).unwrap();
    assert!(matches!(
        Store::open(&garbage, binding()),
        Err(Error::Sql(_))
    ));
    assert_eq!(std::fs::read(&garbage).unwrap(), vec![0x42; 8192]);
    let path = store_path(&dir);
    drop(open(&path));
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("PRAGMA user_version=2;")
        .unwrap();
    assert!(matches!(
        Store::open(&path, binding()),
        Err(Error::UnsupportedSchema(2))
    ));
}

/// The file is named and versioned as spec 9.4 says and runs in WAL mode.
#[test]
fn the_file_is_versioned_and_in_wal_mode() {
    assert_eq!(STORE_FILE, "registry-v1.sqlite");
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let store = open(&path);
    let probe = rusqlite::Connection::open(&path).unwrap();
    let mode: String = probe
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    let version: u32 = probe
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, SCHEMA_VERSION);
    drop(store);
}

/// The file itself is append-only and gap-free: another connection cannot
/// change or delete an entry, insert at a gap or reuse a sequence, or store a
/// wrong length or an all-zero key16.
#[test]
fn the_file_refuses_changes_gaps_and_malformed_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let store = open(&path);
    let authored = addresses(8, 0..3);
    store.register_many(&authored).unwrap();
    let other = rusqlite::Connection::open(&path).unwrap();
    let address = address(8, 3);
    let digest = address.digest();
    let addr = address.to_bytes();
    let mut zero_key = digest;
    zero_key[..16].fill(0);
    let refused: [(i64, Vec<u8>, Vec<u8>); 6] = [
        // A gap, a reused sequence and a negative one.
        (4, digest.to_vec(), addr.to_vec()),
        (2, digest.to_vec(), addr.to_vec()),
        (-1, digest.to_vec(), addr.to_vec()),
        // Lengths and the zero key.
        (3, digest[..31].to_vec(), addr.to_vec()),
        (3, digest.to_vec(), addr[..1215].to_vec()),
        (3, zero_key.to_vec(), addr.to_vec()),
    ];
    for (sequence, digest, addr) in refused {
        assert!(
            other
                .execute(
                    "INSERT INTO registrations (sequence, digest, addr) VALUES (?1, ?2, ?3)",
                    rusqlite::params![sequence, digest, addr],
                )
                .is_err()
        );
    }
    // A repeated digest at the next sequence.
    assert!(
        other
            .execute(
                "INSERT INTO registrations VALUES (3, ?1, ?2)",
                rusqlite::params![authored[0].digest(), authored[0].to_bytes().to_vec()],
            )
            .is_err()
    );
    assert!(
        other
            .execute(
                "UPDATE registrations SET addr = addr WHERE sequence = 1",
                []
            )
            .is_err()
    );
    assert!(
        other
            .execute("DELETE FROM registrations WHERE sequence = 2", [])
            .is_err()
    );
    assert_log(&store, &authored);
    assert_eq!(store.register(&address).unwrap().sequence, 3);
}

/// An entry tampered with behind the store's back (its guarding trigger
/// dropped) is reported, by reads and by a repeat registration.
#[test]
fn tampered_entries_are_reported_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let store = open(&path);
    let authored = addresses(9, 0..4);
    store.register_many(&authored).unwrap();
    let mut changed = authored[2].to_bytes();
    changed[100] ^= 1;
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "DROP TRIGGER registrations_never_changed;
             DROP TRIGGER registrations_never_deleted;",
        )
        .unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE registrations SET addr = ?1 WHERE sequence = 2",
            [changed.as_slice()],
        )
        .unwrap();
    assert!(matches!(store.chunk(0), Err(Error::Corrupt(_))));
    assert!(matches!(
        store.lookup(&authored[2].digest()),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(
        store.register(&authored[2]),
        Err(Error::Corrupt(_))
    ));
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("DELETE FROM registrations WHERE sequence = 1", [])
        .unwrap();
    assert!(matches!(store.chunk(0), Err(Error::Corrupt(_))));
}

/// A failure part way through a group commit writes none of it.
#[test]
fn a_failed_registration_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let store = open(&path);
    let authored = addresses(10, 0..5);
    store.register(&authored[0]).unwrap();
    // Another connection makes the insert of authored[3] fail.
    let poison: String = authored[3]
        .digest()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER fail BEFORE INSERT ON registrations WHEN NEW.digest = x'{poison}'
             BEGIN SELECT RAISE(ABORT, 'injected'); END;"
        ))
        .unwrap();
    assert!(matches!(
        store.register_many(&authored[1..]),
        Err(Error::Sql(_))
    ));
    assert!(matches!(store.register(&authored[3]), Err(Error::Sql(_))));
    assert_log(&store, &authored[..1]);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TRIGGER fail;")
        .unwrap();
    let registrations = store.register_many(&authored[1..]).unwrap();
    assert_eq!(registrations[0].sequence, 1);
    assert_log(&store, &authored);
}

/// After a checkpoint, a copy of the main file alone is a complete backup
/// (spec 9.4): it reopens with identical contents and continues at the same
/// next sequence as the original.
#[test]
fn a_copy_of_the_file_is_a_complete_backup() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let store = open(&path);
    let authored = addresses(11, 0..REGISTRY_CHUNK_ENTRIES + 100);
    store.register_many(&authored).unwrap();
    store.checkpoint().unwrap();
    let wal = dir.path().join(format!("{STORE_FILE}-wal"));
    assert_eq!(wal.metadata().map(|m| m.len()).unwrap_or(0), 0);
    let backup_dir = tempfile::tempdir().unwrap();
    let backup = store_path(&backup_dir);
    std::fs::copy(&path, &backup).unwrap();
    assert!(!backup_dir.path().join(format!("{STORE_FILE}-wal")).exists());

    let restored = open(&backup);
    assert_eq!(
        restored.log_sequence().unwrap(),
        store.log_sequence().unwrap()
    );
    assert_eq!(all_entries(&restored), all_entries(&store));
    assert_log(&restored, &authored);
    let next = address(11, REGISTRY_CHUNK_ENTRIES + 100);
    assert_eq!(
        restored.register(&next).unwrap(),
        store.register(&next).unwrap()
    );
    // The backup keeps its binding.
    assert!(matches!(
        Store::open(
            &backup,
            Binding {
                chain: "main".to_owned(),
                ..binding()
            }
        ),
        Err(Error::BindingMismatch { .. })
    ));
}

/// One chunk of a 100,000-entry log is read, verified against its digests,
/// well inside the budget; so is a lookup.
#[test]
fn one_chunk_of_a_100k_log_is_cheap_to_read() {
    const ENTRIES: u64 = 100_000;
    let dir = tempfile::tempdir().unwrap();
    let store = open(&store_path(&dir));
    let started = Instant::now();
    for start in (0..ENTRIES).step_by(10_000) {
        store
            .register_many(&addresses(12, start..start + 10_000))
            .unwrap();
    }
    let written = started.elapsed();
    assert_eq!(store.log_sequence().unwrap(), ENTRIES);
    assert_eq!(
        store.chunk_count().unwrap(),
        ENTRIES / REGISTRY_CHUNK_ENTRIES + 1
    );

    let started = Instant::now();
    let chunk = store.chunk(16).unwrap();
    let read = started.elapsed();
    assert_eq!(chunk.len() as u64, REGISTRY_CHUNK_ENTRIES);
    for (offset, (sequence, address)) in chunk.iter().enumerate() {
        let expected = chunk_start(16) + offset as u64;
        assert_eq!(*sequence, expected);
        assert_eq!(*address, common::address(12, expected));
    }
    let started = Instant::now();
    let open_chunk = store.chunk(ENTRIES / REGISTRY_CHUNK_ENTRIES).unwrap();
    let open_read = started.elapsed();
    assert_eq!(open_chunk.len() as u64, ENTRIES % REGISTRY_CHUNK_ENTRIES);
    let started = Instant::now();
    let found = store
        .lookup(&common::address(12, 77_777).digest())
        .unwrap()
        .unwrap();
    let lookup = started.elapsed();
    assert_eq!(found.sequence, 77_777);
    eprintln!(
        "100k log: write {written:?}, sealed chunk {read:?}, open chunk {open_read:?}, lookup {lookup:?}"
    );
    assert!(read.as_secs_f64() < 1.0, "sealed chunk read took {read:?}");
    assert!(
        open_read.as_secs_f64() < 1.0,
        "open chunk read took {open_read:?}"
    );
    assert!(lookup.as_secs_f64() < 0.1, "lookup took {lookup:?}");
}
