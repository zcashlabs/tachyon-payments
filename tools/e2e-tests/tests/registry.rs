//! Registry end-to-end acceptance (spec 13.2 scenarios 1-4, 6 and 10-14: the
//! milestone M3 exit criterion of spec 14).
//!
//! Every test runs the registry topology of spec 13.1
//! ([`RegistryTopology`]): the publisher binary with the registry enabled
//! (its identity read once from an in-process node double serving a
//! genesis-only chain), the TLS edge in front of it, the query-server binary
//! trusting the edge, and the probe client as the wallet. The registry's
//! source is the test itself: it registers addresses it generated through
//! the write API, directly on the registry listener with an
//! `X-Forwarded-For` it sets (the test plays the reverse proxy spec 9.3
//! requires), and once through the edge's `/registry/v1/` route. Expected
//! answers come only from [`RegistryOracle`], the test's record of what it
//! registered and what the publisher acknowledged.
use reqwest::StatusCode;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tachyon_pir_cli::{Client, ClientError, LookupOutcome, MapView, Mount, ScopeSelector};
use tachyon_pir_e2e_tests::budget::Budget;
use tachyon_pir_e2e_tests::client;
use tachyon_pir_e2e_tests::memo_checks::row_under_seed0;
use tachyon_pir_e2e_tests::node::start_node;
use tachyon_pir_e2e_tests::oracle::State;
use tachyon_pir_e2e_tests::registry_checks::{
    Probe, check_listing, check_lookups, check_map, hits, judge, misses, registry_scope, sample,
    sessions, spawn_prober,
};
use tachyon_pir_e2e_tests::registry_oracle::{
    Ack, AddressGen, ML_KEM_Q, OracleAddress, REGISTER_BODY_BYTES, REGISTRY_CHUNK_ENTRIES,
    RegistryOracle, invalid,
};
use tachyon_pir_e2e_tests::registry_topology::{
    HeldUpload, Identity, Issued, RegistryOptions, RegistryPublisher, RegistrySettings,
    RegistryTopology, WriteApi, assert_refusal, identity_chain, now_unix, parse_ack, register_body,
    registry_head, solve, subject_sequence, unsolved, wait_map, wait_sequence,
};
use tachyon_pir_e2e_tests::topology::{describe, exclusive, wait_for, wait_limit};
use tachyon_pir_e2e_tests::{Fault, Target, binary};
use tachyon_pir_protocol::Product;

/// Registrations in flight at once in bulk.
const WRITERS: usize = 64;
/// The client address the test, as the proxy, puts on bulk registrations.
const CLIENT: &str = "203.0.113.7";

fn publisher_binary() -> PathBuf {
    binary!("tachyon-pir-publisher-test")
}

fn query_binary() -> PathBuf {
    binary!("tachyon-pir-query-server-test")
}

// ---------------------------------------------------------------------------
// Registering and recording.

/// Registers `addresses` (`WRITERS` at a time) and records every
/// acknowledgement in the oracle, which checks it.
async fn register_recorded(
    api: &WriteApi,
    issued: &Issued,
    oracle: &mut RegistryOracle,
    addresses: &[OracleAddress],
) -> Vec<Ack> {
    let acks = api.register_all(issued, addresses, WRITERS).await;
    for (address, ack) in addresses.iter().zip(&acks) {
        oracle.record(address, *ack);
    }
    oracle.assert_contiguous();
    acks
}

/// Hits of every chunk up to the open one at `log_sequence`, sealed chunks
/// sampled to `sealed_sample` each.
fn sampled_hits(oracle: &RegistryOracle, log_sequence: u64, sealed_sample: usize) -> Vec<Probe> {
    let mut probes = Vec::new();
    for chunk in oracle.chunks(log_sequence) {
        let all = hits(oracle, chunk.index, log_sequence);
        match chunk.state {
            State::Sealed => probes.extend(sample(&all, sealed_sample, chunk.index)),
            State::Open => probes.extend(all),
        }
    }
    probes
}

/// The publisher's current publication with its listing checked against
/// the oracle at `log_sequence`.
async fn check_published(
    publisher: &RegistryPublisher,
    oracle: &RegistryOracle,
    identity: &Identity,
    log_sequence: u64,
    budget: &Budget,
) -> tachyon_pir_e2e_tests::topology::Publication {
    let publication = publisher
        .wait_log_sequence(log_sequence, wait_limit(budget))
        .await;
    assert!(publication.eligible, "published but not eligible");
    let (head, scopes) = publisher.listing(&publication).await;
    tachyon_pir_e2e_tests::registry_checks::check_head(&head, identity, log_sequence);
    assert_eq!(head.generation, publication.inventory.generation);
    assert_eq!(
        head.recovery_epoch.get(),
        publication.inventory.recovery_epoch
    );
    check_listing(&scopes.iter().collect::<Vec<_>>(), oracle, log_sequence);
    let tables: usize = scopes.iter().map(|s| s.tables.len()).sum();
    assert_eq!(
        publication.inventory.tables.len(),
        tables,
        "inventory tables"
    );
    assert_eq!(
        publication.inventory.newest_scope,
        log_sequence / REGISTRY_CHUNK_ENTRIES
    );
    publication
}

// ---------------------------------------------------------------------------
// Scenarios.

/// Scenario 1: with no query host at all, the empty log is published (one
/// open chunk 0 without tables) and every batch of registrations is
/// published in turn: generation strictly increasing, `log_sequence` the
/// number of acknowledged entries, the listing exactly the oracle's, and the
/// publisher healthy and ready with its metrics describing the publication.
#[tokio::test(flavor = "multi_thread")]
async fn publisher_without_consumers_advances() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let chain = identity_chain(1);
            let identity = Identity::of(&chain);
            let node = start_node(chain).await;
            let publisher = RegistryPublisher::start(
                publisher_binary(),
                &node.url(),
                RegistrySettings::bulk(),
                wait_limit(&budget),
            )
            .await;
            let mut oracle = RegistryOracle::new();
            let empty = check_published(&publisher, &oracle, &identity, 0, &budget).await;
            assert!(
                empty.inventory.tables.is_empty(),
                "an empty log has no tables"
            );

            let api = publisher.write(CLIENT);
            let issued = api.challenge().await;
            assert_eq!(
                issued.difficulty_bits,
                RegistrySettings::bulk().difficulty_bits
            );
            let mut addresses = AddressGen::new(1);
            let mut generations = vec![empty.inventory.generation];
            for (round, batch) in [1usize, 7, 30, 2].into_iter().enumerate() {
                let batch = addresses.addresses(batch);
                register_recorded(&api, &issued, &mut oracle, &batch).await;
                let log_sequence = oracle.len();
                let publication =
                    check_published(&publisher, &oracle, &identity, log_sequence, &budget).await;
                assert_eq!(
                    publication.inventory.tables.len(),
                    1,
                    "round {round}: one table for the open chunk"
                );
                generations.push(publication.inventory.generation);
                for route in ["health", "ready"] {
                    let url = format!("http://{}/{route}", publisher.management());
                    client::get(&client::plain(), &url)
                        .await
                        .assert_status(StatusCode::OK, &url);
                }
                let created = publisher
                    .metric("tachyon_registry_registrations_total{result=\"created\"}")
                    .await;
                assert_eq!(created, log_sequence as f64);
                wait_for(
                    "the publisher's gauges to describe the publication",
                    wait_limit(&budget),
                    || async {
                        let entries = publisher.metric("tachyon_registry_entries_total").await;
                        let open = publisher
                            .metric("tachyon_registry_open_scope_entries")
                            .await;
                        if entries == log_sequence as f64 && open == log_sequence as f64 {
                            Ok(())
                        } else {
                            Err(format!(
                                "entries_total {entries}, open_scope_entries {open}"
                            ))
                        }
                    },
                )
                .await;
            }
            assert!(
                generations.windows(2).all(|w| w[0] < w[1]),
                "generations must strictly increase: {generations:?}"
            );
        })
        .await
}

/// Scenarios 2 and 4: cold bootstrap over TLS from an empty state directory
/// (objects held at the edge: not ready, `503`), then the map exactly the
/// oracle's and n of n exact answers in the open chunk, hits and misses.
/// One registration goes through the edge's `/registry/v1/` route.
#[tokio::test(flavor = "multi_thread")]
async fn cold_bootstrap_over_tls_and_exact_answers() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut t = RegistryTopology::launch(
                RegistryOptions::new(2),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let mut oracle = RegistryOracle::new();
            let mut addresses = AddressGen::new(2);
            let fresh = addresses.addresses(4);

            // Through the edge, which sets X-Forwarded-For itself.
            let edge = t.write_via_edge();
            let issued = edge.challenge().await;
            let first = addresses.address();
            let ack = edge.register(&issued, &first).await;
            oracle.record(&first, ack);
            assert_eq!(ack.visible_after_sequence, 0);
            assert!(t.edge.count("/registry/v1/register") >= 1);

            let api = t.write(CLIENT);
            let issued = api.challenge().await;
            register_recorded(&api, &issued, &mut oracle, &addresses.addresses(23)).await;
            let log_sequence = oracle.len();
            check_published(&t.publisher, &oracle, &t.identity, log_sequence, &budget).await;
            assert_eq!(
                std::fs::read_dir(t.query.state_dir()).unwrap().count(),
                0,
                "the query host starts from an empty state directory"
            );

            let hold = t
                .edge
                .inject(Target::ProductObjects("registry".into()), Fault::Hold);
            t.query.start();
            wait_for(
                "an object fetch held at the edge",
                wait_limit(&budget),
                || async {
                    if t.edge.held() > 0 {
                        Ok(())
                    } else {
                        Err("none held".to_owned())
                    }
                },
            )
            .await;
            let ready = t.query.management_get("ready").await;
            assert_eq!(
                ready.status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{}",
                ready.text()
            );
            let wallet = t.query.wallet();
            match wallet.fetch_map().await {
                Err(ClientError::Unavailable { body, .. }) => {
                    assert_eq!(body.code.as_str(), "temporarily_unavailable");
                }
                other => panic!("map before ready: {other:?}"),
            }
            assert!(
                t.edge.count_matching(&Target::Discovery) > 0,
                "discovery was checked before start (spec 11.6)"
            );
            t.edge.remove(hold);
            t.query.wait_ready(wait_limit(&budget)).await;

            let map = wallet.fetch_map().await.expect("the map once ready");
            check_map(&map, &oracle, &t.identity, log_sequence);
            let legacy =
                Client::with_mount(&t.query.origin(), Product::Registry, None, Mount::Legacy)
                    .unwrap()
                    .fetch_map()
                    .await
                    .expect("the legacy mount serves the map");
            assert_eq!(legacy.map_sha256(), map.map_sha256());
            assert_eq!(t.query.metric("log_sequence").await, log_sequence as f64);

            let found = hits(&oracle, 0, log_sequence);
            let missing = misses(&oracle, &fresh, log_sequence, 0);
            let tally = check_lookups(&wallet, &map, &oracle, &found).await;
            assert_eq!(
                tally.found, log_sequence as usize,
                "every registered address found"
            );
            let tally = check_lookups(&wallet, &map, &oracle, &missing).await;
            assert_eq!(tally.absent, missing.len());
            eprintln!(
                "cold bootstrap: {} hits, {} misses in {:?}",
                found.len(),
                missing.len(),
                budget.elapsed()
            );
        })
        .await
}

/// Scenarios 3 and 4 across a sealed and the open chunk. 3,072
/// registrations seal chunk 0 and leave chunk 1 open and empty: the map
/// lists both exactly, and a lookup in the empty open chunk is absent
/// without a query. After 40 more registrations the query host fetches
/// only objects it does not hold (no object of the previous publication,
/// not the sealed chunk's tables or map chunk), the sealed chunk keeps its
/// session ids and answers under the old map as under the new one, and n of
/// n lookups match the oracle in both chunks, hits and misses (sealed hits
/// sampled).
#[tokio::test(flavor = "multi_thread")]
async fn sealed_chunk_incremental_publication_and_exact_answers() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut t = RegistryTopology::launch(
                RegistryOptions::new(3),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let mut oracle = RegistryOracle::new();
            let mut addresses = AddressGen::new(3);
            let fresh = addresses.addresses(3);
            let api = t.write(CLIENT);
            let issued = api.challenge().await;
            let started = Instant::now();
            let bulk = addresses.addresses(REGISTRY_CHUNK_ENTRIES as usize);
            let acks = register_recorded(&api, &issued, &mut oracle, &bulk).await;
            eprintln!("3072 registrations in {:?}", started.elapsed());
            assert!(acks.iter().all(|a| a.chunk_id == 0));
            check_published(&t.publisher, &oracle, &t.identity, 3072, &budget).await;

            t.start_query(&budget).await;
            let wallet = t.query.wallet();
            let old_map = wait_sequence(&wallet, 3072, &budget).await;
            check_map(&old_map, &oracle, &t.identity, 3072);
            assert_eq!(
                registry_scope(&old_map, 0).state,
                tachyon_pir_protocol::ScopeState::Sealed
            );
            assert!(registry_scope(&old_map, 1).tables.is_empty());
            // The empty open chunk right after a seal: absent without a query.
            let queries = wallet.stats().queries;
            for key16 in [fresh[0].key16(), oracle.entries()[0].key16] {
                let probe = Probe { chunk: 1, key16 };
                let outcome = wallet
                    .lookup(&old_map, ScopeSelector::Index(1), &key16)
                    .await
                    .expect("a lookup in the empty open chunk");
                assert!(
                    matches!(outcome, LookupOutcome::Absent { .. }),
                    "{outcome:?}"
                );
                judge(&old_map, &oracle, probe, &outcome);
            }
            assert_eq!(
                wallet.stats().queries,
                queries,
                "no query for an empty chunk"
            );
            let sealed_probes = sample(&hits(&oracle, 0, 3072), 24, 3);
            let tally = check_lookups(&wallet, &old_map, &oracle, &sealed_probes).await;
            assert_eq!(tally.found, sealed_probes.len());

            let old = t.publisher.inventory().await.expect("published");
            t.edge.reset_counts();
            let more = addresses.addresses(40);
            let acks = register_recorded(&api, &issued, &mut oracle, &more).await;
            assert!(
                acks.iter().all(|a| a.chunk_id == 1),
                "after the seal: chunk 1"
            );
            let new = check_published(&t.publisher, &oracle, &t.identity, 3112, &budget).await;
            let new_map = wait_sequence(&wallet, 3112, &budget).await;
            check_map(&new_map, &oracle, &t.identity, 3112);
            assert!(new.inventory.generation > old.inventory.generation);

            let prefix = t.prefix();
            let fetched: BTreeSet<String> = t
                .edge
                .counts()
                .into_keys()
                .filter(|path| Target::Objects.matches(path))
                .collect();
            let held = old.object_paths(prefix);
            let refetched: Vec<&String> = fetched.intersection(&held).collect();
            assert!(
                refetched.is_empty(),
                "objects of the previous publication were fetched again: {refetched:?}"
            );
            let changed: BTreeSet<String> = new
                .object_paths(prefix)
                .difference(&held)
                .cloned()
                .collect();
            let missing: Vec<&String> = changed.difference(&fetched).collect();
            assert!(
                missing.is_empty(),
                "changed objects not fetched: {missing:?}"
            );
            for path in &fetched {
                assert_eq!(t.edge.count(path), 1, "{path} fetched more than once");
            }
            assert_eq!(
                new.inventory.tables.len() - old.inventory.tables.len(),
                1,
                "the open chunk gained one table; the sealed chunk's is unchanged"
            );
            assert_eq!(
                new.inventory.map_chunks, old.inventory.map_chunks,
                "the map chunk of sealed chunk 0"
            );
            assert_eq!(
                sessions(&old_map, 0),
                sessions(&new_map, 0),
                "sealed chunk 0 kept its sessions"
            );

            // The sealed sessions answer under the old map ..
            let tally = check_lookups(&wallet, &old_map, &oracle, &sealed_probes).await;
            assert_eq!(tally.found, sealed_probes.len());
            // .. and both chunks answer exactly under the new one.
            let probes = sampled_hits(&oracle, 3112, 48);
            let tally = check_lookups(&wallet, &new_map, &oracle, &probes).await;
            assert_eq!(tally.found, probes.len());
            assert_eq!(tally.found, 48 + 40);
            let missing = misses(&oracle, &fresh, 3112, 12);
            let tally = check_lookups(&wallet, &new_map, &oracle, &missing).await;
            assert_eq!(tally.absent, missing.len());
            eprintln!("sealed + open: done in {:?}", budget.elapsed());
        })
        .await
}

/// Scenario 6: addresses ground so that under every seed seven `key16`s
/// share one row of chunk 0 (one more than a row's six slots) force seed 0
/// with overflow segments holding exactly the entries beyond six per seed-0
/// row. Every one of the 448 ground addresses (the chunk's whole content) is
/// found, each lookup querying every segment, and misses are absent.
#[tokio::test(flavor = "multi_thread")]
async fn forced_overflow_keeps_every_address_retrievable() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut t = RegistryTopology::launch(
                RegistryOptions::new(6),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let started = Instant::now();
            let mut ground =
                tachyon_pir_e2e_tests::registry_checks::grind_overflow(&t.identity.genesis, 0, 6);
            eprintln!(
                "ground {} addresses in {:?}",
                ground.len(),
                started.elapsed()
            );
            assert_eq!(ground.len(), 64 * 7);
            // Interleave the seeds' groups through the log.
            ground.sort_by_key(OracleAddress::key16);
            let mut oracle = RegistryOracle::new();
            let api = t.write(CLIENT);
            let issued = api.challenge().await;
            register_recorded(&api, &issued, &mut oracle, &ground).await;
            let log_sequence = oracle.len();
            let publication =
                check_published(&t.publisher, &oracle, &t.identity, log_sequence, &budget).await;
            let (_, scopes) = t.publisher.listing(&publication).await;
            let chunk = &scopes[0];
            assert!(
                chunk.tables.len() >= 2,
                "overflow segments: {:?}",
                chunk.tables
            );
            assert_eq!(
                chunk.tables[0].seed, 0,
                "no seed fits: segment 0 under seed 0 (spec 6.4)"
            );
            let overflow = t
                .publisher
                .metric("tachyon_registry_segments_overflow_total")
                .await;
            assert_eq!(overflow, (chunk.tables.len() - 1) as f64);

            t.start_query(&budget).await;
            let wallet = t.query.wallet();
            let map = wait_sequence(&wallet, log_sequence, &budget).await;
            check_map(&map, &oracle, &t.identity, log_sequence);
            // The overflow segments hold exactly the entries beyond six per
            // seed-0 row (segment 0 is filled under seed 0, spec 6.4).
            let mut rows: HashMap<u32, usize> = HashMap::new();
            for registered in oracle.entries() {
                let row = row_under_seed0(
                    Product::Registry,
                    &t.identity.block_hash(),
                    0,
                    &registered.key16,
                );
                *rows.entry(row).or_default() += 1;
            }
            let spilled: usize = rows.values().map(|n| n.saturating_sub(6)).sum();
            assert!(spilled > 0, "a seed-0 row overflows");
            assert_eq!(
                chunk.tables[1..].iter().map(|t| t.entries).sum::<u64>(),
                spilled as u64,
                "the overflow segments hold exactly the entries beyond six per seed-0 row"
            );
            // Every ground address, each lookup querying every segment.
            let found = hits(&oracle, 0, log_sequence);
            assert_eq!(found.len(), ground.len());
            let tally = check_lookups(&wallet, &map, &oracle, &found).await;
            assert_eq!(tally.found, ground.len(), "every ground address found");
            let fresh = AddressGen::new(66).addresses(4);
            let missing = misses(&oracle, &fresh, log_sequence, 0);
            let tally = check_lookups(&wallet, &map, &oracle, &missing).await;
            assert_eq!(tally.absent, missing.len());
            eprintln!(
                "overflow: {} segments, {} lookups in {:?}",
                chunk.tables.len(),
                found.len() + missing.len(),
                budget.elapsed()
            );
        })
        .await
}

/// Scenario 10, for one fault on the new open-chunk table's rows object:
/// the object is refused (the pass fails), the query host keeps serving the
/// previous publication with exact answers (the newer registrations absent),
/// and switches once the object arrives intact. The host's next pass is held
/// at `current` while the log grows, so the fault is in place before the new
/// objects are requested.
async fn refused_object(seed: u64, fault: Fault) {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            // Registered before the query host starts, so its first sync is
            // the one table (the 10 s back-off leaves little room).
            let mut t = RegistryTopology::launch(
                RegistryOptions::new(seed),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let mut oracle = RegistryOracle::new();
            let mut addresses = AddressGen::new(seed);
            let fresh = addresses.addresses(2);
            let api = t.write(CLIENT);
            let issued = api.challenge().await;
            register_recorded(&api, &issued, &mut oracle, &addresses.addresses(12)).await;
            t.publisher.wait_log_sequence(12, wait_limit(&budget)).await;
            t.start_query(&budget).await;
            let wallet = t.query.wallet();
            let old_map = wait_sequence(&wallet, 12, &budget).await;
            let old = t.publisher.inventory().await.expect("published");
            let failed = "sync_passes_total{result=\"failed\"}";

            let hold = t
                .edge
                .inject(Target::Suffix("/current".into()), Fault::Hold);
            wait_for("a pass held at current", wait_limit(&budget), || async {
                if t.edge.held() > 0 {
                    Ok(())
                } else {
                    Err("none held".to_owned())
                }
            })
            .await;
            register_recorded(&api, &issued, &mut oracle, &addresses.addresses(6)).await;
            let new = t.publisher.wait_log_sequence(18, wait_limit(&budget)).await;
            let rebuilt: Vec<String> = new
                .inventory
                .tables
                .iter()
                .filter(|table| !old.inventory.tables.contains(table))
                .map(|table| format!("{}objects/{}", t.prefix(), table.rows.sha256))
                .collect();
            assert_eq!(rebuilt.len(), 1, "one rebuilt table");
            let rows = rebuilt[0].clone();
            let failed_before = t.query.metric(failed).await;
            t.edge.inject(Target::Path(rows.clone()), fault);
            t.edge.remove(hold);
            wait_for(
                "the faulted rows object refused",
                wait_limit(&budget),
                || async {
                    let fetched = t.edge.count(&rows);
                    let failures = t.query.metric(failed).await - failed_before;
                    if fetched > 0 && failures > 0.0 {
                        Ok(())
                    } else {
                        Err(format!("{fetched} fetches, {failures} failed passes"))
                    }
                },
            )
            .await;

            // The previous publication keeps serving, exactly.
            let ready = t.query.management_get("ready").await;
            assert_eq!(ready.status, StatusCode::OK, "{}", ready.text());
            let map = wallet.fetch_map().await.expect("the previous map");
            assert_eq!(
                map.map_sha256(),
                old_map.map_sha256(),
                "the previous publication serves"
            );
            let mut probes = hits(&oracle, 0, 12);
            probes.extend(misses(&oracle, &fresh, 12, 6));
            let tally = check_lookups(&wallet, &map, &oracle, &probes).await;
            assert_eq!((tally.found, tally.absent), (12, probes.len() - 12));

            t.edge.clear_faults();
            let new_map = wait_sequence(&wallet, 18, &budget).await;
            assert_eq!(registry_head(&new_map).generation, new.inventory.generation);
            assert!(t.edge.count(&rows) >= 2, "fetched again intact");
            check_map(&new_map, &oracle, &t.identity, 18);
            let mut probes = hits(&oracle, 0, 18);
            probes.extend(misses(&oracle, &fresh, 18, 0));
            let tally = check_lookups(&wallet, &new_map, &oracle, &probes).await;
            assert_eq!(tally.found, 18);
            eprintln!("switched after {:?}", budget.elapsed());
        })
        .await
}

/// Scenario 10, corrupt: the rows object arrives with one bit flipped.
#[tokio::test(flavor = "multi_thread")]
async fn corrupt_object_is_refused_and_previous_publication_serves() {
    refused_object(10, Fault::CorruptBody).await;
}

/// Scenario 10, truncated: the rows object's connection is cut after half
/// its body.
#[tokio::test(flavor = "multi_thread")]
async fn truncated_object_is_refused_and_previous_publication_serves() {
    refused_object(20, Fault::TruncateBody).await;
}

/// Scenario 11: with the publisher unreachable the query host keeps serving
/// exact answers for `max_unconfirmed_seconds` after its last successful
/// authority check started, then answers `503`, and serves the same
/// publication again once the publisher is back.
#[tokio::test(flavor = "multi_thread")]
async fn publisher_outage_serves_within_authority_then_503() {
    const WINDOW: Duration = Duration::from_secs(3);
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let options = RegistryOptions::new(11)
                .query_setting("max_unconfirmed_seconds", json!(WINDOW.as_secs()));
            let t =
                RegistryTopology::start(options, publisher_binary(), query_binary(), &budget).await;
            let mut oracle = RegistryOracle::new();
            let mut addresses = AddressGen::new(11);
            let fresh = addresses.addresses(2);
            let api = t.write(CLIENT);
            let issued = api.challenge().await;
            register_recorded(&api, &issued, &mut oracle, &addresses.addresses(10)).await;
            let wallet = t.query.wallet();
            let map = wait_sequence(&wallet, 10, &budget).await;
            let mut probes = hits(&oracle, 0, 10);
            probes.extend(misses(&oracle, &fresh, 10, 0));
            check_lookups(&wallet, &map, &oracle, &probes).await;

            t.edge.outage().await;
            let down = Instant::now();
            let mut answered = 0;
            let refused_after = loop {
                let probe = probes[answered % probes.len()];
                match wallet
                    .lookup(&map, ScopeSelector::Index(probe.chunk), &probe.key16)
                    .await
                {
                    Ok(outcome) => {
                        judge(&map, &oracle, probe, &outcome);
                        answered += 1;
                    }
                    Err(ClientError::Unavailable { body, .. }) => {
                        assert_eq!(body.code.as_str(), "temporarily_unavailable");
                        break down.elapsed();
                    }
                    Err(error) => panic!("during the outage: {}", describe(&error)),
                }
                assert!(
                    down.elapsed() < WINDOW + Duration::from_secs(3),
                    "still serving {:?} into the outage, beyond max_unconfirmed_seconds",
                    down.elapsed()
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            };
            assert!(
                refused_after >= WINDOW - Duration::from_secs(1),
                "refused after {refused_after:?}, before the {WINDOW:?} window ran out"
            );
            assert!(answered > 5, "only {answered} answers inside the window");
            eprintln!("outage: {answered} exact answers, 503 after {refused_after:?}");
            match wallet.fetch_map().await {
                Err(ClientError::Unavailable { .. }) => {}
                other => panic!("map after the window: {other:?}"),
            }

            t.edge.resume().await.expect("the edge listens again");
            let back = wait_map(
                &wallet,
                "serving after the outage",
                wait_limit(&budget),
                |_| true,
            )
            .await;
            assert_eq!(
                back.map_sha256(),
                map.map_sha256(),
                "the same publication serves again"
            );
            check_lookups(&wallet, &back, &oracle, &probes).await;
        })
        .await
}

/// Scenario 12: the publisher is killed (`SIGKILL`) right after a `200`,
/// then restarted; the query host is then crashed and restarted. The
/// publisher republishes under a new recovery epoch and the acknowledged
/// entry is in its log (a repeat is `created: false` at the same sequence)
/// and in the next map, found by lookup; the query host recovers from its
/// state directory without fetching a table object again. A prober running
/// throughout never sees a wrong answer.
#[tokio::test(flavor = "multi_thread")]
async fn restart_of_each_process_recovers_without_wrong_answers() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut t = RegistryTopology::start(
                RegistryOptions::new(12),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let mut addresses = AddressGen::new(12);
            let fresh = addresses.addresses(2);
            let api = t.write(CLIENT);
            let issued = api.challenge().await;
            let mut record = RegistryOracle::new();
            register_recorded(&api, &issued, &mut record, &addresses.addresses(16)).await;
            let oracle = Arc::new(Mutex::new(record));
            let wallet = t.query.wallet();
            let map = wait_sequence(&wallet, 16, &budget).await;
            let snapshot = oracle.lock().unwrap().clone();
            let mut probes = hits(&snapshot, 0, 16);
            probes.extend(misses(&snapshot, &fresh, 16, 0));
            check_lookups(&wallet, &map, &snapshot, &probes).await;
            let old = t.publisher.inventory().await.expect("published");

            // Publisher crash right after an acknowledgement.
            let stop = Arc::new(AtomicBool::new(false));
            let prober = spawn_prober(t.query.wallet(), oracle.clone(), probes.clone(), false, stop.clone());
            t.edge.reset_counts();
            let last = addresses.address();
            let ack = api.register(&issued, &last).await;
            t.publisher.process().kill();
            oracle.lock().unwrap().record(&last, ack);
            assert_eq!(ack.visible_after_sequence, 16);
            t.publisher.process().start();
            t.publisher.wait_ready(wait_limit(&budget)).await;
            let new = t
                .publisher
                .wait_publication("the republication with the acknowledged entry", wait_limit(&budget), |p| {
                    p.inventory.recovery_epoch > old.inventory.recovery_epoch && subject_sequence(p) == 17
                })
                .await;
            assert!(new.inventory.generation > old.inventory.generation);
            // The acknowledged entry is in the log: a repeat is not created.
            let repeat = api.register(&issued, &last).await;
            assert!(!repeat.created);
            oracle.lock().unwrap().record(&last, repeat);
            let republished = wait_map(&wallet, "the republished map", wait_limit(&budget), |m| {
                registry_head(m).generation == new.inventory.generation
            })
            .await;
            let snapshot = oracle.lock().unwrap().clone();
            check_map(&republished, &snapshot, &t.identity, 17);
            let mut after = probes.clone();
            after.push(Probe { chunk: 0, key16: last.key16() });
            let tally = check_lookups(&wallet, &republished, &snapshot, &after).await;
            assert_eq!(tally.found, 17, "the entry acknowledged before the crash is served");
            let tables = old.table_object_paths(t.prefix());
            let refetched: Vec<String> =
                t.edge.counts().into_keys().filter(|path| tables.contains(path)).collect();
            assert!(refetched.is_empty(), "table objects fetched again: {refetched:?}");
            stop.store(true, Ordering::Relaxed);
            let seen = prober.await.expect("the prober saw no wrong answer");
            assert!(seen.answers > 0, "{seen:?}");
            eprintln!("publisher restart: {seen:?}");

            // Query host crash.
            t.edge.reset_counts();
            let stop = Arc::new(AtomicBool::new(false));
            let prober = spawn_prober(t.query.wallet(), oracle.clone(), after.clone(), true, stop.clone());
            let served = new;
            t.query.crash_restart(wait_limit(&budget)).await;
            let wallet = t.query.wallet();
            let recovered = wallet.fetch_map().await.expect("a map right after ready");
            assert_eq!(registry_head(&recovered).generation, served.inventory.generation);
            check_map(&recovered, &snapshot, &t.identity, 17);
            let tally = check_lookups(&wallet, &recovered, &snapshot, &after).await;
            assert_eq!(tally.found, 17);
            stop.store(true, Ordering::Relaxed);
            let seen = prober.await.expect("the prober saw no wrong answer");
            assert!(seen.answers > 0, "{seen:?}");
            let tables = served.table_object_paths(t.prefix());
            let refetched: Vec<String> =
                t.edge.counts().into_keys().filter(|path| tables.contains(path)).collect();
            assert!(
                refetched.is_empty(),
                "the restarted query host refetched table objects its state directory holds: {refetched:?}"
            );
            eprintln!("query host restart: {seen:?}");
        })
        .await
}

/// Scenario 13: with admission at one executing and one waiting query, a
/// burst of concurrent lookups is partly refused with `429`,
/// `Retry-After: 1` and the `overloaded` body, while every admitted lookup
/// completes with the exact answer.
#[tokio::test(flavor = "multi_thread")]
async fn overload_refuses_with_retry_after_while_admitted_complete() {
    const BURST: usize = 32;
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let options = RegistryOptions::new(13).query_setting(
                "admission",
                json!({"executing": 1, "waiting": 1, "wait_ms": 200, "per_client": 64}),
            );
            let t =
                RegistryTopology::start(options, publisher_binary(), query_binary(), &budget).await;
            let mut oracle = RegistryOracle::new();
            let mut addresses = AddressGen::new(13);
            let fresh = addresses.addresses(8);
            let api = t.write(CLIENT);
            let issued = api.challenge().await;
            register_recorded(&api, &issued, &mut oracle, &addresses.addresses(24)).await;
            let wallet = t.query.wallet();
            let map = wait_sequence(&wallet, 24, &budget).await;
            let mut probes = hits(&oracle, 0, 24);
            probes.extend(misses(&oracle, &fresh, 24, 0));
            // One lookup first, so the burst is queries only.
            check_lookups(&wallet, &map, &oracle, &probes[..1]).await;
            let overloaded_before = t.query.metric("queries_total{result=\"overloaded\"}").await;

            let barrier = Arc::new(tokio::sync::Barrier::new(BURST));
            let tasks: Vec<_> = probes
                .iter()
                .copied()
                .take(BURST)
                .map(|probe| {
                    let (wallet, map, barrier) = (wallet.clone(), map.clone(), barrier.clone());
                    tokio::spawn(async move {
                        barrier.wait().await;
                        let outcome = wallet
                            .lookup(&map, ScopeSelector::Index(probe.chunk), &probe.key16)
                            .await;
                        (probe, outcome)
                    })
                })
                .collect();
            assert_eq!(tasks.len(), BURST);
            let (mut admitted, mut refused) = (0, 0);
            for task in tasks {
                let (probe, outcome) = task.await.expect("lookup task");
                match outcome {
                    Ok(outcome) => {
                        judge(&map, &oracle, probe, &outcome);
                        admitted += 1;
                    }
                    Err(ClientError::Overloaded {
                        retry_after, body, ..
                    }) => {
                        assert_eq!(retry_after, Some(Duration::from_secs(1)));
                        assert_eq!(body.code.as_str(), "overloaded");
                        refused += 1;
                    }
                    Err(error) => panic!("burst lookup: {}", describe(&error)),
                }
            }
            eprintln!("overload: {admitted} admitted, {refused} refused");
            assert!(refused > 0, "no query was refused");
            assert!(admitted > 0, "no query was admitted");
            let overloaded = t.query.metric("queries_total{result=\"overloaded\"}").await;
            assert_eq!(overloaded - overloaded_before, refused as f64);
            // Admission recovers: sequential lookups all succeed.
            for probe in probes.iter().take(6) {
                check_lookups(&wallet, &map, &oracle, &[*probe]).await;
            }
        })
        .await
}

// ---------------------------------------------------------------------------
// Scenario 14: registry writes.

/// `HMAC-SHA256(key, message)` (RFC 2104), from `sha2` alone.
fn hmac_sha256(key: &[u8; 32], message: &[u8]) -> [u8; 32] {
    let mut block = [0u8; 64];
    block[..32].copy_from_slice(key);
    let pad = |byte: u8| block.map(|b| b ^ byte);
    let inner: [u8; 32] = Sha256::new()
        .chain_update(pad(0x36))
        .chain_update(message)
        .finalize()
        .into();
    Sha256::new()
        .chain_update(pad(0x5c))
        .chain_update(inner)
        .finalize()
        .into()
}

/// The challenge of spec 9.3 for `expires_unix` under the registry's
/// `challenge-secret`: `expires_unix (u64 LE) ||
/// HMAC-SHA256(secret, "tachyon-pir/registry/v1/challenge\0" || expires_unix LE)[0..24]`.
fn challenge_for(secret: &[u8; 32], expires_unix: u64) -> [u8; 32] {
    let mut message = b"tachyon-pir/registry/v1/challenge\0".to_vec();
    message.extend_from_slice(&expires_unix.to_le_bytes());
    let tag = hmac_sha256(secret, &message);
    let mut challenge = [0u8; 32];
    challenge[..8].copy_from_slice(&expires_unix.to_le_bytes());
    challenge[8..].copy_from_slice(&tag[..24]);
    challenge
}

/// Scenario 14, validation: the challenge format; idempotence (a repeat is
/// `created: false` at the same sequence and chunk); proof of work enforced
/// (`400`, counted in `tachyon_registry_pow_rejected_total`); a forged, a
/// re-dated and an expired challenge, a non-canonical `pk` and an `ek`
/// failing the modulus check each `400` naming the check, with the values
/// just inside each boundary accepted; the validation order; body length
/// (`400` short, `413` long); unknown route `404` and wrong method `405`;
/// and every accepted address published.
#[tokio::test(flavor = "multi_thread")]
async fn registry_writes_are_validated_idempotent_and_proof_of_work_bound() {
    const BITS: u32 = 10;
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let chain = identity_chain(14);
            let identity = Identity::of(&chain);
            let node = start_node(chain).await;
            let settings = RegistrySettings {
                difficulty_bits: BITS,
                ..RegistrySettings::bulk()
            };
            let publisher = RegistryPublisher::start(
                publisher_binary(),
                &node.url(),
                settings,
                wait_limit(&budget),
            )
            .await;
            let api = publisher.write(CLIENT);
            let issued = api.challenge().await;
            assert_eq!(issued.difficulty_bits, BITS);
            let legacy = client::get(
                &client::plain(),
                &format!("http://{}/v1/registry/challenge", publisher.listen()),
            )
            .await;
            legacy.assert_status(StatusCode::OK, "legacy challenge");
            let mut oracle = RegistryOracle::new();
            let mut addresses = AddressGen::new(14);
            let metric = |result: &str| {
                format!("tachyon_registry_registrations_total{{result=\"{result}\"}}")
            };

            // Idempotence.
            let a = addresses.address();
            let first = api.register(&issued, &a).await;
            oracle.record(&a, first);
            let other = api.challenge().await;
            let again = api.register(&other, &a).await;
            assert!(!again.created);
            oracle.record(&a, again);
            assert_eq!(
                again,
                Ack {
                    created: false,
                    ..first
                }
            );
            assert_eq!(publisher.metric(&metric("existing")).await, 1.0);

            // Proof of work: a nonce short of the difficulty, and a solution
            // for another address (the work binds the digest).
            let b = addresses.address();
            let reply = api
                .post(register_body(
                    &issued.challenge,
                    &unsolved(&issued.challenge, &b, BITS),
                    &b,
                ))
                .await;
            assert_refusal(
                &reply,
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "proof of work does not meet 10 bits",
            );
            let borrowed = solve(&issued.challenge, &a, BITS);
            if tachyon_pir_e2e_tests::registry_topology::pow_bits(&issued.challenge, &borrowed, &b)
                < BITS
            {
                let reply = api
                    .post(register_body(&issued.challenge, &borrowed, &b))
                    .await;
                assert_refusal(
                    &reply,
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "proof of work",
                );
                assert_eq!(
                    publisher
                        .metric("tachyon_registry_pow_rejected_total")
                        .await,
                    2.0
                );
            } else {
                assert_eq!(
                    publisher
                        .metric("tachyon_registry_pow_rejected_total")
                        .await,
                    1.0
                );
            }
            assert_eq!(
                publisher
                    .metric("tachyon_registry_pow_rejected_total")
                    .await,
                publisher.metric(&metric("pow_rejected")).await
            );
            let ack = api.register(&issued, &b).await;
            oracle.record(&b, ack);

            // Challenges: forged tag, re-dated, expired (authentic, built
            // from the state directory's secret), and one built the same
            // way that has not expired, which is accepted.
            let secret: [u8; 32] = std::fs::read(publisher.state_dir().join("challenge-secret"))
                .expect("the challenge secret")
                .try_into()
                .expect("32 bytes");
            assert_eq!(
                challenge_for(&secret, issued.expires_unix),
                issued.challenge,
                "the issued challenge is the spec's HMAC construction"
            );
            let c = addresses.address();
            let mut forged = issued.challenge;
            forged[31] ^= 1;
            let mut redated = issued.challenge;
            redated[..8].copy_from_slice(&(issued.expires_unix + 1).to_le_bytes());
            for (challenge, message) in [
                (forged, "challenge is not authentic"),
                (redated, "challenge is not authentic"),
                (
                    challenge_for(&secret, now_unix() - 1),
                    "challenge has expired",
                ),
            ] {
                let nonce = solve(&challenge, &c, BITS);
                let reply = api.post(register_body(&challenge, &nonce, &c)).await;
                assert_refusal(&reply, StatusCode::BAD_REQUEST, "invalid_request", message);
            }
            let own = Issued {
                challenge: challenge_for(&secret, now_unix() + 30),
                ..issued
            };
            let ack = api.register(&own, &c).await;
            oracle.record(&c, ack);

            // pk canonical (spec 9.1): p and 2^256 - 1 refused, p - 1 accepted.
            let base = addresses.address();
            for bad in [invalid::pk_equal_p(&base), invalid::pk_all_ones(&base)] {
                let reply = api.register_reply(&issued, &bad).await;
                assert_refusal(
                    &reply,
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "pk is not a canonical",
                );
            }
            let top = invalid::pk_p_minus_one(&base);
            let ack = api.register(&issued, &top).await;
            oracle.record(&top, ack);

            // ek modulus check: q and 4095 in either half of a 3-byte group
            // refused, q - 1 accepted.
            for (index, value) in [(0, ML_KEM_Q), (767, ML_KEM_Q), (300, 4095), (1, ML_KEM_Q)] {
                let bad = invalid::ek_coefficient(&base, index, value);
                let reply = api.register_reply(&issued, &bad).await;
                assert_refusal(
                    &reply,
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "ek fails the ML-KEM modulus check",
                );
            }
            let edge = invalid::ek_coefficient(&base, 767, ML_KEM_Q - 1);
            let ack = api.register(&issued, &edge).await;
            oracle.record(&edge, ack);

            // Validation order: challenge, proof of work, pk, ek.
            let both =
                invalid::ek_coefficient(&invalid::pk_equal_p(&addresses.address()), 5, ML_KEM_Q);
            let cases = [
                (
                    forged,
                    unsolved(&forged, &both, BITS),
                    "challenge is not authentic",
                ),
                (
                    issued.challenge,
                    unsolved(&issued.challenge, &both, BITS),
                    "proof of work",
                ),
                (
                    issued.challenge,
                    solve(&issued.challenge, &both, BITS),
                    "pk is not a canonical",
                ),
            ];
            for (challenge, nonce, message) in cases {
                let reply = api.post(register_body(&challenge, &nonce, &both)).await;
                assert_refusal(&reply, StatusCode::BAD_REQUEST, "invalid_request", message);
            }
            let ek_only = invalid::ek_coefficient(&addresses.address(), 5, ML_KEM_Q);
            let reply = api
                .post(register_body(
                    &issued.challenge,
                    &unsolved(&issued.challenge, &ek_only, BITS),
                    &ek_only,
                ))
                .await;
            assert_refusal(
                &reply,
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "proof of work",
            );

            // Body length: exactly 1,256 bytes.
            let body = register_body(
                &issued.challenge,
                &solve(&issued.challenge, &base, BITS),
                &base,
            );
            for short in [0, 1, REGISTER_BODY_BYTES - 1] {
                let reply = api.post(body[..short].to_vec()).await;
                assert_refusal(
                    &reply,
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "1256 bytes",
                );
            }
            for long in [REGISTER_BODY_BYTES + 1, 4096] {
                let mut longer = body.clone();
                longer.resize(long, 0);
                let reply = api.post(longer).await;
                assert_refusal(
                    &reply,
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "invalid_request",
                    "1256",
                );
            }
            assert_eq!(publisher.metric(&metric("too_large")).await, 2.0);

            // Routes.
            let reply = client::get(&client::plain(), &api.url("register")).await;
            assert_refusal(
                &reply,
                StatusCode::METHOD_NOT_ALLOWED,
                "invalid_request",
                "",
            );
            let reply = client::get(&client::plain(), &api.url("nothing-here")).await;
            assert_refusal(&reply, StatusCode::NOT_FOUND, "invalid_request", "");

            // Only the accepted addresses were appended, and are published.
            oracle.assert_contiguous();
            assert_eq!(oracle.len(), 5);
            assert_eq!(publisher.metric(&metric("created")).await, 5.0);
            check_published(&publisher, &oracle, &identity, oracle.len(), &budget).await;
        })
        .await
}

/// Scenario 14, abuse controls (spec 9.3) on one publisher with 2
/// concurrent registrations and 4 per minute per client and 40 requests per
/// minute in total: a third concurrent upload is `429` with `Retry-After:
/// 1` while another client is unaffected; a fifth registration in the
/// minute is `429` with a `Retry-After` in `1..=60` while another client is
/// unaffected; a request admitted and refused for its body counts; a body
/// not complete within `upload_ms` is `408`; once the global window is
/// full, every client and the challenge route are `429`.
#[tokio::test(flavor = "multi_thread")]
async fn registry_rate_limits_refuse_with_retry_after_per_client_and_globally() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let node = start_node(identity_chain(15)).await;
            let settings = RegistrySettings {
                difficulty_bits: 4,
                per_client_concurrent: 2,
                per_client_per_minute: 4,
                global_per_minute: 40,
                upload_ms: 1500,
                publish_ms: 100,
            };
            let publisher = RegistryPublisher::start(
                publisher_binary(),
                &node.url(),
                settings,
                wait_limit(&budget),
            )
            .await;
            let listen = publisher.listen();
            let mut addresses = AddressGen::new(15);
            let mut oracle = RegistryOracle::new();
            let client_a = publisher.write("198.51.100.1");
            let client_b = publisher.write("198.51.100.2");
            let issued = client_a.challenge().await; // global: 1
            let body_for = |address: &OracleAddress| {
                register_body(
                    &issued.challenge,
                    &solve(&issued.challenge, address, 4),
                    address,
                )
            };
            let metric = |result: &str| {
                format!("tachyon_registry_registrations_total{{result=\"{result}\"}}")
            };

            // Per-client concurrency: two uploads of A held open.
            let held_addresses = addresses.addresses(2);
            let mut held = Vec::new();
            for address in &held_addresses {
                held.push(
                    HeldUpload::start(
                        listen,
                        "198.51.100.1",
                        body_for(address),
                        wait_limit(&budget),
                    )
                    .await,
                );
            } // global: 3, A: 2
            let third = addresses.address();
            let reply = client_a.register_reply(&issued, &third).await;
            assert_refusal(
                &reply,
                StatusCode::TOO_MANY_REQUESTS,
                "overloaded",
                "too many registrations in flight",
            );
            assert_eq!(reply.header("retry-after"), Some("1"));
            assert_eq!(publisher.metric(&metric("overloaded")).await, 1.0);
            let b1 = addresses.address();
            let ack = client_b.register(&issued, &b1).await; // global: 4, B: 1
            oracle.record(&b1, ack);
            for (upload, address) in held.into_iter().zip(&held_addresses) {
                let (status, body) = upload.finish().await;
                assert_eq!(status, 200, "a held upload completes: {body}");
                let reply = client::Reply {
                    status: StatusCode::OK,
                    headers: Default::default(),
                    body: body.into_bytes().into(),
                };
                oracle.record(address, parse_ack(&reply));
            }
            let ack = client_a.register(&issued, &third).await; // global: 5, A: 3
            oracle.record(&third, ack);

            // Per-client rate: A's fourth is admitted, its fifth refused.
            let fourth = addresses.address();
            let ack = client_a.register(&issued, &fourth).await; // global: 6, A: 4
            oracle.record(&fourth, ack);
            let fifth = addresses.address();
            let reply = client_a.register_reply(&issued, &fifth).await;
            assert_refusal(
                &reply,
                StatusCode::TOO_MANY_REQUESTS,
                "overloaded",
                "this client's registration rate is exhausted",
            );
            let retry: u64 = reply
                .header("retry-after")
                .expect("Retry-After")
                .parse()
                .unwrap();
            assert!((1..=60).contains(&retry), "Retry-After {retry}");
            assert_eq!(publisher.metric(&metric("rate_limited")).await, 1.0);
            let b2 = addresses.address();
            let ack = client_b.register(&issued, &b2).await; // global: 7, B: 2
            oracle.record(&b2, ack);

            // A request admitted and refused for its body counts: C sends four
            // short bodies, then a valid registration is over C's rate.
            let client_c = publisher.write("198.51.100.3");
            for _ in 0..4 {
                let reply = client_c.post(vec![0u8; 100]).await;
                assert_refusal(
                    &reply,
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "1256 bytes",
                );
            } // global: 11, C: 4
            let reply = client_c.register_reply(&issued, &addresses.address()).await;
            assert_refusal(
                &reply,
                StatusCode::TOO_MANY_REQUESTS,
                "overloaded",
                "rate is exhausted",
            );

            // Upload deadline: a held body is 408 after upload_ms.
            let late = addresses.address();
            let started = Instant::now();
            let upload =
                HeldUpload::start(listen, "198.51.100.4", body_for(&late), wait_limit(&budget))
                    .await;
            let (status, body) = upload.response().await; // global: 12
            assert_eq!(status, 408, "{body}");
            assert!(body.contains("upload_timeout"), "{body}");
            assert!(
                started.elapsed() >= Duration::from_millis(1400),
                "{:?}",
                started.elapsed()
            );
            assert_eq!(publisher.metric(&metric("upload_timeout")).await, 1.0);

            // The global cap: fill the window with challenges, then every
            // client and route is refused.
            let mut challenges = 0;
            let refused = loop {
                let reply = client_b.challenge_reply().await;
                if reply.status == StatusCode::TOO_MANY_REQUESTS {
                    break reply;
                }
                reply.assert_status(StatusCode::OK, "challenge");
                challenges += 1;
                assert!(challenges <= 40, "the global cap never refused");
            };
            assert_eq!(
                challenges,
                40 - 12,
                "requests admitted in the window so far: 12"
            );
            assert_refusal(
                &refused,
                StatusCode::TOO_MANY_REQUESTS,
                "overloaded",
                "global rate is exhausted",
            );
            let fresh_client = publisher.write("198.51.100.99");
            let reply = fresh_client
                .register_reply(&issued, &addresses.address())
                .await;
            assert_refusal(
                &reply,
                StatusCode::TOO_MANY_REQUESTS,
                "overloaded",
                "global rate is exhausted",
            );
            let retry: u64 = reply
                .header("retry-after")
                .expect("Retry-After")
                .parse()
                .unwrap();
            assert!((1..=60).contains(&retry), "Retry-After {retry}");

            // Everything accepted is published, nothing refused was appended.
            oracle.assert_contiguous();
            assert_eq!(oracle.len(), 6);
            let identity = Identity::of(&node.chain());
            check_published(&publisher, &oracle, &identity, 6, &budget).await;
        })
        .await
}

/// Scenario 14, durability and visibility: each registration is contained
/// in the first map with `log_sequence > visible_after_sequence` (found by
/// lookup through the query host) and absent from the map before it; a
/// concurrent batch is likewise contained once its sequences are covered;
/// and a registration whose `200` is followed at once by `SIGKILL` of the
/// publisher survives: after the restart a repeat is `created: false` at
/// the same sequence and the next map contains it.
#[tokio::test(flavor = "multi_thread")]
async fn registrations_are_durable_and_visible_at_the_promised_sequence() {
    let _serial = exclusive().await;
    let budget = Budget::start();
    budget
        .run(async {
            let mut t = RegistryTopology::start(
                RegistryOptions::new(16),
                publisher_binary(),
                query_binary(),
                &budget,
            )
            .await;
            let mut oracle = RegistryOracle::new();
            let mut addresses = AddressGen::new(16);
            let api = t.write(CLIENT);
            let issued = api.challenge().await;
            let wallet = t.query.wallet();
            let mut before: Arc<MapView> = wait_sequence(&wallet, 0, &budget).await;

            // One at a time: the first map past each sequence contains it.
            for _ in 0..2 {
                let address = addresses.address();
                let ack = api.register(&issued, &address).await;
                oracle.record(&address, ack);
                let s = ack.visible_after_sequence;
                assert!(registry_head(&before).log_sequence <= s);
                let probe = Probe {
                    chunk: ack.chunk_id,
                    key16: address.key16(),
                };
                let outcome = wallet
                    .lookup(
                        &before,
                        ScopeSelector::Index(ack.chunk_id),
                        &address.key16(),
                    )
                    .await;
                match outcome {
                    Ok(outcome) => {
                        assert!(matches!(outcome, LookupOutcome::Absent { .. }));
                        judge(&before, &oracle, probe, &outcome);
                    }
                    Err(error) => panic!("lookup in the map before: {}", describe(&error)),
                }
                let map = wait_map(
                    &wallet,
                    "a map past the sequence",
                    wait_limit(&budget),
                    |m| registry_head(m).log_sequence > s,
                )
                .await;
                assert_eq!(
                    registry_head(&map).log_sequence,
                    s + 1,
                    "the first map past {s}"
                );
                let tally = check_lookups(&wallet, &map, &oracle, &[probe]).await;
                assert_eq!(tally.found, 1, "visible in the first map past its sequence");
                before = map;
            }

            // A concurrent batch: every map covering a sequence contains it.
            let batch = addresses.addresses(12);
            let acks = register_recorded(&api, &issued, &mut oracle, &batch).await;
            let top = acks.iter().map(|a| a.visible_after_sequence).max().unwrap();
            let map = wait_sequence(&wallet, top + 1, &budget).await;
            check_map(&map, &oracle, &t.identity, top + 1);
            let tally = check_lookups(&wallet, &map, &oracle, &hits(&oracle, 0, top + 1)).await;
            assert_eq!(tally.found, oracle.len() as usize);

            // Durable before acknowledgement: SIGKILL right after the 200.
            let epoch = t
                .publisher
                .inventory()
                .await
                .expect("published")
                .inventory
                .recovery_epoch;
            let last = addresses.address();
            let ack = api.register(&issued, &last).await;
            t.publisher.process().kill();
            oracle.record(&last, ack);
            t.publisher.process().start();
            t.publisher.wait_ready(wait_limit(&budget)).await;
            let repeat = api.register(&issued, &last).await;
            assert!(!repeat.created, "the acknowledged entry survived the kill");
            oracle.record(&last, repeat);
            let s = ack.visible_after_sequence;
            let republished = t
                .publisher
                .wait_publication(
                    "the republication past the killed registration",
                    wait_limit(&budget),
                    |p| p.inventory.recovery_epoch > epoch && subject_sequence(p) > s,
                )
                .await;
            assert_eq!(subject_sequence(&republished), s + 1);
            let map = wait_map(
                &wallet,
                "the map past the killed registration",
                wait_limit(&budget),
                |m| registry_head(m).generation == republished.inventory.generation,
            )
            .await;
            check_map(&map, &oracle, &t.identity, s + 1);
            let probe = Probe {
                chunk: ack.chunk_id,
                key16: last.key16(),
            };
            let tally = check_lookups(&wallet, &map, &oracle, &[probe]).await;
            assert_eq!(tally.found, 1, "the killed registration is served");
        })
        .await
}
