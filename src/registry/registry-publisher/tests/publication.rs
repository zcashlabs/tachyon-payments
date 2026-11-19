//! Publication (spec 9.2, 10.3; scenarios 1, 3, 12, 14): the empty log's
//! map, publications advancing with no consumer, visibility at the promised
//! sequence, coalescing and cadence, chunk sealing at exactly 3,072 entries,
//! sealed chunks built once and reused across a restart, and identical
//! sealed tables across cadence and restart.
mod common;

use common::*;
use std::time::{Duration, Instant};
use tachyon_pir_protocol::{RecoveryEpoch, ScopeState};

fn generation(published: &Published) -> u64 {
    published.head.generation
}

/// Poll the distribution until its eligible publication's map has
/// `log_sequence` at least `at_least`.
async fn wait_for(server: &Server, at_least: u64) -> Published {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let current = server.registry.distribution().current();
        if current.eligible && current.publication.is_some() {
            let published = Published::read(server.registry.distribution());
            if published.head.log_sequence >= at_least {
                return published;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no publication with log_sequence >= {at_least}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn publications_advance_without_consumers_and_entries_are_visible_after_their_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path(), true, |_| {}).await;

    // The empty log: one open chunk 0 with no tables, fields in spec order.
    let empty = wait_for(&server, 0).await;
    assert_eq!(empty.head.log_sequence, 0);
    assert!(empty.head.sealed_chunks.is_empty());
    assert_eq!(empty.head.floor_scope, 0);
    let open = &empty.head.open_scopes[0];
    assert_eq!(
        (open.scope_id, open.state, open.entries, open.n_buckets),
        (0, ScopeState::Open, 0, 1)
    );
    assert!(open.tables.is_empty());
    assert!(empty.inventory.tables.is_empty());
    let bytes = server
        .registry
        .distribution()
        .read_object(&empty.inventory.map_head)
        .unwrap();
    let text = String::from_utf8(bytes).unwrap();
    let expected = format!(
        "{{\"protocol\":\"tachyon-registry-pir-v1\",\"profile\":\"reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1\",\
         \"network\":\"test\",\"genesis_hash\":\"{}\",\"generation\":{},\"recovery_epoch\":\"{}\",\
         \"log_sequence\":0,\"floor_scope\":0,\"sealed_chunks\":[],\
         \"open_scopes\":[{{\"scope_id\":0,\"state\":\"open\",\"entries\":0,\"n_buckets\":1,\"tables\":[]}}]}}",
        identity().genesis_hash.display_hex(),
        empty.head.generation,
        empty.head.recovery_epoch.get(),
    );
    assert_eq!(text, expected);

    // Registrations become visible in the first map past their sequence.
    let a = address(100);
    let b = address(101);
    let ra = register(&client, &server, &a, 6).await;
    let rb = register(&client, &server, &b, 6).await;
    assert_eq!(
        (ra.visible_after_sequence, rb.visible_after_sequence),
        (0, 1)
    );
    let published = wait_for(&server, rb.visible_after_sequence + 1).await;
    assert!(published.current.sequence > empty.current.sequence);
    assert!(generation(&published) > generation(&empty));
    assert_eq!(published.head.log_sequence, 2);
    let scope = published.scope(0);
    assert_eq!((scope.entries, scope.n_buckets), (2, 1));
    assert_eq!(scope.tables.len(), 1);
    let rows = published.rows(server.registry.distribution(), 0);
    for address in [&a, &b] {
        assert_eq!(published.find(&rows, 0, address), 1);
    }
    assert_eq!(published.find(&rows, 0, &address(102)), 0);
    let metrics = server.registry.metrics().render();
    assert!(
        metrics.contains("tachyon_registry_entries_total 2\n"),
        "{metrics}"
    );
    assert!(metrics.contains("tachyon_registry_open_scope_entries 2\n"));
    assert!(metrics.contains("tachyon_registry_publication_seconds_count"));
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn registrations_during_the_interval_coalesce_into_one_publication() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let server = Server::start(dir.path(), true, |s| s.publish_ms = 3000).await;
    wait_for(&server, 0).await;
    register(&client, &server, &address(110), 6).await;
    let first = wait_for(&server, 1).await;
    let seen = Instant::now();
    // Three registrations land while the loop waits out publish_ms.
    let three: Vec<_> = (111..114).map(address).collect();
    futures_util::future::join_all(three.iter().map(|a| register(&client, &server, a, 6))).await;
    let second = wait_for(&server, 4).await;
    // One publication carries all three, publish_ms after the previous one
    // started.
    assert_eq!(second.head.log_sequence, 4);
    assert_eq!(generation(&second), generation(&first) + 1);
    assert!(
        seen.elapsed() >= Duration::from_millis(2000),
        "{:?}",
        seen.elapsed()
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn chunks_seal_at_3072_and_sealed_tables_are_built_once_and_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let seeded: Vec<_> = (0..3070).map(address).collect();
    seed_log(dir.path(), &seeded);
    let mut server = Server::start(dir.path(), false, |_| {}).await;
    let report = server.publish().await.unwrap();
    assert_eq!((report.log_sequence, report.tables_built), (3070, 1));

    // The 3,071st and 3,072nd entries fill chunk 0.
    let r1 = register(&client, &server, &address(5000), 6).await;
    let r2 = register(&client, &server, &address(5001), 6).await;
    assert_eq!((r1.chunk_id, r1.visible_after_sequence), (0, 3070));
    assert_eq!((r2.chunk_id, r2.visible_after_sequence), (0, 3071));
    let report = server.publish().await.unwrap();
    assert_eq!((report.log_sequence, report.tables_built), (3072, 1));
    let sealed = Published::read(server.registry.distribution());
    assert_eq!(sealed.head.sealed_chunks.len(), 1);
    assert_eq!(sealed.chunks[0].scopes.len(), 1);
    let chunk0 = sealed.scope(0).clone();
    assert_eq!((chunk0.state, chunk0.entries), (ScopeState::Sealed, 3072));
    let open = &sealed.head.open_scopes[0];
    assert_eq!((open.scope_id, open.entries), (1, 0));
    assert!(open.tables.is_empty());
    let rows = sealed.rows(server.registry.distribution(), 0);
    for address in seeded.iter().chain([&address(5000), &address(5001)]) {
        assert_eq!(sealed.find(&rows, 0, address), 1);
    }
    assert!(
        server
            .registry
            .metrics()
            .render()
            .contains("tachyon_registry_open_scope_entries 0\n")
    );

    // The next registration opens chunk 1; chunk 0 is not rebuilt.
    let r3 = register(&client, &server, &address(5002), 6).await;
    assert_eq!((r3.chunk_id, r3.visible_after_sequence), (1, 3072));
    let report = server.publish().await.unwrap();
    assert_eq!((report.log_sequence, report.tables_built), (3073, 1));
    let grown = Published::read(server.registry.distribution());
    assert_eq!(grown.scope(0), &chunk0);
    assert_eq!(grown.head.sealed_chunks, sealed.head.sealed_chunks);
    assert_eq!(grown.scope(1).entries, 1);
    assert_eq!(server.registry.tables_built(), 3);
    // Nothing new: nothing published.
    assert!(server.publish().await.is_none());
    let epoch = grown.head.recovery_epoch.get();
    server.stop().await;

    // Restart: a new recovery epoch, the previous publication restored under
    // a new generation, no table rebuilt and every session unchanged.
    let mut server = Server::start(dir.path(), false, |_| {}).await;
    assert!(server.publish().await.is_none());
    assert_eq!(server.registry.tables_built(), 0);
    let restored = Published::read(server.registry.distribution());
    assert_eq!(
        restored.head.recovery_epoch,
        RecoveryEpoch::new(epoch + 1).unwrap()
    );
    assert!(restored.head.generation > grown.head.generation);
    assert_eq!(restored.head.log_sequence, 3073);
    assert_eq!(restored.inventory.tables, grown.inventory.tables);
    assert_eq!(restored.head.open_scopes, grown.head.open_scopes);
    assert_eq!(restored.chunks, grown.chunks);
    // Growth rebuilds only the open chunk.
    register(&client, &server, &address(5003), 6).await;
    let report = server.publish().await.unwrap();
    assert_eq!((report.log_sequence, report.tables_built), (3074, 1));
    let after = Published::read(server.registry.distribution());
    assert_eq!(after.scope(0), &chunk0);
    assert_eq!(after.head.recovery_epoch, restored.head.recovery_epoch);
    server.stop().await;

    // Determinism: the same log published at once from an empty state
    // directory gives chunk 0 the identical table.
    let fresh = tempfile::tempdir().unwrap();
    let all: Vec<_> = seeded
        .iter()
        .cloned()
        .chain((5000..5003).map(address))
        .collect();
    seed_log(fresh.path(), &all);
    let mut server = Server::start(fresh.path(), false, |_| {}).await;
    let report = server.publish().await.unwrap();
    assert_eq!((report.log_sequence, report.tables_built), (3073, 2));
    let once = Published::read(server.registry.distribution());
    assert_eq!(once.scope(0), &chunk0);
    assert_eq!(once.scope(1), grown.scope(1));
    server.stop().await;
}
