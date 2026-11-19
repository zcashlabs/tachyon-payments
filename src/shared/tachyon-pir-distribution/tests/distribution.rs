//! The distribution contract across real HTTP(S): a publisher-side
//! `Distribution` served by axum on 127.0.0.1 behind a TLS terminator, and a
//! query-host follower over `HttpsSource` and `ConsumerEngine`.
mod support;

use pir_pipeline::consumer::{ConsumerPass, polling::SyncTask};
use pir_publication::{AuthorityCheckRequest, AuthorityCheckResponse, ObjectReference};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use support::*;
use tachyon_pir_distribution::{
    Current, ServingPolicy, SessionLookup, Subject, contract::MAX_OPEN_SCOPES, lost_race,
};

const LONG: Duration = Duration::from_secs(600);

fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn blob(scope: u64, seed: u8) -> Blob {
    Blob { scope, seed }
}

async fn envelope(publisher: &Publisher) -> (Current, reqwest::header::HeaderMap) {
    let response = get(&publisher.url("current"), None).await;
    assert_eq!(response.status(), 200);
    let headers = response.headers().clone();
    (
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap(),
        headers,
    )
}

async fn check(publisher: &Publisher, body: Vec<u8>) -> reqwest::Response {
    reqwest::Client::new()
        .post(publisher.url("authority/check"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap()
}

fn rows_ref(blob: &Blob) -> ObjectReference {
    ObjectReference::for_bytes(&blob.rows())
}

fn public_ref(blob: &Blob) -> ObjectReference {
    ObjectReference::for_bytes(&blob.public())
}

/// Scenario 1: publications advance with nobody consuming them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publications_advance_without_consumers() {
    let dir = tempdir();
    let publisher = Publisher::start(dir.path(), LONG).await;
    let mut sequence = 0;
    for generation in 1..=5u64 {
        let committed = publisher
            .publish(&[blob(0, generation as u8)], block(10 + generation, 1))
            .unwrap();
        assert_eq!(committed.generation, generation);
        assert_eq!(committed.recovery_epoch, 1);
        assert!(committed.sequence > sequence);
        sequence = committed.sequence;
    }
    let (current, headers) = envelope(&publisher).await;
    assert!(current.eligible);
    assert_eq!(current.sequence, sequence);
    assert_eq!(
        current.scope.product,
        "tachyon-tachygram-prepared-distribution-v1"
    );
    assert_eq!(current.publication.unwrap().descriptor.generation, 5);
    assert_eq!(headers["cache-control"], "no-store");
    assert_eq!(publisher.distribution.last_generation(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routes_serve_conditional_envelopes_immutable_objects_and_bounded_checks() {
    let dir = tempdir();
    let publisher = Publisher::start(dir.path(), LONG).await;
    let a = blob(0, 1);
    let anchor = block(20, 1);
    publisher.publish(&[a], anchor).unwrap();

    let (current, headers) = envelope(&publisher).await;
    let etag = headers["etag"].to_str().unwrap().to_string();
    assert_eq!(etag, pir_publication::conditional_token(&current));
    let unchanged = get(&publisher.url("current"), Some(&etag)).await;
    assert_eq!(unchanged.status(), 304);
    assert_eq!(unchanged.headers()["cache-control"], "no-store");
    assert_eq!(unchanged.headers()["etag"], etag.as_str());

    let rows = get(
        &publisher.url(&format!("objects/{}", rows_ref(&a).sha256)),
        None,
    )
    .await;
    assert_eq!(rows.status(), 200);
    assert_eq!(
        rows.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
    assert_eq!(rows.headers()["content-type"], "application/octet-stream");
    assert_eq!(rows.bytes().await.unwrap(), a.rows());
    let orphan = publisher
        .distribution
        .install(b"installed, never published")
        .unwrap();
    for id in [orphan.sha256.as_str(), "authority.json", &"A".repeat(64)] {
        let missing = get(&publisher.url(&format!("objects/{id}")), None).await;
        assert_eq!(missing.status(), 404, "{id}");
        assert_eq!(missing.headers()["cache-control"], "no-store");
    }

    let authority = current.authority().unwrap();
    let request = |subjects: Vec<Subject>| {
        serde_json::to_vec(&AuthorityCheckRequest {
            authority,
            subjects,
        })
        .unwrap()
    };
    let canonical = Subject::Chain {
        anchor,
        open_scopes: vec![anchor],
    };
    let reorged = Subject::Chain {
        anchor: block(20, 2),
        open_scopes: vec![],
    };
    let answered = check(
        &publisher,
        request(vec![canonical.clone(), reorged.clone()]),
    )
    .await;
    assert_eq!(answered.status(), 200);
    assert_eq!(answered.headers()["cache-control"], "no-store");
    let answered: AuthorityCheckResponse<Subject> =
        serde_json::from_slice(&answered.bytes().await.unwrap()).unwrap();
    assert_eq!(answered.authority, authority);
    let verdicts: Vec<_> = answered
        .answers
        .iter()
        .map(|a| (a.subject.clone(), a.still_authoritative))
        .collect();
    assert_eq!(verdicts, vec![(canonical.clone(), true), (reorged, false)]);
    let checks = publisher
        .hooks
        .checks
        .load(std::sync::atomic::Ordering::SeqCst);

    let mut stale = authority;
    stale.sequence -= 1;
    let stale = serde_json::to_vec(&AuthorityCheckRequest {
        authority: stale,
        subjects: vec![canonical.clone()],
    })
    .unwrap();
    assert_eq!(check(&publisher, stale).await.status(), 409);
    let wide = Subject::Chain {
        anchor,
        open_scopes: vec![anchor; MAX_OPEN_SCOPES],
    };
    for (name, body) in [
        ("empty", request(vec![])),
        ("garbage", b"{".to_vec()),
        (
            "registry subject",
            request(vec![Subject::Registry { log_sequence: 1 }]),
        ),
        ("too many blocks", request(vec![wide; 64])),
        (
            "too many open scopes",
            request(vec![Subject::Chain {
                anchor,
                open_scopes: vec![anchor; MAX_OPEN_SCOPES + 1],
            }]),
        ),
        ("too many subjects", request(vec![canonical.clone(); 257])),
    ] {
        assert_eq!(check(&publisher, body).await.status(), 400, "{name}");
    }
    // Refused requests never reach the product's source.
    assert_eq!(
        publisher
            .hooks
            .checks
            .load(std::sync::atomic::Ordering::SeqCst),
        checks
    );
    publisher.distribution.withdraw().unwrap();
    assert_eq!(
        check(&publisher, request(vec![canonical])).await.status(),
        409
    );
}

/// Scenario 2 at the distribution layer: a cold host reaches a ready
/// publication over TLS, and a host retaining fewer scopes fetches less.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_bootstrap_over_tls_reaches_a_ready_publication() {
    let (dir, host_dir, narrow_dir) = (tempdir(), tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let (a, b) = (blob(0, 1), blob(1, 2));
    let committed = publisher.publish(&[a, b], block(30, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let mut host = Host::start(&edge, host_dir.path(), ServingPolicy::default(), None);
    assert_eq!(
        host.status(&a),
        503,
        "nothing serves before the first commit"
    );
    assert!(!host.publications.is_ready());

    let report = host.commit().await;
    assert_eq!(report.generation, 1);
    assert_eq!(report.recovery_epoch, 1);
    assert_eq!(report.identity.0, committed.reference.publication);
    assert_eq!(host.found(&a), a.rows());
    assert_eq!(host.found(&b), b.rows());
    assert_eq!(host.prepared().len(), 2);
    let gets = publisher.observed.take_gets();
    assert_eq!(
        gets.len(),
        1 + 1 + 1 + 2 * 2,
        "inventory, head, chunk, two tables: {gets:?}"
    );

    let current = host.publications.current().unwrap();
    assert!(host.publications.is_ready());
    assert_eq!(current.floor_scope(), 0);
    let head = committed.reference.descriptor.map_head.clone();
    assert_eq!(current.map_sha256(), head.sha256);
    head.verify(current.map_head()).unwrap();
    let chunk = ObjectReference::for_bytes(b"sealed map chunk 0");
    assert_eq!(
        &**current.map_chunk(&chunk.sha256).unwrap(),
        b"sealed map chunk 0"
    );
    match host.publications.session(&[0xee; 32]) {
        SessionLookup::Unknown { map_sha256 } => assert_eq!(map_sha256, head.sha256),
        other => panic!("unknown sessions answer 409, got {}", other.status()),
    }

    // An unchanged envelope fetches nothing and keeps serving.
    match host.poll().await.unwrap() {
        ConsumerPass::Deferred { report: Some(r) } => assert_eq!(r, report),
        other => panic!("expected an unchanged pass, got {other:?}"),
    }
    assert!(publisher.observed.take_gets().is_empty());

    let mut narrow = Host::start(&edge, narrow_dir.path(), ServingPolicy::default(), Some(1));
    narrow.commit().await;
    assert_eq!(narrow.publications.current().unwrap().floor_scope(), 1);
    assert_eq!(narrow.status(&a), 409, "below this host's floor");
    assert_eq!(narrow.found(&b), b.rows());
    assert!(!narrow.cached(&rows_ref(&a)));
    assert_eq!(publisher.observed.take_gets().len(), 1 + 1 + 1 + 2);
}

/// Scenario 3: only changed objects are fetched and unchanged sessions keep
/// their prepared runtimes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_publication_fetches_only_changed_objects() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let (sealed, open, reopened) = (blob(0, 1), blob(1, 2), blob(1, 3));
    publisher.publish(&[sealed, open], block(40, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let mut host = Host::start(&edge, host_dir.path(), ServingPolicy::default(), None);
    host.commit().await;
    host.prepared();
    publisher.observed.take_gets();
    let before = match host.publications.session(&sealed.session()) {
        SessionLookup::Found { table, .. } => table.runtime,
        other => panic!("sealed table not served: {}", other.status()),
    };

    let committed = publisher
        .publish(&[sealed, reopened], block(41, 1))
        .unwrap();
    let report = host.commit().await;
    assert_eq!(report.generation, 2);
    let gets: BTreeSet<_> = publisher.observed.take_gets().into_iter().collect();
    let expected: BTreeSet<_> = [
        committed.reference.inventory.sha256.clone(),
        committed.reference.descriptor.map_head.sha256.clone(),
        rows_ref(&reopened).sha256,
        public_ref(&reopened).sha256,
    ]
    .into_iter()
    .collect();
    assert_eq!(gets, expected);
    assert_eq!(host.prepared(), vec![reopened.session()]);
    match host.publications.session(&sealed.session()) {
        SessionLookup::Found { table, publication } => {
            assert!(
                Arc::ptr_eq(&table.runtime, &before),
                "sealed runtime reused"
            );
            assert_eq!(publication.generation(), 2);
        }
        other => panic!("sealed table not served: {}", other.status()),
    }
    // The displaced open table serves through its retention window.
    match host.publications.session(&open.session()) {
        SessionLookup::Found { publication, .. } => assert_eq!(publication.generation(), 1),
        other => panic!("displaced table not served: {}", other.status()),
    }
    assert_eq!(host.found(&reopened), reopened.rows());
}

/// Scenario 10: a corrupt or truncated object, or a publication the product
/// refuses, is not served and the previous publication keeps serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_truncated_or_refused_publications_keep_the_previous_one() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let a = blob(0, 1);
    publisher.publish(&[a], block(50, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let mut host = Host::start(&edge, host_dir.path(), ServingPolicy::default(), None);
    let mut serving = host.commit().await;

    for (seed, fault) in [(10u8, "corrupt"), (11, "truncate"), (12, "refuse")] {
        let added = blob(1, seed);
        publisher
            .publish(&[a, added], block(50 + u64::from(seed), 1))
            .unwrap();
        let target = rows_ref(&added).sha256;
        match fault {
            "corrupt" => *publisher.observed.fault.lock().unwrap() = Some(Fault::Corrupt(target)),
            "truncate" => *publisher.observed.fault.lock().unwrap() = Some(Fault::Truncate(target)),
            _ => host
                .stats
                .refuse
                .store(true, std::sync::atomic::Ordering::SeqCst),
        }
        let error = host.poll().await.unwrap_err();
        assert!(!lost_race(&error), "{fault}: {error}");
        assert!(
            !host.manager.task().transient(&error),
            "{fault} backs off: {error}"
        );
        let current = host
            .publications
            .current()
            .expect("previous publication serves");
        assert_eq!(current.identity(), serving.identity, "{fault}");
        assert_eq!(host.found(&a), a.rows());
        assert_eq!(host.status(&added), 409, "{fault}");
        if fault != "refuse" {
            assert!(
                !host.cached(&rows_ref(&added)),
                "{fault} bytes never enter the cache"
            );
        }
        *publisher.observed.fault.lock().unwrap() = None;
        host.stats
            .refuse
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let report = host.commit().await;
        assert_ne!(report.identity, serving.identity);
        serving = report;
        assert_eq!(host.found(&added), added.rows());
    }
}

/// Spec 11.4 shallow reorg: a withdrawal stops serving until the next
/// publication; sessions it drops are unknown (409), not revoked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn withdrawal_fences_serving_until_the_next_publication() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let (a, b, c) = (blob(0, 1), blob(1, 2), blob(1, 3));
    publisher.publish(&[a, b], block(60, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let mut host = Host::start(&edge, host_dir.path(), ServingPolicy::default(), None);
    host.commit().await;

    assert!(
        !publisher.distribution.withdraw_above(Some(60)).unwrap(),
        "anchor not above kept"
    );
    assert!(publisher.distribution.withdraw_above(Some(59)).unwrap());
    assert!(!publisher.distribution.current().eligible);
    match host.poll().await.unwrap() {
        ConsumerPass::Deferred { report: None } => {}
        other => panic!("a withdrawn envelope serves nothing, got {other:?}"),
    }
    assert_eq!(host.status(&a), 503);
    assert_eq!(host.status(&b), 503);
    assert!(!host.publications.is_ready());

    let committed = publisher.publish(&[a, c], block(59, 2)).unwrap();
    assert_eq!(committed.recovery_epoch, 1);
    host.commit().await;
    assert_eq!(host.found(&a), a.rows());
    assert_eq!(host.found(&c), c.rows());
    assert_eq!(host.status(&b), 409);
}

/// Scenario 11: while the publisher is unreachable the host keeps serving
/// what it last confirmed for `max_unconfirmed`, then answers 503 until a
/// pass confirms again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outage_serves_within_the_confirmation_window_then_503() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let a = blob(0, 1);
    publisher.publish(&[a], block(70, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let policy = ServingPolicy {
        retention: LONG,
        max_unconfirmed: Duration::from_secs(1),
    };
    let mut host = Host::start(&edge, host_dir.path(), policy, None);
    let report = host.commit().await;

    edge.set_down(true);
    let error = host.poll().await.unwrap_err();
    assert!(
        host.manager.task().transient(&error),
        "an unreachable publisher is retried fast: {error}"
    );
    assert_eq!(host.found(&a), a.rows(), "serves within the window");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(host.status(&a), 503);
    assert!(!host.publications.is_ready());
    assert!(host.poll().await.is_err());
    assert_eq!(host.status(&a), 503);

    edge.set_down(false);
    match host.poll_reconnecting().await.unwrap() {
        ConsumerPass::Deferred { report: Some(r) } => assert_eq!(r, report),
        other => panic!("expected the same publication confirmed, got {other:?}"),
    }
    assert_eq!(host.found(&a), a.rows());
}

/// Scenario 12 for the publisher: a restart opens a new recovery epoch and
/// restores the previous publication under it, with a new generation; the
/// host refetches only the inventory and re-issued head.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_republishes_under_a_new_recovery_epoch() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let mut publisher = Publisher::start(dir.path(), LONG).await;
    let (a, b) = (blob(0, 1), blob(1, 2));
    let anchor = block(80, 1);
    publisher.publish(&[a, b], anchor).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let mut host = Host::start(&edge, host_dir.path(), ServingPolicy::default(), None);
    host.commit().await;
    host.prepared();
    publisher.observed.take_gets();

    publisher = publisher.restart(LONG).await;
    edge.set_upstream(publisher.addr);
    let current = publisher.distribution.current();
    assert!(!current.eligible);
    assert_eq!(current.fence.recovery_epoch, 2);
    assert_eq!(
        current.publication.as_ref().unwrap().descriptor.generation,
        1
    );
    match host.poll_reconnecting().await.unwrap() {
        ConsumerPass::Deferred { report: None } => {}
        other => panic!("a restarted publisher is withdrawn, got {other:?}"),
    }
    assert_eq!(host.status(&a), 503);

    assert!(publisher.distribution.restore().await.unwrap());
    assert!(
        !publisher.distribution.restore().await.unwrap(),
        "restores once"
    );
    let restored = publisher.distribution.current();
    assert!(restored.eligible);
    let descriptor = &restored.publication.as_ref().unwrap().descriptor;
    assert_eq!((descriptor.generation, descriptor.recovery_epoch), (2, 2));
    let head: Head = serde_json::from_slice(
        &publisher
            .distribution
            .read_object(&descriptor.map_head)
            .unwrap(),
    )
    .unwrap();
    assert_eq!((head.generation, head.recovery_epoch), (2, 2));

    let report = host.commit().await;
    assert_eq!((report.generation, report.recovery_epoch), (2, 2));
    assert_eq!(host.found(&a), a.rows());
    assert_eq!(host.found(&b), b.rows());
    assert!(
        host.prepared().is_empty(),
        "restored tables reuse their runtimes"
    );
    let gets: BTreeSet<_> = publisher.observed.take_gets().into_iter().collect();
    let expected: BTreeSet<_> = [
        restored
            .publication
            .as_ref()
            .unwrap()
            .inventory
            .sha256
            .clone(),
        descriptor.map_head.sha256.clone(),
    ]
    .into_iter()
    .collect();
    assert_eq!(gets, expected);

    let next = publisher.publish(&[a, b], block(81, 1)).unwrap();
    assert_eq!((next.generation, next.recovery_epoch), (3, 2));

    // A publication whose anchor is no longer canonical is not restored.
    publisher = publisher.restart(LONG).await;
    publisher.hooks.canonical(&[block(81, 9)]);
    assert!(!publisher.distribution.restore().await.unwrap());
    assert!(!publisher.distribution.current().eligible);
    assert_eq!(publisher.distribution.current().fence.recovery_epoch, 3);
    assert_eq!(publisher.distribution.last_generation(), 3);
}

/// Scenario 9 and spec 10.5: a recovery revokes the sessions the next
/// publication drops (410) for the retention window; a later shallow
/// rewind's dropped sessions are only unknown (409).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_reorg_revokes_sessions_and_shallow_reorg_leaves_them_unknown() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let (a, b, c, d) = (blob(0, 1), blob(1, 2), blob(1, 3), blob(1, 4));
    publisher.publish(&[a, b], block(90, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let policy = ServingPolicy {
        retention: Duration::from_secs(2),
        max_unconfirmed: LONG,
    };
    let mut host = Host::start(&edge, host_dir.path(), policy, None);
    host.commit().await;

    publisher.distribution.recover().unwrap();
    assert_eq!(publisher.distribution.current().fence.recovery_epoch, 2);
    host.poll().await.unwrap();
    assert_eq!(host.status(&a), 503);
    let rebuilt = publisher.publish(&[a, c], block(90, 2)).unwrap();
    assert_eq!(rebuilt.recovery_epoch, 2);
    let report = host.commit().await;
    assert_eq!(report.recovery_epoch, 2);
    assert_eq!(host.found(&a), a.rows());
    assert_eq!(host.found(&c), c.rows());
    assert_eq!(host.status(&b), 410);
    assert_eq!(host.status(&blob(5, 99)), 409);

    publisher.distribution.withdraw().unwrap();
    publisher.publish(&[a, d], block(89, 3)).unwrap();
    host.commit().await;
    assert_eq!(host.status(&c), 409, "a shallow rewind does not revoke");
    assert_eq!(host.found(&d), d.rows());
    assert_eq!(host.status(&b), 410, "still inside the window");
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert_eq!(
        host.status(&b),
        409,
        "revocation is remembered for the window only"
    );
}

/// A view the publisher no longer confirms stops serving at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refuted_subject_stops_serving_its_view() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let (a, e) = (blob(0, 1), blob(1, 5));
    publisher.publish(&[a], block(100, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let mut host = Host::start(&edge, host_dir.path(), ServingPolicy::default(), None);
    host.commit().await;

    // The publisher's source reorged; it has not withdrawn yet.
    publisher.hooks.canonical(&[block(100, 2)]);
    let error = host.poll().await.unwrap_err();
    assert!(
        lost_race(&error),
        "the refuted publication is not taken again: {error}"
    );
    assert!(host.manager.task().transient(&error));
    assert_eq!(host.status(&a), 503);

    publisher.distribution.recover().unwrap();
    publisher.publish(&[a, e], block(100, 2)).unwrap();
    host.commit().await;
    assert_eq!(host.found(&a), a.rows());
    assert_eq!(host.found(&e), e.rows());
}

/// Normative rule 5 on both sides: objects of the current publication and of
/// publications displaced within the window are kept; the rest is collected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objects_are_retained_while_needed_then_collected() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let window = Duration::from_millis(400);
    let publisher = Publisher::start(dir.path(), window).await;
    let (x, y, z) = (blob(0, 1), blob(0, 2), blob(0, 3));
    publisher.publish(&[x], block(110, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let policy = ServingPolicy {
        retention: window,
        max_unconfirmed: LONG,
    };
    let mut host = Host::start(&edge, host_dir.path(), policy, None);
    host.commit().await;

    let second = publisher.publish(&[y], block(111, 1)).unwrap();
    host.commit().await;
    let object_status = |object: &ObjectReference| {
        let url = publisher.url(&format!("objects/{}", object.sha256));
        async move { get(&url, None).await.status().as_u16() }
    };
    assert_eq!(
        object_status(&rows_ref(&x)).await,
        200,
        "displaced, inside the window"
    );
    assert!(host.cached(&rows_ref(&x)));
    assert_eq!(host.found(&x), x.rows(), "displaced publication serves");
    let orphan = publisher
        .distribution
        .install(b"prepared, never published")
        .unwrap();
    assert!(publisher.object_path(&orphan).exists());
    assert_eq!(
        object_status(&orphan).await,
        404,
        "installing grants no serving"
    );

    tokio::time::sleep(window + Duration::from_millis(100)).await;
    publisher.publish(&[y, z], block(112, 1)).unwrap();
    for object in [rows_ref(&x), public_ref(&x), orphan] {
        assert_eq!(object_status(&object).await, 404);
        assert!(!publisher.object_path(&object).exists(), "collected");
    }
    for object in [
        rows_ref(&y),
        rows_ref(&z),
        second.reference.descriptor.map_head.clone(),
    ] {
        assert_eq!(object_status(&object).await, 200);
    }

    host.commit().await;
    assert!(
        !host.cached(&rows_ref(&x)),
        "expired displaced view collected"
    );
    assert!(host.cached(&rows_ref(&y)));
    assert!(
        host.cached(&second.reference.descriptor.map_head),
        "displaced view kept"
    );
    assert_eq!(host.status(&x), 409);
    assert_eq!(host.found(&z), z.rows());
}

/// An uncertain authority write is recovered before publishing again, and
/// the candidate is re-issued under the new epoch (the `RecoveryRequired`
/// idiom), which the host accepts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_uncertain_authority_write_recovers_and_reissues_the_head() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let (a, f) = (blob(0, 1), blob(1, 6));
    publisher.publish(&[a], block(120, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let mut host = Host::start(&edge, host_dir.path(), ServingPolicy::default(), None);
    host.commit().await;

    let record = dir
        .path()
        .join("distribution")
        .join(PRODUCT.name())
        .join("authority.json");
    std::fs::remove_file(&record).unwrap();
    std::fs::create_dir(&record).unwrap();
    std::fs::write(record.join("blocker"), b"x").unwrap();
    assert!(publisher.publish(&[a, f], block(121, 1)).is_err());
    assert!(
        !publisher.distribution.current().eligible,
        "an uncertain write withdraws"
    );
    std::fs::remove_dir_all(&record).unwrap();

    let committed = publisher.publish(&[a, f], block(121, 1)).unwrap();
    assert_eq!((committed.generation, committed.recovery_epoch), (3, 2));
    let head: Head = serde_json::from_slice(
        &publisher
            .distribution
            .read_object(&committed.reference.descriptor.map_head)
            .unwrap(),
    )
    .unwrap();
    assert_eq!((head.generation, head.recovery_epoch), (3, 2));
    let saved: Current = serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
    assert!(saved.eligible);
    assert_eq!(saved.fence.recovery_epoch, 2);

    let report = host.commit().await;
    assert_eq!(report.recovery_epoch, 2);
    assert_eq!(host.found(&a), a.rows());
    assert_eq!(host.found(&f), f.rows());
}

/// Spec 11.6: a pass that loses a race with a withdrawal (the publisher
/// withdraws while the pass is fetching, then republishes) is retried as a
/// transient condition, so the host adopts the new publication within about a
/// second instead of after the 10 s back-off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_withdrawal_during_a_fetch_is_retried_without_back_off() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let (a, c, d) = (blob(0, 1), blob(1, 2), blob(1, 3));
    publisher.publish(&[a], block(130, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let host = Host::start(&edge, host_dir.path(), ServingPolicy::default(), None);
    let running = host.run(Duration::from_millis(200));
    running
        .serves(1, Duration::from_secs(10))
        .await
        .expect("first publication serves");

    let hold = Hold::new(rows_ref(&c).sha256);
    *publisher.observed.hold.lock().unwrap() = Some(hold.clone());
    publisher.publish(&[a, c], block(131, 1)).unwrap();
    tokio::time::timeout(Duration::from_secs(10), hold.entered.notified())
        .await
        .expect("the pass fetches the new table");
    publisher.distribution.withdraw().unwrap();
    let before = publisher.observed.served_count();
    hold.release.add_permits(1);
    // The pass finishes its fetch and loads, then re-observes the withdrawn
    // envelope and fails: it lost the race, nothing it fetched was refused.
    publisher.observed.wait_served("current", before).await;
    let republished = publisher.publish(&[a, d], block(130, 2)).unwrap();
    assert_eq!(republished.generation, 3);
    let took = running
        .serves(3, Duration::from_secs(8))
        .await
        .expect("the republished publication serves");
    assert!(
        took < Duration::from_secs(3),
        "adopted after {took:?}, not within ~1 s"
    );
    match running.publications.session(&d.session()) {
        SessionLookup::Found { .. } => {}
        other => panic!("new table not served: {}", other.status()),
    }
}

/// The race the memo end-to-end tests hit: the publisher's source reorged,
/// so the host's authority check refutes the current publication before the
/// publisher has withdrawn it, and that pass fails with the candidate
/// invalidated. It is retried within about a second, so the recovered
/// publication serves without the 10 s back-off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refuted_publication_race_is_retried_without_back_off() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let (a, e) = (blob(0, 1), blob(1, 5));
    publisher.publish(&[a], block(140, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let host = Host::start(&edge, host_dir.path(), ServingPolicy::default(), None);
    let running = host.run(Duration::from_millis(200));
    running
        .serves(1, Duration::from_secs(10))
        .await
        .expect("first publication serves");

    let before = publisher.observed.served_count();
    publisher.hooks.canonical(&[block(140, 2)]);
    publisher
        .observed
        .wait_served("authority/check", before)
        .await;
    publisher.distribution.recover().unwrap();
    publisher.publish(&[a, e], block(140, 2)).unwrap();
    let took = running
        .serves(2, Duration::from_secs(8))
        .await
        .expect("the recovered publication serves");
    assert!(
        took < Duration::from_secs(3),
        "adopted after {took:?}, not within ~1 s"
    );
}

/// A refused object is still a failure of the publisher's bytes: the next
/// attempt waits for `pir_pipeline`'s back-off (10 s), even once the object
/// is served intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_object_still_backs_off() {
    let (dir, host_dir) = (tempdir(), tempdir());
    let publisher = Publisher::start(dir.path(), LONG).await;
    let (a, c) = (blob(0, 1), blob(1, 2));
    publisher.publish(&[a], block(150, 1)).unwrap();
    let edge = Edge::start(publisher.addr).await;
    let host = Host::start(&edge, host_dir.path(), ServingPolicy::default(), None);
    let running = host.run(Duration::from_millis(200));
    running
        .serves(1, Duration::from_secs(10))
        .await
        .expect("first publication serves");

    let target = rows_ref(&c).sha256;
    *publisher.observed.fault.lock().unwrap() = Some(Fault::Corrupt(target.clone()));
    let before = publisher.observed.served_count();
    publisher.publish(&[a, c], block(151, 1)).unwrap();
    publisher
        .observed
        .wait_served(&format!("objects/{target}"), before)
        .await;
    *publisher.observed.fault.lock().unwrap() = None;
    let took = running
        .serves(2, Duration::from_secs(16))
        .await
        .expect("adopted after the back-off");
    assert!(
        took >= Duration::from_secs(9),
        "a refused object backs off, adopted after {took:?}"
    );
}
