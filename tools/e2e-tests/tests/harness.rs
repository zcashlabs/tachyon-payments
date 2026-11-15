//! The harness itself, across real sockets: the TLS edge in front of in-test
//! upstreams, its routing, faults, outage and counting; the query host's
//! `HttpsSource` trusting the edge's certificate; spawned node-double processes;
//! port reservation; the budget guard.
use axum::Router;
use axum::body::Body;
use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use pir_sync_manager::HttpsSource;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tachyon_fixtures::{ChainConfig, ChainSpec, SyntheticChain};
use tachyon_pir_e2e_tests::budget::{self, Budget};
use tachyon_pir_e2e_tests::client::{self, get_bytes, get_json, post_octets};
use tachyon_pir_e2e_tests::node::{SpawnedNode, rpc, start_node};
use tachyon_pir_e2e_tests::process::{Process, group_exists};
use tachyon_pir_e2e_tests::{Fault, Target, TlsEdge, binary, free_addrs};
use tokio::sync::Notify;

const PREFIX: &str = "/provider/tachygram/v1/";
const OBJECT_A: &str = "/provider/tachygram/v1/objects/aaaa";
const OBJECT_B: &str = "/provider/tachygram/v1/objects/bbbb";
const CURRENT: &str = "/provider/tachygram/v1/current";
const OBJECT_LEN: usize = 64 * 1024;
const CHUNK: usize = 16 * 1024;

fn object(name: &str) -> Vec<u8> {
    let seed = name
        .bytes()
        .fold(7u32, |acc, byte| acc * 31 + u32::from(byte));
    (0..OBJECT_LEN)
        .map(|i| ((i as u32).wrapping_mul(2_654_435_761) ^ seed) as u8)
        .collect()
}

#[derive(Clone)]
struct Upstream {
    name: &'static str,
    gate: Arc<Notify>,
}

/// A plain-HTTP upstream standing in for one publisher listener.
async fn upstream(name: &'static str) -> (String, Arc<Notify>) {
    let state = Upstream {
        name,
        gate: Arc::new(Notify::new()),
    };
    let gate = state.gate.clone();
    let router = Router::new()
        .route(
            "/.well-known/pir-service-discovery",
            get(|State(state): State<Upstream>| async move {
                if state.name == "management" {
                    (
                        [("content-type", "application/json")],
                        pir_service_discovery::Document::publisher().to_json(),
                    )
                } else {
                    // A product listener must never be asked for discovery.
                    ([("content-type", "application/json")], b"{}".to_vec())
                }
            }),
        )
        .route(
            "/ready",
            get(|State(state): State<Upstream>| async move { format!("{} ready", state.name) }),
        )
        .route(
            "/provider/:product/v1/current",
            get(
                |State(state): State<Upstream>, UrlPath(product): UrlPath<String>| async move {
                    axum::Json(json!({"upstream": state.name, "product": product}))
                },
            ),
        )
        .route(
            "/provider/:product/v1/objects/stream",
            get(|State(state): State<Upstream>| async move {
                let first = Ok::<_, std::io::Error>(bytes::Bytes::from(vec![1u8; CHUNK]));
                let gate = state.gate.clone();
                let rest = futures_util::stream::once(async move {
                    gate.notified().await;
                    Ok::<_, std::io::Error>(bytes::Bytes::from(vec![2u8; CHUNK]))
                });
                Body::from_stream(futures_util::StreamExt::chain(
                    futures_util::stream::iter([first]),
                    rest,
                ))
            }),
        )
        .route(
            "/provider/:product/v1/objects/:name",
            get(|UrlPath((_, name)): UrlPath<(String, String)>| async move { object(&name) }),
        )
        .route(
            "/provider/:product/v1/authority/check",
            post(
                |State(state): State<Upstream>, headers: HeaderMap, body: bytes::Bytes| async move {
                    let forwarded = headers
                        .get("x-forwarded-for")
                        .map(|value| value.to_str().unwrap().to_owned());
                    (
                        StatusCode::OK,
                        [
                            ("x-upstream", state.name.to_owned()),
                            ("x-forwarded-for-seen", forwarded.unwrap_or_default()),
                        ],
                        body,
                    )
                },
            ),
        )
        .route(
            "/registry/v1/challenge",
            get(|State(state): State<Upstream>| async move { format!("{} challenge", state.name) }),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (addr, gate)
}

/// An edge with a management upstream and a tachygram upstream.
struct Setup {
    edge: TlsEdge,
    client: reqwest::Client,
    gate: Arc<Notify>,
}

async fn setup() -> Setup {
    let (management, _) = upstream("management").await;
    let (tachygram, gate) = upstream("tachygram").await;
    let (registry, _) = upstream("registry").await;
    let edge = TlsEdge::builder()
        .management(management)
        .product("tachygram", tachygram.clone())
        .product("memo", registry.clone())
        .prefix("/registry/v1/", registry)
        .start()
        .await
        .unwrap();
    let client = edge.client();
    Setup { edge, client, gate }
}

fn source(edge: &TlsEdge, roots: &[reqwest::Certificate]) -> HttpsSource {
    // As the query host builds it: the configured origin, the product's
    // distribution prefix, a byte budget and the `publisher_ca` roots.
    let pem = std::fs::read(edge.ca_path()).unwrap();
    assert_eq!(pem, edge.certificate().cert_pem.as_bytes());
    let _ = reqwest::Certificate::from_pem(&pem).unwrap();
    HttpsSource::new(&edge.origin(), PREFIX, 1 << 24, roots).unwrap()
}

fn ca(edge: &TlsEdge) -> Vec<reqwest::Certificate> {
    vec![reqwest::Certificate::from_pem(&std::fs::read(edge.ca_path()).unwrap()).unwrap()]
}

async fn source_body(source: &mut HttpsSource, suffix: &str) -> (u16, Vec<u8>) {
    let mut response = source.request(suffix, None).await.unwrap();
    let status = response.status().as_u16();
    let body = response.read_limited(1 << 20).await.unwrap();
    (status, body)
}

#[tokio::test(flavor = "multi_thread")]
async fn edge_proxies_get_and_post_over_https_with_only_its_certificate() {
    budget::e2e(async {
        let Setup { edge, client, .. } = setup().await;
        assert!(edge.origin().starts_with("https://localhost:"));
        let current: Value = get_json(&client, &edge.url(CURRENT), StatusCode::OK).await;
        assert_eq!(
            current,
            json!({"upstream": "tachygram", "product": "tachygram"})
        );
        let body = get_bytes(&client, &edge.url(OBJECT_A), StatusCode::OK).await;
        assert_eq!(body, object("aaaa"));

        // A binary POST body is forwarded unchanged, and the edge sets
        // X-Forwarded-For to the connecting peer.
        let query: Vec<u8> = (0..200_000u32).map(|i| (i % 253) as u8).collect();
        let url = edge.url("/provider/tachygram/v1/authority/check");
        let reply = post_octets(&client, &url, query.clone()).await;
        reply.assert_status(StatusCode::OK, &url);
        assert_eq!(reply.body, query);
        assert_eq!(reply.header("x-upstream"), Some("tachygram"));
        assert_eq!(reply.header("x-forwarded-for-seen"), Some("127.0.0.1"));

        // The certificate also names 127.0.0.1.
        let by_ip = format!("https://127.0.0.1:{}{CURRENT}", edge.port());
        let _: Value = get_json(&client, &by_ip, StatusCode::OK).await;

        // Without the generated certificate as a root the handshake fails.
        let untrusting = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let error = untrusting.get(edge.url(CURRENT)).send().await.unwrap_err();
        assert!(error.is_connect(), "{error:?}");
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn discovery_goes_to_management_and_unrouted_paths_are_404() {
    budget::e2e(async {
        let Setup { edge, client, .. } = setup().await;
        let document = get_bytes(
            &client,
            &edge.url("/.well-known/pir-service-discovery"),
            StatusCode::OK,
        )
        .await;
        assert_eq!(
            document,
            pir_service_discovery::Document::publisher().to_json()
        );
        // memo is mapped to the registry upstream; the product map decides.
        let current: Value = get_json(
            &client,
            &edge.url("/provider/memo/v1/current"),
            StatusCode::OK,
        )
        .await;
        assert_eq!(current, json!({"upstream": "registry", "product": "memo"}));
        let challenge =
            get_bytes(&client, &edge.url("/registry/v1/challenge"), StatusCode::OK).await;
        assert_eq!(challenge, "registry challenge");
        for path in [
            "/ready",
            "/",
            "/provider/registry/v1/current",
            "/provider/tachygram/v2/current",
            "/provider//v1/current",
            "/.well-known/other",
        ] {
            let reply = client::get(&client, &edge.url(path)).await;
            reply.assert_status(StatusCode::NOT_FOUND, path);
            assert!(
                reply.text().contains("no route"),
                "{path}: {}",
                reply.text()
            );
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn bodies_stream_and_connections_are_kept_alive() {
    budget::e2e(async {
        let Setup { edge, client, gate } = setup().await;
        let mut response = client
            .get(edge.url("/provider/tachygram/v1/objects/stream"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // The upstream holds the second half until released: the first half
        // must arrive before that, so the edge does not buffer the body.
        let mut received = Vec::new();
        while received.len() < CHUNK {
            let chunk = tokio::time::timeout(Duration::from_secs(5), response.chunk())
                .await
                .expect("the first half streams before the upstream finishes")
                .unwrap()
                .expect("more body");
            received.extend_from_slice(&chunk);
        }
        assert_eq!(received, vec![1u8; CHUNK]);
        gate.notify_one();
        while let Some(chunk) = response.chunk().await.unwrap() {
            received.extend_from_slice(&chunk);
        }
        assert_eq!(received.len(), 2 * CHUNK);
        assert!(received[CHUNK..].iter().all(|byte| *byte == 2));

        // Sequential requests on one client reuse one TLS connection.
        let before = edge.connections();
        for _ in 0..5 {
            let body = get_bytes(&client, &edge.url(OBJECT_B), StatusCode::OK).await;
            assert_eq!(body, object("bbbb"));
            let _: Value = get_json(&client, &edge.url(CURRENT), StatusCode::OK).await;
        }
        assert_eq!(edge.connections(), before);
        assert_eq!(edge.connections(), 1);
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupt_and_truncated_object_bodies() {
    budget::e2e(async {
        let Setup { edge, client, .. } = setup().await;
        let expected = object("aaaa");

        let corrupt = edge.inject(Target::Objects, Fault::CorruptBody);
        let body = get_bytes(&client, &edge.url(OBJECT_A), StatusCode::OK).await;
        assert_eq!(body.len(), expected.len());
        let differing: Vec<usize> = (0..body.len())
            .filter(|&i| body[i] != expected[i])
            .collect();
        assert_eq!(differing, vec![0]);
        assert_eq!(body[0], expected[0] ^ 1);
        // Metadata is not an object and is forwarded intact.
        let _: Value = get_json(&client, &edge.url(CURRENT), StatusCode::OK).await;
        edge.remove(corrupt);
        assert_eq!(
            get_bytes(&client, &edge.url(OBJECT_A), StatusCode::OK).await,
            expected
        );

        // Truncation cuts the body short and aborts the connection, once.
        edge.inject_times(
            Target::ProductObjects("tachygram".into()),
            Fault::TruncateBody,
            1,
        );
        let response = client.get(edge.url(OBJECT_A)).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.content_length(), Some(OBJECT_LEN as u64));
        let error = response.bytes().await.unwrap_err();
        assert!(error.is_body() || error.is_decode(), "{error:?}");
        assert_eq!(
            get_bytes(&client, &edge.url(OBJECT_A), StatusCode::OK).await,
            expected
        );

        // Through the query host's transport, a truncated object is an error.
        let mut source = source(&edge, &ca(&edge));
        edge.inject_times(Target::Objects, Fault::TruncateBody, 1);
        let mut response = source.request("objects/aaaa", None).await.unwrap();
        assert!(response.read_limited(1 << 20).await.is_err());
        drop(response);
        let (status, body) = source_body(&mut source, "objects/aaaa").await;
        assert_eq!((status, body), (200, expected));
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_reset_and_missing_objects() {
    budget::e2e(async {
        let Setup { edge, client, .. } = setup().await;

        let missing = edge.inject(Target::Objects, Fault::not_found());
        let reply = client::get(&client, &edge.url(OBJECT_A)).await;
        reply.assert_status(StatusCode::NOT_FOUND, OBJECT_A);
        let _: Value = get_json(&client, &edge.url(CURRENT), StatusCode::OK).await;
        edge.remove(missing);

        let refused = edge.inject(Target::Product("tachygram".into()), Fault::bad_gateway());
        let reply = client::get(&client, &edge.url(CURRENT)).await;
        reply.assert_status(StatusCode::BAD_GATEWAY, CURRENT);
        // Discovery is not under the product and still answers.
        let _ = get_bytes(
            &client,
            &edge.url("/.well-known/pir-service-discovery"),
            StatusCode::OK,
        )
        .await;
        edge.remove(refused);

        // A reset fails the request on the client; the next one succeeds.
        edge.inject_times(Target::Path(CURRENT.into()), Fault::Reset, 1);
        let fresh = edge.client();
        let error = fresh.get(edge.url(CURRENT)).send().await.unwrap_err();
        assert!(error.is_request() || error.is_connect(), "{error:?}");
        assert_eq!(edge.count(CURRENT), 3);
        let _: Value = get_json(&fresh, &edge.url(CURRENT), StatusCode::OK).await;
        assert_eq!(edge.count(CURRENT), 4);

        // A stopped upstream is a 502 from the edge, as from a real proxy.
        let dead = free_addrs(1)[0].to_string();
        let lonely = TlsEdge::builder()
            .product("tachygram", dead)
            .start()
            .await
            .unwrap();
        let reply = client::get(&lonely.client(), &lonely.url(CURRENT)).await;
        reply.assert_status(StatusCode::BAD_GATEWAY, CURRENT);
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn pause_delays_and_holds_requests() {
    budget::e2e(async {
        let Setup { edge, client, .. } = setup().await;
        let delay = edge.inject(
            Target::Suffix("/current".into()),
            Fault::Delay(Duration::from_millis(400)),
        );
        let started = Instant::now();
        let _: Value = get_json(&client, &edge.url(CURRENT), StatusCode::OK).await;
        assert!(started.elapsed() >= Duration::from_millis(400));
        edge.remove(delay);

        // A delay combines with a later fault of the same request.
        edge.inject_times(Target::Objects, Fault::Delay(Duration::from_millis(200)), 1);
        edge.inject_times(Target::Objects, Fault::not_found(), 1);
        let started = Instant::now();
        let reply = client::get(&client, &edge.url(OBJECT_A)).await;
        reply.assert_status(StatusCode::NOT_FOUND, OBJECT_A);
        assert!(started.elapsed() >= Duration::from_millis(200));

        let hold = edge.inject(Target::Objects, Fault::Hold);
        let url = edge.url(OBJECT_B);
        let held = tokio::spawn({
            let client = client.clone();
            async move { get_bytes(&client, &url, StatusCode::OK).await }
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while edge.held() != 1 {
            assert!(Instant::now() < deadline, "the request was never held");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!held.is_finished());
        // Other paths are not held.
        let _: Value = get_json(&client, &edge.url(CURRENT), StatusCode::OK).await;
        edge.remove(hold);
        let body = tokio::time::timeout(Duration::from_secs(5), held)
            .await
            .expect("released")
            .unwrap();
        assert_eq!(body, object("bbbb"));
        assert_eq!(edge.held(), 0);
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn outage_and_resume_on_the_same_port() {
    budget::e2e(async {
        let Setup { edge, client, .. } = setup().await;
        let source = source(&edge, &ca(&edge));
        source.discover().await.unwrap();
        let _: Value = get_json(&client, &edge.url(CURRENT), StatusCode::OK).await;
        let port = edge.port();

        // An outage also drops held requests and open connections.
        edge.inject(Target::Objects, Fault::Hold);
        let url = edge.url(OBJECT_A);
        let held = tokio::spawn({
            let client = client.clone();
            async move { client.get(&url).send().await }
        });
        while edge.held() != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        edge.outage().await;
        assert!(!edge.is_up());
        assert!(held.await.unwrap().is_err());
        assert_eq!(edge.held(), 0);
        edge.clear_faults();

        let error = client.get(edge.url(CURRENT)).send().await.unwrap_err();
        assert!(error.is_connect(), "{error:?}");
        let refused = source.discover().await.unwrap_err();
        assert!(refused.unavailable(), "{refused}");

        edge.resume().await.unwrap();
        assert!(edge.is_up());
        assert_eq!(edge.port(), port);
        let _: Value = get_json(&client, &edge.url(CURRENT), StatusCode::OK).await;
        source.discover().await.unwrap();
        // Counts span the outage.
        assert_eq!(edge.count(CURRENT), 2);
        assert_eq!(edge.count("/.well-known/pir-service-discovery"), 2);
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn https_source_trusts_the_edge_through_publisher_ca() {
    budget::e2e(async {
        let Setup { edge, .. } = setup().await;
        let mut source = source(&edge, &ca(&edge));
        let version = source.discover().await.unwrap();
        assert_eq!(version, pir_service_discovery::version());
        let (status, body) = source_body(&mut source, "current").await;
        assert_eq!(status, 200);
        let current: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(current["upstream"], "tachygram");
        let (status, body) = source_body(&mut source, "objects/bbbb").await;
        assert_eq!((status, body), (200, object("bbbb")));

        // Discovery faults reach the query host's discovery check.
        let wrong = edge.inject(Target::Discovery, Fault::DiscoveryWrongMajor);
        let error = source.discover().await.unwrap_err();
        let incompatible = error.incompatible().expect("a confirmed mismatch");
        assert_eq!(
            incompatible.publisher.major,
            pir_service_discovery::version().major + 1
        );
        assert!(pir_sync_manager::is_incompatible(&error));
        edge.remove(wrong);
        edge.inject(Target::Discovery, Fault::DiscoveryMalformed);
        let error = source.discover().await.unwrap_err();
        assert!(
            error.incompatible().is_none() && !error.unavailable(),
            "{error}"
        );
        edge.clear_faults();
        source.discover().await.unwrap();

        // Without `publisher_ca` the system roots do not trust the edge.
        let untrusting = HttpsSource::new(&edge.origin(), PREFIX, 1 << 20, &[]).unwrap();
        let error = untrusting.discover().await.unwrap_err();
        assert!(error.unavailable(), "{error}");
        // Another edge's certificate does not either.
        let other = TlsEdge::builder().start().await.unwrap();
        let mistrusting = HttpsSource::new(&edge.origin(), PREFIX, 1 << 20, &ca(&other)).unwrap();
        assert!(mistrusting.discover().await.is_err());
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_are_counted_per_path() {
    budget::e2e(async {
        let Setup { edge, client, .. } = setup().await;
        for _ in 0..3 {
            let _: Value = get_json(&client, &edge.url(CURRENT), StatusCode::OK).await;
        }
        let _ = get_bytes(&client, &edge.url(OBJECT_A), StatusCode::OK).await;
        // Faulted and unrouted requests count too.
        edge.inject_times(Target::Path(OBJECT_B.into()), Fault::not_found(), 1);
        let _ = client::get(&client, &edge.url(OBJECT_B)).await;
        let _ = get_bytes(&client, &edge.url(OBJECT_B), StatusCode::OK).await;
        let _ = client::get(&client, &edge.url("/nowhere?x=1")).await;
        let counts = edge.counts();
        assert_eq!(
            counts.into_iter().collect::<Vec<_>>(),
            vec![
                ("/nowhere".to_owned(), 1),
                (CURRENT.to_owned(), 3),
                (OBJECT_A.to_owned(), 1),
                (OBJECT_B.to_owned(), 2),
            ]
        );
        assert_eq!(edge.count_matching(&Target::Objects), 3);
        assert_eq!(edge.count_matching(&Target::Product("tachygram".into())), 6);
        let requests = edge.requests();
        assert_eq!(requests.len(), 7);
        assert!(requests.iter().all(|request| request.method == "GET"));
        edge.reset_counts();
        assert!(edge.counts().is_empty());
        let _ = get_bytes(&client, &edge.url(OBJECT_B), StatusCode::OK).await;
        assert_eq!(edge.count(OBJECT_B), 1);
        assert_eq!(edge.count(OBJECT_A), 0);
    })
    .await
}

fn chain_spec() -> ChainSpec {
    ChainSpec {
        seed: 11,
        random_blocks: 12,
        ..ChainSpec::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn node_double_binary_spawns_restarts_and_dies_with_its_handle() {
    budget::e2e(async {
        let budget = Budget::start();
        let spec = chain_spec();
        let mut node = SpawnedNode::spawn(
            binary!("tachyon-node-double-test"),
            &spec,
            budget.remaining(),
        )
        .await;
        let expected = node.chain();
        assert_eq!(expected.tip_height(), 12);
        let count = rpc(node.url(), "getblockcount", json!([])).await;
        assert_eq!(count, json!(12));
        let hash = rpc(node.url(), "getbestblockhash", json!([])).await;
        assert_eq!(hash, json!(expected.tip().hash_display()));

        let state = node.control("append", json!({"random_blocks": 3})).await;
        assert_eq!(state["tip"], json!(15));
        assert_eq!(rpc(node.url(), "getblockcount", json!([])).await, json!(15));

        // A restart keeps the URL and rebuilds the chain from the spec.
        let url = node.url().to_owned();
        let first = node.process().pid().unwrap();
        node.restart(budget.remaining()).await;
        let second = node.process().pid().unwrap();
        assert_ne!(first, second);
        assert!(!group_exists(first));
        assert_eq!(node.url(), url);
        assert_eq!(rpc(node.url(), "getblockcount", json!([])).await, json!(12));
        assert_eq!(node.state().await["tip"], json!(12));
        assert!(node.process().stdout().starts_with(&url));
        assert!(node.process().logs().contains("run 1 stdout"));

        drop(node);
        assert!(!group_exists(second), "the node double outlived its handle");
        let error = reqwest::get(format!("{url}/control/state"))
            .await
            .unwrap_err();
        assert!(error.is_connect(), "{error:?}");
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn processes_report_exits_and_readiness() {
    budget::e2e(async {
        // A refused config exits non-zero with its error on stderr.
        let mut bad = Process::new("bad-node", binary!("tachyon-node-double-test"));
        bad.config("--spec", "spec.json", &json!({"seed": 1, "unknown": true}));
        bad.start();
        let status = bad.wait_exit(Duration::from_secs(10)).await;
        assert!(!status.success());
        assert!(bad.stderr().contains("unknown"), "{}", bad.stderr());

        // A process that dies while being waited on fails the wait at once,
        // with its logs, long before the deadline.
        let mut dying = Process::new("dying-node", binary!("tachyon-node-double-test"));
        dying.config("--spec", "spec.json", &json!({"seed": 1, "unknown": true}));
        let addr = free_addrs(1)[0];
        dying.start();
        let started = Instant::now();
        let failure = tokio::spawn(async move {
            dying
                .wait_ready(&addr.to_string(), Duration::from_secs(20))
                .await
        })
        .await
        .unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(5));
        let message = failure.into_panic();
        let message = message
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_default();
        assert!(message.contains("dying-node exited"), "{message}");
        assert!(message.contains("run 0 stderr"), "{message}");

        // Readiness polls `GET /ready` on the given listener until 200.
        let spec = serde_json::to_value(chain_spec()).unwrap();
        let mut node = Process::new("ready-node", binary!("tachyon-node-double-test"));
        node.config("--spec", "spec.json", &spec);
        node.start();
        let url = node.wait_stdout_line(Duration::from_secs(10)).await;
        let ready = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let management = ready.local_addr().unwrap().to_string();
        let late = Arc::new(Notify::new());
        let router = Router::new().route(
            "/ready",
            get({
                let late = late.clone();
                move || async move {
                    late.notify_one();
                    StatusCode::OK
                }
            }),
        );
        let waiting = tokio::spawn(async move {
            node.wait_ready(&management, Duration::from_secs(10)).await;
            node
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!waiting.is_finished(), "ready before anything listened");
        tokio::spawn(async move { axum::serve(ready, router).await.unwrap() });
        let mut node = waiting.await.unwrap();
        late.notified().await;
        node.assert_running();
        assert!(url.starts_with("http://127.0.0.1:"));
        let pid = node.pid().unwrap();
        let status = node.terminate(Duration::from_secs(10)).await;
        assert!(!status.success());
        assert!(!group_exists(pid));
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn in_process_node_double_serves_the_chain() {
    budget::e2e(async {
        let chain = SyntheticChain::generate(ChainConfig::with_seed(3), 9);
        let tip = chain.tip().hash_display();
        let node = start_node(chain).await;
        assert_eq!(rpc(&node.url(), "getblockcount", json!([])).await, json!(9));
        assert_eq!(
            rpc(&node.url(), "getblockhash", json!([9])).await,
            json!(tip)
        );
    })
    .await
}

#[test]
fn reserved_ports_are_distinct_and_free() {
    let _budget = Budget::start();
    let addrs = free_addrs(16);
    let distinct: BTreeSet<_> = addrs.iter().map(|addr| addr.port()).collect();
    assert_eq!(distinct.len(), 16);
    for addr in addrs {
        assert!(addr.ip().is_loopback() && addr.port() != 0);
        std::net::TcpListener::bind(addr).expect("a reserved port can be bound");
    }
    assert_ne!(tachyon_pir_e2e_tests::free_port(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn budget_fails_a_test_that_overruns() {
    let overrun = tokio::spawn(budget::within(
        Duration::from_millis(50),
        tokio::time::sleep(Duration::from_secs(5)),
    ))
    .await
    .unwrap_err()
    .into_panic();
    let message = overrun
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(
        message.contains("exceeded its wall-clock budget"),
        "{message}"
    );
    assert_eq!(budget::e2e(async { 7 }).await, 7);
    let budget = Budget::with_limit(Duration::from_millis(100));
    std::thread::sleep(Duration::from_millis(150));
    let dropped = std::panic::catch_unwind(move || drop(budget)).unwrap_err();
    let message = dropped
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(message.contains("budget 0.1 s"), "{message}");
}
