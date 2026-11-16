//! Pure client checks that need no server: request layout, map and chunk
//! digests, floor refusal and empty scopes.

use tachyon_pir_cli::{
    Client, ClientError, Coverage, MapHead, MapView, ScopeSelector, Stats, parse_key, query_body,
};
use tachyon_pir_protocol::{
    BlockHash, Bytes32, ChainMapChunk, ChainMapHead, Geometry, HEADER_BYTES, Product,
    RecoveryEpoch, RegistryMapHead, RegistryScope, Scope, ScopeState, SealedChunkRef, Table,
    map_sha256,
};

const ACTIVATION: u32 = 8;
const EPOCH: u32 = 16;
/// Nothing listens here: any request the client sent would fail.
const DEAD_ORIGIN: &str = "http://127.0.0.1:9";

fn genesis() -> BlockHash {
    BlockHash::from_protocol_bytes([0x11; 32])
}

fn table(product: Product, scope_id: u64, entries: u64) -> Table {
    let mut table = Table {
        bucket: 0,
        segment: 0,
        geometry: Geometry::Tachyon4k8,
        seed: 0,
        entries,
        rows_sha256: Bytes32([0x22; 32]),
        public_sha256: Bytes32([0x33; 32]),
        session_id: Bytes32([0; 32]),
    };
    table.session_id = table.expected_session_id(product, scope_id);
    table
}

fn scope(product: Product, scope_id: u64, state: ScopeState, last: u32, entries: u64) -> Scope {
    let first = ACTIVATION + scope_id as u32 * EPOCH;
    Scope {
        scope_id,
        state,
        first_height: first,
        last_height: last,
        entries,
        n_buckets: 1,
        tables: if entries == 0 {
            Vec::new()
        } else {
            vec![table(product, scope_id, entries)]
        },
    }
}

/// A memo map: scope 0 sealed with one table, scope 1 sealed and empty (both
/// in chunk 0), scope 2 open with one table; floor 1.
fn memo_map() -> (ChainMapHead, Vec<u8>) {
    let product = Product::Memo;
    let chunk = ChainMapChunk {
        protocol: product.protocol().into(),
        base: 0,
        scopes: vec![
            scope(product, 0, ScopeState::Sealed, ACTIVATION + EPOCH - 1, 5),
            scope(
                product,
                1,
                ScopeState::Sealed,
                ACTIVATION + 2 * EPOCH - 1,
                0,
            ),
        ],
    };
    let chunk_bytes = chunk.to_bytes();
    let anchor = ACTIVATION + 2 * EPOCH + 3;
    let head = ChainMapHead {
        protocol: product.protocol().into(),
        profile: tachyon_pir_protocol::PROFILE.into(),
        network: "test".into(),
        genesis_hash: genesis(),
        activation_height: ACTIVATION,
        epoch_length: EPOCH,
        generation: 7,
        recovery_epoch: RecoveryEpoch::new(1).unwrap(),
        anchor_height: anchor,
        anchor_hash: BlockHash::from_protocol_bytes([0x44; 32]),
        floor_scope: 1,
        sealed_chunks: vec![SealedChunkRef {
            base: 0,
            sha256: map_sha256(&chunk_bytes),
        }],
        open_scopes: vec![scope(product, 2, ScopeState::Open, anchor, 3)],
    };
    (head, chunk_bytes)
}

fn memo_view() -> MapView {
    let (head, chunk) = memo_map();
    let bytes = head.to_bytes().unwrap();
    let sha = map_sha256(&bytes);
    let (head, digest) = MapHead::from_response(Product::Memo, Some(&sha.hex()), &bytes).unwrap();
    let chunk = head.parse_chunk(Product::Memo, 0, &chunk).unwrap();
    MapView::build(Product::Memo, digest, head, vec![chunk]).unwrap()
}

#[test]
fn request_body_layout() {
    for product in Product::ALL {
        let geometry = Geometry::Tachyon4k8;
        let payload: Vec<u8> = (0..geometry.payload_len()).map(|i| i as u8).collect();
        let session = Bytes32([0xa5; 32]);
        let nonce = [0x5a; 16];
        let (header, body) = query_body(product, session, nonce, &payload);
        assert_eq!(body.len(), 28_212);
        assert_eq!(body.len(), geometry.request_len());
        let magic: &[u8; 4] = match product {
            Product::Memo => b"TMQ1",
            Product::FirstContact => b"TFQ1",
            Product::Tachygram => b"TGQ1",
            Product::Registry => b"TRQ1",
        };
        assert_eq!(&body[..4], magic);
        assert_eq!(&body[4..36], &[0xa5; 32]);
        assert_eq!(&body[36..52], &[0x5a; 16]);
        assert_eq!(&body[HEADER_BYTES..], &payload[..]);
        assert_eq!(header.encode(), body[..52]);
    }
}

#[test]
fn map_head_digest_header_and_encoding_are_checked() {
    let (head, _) = memo_map();
    let bytes = head.to_bytes().unwrap();
    let sha = map_sha256(&bytes).hex();
    MapHead::from_response(Product::Memo, Some(&sha), &bytes).unwrap();
    // Surrounding whitespace in the header value is tolerated.
    MapHead::from_response(Product::Memo, Some(&format!(" {sha} ")), &bytes).unwrap();

    assert!(matches!(
        MapHead::from_response(Product::Memo, None, &bytes),
        Err(ClientError::MapHeader { header: None, .. })
    ));
    let wrong = "00".repeat(32);
    assert!(matches!(
        MapHead::from_response(Product::Memo, Some(&wrong), &bytes),
        Err(ClientError::MapHeader { .. })
    ));
    // Tampered body under the original header: the digest no longer matches.
    let mut tampered = bytes.clone();
    let at = tampered.len() / 2;
    tampered[at] ^= 0x01;
    assert!(matches!(
        MapHead::from_response(Product::Memo, Some(&sha), &tampered),
        Err(ClientError::MapHeader { .. })
    ));
    // A valid head in a non-canonical encoding under a matching header.
    let pretty = serde_json::to_vec_pretty(&head).unwrap();
    let pretty_sha = map_sha256(&pretty).hex();
    assert!(matches!(
        MapHead::from_response(Product::Memo, Some(&pretty_sha), &pretty),
        Err(ClientError::NonCanonical("map head"))
    ));
    // A head of another product's protocol is refused.
    assert!(matches!(
        MapHead::from_response(Product::Tachygram, Some(&sha), &bytes),
        Err(ClientError::Map(_))
    ));
    // The registry form of a chain product's map does not parse.
    assert!(matches!(
        MapHead::from_response(Product::Registry, Some(&sha), &bytes),
        Err(ClientError::Map(_))
    ));
}

#[test]
fn map_chunk_digest_and_encoding_are_checked() {
    let (head, chunk) = memo_map();
    let bytes = head.to_bytes().unwrap();
    let sha = map_sha256(&bytes).hex();
    let (parsed, _) = MapHead::from_response(Product::Memo, Some(&sha), &bytes).unwrap();
    parsed.parse_chunk(Product::Memo, 0, &chunk).unwrap();

    let mut tampered = chunk.clone();
    let at = tampered.len() / 2;
    tampered[at] ^= 0x01;
    assert!(matches!(
        parsed.parse_chunk(Product::Memo, 0, &tampered),
        Err(ClientError::ChunkDigest { base: 0 })
    ));
    assert!(matches!(
        parsed.parse_chunk(Product::Memo, 0, &chunk[..chunk.len() - 1]),
        Err(ClientError::ChunkDigest { base: 0 })
    ));
    // A head that references a non-canonical encoding of a valid chunk.
    let value: ChainMapChunk = serde_json::from_slice(&chunk).unwrap();
    let pretty = serde_json::to_vec_pretty(&value).unwrap();
    let mut head = head;
    head.sealed_chunks[0].sha256 = map_sha256(&pretty);
    let bytes = head.to_bytes().unwrap();
    let sha = map_sha256(&bytes).hex();
    let (parsed, _) = MapHead::from_response(Product::Memo, Some(&sha), &bytes).unwrap();
    assert!(matches!(
        parsed.parse_chunk(Product::Memo, 0, &pretty),
        Err(ClientError::NonCanonical("map chunk"))
    ));
    // A chunk the digest names but whose content breaks the listing: scope 0
    // claims entries its table does not hold.
    let mut bad = value.clone();
    bad.scopes[0].entries = 6;
    let bad_bytes = bad.to_bytes();
    head.sealed_chunks[0].sha256 = map_sha256(&bad_bytes);
    let bytes = head.to_bytes().unwrap();
    let sha = map_sha256(&bytes).hex();
    let (parsed, _) = MapHead::from_response(Product::Memo, Some(&sha), &bytes).unwrap();
    assert!(matches!(
        parsed.parse_chunk(Product::Memo, 0, &bad_bytes),
        Err(ClientError::Map(_))
    ));
}

#[test]
fn view_lists_scopes_by_index_and_id() {
    let view = memo_view();
    assert_eq!(view.scopes().len(), 3);
    for i in 0..3 {
        let (index, scope) = view.scope(ScopeSelector::Index(i)).unwrap();
        assert_eq!((index, scope.scope_id()), (i, i));
        assert_eq!(view.scope(ScopeSelector::Id(i)).unwrap().0, i);
    }
    assert!(view.scope(ScopeSelector::Index(3)).is_none());
    assert!(view.scope(ScopeSelector::Id(3)).is_none());
    assert_eq!(view.floor_scope(), 1);
}

#[tokio::test]
async fn below_floor_is_refused_without_a_request() {
    let view = memo_view();
    let client = Client::new(DEAD_ORIGIN, Product::Memo, None).unwrap();
    let key = [0x77; 16];
    for selector in [ScopeSelector::Index(0), ScopeSelector::Id(0)] {
        let outcome = client.lookup(&view, selector, &key).await.unwrap();
        assert_eq!(
            outcome,
            tachyon_pir_cli::LookupOutcome::BelowFloor {
                scope_index: 0,
                floor_scope: 1
            }
        );
    }
    assert!(matches!(
        client.dummy_query(&view, ScopeSelector::Index(0)).await,
        Err(ClientError::BelowFloor {
            scope_index: 0,
            floor_scope: 1
        })
    ));
    assert_eq!(client.stats(), Stats::default());
}

#[tokio::test]
async fn empty_scope_is_absent_with_zero_queries() {
    let view = memo_view();
    let client = Client::new(DEAD_ORIGIN, Product::Memo, None).unwrap();
    let outcome = client
        .lookup(&view, ScopeSelector::Index(1), &[0x77; 16])
        .await
        .unwrap();
    let tachyon_pir_cli::LookupOutcome::Absent { answer } = outcome else {
        panic!("{outcome:?}");
    };
    assert!(answer.queries.is_empty());
    assert_eq!(answer.bucket, None);
    assert_eq!(answer.scope_id, 1);
    assert_eq!(answer.map_sha256, view.map_sha256());
    assert_eq!(
        answer.coverage,
        Coverage::Heights {
            state: ScopeState::Sealed,
            first_height: ACTIVATION + EPOCH,
            last_height: ACTIVATION + 2 * EPOCH - 1,
            entries: 0,
        }
    );
    // A dummy query cannot be sent to a scope with no tables.
    assert!(matches!(
        client.dummy_query(&view, ScopeSelector::Index(1)).await,
        Err(ClientError::EmptyScope(1))
    ));
    assert_eq!(client.stats(), Stats::default());
}

#[tokio::test]
async fn unknown_scope_and_bad_key_send_nothing() {
    let view = memo_view();
    let client = Client::new(DEAD_ORIGIN, Product::Memo, None).unwrap();
    assert!(matches!(
        client
            .lookup(&view, ScopeSelector::Index(9), &[1; 16])
            .await,
        Err(ClientError::UnknownScope(ScopeSelector::Index(9)))
    ));
    assert!(matches!(
        client
            .lookup(&view, ScopeSelector::Index(2), &[1; 32])
            .await,
        Err(ClientError::KeyLength {
            expected: 16,
            actual: 32
        })
    ));
    assert_eq!(client.stats(), Stats::default());
}

#[test]
fn registry_map_coverage_is_by_sequence() {
    let product = Product::Registry;
    let mut open_table = table(product, 0, 4);
    open_table.session_id = open_table.expected_session_id(product, 0);
    let head = RegistryMapHead {
        protocol: product.protocol().into(),
        profile: tachyon_pir_protocol::PROFILE.into(),
        network: "test".into(),
        genesis_hash: genesis(),
        generation: 3,
        recovery_epoch: RecoveryEpoch::new(2).unwrap(),
        log_sequence: 4,
        floor_scope: 0,
        sealed_chunks: Vec::new(),
        open_scopes: vec![RegistryScope {
            scope_id: 0,
            state: ScopeState::Open,
            entries: 4,
            n_buckets: 1,
            tables: vec![open_table],
        }],
    };
    let bytes = head.to_bytes().unwrap();
    let sha = map_sha256(&bytes);
    let (parsed, digest) = MapHead::from_response(product, Some(&sha.hex()), &bytes).unwrap();
    let view = MapView::build(product, digest, parsed, Vec::new()).unwrap();
    let (_, scope) = view.scope(ScopeSelector::Index(0)).unwrap();
    assert_eq!(
        scope.coverage(),
        Coverage::Sequences {
            state: ScopeState::Open,
            first_sequence: 0,
            entries: 4,
        }
    );
    // A chain product refuses the registry form.
    assert!(MapHead::from_response(Product::Memo, Some(&sha.hex()), &bytes).is_err());
}

#[test]
fn keys_parse_as_key16_or_full_tag() {
    let tag = "ab".repeat(32);
    assert_eq!(parse_key(Product::Memo, &tag).unwrap(), vec![0xab; 16]);
    assert_eq!(
        parse_key(Product::Memo, &"cd".repeat(16)).unwrap(),
        vec![0xcd; 16]
    );
    assert_eq!(parse_key(Product::Tachygram, &tag).unwrap(), vec![0xab; 32]);
    assert!(parse_key(Product::Tachygram, &"ab".repeat(16)).is_err());
    assert!(parse_key(Product::Registry, "zz").is_err());
    assert!(parse_key(Product::FirstContact, &"ab".repeat(20)).is_err());
}

#[test]
fn origins_must_be_http_urls() {
    for origin in ["ftp://x", "not a url", "unix:/tmp/sock"] {
        assert!(matches!(
            Client::new(origin, Product::Memo, None),
            Err(ClientError::Origin(_))
        ));
    }
    let client = Client::new("https://example.test:8443/", Product::FirstContact, None).unwrap();
    assert_eq!(
        client.base_url(),
        "https://example.test:8443/first-contact/v1"
    );
    assert!(matches!(
        Client::new("https://x", Product::Memo, Some(b"not a pem")),
        Err(ClientError::Certificate(_))
    ));
}
