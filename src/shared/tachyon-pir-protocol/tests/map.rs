//! Map head, chunk, scope and table rules (spec 6.3, 6.4, 10.3), and the query
//! plan for one key.

use tachyon_pir_protocol::{
    BlockHash, Bytes32, ChainMapChunk, ChainMapHead, Entry, Geometry, MapChunk, MapError,
    MemoEntry, PROFILE, Product, RecoveryEpoch, RegistryMapChunk, RegistryMapHead, RegistryScope,
    Scope, ScopeState, ScopeTables, SealedChunkRef, Table, bucket, encode_row, map_sha256,
    validate_row,
};

const GENESIS: &str = "05a60a92d99d85997cce3b87616c089f6124d7342af37106edc76126334a2c38";
const ANCHOR: &str = "12a3e1410d735294ab607da65d8e471183c3e61e3be4f5ebdff1d1203cd794b9";

fn genesis() -> BlockHash {
    GENESIS.parse().unwrap()
}

/// `(bucket, segment, seed, entries)`.
type Spec = (u32, u32, u32, u64);

/// A named change to a document that must make it invalid.
type Mutation<T> = (&'static str, Box<dyn Fn(&mut T)>);

fn table(product: Product, scope_id: u64, (bucket, segment, seed, entries): Spec) -> Table {
    let tag = format!("{product}/{scope_id}/{bucket}/{segment}/{seed}/{entries}");
    let mut t = Table {
        bucket,
        segment,
        geometry: Geometry::Tachyon4k8,
        seed,
        entries,
        rows_sha256: map_sha256(format!("rows {tag}").as_bytes()),
        public_sha256: map_sha256(format!("public {tag}").as_bytes()),
        session_id: Bytes32::default(),
    };
    t.session_id = t.expected_session_id(product, scope_id);
    t
}

fn resign(product: Product, scope_id: u64, tables: &mut [Table]) {
    for t in tables {
        t.session_id = t.expected_session_id(product, scope_id);
    }
}

fn scope(
    product: Product,
    scope_id: u64,
    state: ScopeState,
    (first_height, last_height): (u32, u32),
    n_buckets: u32,
    specs: &[Spec],
) -> Scope {
    Scope {
        scope_id,
        state,
        first_height,
        last_height,
        entries: specs.iter().map(|s| s.3).sum(),
        n_buckets,
        tables: specs.iter().map(|s| table(product, scope_id, *s)).collect(),
    }
}

const ACTIVATION: u32 = 8;
const EPOCH: u32 = 16;

fn epoch(e: u32) -> (u32, u32) {
    let first = ACTIVATION + e * EPOCH;
    (first, first + EPOCH - 1)
}

/// The tables of sealed epoch `e`: some empty, some with two buckets and an
/// overflow segment, the rest one bucket under a nonzero seed.
fn epoch_tables(e: u32) -> (u32, Vec<Spec>) {
    if e.is_multiple_of(5) {
        (1, vec![])
    } else if e % 7 == 3 {
        (2, vec![(0, 0, 0, 10), (0, 1, 4, 2), (1, 0, 0, 9)])
    } else {
        (1, vec![(0, 0, e % 64, 5)])
    }
}

fn head(
    product: Product,
    chunks: &[ChainMapChunk],
    open_scopes: Vec<Scope>,
    anchor_height: u32,
) -> ChainMapHead {
    ChainMapHead {
        protocol: product.protocol().into(),
        profile: PROFILE.into(),
        network: "test".into(),
        genesis_hash: genesis(),
        activation_height: ACTIVATION,
        epoch_length: EPOCH,
        generation: 9123,
        recovery_epoch: RecoveryEpoch::new(3).unwrap(),
        anchor_height,
        anchor_hash: ANCHOR.parse().unwrap(),
        floor_scope: 0,
        sealed_chunks: chunks
            .iter()
            .map(|c| SealedChunkRef {
                base: c.base,
                sha256: c.sha256(),
            })
            .collect(),
        open_scopes,
    }
}

/// 34 sealed epochs in two chunks, epoch 34 complete but open, epoch 35 open
/// up to the anchor at 570.
fn epoch_fixture(product: Product) -> (ChainMapHead, Vec<ChainMapChunk>) {
    let sealed: Vec<Scope> = (0..34)
        .map(|e| {
            let (n, specs) = epoch_tables(e);
            scope(product, e.into(), ScopeState::Sealed, epoch(e), n, &specs)
        })
        .collect();
    let chunks: Vec<ChainMapChunk> = sealed
        .chunks(32)
        .enumerate()
        .map(|(k, scopes)| MapChunk {
            protocol: product.protocol().into(),
            base: 32 * k as u64,
            scopes: scopes.to_vec(),
        })
        .collect();
    let open = vec![
        scope(product, 34, ScopeState::Open, epoch(34), 1, &[(0, 0, 1, 3)]),
        scope(
            product,
            35,
            ScopeState::Open,
            (epoch(35).0, 570),
            1,
            &[(0, 0, 0, 1)],
        ),
    ];
    (head(product, &chunks, open, 570), chunks)
}

fn invalid(result: Result<(), MapError>) -> String {
    match result {
        Err(MapError::Invalid(reason)) => reason,
        other => panic!("expected an invalid map, got {other:?}"),
    }
}

#[test]
fn spec_example_head_is_valid() {
    let session = {
        let t = Table {
            bucket: 0,
            segment: 0,
            geometry: Geometry::Tachyon4k8,
            seed: 0,
            entries: 51230,
            rows_sha256: Bytes32([0x11; 32]),
            public_sha256: Bytes32([0x22; 32]),
            session_id: Bytes32::default(),
        };
        t.expected_session_id(Product::Memo, 9)
    };
    let json = format!(
        r#"{{
          "protocol": "tachyon-memo-pir-v1",
          "profile": "reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1",
          "network": "test",
          "genesis_hash": "{GENESIS}",
          "activation_height": 8,
          "epoch_length": 4096,
          "generation": 9123,
          "recovery_epoch": "3",
          "anchor_height": 39584,
          "anchor_hash": "{ANCHOR}",
          "floor_scope": 0,
          "sealed_chunks": [ {{ "base": 0, "sha256": "{chunk}" }} ],
          "open_scopes": [ {{
            "scope_id": 9, "state": "open", "first_height": 36872, "last_height": 39584,
            "entries": 51230, "n_buckets": 1,
            "tables": [ {{
              "bucket": 0, "segment": 0, "geometry": "tachyon-4k8", "seed": 0, "entries": 51230,
              "rows_sha256": "{rows}", "public_sha256": "{public}", "session_id": "{session}"
            }} ]
          }} ]
        }}"#,
        chunk = "33".repeat(32),
        rows = "11".repeat(32),
        public = "22".repeat(32),
    );
    let head = ChainMapHead::parse(json.as_bytes()).unwrap();
    head.validate(Product::Memo).unwrap();
    assert_eq!(head.genesis_hash.protocol_bytes()[0], 0x38);
    assert_eq!(head.recovery_epoch.get(), 3);
    assert!(head.validate(Product::Tachygram).is_err());
    assert!(head.validate(Product::Registry).is_err());
    // Canonical bytes round-trip.
    let bytes = head.to_bytes().unwrap();
    assert_eq!(ChainMapHead::parse(&bytes).unwrap(), head);
    assert!(
        String::from_utf8(bytes)
            .unwrap()
            .contains(r#""recovery_epoch":"3""#)
    );
}

#[test]
fn chain_listing_is_valid_for_memo_and_tachygram() {
    for product in [Product::Memo, Product::Tachygram] {
        let (head, chunks) = epoch_fixture(product);
        head.validate(product).unwrap();
        head.check_listing(product, &chunks).unwrap();
        for (position, chunk) in chunks.iter().enumerate() {
            let parsed = head
                .parse_chunk(product, position, &chunk.to_bytes())
                .unwrap();
            assert_eq!(&parsed, chunk);
        }
        assert_eq!(
            ChainMapHead::parse(&head.to_bytes().unwrap()).unwrap(),
            head
        );
    }
}

#[test]
fn chunk_digest_and_shape_are_checked() {
    let product = Product::Memo;
    let (head, chunks) = epoch_fixture(product);
    let mut bytes = chunks[1].to_bytes();
    bytes.push(b' ');
    assert!(matches!(
        head.parse_chunk(product, 1, &bytes),
        Err(MapError::Digest)
    ));
    assert!(matches!(
        head.parse_chunk(product, 2, &chunks[1].to_bytes()),
        Err(MapError::Invalid(_))
    ));
    // Chunks swapped.
    assert!(head.parse_chunk(product, 0, &chunks[1].to_bytes()).is_err());

    let checks: Vec<Mutation<ChainMapChunk>> = vec![
        (
            "protocol",
            Box::new(|c| c.protocol = Product::Tachygram.protocol().into()),
        ),
        ("base", Box::new(|c| c.base = 1)),
        (
            "short",
            Box::new(|c| {
                c.scopes.pop();
            }),
        ),
        (
            "open scope",
            Box::new(|c| c.scopes[3].state = ScopeState::Open),
        ),
        (
            "gap",
            Box::new(|c| {
                c.scopes.remove(4);
                let extra = c.scopes[30].clone();
                c.scopes.push(extra);
            }),
        ),
    ];
    for (name, mutate) in checks {
        let mut chunk = chunks[0].clone();
        mutate(&mut chunk);
        assert!(head.check_chunk(product, 0, &chunk).is_err(), "{name}");
    }

    // The last chunk must end right before the first open scope.
    let mut last = chunks[1].clone();
    last.scopes.pop();
    assert!(head.check_chunk(product, 1, &last).is_err());
    // A listing needs every chunk.
    assert!(head.check_listing(product, &chunks[..1]).is_err());
}

#[test]
fn head_rules() {
    let product = Product::Tachygram;
    let (good, chunks) = epoch_fixture(product);
    let checks: Vec<Mutation<ChainMapHead>> = vec![
        (
            "protocol",
            Box::new(|h| h.protocol = Product::Memo.protocol().into()),
        ),
        (
            "profile",
            Box::new(|h| h.profile = "reinspiring-two-mask-p16".into()),
        ),
        ("epoch length", Box::new(|h| h.epoch_length = 0)),
        ("chunk base", Box::new(|h| h.sealed_chunks[1].base = 33)),
        (
            "chunk count",
            Box::new(|h| {
                h.sealed_chunks.pop();
            }),
        ),
        (
            "sealed in open",
            Box::new(|h| h.open_scopes[0].state = ScopeState::Sealed),
        ),
        ("anchor", Box::new(|h| h.anchor_height = 571)),
        (
            "anchor above epoch",
            Box::new(|h| {
                h.anchor_height = 600;
                h.open_scopes[1].last_height = 600;
            }),
        ),
        (
            "first height",
            Box::new(|h| h.open_scopes[1].first_height += 1),
        ),
        (
            "incomplete older open",
            Box::new(|h| h.open_scopes[0].last_height -= 1),
        ),
        ("floor", Box::new(|h| h.floor_scope = 36)),
        ("order", Box::new(|h| h.open_scopes.swap(0, 1))),
        (
            "no scopes",
            Box::new(|h| {
                h.open_scopes.clear();
                h.sealed_chunks.clear();
            }),
        ),
    ];
    for (name, mutate) in checks {
        let mut head = good.clone();
        mutate(&mut head);
        assert!(head.validate(product).is_err(), "{name}");
    }

    // A missing open scope is consistent with the head's chunk count; the
    // last chunk's join with the open scopes catches it.
    let mut gap = good.clone();
    gap.open_scopes.remove(0);
    gap.validate(product).unwrap();
    assert!(gap.check_listing(product, &chunks).is_err());

    let mut floor = good.clone();
    floor.floor_scope = 35;
    floor.check_listing(product, &chunks).unwrap();

    // All sealed: the newest sealed scope must end at the anchor.
    let mut sealed_only = good.clone();
    sealed_only.open_scopes.clear();
    sealed_only.anchor_height = epoch(33).1;
    sealed_only.check_listing(product, &chunks).unwrap();
    sealed_only.anchor_height = epoch(33).1 + 1;
    assert!(sealed_only.check_listing(product, &chunks).is_err());

    // Before activation nothing is listed.
    let mut early = good.clone();
    early.anchor_height = ACTIVATION - 1;
    early.open_scopes.clear();
    early.sealed_chunks.clear();
    early.validate(product).unwrap();
    early.check_listing(product, &[]).unwrap();
}

#[test]
fn table_rules() {
    let product = Product::Memo;
    let base = scope(
        product,
        4,
        ScopeState::Open,
        epoch(4),
        2,
        &[(0, 0, 0, 10), (0, 1, 4, 2), (1, 0, 0, 9)],
    );
    base.check_tables(product).unwrap();
    let checks: Vec<Mutation<Scope>> = vec![
        ("bucket range", Box::new(|s| s.tables[2].bucket = 2)),
        ("segment gap", Box::new(|s| s.tables[1].segment = 2)),
        ("segment-0 seeds", Box::new(|s| s.tables[2].seed = 1)),
        ("entry sum", Box::new(|s| s.entries += 1)),
        (
            "session",
            Box::new(|s| s.tables[0].session_id = Bytes32([1; 32])),
        ),
        ("seed range", Box::new(|s| s.tables[1].seed = 64)),
        ("order", Box::new(|s| s.tables.swap(0, 1))),
        (
            "duplicate",
            Box::new(|s| {
                let t = s.tables[2].clone();
                s.tables.push(t);
                s.entries += 9;
            }),
        ),
        (
            "empty overflow",
            Box::new(|s| {
                s.entries -= s.tables[1].entries;
                s.tables[1].entries = 0;
            }),
        ),
        (
            "missing bucket",
            Box::new(|s| {
                s.entries -= 9;
                s.tables.pop();
            }),
        ),
        ("n_buckets", Box::new(|s| s.n_buckets = 3)),
        ("n_buckets max", Box::new(|s| s.n_buckets = 128)),
        (
            "geometry",
            Box::new(|s| s.tables[0].geometry = Geometry::Tachyon2k8),
        ),
        (
            "capacity",
            Box::new(|s| {
                s.tables[0].entries = 56 * 4096 + 1;
                s.entries = s.tables.iter().map(|t| t.entries).sum();
            }),
        ),
    ];
    for (name, mutate) in checks {
        let mut s = base.clone();
        mutate(&mut s);
        if name != "session" {
            resign(product, s.scope_id, &mut s.tables);
        }
        assert!(!invalid(s.check_tables(product)).is_empty(), "{name}");
    }

    // A nonzero scope seed means every entry fitted: no overflow segments.
    let mut s = scope(
        product,
        4,
        ScopeState::Open,
        epoch(4),
        1,
        &[(0, 0, 3, 10), (0, 1, 4, 2)],
    );
    invalid(s.check_tables(product));
    s = scope(
        product,
        4,
        ScopeState::Open,
        epoch(4),
        1,
        &[(0, 0, 0, 10), (0, 1, 4, 2)],
    );
    s.check_tables(product).unwrap();

    // Eight segments at most.
    let eight: Vec<Spec> = (0..8).map(|k| (0, k, k, 1)).collect();
    scope(product, 4, ScopeState::Open, epoch(4), 1, &eight)
        .check_tables(product)
        .unwrap();
    let nine: Vec<Spec> = (0..9).map(|k| (0, k, k, 1)).collect();
    invalid(scope(product, 4, ScopeState::Open, epoch(4), 1, &nine).check_tables(product));

    // An empty scope has one bucket and no tables.
    let empty = scope(product, 4, ScopeState::Open, epoch(4), 1, &[]);
    empty.check_tables(product).unwrap();
    let mut two = empty.clone();
    two.n_buckets = 2;
    invalid(two.check_tables(product));
    invalid(
        scope(product, 4, ScopeState::Open, epoch(4), 1, &[(0, 0, 0, 0)]).check_tables(product),
    );

    // A table's session belongs to its product and scope.
    let other = scope(
        Product::Tachygram,
        4,
        ScopeState::Open,
        epoch(4),
        1,
        &[(0, 0, 0, 3)],
    );
    invalid(other.check_tables(product));
    let mut moved = base.clone();
    moved.scope_id = 5;
    invalid(moved.check_tables(product));
}

#[test]
fn first_contact_spans() {
    let product = Product::FirstContact;
    let span = |id: u32, epochs: u32, state, specs: &[Spec]| {
        let first = epoch(id).0;
        scope(
            product,
            id.into(),
            state,
            (first, first + epochs * EPOCH - 1),
            1,
            specs,
        )
    };
    let sealed = vec![
        span(0, 3, ScopeState::Sealed, &[(0, 0, 0, 3100)]),
        span(3, 1, ScopeState::Sealed, &[(0, 0, 2, 3500)]),
        span(4, 64, ScopeState::Sealed, &[]),
    ];
    let chunk = MapChunk {
        protocol: product.protocol().into(),
        base: 0,
        scopes: sealed,
    };
    let open_first = epoch(68).0;
    let open = scope(
        product,
        68,
        ScopeState::Open,
        (open_first, open_first + 4),
        1,
        &[(0, 0, 0, 7)],
    );
    let good = head(
        product,
        std::slice::from_ref(&chunk),
        vec![open],
        open_first + 4,
    );
    good.validate(product).unwrap();
    good.check_listing(product, std::slice::from_ref(&chunk))
        .unwrap();

    // floor_scope is a scope index (span position), so 3 is the newest.
    let mut floor = good.clone();
    floor.floor_scope = 3;
    floor
        .check_listing(product, std::slice::from_ref(&chunk))
        .unwrap();
    floor.floor_scope = 4;
    assert!(
        floor
            .check_listing(product, std::slice::from_ref(&chunk))
            .is_err()
    );

    let checks: Vec<Mutation<ChainMapChunk>> = vec![
        (
            "not on an epoch boundary",
            Box::new(|c| c.scopes[0].last_height -= 1),
        ),
        (
            "more than 64 epochs",
            Box::new(|c| c.scopes[2].last_height += EPOCH),
        ),
        (
            "gap",
            Box::new(|c| {
                c.scopes[1].scope_id = 4;
                c.scopes[1].first_height = epoch(4).0;
                c.scopes[1].last_height = epoch(4).1;
                c.scopes[1].tables[0].session_id =
                    c.scopes[1].tables[0].expected_session_id(Product::FirstContact, 4);
            }),
        ),
        (
            "start",
            Box::new(|c| {
                c.scopes.remove(0);
            }),
        ),
    ];
    for (name, mutate) in checks {
        let mut c = chunk.clone();
        mutate(&mut c);
        let h = head(
            product,
            std::slice::from_ref(&c),
            good.open_scopes.clone(),
            good.anchor_height,
        );
        assert!(h.check_listing(product, &[c]).is_err(), "{name}");
    }

    // The chunk count must be possible for the first open span: 68 epochs in
    // two or more spans cannot fill 33+ sealed chunks, nor zero.
    let mut none = good.clone();
    none.sealed_chunks.clear();
    assert!(none.validate(product).is_err());
}

fn registry_fixture() -> (RegistryMapHead, Vec<RegistryMapChunk>) {
    let product = Product::Registry;
    let registry_scope = |id: u64, state, entries: u64| {
        let tables = if entries == 0 {
            vec![]
        } else {
            vec![table(product, id, (0, 0, (id % 3) as u32, entries))]
        };
        RegistryScope {
            scope_id: id,
            state,
            entries,
            n_buckets: 1,
            tables,
        }
    };
    let sealed: Vec<RegistryScope> = (0..33)
        .map(|id| registry_scope(id, ScopeState::Sealed, 3072))
        .collect();
    let chunks: Vec<RegistryMapChunk> = sealed
        .chunks(32)
        .enumerate()
        .map(|(k, scopes)| MapChunk {
            protocol: product.protocol().into(),
            base: 32 * k as u64,
            scopes: scopes.to_vec(),
        })
        .collect();
    let head = RegistryMapHead {
        protocol: product.protocol().into(),
        profile: PROFILE.into(),
        network: "test".into(),
        genesis_hash: genesis(),
        generation: 4,
        recovery_epoch: RecoveryEpoch::new(1).unwrap(),
        log_sequence: 33 * 3072 + 5,
        floor_scope: 0,
        sealed_chunks: chunks
            .iter()
            .map(|c| SealedChunkRef {
                base: c.base,
                sha256: c.sha256(),
            })
            .collect(),
        open_scopes: vec![registry_scope(33, ScopeState::Open, 5)],
    };
    (head, chunks)
}

#[test]
fn registry_map() {
    let (good, chunks) = registry_fixture();
    good.validate().unwrap();
    good.check_listing(&chunks).unwrap();
    assert_eq!(
        good.parse_chunk(1, &chunks[1].to_bytes()).unwrap(),
        chunks[1]
    );
    assert_eq!(
        RegistryMapHead::parse(&good.to_bytes().unwrap()).unwrap(),
        good
    );

    let checks: Vec<Mutation<RegistryMapHead>> = vec![
        (
            "protocol",
            Box::new(|h| h.protocol = Product::Memo.protocol().into()),
        ),
        ("log sequence", Box::new(|h| h.log_sequence += 1)),
        (
            "two open",
            Box::new(|h| {
                let o = h.open_scopes[0].clone();
                h.open_scopes.push(o);
            }),
        ),
        ("no open", Box::new(|h| h.open_scopes.clear())),
        ("buckets", Box::new(|h| h.open_scopes[0].n_buckets = 2)),
        ("floor", Box::new(|h| h.floor_scope = 34)),
        (
            "chunks",
            Box::new(|h| {
                h.sealed_chunks.pop();
            }),
        ),
    ];
    for (name, mutate) in checks {
        let mut head = good.clone();
        mutate(&mut head);
        assert!(head.validate().is_err(), "{name}");
    }

    let mut not_full = chunks[0].clone();
    not_full.scopes[5].entries = 3071;
    not_full.scopes[5].tables[0].entries = 3071;
    let sid = not_full.scopes[5].tables[0].expected_session_id(Product::Registry, 5);
    not_full.scopes[5].tables[0].session_id = sid;
    assert!(good.check_chunk(0, &not_full).is_err());

    // An empty log: one open chunk 0 with no entries and no tables.
    let empty = RegistryMapHead {
        log_sequence: 0,
        sealed_chunks: vec![],
        open_scopes: vec![RegistryScope {
            scope_id: 0,
            state: ScopeState::Open,
            entries: 0,
            n_buckets: 1,
            tables: vec![],
        }],
        ..good.clone()
    };
    empty.validate().unwrap();
    empty.check_listing(&[]).unwrap();

    // A just-closed chunk: chunk 0 sealed full, chunk 1 open and empty.
    let closed = RegistryMapHead {
        log_sequence: 3072,
        sealed_chunks: vec![SealedChunkRef {
            base: 0,
            sha256: Bytes32::default(),
        }],
        open_scopes: vec![RegistryScope {
            scope_id: 1,
            ..empty.open_scopes[0].clone()
        }],
        ..good
    };
    closed.validate().unwrap();
}

#[test]
fn unknown_fields_are_refused_everywhere() {
    let (head, chunks) = epoch_fixture(Product::Memo);
    let value = serde_json::to_value(&head).unwrap();
    let with = |path: &[&str], field: &str| {
        let mut v = value.clone();
        let mut target = &mut v;
        for p in path {
            target = match p.parse::<usize>() {
                Ok(i) => &mut target[i],
                Err(_) => &mut target[*p],
            };
        }
        target
            .as_object_mut()
            .unwrap()
            .insert(field.into(), serde_json::json!(1));
        serde_json::to_vec(&v).unwrap()
    };
    assert!(ChainMapHead::parse(&serde_json::to_vec(&value).unwrap()).is_ok());
    for path in [
        &[][..],
        &["open_scopes", "0"],
        &["open_scopes", "0", "tables", "0"],
        &["sealed_chunks", "0"],
    ] {
        assert!(
            matches!(
                ChainMapHead::parse(&with(path, "extra")),
                Err(MapError::Json(_))
            ),
            "{path:?}"
        );
    }
    let mut chunk = serde_json::to_value(&chunks[0]).unwrap();
    chunk["extra"] = serde_json::json!(true);
    assert!(serde_json::from_value::<ChainMapChunk>(chunk).is_err());

    // Registry documents refuse the chain fields.
    let (registry, _) = registry_fixture();
    let value = serde_json::to_value(&registry).unwrap();
    for field in ["activation_height", "epoch_length", "anchor_height"] {
        let mut v = value.clone();
        v[field] = serde_json::json!(8);
        assert!(
            serde_json::from_value::<RegistryMapHead>(v).is_err(),
            "{field}"
        );
    }
    let mut v = value.clone();
    v["open_scopes"][0]["first_height"] = serde_json::json!(8);
    assert!(serde_json::from_value::<RegistryMapHead>(v).is_err());
    // And a chain head missing them is refused.
    let mut v = serde_json::to_value(&head).unwrap();
    v.as_object_mut().unwrap().remove("epoch_length");
    assert!(serde_json::from_value::<ChainMapHead>(v).is_err());
}

#[test]
fn geometry_and_hex_forms_are_strict() {
    let (head, _) = epoch_fixture(Product::Memo);
    let text = String::from_utf8(head.to_bytes().unwrap()).unwrap();
    // Display order on the wire for block hashes.
    assert!(text.contains(&format!(r#""genesis_hash":"{GENESIS}""#)));
    // An unknown geometry does not parse; the defined but unpublished one
    // parses and is refused.
    assert!(ChainMapHead::parse(text.replacen("tachyon-4k8", "txid-4k8", 1).as_bytes()).is_err());
    let unpublished =
        ChainMapHead::parse(text.replacen("tachyon-4k8", "tachyon-2k8", 1).as_bytes()).unwrap();
    assert!(unpublished.validate(Product::Memo).is_err());
    // Uppercase hex is refused.
    let upper = text.replacen(GENESIS, &GENESIS.to_uppercase(), 1);
    assert!(ChainMapHead::parse(upper.as_bytes()).is_err());
    // recovery_epoch as a number is refused.
    let number = text.replacen(r#""recovery_epoch":"3""#, r#""recovery_epoch":3"#, 1);
    assert!(ChainMapHead::parse(number.as_bytes()).is_err());
}

#[test]
fn head_size_limit_is_one_mebibyte() {
    let product = Product::Memo;
    assert_eq!(tachyon_pir_protocol::MAX_MAP_HEAD_BYTES, 1024 * 1024);

    // One open scope of 64 buckets, over the old 16 KiB limit, is accepted.
    let wide: Vec<Spec> = (0..64).map(|b| (b, 0, 0, 100)).collect();
    let (mut head, chunks) = epoch_fixture(product);
    head.open_scopes[1] = scope(product, 35, ScopeState::Open, (epoch(35).0, 570), 64, &wide);
    let bytes = head.to_bytes().unwrap();
    assert!(bytes.len() > 16 * 1024);
    ChainMapHead::parse(&bytes)
        .unwrap()
        .check_listing(product, &chunks)
        .unwrap();

    // The spec's sizing case: two open scopes of 64 buckets x 8 segments fit.
    let full: Vec<Spec> = (0..64)
        .flat_map(|b| (0..8).map(move |k| (b, k, k, 1)))
        .collect();
    head.open_scopes[0] = scope(product, 34, ScopeState::Open, epoch(34), 64, &full);
    head.open_scopes[1] = scope(product, 35, ScopeState::Open, (epoch(35).0, 570), 64, &full);
    let bytes = head.to_bytes().unwrap();
    assert!(bytes.len() > 256 * 1024);
    ChainMapHead::parse(&bytes)
        .unwrap()
        .check_listing(product, &chunks)
        .unwrap();

    // Padded to exactly 1 MiB the head is accepted; one byte more is refused
    // by both the reader (before parsing) and the writer.
    let pad = |head: &mut ChainMapHead, total: usize| {
        let base = serde_json::to_vec(&*head).unwrap().len() - head.network.len();
        head.network = "x".repeat(total - base);
        assert_eq!(serde_json::to_vec(&*head).unwrap().len(), total);
    };
    pad(&mut head, 1024 * 1024);
    let bytes = head.to_bytes().unwrap();
    assert_eq!(ChainMapHead::parse(&bytes).unwrap(), head);
    pad(&mut head, 1024 * 1024 + 1);
    let bytes = serde_json::to_vec(&head).unwrap();
    assert!(matches!(head.to_bytes(), Err(MapError::TooLarge(n)) if n == 1024 * 1024 + 1));
    assert!(matches!(
        ChainMapHead::parse(&bytes),
        Err(MapError::TooLarge(_))
    ));

    let (mut registry, _) = registry_fixture();
    registry.network = "x".repeat(1024 * 1024);
    assert!(matches!(registry.to_bytes(), Err(MapError::TooLarge(_))));
    let bytes = serde_json::to_vec(&registry).unwrap();
    assert!(matches!(
        RegistryMapHead::parse(&bytes),
        Err(MapError::TooLarge(_))
    ));
}

#[test]
fn query_plan_covers_every_segment_of_the_bucket() {
    let product = Product::Memo;
    let g = genesis();
    let s = scope(
        product,
        4,
        ScopeState::Open,
        epoch(4),
        2,
        &[(0, 0, 0, 10), (0, 1, 4, 2), (1, 0, 0, 9)],
    );
    s.check_tables(product).unwrap();
    let mut seen = [false; 2];
    for i in 0u8..=255 {
        let key = [i.max(1); 16];
        let b = bucket(product, &g, 4, 0, 2, &key).unwrap();
        let plan = s.query_plan(product, &g, &key).unwrap();
        seen[b as usize] = true;
        let expected: Vec<(u32, u32)> = s
            .tables
            .iter()
            .filter(|t| t.bucket == b)
            .map(|t| (t.segment, t.seed))
            .collect();
        let got: Vec<(u32, u32)> = plan
            .iter()
            .map(|q| (q.table.segment, q.table.seed))
            .collect();
        assert_eq!(got, expected);
        for target in &plan {
            let row =
                tachyon_pir_protocol::row(product, &g, 4, target.table.seed, 4096, &key).unwrap();
            assert_eq!(target.row, row);
            assert_eq!(target.context.scope_seed, 0);
            assert_eq!(target.context.seed, target.table.seed);
            assert_eq!(target.context.bucket, b);
            assert_eq!(target.context.heights, Some(epoch(4)));
            // A row holding the key validates under the plan's context.
            let entry = Entry::Memo(MemoEntry {
                key,
                ciphertext: [i; 128],
            });
            let bytes = encode_row(product, std::slice::from_ref(&entry)).unwrap();
            assert_eq!(
                validate_row(product, &bytes, &target.context).unwrap(),
                vec![entry]
            );
        }
    }
    assert_eq!(seen, [true, true]);

    let empty = scope(product, 5, ScopeState::Open, epoch(5), 1, &[]);
    assert!(empty.query_plan(product, &g, &[1; 16]).unwrap().is_empty());
    assert!(s.query_plan(product, &g, &[1; 32]).is_err());
}
