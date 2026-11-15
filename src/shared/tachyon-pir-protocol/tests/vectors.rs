//! Known-answer tests against `tests/vectors/vectors.json`, which
//! `tests/vectors/generate.py` computes independently with Python's hashlib.

use serde_json::Value;
use sha2::{Digest, Sha256};
use tachyon_pir_protocol::registry::{leading_zero_bits, solve_pow};
use tachyon_pir_protocol::{
    Address, BlockHash, Entry, FirstContactEntry, Geometry, MemoEntry, Product, RegisterRequest,
    RegistryEntry, RowContext, SeedPurpose, SessionParts, TachygramEntry, bucket, encode_row,
    lookup, n_buckets_for, placement_hash, pow_hash, pow_meets, row, seed, session_id,
    validate_row,
};

fn vectors() -> Value {
    serde_json::from_str(include_str!("vectors/vectors.json")).expect("vectors.json parses")
}

fn bytes(v: &Value) -> Vec<u8> {
    hex::decode(v.as_str().expect("hex string")).expect("hex")
}

fn array<const N: usize>(v: &Value) -> [u8; N] {
    bytes(v).try_into().expect("length")
}

fn int(v: &Value) -> u64 {
    v.as_u64().expect("integer")
}

fn product(v: &Value) -> Product {
    v.as_str().unwrap().parse().unwrap()
}

fn geometry(v: &Value) -> Geometry {
    v.as_str().unwrap().parse().unwrap()
}

fn genesis(v: &Value) -> BlockHash {
    let g = BlockHash::parse_display(v["genesis_display"].as_str().unwrap()).unwrap();
    assert_eq!(g.protocol_bytes().to_vec(), bytes(&v["genesis_protocol"]));
    g
}

#[test]
fn pir_seeds() {
    let v = vectors();
    let cases = v["seeds"].as_array().unwrap();
    assert_eq!(cases.len(), 16);
    for case in cases {
        let purpose = match case["purpose"].as_str().unwrap() {
            "query-masks" => SeedPurpose::QueryMasks,
            "packing-setup" => SeedPurpose::PackingSetup,
            other => panic!("purpose {other}"),
        };
        assert_eq!(
            seed(
                product(&case["product"]),
                geometry(&case["geometry"]),
                purpose
            ),
            array::<32>(&case["seed"]),
            "{case}"
        );
    }
}

#[test]
fn placement() {
    let v = vectors();
    let g = genesis(&v);
    for case in v["placement"].as_array().unwrap() {
        let p = product(&case["product"]);
        let scope_id = int(&case["scope_id"]);
        let seed = u32::try_from(int(&case["seed"])).unwrap();
        let key = bytes(&case["key"]);
        let n = u32::try_from(int(&case["n_buckets"])).unwrap();
        let rows = geometry(&case["geometry"]).rows();
        assert_eq!(
            placement_hash(p, &g, scope_id, seed, &key).unwrap(),
            array::<32>(&case["hash"]),
            "{case}"
        );
        assert_eq!(
            u64::from(bucket(p, &g, scope_id, seed, n, &key).unwrap()),
            int(&case["bucket"]),
            "{case}"
        );
        assert_eq!(
            u64::from(row(p, &g, scope_id, seed, rows, &key).unwrap()),
            int(&case["row"]),
            "{case}"
        );
    }
}

#[test]
fn n_buckets() {
    let v = vectors();
    for case in v["n_buckets"].as_array().unwrap() {
        let n = n_buckets_for(
            product(&case["product"]),
            geometry(&case["geometry"]),
            int(&case["entries"]),
        );
        assert_eq!(u64::from(n), int(&case["n_buckets"]), "{case}");
    }
}

#[test]
fn session_ids() {
    let v = vectors();
    for case in v["sessions"].as_array().unwrap() {
        let parts = SessionParts {
            scope_id: int(&case["scope_id"]),
            bucket: int(&case["bucket"]) as u32,
            segment: int(&case["segment"]) as u32,
            rows: int(&case["rows"]) as u32,
            seed: int(&case["seed"]) as u32,
            rows_sha256: array(&case["rows_sha256"]),
            public_sha256: array(&case["public_sha256"]),
        };
        assert_eq!(
            session_id(product(&case["product"]), &parts),
            array::<32>(&case["session_id"]),
            "{case}"
        );
    }
}

fn entry(p: Product, e: &Value) -> Entry {
    match p {
        Product::Memo => Entry::Memo(MemoEntry {
            key: array(&e["key"]),
            ciphertext: array(&e["ciphertext"]),
        }),
        Product::FirstContact => Entry::FirstContact(Box::new(FirstContactEntry {
            key: array(&e["key"]),
            kem_ciphertext: array(&e["kem_ciphertext"]),
            ciphertext: array(&e["ciphertext"]),
        })),
        Product::Tachygram => Entry::Tachygram(TachygramEntry {
            tachygram: array(&e["tachygram"]),
            blk_height: int(&e["blk_height"]) as u32,
            stamp_ordinal: int(&e["stamp_ordinal"]) as u32,
        }),
        Product::Registry => {
            let mut addr = bytes(&e["pk"]);
            addr.extend(bytes(&e["ek"]));
            let entry = RegistryEntry {
                key: array(&e["key"]),
                address: Address::from_bytes(&addr).unwrap(),
            };
            assert_eq!(entry, RegistryEntry::new(entry.address.clone()));
            Entry::Registry(Box::new(entry))
        }
    }
}

#[test]
fn canonical_rows() {
    let v = vectors();
    let g = genesis(&v);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    for case in rows {
        let p = product(&case["product"]);
        let entries: Vec<Entry> = case["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| entry(p, e))
            .collect();
        let encoded = encode_row(p, &entries).unwrap();
        assert_eq!(
            Sha256::digest(&encoded).to_vec(),
            bytes(&case["row_sha256"]),
            "{p}"
        );
        let heights = case["first_height"]
            .as_u64()
            .map(|first| (first as u32, int(&case["last_height"]) as u32));
        let context = RowContext {
            product: p,
            genesis: g,
            scope_id: int(&case["scope_id"]),
            n_buckets: int(&case["n_buckets"]) as u32,
            scope_seed: int(&case["scope_seed"]) as u32,
            bucket: int(&case["bucket"]) as u32,
            seed: int(&case["seed"]) as u32,
            rows: 4096,
            row: int(&case["row"]) as u32,
            heights,
        };
        let decoded = validate_row(p, &encoded, &context).unwrap();
        assert_eq!(decoded, entries, "{p}");
        for e in &entries {
            let hits = lookup(&decoded, e.key());
            assert!(hits.contains(&e));
        }
        if p == Product::Memo {
            // The vector repeats its second key: both entries, in row order.
            let hits = lookup(&decoded, entries[1].key());
            assert_eq!(hits, vec![&entries[1], &entries[2]]);
        }
        // The same bytes under a neighbouring row are not canonical.
        let shifted = RowContext {
            row: (context.row + 1) % 4096,
            ..context
        };
        assert!(validate_row(p, &encoded, &shifted).is_err(), "{p}");
    }
}

#[test]
fn registry_wire() {
    let v = vectors();
    let r = &v["registry"];
    let address = Address::from_bytes(&bytes(&r["addr"])).unwrap();
    assert_eq!(address.digest(), array::<32>(&r["digest"]));
    assert_eq!(address.key16(), array::<16>(&r["key16"]));
    let challenge = array::<32>(&r["challenge"]);
    let nonce = array::<8>(&r["nonce"]);
    let hash = pow_hash(&challenge, &nonce, &address.digest());
    assert_eq!(hash, array::<32>(&r["pow_hash"]));
    let zeros = int(&r["pow_leading_zero_bits"]) as u32;
    assert_eq!(leading_zero_bits(&hash), zeros);
    assert!(pow_meets(&hash, zeros));
    assert!(!pow_meets(&hash, zeros + 1));

    let difficulty = int(&r["difficulty_bits"]) as u32;
    let solved = solve_pow(&challenge, &address.digest(), difficulty).unwrap();
    assert_eq!(solved, array::<8>(&r["solved_nonce"]));
    assert_eq!(
        pow_hash(&challenge, &solved, &address.digest()),
        array::<32>(&r["solved_hash"])
    );

    let request = RegisterRequest {
        challenge,
        nonce,
        address: address.clone(),
    };
    let body = request.encode();
    assert_eq!(Sha256::digest(&body).to_vec(), bytes(&r["body_sha256"]));
    assert_eq!(RegisterRequest::parse(&body).unwrap(), request);
    assert_eq!(request.pow_hash(), hash);
}
