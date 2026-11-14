//! Helpers shared by the integration tests. The served JSON is read with
//! `serde_json` only: no Tachyon decoder is involved.
#![allow(dead_code)]

use pir_node_rpc::{Credentials, HttpPolicy, RpcClient};
use serde_json::{Value, json};
use std::time::Duration;

/// A `pir-node-rpc` client for `url`, as the publisher builds one.
pub fn client(url: &str, credentials: Credentials) -> RpcClient {
    let policy = HttpPolicy {
        connection_timeout: Some(Duration::from_secs(5)),
        request_timeout: Duration::from_secs(10),
        response_limit: 64 * 1024 * 1024,
    };
    RpcClient::new(url.to_owned(), credentials, "fixtures-test", policy).unwrap()
}

/// `getblock <hash> 2`.
pub async fn block_v2(rpc: &RpcClient, hash: &str) -> Value {
    rpc.call("getblock", json!([hash, 2])).await.unwrap()
}

/// POSTs `body` to the control endpoint `action` and returns the state answered.
pub async fn control(url: &str, action: &str, body: Value) -> Value {
    let response = reqwest::Client::new()
        .post(format!("{url}/control/{action}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "control {action}: {}",
        response.status()
    );
    response.json().await.unwrap()
}

/// A saved testnet response from `tests/data`.
pub fn sample(name: &str) -> Value {
    let path = format!("{}/tests/data/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// Keys whose values are fixed-length hex in the node's output; ours must have
/// the same length.
const FIXED_HEX: &[&str] = &[
    "hash",
    "merkleroot",
    "blockcommitments",
    "finalsaplingroot",
    "finalorchardroot",
    "nonce",
    "solution",
    "bits",
    "previousblockhash",
    "nextblockhash",
    "txid",
    "authdigest",
    "blockhash",
    "versiongroupid",
    "cv",
    "rk",
    "spendAuthSig",
    "cvFeeMax",
    "bindingSig",
    "coverage",
    "anchor",
    "tachygramSet",
    "tachygrams",
    "aggregateId",
    "bestblockhash",
    "chaintip",
    "nextblock",
];

fn is_hex(text: &str) -> bool {
    text.len().is_multiple_of(2) && text.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// What distinguishes array elements of different kinds: a transaction with or
/// without a bundle, and a stamp's type.
fn kind(value: &Value) -> String {
    let stamp = value
        .pointer("/tachyon/stamp/type")
        .and_then(Value::as_str)
        .unwrap_or("-");
    format!("{}/{stamp}", value.get("tachyon").is_some())
}

/// Checks that `ours` has exactly the fields of `sample`, recursively, with the
/// same JSON types (integers and floats told apart) and the same fixed hex
/// lengths. Array elements are compared with the sample element of their kind.
/// Returns the differences found.
pub fn conformance(ours: &Value, sample: &Value) -> Vec<String> {
    let mut errors = Vec::new();
    compare("", "", ours, sample, &mut errors);
    errors
}

fn compare(path: &str, key: &str, ours: &Value, sample: &Value, errors: &mut Vec<String>) {
    match (ours, sample) {
        (Value::Object(a), Value::Object(b)) => {
            for name in b.keys().filter(|name| !a.contains_key(*name)) {
                errors.push(format!("{path}: missing {name}"));
            }
            for name in a.keys().filter(|name| !b.contains_key(*name)) {
                errors.push(format!("{path}: extra {name}"));
            }
            for (name, value) in a {
                if let Some(expected) = b.get(name) {
                    compare(&format!("{path}/{name}"), name, value, expected, errors);
                }
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            if b.is_empty() {
                if !a.is_empty() {
                    errors.push(format!("{path}: sample array is empty, ours is not"));
                }
                return;
            }
            for (index, value) in a.iter().enumerate() {
                let expected = b.iter().find(|candidate| kind(candidate) == kind(value));
                match expected {
                    Some(expected) => {
                        compare(&format!("{path}[{index}]"), key, value, expected, errors)
                    }
                    None => errors.push(format!(
                        "{path}[{index}]: no sample of kind {}",
                        kind(value)
                    )),
                }
            }
        }
        (Value::String(a), Value::String(b)) => {
            if is_hex(b) && !is_hex(a) && !(b.is_empty() && a.is_empty()) {
                errors.push(format!("{path}: {a:?} is not hex"));
            }
            if FIXED_HEX.contains(&key) && a.len() != b.len() {
                errors.push(format!(
                    "{path}: length {} where the node has {}",
                    a.len(),
                    b.len()
                ));
            }
        }
        (Value::Number(a), Value::Number(b)) => {
            if a.is_f64() != b.is_f64() {
                errors.push(format!("{path}: {a} and the node's {b} differ in kind"));
            }
        }
        (Value::Bool(_), Value::Bool(_)) | (Value::Null, Value::Null) => {}
        _ => errors.push(format!("{path}: {ours} where the node has {sample}")),
    }
}
