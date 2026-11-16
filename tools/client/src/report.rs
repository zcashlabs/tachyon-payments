//! JSON renderings of maps, outcomes, entries and errors for the binary.

use serde_json::{Value, json};
use tachyon_pir_protocol::Entry;

use crate::client::{AnchorAccepted, Answer, LookupOutcome};
use crate::error::ClientError;
use crate::map_view::{MapHead, MapView, ViewScope};

/// One entry, with byte fields in hex.
pub fn entry_json(entry: &Entry) -> Value {
    match entry {
        Entry::Memo(e) => json!({
            "key": hex::encode(e.key),
            "ciphertext": hex::encode(e.ciphertext),
        }),
        Entry::FirstContact(e) => json!({
            "key": hex::encode(e.key),
            "kem_ciphertext": hex::encode(e.kem_ciphertext),
            "ciphertext": hex::encode(e.ciphertext),
        }),
        Entry::Tachygram(e) => json!({
            "tachygram": hex::encode(e.tachygram),
            "blk_height": e.blk_height,
            "stamp_ordinal": e.stamp_ordinal,
        }),
        Entry::Registry(e) => json!({
            "key": hex::encode(e.key),
            "digest": hex::encode(e.address.digest()),
            "pk": hex::encode(e.address.pk),
            "ek": hex::encode(e.address.ek),
        }),
    }
}

fn scope_json(index: u64, scope: &ViewScope) -> Value {
    let tables = scope.tables();
    json!({
        "index": index,
        "scope_id": tables.scope_id(),
        "coverage": scope.coverage(),
        "n_buckets": tables.n_buckets(),
        "tables": tables.tables().iter().map(|t| json!({
            "bucket": t.bucket,
            "segment": t.segment,
            "geometry": t.geometry.name(),
            "seed": t.seed,
            "entries": t.entries,
            "session_id": t.session_id,
        })).collect::<Vec<_>>(),
    })
}

/// A map summary: head fields and every scope with its tables.
pub fn map_json(map: &MapView) -> Value {
    let head = map.head();
    let mut out = json!({
        "product": map.product().name(),
        "map_sha256": map.map_sha256(),
        "network": head.network(),
        "genesis_hash": head.genesis_hash().display_hex(),
        "generation": head.generation(),
        "recovery_epoch": head.recovery_epoch().to_string(),
        "floor_scope": head.floor_scope(),
        "sealed_chunks": head.sealed_chunks().len(),
        "scopes": map.scopes().iter().enumerate()
            .map(|(i, s)| scope_json(i as u64, s)).collect::<Vec<_>>(),
    });
    match head {
        MapHead::Chain(h) => {
            out["activation_height"] = json!(h.activation_height);
            out["epoch_length"] = json!(h.epoch_length);
            out["anchor_height"] = json!(h.anchor_height);
            out["anchor_hash"] = json!(h.anchor_hash.display_hex());
        }
        MapHead::Registry(h) => {
            out["log_sequence"] = json!(h.log_sequence);
        }
    }
    out
}

/// A lookup or dummy answer's provenance.
pub fn answer_json(answer: &Answer) -> Value {
    serde_json::to_value(answer).expect("answer serializes")
}

pub fn outcome_json(outcome: &LookupOutcome) -> Value {
    match outcome {
        LookupOutcome::Found { entries, answer } => json!({
            "result": "found",
            "entries": entries.iter().map(entry_json).collect::<Vec<_>>(),
            "answer": answer_json(answer),
        }),
        LookupOutcome::Absent { answer } => json!({
            "result": "absent",
            "answer": answer_json(answer),
        }),
        LookupOutcome::BelowFloor {
            scope_index,
            floor_scope,
        } => json!({
            "result": "below_floor",
            "scope_index": scope_index,
            "floor_scope": floor_scope,
        }),
    }
}

pub fn anchor_json(anchor: &AnchorAccepted) -> Value {
    serde_json::to_value(anchor).expect("anchor serializes")
}

pub fn error_json(error: &ClientError) -> Value {
    let mut out = json!({
        "kind": error.kind(),
        "message": error.to_string(),
    });
    if let Some(retry) = error.retry_after() {
        out["retry_after_seconds"] = json!(retry.as_secs());
    }
    if let ClientError::StaleRouting { map_sha256, .. } = error {
        out["map_sha256"] = json!(map_sha256);
    }
    json!({ "error": out })
}
