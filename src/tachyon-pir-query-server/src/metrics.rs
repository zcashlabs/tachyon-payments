//! Product metrics, prefixed `tachyon_<product>_` (spec 11.8). Labels are
//! fixed sets; no client byte, session id or path ever becomes one.

use std::{
    fmt::Write as _,
    sync::atomic::{AtomicU64, Ordering},
};

/// How one query ended, as a fixed label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryOutcome {
    Answered,
    Invalid,
    TooLarge,
    Timeout,
    Stale,
    Revoked,
    Unavailable,
    Overloaded,
}

impl QueryOutcome {
    const ALL: [QueryOutcome; 8] = [
        QueryOutcome::Answered,
        QueryOutcome::Invalid,
        QueryOutcome::TooLarge,
        QueryOutcome::Timeout,
        QueryOutcome::Stale,
        QueryOutcome::Revoked,
        QueryOutcome::Unavailable,
        QueryOutcome::Overloaded,
    ];

    const fn label(self) -> &'static str {
        match self {
            QueryOutcome::Answered => "answered",
            QueryOutcome::Invalid => "invalid_request",
            QueryOutcome::TooLarge => "too_large",
            QueryOutcome::Timeout => "upload_timeout",
            QueryOutcome::Stale => "stale_routing",
            QueryOutcome::Revoked => "session_unavailable",
            QueryOutcome::Unavailable => "temporarily_unavailable",
            QueryOutcome::Overloaded => "overloaded",
        }
    }
}

/// How one synchronization pass ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassOutcome {
    Committed,
    Deferred,
    Failed,
}

/// Counters of one product. Gauges are read from the serving state at render
/// time ([`Gauges`]).
#[derive(Default)]
pub struct ProductMetrics {
    queries: [AtomicU64; 8],
    passes: [AtomicU64; 3],
    sessions_revoked: AtomicU64,
}

/// Point-in-time values of one product.
#[derive(Clone, Copy, Debug, Default)]
pub struct Gauges {
    pub ready: bool,
    pub anchor_height: Option<u32>,
    pub generation: Option<u64>,
    pub recovery_epoch: Option<u64>,
    pub floor_scope: Option<u64>,
    pub newest_scope: Option<u64>,
    pub entries_total: Option<u64>,
    pub open_scope_entries: Option<u64>,
    pub tables_served: u64,
    pub runtime_held_bytes: u64,
}

impl ProductMetrics {
    pub fn query(&self, outcome: QueryOutcome) {
        let index = QueryOutcome::ALL
            .iter()
            .position(|o| *o == outcome)
            .expect("every outcome is listed");
        self.queries[index].fetch_add(1, Ordering::Relaxed);
    }

    pub fn pass(&self, outcome: PassOutcome) {
        let index = match outcome {
            PassOutcome::Committed => 0,
            PassOutcome::Deferred => 1,
            PassOutcome::Failed => 2,
        };
        self.passes[index].fetch_add(1, Ordering::Relaxed);
    }

    pub fn sessions_revoked(&self, count: u64) {
        self.sessions_revoked.fetch_add(count, Ordering::Relaxed);
    }

    /// Prometheus text for this product.
    pub fn render(&self, prefix: &str, gauges: &Gauges) -> String {
        let mut out = String::new();
        let mut line = |name: &str, labels: &str, value: u64| {
            let _ = writeln!(out, "{prefix}{name}{labels} {value}");
        };
        line("ready", "", u64::from(gauges.ready));
        for (name, value) in [
            ("anchor_height", gauges.anchor_height.map(u64::from)),
            ("generation", gauges.generation),
            ("recovery_epoch", gauges.recovery_epoch),
            ("floor_scope", gauges.floor_scope),
            ("newest_scope", gauges.newest_scope),
            ("entries_total", gauges.entries_total),
            ("open_scope_entries", gauges.open_scope_entries),
        ] {
            if let Some(value) = value {
                line(name, "", value);
            }
        }
        line("tables_served", "", gauges.tables_served);
        line("runtime_held_bytes", "", gauges.runtime_held_bytes);
        line(
            "sessions_revoked_total",
            "",
            self.sessions_revoked.load(Ordering::Relaxed),
        );
        for (outcome, counter) in QueryOutcome::ALL.iter().zip(&self.queries) {
            line(
                "queries_total",
                &format!("{{result=\"{}\"}}", outcome.label()),
                counter.load(Ordering::Relaxed),
            );
        }
        for (label, counter) in ["committed", "deferred", "failed"].iter().zip(&self.passes) {
            line(
                "sync_passes_total",
                &format!("{{result=\"{label}\"}}"),
                counter.load(Ordering::Relaxed),
            );
        }
        out
    }
}
