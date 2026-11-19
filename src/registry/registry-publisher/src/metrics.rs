//! Product metrics, served as Prometheus text on the product listener at
//! `GET /metrics` (spec 11.8): the publisher host's management listener has
//! no metrics route.
//!
//! | Metric | Type | Meaning |
//! |---|---|---|
//! | `tachyon_registry_entries_total` | gauge | entries in every chunk of the current publication (its `log_sequence`) |
//! | `tachyon_registry_open_scope_entries` | gauge | entries in its open chunk |
//! | `tachyon_registry_segments_overflow_total` | gauge | overflow segments (segment >= 1) in it |
//! | `tachyon_registry_publication_seconds` | histogram | build to commit of each committed publication |
//! | `tachyon_registry_sessions_revoked_total` | counter | sessions revoked by a sealed-scope rewind; the registry has none, so it stays 0 |
//! | `tachyon_registry_registrations_total{result}` | counter | registration requests by outcome, fixed labels [`RESULTS`] |
//! | `tachyon_registry_pow_rejected_total` | counter | registrations refused for their proof of work |
//!
//! HTTP metrics of the listener come from `pir-observability`, with fixed
//! endpoint labels. No label is ever derived from client bytes.
use pir_observability::{Distribution, HttpMetrics};
use std::sync::atomic::{AtomicU64, Ordering};

const PREFIX: &str = "tachyon_registry_";

/// Fixed endpoint labels of the product listener's HTTP metrics.
pub const ENDPOINTS: [&str; 6] = [
    "current",
    "objects",
    "authority_check",
    "metrics",
    "challenge",
    "register",
];

/// The outcome of one registration request: the `result` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// `200`, a new log entry.
    Created,
    /// `200`, the address was already registered.
    Existing,
    /// `400`: wrong length, forged or expired challenge, `pk`, `ek` or
    /// `key16` refused.
    Invalid,
    /// `400`: the proof of work does not meet the difficulty.
    PowRejected,
    /// `413`: body over 1,256 bytes.
    TooLarge,
    /// `408`: body not received within `upload_ms`.
    UploadTimeout,
    /// `429`: the client's rate or the global cap.
    RateLimited,
    /// `429`: the client already has `per_client_concurrent` in flight.
    Overloaded,
    /// `503`: the log could not be written.
    Unavailable,
}

/// Every `result` label value, in [`Outcome`] order.
pub const RESULTS: [&str; 9] = [
    "created",
    "existing",
    "invalid",
    "pow_rejected",
    "too_large",
    "upload_timeout",
    "rate_limited",
    "overloaded",
    "unavailable",
];

impl Outcome {
    pub const fn label(self) -> &'static str {
        RESULTS[self as usize]
    }
}

pub struct Metrics {
    entries: AtomicU64,
    open_scope_entries: AtomicU64,
    overflow_segments: AtomicU64,
    sessions_revoked: AtomicU64,
    publication_seconds: Distribution,
    registrations: [AtomicU64; RESULTS.len()],
    pow_rejected: AtomicU64,
    pub http: HttpMetrics,
}

impl Default for Metrics {
    fn default() -> Self {
        let http = HttpMetrics::default();
        http.initialize(&ENDPOINTS);
        Self {
            entries: AtomicU64::new(0),
            open_scope_entries: AtomicU64::new(0),
            overflow_segments: AtomicU64::new(0),
            sessions_revoked: AtomicU64::new(0),
            publication_seconds: Distribution::default(),
            registrations: Default::default(),
            pow_rejected: AtomicU64::new(0),
            http,
        }
    }
}

/// What the current publication holds, for the gauges.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublicationFigures {
    pub entries: u64,
    pub open_scope_entries: u64,
    pub overflow_segments: u64,
}

impl Metrics {
    pub fn published(&self, figures: PublicationFigures, seconds: Option<f64>) {
        self.entries.store(figures.entries, Ordering::Relaxed);
        self.open_scope_entries
            .store(figures.open_scope_entries, Ordering::Relaxed);
        self.overflow_segments
            .store(figures.overflow_segments, Ordering::Relaxed);
        if let Some(seconds) = seconds {
            self.publication_seconds.observe(seconds);
        }
    }

    /// Count one registration request's outcome.
    pub fn registration(&self, outcome: Outcome) {
        self.registrations[outcome as usize].fetch_add(1, Ordering::Relaxed);
        if outcome == Outcome::PowRejected {
            self.pow_rejected.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Requests counted with `outcome` so far.
    pub fn registrations(&self, outcome: Outcome) -> u64 {
        self.registrations[outcome as usize].load(Ordering::Relaxed)
    }

    /// Prometheus text exposition of the product metrics and the listener's
    /// HTTP metrics.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let mut series = |name: &str, kind: &str, help: &str, value: u64| {
            out.push_str(&format!(
                "# HELP {PREFIX}{name} {help}\n# TYPE {PREFIX}{name} {kind}\n{PREFIX}{name} {value}\n"
            ));
        };
        series(
            "entries_total",
            "gauge",
            "Registry entries in every chunk of the current publication.",
            self.entries.load(Ordering::Relaxed),
        );
        series(
            "open_scope_entries",
            "gauge",
            "Registry entries in the open chunk of the current publication.",
            self.open_scope_entries.load(Ordering::Relaxed),
        );
        series(
            "segments_overflow_total",
            "gauge",
            "Overflow segments (segment >= 1) in the current publication.",
            self.overflow_segments.load(Ordering::Relaxed),
        );
        series(
            "sessions_revoked_total",
            "counter",
            "Sessions in scopes that a rewind into a sealed scope reached (the registry has none).",
            self.sessions_revoked.load(Ordering::Relaxed),
        );
        series(
            "pow_rejected_total",
            "counter",
            "Registrations refused because their proof of work missed the difficulty.",
            self.pow_rejected.load(Ordering::Relaxed),
        );
        let name = format!("{PREFIX}registrations_total");
        out.push_str(&format!(
            "# HELP {name} Registration requests by outcome.\n# TYPE {name} counter\n"
        ));
        for (label, count) in RESULTS.iter().zip(&self.registrations) {
            out.push_str(&format!(
                "{name}{{result=\"{label}\"}} {}\n",
                count.load(Ordering::Relaxed)
            ));
        }
        let name = format!("{PREFIX}publication_seconds");
        out.push_str(&format!(
            "# HELP {name} Seconds from build to commit of each committed publication.\n# TYPE {name} histogram\n"
        ));
        out.push_str(&self.publication_seconds.render(&name));
        out.push_str(&self.http.render());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_every_spec_metric_with_fixed_labels() {
        let metrics = Metrics::default();
        let text = metrics.render();
        for label in RESULTS {
            assert!(text.contains(&format!(
                "tachyon_registry_registrations_total{{result=\"{label}\"}} 0\n"
            )));
        }
        metrics.published(
            PublicationFigures {
                entries: 3080,
                open_scope_entries: 8,
                overflow_segments: 1,
            },
            Some(0.5),
        );
        metrics.registration(Outcome::Created);
        metrics.registration(Outcome::Created);
        metrics.registration(Outcome::PowRejected);
        metrics.registration(Outcome::RateLimited);
        let text = metrics.render();
        for line in [
            "tachyon_registry_entries_total 3080\n",
            "tachyon_registry_open_scope_entries 8\n",
            "tachyon_registry_segments_overflow_total 1\n",
            "tachyon_registry_sessions_revoked_total 0\n",
            "tachyon_registry_pow_rejected_total 1\n",
            "tachyon_registry_registrations_total{result=\"created\"} 2\n",
            "tachyon_registry_registrations_total{result=\"pow_rejected\"} 1\n",
            "tachyon_registry_registrations_total{result=\"rate_limited\"} 1\n",
            "tachyon_registry_registrations_total{result=\"existing\"} 0\n",
            "# TYPE tachyon_registry_publication_seconds histogram\n",
            "tachyon_registry_publication_seconds_count 1\n",
            "pir_http_arrivals_total{endpoint=\"register\"} 0\n",
            "pir_http_arrivals_total{endpoint=\"challenge\"} 0\n",
        ] {
            assert!(text.contains(line), "missing {line:?} in\n{text}");
        }
        assert_eq!(Outcome::Unavailable.label(), "unavailable");
        assert_eq!(Outcome::Created.label(), "created");
    }
}
