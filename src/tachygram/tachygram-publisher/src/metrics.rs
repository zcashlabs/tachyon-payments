//! Product metrics, served as Prometheus text on the product listener at
//! `GET /metrics` (spec 11.8): the publisher host's management listener has
//! no metrics route.
//!
//! | Metric | Type | Meaning |
//! |---|---|---|
//! | `tachyon_tachygram_ingested_height` | gauge | newest journal block |
//! | `tachyon_tachygram_anchor_height` | gauge | anchor of the current publication |
//! | `tachyon_tachygram_entries_total` | gauge | entries in every scope of the current publication |
//! | `tachyon_tachygram_open_scope_entries` | gauge | entries in its open scopes |
//! | `tachyon_tachygram_segments_overflow_total` | gauge | overflow segments (segment >= 1) in it |
//! | `tachyon_tachygram_publication_seconds` | histogram | build to commit of each committed publication |
//! | `tachyon_tachygram_sessions_revoked_total` | counter | sessions of the last publication in scopes a sealed-scope rewind reached |
//!
//! Gauges are `-1`/`0` until the first value is known. HTTP metrics of the
//! listener come from `pir-observability`, with fixed endpoint labels.
use pir_observability::{Distribution, HttpMetrics};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

const PREFIX: &str = "tachyon_tachygram_";

/// Fixed endpoint labels of the product listener's HTTP metrics.
pub const ENDPOINTS: [&str; 4] = ["current", "objects", "authority_check", "metrics"];

pub struct Metrics {
    ingested_height: AtomicI64,
    anchor_height: AtomicI64,
    entries: AtomicU64,
    open_scope_entries: AtomicU64,
    overflow_segments: AtomicU64,
    sessions_revoked: AtomicU64,
    publication_seconds: Distribution,
    pub http: HttpMetrics,
}

impl Default for Metrics {
    fn default() -> Self {
        let http = HttpMetrics::default();
        http.initialize(&ENDPOINTS);
        Self {
            ingested_height: AtomicI64::new(-1),
            anchor_height: AtomicI64::new(-1),
            entries: AtomicU64::new(0),
            open_scope_entries: AtomicU64::new(0),
            overflow_segments: AtomicU64::new(0),
            sessions_revoked: AtomicU64::new(0),
            publication_seconds: Distribution::default(),
            http,
        }
    }
}

/// What the current publication holds, for the gauges.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublicationFigures {
    pub anchor_height: u32,
    pub entries: u64,
    pub open_scope_entries: u64,
    pub overflow_segments: u64,
}

impl Metrics {
    pub fn ingested(&self, height: Option<u32>) {
        self.ingested_height
            .store(height.map_or(-1, i64::from), Ordering::Relaxed);
    }

    pub fn published(&self, figures: PublicationFigures, seconds: Option<f64>) {
        self.anchor_height
            .store(i64::from(figures.anchor_height), Ordering::Relaxed);
        self.entries.store(figures.entries, Ordering::Relaxed);
        self.open_scope_entries
            .store(figures.open_scope_entries, Ordering::Relaxed);
        self.overflow_segments
            .store(figures.overflow_segments, Ordering::Relaxed);
        if let Some(seconds) = seconds {
            self.publication_seconds.observe(seconds);
        }
    }

    pub fn revoked(&self, sessions: u64) {
        self.sessions_revoked.fetch_add(sessions, Ordering::Relaxed);
    }

    /// Prometheus text exposition of the product metrics and the listener's
    /// HTTP metrics.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let mut series = |name: &str, kind: &str, help: &str, value: String| {
            out.push_str(&format!(
                "# HELP {PREFIX}{name} {help}\n# TYPE {PREFIX}{name} {kind}\n{PREFIX}{name} {value}\n"
            ));
        };
        series(
            "ingested_height",
            "gauge",
            "Height of the newest block in the journal (-1 when empty).",
            self.ingested_height.load(Ordering::Relaxed).to_string(),
        );
        series(
            "anchor_height",
            "gauge",
            "Anchor height of the current publication (-1 before the first).",
            self.anchor_height.load(Ordering::Relaxed).to_string(),
        );
        series(
            "entries_total",
            "gauge",
            "Tachygram entries in every scope of the current publication.",
            self.entries.load(Ordering::Relaxed).to_string(),
        );
        series(
            "open_scope_entries",
            "gauge",
            "Tachygram entries in the open scopes of the current publication.",
            self.open_scope_entries.load(Ordering::Relaxed).to_string(),
        );
        series(
            "segments_overflow_total",
            "gauge",
            "Overflow segments (segment >= 1) in the current publication.",
            self.overflow_segments.load(Ordering::Relaxed).to_string(),
        );
        series(
            "sessions_revoked_total",
            "counter",
            "Sessions in scopes that a rewind into a sealed scope reached.",
            self.sessions_revoked.load(Ordering::Relaxed).to_string(),
        );
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
    fn renders_every_spec_metric_in_prometheus_text() {
        let metrics = Metrics::default();
        let text = metrics.render();
        assert!(text.contains("\ntachyon_tachygram_ingested_height -1\n"));
        metrics.ingested(Some(42));
        metrics.published(
            PublicationFigures {
                anchor_height: 40,
                entries: 7,
                open_scope_entries: 3,
                overflow_segments: 1,
            },
            Some(0.75),
        );
        metrics.revoked(2);
        metrics.revoked(1);
        let text = metrics.render();
        for line in [
            "tachyon_tachygram_ingested_height 42\n",
            "tachyon_tachygram_anchor_height 40\n",
            "tachyon_tachygram_entries_total 7\n",
            "tachyon_tachygram_open_scope_entries 3\n",
            "tachyon_tachygram_segments_overflow_total 1\n",
            "tachyon_tachygram_sessions_revoked_total 3\n",
            "# TYPE tachyon_tachygram_publication_seconds histogram\n",
            "tachyon_tachygram_publication_seconds_bucket{le=\"1\"} 1\n",
            "tachyon_tachygram_publication_seconds_count 1\n",
            "pir_http_arrivals_total{endpoint=\"current\"} 0\n",
        ] {
            assert!(text.contains(line), "missing {line:?} in\n{text}");
        }
    }
}
