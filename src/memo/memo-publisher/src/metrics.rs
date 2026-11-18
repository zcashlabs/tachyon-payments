//! Product metrics, served as Prometheus text on the memo listener at
//! `GET /metrics` (spec 11.8): the publisher host's management listener has
//! no metrics route. One listener carries both products, so both series
//! families render here; the first-contact family only when it is enabled.
//!
//! | Metric (`tachyon_memo_` and `tachyon_first_contact_`) | Type | Meaning |
//! |---|---|---|
//! | `ingested_height` | gauge | newest journal block (one journal for both) |
//! | `anchor_height` | gauge | anchor of the product's current publication |
//! | `entries_total` | gauge | entries in every scope of the current publication |
//! | `open_scope_entries` | gauge | entries in its open scopes |
//! | `segments_overflow_total` | gauge | overflow segments (segment >= 1) in it |
//! | `publication_seconds` | histogram | build to commit of each committed publication |
//! | `sessions_revoked_total` | counter | sessions of the last publication in scopes a sealed-scope rewind reached |
//!
//! Memo adds, describing the journal (the canonical chain as ingested, so a
//! rewind lowers them and a restart keeps them):
//!
//! | Metric | Type | Meaning |
//! |---|---|---|
//! | `tachyon_memo_envelopes_invalid_total` | gauge | non-empty memos of the journal's blocks that are not valid envelopes (spec 5.2) |
//! | `tachyon_memo_state_entries_total` | gauge | wallet-state entries (kind `0x03`) of the journal's valid envelopes |
//!
//! Gauges are `-1`/`0` until the first value is known. HTTP metrics of the
//! listener come from `pir-observability`, with fixed endpoint labels.
use memo_store::Counts;
use pir_observability::{Distribution, HttpMetrics};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use tachyon_pir_protocol::Product;

/// Fixed endpoint labels of the listener's HTTP metrics.
pub const ENDPOINTS: [&str; 7] = [
    "memo_current",
    "memo_objects",
    "memo_authority_check",
    "first_contact_current",
    "first_contact_objects",
    "first_contact_authority_check",
    "metrics",
];

/// What a product's current publication holds, for the gauges.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublicationFigures {
    pub anchor_height: u32,
    pub entries: u64,
    pub open_scope_entries: u64,
    pub overflow_segments: u64,
}

/// One product's series.
struct ProductSeries {
    anchor_height: AtomicI64,
    entries: AtomicU64,
    open_scope_entries: AtomicU64,
    overflow_segments: AtomicU64,
    sessions_revoked: AtomicU64,
    publication_seconds: Distribution,
}

impl Default for ProductSeries {
    fn default() -> Self {
        Self {
            anchor_height: AtomicI64::new(-1),
            entries: AtomicU64::new(0),
            open_scope_entries: AtomicU64::new(0),
            overflow_segments: AtomicU64::new(0),
            sessions_revoked: AtomicU64::new(0),
            publication_seconds: Distribution::default(),
        }
    }
}

pub struct Metrics {
    ingested_height: AtomicI64,
    envelopes_invalid: AtomicU64,
    state_entries: AtomicU64,
    memo: ProductSeries,
    first_contact: ProductSeries,
    first_contact_enabled: AtomicBool,
    pub http: HttpMetrics,
}

impl Metrics {
    /// The metrics of a memo publisher, with the first-contact family when
    /// `first_contact` is published.
    pub fn new(first_contact: bool) -> Self {
        let http = HttpMetrics::default();
        let endpoints: Vec<&'static str> = ENDPOINTS
            .into_iter()
            .filter(|label| first_contact || !label.starts_with("first_contact_"))
            .collect();
        http.initialize(&endpoints);
        Self {
            ingested_height: AtomicI64::new(-1),
            envelopes_invalid: AtomicU64::new(0),
            state_entries: AtomicU64::new(0),
            memo: ProductSeries::default(),
            first_contact: ProductSeries::default(),
            first_contact_enabled: AtomicBool::new(first_contact),
            http,
        }
    }

    fn series(&self, product: Product) -> &ProductSeries {
        match product {
            Product::FirstContact => &self.first_contact,
            _ => &self.memo,
        }
    }

    /// The journal's tip and envelope counts after an append, a rewind or an
    /// open.
    pub fn journal(&self, height: Option<u32>, totals: &Counts) {
        self.ingested_height
            .store(height.map_or(-1, i64::from), Ordering::Relaxed);
        self.envelopes_invalid
            .store(totals.envelopes_invalid, Ordering::Relaxed);
        self.state_entries
            .store(totals.state_entries, Ordering::Relaxed);
    }

    pub fn published(&self, product: Product, figures: PublicationFigures, seconds: Option<f64>) {
        let series = self.series(product);
        series
            .anchor_height
            .store(i64::from(figures.anchor_height), Ordering::Relaxed);
        series.entries.store(figures.entries, Ordering::Relaxed);
        series
            .open_scope_entries
            .store(figures.open_scope_entries, Ordering::Relaxed);
        series
            .overflow_segments
            .store(figures.overflow_segments, Ordering::Relaxed);
        if let Some(seconds) = seconds {
            series.publication_seconds.observe(seconds);
        }
    }

    pub fn revoked(&self, product: Product, sessions: u64) {
        self.series(product)
            .sessions_revoked
            .fetch_add(sessions, Ordering::Relaxed);
    }

    /// Prometheus text exposition of the product metrics and the listener's
    /// HTTP metrics.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let mut series = |prefix: &str, name: &str, kind: &str, help: &str, value: String| {
            out.push_str(&format!(
                "# HELP {prefix}{name} {help}\n# TYPE {prefix}{name} {kind}\n{prefix}{name} {value}\n"
            ));
        };
        let ingested = self.ingested_height.load(Ordering::Relaxed).to_string();
        let mut products = vec![(Product::Memo, "tachyon_memo_", "Memo")];
        if self.first_contact_enabled.load(Ordering::Relaxed) {
            products.push((
                Product::FirstContact,
                "tachyon_first_contact_",
                "First-contact",
            ));
        }
        for (product, prefix, label) in &products {
            let product = self.series(*product);
            series(
                prefix,
                "ingested_height",
                "gauge",
                "Height of the newest block in the memo journal (-1 when empty).",
                ingested.clone(),
            );
            series(
                prefix,
                "anchor_height",
                "gauge",
                "Anchor height of the current publication (-1 before the first).",
                product.anchor_height.load(Ordering::Relaxed).to_string(),
            );
            series(
                prefix,
                "entries_total",
                "gauge",
                &format!("{label} entries in every scope of the current publication."),
                product.entries.load(Ordering::Relaxed).to_string(),
            );
            series(
                prefix,
                "open_scope_entries",
                "gauge",
                &format!("{label} entries in the open scopes of the current publication."),
                product
                    .open_scope_entries
                    .load(Ordering::Relaxed)
                    .to_string(),
            );
            series(
                prefix,
                "segments_overflow_total",
                "gauge",
                "Overflow segments (segment >= 1) in the current publication.",
                product
                    .overflow_segments
                    .load(Ordering::Relaxed)
                    .to_string(),
            );
            series(
                prefix,
                "sessions_revoked_total",
                "counter",
                "Sessions in scopes that a rewind into a sealed scope reached.",
                product.sessions_revoked.load(Ordering::Relaxed).to_string(),
            );
        }
        series(
            "tachyon_memo_",
            "envelopes_invalid_total",
            "gauge",
            "Non-empty memos in the journal's blocks that are not valid envelopes.",
            self.envelopes_invalid.load(Ordering::Relaxed).to_string(),
        );
        series(
            "tachyon_memo_",
            "state_entries_total",
            "gauge",
            "Wallet-state entries of the valid envelopes in the journal's blocks.",
            self.state_entries.load(Ordering::Relaxed).to_string(),
        );
        for (product, prefix, _) in &products {
            let name = format!("{prefix}publication_seconds");
            out.push_str(&format!(
                "# HELP {name} Seconds from build to commit of each committed publication.\n# TYPE {name} histogram\n"
            ));
            out.push_str(&self.series(*product).publication_seconds.render(&name));
        }
        out.push_str(&self.http.render());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_every_spec_metric_in_prometheus_text() {
        let metrics = Metrics::new(true);
        let text = metrics.render();
        assert!(text.contains("\ntachyon_memo_ingested_height -1\n"));
        assert!(text.contains("\ntachyon_first_contact_ingested_height -1\n"));
        metrics.journal(
            Some(42),
            &Counts {
                envelopes_invalid: 5,
                state_entries: 9,
                ..Counts::default()
            },
        );
        metrics.published(
            Product::Memo,
            PublicationFigures {
                anchor_height: 40,
                entries: 7,
                open_scope_entries: 3,
                overflow_segments: 1,
            },
            Some(0.75),
        );
        metrics.published(
            Product::FirstContact,
            PublicationFigures {
                anchor_height: 39,
                entries: 2,
                open_scope_entries: 2,
                overflow_segments: 0,
            },
            None,
        );
        metrics.revoked(Product::Memo, 2);
        metrics.revoked(Product::Memo, 1);
        metrics.revoked(Product::FirstContact, 4);
        let text = metrics.render();
        for line in [
            "tachyon_memo_ingested_height 42\n",
            "tachyon_memo_anchor_height 40\n",
            "tachyon_memo_entries_total 7\n",
            "tachyon_memo_open_scope_entries 3\n",
            "tachyon_memo_segments_overflow_total 1\n",
            "tachyon_memo_sessions_revoked_total 3\n",
            "tachyon_memo_envelopes_invalid_total 5\n",
            "tachyon_memo_state_entries_total 9\n",
            "# TYPE tachyon_memo_publication_seconds histogram\n",
            "tachyon_memo_publication_seconds_bucket{le=\"1\"} 1\n",
            "tachyon_memo_publication_seconds_count 1\n",
            "tachyon_first_contact_ingested_height 42\n",
            "tachyon_first_contact_anchor_height 39\n",
            "tachyon_first_contact_entries_total 2\n",
            "tachyon_first_contact_open_scope_entries 2\n",
            "tachyon_first_contact_segments_overflow_total 0\n",
            "tachyon_first_contact_sessions_revoked_total 4\n",
            "tachyon_first_contact_publication_seconds_count 0\n",
            "pir_http_arrivals_total{endpoint=\"memo_current\"} 0\n",
            "pir_http_arrivals_total{endpoint=\"first_contact_current\"} 0\n",
        ] {
            assert!(text.contains(line), "missing {line:?} in\n{text}");
        }
    }

    #[test]
    fn first_contact_series_render_only_when_enabled() {
        let text = Metrics::new(false).render();
        assert!(text.contains("tachyon_memo_entries_total 0\n"));
        assert!(!text.contains("tachyon_first_contact_"));
        assert!(!text.contains("first_contact_current"));
    }
}
