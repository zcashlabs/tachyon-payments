//! One product's synchronization: `tachyon_pir_distribution::follow` over
//! `HttpsSource`, driven by `SyncManager`, wrapped to count passes and
//! revocations for the product's metrics.
//!
//! Service discovery runs in every pass (`EnvelopeConsumer::settle`), and a
//! confirmed major-version mismatch is the engine's only terminal error, so
//! `SyncManager::run_until` returns it and the host exits (spec 11.6).

use crate::{
    Error, config::ProductConfig, metrics::PassOutcome, prepare::HostPreparer,
    serving::ProductState,
};
use pir_pipeline::consumer::{ConsumerPass, polling::SyncTask};
use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tachyon_pir_distribution::{
    Engine, FollowerConfig, Publication, ServingReport, SessionId, follow,
};
use tachyon_pir_table::TableRuntime;

/// The follower configuration of one product.
pub fn follower_config(config: &ProductConfig) -> Result<FollowerConfig, Error> {
    let mut follower = FollowerConfig::new(
        config.module.distribution(),
        config.binding(),
        config.publisher.clone(),
        config.state_dir.clone(),
    );
    if let Some(path) = &config.publisher_ca {
        follower.publisher_ca = Some(
            std::fs::read(path)
                .map_err(|error| format!("publisher_ca {}: {error}", path.display()))?,
        );
    }
    follower.retain_scopes = config.retain_scopes;
    Ok(follower)
}

/// The product's engine with pass accounting.
pub struct Tracked {
    engine: Engine<HostPreparer>,
    state: Arc<ProductState>,
    last: Option<Arc<Publication<TableRuntime>>>,
}

/// Open the product's follower: its state directory lock, floor, object
/// cache and HTTPS source.
pub fn open(
    config: &ProductConfig,
    state: Arc<ProductState>,
    pool: Arc<rayon::ThreadPool>,
) -> Result<Tracked, Error> {
    let preparer = HostPreparer::new(config, pool, state.maps.clone());
    let engine = follow(
        follower_config(config)?,
        preparer,
        state.publications.clone(),
    )?;
    Ok(Tracked {
        engine,
        state,
        last: None,
    })
}

impl Tracked {
    /// After a commit: count the sessions a recovery revoked (those of the
    /// previous publication absent from one of a later recovery epoch).
    fn installed(&mut self) {
        let Some(current) = self.state.publications.current() else {
            return;
        };
        if let Some(previous) = &self.last
            && current.recovery_epoch() > previous.recovery_epoch()
        {
            let kept: BTreeSet<SessionId> =
                current.tables().map(|t| t.objects.session_id).collect();
            let revoked = previous
                .tables()
                .filter(|t| !kept.contains(&t.objects.session_id))
                .count() as u64;
            self.state.metrics.sessions_revoked(revoked);
        }
        self.last = Some(current);
    }
}

impl SyncTask for Tracked {
    type Report = ConsumerPass<Option<ServingReport>>;
    type Error = Error;

    async fn synchronize(&mut self) -> Result<Self::Report, Self::Error> {
        let result = self.engine.synchronize().await;
        match &result {
            Ok(ConsumerPass::Committed { .. }) => {
                self.state.metrics.pass(PassOutcome::Committed);
                self.installed();
            }
            Ok(ConsumerPass::Deferred { .. }) => self.state.metrics.pass(PassOutcome::Deferred),
            Err(_) => self.state.metrics.pass(PassOutcome::Failed),
        }
        result
    }

    fn started(&mut self) {
        self.engine.started();
    }

    fn finished(
        &mut self,
        result: &Result<Self::Report, Self::Error>,
        completed: SystemTime,
        elapsed: Duration,
    ) {
        self.engine.finished(result, completed, elapsed);
    }

    fn report(&self, result: &Result<Self::Report, Self::Error>) {
        self.engine.report(result);
    }

    fn terminal(&self, error: &Self::Error) -> bool {
        self.engine.terminal(error)
    }

    fn transient(&self, error: &Self::Error) -> bool {
        // The distribution `Engine` also retries a pass that lost a race with
        // a withdrawal at once (`tachyon_pir_distribution::lost_race`, spec 11.6).
        self.engine.transient(error)
    }

    async fn wait_until(&mut self, deadline: tokio::time::Instant) {
        self.engine.wait_until(deadline).await
    }
}

#[cfg(all(test, feature = "tachygram"))]
mod tests {
    use super::*;
    use pir_sync_manager::publication::PublicationLifecycleError as Lifecycle;

    /// The host's engine retries a pass that lost a race with a withdrawal at
    /// once, and backs off from anything else (spec 11.6).
    #[test]
    fn the_host_engine_retries_lost_races_without_back_off() {
        let dir = tempfile::tempdir().unwrap();
        let config = serde_json::json!({
            "management_listen": "127.0.0.1:9500",
            "public_listen": "127.0.0.1:9510",
            "tachygram": {"enabled": true, "settings": {
                "publisher": "https://publisher.invalid/",
                "state_dir": dir.path().join("state"),
                "role": "combined",
                "publisher_network": "test",
                "publisher_genesis": "ab".repeat(32),
                "activation_height": 8,
                "epoch_length": 16,
            }},
        });
        let validated = crate::Config::parse(config.to_string().as_bytes())
            .unwrap()
            .validate()
            .unwrap();
        let product = &validated.products[0];
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        );
        let tracked = open(product, ProductState::new(product), pool).unwrap();
        for retried in [
            Lifecycle::WithdrawnBeforeCommit,
            Lifecycle::CandidateInvalidated,
            Lifecycle::CommitFenced,
        ] {
            assert!(tracked.transient(&(Box::new(retried) as Error)));
        }
        for failed in [
            Lifecycle::MalformedEnvelope,
            Lifecycle::AuthorityResponseMismatch,
        ] {
            assert!(!tracked.transient(&(Box::new(failed) as Error)));
        }
        assert!(!tracked.transient(&Error::from("table refused: row 3")));
    }
}
