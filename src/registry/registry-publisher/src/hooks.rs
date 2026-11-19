//! What the generic distribution asks of the registry (`PublisherHooks`):
//! whether a subject is still authoritative, and the map head re-issued under
//! a new generation and recovery epoch.
use crate::core::PRODUCT;
use registry_store::Store;
use std::sync::Arc;
use tachyon_pir_distribution::{
    Error, Subject,
    publisher::{BoxFuture, PublisherHooks},
};
use tachyon_pir_protocol::{RecoveryEpoch, RegistryMapHead};

pub(crate) struct RegistryHooks {
    pub store: Arc<Store>,
}

impl PublisherHooks for RegistryHooks {
    /// A registry subject `{log_sequence}` stays authoritative while the log
    /// still begins with that many entries. The log is append-only and never
    /// rewound, so that is while it holds at least `log_sequence` entries.
    fn check_subjects(&self, subjects: Vec<Subject>) -> BoxFuture<Result<Vec<bool>, Error>> {
        let store = self.store.clone();
        Box::pin(async move {
            let stored = tokio::task::spawn_blocking(move || store.log_sequence()).await??;
            subjects
                .iter()
                .map(|subject| match subject {
                    Subject::Registry { log_sequence } => Ok(*log_sequence <= stored),
                    Subject::Chain { .. } => Err("registry subjects name a log sequence".into()),
                })
                .collect()
        })
    }

    fn reissue_head(
        &self,
        head: &[u8],
        generation: u64,
        recovery_epoch: u64,
    ) -> Result<Vec<u8>, Error> {
        let mut head = RegistryMapHead::parse(head)?;
        if head.protocol != PRODUCT.protocol() {
            return Err("not a registry map head".into());
        }
        head.generation = generation;
        head.recovery_epoch =
            RecoveryEpoch::new(recovery_epoch).ok_or("recovery epoch must be positive")?;
        Ok(head.to_bytes()?)
    }
}
