//! The tachygram journal's writer, driven by the root chain follower through
//! `pir_pipeline::ingestion::Ingestion` (spec 11.2).
//!
//! `pir_pipeline` carries block hashes as display-order strings and heights
//! as `u64`; they become `tachyon_chain` protocol-order hashes and `u32`
//! heights here, at this boundary (spec 3, 11.2).
use crate::{
    core::{Core, Loaded, OfferKey},
    error::{Error, Result},
    publication::{Work, snapshot},
};
use futures_util::{StreamExt, TryStreamExt};
use pir_pipeline::{
    FailurePolicy,
    ingestion::{BlockId as PipelineBlock, ChainObservation, Ingestion},
};
use std::sync::Arc;
use tachygram_store::{IndexedBlock, Store};
use tachyon_chain::{BlockId, CONFIRMATION_DEPTH, Hash, NodeError};
use tokio::sync::watch;

/// Blocks fetched concurrently by one `ingest` call; they are appended in
/// height order.
const FETCH_CONCURRENCY: usize = 8;

/// The display-order form `pir_pipeline` uses.
pub(crate) fn to_pipeline(id: &BlockId) -> PipelineBlock {
    PipelineBlock {
        height: u64::from(id.height),
        hash: id.hash.display_hex(),
    }
}

/// The protocol-order form of a `pir_pipeline` block.
pub(crate) fn from_pipeline(id: &PipelineBlock) -> Result<BlockId> {
    Ok(BlockId {
        height: u32::try_from(id.height)
            .map_err(|_| Error::other(format!("height {} is beyond u32", id.height)))?,
        hash: Hash::parse_display(&id.hash).map_err(Error::other)?,
    })
}

pub struct Writer {
    pub(crate) core: Arc<Core>,
    pub(crate) store: Store,
    pub(crate) slot: watch::Sender<Option<Arc<Work>>>,
    /// The previous process's publication has not been offered again yet.
    pub(crate) restore: bool,
}

impl Writer {
    /// The journal.
    pub fn store(&self) -> &Store {
        &self.store
    }

    async fn check_identity(core: Arc<Core>) -> Result<()> {
        let identity = core.node.network_identity().await.map_err(identity_error)?;
        if identity != core.identity {
            core.distribution.withdraw()?;
            return Err(Error::Network(format!(
                "the node reports {identity:?}, the journal is bound to {:?}",
                core.identity
            )));
        }
        Ok(())
    }

    /// The decision of spec 11.4, durable before the journal rewinds:
    /// `Recover` if the rewind reaches a sealed scope of the last publication,
    /// else `Withdraw` if that publication's anchor is above the kept block.
    fn withdraw_for(&self, keep: Option<&BlockId>) -> Result<()> {
        let removed_from = keep.map_or(0, |keep| keep.height.saturating_add(1));
        let last = self.core.distribution.current_inventory()?;
        let reaches_sealed = last.as_ref().and_then(|inventory| {
            let tachyon_pir_distribution::Subject::Chain { anchor, .. } = &inventory.subject else {
                return None;
            };
            let anchor = u32::try_from(anchor.height).ok()?;
            let newest_sealed = (0..=self.core.schedule.epoch_of(anchor)?)
                .take_while(|epoch| self.core.sealed(*epoch, anchor))
                .last()?;
            let sealed_last = self.core.schedule.last_height(newest_sealed)?;
            (removed_from <= sealed_last).then_some(inventory)
        });
        if let Some(inventory) = reaches_sealed {
            let first_epoch = u64::from(self.core.schedule.epoch_of(removed_from).unwrap_or(0));
            let revoked = inventory
                .tables
                .iter()
                .filter(|table| table.scope_index >= first_epoch)
                .count() as u64;
            self.core.distribution.recover()?;
            self.core.metrics.revoked(revoked);
            tracing::warn!(
                removed_from,
                revoked,
                "tachygram rewind reaches a sealed scope; recovery epoch advanced"
            );
        } else if self
            .core
            .distribution
            .withdraw_above(keep.map(|keep| u64::from(keep.height)))?
        {
            tracing::info!(removed_from, "tachygram publication withdrawn for a rewind");
        }
        Ok(())
    }

    fn offer(&mut self, anchor: BlockId) -> Result<()> {
        let current = self.core.distribution.current();
        let key = OfferKey {
            anchor,
            fence: current.fence,
            rewinds: self.core.rewinds(),
        };
        {
            let offer = self.core.offer.lock().unwrap();
            if offer.key == Some(key) && !offer.failed {
                return Ok(());
            }
        }
        // A publication this process made or restored at this anchor serves.
        if current.eligible
            && *self.core.served.lock().unwrap() == Some(crate::core::block_ref(&anchor))
        {
            *self.core.offer.lock().unwrap() = crate::core::Offer {
                key: Some(key),
                failed: false,
            };
            return Ok(());
        }
        let work = snapshot(&self.core, &self.store, key)?;
        *self.core.offer.lock().unwrap() = crate::core::Offer {
            key: Some(key),
            failed: false,
        };
        self.slot.send_replace(Some(Arc::new(work)));
        Ok(())
    }

    async fn restore(&mut self) {
        match self.core.distribution.restore().await {
            Ok(true) => match Loaded::read(&self.core.distribution) {
                Ok(Some(loaded)) => {
                    *self.core.served.lock().unwrap() = loaded.anchor();
                    self.core.metrics.published(loaded.figures(), None);
                    tracing::info!(
                        anchor = loaded.head.anchor_height,
                        "tachygram serves its previous publication again"
                    );
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(%error, "restored tachygram publication unreadable"),
            },
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(%error, "tachygram previous publication not restored")
            }
        }
    }
}

fn identity_error(error: NodeError) -> Error {
    match error {
        NodeError::Identity(error) => Error::Network(error.to_string()),
        other => Error::from(other),
    }
}

impl Ingestion for Writer {
    type Error = Error;

    fn target_height(&self, tip: u64) -> u64 {
        tip.saturating_sub(u64::from(CONFIRMATION_DEPTH))
    }

    /// Every journal starts at the activation height (spec 7.3).
    fn start_height(&self, _tip: u64) -> u64 {
        u64::from(self.core.identity.activation_height)
    }

    /// The network policy (spec 11.1): the observation's genesis and the
    /// node's chain, genesis and `NuTachyon` activation must be the journal's.
    /// A mismatch withdraws and stops the product. The first pass restores the
    /// previous process's publication.
    async fn observed(&mut self, observation: &ChainObservation) -> Result<()> {
        let genesis = self.core.identity.genesis_hash.display_hex();
        if observation.genesis != genesis {
            self.core.distribution.withdraw()?;
            return Err(Error::Network(format!(
                "observed genesis {} is not the journal's {genesis}",
                observation.genesis
            )));
        }
        Self::check_identity(self.core.clone()).await?;
        if std::mem::take(&mut self.restore) {
            self.restore().await;
        }
        Ok(())
    }

    fn committed(&self) -> Option<PipelineBlock> {
        self.store.committed().as_ref().map(to_pipeline)
    }

    fn checkpoint_at_or_below(
        &self,
        height: u64,
    ) -> impl Future<Output = Option<PipelineBlock>> + Send {
        let height = u32::try_from(height).unwrap_or(u32::MAX);
        std::future::ready(match self.store.checkpoint_at_or_below(height) {
            Ok(block) => block.as_ref().map(to_pipeline),
            Err(error) => {
                // Reconciliation then settles on `None`; `withdraw_above`
                // reads again and refuses to empty the journal on a failure.
                tracing::error!(%error, "tachygram checkpoint read failed");
                None
            }
        })
    }

    async fn withdraw_above(&mut self, keep: Option<&PipelineBlock>) -> Result<()> {
        let keep = keep.map(from_pipeline).transpose()?;
        // Re-read the checkpoint the scheduler settled on: a `None` that came
        // from a failed read must not empty the journal.
        if keep.is_none() && self.store.checkpoint_at_or_below(u32::MAX).is_err() {
            return Err(Error::other("checkpoint reads fail; not rewinding"));
        }
        let core = self.core.clone();
        let mut rewinds = core.rewinds.lock().unwrap();
        *rewinds += 1;
        self.slot.send_replace(None);
        self.withdraw_for(keep.as_ref())?;
        *self.core.served.lock().unwrap() = None;
        drop(rewinds);
        Ok(())
    }

    async fn rewind(&mut self, keep: Option<&PipelineBlock>) -> Result<()> {
        let keep = keep.map(from_pipeline).transpose()?;
        self.store.rewind(keep.as_ref())?;
        let committed = self.store.committed();
        self.core.metrics.ingested(committed.map(|c| c.height));
        tracing::info!(kept = ?committed.map(|c| c.height), "tachygram journal rewound");
        Ok(())
    }

    /// Fetch `from..=through` (each block by hash after resolving its height
    /// once, spec 11.1), decode, extract and append in order. An integrity
    /// fault stops ingestion (spec 8.3); the faulty block is not written.
    async fn ingest(&mut self, from: u64, through: u64) -> Result<()> {
        let next = match self.store.committed() {
            Some(tip) => u64::from(tip.height) + 1,
            None => u64::from(self.core.identity.activation_height),
        };
        if from != next {
            return Err(Error::other(format!(
                "batch from {from} does not continue the journal at {next}"
            )));
        }
        let from = u32::try_from(from).map_err(Error::other)?;
        let through = u32::try_from(through).map_err(Error::other)?;
        let node = self.core.node.clone();
        let schedule = self.core.schedule;
        let mut blocks = futures_util::stream::iter(from..=through)
            .map(|height| {
                let node = node.clone();
                async move {
                    let hash = node.block_hash(height).await?;
                    let block = node.block_at(&BlockId { height, hash }).await?;
                    Ok::<_, Error>(IndexedBlock::new(&block, &schedule))
                }
            })
            .buffered(FETCH_CONCURRENCY);
        while let Some(block) = blocks.try_next().await? {
            match self.store.append(&block) {
                Ok(()) => self.core.metrics.ingested(Some(block.id.height)),
                Err(error) => {
                    let error = Error::from(error);
                    if let Some(fault) = error.integrity_fault() {
                        tracing::error!(
                            height = block.id.height,
                            %fault,
                            "ALERT: tachygram integrity fault; ingestion stops and nothing containing this block is published"
                        );
                    }
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    /// Offer the journal at its tip for publication, unless this exact state
    /// was already offered (and has not failed) or is already served.
    async fn caught_up(&mut self, _observation: &ChainObservation) -> Result<()> {
        let Some(anchor) = self.store.committed() else {
            return Ok(());
        };
        self.offer(anchor)
    }

    fn failed(&mut self, error: &Error) -> FailurePolicy {
        let policy = error.policy();
        match policy {
            FailurePolicy::Stop => {
                tracing::error!(%error, "ALERT: tachygram ingestion stops")
            }
            FailurePolicy::NextPoll => {
                tracing::warn!(%error, "tachygram ingestion deferred to the next pass")
            }
        }
        policy
    }
}
