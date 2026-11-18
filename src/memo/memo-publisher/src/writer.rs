//! The memo journal's writer, driven by the root chain follower through
//! `pir_pipeline::ingestion::Ingestion` (spec 11.2). One writer feeds both
//! products: memo and, when enabled, first-contact.
//!
//! `pir_pipeline` carries block hashes as display-order strings and heights
//! as `u64`; they become `tachyon_chain` protocol-order hashes and `u32`
//! heights here, at this boundary (spec 3, 11.2).
use crate::{
    core::{Core, Loaded, Offer, OfferKey, Side, block_ref},
    error::{Error, Result},
    publication::{Work, snapshot},
};
use futures_util::{StreamExt, TryStreamExt};
use memo_store::{Counts, IndexedBlock, Store};
use pir_pipeline::{
    FailurePolicy,
    ingestion::{BlockId as PipelineBlock, ChainObservation, Ingestion},
};
use std::sync::Arc;
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

/// One product's pending-publication slot.
pub(crate) struct Slot {
    pub side: Arc<Side>,
    pub sender: watch::Sender<Option<Arc<Work>>>,
}

pub struct Writer {
    pub(crate) core: Arc<Core>,
    pub(crate) store: Store,
    pub(crate) slots: Vec<Slot>,
    /// The journal's envelope counts, kept for the metrics.
    pub(crate) totals: Counts,
    /// The previous process's publications have not been offered again yet.
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
            for side in core.sides() {
                side.distribution.withdraw()?;
            }
            return Err(Error::Network(format!(
                "the node reports {identity:?}, the journal is bound to {:?}",
                core.identity
            )));
        }
        Ok(())
    }

    /// The decision of spec 11.4 for one product, durable before the journal
    /// rewinds: `Recover` if the rewind reaches a sealed scope of that
    /// product's last publication (eligible or not), else `Withdraw` if that
    /// publication's anchor is above the kept block. Each product decides
    /// against its own publication: a rewind can reach a sealed memo epoch
    /// while every first-contact span it touches is open, or a sealed span
    /// while every memo epoch it touches is open.
    fn withdraw_for(&self, side: &Side, keep: Option<&BlockId>) -> Result<()> {
        let product = side.product;
        let removed_from = keep.map_or(0, |keep| keep.height.saturating_add(1));
        let reached = match Loaded::read(&side.distribution, product) {
            Ok(None) => None,
            Ok(Some(loaded)) => loaded
                .sealed_scopes()
                .any(|scope| scope.last_height >= removed_from)
                .then(|| {
                    loaded
                        .indexed_scopes()
                        .filter(|(_, scope)| scope.last_height >= removed_from)
                        .map(|(_, scope)| scope.tables.len() as u64)
                        .sum::<u64>()
                }),
            Err(error) => {
                // Unreadable: whether a sealed scope is reached is unknown, so
                // take the transition that is safe either way.
                tracing::warn!(%product, %error, "last publication unreadable; recovering for the rewind");
                Some(0)
            }
        };
        if let Some(revoked) = reached {
            side.distribution.recover()?;
            self.core.metrics.revoked(product, revoked);
            tracing::warn!(
                %product,
                removed_from,
                revoked,
                "rewind reaches a sealed scope; recovery epoch advanced"
            );
        } else if side
            .distribution
            .withdraw_above(keep.map(|keep| u64::from(keep.height)))?
        {
            tracing::info!(%product, removed_from, "publication withdrawn for a rewind");
        }
        Ok(())
    }

    fn offer(&mut self, anchor: BlockId) -> Result<()> {
        let mut failure = None;
        for slot in &self.slots {
            if let Err(error) = Self::offer_side(&self.core, &self.store, slot, anchor) {
                tracing::warn!(product = %slot.side.product, %error, "snapshot not offered");
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn offer_side(core: &Core, store: &Store, slot: &Slot, anchor: BlockId) -> Result<()> {
        let side = &slot.side;
        let current = side.distribution.current();
        let key = OfferKey {
            anchor,
            fence: current.fence,
            rewinds: core.rewinds(),
        };
        {
            let offer = side.offer.lock().unwrap();
            if offer.key == Some(key) && !offer.failed {
                return Ok(());
            }
        }
        // A publication this process made or restored at this anchor serves.
        if current.eligible && *side.served.lock().unwrap() == Some(block_ref(&anchor)) {
            *side.offer.lock().unwrap() = Offer {
                key: Some(key),
                failed: false,
            };
            return Ok(());
        }
        let work = snapshot(core, side, store, key)?;
        *side.offer.lock().unwrap() = Offer {
            key: Some(key),
            failed: false,
        };
        slot.sender.send_replace(Some(Arc::new(work)));
        Ok(())
    }

    async fn restore(&mut self) {
        for side in self.core.sides() {
            let product = side.product;
            match side.distribution.restore().await {
                Ok(true) => match Loaded::read(&side.distribution, product) {
                    Ok(Some(loaded)) => {
                        *side.served.lock().unwrap() = loaded.anchor();
                        self.core.metrics.published(product, loaded.figures(), None);
                        tracing::info!(
                            %product,
                            anchor = loaded.head.anchor_height,
                            "previous publication serves again"
                        );
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(%product, %error, "restored publication unreadable")
                    }
                },
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%product, %error, "previous publication not restored")
                }
            }
        }
    }
}

fn add_counts(totals: &mut Counts, block: &Counts) {
    totals.envelopes_valid += block.envelopes_valid;
    totals.envelopes_invalid += block.envelopes_invalid;
    totals.memo_entries += block.memo_entries;
    totals.first_contact_entries += block.first_contact_entries;
    totals.state_entries += block.state_entries;
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
    /// A mismatch withdraws both products and stops. The first pass restores
    /// the previous process's publications.
    async fn observed(&mut self, observation: &ChainObservation) -> Result<()> {
        let genesis = self.core.identity.genesis_hash.display_hex();
        if observation.genesis != genesis {
            for side in self.core.sides() {
                side.distribution.withdraw()?;
            }
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
                tracing::error!(%error, "memo checkpoint read failed");
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
        for slot in &self.slots {
            slot.sender.send_replace(None);
        }
        for slot in &self.slots {
            self.withdraw_for(&slot.side, keep.as_ref())?;
            *slot.side.served.lock().unwrap() = None;
        }
        drop(rewinds);
        Ok(())
    }

    async fn rewind(&mut self, keep: Option<&PipelineBlock>) -> Result<()> {
        let keep = keep.map(from_pipeline).transpose()?;
        self.store.rewind(keep.as_ref())?;
        self.totals = self.store.totals()?;
        let committed = self.store.committed();
        self.core
            .metrics
            .journal(committed.map(|c| c.height), &self.totals);
        tracing::info!(kept = ?committed.map(|c| c.height), "memo journal rewound");
        Ok(())
    }

    /// Fetch `from..=through` (each block by hash after resolving its height
    /// once, spec 11.1), decode, extract the envelopes and append in order.
    /// Invalid envelopes are skipped and counted by the extraction; they never
    /// fail ingestion (spec 5.2).
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
            self.store.append(&block)?;
            let counts = block.counts();
            if counts.envelopes_invalid > 0 {
                // Never the memo bytes or the parse error, which quote them.
                tracing::debug!(
                    height = block.id.height,
                    invalid = counts.envelopes_invalid,
                    "invalid memo envelopes skipped"
                );
            }
            add_counts(&mut self.totals, &counts);
            self.core
                .metrics
                .journal(Some(block.id.height), &self.totals);
        }
        Ok(())
    }

    /// Offer the journal at its tip to each product for publication, unless
    /// that product already has this exact state offered (and not failed) or
    /// served.
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
                tracing::error!(%error, "ALERT: memo ingestion stops")
            }
            FailurePolicy::NextPoll => {
                tracing::warn!(%error, "memo ingestion deferred to the next pass")
            }
        }
        policy
    }
}
