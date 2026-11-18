//! What the generic distribution asks of the memo and first-contact products
//! (`PublisherHooks`): whether subjects are still canonical, and the map head
//! re-issued under a new generation and recovery epoch.
use std::collections::HashMap;
use tachyon_chain::NodeClient;
use tachyon_pir_distribution::{
    Error, Subject,
    publisher::{BoxFuture, PublisherHooks},
};
use tachyon_pir_protocol::{ChainMapHead, Product, RecoveryEpoch};

pub(crate) struct ChainHooks {
    pub node: NodeClient,
    pub product: Product,
}

impl PublisherHooks for ChainHooks {
    /// Every block a subject names (the anchor and each open scope's last
    /// block) must be the node's block at its height. A block above the node's
    /// tip is neither confirmed nor refuted, so the check fails (`503`) rather
    /// than answer either way.
    fn check_subjects(&self, subjects: Vec<Subject>) -> BoxFuture<Result<Vec<bool>, Error>> {
        let node = self.node.clone();
        let product = self.product;
        Box::pin(async move {
            let tip = node.tip().await?.height;
            let mut canonical = HashMap::new();
            let mut answers = Vec::with_capacity(subjects.len());
            for subject in &subjects {
                if !matches!(subject, Subject::Chain { .. }) {
                    return Err(format!("{product} subjects name chain blocks").into());
                }
                let mut confirmed = true;
                for block in subject.blocks() {
                    let height = u32::try_from(block.height)
                        .map_err(|_| "subject block height beyond u32")?;
                    if height > tip {
                        return Err(format!(
                            "subject block {height} is above the node's tip {tip}"
                        )
                        .into());
                    }
                    let hash = match canonical.get(&height) {
                        Some(hash) => *hash,
                        None => {
                            let hash = node.block_hash(height).await?;
                            canonical.insert(height, hash);
                            hash
                        }
                    };
                    if *hash.protocol_bytes() != block.hash {
                        confirmed = false;
                        break;
                    }
                }
                answers.push(confirmed);
            }
            Ok(answers)
        })
    }

    fn reissue_head(
        &self,
        head: &[u8],
        generation: u64,
        recovery_epoch: u64,
    ) -> Result<Vec<u8>, Error> {
        let mut head = ChainMapHead::parse(head)?;
        if head.product() != Some(self.product) {
            return Err(format!("not a {} map head", self.product).into());
        }
        head.generation = generation;
        head.recovery_epoch =
            RecoveryEpoch::new(recovery_epoch).ok_or("recovery epoch must be positive")?;
        Ok(head.to_bytes()?)
    }
}
