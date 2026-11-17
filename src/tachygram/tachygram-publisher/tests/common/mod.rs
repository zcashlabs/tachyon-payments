//! Shared fixtures: synthetic chains served by the node double, an
//! in-process driver over `pir_pipeline::ingestion::catch_up`, the published
//! map and tables read back, and the oracle computed from the authored chain.
#![allow(dead_code)]

use pir_pipeline::{
    engine::PublisherEngine,
    ingestion::{BlockId, CanonicalChain, ChainFuture, ChainObservation, Outcome, catch_up},
    service::Shutdown,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::Arc,
};
use tachygram_publisher::{Error, Report, Tachygram, TachygramConfig, TachygramSettings};
use tachyon_chain::{Credentials, DecodeOptions, NodeClient};
use tachyon_fixtures::{
    BlockSpec, ChainConfig, NodeDouble, NodeOptions, SyntheticChain, TxMix, TxSpec,
};
use tachyon_pir_distribution::{Distribution, Inventory, Subject};
use tachyon_pir_protocol::{
    BlockHash, ChainMapChunk, ChainMapHead, Entry, Product, Scope, ScopeState, ScopeTables,
    TachygramEntry, validate_row,
};

pub const PRODUCT: Product = Product::Tachygram;
pub const ACTIVATION: u64 = 8;
pub const EPOCH_LENGTH: u32 = 16;
pub const SEAL_DEPTH: u64 = 100;
pub const CONFIRMATION_DEPTH: u64 = 2;

/// The first and last heights of `epoch`.
pub fn epoch_bounds(epoch: u64) -> (u64, u64) {
    let first = ACTIVATION + epoch * u64::from(EPOCH_LENGTH);
    (first, first + u64::from(EPOCH_LENGTH) - 1)
}

pub fn epoch_of(height: u64) -> u64 {
    (height - ACTIVATION) / u64::from(EPOCH_LENGTH)
}

/// What a block at a given height carries: proof stamps with this many fresh
/// tachygrams each (a pointer stamp between them), or explicit tachygrams.
#[derive(Clone, Debug)]
pub enum Content {
    Fresh(Vec<u32>),
    Explicit(Vec<Vec<[u8; 32]>>),
}

/// Heights with tachygrams; every other block carries only pointer stamps.
#[derive(Clone, Debug, Default)]
pub struct Plan(pub BTreeMap<u64, Content>);

impl Plan {
    pub fn fresh(blocks: &[(u64, &[u32])]) -> Self {
        Self(
            blocks
                .iter()
                .map(|(height, counts)| (*height, Content::Fresh(counts.to_vec())))
                .collect(),
        )
    }

    pub fn spec(&self, chain: &mut SyntheticChain, height: u64) -> BlockSpec {
        match self.0.get(&height) {
            None => chain.random_block_spec(height),
            Some(Content::Fresh(counts)) => {
                let mut spec = BlockSpec::new();
                for (i, count) in counts.iter().enumerate() {
                    if i > 0 {
                        spec = spec.tx(TxSpec::tachyon(1).pointer());
                    }
                    spec = spec.tx(TxSpec::tachyon(2).random_proof(*count));
                }
                spec
            }
            Some(Content::Explicit(stamps)) => {
                let mut spec = BlockSpec::new();
                for tachygrams in stamps {
                    spec = spec.tx(TxSpec::tachyon(1).proof(tachygrams.clone()));
                }
                spec
            }
        }
    }
}

/// A chain of pointer-only random blocks (no random proof stamps), so the
/// plan alone decides where tachygrams are.
pub fn new_chain(seed: u64) -> SyntheticChain {
    SyntheticChain::new(ChainConfig {
        mix: TxMix {
            tachyon_txs: [0, 2],
            proof_percent: 0,
            ..TxMix::default()
        },
        ..ChainConfig::with_seed(seed)
    })
}

/// Extend `chain` with the plan's blocks up to `height`.
pub fn grow(chain: &mut SyntheticChain, plan: &Plan, height: u64) {
    while chain.tip_height() < height {
        let next = chain.tip_height() + 1;
        let spec = plan.spec(chain, next);
        chain.append(spec);
    }
}

pub async fn node(chain: SyntheticChain) -> NodeDouble {
    NodeDouble::start(chain, NodeOptions::default())
        .await
        .unwrap()
}

pub fn client(node: &NodeDouble) -> NodeClient {
    NodeClient::connect(node.url(), Credentials::None, DecodeOptions::default()).unwrap()
}

/// Canonical hashes from the node, in display order, for the scheduler.
pub struct Canonical(pub NodeClient);
impl CanonicalChain for Canonical {
    fn hash_at(&self, height: u64) -> ChainFuture<'_, String> {
        Box::pin(async move {
            let height = u32::try_from(height)?;
            Ok(self.0.block_hash(height).await?.display_hex())
        })
    }
}

/// The observation a follower would make of `node` now.
pub fn observe(node: &NodeDouble, pass: u64) -> ChainObservation {
    let chain = node.chain();
    ChainObservation {
        genesis: tachyon_fixtures::display(&chain.genesis_hash()),
        tip: BlockId {
            height: chain.tip_height(),
            hash: chain.tip().hash_display(),
        },
        pass,
    }
}

pub fn config(state_dir: &Path) -> TachygramConfig {
    TachygramConfig::new(
        TachygramSettings {
            listen: "127.0.0.1:9402".parse().unwrap(),
            state_dir: state_dir.to_path_buf(),
        },
        EPOCH_LENGTH,
    )
}

pub async fn open(node: &NodeDouble, state_dir: &Path) -> Tachygram {
    Tachygram::open(&config(state_dir), client(node), Shutdown::default())
        .await
        .unwrap()
}

/// One follower pass in the caller's task, then the publication it offered.
pub async fn step(
    tachygram: &mut Tachygram,
    node: &NodeDouble,
    pass: u64,
) -> Result<Option<Report>, Error> {
    let observation = observe(node, pass);
    let canonical = Canonical(client(node));
    match catch_up(&mut tachygram.writer, &canonical, &observation).await {
        Ok(()) => {}
        Err(Outcome::Writer(error)) => return Err(error),
        Err(Outcome::Source(error)) => panic!("chain source: {error}"),
    }
    tachygram.publisher.publish_pending(&PublisherEngine).await
}

/// `(rows_sha256, public_sha256, session_id, seed, entries)` of one table.
pub type TableDigest = ([u8; 32], [u8; 32], [u8; 32], u32, u64);

/// The current envelope's publication, read back from the object store and
/// checked as a client checks it.
pub struct Published {
    pub inventory: Inventory,
    pub head: ChainMapHead,
    pub chunks: Vec<ChainMapChunk>,
    pub eligible: bool,
    pub sequence: u64,
}

impl Published {
    pub fn read(distribution: &Distribution) -> Self {
        let current = distribution.current();
        let inventory = distribution.current_inventory().unwrap().unwrap();
        let head =
            ChainMapHead::parse(&distribution.read_object(&inventory.map_head).unwrap()).unwrap();
        head.validate(PRODUCT).unwrap();
        let chunks: Vec<_> = inventory
            .map_chunks
            .iter()
            .enumerate()
            .map(|(position, object)| {
                head.parse_chunk(
                    PRODUCT,
                    position,
                    &distribution.read_object(object).unwrap(),
                )
                .unwrap()
            })
            .collect();
        head.check_listing(PRODUCT, &chunks).unwrap();
        assert_eq!(
            current.publication.as_ref().unwrap().descriptor.generation,
            head.generation
        );
        Self {
            inventory,
            head,
            chunks,
            eligible: current.eligible,
            sequence: current.sequence,
        }
    }

    pub fn scopes(&self) -> Vec<&Scope> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.scopes.iter())
            .chain(self.head.open_scopes.iter())
            .collect()
    }

    pub fn scope(&self, epoch: u64) -> &Scope {
        self.scopes()
            .into_iter()
            .find(|scope| scope.scope_id == epoch)
            .unwrap()
    }

    pub fn sessions(&self) -> BTreeSet<[u8; 32]> {
        self.inventory.tables.iter().map(|t| t.session_id).collect()
    }

    /// `(rows_sha256, public_sha256, session_id, seed, entries)` of every table
    /// of `epoch`.
    pub fn tables(&self, epoch: u64) -> Vec<TableDigest> {
        self.scope(epoch)
            .tables
            .iter()
            .map(|t| {
                (
                    t.rows_sha256.0,
                    t.public_sha256.0,
                    t.session_id.0,
                    t.seed,
                    t.entries,
                )
            })
            .collect()
    }

    /// The head with its per-publication fields cleared, for comparing two
    /// publications of the same chain.
    pub fn content(&self) -> ChainMapHead {
        ChainMapHead {
            generation: 0,
            recovery_epoch: tachyon_pir_protocol::RecoveryEpoch::new(1).unwrap(),
            ..self.head.clone()
        }
    }

    pub fn genesis(&self) -> BlockHash {
        self.head.genesis_hash
    }

    /// Every row of every table of `epoch`, validated as a client validates
    /// it, keyed by `(bucket, segment, row)`.
    pub fn rows(
        &self,
        distribution: &Distribution,
        epoch: u64,
    ) -> HashMap<(u32, u32, u32), Vec<TachygramEntry>> {
        let scope = self.scope(epoch);
        let genesis = self.genesis();
        let mut rows = HashMap::new();
        for table in &scope.tables {
            let objects = self
                .inventory
                .tables
                .iter()
                .find(|t| t.scope_index == epoch && t.session_id == table.session_id.0)
                .unwrap();
            let bytes = distribution.read_object(&objects.rows).unwrap();
            assert_eq!(bytes.len(), 4096 * 8192);
            let mut count = 0;
            for row in 0..4096u32 {
                let context = scope.row_context(PRODUCT, &genesis, table, row);
                let slice = &bytes[row as usize * 8192..(row as usize + 1) * 8192];
                let entries = validate_row(PRODUCT, slice, &context).unwrap();
                count += entries.len() as u64;
                rows.insert(
                    (table.bucket, table.segment, row),
                    entries
                        .into_iter()
                        .map(|entry| match entry {
                            Entry::Tachygram(entry) => entry,
                            other => panic!("not a tachygram entry: {other:?}"),
                        })
                        .collect(),
                );
            }
            assert_eq!(count, table.entries, "table entry count");
        }
        rows
    }

    /// Every entry of `epoch`'s tables, sorted.
    pub fn entries(&self, distribution: &Distribution, epoch: u64) -> Vec<TachygramEntry> {
        let mut all: Vec<_> = self
            .rows(distribution, epoch)
            .into_values()
            .flatten()
            .collect();
        all.sort_by_key(|e| (e.tachygram, e.blk_height, e.stamp_ordinal));
        all
    }

    /// The subject: anchor and each open scope's last block.
    pub fn subject(&self) -> (u64, Vec<u64>) {
        match &self.inventory.subject {
            Subject::Chain {
                anchor,
                open_scopes,
            } => (
                anchor.height,
                open_scopes.iter().map(|b| b.height).collect(),
            ),
            Subject::Registry { .. } => panic!("registry subject"),
        }
    }
}

/// The authored tachygram records of `epoch` up to `anchor`, sorted: every
/// proof stamp's tachygrams, with the stamp's ordinal among the epoch's proof
/// stamps in block then transaction order.
pub fn oracle(chain: &SyntheticChain, epoch: u64, anchor: u64) -> Vec<TachygramEntry> {
    let (first, last) = epoch_bounds(epoch);
    let mut ordinal = 0u32;
    let mut out = Vec::new();
    for height in first..=last.min(anchor) {
        let block = chain.block(height).unwrap();
        for tx in block.tachyon_txs() {
            if !tx.stamp.is_proof() {
                continue;
            }
            for tachygram in tx.stamp.tachygrams() {
                out.push(TachygramEntry {
                    tachygram: *tachygram,
                    blk_height: height as u32,
                    stamp_ordinal: ordinal,
                });
            }
            ordinal += 1;
        }
    }
    out.sort_by_key(|e| (e.tachygram, e.blk_height, e.stamp_ordinal));
    out
}

/// Checks the publication against the authored chain: anchor, every epoch
/// from 0 listed once with the right state and heights, the subject, and
/// every table's rows decoding to exactly the epoch's authored records.
pub fn check_against_chain(
    published: &Published,
    distribution: &Distribution,
    chain: &SyntheticChain,
) {
    let anchor = chain.tip_height() - CONFIRMATION_DEPTH;
    assert_eq!(u64::from(published.head.anchor_height), anchor);
    assert_eq!(
        published.head.anchor_hash.protocol_bytes(),
        &chain.block(anchor).unwrap().hash()
    );
    assert_eq!(
        published.head.genesis_hash.protocol_bytes(),
        &chain.genesis_hash()
    );
    assert_eq!(published.head.network, "test");
    assert_eq!(u64::from(published.head.activation_height), ACTIVATION);
    assert_eq!(published.head.epoch_length, EPOCH_LENGTH);
    assert_eq!(published.head.floor_scope, 0);
    let newest = epoch_of(anchor);
    let scopes = published.scopes();
    assert_eq!(scopes.len() as u64, newest + 1, "every epoch listed once");
    let mut open_lasts = Vec::new();
    for (index, scope) in scopes.iter().enumerate() {
        let epoch = index as u64;
        assert_eq!(scope.scope_id, epoch);
        let (first, last) = epoch_bounds(epoch);
        let sealed = last + SEAL_DEPTH <= anchor;
        assert_eq!(
            scope.state,
            if sealed {
                ScopeState::Sealed
            } else {
                ScopeState::Open
            },
            "epoch {epoch} state"
        );
        assert_eq!(u64::from(scope.first_height), first);
        let expected_last = if epoch == newest { anchor } else { last };
        assert_eq!(
            u64::from(scope.last_height),
            expected_last,
            "epoch {epoch} last"
        );
        if !sealed {
            open_lasts.push(expected_last);
        }
        let expected = oracle(chain, epoch, anchor);
        assert_eq!(
            scope.entries,
            expected.len() as u64,
            "epoch {epoch} entries"
        );
        if expected.is_empty() {
            assert!(scope.tables.is_empty());
            assert_eq!(scope.n_buckets, 1);
        } else {
            assert_eq!(
                published.entries(distribution, epoch),
                expected,
                "epoch {epoch} records"
            );
        }
    }
    assert_eq!(published.subject(), (anchor, open_lasts));
}

/// A distribution handle to read published objects with.
pub fn distribution(tachygram: &Tachygram) -> Distribution {
    tachygram.distribution().clone()
}

pub fn arc<T>(value: T) -> Arc<T> {
    Arc::new(value)
}
