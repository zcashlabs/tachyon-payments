//! Shared fixtures: synthetic chains with authored envelopes served by the
//! node double, an in-process driver over `pir_pipeline::ingestion::catch_up`,
//! both products' published maps and tables read back, and the oracle
//! computed from the fixture's authored envelopes (never from the journal or
//! the tables).
#![allow(dead_code)]

use memo_publisher::{Error, MemoConfig, MemoPublisher, MemoSettings, Report};
use pir_pipeline::{
    engine::PublisherEngine,
    ingestion::{BlockId, CanonicalChain, ChainFuture, ChainObservation, Outcome, catch_up},
    service::Shutdown,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
};
use tachyon_chain::{Credentials, DecodeOptions, NodeClient};
use tachyon_fixtures::{
    AuthoredEntry, BlockSpec, ChainConfig, EntryKind, Malformation, NodeDouble, NodeOptions,
    SyntheticChain, TxMix, TxSpec, envelope::pack,
};
use tachyon_pir_distribution::{Distribution, Inventory, Subject};
use tachyon_pir_protocol::{
    BlockHash, ChainMapChunk, ChainMapHead, Entry, FirstContactEntry, MemoEntry, Product, Scope,
    ScopeState, ScopeTables, validate_row,
};

pub const ACTIVATION: u64 = 8;
pub const EPOCH_LENGTH: u32 = 4;
pub const SEAL_DEPTH: u64 = 100;
pub const CONFIRMATION_DEPTH: u64 = 2;
/// The span rule's constants (spec 7.2, 15), restated for the oracle.
pub const FC_SPAN_TARGET: usize = 3072;
pub const FC_SPAN_MAX_EPOCHS: u64 = 64;
pub const PRODUCTS: [Product; 2] = [Product::Memo, Product::FirstContact];

/// The first and last heights of `epoch`.
pub fn epoch_bounds(epoch: u64) -> (u64, u64) {
    let first = ACTIVATION + epoch * u64::from(EPOCH_LENGTH);
    (first, first + u64::from(EPOCH_LENGTH) - 1)
}

pub fn epoch_of(height: u64) -> u64 {
    (height - ACTIVATION) / u64::from(EPOCH_LENGTH)
}

/// What one transaction group of a block carries.
#[derive(Clone, Debug)]
pub enum Item {
    /// Random entries of each kind, packed into valid envelopes.
    Random {
        follow_ups: usize,
        first_contacts: usize,
        wallet_states: usize,
    },
    /// Exactly these entries, in this order, packed into valid envelopes.
    Entries(Vec<AuthoredEntry>),
    /// One transaction with an envelope malformed this way.
    Malformed(Malformation),
}

pub fn follow_ups(count: usize) -> Item {
    Item::Random {
        follow_ups: count,
        first_contacts: 0,
        wallet_states: 0,
    }
}

pub fn first_contacts(count: usize) -> Item {
    Item::Random {
        follow_ups: 0,
        first_contacts: count,
        wallet_states: 0,
    }
}

/// Heights with envelopes; every other block carries only empty memos.
#[derive(Clone, Debug, Default)]
pub struct Plan(pub BTreeMap<u64, Vec<Item>>);

impl Plan {
    pub fn new(blocks: Vec<(u64, Vec<Item>)>) -> Self {
        Self(blocks.into_iter().collect())
    }

    pub fn spec(&self, chain: &mut SyntheticChain, height: u64) -> BlockSpec {
        let Some(items) = self.0.get(&height) else {
            return chain.random_block_spec(height);
        };
        let mut spec = BlockSpec::new();
        for item in items {
            match item {
                Item::Random {
                    follow_ups,
                    first_contacts,
                    wallet_states,
                } => {
                    let mut entries = Vec::new();
                    for (kind, count) in [
                        (EntryKind::FollowUp, *follow_ups),
                        (EntryKind::FirstContact, *first_contacts),
                        (EntryKind::WalletState, *wallet_states),
                    ] {
                        for _ in 0..count {
                            entries.push(AuthoredEntry::random(chain.rng(), kind));
                        }
                    }
                    spec = spec.envelopes(&pack(chain.rng(), entries));
                }
                Item::Entries(entries) => {
                    spec = spec.envelopes(&pack(chain.rng(), entries.clone()));
                }
                Item::Malformed(malformation) => {
                    spec = spec.tx(TxSpec::tachyon(1).malformed_memo(*malformation));
                }
            }
        }
        spec
    }
}

/// A chain whose random blocks carry no memos and no proof stamps, so the
/// plan alone decides where envelopes are.
pub fn new_chain(seed: u64) -> SyntheticChain {
    SyntheticChain::new(ChainConfig {
        mix: TxMix {
            tachyon_txs: [0, 2],
            proof_percent: 0,
            memo_percent: 0,
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

/// `count` entries of `kind` that all carry the same tag.
pub fn repeated(chain: &mut SyntheticChain, kind: EntryKind, count: usize) -> Vec<AuthoredEntry> {
    let tag = tachyon_fixtures::envelope::random_tag(chain.rng());
    (0..count)
        .map(|_| AuthoredEntry::random_with_tag(chain.rng(), kind, tag))
        .collect()
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

pub fn config(state_dir: &Path) -> MemoConfig {
    MemoConfig::new(
        MemoSettings {
            listen: "127.0.0.1:9401".parse().unwrap(),
            state_dir: state_dir.to_path_buf(),
        },
        EPOCH_LENGTH,
    )
    .with_first_contact(Default::default())
}

pub async fn open(node: &NodeDouble, state_dir: &Path) -> MemoPublisher {
    MemoPublisher::open(&config(state_dir), client(node), Shutdown::default())
        .await
        .unwrap()
}

/// What one pass published, per product.
#[derive(Debug, Default)]
pub struct Reports {
    pub memo: Option<Report>,
    pub first_contact: Option<Report>,
}

impl Reports {
    pub fn get(&self, product: Product) -> Option<&Report> {
        match product {
            Product::FirstContact => self.first_contact.as_ref(),
            _ => self.memo.as_ref(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.memo.is_none() && self.first_contact.is_none()
    }
}

/// One follower pass in the caller's task, then each product's offered
/// publication.
pub async fn step(
    publisher: &mut MemoPublisher,
    node: &NodeDouble,
    pass: u64,
) -> Result<Reports, Error> {
    let observation = observe(node, pass);
    let canonical = Canonical(client(node));
    match catch_up(&mut publisher.writer, &canonical, &observation).await {
        Ok(()) => {}
        Err(Outcome::Writer(error)) => return Err(error),
        Err(Outcome::Source(error)) => panic!("chain source: {error}"),
    }
    let memo = publisher.memo.publish_pending(&PublisherEngine).await?;
    let first_contact = match publisher.first_contact.as_mut() {
        Some(task) => task.publish_pending(&PublisherEngine).await?,
        None => None,
    };
    Ok(Reports {
        memo,
        first_contact,
    })
}

/// `(rows_sha256, public_sha256, session_id, seed, entries)` of one table.
pub type TableDigest = ([u8; 32], [u8; 32], [u8; 32], u32, u64);

/// A product's current envelope publication, read back from the object store
/// and checked as a client checks it.
pub struct Published {
    pub product: Product,
    pub inventory: Inventory,
    pub head: ChainMapHead,
    pub chunks: Vec<ChainMapChunk>,
    pub eligible: bool,
    pub sequence: u64,
}

impl Published {
    pub fn read(publisher: &MemoPublisher, product: Product) -> Self {
        Self::read_from(publisher.distribution(product).unwrap(), product)
    }

    pub fn read_from(distribution: &Distribution, product: Product) -> Self {
        let current = distribution.current();
        let inventory = distribution.current_inventory().unwrap().unwrap();
        let head =
            ChainMapHead::parse(&distribution.read_object(&inventory.map_head).unwrap()).unwrap();
        head.validate(product).unwrap();
        let chunks: Vec<_> = inventory
            .map_chunks
            .iter()
            .enumerate()
            .map(|(position, object)| {
                head.parse_chunk(
                    product,
                    position,
                    &distribution.read_object(object).unwrap(),
                )
                .unwrap()
            })
            .collect();
        head.check_listing(product, &chunks).unwrap();
        assert_eq!(
            current.publication.as_ref().unwrap().descriptor.generation,
            head.generation
        );
        Self {
            product,
            inventory,
            head,
            chunks,
            eligible: current.eligible,
            sequence: current.sequence,
        }
    }

    /// Every listed scope, in scope-index order.
    pub fn scopes(&self) -> Vec<&Scope> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.scopes.iter())
            .chain(self.head.open_scopes.iter())
            .collect()
    }

    /// The scope at scope index `index`.
    pub fn scope(&self, index: u64) -> &Scope {
        self.scopes()[index as usize]
    }

    pub fn sessions(&self) -> BTreeSet<[u8; 32]> {
        self.inventory.tables.iter().map(|t| t.session_id).collect()
    }

    pub fn scope_sessions(&self, index: u64) -> BTreeSet<[u8; 32]> {
        self.scope(index)
            .tables
            .iter()
            .map(|t| t.session_id.0)
            .collect()
    }

    /// `(rows_sha256, public_sha256, session_id, seed, entries)` of every table
    /// of the scope at `index`.
    pub fn tables(&self, index: u64) -> Vec<TableDigest> {
        self.scope(index)
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

    /// Every row of every table of the scope at `index`, validated as a
    /// client validates it (`validate_row`), keyed by `(bucket, segment, row)`.
    pub fn rows(
        &self,
        distribution: &Distribution,
        index: u64,
    ) -> HashMap<(u32, u32, u32), Vec<Entry>> {
        let scope = self.scope(index);
        let genesis = self.genesis();
        let mut rows = HashMap::new();
        for table in &scope.tables {
            let objects = self
                .inventory
                .tables
                .iter()
                .find(|t| t.scope_index == index && t.session_id == table.session_id.0)
                .unwrap();
            let bytes = distribution.read_object(&objects.rows).unwrap();
            assert_eq!(bytes.len(), 4096 * 8192);
            let mut count = 0;
            for row in 0..4096u32 {
                let context = scope.row_context(self.product, &genesis, table, row);
                let slice = &bytes[row as usize * 8192..(row as usize + 1) * 8192];
                let entries = validate_row(self.product, slice, &context).unwrap();
                count += entries.len() as u64;
                rows.insert((table.bucket, table.segment, row), entries);
            }
            assert_eq!(count, table.entries, "table entry count");
        }
        rows
    }

    /// Every entry for `key` the client's query plan returns from `rows`:
    /// each segment of the key's bucket at the key's row, in segment order.
    pub fn lookup(
        &self,
        rows: &HashMap<(u32, u32, u32), Vec<Entry>>,
        index: u64,
        key: &[u8; 16],
    ) -> Vec<Entry> {
        let scope = self.scope(index);
        let mut found = Vec::new();
        for target in scope
            .query_plan(self.product, &self.genesis(), key)
            .unwrap()
        {
            found.extend(
                rows[&(target.table.bucket, target.table.segment, target.row)]
                    .iter()
                    .filter(|entry| entry.key() == key)
                    .cloned(),
            );
        }
        found
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

/// One scope the oracle expects.
#[derive(Clone, Debug)]
pub struct Expected {
    pub index: u64,
    pub scope_id: u64,
    pub state: ScopeState,
    pub first_height: u64,
    pub last_height: u64,
    /// The authored entries in ordinal order.
    pub entries: Vec<Entry>,
}

fn memo_entries(chain: &SyntheticChain, from: u64, through: u64) -> Vec<Entry> {
    let mut truth = chain.follow_ups(from..=through);
    truth.sort_by_key(|t| t.ordinal());
    truth
        .into_iter()
        .map(|t| {
            Entry::Memo(MemoEntry {
                key: t.key16(),
                ciphertext: t.ciphertext,
            })
        })
        .collect()
}

fn first_contact_entries(chain: &SyntheticChain, from: u64, through: u64) -> Vec<Entry> {
    let mut truth = chain.first_contacts(from..=through);
    truth.sort_by_key(|t| t.ordinal());
    truth
        .into_iter()
        .map(|t| {
            Entry::FirstContact(Box::new(FirstContactEntry {
                key: t.key16(),
                kem_ciphertext: t.kem_ciphertext,
                ciphertext: t.ciphertext,
            }))
        })
        .collect()
}

/// Memo scopes from the authored chain: one per epoch up to the anchor's;
/// the newest ends at the anchor, older ones at their epoch's last block.
pub fn expected_memo(chain: &SyntheticChain, anchor: u64) -> Vec<Expected> {
    let newest = epoch_of(anchor);
    (0..=newest)
        .map(|epoch| {
            let (first, last) = epoch_bounds(epoch);
            let last_height = if epoch == newest { anchor } else { last };
            Expected {
                index: epoch,
                scope_id: epoch,
                state: if last + SEAL_DEPTH <= anchor {
                    ScopeState::Sealed
                } else {
                    ScopeState::Open
                },
                first_height: first,
                last_height,
                entries: memo_entries(chain, first, last_height),
            }
        })
        .collect()
}

/// The spans the span rule gives the authored chain up to `anchor`
/// (spec 7.2): `(first_epoch, last_epoch once closed)`. A boundary is
/// decided once an epoch's last block is at or below the anchor.
pub fn spans(chain: &SyntheticChain, anchor: u64) -> Vec<(u64, Option<u64>)> {
    let newest = epoch_of(anchor);
    let mut spans = Vec::new();
    let mut start = 0;
    let mut sum = 0;
    for epoch in 0..=newest {
        let (first, last) = epoch_bounds(epoch);
        sum += chain.first_contacts(first..=last.min(anchor)).len();
        if last <= anchor && (sum >= FC_SPAN_TARGET || epoch - start + 1 >= FC_SPAN_MAX_EPOCHS) {
            spans.push((start, Some(epoch)));
            start = epoch + 1;
            sum = 0;
        }
    }
    if start <= newest {
        spans.push((start, None));
    }
    spans
}

/// First-contact scopes from the authored chain: span `i` at index `i`,
/// `scope_id` its first epoch; closed spans end at their last epoch's last
/// block, the open one at the anchor.
pub fn expected_first_contact(chain: &SyntheticChain, anchor: u64) -> Vec<Expected> {
    spans(chain, anchor)
        .into_iter()
        .enumerate()
        .map(|(index, (first_epoch, last_epoch))| {
            let first = epoch_bounds(first_epoch).0;
            let (last_height, sealed) = match last_epoch {
                Some(last_epoch) => {
                    let last = epoch_bounds(last_epoch).1;
                    (last, last + SEAL_DEPTH <= anchor)
                }
                None => (anchor, false),
            };
            Expected {
                index: index as u64,
                scope_id: first_epoch,
                state: if sealed {
                    ScopeState::Sealed
                } else {
                    ScopeState::Open
                },
                first_height: first,
                last_height,
                entries: first_contact_entries(chain, first, last_height),
            }
        })
        .collect()
}

pub fn expected(chain: &SyntheticChain, product: Product, anchor: u64) -> Vec<Expected> {
    match product {
        Product::FirstContact => expected_first_contact(chain, anchor),
        _ => expected_memo(chain, anchor),
    }
}

fn sorted(entries: &[Entry]) -> Vec<Vec<u8>> {
    let mut bytes: Vec<_> = entries.iter().map(Entry::to_bytes).collect();
    bytes.sort();
    bytes
}

/// Checks a product's publication against the authored chain: anchor, every
/// scope from index 0 listed once with the right id, state and heights, the
/// subject, every table's rows decoding to exactly the scope's authored
/// entries, and every key's lookup through the query plan returning all of
/// its entries in ordinal order (spec 5.3).
pub fn check_against_chain(
    published: &Published,
    distribution: &Distribution,
    chain: &SyntheticChain,
) {
    let product = published.product;
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
    assert_eq!(published.head.protocol, product.protocol());
    assert_eq!(published.head.network, "test");
    assert_eq!(u64::from(published.head.activation_height), ACTIVATION);
    assert_eq!(published.head.epoch_length, EPOCH_LENGTH);
    assert_eq!(published.head.floor_scope, 0);
    let expected = expected(chain, product, anchor);
    let scopes = published.scopes();
    assert_eq!(
        scopes.len(),
        expected.len(),
        "{product}: every scope listed once"
    );
    assert_eq!(
        published.inventory.newest_scope,
        expected.len() as u64 - 1,
        "{product}: newest scope"
    );
    let mut open_lasts = Vec::new();
    for (scope, want) in scopes.iter().zip(&expected) {
        let index = want.index;
        assert_eq!(scope.scope_id, want.scope_id, "{product} {index}: scope_id");
        assert_eq!(scope.state, want.state, "{product} {index}: state");
        assert_eq!(u64::from(scope.first_height), want.first_height);
        assert_eq!(
            u64::from(scope.last_height),
            want.last_height,
            "{product} {index}: last_height"
        );
        if want.state == ScopeState::Open {
            open_lasts.push(want.last_height);
        }
        assert_eq!(
            scope.entries,
            want.entries.len() as u64,
            "{product} {index}: entries"
        );
        let objects: Vec<_> = published
            .inventory
            .tables
            .iter()
            .filter(|t| t.scope_index == index)
            .collect();
        assert_eq!(
            objects.len(),
            scope.tables.len(),
            "{product} {index}: inventory"
        );
        if want.entries.is_empty() {
            assert!(scope.tables.is_empty());
            assert_eq!(scope.n_buckets, 1);
            continue;
        }
        let rows = published.rows(distribution, index);
        let decoded: Vec<Entry> = rows.values().flatten().cloned().collect();
        assert_eq!(
            sorted(&decoded),
            sorted(&want.entries),
            "{product} {index}: rows hold exactly the authored entries"
        );
        let keys: BTreeSet<[u8; 16]> = want
            .entries
            .iter()
            .map(|e| e.key().try_into().unwrap())
            .collect();
        for key in keys {
            let ordinal: Vec<Entry> = want
                .entries
                .iter()
                .filter(|e| e.key() == key)
                .cloned()
                .collect();
            assert_eq!(
                published.lookup(&rows, index, &key),
                ordinal,
                "{product} {index}: every entry of a key, in ordinal order"
            );
        }
    }
    assert_eq!(published.subject(), (anchor, open_lasts));
}

/// Checks both products.
pub fn check_both(publisher: &MemoPublisher, chain: &SyntheticChain) {
    for product in PRODUCTS {
        let published = Published::read(publisher, product);
        assert!(published.eligible, "{product} eligible");
        check_against_chain(&published, publisher.distribution(product).unwrap(), chain);
    }
}

/// The value of an unlabelled series in Prometheus text.
pub fn metric(text: &str, name: &str) -> String {
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("no {name} in\n{text}"))
        .to_owned()
}
