//! Preparation of a publication before it serves (spec 6.6, 11.6, scenario
//! 10): the whole map is checked against the configuration and the
//! inventory, then every new table's rows are validated row by row with the
//! table crate's validation, its runtime is built on the dedicated
//! preparation pool, and its public setup is checked against the published
//! object. Any failure refuses the publication; the previous one keeps
//! serving.

use crate::{
    Error,
    config::ProductConfig,
    map::{self, ListedTable, MapInfo},
    product::ProductModule,
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use tachyon_pir_distribution::{Preparer, SessionId, Subject, VerifiedPublication, VerifiedTable};
use tachyon_pir_protocol::BlockHash;
use tachyon_pir_table::{TableRuntime, prepare_published_in, rows_sha256, validate_table};

/// Map infos kept for the routes: the current map and a few predecessors.
const MAP_INFOS: usize = 8;

/// Per-map facts the routes and metrics read, keyed by map head digest.
#[derive(Clone, Default)]
pub struct MapCache(Arc<Mutex<VecDeque<KeyedInfo>>>);

type KeyedInfo = (String, Arc<MapInfo>);

impl MapCache {
    pub fn get(&self, map_sha256: &str) -> Option<Arc<MapInfo>> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|(sha, _)| sha == map_sha256)
            .map(|(_, info)| info.clone())
    }

    fn insert(&self, map_sha256: String, info: MapInfo) {
        let mut infos = self.0.lock().unwrap();
        infos.retain(|(sha, _)| *sha != map_sha256);
        infos.push_back((map_sha256, Arc::new(info)));
        while infos.len() > MAP_INFOS {
            infos.pop_front();
        }
    }
}

/// What the configuration says every map must state.
#[derive(Clone, Debug)]
struct Expected {
    network: String,
    genesis: BlockHash,
    chain: Option<crate::config::ChainParams>,
}

/// The host's [`Preparer`] for one product.
pub struct HostPreparer {
    module: &'static ProductModule,
    expected: Expected,
    pool: Arc<rayon::ThreadPool>,
    maps: MapCache,
    /// Tables of the publication last checked, by session.
    pending: Mutex<HashMap<SessionId, ListedTable>>,
}

impl HostPreparer {
    pub fn new(config: &ProductConfig, pool: Arc<rayon::ThreadPool>, maps: MapCache) -> Self {
        Self {
            module: config.module,
            expected: Expected {
                network: config.network.clone(),
                genesis: config.genesis,
                chain: config.chain,
            },
            pool,
            maps,
            pending: Mutex::default(),
        }
    }
}

fn hex32(bytes: &[u8; 32]) -> String {
    hex::encode(bytes)
}

impl Preparer for HostPreparer {
    type Runtime = TableRuntime;

    fn check(&self, publication: &VerifiedPublication<'_>) -> Result<(), Error> {
        let product = self.module.product;
        let inventory = publication.inventory;
        let listing = map::read(
            self.module.map,
            product,
            publication.map_head,
            publication.map_chunks,
        )?;
        if listing.network != self.expected.network {
            return Err(format!(
                "map network {:?} is not the configured publisher_network {:?}",
                listing.network, self.expected.network
            )
            .into());
        }
        if listing.genesis != self.expected.genesis {
            return Err("map genesis_hash is not the configured publisher_genesis".into());
        }
        if listing.generation != inventory.generation
            || listing.recovery_epoch != inventory.recovery_epoch
        {
            return Err("map generation or recovery_epoch differs from the inventory".into());
        }
        match (&listing.chain, &self.expected.chain, &inventory.subject) {
            (
                Some(chain),
                Some(expected),
                Subject::Chain {
                    anchor,
                    open_scopes,
                },
            ) => {
                if chain.activation_height != expected.activation_height
                    || chain.epoch_length != expected.epoch_length
                {
                    return Err(
                        "map activation_height or epoch_length is not the configured one".into(),
                    );
                }
                if anchor.height != u64::from(chain.anchor_height)
                    || anchor.hash != *chain.anchor_hash.protocol_bytes()
                {
                    return Err("the authority subject's anchor is not the map's anchor".into());
                }
                let subject_heights: Vec<u64> = open_scopes.iter().map(|b| b.height).collect();
                let map_heights: Vec<u64> = chain
                    .open_last_heights
                    .iter()
                    .map(|h| u64::from(*h))
                    .collect();
                if subject_heights != map_heights {
                    return Err(
                        "the authority subject does not name the last block of every open scope"
                            .into(),
                    );
                }
            }
            (None, None, Subject::Registry { log_sequence }) => {
                if listing.log_sequence != Some(*log_sequence) {
                    return Err("the authority subject's log_sequence is not the map's".into());
                }
            }
            _ => return Err("the publication's subject does not fit the product".into()),
        }
        let newest = listing.scopes.saturating_sub(1);
        if inventory.newest_scope != newest {
            return Err("inventory newest_scope is not the map's newest scope".into());
        }
        // The inventory names exactly the map's tables, with the map's digests.
        let listed: BTreeMap<SessionId, (u64, String, String)> = listing
            .tables
            .iter()
            .map(|t| {
                (
                    t.table.session_id.0,
                    (
                        t.scope_index,
                        t.table.rows_sha256.hex(),
                        t.table.public_sha256.hex(),
                    ),
                )
            })
            .collect();
        let named: BTreeMap<SessionId, (u64, String, String)> = inventory
            .tables
            .iter()
            .map(|t| {
                (
                    t.session_id,
                    (
                        t.scope_index,
                        t.rows.sha256.clone(),
                        t.public.sha256.clone(),
                    ),
                )
            })
            .collect();
        if listed.len() != listing.tables.len() || listed != named {
            return Err("inventory tables differ from the map's tables".into());
        }
        let pending = listing
            .tables
            .iter()
            .filter(|t| t.scope_index >= publication.floor_scope)
            .map(|t| (t.table.session_id.0, t.clone()))
            .collect();
        *self.pending.lock().unwrap() = pending;
        self.maps
            .insert(inventory.map_head.sha256.clone(), listing.info());
        Ok(())
    }

    fn prepare(&self, table: &VerifiedTable) -> Result<TableRuntime, Error> {
        let product = self.module.product;
        let session = table.objects.session_id;
        let listed = self
            .pending
            .lock()
            .unwrap()
            .get(&session)
            .cloned()
            .ok_or("table is not one the checked map lists")?;
        let rows = table.read_rows()?;
        if rows_sha256(&rows) != listed.table.rows_sha256.0 {
            return Err("rows do not hash to the map's rows_sha256".into());
        }
        let entries = self
            .pool
            .install(|| validate_table(product, &listed.template, &rows))
            .map_err(|error| format!("table {} refused: {error}", hex32(&session)))?;
        if entries != listed.table.entries {
            return Err(format!(
                "table {} holds {entries} entries, the map says {}",
                hex32(&session),
                listed.table.entries
            )
            .into());
        }
        let prepared =
            prepare_published_in(&self.pool, product, listed.scope_id, &listed.table, &rows)
                .map_err(|error| format!("table {} refused: {error}", hex32(&session)))?;
        if prepared.runtime.public() != &table.public[..] {
            return Err(format!(
                "table {}: prepared public setup differs from the published object",
                hex32(&session)
            )
            .into());
        }
        Ok(prepared.runtime)
    }
}
