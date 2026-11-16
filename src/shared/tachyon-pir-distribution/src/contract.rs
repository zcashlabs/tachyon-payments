//! The publisher-to-query-host contract (spec 11.3): product naming, the
//! envelope descriptor, the inventory a publication consists of, and the
//! subject a query host asks the publisher to confirm.
use crate::Error;
use pir_publication::{
    ObjectReference, PublicationEnvelope, PublicationReference, PublicationScope,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// A content-addressed table session (spec 10.4).
pub type SessionId = [u8; 32];

/// The envelope every product serves at `current`.
pub type Current = PublicationEnvelope<Descriptor>;

/// Rows object of one `tachyon-4k8` table: 4,096 rows of 8,192 bytes
/// (spec 6.1, the only published geometry in version 1).
pub const SPEC_ROWS_BYTES: u64 = 4096 * 8192;
/// Public setup object of one table, `P8.public_len(8192)` (spec 6.6).
pub const SPEC_PUBLIC_BYTES: u64 = 45_056;
/// The map head is at most `MAX_MAP_HEAD_BYTES = 1 MiB` (spec 10.3, 15).
pub const MAP_HEAD_LIMIT: u64 = 1 << 20;
/// Bound on one map chunk object. The spec fixes none; 32 scopes of 64
/// buckets and 8 segments each stay below it.
pub const MAP_CHUNK_LIMIT: u64 = 8 << 20;
/// Bound on the inventory object itself.
pub const INVENTORY_LIMIT: u64 = 16 << 20;
/// Scopes per map chunk (spec 15, `MAP_CHUNK_SCOPES`).
pub const MAP_CHUNK_SCOPES: u64 = 32;
/// Bound on the open scopes a chain subject names.
pub const MAX_OPEN_SCOPES: usize = 64;
/// Bound on blocks across all subjects of one authority check, so the
/// publisher's work per request is bounded before any lookup.
pub const MAX_CHECK_BLOCKS: usize = 4096;

const SOURCE_DOMAIN: &[u8] = b"tachyon-pir/v1/distribution-source\0";

/// One of the four products (spec 2). Every name the distribution contract
/// uses is derived from [`Product::name`] (spec 11.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Product {
    Memo,
    FirstContact,
    Tachygram,
    Registry,
}

impl Product {
    pub const ALL: [Product; 4] = [
        Product::Memo,
        Product::FirstContact,
        Product::Tachygram,
        Product::Registry,
    ];

    /// The product name: `memo`, `first-contact`, `tachygram` or `registry`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Memo => "memo",
            Self::FirstContact => "first-contact",
            Self::Tachygram => "tachygram",
            Self::Registry => "registry",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|product| product.name() == name)
    }

    /// Distribution prefix `/provider/<product>/v1/`, as `HttpsSource` takes it.
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::Memo => "/provider/memo/v1/",
            Self::FirstContact => "/provider/first-contact/v1/",
            Self::Tachygram => "/provider/tachygram/v1/",
            Self::Registry => "/provider/registry/v1/",
        }
    }

    /// The prefix without its trailing slash, as `Router::nest` takes it.
    pub const fn mount_path(self) -> &'static str {
        match self {
            Self::Memo => "/provider/memo/v1",
            Self::FirstContact => "/provider/first-contact/v1",
            Self::Tachygram => "/provider/tachygram/v1",
            Self::Registry => "/provider/registry/v1",
        }
    }

    /// Distribution schema `tachyon-<product>-prepared-distribution-v1`, carried
    /// as the envelope scope's `product`.
    pub const fn schema(self) -> &'static str {
        match self {
            Self::Memo => "tachyon-memo-prepared-distribution-v1",
            Self::FirstContact => "tachyon-first-contact-prepared-distribution-v1",
            Self::Tachygram => "tachyon-tachygram-prepared-distribution-v1",
            Self::Registry => "tachyon-registry-prepared-distribution-v1",
        }
    }

    /// Whether the product's publications depend on the chain (every product
    /// but the registry).
    pub const fn follows_chain(self) -> bool {
        !matches!(self, Self::Registry)
    }

    /// The envelope scope this product publishes under.
    pub fn scope(self, binding: &ScopeBinding) -> PublicationScope {
        PublicationScope {
            product: self.schema().into(),
            chain: binding.genesis,
            source: binding.source,
        }
    }
}

/// What a publisher and a query host must agree on before any envelope is
/// accepted: the network (genesis hash, protocol order) and the source
/// parameters that shape every scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScopeBinding {
    pub genesis: [u8; 32],
    pub source: [u8; 32],
}

impl ScopeBinding {
    /// A chain product: `source = SHA-256("tachyon-pir/v1/distribution-source\0"
    /// || "chain\0" || activation_height(u64 LE) || epoch_length(u64 LE))`.
    pub fn chain(genesis: [u8; 32], activation_height: u64, epoch_length: u64) -> Self {
        let mut hash = Sha256::new();
        hash.update(SOURCE_DOMAIN);
        hash.update(b"chain\0");
        hash.update(activation_height.to_le_bytes());
        hash.update(epoch_length.to_le_bytes());
        Self {
            genesis,
            source: hash.finalize().into(),
        }
    }

    /// The registry: `source = SHA-256("tachyon-pir/v1/distribution-source\0"
    /// || "registry\0" || chunk_entries(u64 LE))`.
    pub fn registry(genesis: [u8; 32], chunk_entries: u64) -> Self {
        let mut hash = Sha256::new();
        hash.update(SOURCE_DOMAIN);
        hash.update(b"registry\0");
        hash.update(chunk_entries.to_le_bytes());
        Self {
            genesis,
            source: hash.finalize().into(),
        }
    }
}

/// Exact and maximum object lengths an inventory is validated against.
///
/// Production code uses [`ObjectLimits::SPEC`] (the default): table lengths
/// from spec 6.1 and 6.6, the map head bound from spec 10.3. Tests that use
/// small byte blobs instead of real tables override the two table lengths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectLimits {
    /// Exact length of every rows object.
    pub rows_bytes: u64,
    /// Exact length of every public setup object.
    pub public_bytes: u64,
    /// Maximum length of the map head.
    pub map_head_bytes: u64,
    /// Maximum length of one map chunk.
    pub map_chunk_bytes: u64,
}

impl ObjectLimits {
    pub const SPEC: Self = Self {
        rows_bytes: SPEC_ROWS_BYTES,
        public_bytes: SPEC_PUBLIC_BYTES,
        map_head_bytes: MAP_HEAD_LIMIT,
        map_chunk_bytes: MAP_CHUNK_LIMIT,
    };
}

impl Default for ObjectLimits {
    fn default() -> Self {
        Self::SPEC
    }
}

/// The product's typed part of the envelope (spec 11.3).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    /// The wallet-facing map head (spec 10.3), an object.
    pub map_head: ObjectReference,
    /// The product's durable publication counter, incremented by every
    /// publication.
    pub generation: u64,
    /// The authority fence's recovery epoch the publication was made under.
    pub recovery_epoch: u64,
}

/// A block a chain publication depends on. JSON writes the hash in display
/// order (spec 3); in memory it is in protocol order.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct BlockRef {
    pub height: u64,
    #[serde(with = "display_hash")]
    pub hash: [u8; 32],
}

/// What a query host asks the publisher to confirm for each view it serves
/// (`authority/check`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum Subject {
    /// Chain products: the map's anchor and the last block of every open scope.
    /// The view stays authoritative while all of them are canonical.
    Chain {
        anchor: BlockRef,
        open_scopes: Vec<BlockRef>,
    },
    /// The registry: the number of log entries the publication contains
    /// (the map's `log_sequence`). The view stays authoritative while the
    /// publisher's log still begins with those entries.
    Registry { log_sequence: u64 },
}

impl Subject {
    /// Blocks the publisher looks up to answer (one for the registry).
    pub fn weight(&self) -> usize {
        match self {
            Self::Chain { open_scopes, .. } => 1 + open_scopes.len(),
            Self::Registry { .. } => 1,
        }
    }

    /// Every block a chain subject names, anchor first; none for the registry.
    pub fn blocks(&self) -> Vec<BlockRef> {
        match self {
            Self::Chain {
                anchor,
                open_scopes,
            } => std::iter::once(*anchor)
                .chain(open_scopes.iter().copied())
                .collect(),
            Self::Registry { .. } => Vec::new(),
        }
    }

    /// The subject kind matches the product, and a chain subject is bounded
    /// and names no open-scope block above its anchor.
    pub fn validate(&self, product: Product) -> Result<(), Error> {
        match self {
            Self::Chain {
                anchor,
                open_scopes,
            } => {
                if !product.follows_chain() {
                    return Err(
                        format!("{} publications have no chain subject", product.name()).into(),
                    );
                }
                if open_scopes.len() > MAX_OPEN_SCOPES
                    || open_scopes.iter().any(|block| block.height > anchor.height)
                {
                    return Err("chain subject names too many or later open-scope blocks".into());
                }
            }
            Self::Registry { .. } => {
                if product.follows_chain() {
                    return Err(
                        format!("{} publications have a chain subject", product.name()).into(),
                    );
                }
            }
        }
        Ok(())
    }
}

/// The objects of one published table: rows and public setup material, bound
/// to the table's session id (spec 10.4).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TableObjects {
    /// The scope index (spec 10.3) of the scope the table belongs to, so a
    /// query host can fetch only the scopes it retains (spec 11.5).
    pub scope_index: u64,
    #[serde(with = "plain_hash")]
    pub session_id: SessionId,
    pub rows: ObjectReference,
    pub public: ObjectReference,
}

/// Everything one publication consists of. It is itself stored as an object,
/// named by the envelope's publication reference.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub generation: u64,
    pub recovery_epoch: u64,
    pub map_head: ObjectReference,
    /// Every sealed map chunk, by ascending `base`.
    pub map_chunks: Vec<ObjectReference>,
    /// Index of the newest scope the map lists.
    pub newest_scope: u64,
    /// Every table of every scope the map lists, by ascending scope index.
    pub tables: Vec<TableObjects>,
    pub subject: Subject,
}

impl Inventory {
    pub fn descriptor(&self) -> Descriptor {
        Descriptor {
            map_head: self.map_head.clone(),
            generation: self.generation,
            recovery_epoch: self.recovery_epoch,
        }
    }

    /// `SHA-256` of the inventory JSON, the publication's material identity.
    pub fn material(&self) -> [u8; 32] {
        Sha256::digest(serde_json::to_vec(self).expect("inventory serializes")).into()
    }

    /// The publication reference naming this inventory stored as `object`.
    pub fn reference(&self, object: ObjectReference) -> PublicationReference<Descriptor> {
        PublicationReference::new(self.material(), object, self.descriptor())
    }

    /// The oldest scope a host retaining `retain_scopes` scopes serves.
    pub fn floor_scope(&self, retain_scopes: Option<u64>) -> u64 {
        match retain_scopes {
            Some(retain) => (self.newest_scope + 1).saturating_sub(retain.max(1)),
            None => 0,
        }
    }

    /// Every object the publication references, each once.
    pub fn objects(&self) -> Vec<ObjectReference> {
        self.objects_from(0)
    }

    /// The map head, every map chunk and the tables of scopes at or above
    /// `floor_scope`, each once, in that order.
    pub fn objects_from(&self, floor_scope: u64) -> Vec<ObjectReference> {
        let mut seen = BTreeSet::new();
        std::iter::once(&self.map_head)
            .chain(&self.map_chunks)
            .chain(
                self.tables
                    .iter()
                    .filter(|table| table.scope_index >= floor_scope)
                    .flat_map(|table| [&table.rows, &table.public]),
            )
            .filter(|object| seen.insert(object.sha256.clone()))
            .cloned()
            .collect()
    }

    /// Structure and object bindings, before anything is fetched or loaded:
    /// exact table object lengths, bounded metadata objects, unique sessions
    /// and chunks, ordered scopes and a subject of the product's kind.
    pub fn validate(&self, product: Product, limits: &ObjectLimits) -> Result<(), Error> {
        if self.generation == 0 || self.recovery_epoch == 0 {
            return Err("inventory generation and recovery epoch start at 1".into());
        }
        self.map_head.validate(limits.map_head_bytes)?;
        if self.map_chunks.len() as u64 > self.newest_scope / MAP_CHUNK_SCOPES + 1 {
            return Err("inventory lists more map chunks than its scopes fill".into());
        }
        let mut metadata = BTreeSet::from([self.map_head.sha256.as_str()]);
        for chunk in &self.map_chunks {
            chunk.validate(limits.map_chunk_bytes)?;
            if !metadata.insert(chunk.sha256.as_str()) {
                return Err("inventory repeats a map object".into());
            }
        }
        let mut sessions = BTreeSet::new();
        let mut previous_scope = 0;
        for table in &self.tables {
            if table.scope_index > self.newest_scope || table.scope_index < previous_scope {
                return Err("inventory tables are not in scope order within the map".into());
            }
            previous_scope = table.scope_index;
            if !sessions.insert(table.session_id) {
                return Err("inventory repeats a table session".into());
            }
            for (object, expected) in [
                (&table.rows, limits.rows_bytes),
                (&table.public, limits.public_bytes),
            ] {
                object.validate(expected)?;
                if object.bytes != expected {
                    return Err(format!(
                        "table object is {} bytes, expected exactly {expected}",
                        object.bytes
                    )
                    .into());
                }
            }
        }
        self.subject.validate(product)
    }

    /// Whether `reference` names exactly this inventory stored as `object`.
    pub fn describes(
        &self,
        object: &ObjectReference,
        reference: &PublicationReference<Descriptor>,
    ) -> bool {
        &self.reference(object.clone()) == reference
    }
}

/// 32-byte values written as lowercase hex in their byte order.
pub(crate) mod plain_hash {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(value))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 32], D::Error> {
        let text = String::deserialize(deserializer)?;
        if text.len() != 64 || text.bytes().any(|c| c.is_ascii_uppercase()) {
            return Err(D::Error::custom("expected 64 lowercase hex digits"));
        }
        let mut value = [0; 32];
        hex::decode_to_slice(&text, &mut value).map_err(D::Error::custom)?;
        Ok(value)
    }
}

/// Block hashes held in protocol order and written in display order (spec 3).
pub(crate) mod display_hash {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error> {
        let mut display = *value;
        display.reverse();
        super::plain_hash::serialize(&display, serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 32], D::Error> {
        let mut value = super::plain_hash::deserialize(deserializer)?;
        value.reverse();
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: ObjectLimits = ObjectLimits {
        rows_bytes: 64,
        public_bytes: 16,
        ..ObjectLimits::SPEC
    };

    fn table(scope_index: u64, seed: u8) -> TableObjects {
        TableObjects {
            scope_index,
            session_id: [seed; 32],
            rows: ObjectReference::for_bytes(&[seed; 64]),
            public: ObjectReference::for_bytes(&[seed; 16]),
        }
    }

    fn inventory() -> Inventory {
        Inventory {
            generation: 3,
            recovery_epoch: 2,
            map_head: ObjectReference::for_bytes(b"head"),
            map_chunks: vec![ObjectReference::for_bytes(b"chunk 0")],
            newest_scope: 1,
            tables: vec![table(0, 1), table(1, 2)],
            subject: Subject::Chain {
                anchor: BlockRef {
                    height: 90,
                    hash: [9; 32],
                },
                open_scopes: vec![BlockRef {
                    height: 90,
                    hash: [9; 32],
                }],
            },
        }
    }

    #[test]
    fn names_derive_from_the_product_name() {
        for product in Product::ALL {
            let name = product.name();
            assert_eq!(Product::from_name(name), Some(product));
            assert_eq!(product.prefix(), format!("/provider/{name}/v1/"));
            assert_eq!(product.mount_path(), format!("/provider/{name}/v1"));
            assert_eq!(
                product.schema(),
                format!("tachyon-{name}-prepared-distribution-v1")
            );
            assert_eq!(product.follows_chain(), name != "registry");
        }
        assert_eq!(Product::from_name("receiver"), None);
    }

    #[test]
    fn scope_bindings_separate_networks_and_parameters() {
        let a = ScopeBinding::chain([1; 32], 8, 4096);
        assert_eq!(a, ScopeBinding::chain([1; 32], 8, 4096));
        assert_ne!(a.source, ScopeBinding::chain([1; 32], 8, 64).source);
        assert_ne!(a.source, ScopeBinding::chain([1; 32], 9, 4096).source);
        assert_ne!(a.source, ScopeBinding::registry([1; 32], 4096).source);
        let scope = Product::Tachygram.scope(&a);
        assert_eq!(scope.product, "tachyon-tachygram-prepared-distribution-v1");
        assert_eq!(scope.chain, [1; 32]);
    }

    #[test]
    fn a_valid_inventory_lists_each_object_once_and_filters_by_floor() {
        let mut inventory = inventory();
        inventory.validate(Product::Memo, &LIMITS).unwrap();
        // Two tables may share identical bytes (empty buckets of different scopes).
        inventory.tables[1].rows = inventory.tables[0].rows.clone();
        inventory.validate(Product::Memo, &LIMITS).unwrap();
        assert_eq!(inventory.objects().len(), 2 + 3);
        assert_eq!(inventory.objects_from(1).len(), 2 + 2);
        assert_eq!(inventory.floor_scope(Some(1)), 1);
        assert_eq!(inventory.floor_scope(Some(64)), 0);
        assert_eq!(inventory.floor_scope(None), 0);
    }

    #[test]
    fn invalid_inventories_are_refused() {
        type Mutation = Box<dyn Fn(&mut Inventory)>;
        let cases: Vec<(&str, Mutation)> = vec![
            (
                "short rows",
                Box::new(|i| i.tables[0].rows = ObjectReference::for_bytes(&[1; 63])),
            ),
            (
                "long public",
                Box::new(|i| i.tables[0].public = ObjectReference::for_bytes(&[1; 17])),
            ),
            (
                "repeated session",
                Box::new(|i| i.tables[1].session_id = i.tables[0].session_id),
            ),
            ("scope order", Box::new(|i| i.tables.reverse())),
            ("scope beyond map", Box::new(|i| i.newest_scope = 0)),
            (
                "repeated chunk",
                Box::new(|i| i.map_chunks.push(i.map_chunks[0].clone())),
            ),
            (
                "chunk is head",
                Box::new(|i| i.map_chunks[0] = i.map_head.clone()),
            ),
            (
                "oversized head",
                Box::new(|i| i.map_head.bytes = MAP_HEAD_LIMIT + 1),
            ),
            (
                "malformed digest",
                Box::new(|i| i.tables[0].rows.sha256 = "../objects".into()),
            ),
            (
                "too many chunks",
                Box::new(|i| i.map_chunks.push(ObjectReference::for_bytes(b"chunk 1"))),
            ),
            ("generation zero", Box::new(|i| i.generation = 0)),
            ("epoch zero", Box::new(|i| i.recovery_epoch = 0)),
            (
                "registry subject",
                Box::new(|i| i.subject = Subject::Registry { log_sequence: 1 }),
            ),
            (
                "open scope above anchor",
                Box::new(|i| {
                    if let Subject::Chain { open_scopes, .. } = &mut i.subject {
                        open_scopes[0].height = 91;
                    }
                }),
            ),
        ];
        for (name, mutate) in cases {
            let mut inventory = inventory();
            mutate(&mut inventory);
            assert!(
                inventory.validate(Product::Tachygram, &LIMITS).is_err(),
                "{name}"
            );
        }
        let spec = inventory();
        assert!(
            spec.validate(Product::Tachygram, &ObjectLimits::SPEC)
                .is_err()
        );
        assert!(spec.validate(Product::Registry, &LIMITS).is_err());
        let mut registry = inventory();
        registry.subject = Subject::Registry { log_sequence: 0 };
        registry.validate(Product::Registry, &LIMITS).unwrap();
    }

    #[test]
    fn unknown_fields_are_refused_and_hashes_use_spec_byte_order() {
        let inventory = inventory();
        let mut json: serde_json::Value = serde_json::to_value(&inventory).unwrap();
        assert_eq!(json["tables"][0]["session_id"], hex::encode([1u8; 32]));
        let mut protocol = [0u8; 32];
        protocol[0] = 0xab;
        let block = BlockRef {
            height: 5,
            hash: protocol,
        };
        let text = serde_json::to_string(&block).unwrap();
        assert!(text.ends_with("ab\"}"), "display order reverses: {text}");
        assert_eq!(serde_json::from_str::<BlockRef>(&text).unwrap(), block);
        assert_eq!(
            serde_json::from_value::<Inventory>(json.clone()).unwrap(),
            inventory
        );
        json["extra"] = 1.into();
        assert!(serde_json::from_value::<Inventory>(json.clone()).is_err());
        json.as_object_mut().unwrap().remove("extra");
        json["tables"][0]["extra"] = 1.into();
        assert!(serde_json::from_value::<Inventory>(json.clone()).is_err());
        json["tables"][0].as_object_mut().unwrap().remove("extra");
        json["subject"]["chain"]["extra"] = 1.into();
        assert!(serde_json::from_value::<Inventory>(json.clone()).is_err());
        json["subject"]["chain"]
            .as_object_mut()
            .unwrap()
            .remove("extra");
        json["tables"][0]["session_id"] = "AB".repeat(32).into();
        assert!(serde_json::from_value::<Inventory>(json).is_err());
        let descriptor = serde_json::json!({
            "map_head": inventory.map_head, "generation": 1, "recovery_epoch": 1, "anchor": 3
        });
        assert!(serde_json::from_value::<Descriptor>(descriptor).is_err());
    }

    #[test]
    fn the_reference_binds_material_and_descriptor() {
        let inventory = inventory();
        let object = ObjectReference::for_bytes(&serde_json::to_vec(&inventory).unwrap());
        let reference = inventory.reference(object.clone());
        assert!(reference.verify().is_ok());
        assert!(inventory.describes(&object, &reference));
        assert_eq!(reference.descriptor.generation, 3);
        assert_eq!(reference.descriptor.recovery_epoch, 2);
        let mut restored = inventory.clone();
        restored.recovery_epoch = 3;
        assert!(!restored.describes(&object, &reference));
        assert_ne!(restored.material(), inventory.material());
    }
}
