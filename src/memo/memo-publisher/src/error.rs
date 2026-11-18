//! Why ingestion or publication did not complete, and what the scheduler does
//! about it.
//!
//! Nothing a memo carries is an error: invalid envelopes are skipped and
//! counted by `memo-store` (spec 5.2), so no chain content stops ingestion.
use pir_pipeline::FailurePolicy;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A failed writer or publication operation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The node is on another network, lost the `NuTachyon` upgrade or
    /// changed its activation height (spec 11.1). Never reconciled.
    #[error("network identity: {0}")]
    Network(String),
    /// The journal refused an operation.
    #[error("memo journal: {0}")]
    Journal(#[from] memo_store::Error),
    /// Anything else: node outages, stale observations, refused commits,
    /// I/O. Retried on the next pass.
    #[error("{0}")]
    Other(BoxError),
}

impl Error {
    pub fn other(error: impl Into<BoxError>) -> Self {
        Self::Other(error.into())
    }

    /// The scheduler's policy (`Ingestion::failed`): a network mismatch and a
    /// journal that cannot be what it claims (bound elsewhere, corrupt, an
    /// unknown schema) stop the product; everything else, including a block
    /// that does not follow the tip because the chain moved under the batch,
    /// waits for the next poll.
    pub fn policy(&self) -> FailurePolicy {
        use memo_store::Error as Store;
        match self {
            Self::Network(_) => FailurePolicy::Stop,
            Self::Journal(
                Store::BindingMismatch { .. }
                | Store::SpanRuleMismatch { .. }
                | Store::InvalidBinding(_)
                | Store::UnsupportedSchema(_)
                | Store::NotAStore
                | Store::JournalMode(_)
                | Store::Corrupt(_),
            ) => FailurePolicy::Stop,
            Self::Journal(_) | Self::Other(_) => FailurePolicy::NextPoll,
        }
    }
}

impl From<tachyon_chain::NodeError> for Error {
    fn from(error: tachyon_chain::NodeError) -> Self {
        Self::Other(error.into())
    }
}

impl From<BoxError> for Error {
    fn from(error: BoxError) -> Self {
        Self::Other(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Other(error.into())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
