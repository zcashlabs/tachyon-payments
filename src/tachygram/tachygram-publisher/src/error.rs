//! Why ingestion or publication did not complete, and what the scheduler does
//! about it.
use pir_pipeline::FailurePolicy;
use tachygram_store::IntegrityFault;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A failed writer or publication operation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The chain carries tachygrams that cannot be canonical content: a
    /// duplicate within an epoch or an all-zero tachygram (spec 8.3, 6.2).
    /// Ingestion stops and alerts; nothing containing the block is published.
    #[error("tachygram integrity fault: {0}")]
    Integrity(#[from] IntegrityFault),
    /// The node is on another network, lost the `NuTachyon` upgrade or
    /// changed its activation height (spec 11.1). Never reconciled.
    #[error("network identity: {0}")]
    Network(String),
    /// Anything else: node outages, stale observations, refused commits,
    /// I/O. Retried on the next pass.
    #[error("{0}")]
    Other(BoxError),
}

impl Error {
    pub fn other(error: impl Into<BoxError>) -> Self {
        Self::Other(error.into())
    }

    /// The scheduler's policy (`Ingestion::failed`): integrity and network
    /// faults stop the product (spec 8.3, 11.1); everything else waits for the
    /// next poll.
    pub fn policy(&self) -> FailurePolicy {
        match self {
            Self::Integrity(_) | Self::Network(_) => FailurePolicy::Stop,
            Self::Other(_) => FailurePolicy::NextPoll,
        }
    }

    /// The integrity fault, if this is one.
    pub fn integrity_fault(&self) -> Option<&IntegrityFault> {
        match self {
            Self::Integrity(fault) => Some(fault),
            _ => None,
        }
    }
}

impl From<tachygram_store::Error> for Error {
    fn from(error: tachygram_store::Error) -> Self {
        match error {
            tachygram_store::Error::Integrity(fault) => Self::Integrity(fault),
            other => Self::Other(other.into()),
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
