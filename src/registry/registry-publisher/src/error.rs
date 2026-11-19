//! Why publication did not complete, and what the scheduler does about it.
use pir_pipeline::FailurePolicy;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A failed publication or start-up step.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The registration log refused an operation.
    #[error("registry log: {0}")]
    Store(#[from] registry_store::Error),
    /// Anything else: refused commits, object I/O, a shutdown. Retried at the
    /// next publication attempt.
    #[error("{0}")]
    Other(BoxError),
}

impl Error {
    pub fn other(error: impl Into<BoxError>) -> Self {
        Self::Other(error.into())
    }

    /// The publication loop's policy: always retry at the next attempt; the
    /// previous publication keeps serving meanwhile.
    pub fn policy(&self) -> FailurePolicy {
        FailurePolicy::NextPoll
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
