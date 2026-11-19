//! End-to-end harness for the Tachyon PIR services (spec sections 4.1 and 13.1).
//!
//! The pieces product end-to-end tests are assembled from:
//!
//! - [`tls_edge`]: an in-process HTTPS reverse proxy in front of the publisher's
//!   plain-HTTP listeners, with a generated `localhost` certificate (the query
//!   host's `publisher_ca`), discovery routing to the management listener,
//!   runtime fault injection, outage and resume on the same port, and request
//!   counting.
//! - [`process`]: spawned binaries with a JSON config in a fresh temporary
//!   directory, logs captured to files and printed when the test fails, killed
//!   with their process group on drop, restart, readiness polling, and fixed-port
//!   reservation.
//! - [`node`]: the node double, in process or as the spawned
//!   `tachyon-node-double-test` binary.
//! - [`client`]: HTTP clients and request helpers.
//! - [`budget`]: the wall-clock budget every end-to-end test runs inside.
//! - [`topology`]: a chain product's whole topology (node double, publisher,
//!   TLS edge, query host) and the waits on its publications and maps.
//! - [`oracle`]: the independent oracle, computed from the authored chain
//!   only (spec 13.1).
//! - [`memo_oracle`]: the independent memo and first-contact oracle, from
//!   the authored envelopes only (epochs, the span rule, sealing, lookups).
//! - [`memo_checks`]: maps, scope listings and lookup answers of those two
//!   products judged against [`memo_oracle`].
//!
//! See the crate README for how a product test strings them together.
pub mod budget;
pub mod client;
pub mod memo_checks;
pub mod memo_oracle;
pub mod node;
pub mod oracle;
pub mod process;
pub mod tls_edge;
pub mod topology;

pub use budget::{Budget, DEFAULT_BUDGET};
pub use process::{Process, free_addr, free_addrs, free_port};
pub use tls_edge::{EdgeCertificate, Fault, FaultId, Target, TlsEdge};

/// The path of a binary of this package, for use in its integration tests.
///
/// `binary!("tachyon-node-double-test")` reads `CARGO_BIN_EXE_<name>`, which
/// Cargo sets only while compiling this package's own tests, so the macro
/// expands in the test crate. At run time the environment variable
/// `TACHYON_E2E_BIN_<NAME>` (the name upper-cased, `-` as `_`) overrides it.
#[macro_export]
macro_rules! binary {
    ($name:literal) => {
        $crate::process::locate($name, env!(concat!("CARGO_BIN_EXE_", $name)))
    };
}
