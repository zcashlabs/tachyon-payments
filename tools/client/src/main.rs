//! `tachyon-pir-cli`: the diagnostic probe client. Prints one JSON document
//! on stdout; exits 1 on any error (also printed as JSON).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use serde_json::{Value, json};
use tachyon_pir_cli::report::{anchor_json, answer_json, error_json, map_json, outcome_json};
use tachyon_pir_cli::{Client, ClientError, Mount, ScopeSelector, parse_key};
use tachyon_pir_protocol::Product;

#[derive(Parser)]
#[command(
    name = "tachyon-pir-cli",
    version,
    about = "Tachyon PIR diagnostic probe client"
)]
struct Cli {
    /// Query host origin, e.g. https://127.0.0.1:8443.
    #[arg(long)]
    origin: String,
    /// memo, first-contact, tachygram or registry.
    #[arg(long)]
    product: Product,
    /// PEM CA certificate to trust (instead of the built-in roots).
    #[arg(long)]
    ca: Option<PathBuf>,
    /// Use the legacy mount `/v1/<product>` instead of `/<product>/v1`.
    #[arg(long)]
    legacy_mount: bool,
    /// Node JSON-RPC URL; when given, the map's genesis and anchor are
    /// accepted against it before anything else (chain products only).
    #[arg(long)]
    node: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Args, Clone, Copy)]
#[group(required = true, multiple = false)]
struct ScopeArg {
    /// Scope index (epoch for memo and tachygram, span position for
    /// first-contact, chunk id for registry).
    #[arg(long)]
    scope: Option<u64>,
    /// Scope by scope_id.
    #[arg(long)]
    scope_id: Option<u64>,
}

impl ScopeArg {
    fn selector(self) -> ScopeSelector {
        self.scope
            .map(ScopeSelector::Index)
            .or(self.scope_id.map(ScopeSelector::Id))
            .expect("clap requires one of --scope and --scope-id")
    }
}

#[derive(Subcommand)]
enum Command {
    /// Fetch and validate the map; print a summary of every scope.
    Map,
    /// Privately look up a key in one scope.
    Lookup {
        #[command(flatten)]
        scope: ScopeArg,
        /// The key in hex: key16 or the 32-byte tag/digest (memo,
        /// first-contact, registry), or the 32-byte tachygram.
        #[arg(long)]
        key: String,
    },
    /// Send a dummy lookup to one scope, indistinguishable from a real one.
    Dummy {
        #[command(flatten)]
        scope: ScopeArg,
    },
}

async fn run(cli: Cli) -> Result<Value, ClientError> {
    let ca = match &cli.ca {
        Some(path) => Some(
            std::fs::read(path)
                .map_err(|e| ClientError::Certificate(format!("{}: {e}", path.display())))?,
        ),
        None => None,
    };
    let mount = if cli.legacy_mount {
        Mount::Legacy
    } else {
        Mount::Canonical
    };
    let client = Client::with_mount(&cli.origin, cli.product, ca.as_deref(), mount)?;
    let map = client.fetch_map().await?;
    let anchor = match &cli.node {
        Some(node) => Some(anchor_json(&client.accept_anchor(&map, node).await?)),
        None => None,
    };
    let mut out = match cli.command {
        Command::Map => map_json(&map),
        Command::Lookup { scope, key } => {
            let key = parse_key(cli.product, &key)?;
            let outcome = client.lookup(&map, scope.selector(), &key).await?;
            outcome_json(&outcome)
        }
        Command::Dummy { scope } => {
            let answer = client.dummy_query(&map, scope.selector()).await?;
            json!({ "result": "dummy", "answer": answer_json(&answer) })
        }
    };
    if let Some(anchor) = anchor {
        out["anchor"] = anchor;
    }
    out["requests"] = serde_json::to_value(client.stats()).expect("stats serialize");
    Ok(out)
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(out) => {
            println!("{out}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            println!("{}", error_json(&error));
            ExitCode::FAILURE
        }
    }
}
