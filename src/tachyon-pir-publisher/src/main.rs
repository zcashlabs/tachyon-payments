//! `tachyon-pir-publisher --config FILE [--check]` (spec 11.7).
//!
//! Exit status: 0 after a valid `--check` or a requested shutdown (SIGTERM or
//! SIGINT); 1 when the configuration is invalid, the node refuses start-up,
//! or the host fails; 2 for command-line usage errors.
use clap::Parser;
use std::{io::IsTerminal, path::PathBuf, process::ExitCode};
use tachyon_pir_publisher::{Config, PRODUCTS, Result, Shutdown};

#[derive(Parser)]
#[command(
    name = "tachyon-pir-publisher",
    version,
    about = "Tachyon PIR publisher host"
)]
struct Args {
    /// The JSON configuration file.
    #[arg(long, value_name = "FILE", required_unless_present = "contract_info")]
    config: Option<PathBuf>,
    /// Validate the configuration offline (no node, sockets or state) and exit.
    #[arg(long, requires = "config")]
    check: bool,
    /// Print the service-discovery contract and compiled products as JSON and
    /// exit without reading configuration.
    #[arg(long, exclusive = true)]
    contract_info: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    match command(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let mut message = error.to_string();
            let mut source = error.source();
            while let Some(cause) = source {
                message.push_str(&format!(": {cause}"));
                source = cause.source();
            }
            tracing::error!(error = %message, "publisher stopped");
            eprintln!("tachyon-pir-publisher: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn command(args: Args) -> Result<()> {
    if args.contract_info {
        let info = pir_service_discovery::ContractInfo::new("tachyon-pir-publisher", PRODUCTS, &[]);
        println!("{}", info.to_json());
        return Ok(());
    }
    let path = args.config.ok_or("--config FILE is required")?;
    let config = Config::load(&path)?;
    if args.check {
        let plan = config.plan()?;
        let products = plan.product_names();
        println!(
            "configuration {} is valid: management {}, products [{}]; the node was not contacted",
            path.display(),
            plan.management_listen,
            products.join(", ")
        );
        return Ok(());
    }
    let shutdown = Shutdown::default();
    let signals = signals(shutdown.clone())?;
    let result = tachyon_pir_publisher::run(config, shutdown).await;
    signals.abort();
    result?;
    tracing::info!("publisher stopped cleanly");
    Ok(())
}

/// Request `shutdown` on the first SIGTERM or SIGINT. The handlers are
/// installed before start-up, so an early signal is not lost.
fn signals(shutdown: Shutdown) -> Result<tokio::task::JoinHandle<()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    Ok(tokio::spawn(async move {
        let name = tokio::select! {
            _ = terminate.recv() => "SIGTERM",
            _ = interrupt.recv() => "SIGINT",
        };
        tracing::info!(signal = name, "shutdown requested");
        shutdown.request();
    }))
}
