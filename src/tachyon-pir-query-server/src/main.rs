//! `tachyon-pir-query-server --config FILE [--check]`.

use clap::Parser;
use std::{path::PathBuf, process::ExitCode};
use tachyon_pir_query_server::{Config, Shutdown, Validated};

#[derive(Parser)]
#[command(about = "Tachyon payment PIR query host")]
struct Args {
    /// The JSON configuration file (spec 11.7).
    #[arg(long)]
    config: PathBuf,
    /// Validate the configuration offline and exit: 0 when valid, 1 otherwise.
    #[arg(long)]
    check: bool,
}

fn load(path: &PathBuf) -> Result<Validated, tachyon_pir_query_server::Error> {
    let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    Config::parse(&bytes)?.validate()
}

fn main() -> ExitCode {
    let args = Args::parse();
    let config = match load(&args.config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("invalid configuration: {error}");
            return ExitCode::FAILURE;
        }
    };
    if args.check {
        println!("query server configuration is valid; publishers were not contacted");
        return ExitCode::SUCCESS;
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("cannot start the runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(async move {
        let stop = Shutdown::default();
        let signals = stop.clone();
        tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! {
                result = tokio::signal::ctrl_c() => result?,
                _ = term.recv() => {}
            }
            tracing::info!("stop requested; draining");
            signals.request();
            Ok::<_, std::io::Error>(())
        });
        tachyon_pir_query_server::run(config, stop).await
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "query server stopped");
            eprintln!("query server stopped: {error}");
            ExitCode::FAILURE
        }
    }
}
