//! Serves a synthetic Tachyon chain as a Zakura JSON-RPC node, for end-to-end
//! tests that need the node in a separate process.
//!
//! ```text
//! tachyon-node-double --spec <chain-spec.json> [--listen <addr>]
//!                     [--auth <user:password>] [--cookie <path>]
//!                     [--stale-blocks not_found|serve]
//! ```
//!
//! Once listening it prints one line, the JSON-RPC URL (`http://<ip>:<port>`), on
//! stdout. Control endpoints live under `/control/` on the same listener; see the
//! crate README.
use std::io::Write;
use std::net::SocketAddr;
use std::process::ExitCode;
use tachyon_fixtures::{BasicAuth, ChainSpec, NodeDouble, NodeOptions, StaleBlocks};

const USAGE: &str = "usage: tachyon-node-double --spec <chain-spec.json> [--listen <addr>] \
[--auth <user:password>] [--cookie <path>] [--stale-blocks not_found|serve]";

struct Args {
    spec: String,
    listen: SocketAddr,
    auth: Option<BasicAuth>,
    cookie: Option<String>,
    stale_blocks: StaleBlocks,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let mut spec = None;
    let mut listen = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut auth = None;
    let mut cookie = None;
    let mut stale_blocks = StaleBlocks::NotFound;
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--spec" => spec = Some(value()?),
            "--listen" => {
                listen = value()?
                    .parse()
                    .map_err(|error| format!("--listen: {error}"))?
            }
            "--auth" => {
                let value = value()?;
                let (user, password) = value
                    .split_once(':')
                    .filter(|(user, password)| !user.is_empty() && !password.is_empty())
                    .ok_or("--auth takes user:password")?;
                auth = Some(BasicAuth {
                    user: user.to_owned(),
                    password: password.to_owned(),
                });
            }
            "--cookie" => cookie = Some(value()?),
            "--stale-blocks" => {
                stale_blocks = match value()?.as_str() {
                    "not_found" => StaleBlocks::NotFound,
                    "serve" => StaleBlocks::Serve,
                    other => return Err(format!("--stale-blocks: unknown policy {other:?}")),
                }
            }
            "--help" | "-h" => return Err(USAGE.to_owned()),
            other => return Err(format!("unknown argument {other:?}\n{USAGE}")),
        }
    }
    Ok(Args {
        spec: spec.ok_or(USAGE)?,
        listen,
        auth,
        cookie,
        stale_blocks,
    })
}

async fn run(args: Args) -> Result<(), String> {
    let text =
        std::fs::read_to_string(&args.spec).map_err(|error| format!("{}: {error}", args.spec))?;
    let spec: ChainSpec =
        serde_json::from_str(&text).map_err(|error| format!("{}: {error}", args.spec))?;
    let chain = spec.build().map_err(|error| error.to_string())?;
    if let (Some(path), Some(auth)) = (&args.cookie, &args.auth) {
        auth.write_cookie(path.as_ref())
            .map_err(|error| format!("--cookie {path}: {error}"))?;
    } else if args.cookie.is_some() {
        return Err("--cookie needs --auth".into());
    }
    let options = NodeOptions {
        auth: args.auth,
        stale_blocks: args.stale_blocks,
    };
    let node = NodeDouble::start_on(chain, options, args.listen)
        .await
        .map_err(|error| format!("listen on {}: {error}", args.listen))?;
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{}", node.url()).map_err(|error| error.to_string())?;
    stdout.flush().map_err(|error| error.to_string())?;
    drop(stdout);
    tokio::signal::ctrl_c()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let result = match parse_args() {
        Ok(args) => run(args).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tachyon-node-double: {error}");
            ExitCode::FAILURE
        }
    }
}
