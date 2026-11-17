//! Process helpers for tests that run the built binary.
#![allow(dead_code)]
use serde_json::Value;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::{Duration, Instant},
};
use tokio::process::{Child, Command};

pub const BINARY: &str = env!("CARGO_BIN_EXE_tachyon-pir-publisher");

/// A loopback address whose port was free a moment ago. `run_registered`
/// refuses port 0, so the port is chosen here and handed over (a tolerated
/// race, as in wallet-pir's harness).
pub fn free() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

/// Write `config` as JSON to `dir/name`.
pub fn write_config(dir: &Path, name: &str, config: &Value) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, serde_json::to_vec_pretty(config).unwrap()).unwrap();
    path
}

/// The binary with `args`, its output captured.
pub fn run_to_completion(args: &[&std::ffi::OsStr]) -> std::process::Output {
    std::process::Command::new(BINARY)
        .args(args)
        .output()
        .unwrap()
}

/// A running publisher whose stdout and stderr go to one log file.
pub struct Publisher {
    child: Child,
    log: PathBuf,
    pub management: SocketAddr,
}

impl Publisher {
    pub fn spawn(config: &Path, management: SocketAddr, log: PathBuf) -> Self {
        let file = std::fs::File::create(&log).unwrap();
        let child = Command::new(BINARY)
            .arg("--config")
            .arg(config)
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
            )
            .stdin(Stdio::null())
            .stdout(file.try_clone().unwrap())
            .stderr(file)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        Self {
            child,
            log,
            management,
        }
    }

    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Poll `GET /ready` on the management listener every 50 ms until it
    /// answers 200, failing fast with the log if the process exits.
    pub async fn ready(&mut self, within: Duration) -> Value {
        let client = client();
        let url = format!("http://{}/ready", self.management);
        let deadline = Instant::now() + within;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!(
                    "publisher exited with {status} before ready:\n{}",
                    self.log()
                );
            }
            let mut last = None;
            if let Ok(response) = client.get(&url).send().await {
                let ready = response.status().is_success();
                let report: Value = response.json().await.unwrap();
                if ready {
                    return report;
                }
                last = Some(report);
            }
            assert!(
                Instant::now() < deadline,
                "publisher not ready within {within:?} (last report {last:?}):\n{}",
                self.log()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait for the process to exit by itself.
    pub async fn exit(&mut self, within: Duration) -> ExitStatus {
        match tokio::time::timeout(within, self.child.wait()).await {
            Ok(status) => status.unwrap(),
            Err(_) => panic!("publisher still running after {within:?}:\n{}", self.log()),
        }
    }

    /// Send `signal` and wait for the exit.
    pub async fn stop(&mut self, signal: i32, within: Duration) -> ExitStatus {
        let pid = self.child.id().expect("publisher still running");
        // SAFETY: `kill` takes no pointers; the pid is our own live child.
        assert_eq!(unsafe { libc::kill(pid as i32, signal) }, 0);
        self.exit(within).await
    }
}

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

/// Wait until nothing listens on `address` any more.
pub async fn released(address: SocketAddr) {
    for _ in 0..100 {
        if std::net::TcpListener::bind(address).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{address} is still bound");
}
