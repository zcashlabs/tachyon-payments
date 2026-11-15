//! The node double (`tachyon-fixtures`), in process or as a separate process.
//!
//! Both serve the chain a test authored, so the test holds the ground truth its
//! oracle is computed from: [`NodeDouble::chain`] in process, or
//! [`SpawnedNode::chain`] (the same [`ChainSpec`] built locally) for the binary.
use crate::process::{Process, free_addr};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tachyon_fixtures::{ChainSpec, NodeDouble, NodeOptions, SyntheticChain};

/// Serves `chain` in process on an ephemeral port of 127.0.0.1, without
/// authentication. Its URL is [`NodeDouble::url`].
pub async fn start_node(chain: SyntheticChain) -> NodeDouble {
    NodeDouble::start(chain, NodeOptions::default())
        .await
        .expect("start the in-process node double")
}

/// The `tachyon-node-double` binary, spawned from a chain spec.
///
/// It listens on a reserved fixed port, so its URL survives
/// [`SpawnedNode::restart`]. A restart rebuilds the chain from the spec: control
/// edits made before it are gone.
pub struct SpawnedNode {
    process: Process,
    spec: ChainSpec,
    addr: SocketAddr,
    url: String,
}

impl SpawnedNode {
    /// Spawns `binary` (normally `binary!("tachyon-node-double-test")`) serving
    /// `spec`, and waits up to `timeout` for it to print its URL.
    pub async fn spawn(binary: impl Into<PathBuf>, spec: &ChainSpec, timeout: Duration) -> Self {
        let mut process = Process::new("node-double", binary);
        let value = serde_json::to_value(spec).expect("serialize the chain spec");
        process.config("--spec", "chain-spec.json", &value);
        let addr = free_addr();
        process.arg("--listen").arg(addr.to_string());
        process.start();
        let url = process.wait_stdout_line(timeout).await;
        assert_eq!(url, format!("http://{addr}"), "{}", process.logs());
        Self {
            process,
            spec: spec.clone(),
            addr,
            url,
        }
    }

    /// The JSON-RPC URL, `http://127.0.0.1:<port>` (the publisher's
    /// `chain.rpc_url`).
    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn spec(&self) -> &ChainSpec {
        &self.spec
    }

    /// The chain the binary started with, built in this process.
    pub fn chain(&self) -> SyntheticChain {
        self.spec.build().expect("the spec builds")
    }

    pub fn process(&mut self) -> &mut Process {
        &mut self.process
    }

    /// Kills the binary and starts it again on the same port.
    pub async fn restart(&mut self, timeout: Duration) {
        self.process.restart();
        let url = self.process.wait_stdout_line(timeout).await;
        assert_eq!(url, self.url, "{}", self.process.logs());
    }

    /// `GET /control/state`.
    pub async fn state(&self) -> Value {
        let url = format!("{}/control/state", self.url);
        crate::client::get_json(&crate::client::plain(), &url, reqwest::StatusCode::OK).await
    }

    /// `POST /control/<endpoint>` with `body`, returning the node state it
    /// answers (see the fixtures README for the endpoints).
    pub async fn control(&self, endpoint: &str, body: Value) -> Value {
        let url = format!("{}/control/{endpoint}", self.url);
        crate::client::post_json(
            &crate::client::plain(),
            &url,
            &body,
            reqwest::StatusCode::OK,
        )
        .await
    }
}

/// One JSON-RPC call to a node at `url`, returning `result`; panics on a
/// transport failure or a JSON-RPC error.
pub async fn rpc(url: &str, method: &str, params: Value) -> Value {
    let body = json!({"jsonrpc": "1.0", "id": "e2e", "method": method, "params": params});
    let mut reply: Value =
        crate::client::post_json(&crate::client::plain(), url, &body, reqwest::StatusCode::OK)
            .await;
    assert!(
        reply["error"].is_null(),
        "{method} at {url} failed: {}",
        reply["error"]
    );
    reply["result"].take()
}
