//! Spawned binaries: the publisher, the query host and the node double as
//! separate processes (spec section 13.1).
//!
//! A [`Process`] owns a fresh temporary directory for its configuration and
//! state. Its stdout and stderr go to one file each per run (start or restart),
//! and every run's logs are printed if the test panics while the process is
//! alive in scope. It is started in its own process group, and killing it
//! (explicitly, by [`Process::restart`], or on drop) kills the whole group.
use std::ffi::{OsStr, OsString};
use std::fmt::Write as _;
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// How often waits poll the process and its listeners.
const POLL: Duration = Duration::from_millis(50);

/// The path of binary `name` of this package: `TACHYON_E2E_BIN_<NAME>` if set,
/// else `built` (the `CARGO_BIN_EXE_<name>` the [`crate::binary!`] macro reads).
pub fn locate(name: &str, built: &str) -> PathBuf {
    let variable = format!(
        "TACHYON_E2E_BIN_{}",
        name.to_ascii_uppercase().replace('-', "_")
    );
    let path = std::env::var_os(&variable)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(built));
    assert!(
        path.is_file(),
        "binary {name} not found at {} (set {variable} to override)",
        path.display()
    );
    path
}

/// Lowest port handed out when the pool sits below the ephemeral range.
const POOL_FLOOR: u16 = 20_000;

/// The kernel's ephemeral port range (`/proc/sys/net/ipv4/ip_local_port_range`),
/// or Linux's default when it cannot be read.
fn ephemeral_range() -> (u16, u16) {
    std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .ok()
        .and_then(|text| {
            let mut fields = text.split_whitespace().map(str::parse::<u16>);
            match (fields.next(), fields.next()) {
                (Some(Ok(low)), Some(Ok(high))) if low <= high => Some((low, high)),
                _ => None,
            }
        })
        .unwrap_or((32_768, 60_999))
}

/// The ports reservations come from: the larger gap outside the ephemeral
/// range, so no `:0` bind (by anyone) and no outgoing connection can take one.
fn pool() -> std::ops::Range<u32> {
    let (low, high) = ephemeral_range();
    let below = u32::from(POOL_FLOOR)..u32::from(low);
    let above = (u32::from(high) + 1)..65_536;
    if below.len() >= above.len() {
        below
    } else {
        above
    }
}

/// A per-process starting offset into the pool, so concurrent test processes
/// walk the pool from different places.
/// `TACHYON_E2E_PORT_START` pins it instead (tests use this to make two
/// processes walk the same ports and prove the cross-process lock).
fn process_start() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    if let Some(start) = std::env::var("TACHYON_E2E_PORT_START")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        return start;
    }
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u32(std::process::id());
    if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        hasher.write_u128(now.as_nanos());
    }
    hasher.finish()
}

/// The lock file whose byte `p` stands for port `p`, shared by every test
/// process of this user: `<temp dir>/tachyon-pir-e2e-ports.lock`, or the path
/// in `TACHYON_E2E_PORT_LOCKS` (tests use a private file to watch a few
/// processes contend alone).
const PORT_LOCKS: &str = "tachyon-pir-e2e-ports.lock";

/// The open lock file, or `None` when it cannot be opened (reservations then
/// rely on the bind check alone). It stays open for the life of the process:
/// POSIX record locks are dropped when the process closes any descriptor of
/// the file, and released by the kernel when the process exits.
fn port_locks() -> Option<&'static std::fs::File> {
    static LOCKS: std::sync::OnceLock<Option<std::fs::File>> = std::sync::OnceLock::new();
    LOCKS
        .get_or_init(|| {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o666)
                .open(
                    std::env::var_os("TACHYON_E2E_PORT_LOCKS")
                        .map(PathBuf::from)
                        .unwrap_or_else(|| std::env::temp_dir().join(PORT_LOCKS)),
                )
                .ok()
        })
        .as_ref()
}

/// Takes the cross-process lock of `port` for the rest of this process's
/// life; false if another live process holds it.
fn lock_port(port: u16) -> bool {
    use std::os::fd::AsRawFd;
    let Some(file) = port_locks() else {
        return true;
    };
    // SAFETY: an all-zero `flock` is a valid value; the fields that matter are
    // set below.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as libc::c_short;
    lock.l_whence = libc::SEEK_SET as libc::c_short;
    lock.l_start = libc::off_t::from(port);
    lock.l_len = 1;
    // SAFETY: `file` is open for writing and `lock` is a valid `flock`;
    // F_SETLK does not block.
    unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock) == 0 }
}

/// Whether 127.0.0.1:`port` can be bound with `SO_REUSEADDR`, as the hosts and
/// the harness bind their listeners: it fails while anything listens there (or
/// holds the port without `SO_REUSEADDR`).
///
/// The probe binds without listening. A listening probe would be visible to
/// everyone: `Command::spawn` on another thread copies the descriptor table,
/// so the spawned child briefly holds the probe until its exec closes it, and
/// a bind of the reserved port in that window fails. A bound, non-listening
/// socket blocks no other `SO_REUSEADDR` listener, wherever a copy of it lives.
fn bindable(port: u16) -> bool {
    let Ok(socket) = tokio::net::TcpSocket::new_v4() else {
        return false;
    };
    socket.set_reuseaddr(true).is_ok()
        && socket
            .bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
            .is_ok()
}

/// A free port of 127.0.0.1 for a listener the caller (or a process it spawns)
/// binds later, at a fixed port (`run_registered` refuses port 0).
///
/// - Ports come from outside the kernel's ephemeral range, so nothing that
///   binds `:0` or connects out can take one between reservation and use.
/// - Within this process an atomic counter walks the pool from a random start,
///   so every reservation is distinct until the pool (over 10,000 ports) wraps.
/// - Across processes, a reserved port is locked (a POSIX record lock on byte
///   `port` of `<temp dir>/tachyon-pir-e2e-ports.lock`) until the reserving
///   process exits. Another test process skips it even before anything binds it.
/// - Each candidate is checked by binding it on 127.0.0.1 (without listening,
///   see `bindable`); a port in use by anything else on the machine is
///   skipped.
pub fn free_port() -> u16 {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    static POOL: OnceLock<(std::ops::Range<u32>, u64)> = OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let (pool, start) = POOL.get_or_init(|| (pool(), process_start()));
    let size = pool.len() as u64;
    assert!(size > 0, "no port range outside the ephemeral range");
    for _ in 0..size {
        let offset = start.wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed)) % size;
        let port = u16::try_from(u64::from(pool.start) + offset).expect("pool ports fit u16");
        if lock_port(port) && bindable(port) {
            return port;
        }
    }
    panic!("no free port of 127.0.0.1 in {pool:?}");
}

/// `127.0.0.1:<free port>`; see [`free_port`].
pub fn free_addr() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, free_port()))
}

/// `n` distinct free addresses of 127.0.0.1; see [`free_port`].
pub fn free_addrs(n: usize) -> Vec<SocketAddr> {
    (0..n).map(|_| free_addr()).collect()
}

struct Run {
    stdout: PathBuf,
    stderr: PathBuf,
}

/// One spawned binary with its directory, arguments and logs.
pub struct Process {
    name: String,
    program: PathBuf,
    args: Vec<OsString>,
    envs: Vec<(OsString, OsString)>,
    dir: tempfile::TempDir,
    child: Option<Child>,
    runs: Vec<Run>,
}

impl Process {
    /// A process named `name` (used in file names and messages) running
    /// `program`, with a fresh temporary directory. Nothing runs until
    /// [`Process::start`].
    pub fn new(name: &str, program: impl Into<PathBuf>) -> Self {
        let dir = tempfile::Builder::new()
            .prefix(&format!("{name}-"))
            .tempdir()
            .expect("create the process directory");
        Self {
            name: name.to_owned(),
            program: program.into(),
            args: Vec::new(),
            envs: Vec::new(),
            dir,
            child: None,
            runs: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The process's directory (configs, state and logs live here). It is
    /// removed when the `Process` is dropped.
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// `dir()/relative`.
    pub fn path(&self, relative: &str) -> PathBuf {
        self.dir.path().join(relative)
    }

    /// Creates `dir()/name` and returns it (an absolute path, for a config's
    /// `state_dir`). It survives restarts.
    pub fn state_dir(&self, name: &str) -> PathBuf {
        let path = self.path(name);
        std::fs::create_dir_all(&path).expect("create the state directory");
        path
    }

    /// Writes `value` as pretty JSON to `dir()/file` and returns the path.
    /// Rewrite a config this way, then [`Process::restart`], to restart with a
    /// changed configuration.
    pub fn write_json(&self, file: &str, value: &serde_json::Value) -> PathBuf {
        let path = self.path(file);
        let bytes = serde_json::to_vec_pretty(value).expect("serialize the JSON file");
        std::fs::write(&path, bytes).expect("write the JSON file");
        path
    }

    /// Writes `value` to `dir()/file` and passes it as `flag <path>`
    /// (`--config` for the hosts, `--spec` for the node double).
    pub fn config(&mut self, flag: &str, file: &str, value: &serde_json::Value) -> PathBuf {
        let path = self.write_json(file, value);
        self.args.push(flag.into());
        self.args.push(path.clone().into());
        path
    }

    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.args.push(arg.as_ref().to_owned());
        self
    }

    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        for arg in args {
            self.arg(arg);
        }
        self
    }

    /// An environment variable of every run. `RUST_LOG` defaults to the test's
    /// own `RUST_LOG`, else `info`.
    pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        self.envs
            .push((key.as_ref().to_owned(), value.as_ref().to_owned()));
        self
    }

    /// Starts a run. Panics if one is already running or the spawn fails.
    pub fn start(&mut self) -> &mut Self {
        assert!(self.child.is_none(), "{} is already running", self.name);
        let run = self.runs.len();
        let stdout = self.path(&format!("{}-{run}.stdout.log", self.name));
        let stderr = self.path(&format!("{}-{run}.stderr.log", self.name));
        let mut command = Command::new(&self.program);
        command
            .args(&self.args)
            .current_dir(self.dir.path())
            .env(
                "RUST_LOG",
                std::env::var_os("RUST_LOG").unwrap_or_else(|| "info".into()),
            )
            .envs(self.envs.iter().map(|(key, value)| (key, value)))
            .stdin(Stdio::null())
            .stdout(std::fs::File::create(&stdout).expect("create the stdout log"))
            .stderr(std::fs::File::create(&stderr).expect("create the stderr log"))
            .process_group(0);
        let child = command.spawn().unwrap_or_else(|error| {
            panic!("spawn {} ({}): {error}", self.name, self.program.display())
        });
        self.runs.push(Run { stdout, stderr });
        self.child = Some(child);
        self
    }

    /// The running process's id (also its process group id).
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    /// The exit status, if the current run has exited (it is then reaped).
    pub fn try_status(&mut self) -> Option<ExitStatus> {
        self.child
            .as_mut()?
            .try_wait()
            .expect("query the process status")
    }

    /// Panics with the logs if the current run has exited.
    pub fn assert_running(&mut self) {
        assert!(self.child.is_some(), "{} was never started", self.name);
        if let Some(status) = self.try_status() {
            panic!("{} exited with {status}\n{}", self.name, self.logs());
        }
    }

    /// Kills the process group with `SIGKILL` and reaps the process; a no-op
    /// when nothing runs.
    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            signal_group(child.id(), libc::SIGKILL);
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Sends `SIGTERM` to the process group, as systemd does, and waits up to
    /// `timeout` for the process to exit; then kills it and panics.
    pub async fn terminate(&mut self, timeout: Duration) -> ExitStatus {
        let pid = self.pid().expect("terminate a running process");
        signal_group(pid, libc::SIGTERM);
        self.wait_exit(timeout).await
    }

    /// Kills the current run and starts a new one with the same arguments, the
    /// same directory (configs and state) and fresh log files.
    pub fn restart(&mut self) -> &mut Self {
        self.kill();
        self.start()
    }

    /// Waits up to `timeout` for the current run to exit by itself (it is
    /// killed and the test fails otherwise).
    pub async fn wait_exit(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.try_status() {
                self.child = None;
                return status;
            }
            if Instant::now() >= deadline {
                let logs = self.logs();
                self.kill();
                panic!("{} did not exit within {timeout:?}\n{logs}", self.name);
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Polls `GET <management>/ready` until it answers `200`, failing at once
    /// (with the logs) if the process exits and after `timeout` otherwise.
    /// `management` is `host:port` or an `http://` URL.
    pub async fn wait_ready(&mut self, management: &str, timeout: Duration) {
        let base = if management.contains("://") {
            management.trim_end_matches('/').to_owned()
        } else {
            format!("http://{management}")
        };
        let url = format!("{base}/ready");
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("build the readiness client");
        let deadline = Instant::now() + timeout;
        loop {
            self.assert_running();
            let last = match client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    if status == reqwest::StatusCode::OK {
                        return;
                    }
                    format!("{status}: {body}")
                }
                Err(error) => error.to_string(),
            };
            if Instant::now() >= deadline {
                panic!(
                    "{} not ready at {url} within {timeout:?} (last answer: {last})\n{}",
                    self.name,
                    self.logs()
                );
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Waits up to `timeout` for the current run to print its first stdout line
    /// and returns it, trimmed (the node double prints its URL this way).
    pub async fn wait_stdout_line(&mut self, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            let stdout = self.stdout();
            if let Some((line, _)) = stdout.split_once('\n') {
                return line.trim().to_owned();
            }
            self.assert_running();
            if Instant::now() >= deadline {
                panic!(
                    "{} printed no line within {timeout:?}\n{}",
                    self.name,
                    self.logs()
                );
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// The current (or last) run's stdout.
    pub fn stdout(&self) -> String {
        self.runs
            .last()
            .map(|run| read_lossy(&run.stdout))
            .unwrap_or_default()
    }

    /// The current (or last) run's stderr.
    pub fn stderr(&self) -> String {
        self.runs
            .last()
            .map(|run| read_lossy(&run.stderr))
            .unwrap_or_default()
    }

    /// Every run's stdout and stderr, labelled.
    pub fn logs(&self) -> String {
        let mut out = String::new();
        for (index, run) in self.runs.iter().enumerate() {
            for (stream, path) in [("stdout", &run.stdout), ("stderr", &run.stderr)] {
                let _ = writeln!(
                    out,
                    "----- {} run {index} {stream} ({}) -----\n{}",
                    self.name,
                    path.display(),
                    read_lossy(path)
                );
            }
        }
        out
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if std::thread::panicking() && !self.runs.is_empty() {
            eprintln!("{}", self.logs());
        }
        self.kill();
    }
}

fn read_lossy(path: &Path) -> String {
    std::fs::read(path)
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_default()
}

/// Sends `signal` to the process group led by `pid`.
fn signal_group(pid: u32, signal: libc::c_int) {
    let pgid = libc::pid_t::try_from(pid).expect("a pid fits pid_t");
    // SAFETY: kill(2) with a negative pid signals that process group; it has no
    // memory-safety preconditions.
    unsafe {
        libc::kill(-pgid, signal);
    }
}

/// Whether any process of group `pgid` still exists (zombies included).
pub fn group_exists(pgid: u32) -> bool {
    let pgid = libc::pid_t::try_from(pgid).expect("a pid fits pid_t");
    // SAFETY: signal 0 only checks for existence and permission.
    unsafe { libc::kill(-pgid, 0) == 0 }
}
