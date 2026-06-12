//! WP-5: acceptance suite A1–A8 + A10 harness (build-spec-r0 §7).
//!
//! FROZEN laws:
//! - Probe-truthful: every item SKIPs loudly with a reason when its probe
//!   fails (e.g. no crictl on macOS) — never silent, never green-by-skip
//!   without the skip being visible in output.
//! - Tests live in tests/ and drive the release `lightr-cri` bin over a UDS
//!   with `crictl` (pinned version, see ci/).
//! - A8 (crash-only): kill -9 the server mid-suite, restart, state must
//!   re-derive identically; a Running container's process survives.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Probe: is `bin` runnable in PATH?
pub fn have(bin: &str) -> bool {
    std::process::Command::new(bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Loud skip helper — prints the frozen skip format.
///
/// Fail-closed law: when `LIGHTR_CRI_REQUIRE_PROBES=1` (set in Linux CI,
/// where crictl/critest are mandatory), a missing probe is a FAILURE, not a
/// skip — otherwise the whole conformance suite could evaporate into green
/// skips (cold-critic finding 2026-06-11).
pub fn skip(item: &str, reason: &str) {
    if std::env::var("LIGHTR_CRI_REQUIRE_PROBES").as_deref() == Ok("1") {
        panic!("PROBE REQUIRED but missing — {item}: {reason} (LIGHTR_CRI_REQUIRE_PROBES=1)");
    }
    eprintln!("SKIP {item}: {reason} (probe-truthful; see build-spec-r0 §7)");
}

/// Locate the `lightr-cri` binary.
/// Checks env LIGHTR_CRI_BIN, then target/release/lightr-cri, then target/debug/lightr-cri.
pub fn find_server_bin() -> Option<PathBuf> {
    // env override
    if let Ok(v) = std::env::var("LIGHTR_CRI_BIN") {
        let p = PathBuf::from(v);
        if p.exists() {
            return Some(p);
        }
    }
    // workspace root heuristic: walk up from CARGO_MANIFEST_DIR or cwd
    let start = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
    let mut cur = start.as_path();
    loop {
        let release = cur.join("target/release/lightr-cri");
        if release.exists() {
            return Some(release);
        }
        let debug = cur.join("target/debug/lightr-cri");
        if debug.exists() {
            return Some(debug);
        }
        match cur.parent() {
            Some(p) => cur = p,
            None => break,
        }
    }
    None
}

/// Server harness: spawns the binary with `--socket <tmpdir>/cri.sock --state <tmpdir>/state`.
/// Waits up to 5 s (50 ms poll) for the socket file to appear.
/// Kills + waits the child on drop.  Cleans up the tmpdir on drop.
pub struct ServerHandle {
    pub child: Child,
    pub socket: PathBuf,
    pub state_dir: PathBuf,
    /// The root temp directory — removed on drop.
    pub tmpdir: PathBuf,
    /// Whether to remove tmpdir on drop (false for A8 mid-test hand-off).
    pub keep_tmpdir: bool,
}

impl ServerHandle {
    /// Spawn the server at `bin` using a fresh tmpdir for socket + state.
    pub fn spawn(bin: &Path) -> std::io::Result<Self> {
        // Unique subdir under the system temp dir.
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let tmpdir = std::env::temp_dir().join(format!("lightr-cri-test-{id}"));
        std::fs::create_dir_all(&tmpdir)?;
        let socket = tmpdir.join("cri.sock");
        let state_dir = tmpdir.join("state");
        std::fs::create_dir_all(&state_dir)?;
        Self::spawn_inner(bin, socket, state_dir, tmpdir, false)
    }

    /// Restart the server on an *existing* socket+state (A8 pattern).
    /// The caller owns `tmpdir` and must keep it alive; `keep_tmpdir=true` prevents
    /// the new handle from removing it on drop.
    pub fn restart(
        bin: &Path,
        socket: PathBuf,
        state_dir: PathBuf,
        tmpdir: PathBuf,
    ) -> std::io::Result<Self> {
        // Remove stale socket if present.
        let _ = std::fs::remove_file(&socket);
        Self::spawn_inner(bin, socket, state_dir, tmpdir, true)
    }

    fn spawn_inner(
        bin: &Path,
        socket: PathBuf,
        state_dir: PathBuf,
        tmpdir: PathBuf,
        keep_tmpdir: bool,
    ) -> std::io::Result<Self> {
        let child = Command::new(bin)
            .arg("--socket")
            .arg(&socket)
            .arg("--state")
            .arg(&state_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?;

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if socket.exists() {
                return Ok(Self {
                    child,
                    socket,
                    state_dir,
                    tmpdir,
                    keep_tmpdir,
                });
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let mut child = child;
        let _ = child.kill();
        let _ = child.wait();
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "server socket did not appear within 5 s",
        ))
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !self.keep_tmpdir {
            let _ = std::fs::remove_dir_all(&self.tmpdir);
        }
    }
}

/// crictl invocation helper.
/// Builds: `crictl --runtime-endpoint unix://<sock> --image-endpoint unix://<sock> <args>`
/// Captures stdout+stderr; returns the Command output.
pub fn crictl(sock: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    let ep = format!("unix://{}", sock.display());
    Command::new("crictl")
        .arg("--runtime-endpoint")
        .arg(&ep)
        .arg("--image-endpoint")
        .arg(&ep)
        .args(args)
        .output()
}

/// Probe: is `cap` set in the effective capability bitmask?
///
/// Uses `/proc/self/status` (CapEff line) on Linux; always returns `false`
/// on other platforms.  Errors are treated as "not available".
pub fn have_cap_sys_admin() -> bool {
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
    #[cfg(target_os = "linux")]
    {
        use std::io::BufRead;
        let Ok(f) = std::fs::File::open("/proc/self/status") else {
            return false;
        };
        for line in std::io::BufReader::new(f).lines().map_while(Result::ok) {
            if let Some(hex) = line.strip_prefix("CapEff:\t") {
                if let Ok(bits) = u64::from_str_radix(hex.trim(), 16) {
                    // CAP_SYS_ADMIN = 21, CAP_NET_ADMIN = 12
                    let sys_admin = (bits >> 21) & 1 == 1;
                    let net_admin = (bits >> 12) & 1 == 1;
                    return sys_admin && net_admin;
                }
            }
        }
        false
    }
}

/// A10 harness: builds a `critest` Command reading the skip list from
/// `skips_file` (lines; `#` comments; the regex is everything before any ` #`).
pub fn critest_cmd(sock: &Path, skips_file: &Path) -> Command {
    let ep = format!("unix://{}", sock.display());

    // Parse skips file: strip comments, collect non-empty regex lines.
    let skip_regexes: Vec<String> = std::fs::read_to_string(skips_file)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            // strip inline comment (space-hash boundary)
            let trimmed = match line.split_once(" #") {
                Some((pre, _)) => pre.trim(),
                None => line.trim(),
            };
            // skip blank lines and pure comment lines
            if trimmed.is_empty() || trimmed.starts_with('#') {
                None
            } else {
                Some(trimmed.to_owned())
            }
        })
        .collect();

    let mut cmd = Command::new("critest");
    cmd.arg("--runtime-endpoint").arg(&ep);
    if !skip_regexes.is_empty() {
        let joined = skip_regexes.join("|");
        cmd.arg("--ginkgo.skip").arg(joined);
    }
    cmd
}
