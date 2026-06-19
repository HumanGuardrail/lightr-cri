//! The honest fake backend (build-spec-r0 §4) — WP-1.
//!
//! Laws (FROZEN, transcribe — do not reinterpret):
//! - File-backed: state root (env `LIGHTR_CRI_STATE`, default
//!   `$TMPDIR/lightr-cri-fake`): `sandboxes/<id>.json`, `containers/<id>.json`,
//!   `images/<name-hash>.json`. Atomic write law: tmp + rename, fsync file.
//! - In-memory index is a CACHE rebuilt from disk at `open`. Crash-recovery law:
//!   survived processes are re-adopted; a kill between spawn and pid-persist
//!   recovers as Exited/-1 'lost-start-window'; exit codes of containers that
//!   exit while no listener is alive recover as -1 'lost-exit-reaped-elsewhere'
//!   (fidelity limit of the fake; the real backend's supervisor closes it).
//! - Execution is REAL: `start_container` spawns the configured command as a
//!   plain host process (no isolation); `exec_sync` really runs and captures.
//! - Images are fake CAS records: `pull_image` synthesizes PulledImage with a
//!   BLAKE3 root over the ref string, instantly (lazy law rehearsed); refuse
//!   unparseable refs with InvalidArgument.
//! - Stats: real /proc/<pid>-based on Linux; zeroed-with-timestamp elsewhere
//!   (probe-truthful).
//! - v1.1 (WP-A): start_container TEEs stdout/stderr to CRI log file at
//!   `sandbox.log_directory + "/" + container.log_path` (§C format);
//!   open_exec spawns cmd in container context (pipes or pty); open_attach
//!   returns held stdio handles from a side-table.
//! - v1.1 (WP-D): run_sandbox wires CNI (lightr-cri-net) when CNI available
//!   and host_network=false; remove_sandbox tears down; open_exec setns.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use lightr_cri_backend::*;

// ---------------------------------------------------------------------------
// Internal persisted record types (extend ContainerStatus with pid)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct SandboxRecord {
    pub id: SandboxId,
    pub config: SandboxConfig,
    pub state: SandboxState,
    pub created_at_nanos: i64,
    /// Host-routable pod IP assigned by CNI ADD (§D). None when host_network or CNI unavailable.
    #[serde(default)]
    pub ip: Option<String>,
    /// Path of the pinned network namespace bind-mount (§D). None when host_network or CNI unavailable.
    #[serde(default)]
    pub netns_path: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ContainerRecord {
    pub id: ContainerId,
    pub sandbox: SandboxId,
    pub config: ContainerConfig,
    pub state: ContainerState,
    pub created_at_nanos: i64,
    pub started_at_nanos: i64,
    pub finished_at_nanos: i64,
    pub exit_code: i32,
    pub reason: String,
    pub message: String,
    /// PID of the spawned host process; 0 = not started / exited
    pub pid: u32,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ImageDiskRecord {
    pub id: String,
    pub ref_name: String,
    pub size: u64,
}

// ---------------------------------------------------------------------------
// Stdio handle side-table (not persisted — fake limitation: attach unavailable
// after restart; see open_attach implementation comment).
// ---------------------------------------------------------------------------

/// Held stdio for a running container. The tty case keeps one pty master fd
/// (cloned for each attach call). The pipe case holds the read-end of stdout
/// and stderr, plus write-end of stdin (None if container.stdin=false).
///
/// NOT serialized. These fds are valid only in the current process.
struct ContainerIo {
    /// If the container was started with tty=true, the pty master fd.
    pty_master: Option<std::fs::File>,
    /// Pipe-mode: read end of the process stdout pipe.
    stdout_rd: Option<std::fs::File>,
    /// Pipe-mode: read end of the process stderr pipe.
    stderr_rd: Option<std::fs::File>,
    /// Write end of the process stdin pipe (if config.stdin=true).
    stdin_wr: Option<std::fs::File>,
    /// Pipe-mode: output fan-out. The single tee thread per stream reads the
    /// container's read-end ONCE and (a) writes the CRI log and (b) writes the
    /// raw bytes to every registered attacher sink here. `open_attach` registers
    /// a fresh pipe write-end; the tee prunes any sink whose pipe is broken
    /// (attacher gone). `None` for tty containers.
    fanout: Option<Arc<FanOut>>,
}

/// Output fan-out shared between the per-stream tee threads (writers) and
/// `open_attach` (registers sinks). Holds the write-ends of one OS pipe per
/// live attacher, split by stream so the CRI streaming server can deliver
/// stdout and stderr separately. The tee thread is the SOLE reader of the
/// container's output; it copies raw bytes to each sink, so there is no second
/// reader racing the log. Sinks that fail to write (attacher detached, pipe
/// closed) are dropped on the next write — bounded by the number of live
/// attachers, no leak.
#[derive(Default)]
struct FanOut {
    /// Write-ends of attacher pipes for the stdout stream.
    stdout_sinks: Mutex<Vec<std::fs::File>>,
    /// Write-ends of attacher pipes for the stderr stream.
    stderr_sinks: Mutex<Vec<std::fs::File>>,
}

impl FanOut {
    /// Write `data` to every live sink for `stream_name` ("stdout"/"stderr"),
    /// pruning any sink whose pipe is broken (attacher gone). Single-reader
    /// fan-out: only the tee thread calls this.
    fn broadcast(&self, stream_name: &str, data: &[u8]) {
        use std::io::Write;
        let sinks = if stream_name == "stderr" {
            &self.stderr_sinks
        } else {
            &self.stdout_sinks
        };
        let mut guard = sinks.lock().unwrap();
        guard.retain_mut(|w| w.write_all(data).and_then(|()| w.flush()).is_ok());
    }
}

/// Create an OS pipe, returning (read_end, write_end) as std::fs::File.
#[cfg(unix)]
fn make_pipe() -> std::io::Result<(std::fs::File, std::fs::File)> {
    use std::os::unix::io::FromRawFd;
    let mut fds = [0i32; 2];
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let r = unsafe { std::fs::File::from_raw_fd(fds[0]) };
    let w = unsafe { std::fs::File::from_raw_fd(fds[1]) };
    Ok((r, w))
}

// ---------------------------------------------------------------------------
// ID generation
// ---------------------------------------------------------------------------

static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn new_id(prefix: &str) -> String {
    let n = now_nanos();
    let c = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{prefix}{n}-{c}")
}

// ---------------------------------------------------------------------------
// Atomic write helper
// ---------------------------------------------------------------------------

fn atomic_write(dir: &Path, filename: &str, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp_name = format!(".tmp-{pid}-{nanos}");
    let tmp_path = dir.join(&tmp_name);
    let final_path = dir.join(filename);

    {
        let mut f = fs::File::create(&tmp_path)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

fn atomic_write_json<T: serde::Serialize>(dir: &Path, filename: &str, value: &T) -> Result<()> {
    let data =
        serde_json::to_vec(value).map_err(|e| BackendError::Internal(format!("serialize: {e}")))?;
    atomic_write(dir, filename, &data).map_err(BackendError::Io)
}

// ---------------------------------------------------------------------------
// In-memory cache
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Cache {
    sandboxes: BTreeMap<SandboxId, SandboxRecord>,
    containers: BTreeMap<ContainerId, ContainerRecord>,
    images: BTreeMap<String, ImageDiskRecord>, // key = filename stem (image id)
}

// ---------------------------------------------------------------------------
// FakeBackend
// ---------------------------------------------------------------------------

pub struct FakeBackend {
    state_root: PathBuf,
    sandboxes_dir: PathBuf,
    containers_dir: PathBuf,
    images_dir: PathBuf,
    cache: Arc<Mutex<Cache>>,
    /// Side-table: held stdio for containers spawned in this process.
    /// Keyed by ContainerId; entries removed when the container exits.
    /// NOT behind the same Mutex as the cache to avoid lock ordering issues.
    io_table: Arc<Mutex<BTreeMap<ContainerId, ContainerIo>>>,
}

impl FakeBackend {
    /// Poll the cache until the container's detached reaper thread has recorded
    /// the terminal (Exited) state, or `timeout` elapses. Returns true once the
    /// container is no longer Running. The reaper (start_container) owns the real
    /// exit_code; we only wait for it to land so stop is synchronous to callers.
    fn wait_until_exited(&self, id: &ContainerId, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            {
                let cache = self.cache.lock().unwrap();
                match cache.containers.get(id) {
                    Some(r) if r.state != ContainerState::Running => return true,
                    None => return true, // removed underneath us
                    _ => {}
                }
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Open (or create) the state root and rebuild the cache from disk.
    pub fn open(state_root: &Path) -> std::io::Result<Self> {
        let sandboxes_dir = state_root.join("sandboxes");
        let containers_dir = state_root.join("containers");
        let images_dir = state_root.join("images");

        fs::create_dir_all(&sandboxes_dir)?;
        fs::create_dir_all(&containers_dir)?;
        fs::create_dir_all(&images_dir)?;

        let mut cache = Cache::default();

        // Rebuild sandboxes from disk
        for entry in fs::read_dir(&sandboxes_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            if let Ok(data) = fs::read(&path) {
                if let Ok(rec) = serde_json::from_slice::<SandboxRecord>(&data) {
                    cache.sandboxes.insert(rec.id.clone(), rec);
                }
            }
        }

        // Rebuild containers from disk — apply pid-alive check for Running containers
        for entry in fs::read_dir(&containers_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            if let Ok(data) = fs::read(&path) {
                if let Ok(mut rec) = serde_json::from_slice::<ContainerRecord>(&data) {
                    if rec.state == ContainerState::Running {
                        // LEAD DECISION: if kill(pid, 0) says alive → still Running;
                        // if dead and no recorded exit → state=Exited, exit_code=-1
                        if rec.pid > 0 && !pid_alive(rec.pid) {
                            rec.state = ContainerState::Exited;
                            rec.exit_code = -1;
                            rec.reason = "lost-exit-reaped-elsewhere".to_string();
                            rec.finished_at_nanos = now_nanos();
                            // persist the corrected record
                            let filename = format!("{}.json", rec.id.0);
                            if let Ok(data2) = serde_json::to_vec(&rec) {
                                let _ = atomic_write(&containers_dir, &filename, &data2);
                            }
                        } else if rec.pid == 0 {
                            // Lost-start-window: Running persisted but pid never
                            // written (crash between spawn and pid-persist).
                            rec.state = ContainerState::Exited;
                            rec.exit_code = -1;
                            rec.reason = "lost-start-window".to_string();
                            rec.finished_at_nanos = now_nanos();
                            let filename = format!("{}.json", rec.id.0);
                            if let Ok(data2) = serde_json::to_vec(&rec) {
                                let _ = atomic_write(&containers_dir, &filename, &data2);
                            }
                        }
                    }
                    cache.containers.insert(rec.id.clone(), rec);
                }
            }
        }

        // Rebuild images from disk
        for entry in fs::read_dir(&images_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            if let Ok(data) = fs::read(&path) {
                if let Ok(rec) = serde_json::from_slice::<ImageDiskRecord>(&data) {
                    cache.images.insert(rec.id.clone(), rec);
                }
            }
        }

        Ok(FakeBackend {
            state_root: state_root.to_path_buf(),
            sandboxes_dir,
            containers_dir,
            images_dir,
            cache: Arc::new(Mutex::new(cache)),
            io_table: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }
}

// ---------------------------------------------------------------------------
// Platform helpers
// ---------------------------------------------------------------------------

fn pid_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(target_os = "linux")]
fn read_proc_stats(pid: u32) -> (u64, u64) {
    // cpu_core_nanos from /proc/<pid>/stat fields utime+stime (in CLK_TCK ticks)
    let clk_tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) as u64 };
    let nanos_per_tick = 1_000_000_000u64.checked_div(clk_tck).unwrap_or(0);
    let cpu_nanos = if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
        // fields are space-separated; utime=14th field (0-indexed=13), stime=15th (0-indexed=14)
        let fields: Vec<&str> = stat.split_whitespace().collect();
        if fields.len() > 14 {
            let utime: u64 = fields[13].parse().unwrap_or(0);
            let stime: u64 = fields[14].parse().unwrap_or(0);
            (utime + stime) * nanos_per_tick
        } else {
            0
        }
    } else {
        0
    };

    // memory VmRSS from /proc/<pid>/status
    let mem_bytes = if let Ok(status) = fs::read_to_string(format!("/proc/{pid}/status")) {
        let mut rss_kb: u64 = 0;
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    rss_kb = parts[1].parse().unwrap_or(0);
                }
                break;
            }
        }
        rss_kb * 1024
    } else {
        0
    };

    (cpu_nanos, mem_bytes)
}

#[cfg(not(target_os = "linux"))]
fn read_proc_stats(_pid: u32) -> (u64, u64) {
    (0, 0)
}

// ---------------------------------------------------------------------------
// Filter helpers
// ---------------------------------------------------------------------------

fn sandbox_matches(rec: &SandboxRecord, filter: &SandboxFilter) -> bool {
    if let Some(id) = &filter.id {
        if &rec.id != id {
            return false;
        }
    }
    if let Some(state) = &filter.state {
        if &rec.state != state {
            return false;
        }
    }
    for (k, v) in &filter.label_selector {
        if rec.config.labels.get(k).map(String::as_str) != Some(v.as_str()) {
            return false;
        }
    }
    true
}

fn container_matches(rec: &ContainerRecord, filter: &ContainerFilter) -> bool {
    if let Some(id) = &filter.id {
        if &rec.id != id {
            return false;
        }
    }
    if let Some(sb) = &filter.sandbox {
        if &rec.sandbox != sb {
            return false;
        }
    }
    if let Some(state) = &filter.state {
        if &rec.state != state {
            return false;
        }
    }
    for (k, v) in &filter.label_selector {
        if rec.config.labels.get(k).map(String::as_str) != Some(v.as_str()) {
            return false;
        }
    }
    true
}

fn rec_to_status(rec: &ContainerRecord, sandbox_log_dir: &str) -> ContainerStatus {
    // critest/CRI normalization (G1): for a terminal (Exited) container the
    // `reason` MUST be exactly "Completed" (exit_code == 0) or "Error" (any
    // non-zero exit, including signal-kill). The human-readable detail recorded
    // internally (e.g. "killed-by-signal-15", "stopped", "lost-exit-…") is
    // preserved in `message` so no information is lost. Because BOTH
    // container_status and list_containers read through this single function,
    // the two paths are guaranteed to agree on the normalized reason.
    let (reason, message) = if rec.state == ContainerState::Exited {
        let normalized = if rec.exit_code == 0 {
            "Completed"
        } else {
            "Error"
        };
        // Keep the raw reason as the human detail when no explicit message was set.
        let message = if rec.message.is_empty() {
            rec.reason.clone()
        } else {
            rec.message.clone()
        };
        (normalized.to_string(), message)
    } else {
        (rec.reason.clone(), rec.message.clone())
    };
    // CRI: ContainerStatus.log_path is the ABSOLUTE path (sandbox log_directory
    // joined with the container's relative ContainerConfig.log_path). crictl logs
    // lstat()s this path directly, so a relative value yields "no such file".
    // The record keeps the relative log_path (open_cri_log joins it itself).
    let mut config = rec.config.clone();
    if !config.log_path.is_empty() && !sandbox_log_dir.is_empty() {
        config.log_path = std::path::Path::new(sandbox_log_dir)
            .join(&config.log_path)
            .to_string_lossy()
            .into_owned();
    }
    ContainerStatus {
        id: rec.id.clone(),
        sandbox: rec.sandbox.clone(),
        config,
        state: rec.state,
        created_at_nanos: rec.created_at_nanos,
        started_at_nanos: rec.started_at_nanos,
        finished_at_nanos: rec.finished_at_nanos,
        exit_code: rec.exit_code,
        reason,
        message,
    }
}

fn sandbox_rec_to_status(rec: &SandboxRecord) -> SandboxStatus {
    SandboxStatus {
        id: rec.id.clone(),
        config: rec.config.clone(),
        state: rec.state,
        created_at_nanos: rec.created_at_nanos,
        ip: rec.ip.clone(),
        netns_path: rec.netns_path.clone(),
    }
}

// ---------------------------------------------------------------------------
// CRI log tee helpers (§C)
// ---------------------------------------------------------------------------

/// Format one CRI log line: `<RFC3339Nano> <stdout|stderr> <F|P> <data>\n`
/// F = full line (data ends with '\n'); P = partial (no trailing newline).
fn cri_log_line(stream: &str, data: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let ts = {
        // RFC3339 with nanosecond precision
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let nanos = now.subsec_nanos();
        // Compute UTC datetime from epoch seconds
        let (y, mo, d, h, mi, s) = epoch_to_ymd_hms(secs);
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
            y, mo, d, h, mi, s, nanos
        )
    };
    let tag = if data.ends_with(b"\n") { "F" } else { "P" };
    let mut out = Vec::with_capacity(ts.len() + 3 + stream.len() + 1 + data.len() + 1);
    write!(out, "{} {} {} ", ts, stream, tag).unwrap();
    out.extend_from_slice(data);
    if !data.ends_with(b"\n") {
        out.push(b'\n');
    }
    out
}

/// Minimal UTC decomposition from Unix epoch (no external dep).
fn epoch_to_ymd_hms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86400;
    // Shift to 1 March 2000 epoch for easier leap-year math (Rata Die variant)
    let z = days + 719468;
    let era = z / 146097;
    let doe = z % 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    (y as u32, mo as u32, d as u32, h as u32, m as u32, s as u32)
}

/// Open (create-or-append) the CRI log file at `log_dir/log_path`.
/// Creates parent dirs. Creates an empty file if it doesn't exist yet.
fn open_cri_log(log_dir: &str, log_path: &str) -> std::io::Result<Option<fs::File>> {
    if log_dir.is_empty() || log_path.is_empty() {
        return Ok(None);
    }
    let path = PathBuf::from(log_dir).join(log_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    Ok(Some(f))
}

/// Spawn a tee thread that reads from `reader` and writes CRI-formatted lines
/// to the log file. The log file handle is Arc<Mutex<>> so multiple streams
/// can interleave safely.
///
/// This is the SINGLE reader of the container's output for this stream. When a
/// `fanout` is supplied it ALSO copies the raw line bytes (byte-for-byte, the
/// same bytes that would appear on the container's fd) to every registered
/// attacher sink, so attach is purely additive and never introduces a second
/// reader racing the log. The CRI log write path is unchanged and remains
/// byte-identical regardless of whether attachers are present.
fn spawn_tee_thread(
    stream_name: &'static str,
    reader: std::fs::File,
    log: Arc<Mutex<Option<fs::File>>>,
    fanout: Option<Arc<FanOut>>,
) {
    std::thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let br = BufReader::new(reader);
        for line in br.split(b'\n') {
            match line {
                Ok(mut data) => {
                    // split() strips the delimiter — re-add newline for F tag
                    data.push(b'\n');
                    // (a) CRI log — byte-identical to pre-fanout behavior.
                    let formatted = cri_log_line(stream_name, &data);
                    {
                        let mut lg = log.lock().unwrap();
                        if let Some(f) = lg.as_mut() {
                            use std::io::Write;
                            let _ = f.write_all(&formatted);
                        }
                    }
                    // (b) live attachers — receive the raw container bytes.
                    if let Some(fo) = &fanout {
                        fo.broadcast(stream_name, &data);
                    }
                }
                Err(_) => break,
            }
        }
    });
}

// ---------------------------------------------------------------------------
// pty helpers
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn dup_file(f: &std::fs::File) -> std::io::Result<std::fs::File> {
    use std::os::unix::io::{AsRawFd, FromRawFd};
    let fd = unsafe { libc::dup(f.as_raw_fd()) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

// ---------------------------------------------------------------------------
// ExitWaiter implementations
// ---------------------------------------------------------------------------

/// Waiter for a child spawned via std::process::Child.
struct ChildWaiter {
    child: std::process::Child,
}

impl ExitWaiter for ChildWaiter {
    fn wait(mut self: Box<Self>) -> Result<i32> {
        let status = self
            .child
            .wait()
            .map_err(|e| BackendError::Internal(format!("wait: {e}")))?;
        Ok(exit_code_from_status(&status))
    }
}

// ---------------------------------------------------------------------------
// CriBackend impl
// ---------------------------------------------------------------------------

impl CriBackend for FakeBackend {
    // ---- sandbox plane ----

    fn run_sandbox(&self, cfg: SandboxConfig) -> Result<SandboxId> {
        let id = SandboxId(new_id("sb-"));

        // §D: if not host_network and CNI is available, create netns + invoke CNI ADD.
        // On macOS / unprivileged: cni_available() returns None → host-network fallback.
        let (ip, netns_path) = if !cfg.host_network {
            if let Some(cni_env) = lightr_cri_net::cni_available() {
                match cni_setup(&id, &cni_env, &cfg.port_mappings) {
                    Ok((resolved_ip, ns_path)) => (resolved_ip, Some(ns_path)),
                    Err(e) => {
                        return Err(BackendError::Internal(format!(
                            "CNI setup for sandbox {}: {e}",
                            id.0
                        )));
                    }
                }
            } else {
                // CNI unavailable (macOS / unprivileged) — host-network fallback (probe-truthful).
                (None, None)
            }
        } else {
            // host_network=true: no netns, no IP.
            (None, None)
        };

        let rec = SandboxRecord {
            id: id.clone(),
            config: cfg,
            state: SandboxState::Ready,
            created_at_nanos: now_nanos(),
            ip,
            netns_path,
        };
        let filename = format!("{}.json", id.0);
        // Crash-only §D: persist BEFORE returning.
        atomic_write_json(&self.sandboxes_dir, &filename, &rec)?;
        let mut cache = self.cache.lock().unwrap();
        cache.sandboxes.insert(id.clone(), rec);
        Ok(id)
    }

    fn stop_sandbox(&self, id: &SandboxId) -> Result<()> {
        let mut cache = self.cache.lock().unwrap();
        let rec = match cache.sandboxes.get_mut(id) {
            Some(r) => r,
            None => return Ok(()), // idempotent: already gone
        };
        if rec.state == SandboxState::NotReady {
            return Ok(()); // already stopped
        }
        rec.state = SandboxState::NotReady;
        let rec_clone = rec.clone();
        drop(cache);
        let filename = format!("{}.json", id.0);
        atomic_write_json(&self.sandboxes_dir, &filename, &rec_clone)?;
        Ok(())
    }

    fn remove_sandbox(&self, id: &SandboxId) -> Result<()> {
        // First stop it (idempotent)
        self.stop_sandbox(id)?;

        // Collect containers belonging to this sandbox, and snapshot netns/ip for §D teardown.
        let (container_ids, sandbox_ip, sandbox_netns_path) = {
            let cache = self.cache.lock().unwrap();
            if !cache.sandboxes.contains_key(id) {
                return Ok(()); // already gone
            }
            let containers: Vec<ContainerId> = cache
                .containers
                .values()
                .filter(|c| &c.sandbox == id)
                .map(|c| c.id.clone())
                .collect();
            let (ip, netns) = cache
                .sandboxes
                .get(id)
                .map(|s| (s.ip.clone(), s.netns_path.clone()))
                .unwrap_or((None, None));
            (containers, ip, netns)
        };

        // Stop+remove each container
        for cid in &container_ids {
            self.stop_container(cid, 0)?;
            self.remove_container(cid)?;
        }

        // §D: CNI DEL + netns teardown (idempotent; fail-closed on DEL error).
        if let Some(ref ns_path) = sandbox_netns_path {
            cni_teardown(id, ns_path, &sandbox_ip);
        }

        // Remove the sandbox record
        {
            let mut cache = self.cache.lock().unwrap();
            if cache.sandboxes.remove(id).is_none() {
                return Ok(());
            }
        }
        let filename = format!("{}.json", id.0);
        let path = self.sandboxes_dir.join(&filename);
        let _ = fs::remove_file(path);
        Ok(())
    }

    fn sandbox_status(&self, id: &SandboxId) -> Result<SandboxStatus> {
        let cache = self.cache.lock().unwrap();
        cache
            .sandboxes
            .get(id)
            .map(sandbox_rec_to_status)
            .ok_or_else(|| BackendError::NotFound(format!("sandbox {}", id.0)))
    }

    fn list_sandboxes(&self, filter: &SandboxFilter) -> Result<Vec<SandboxStatus>> {
        let cache = self.cache.lock().unwrap();
        Ok(cache
            .sandboxes
            .values()
            .filter(|r| sandbox_matches(r, filter))
            .map(sandbox_rec_to_status)
            .collect())
    }

    // ---- container plane ----

    fn create_container(&self, sandbox: &SandboxId, cfg: ContainerConfig) -> Result<ContainerId> {
        {
            let cache = self.cache.lock().unwrap();
            match cache.sandboxes.get(sandbox) {
                None => return Err(BackendError::NotFound(format!("sandbox {}", sandbox.0))),
                Some(sb) if sb.state != SandboxState::Ready => {
                    return Err(BackendError::FailedPrecondition(format!(
                        "sandbox {} is not Ready",
                        sandbox.0
                    )))
                }
                Some(_) => {}
            }
        }
        let id = ContainerId(new_id("ct-"));
        let rec = ContainerRecord {
            id: id.clone(),
            sandbox: sandbox.clone(),
            config: cfg,
            state: ContainerState::Created,
            created_at_nanos: now_nanos(),
            started_at_nanos: 0,
            finished_at_nanos: 0,
            exit_code: 0,
            reason: String::new(),
            message: String::new(),
            pid: 0,
        };
        let filename = format!("{}.json", id.0);
        atomic_write_json(&self.containers_dir, &filename, &rec)?;
        let mut cache = self.cache.lock().unwrap();
        cache.containers.insert(id.clone(), rec);
        Ok(id)
    }

    fn start_container(&self, id: &ContainerId) -> Result<()> {
        let rec = {
            let cache = self.cache.lock().unwrap();
            cache
                .containers
                .get(id)
                .cloned()
                .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))?
        };

        if rec.state != ContainerState::Created {
            return Err(BackendError::FailedPrecondition(format!(
                "container {} is in state {:?}, must be Created to start",
                id.0, rec.state
            )));
        }

        // Fetch the sandbox record to get log_directory, dns, hostname.
        let (sandbox_log_dir, sandbox_dns, sandbox_hostname) = {
            let cache = self.cache.lock().unwrap();
            cache
                .sandboxes
                .get(&rec.sandbox)
                .map(|s| {
                    (
                        s.config.log_directory.clone(),
                        s.config.dns.clone(),
                        s.config.hostname.clone(),
                    )
                })
                .unwrap_or_default()
        };

        // §D resolv.conf synthesis: if the sandbox has dns config, write a
        // per-sandbox resolv.conf into the state dir so pre_exec can bind-mount
        // it (Linux only; macOS falls through to no-op).
        let resolv_conf_path: Option<std::path::PathBuf> = if let Some(ref dns) = sandbox_dns {
            match write_sandbox_resolv_conf(&self.state_root, &rec.sandbox, dns) {
                Ok(p) => Some(p),
                Err(e) => {
                    // Non-fatal: log and continue without resolv.conf override
                    eprintln!(
                        "[lightr-cri-fake] resolv.conf synthesis for sandbox {}: {e}",
                        rec.sandbox.0
                    );
                    None
                }
            }
        } else {
            None
        };

        // Open (or create) the CRI log file — empty file must exist from start (kubelet law §C)
        let log_file_opt =
            open_cri_log(&sandbox_log_dir, &rec.config.log_path).map_err(BackendError::Io)?;
        let log_shared: Arc<Mutex<Option<fs::File>>> = Arc::new(Mutex::new(log_file_opt));

        // Build command.
        //
        // Honest fake default entrypoint: when both `command` and `args` are
        // empty the container config carries no entrypoint (typical of critest
        // synthetic fake images which have no OCI image config). A real runtime
        // would fall back to the image's Cmd/Entrypoint; the fake has no image
        // layer, so we substitute a pause-equivalent shell loop that keeps the
        // container RUNNING so critest can exec into it. This is documented
        // fake behavior, NOT a faked result — containers with an explicit
        // command continue to use it unchanged.
        let default_command: Vec<String>;
        let (effective_command, effective_args): (&[String], &[String]) =
            if rec.config.command.is_empty() && rec.config.args.is_empty() {
                default_command = vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    "while true; do sleep 2147483647; done".to_string(),
                ];
                (&default_command, &[])
            } else {
                (&rec.config.command, &rec.config.args)
            };

        let mut cmd_iter = effective_command.iter().chain(effective_args.iter());
        let program = cmd_iter
            .next()
            .ok_or_else(|| BackendError::InvalidArgument("empty command".to_string()))?;

        let mut cmd = std::process::Command::new(program);
        cmd.args(cmd_iter);
        if !rec.config.working_dir.is_empty() {
            cmd.current_dir(&rec.config.working_dir);
        }
        for (k, v) in &rec.config.envs {
            cmd.env(k, v);
        }

        // ── Set up stdio ─────────────────────────────────────────────────────

        // Decide on pty vs. pipe mode.
        //
        // Normally this follows config.tty. ONE narrow exception: some programs
        // refuse to run without a controlling terminal and exit immediately when
        // their stdio is a pipe/null. The canonical case is procps `top`, which
        // cri-tools uses as its DefaultLinuxContainerCommand keep-alive entrypoint
        // (`["top"]`): with no tty it prints "failed tty get" and exits within ~1s,
        // so the container reaches Exited before a slow exec (e.g. the
        // "execSync with timeout" conformance loop) can run — the container
        // self-exits, independent of any kill path.
        //
        // To make such a keep-alive command GENUINELY persist (real execution on a
        // real pty, NOT a faked Running state) we route it through the pty path even
        // when config.tty == false: the child sees a tty, runs its loop, and stays
        // Running. The detection is deliberately minimal — the program basename,
        // not a heuristic on flags — so we only special-case the documented
        // terminal-requiring keep-alive command and nothing else. Containers whose
        // command does not need a tty keep the existing pipe/null stdio + fan-out.
        let program_basename = std::path::Path::new(program)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(program.as_str());
        let requires_tty = program_basename == "top";

        let use_tty = rec.config.tty || requires_tty;

        // tty=false (and not a terminal-requiring command): delegate to pipe-mode
        // helper and return early.
        if !use_tty {
            use std::process::Stdio;
            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());
            if rec.config.stdin {
                cmd.stdin(Stdio::piped());
            } else {
                cmd.stdin(Stdio::null());
            }
            // Install namespace pre_exec (setsid + optional UTS/mount namespaces
            // for hostname and resolv.conf).  Linux-only paths are #[cfg]-guarded.
            install_container_pre_exec(
                &mut cmd,
                sandbox_hostname.clone(),
                resolv_conf_path.clone(),
            );
            return self.start_container_pipe_mode(id, rec, cmd, sandbox_log_dir, log_shared);
        }

        // tty=true: open a pty pair, connect child to slave
        use nix::pty::openpty;
        let pty =
            openpty(None, None).map_err(|e| BackendError::Internal(format!("openpty: {e}")))?;

        // OwnedFd → std::fs::File (both implement the From conversion)
        let master_file: std::fs::File = pty.master.into();
        let slave_file: std::fs::File = pty.slave.into();

        // Clone slave for stdin/stdout/stderr of child
        let slave_stdin = dup_file(&slave_file).map_err(BackendError::Io)?;
        let slave_stdout = dup_file(&slave_file).map_err(BackendError::Io)?;
        let slave_stderr = slave_file; // last use — move it

        cmd.stdin(slave_stdin);
        cmd.stdout(slave_stdout);
        cmd.stderr(slave_stderr);

        // setsid + optional UTS/mount namespaces for hostname and resolv.conf.
        install_container_pre_exec(&mut cmd, sandbox_hostname, resolv_conf_path);

        // Tee: master fd carries all output (stdout+stderr merged on pty).
        // tty attach is served by duping the pty master (kernel-multiplexed),
        // so no fan-out is needed here.
        let master_for_tee = dup_file(&master_file).map_err(BackendError::Io)?;
        spawn_tee_thread("stdout", master_for_tee, Arc::clone(&log_shared), None);

        let container_io = ContainerIo {
            pty_master: Some(master_file),
            stdout_rd: None,
            stderr_rd: None,
            stdin_wr: None,
            fanout: None,
        };

        // ── Persist start intent (crash-only) ────────────────────────────────
        let started_at = now_nanos();
        {
            let mut cache = self.cache.lock().unwrap();
            let entry = cache
                .containers
                .get_mut(id)
                .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))?;
            entry.state = ContainerState::Running;
            entry.started_at_nanos = started_at;
            entry.pid = 0;
            entry.reason = "starting".to_string();
            let rec_clone = entry.clone();
            drop(cache);
            let filename = format!("{}.json", id.0);
            atomic_write_json(&self.containers_dir, &filename, &rec_clone)?;
        }

        let child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let mut cache = self.cache.lock().unwrap();
                if let Some(entry) = cache.containers.get_mut(id) {
                    entry.state = ContainerState::Exited;
                    entry.finished_at_nanos = now_nanos();
                    entry.exit_code = -1;
                    entry.reason = "spawn-failed".to_string();
                    entry.message = e.to_string();
                    let rec_clone = entry.clone();
                    drop(cache);
                    let filename = format!("{}.json", id.0);
                    let _ = atomic_write_json(&self.containers_dir, &filename, &rec_clone);
                }
                return Err(BackendError::Internal(format!(
                    "spawn container {}: {e}",
                    id.0
                )));
            }
        };

        let child_pid = child.id();

        // Store the io_table entry
        {
            let mut io = self.io_table.lock().unwrap();
            io.insert(id.clone(), container_io);
        }

        // Persist real pid
        {
            let mut cache = self.cache.lock().unwrap();
            let entry = cache
                .containers
                .get_mut(id)
                .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))?;
            entry.pid = child_pid;
            entry.reason = String::new();
            let rec_clone = entry.clone();
            drop(cache);
            let filename = format!("{}.json", id.0);
            atomic_write_json(&self.containers_dir, &filename, &rec_clone)?;
        }

        // Spawn reaper thread
        let containers_dir = self.containers_dir.clone();
        let cid = id.clone();
        let cache_arc = Arc::clone(&self.cache);
        let io_table_arc = Arc::clone(&self.io_table);

        std::thread::spawn(move || {
            let mut child = child;
            let status = child.wait();
            let finished_at = now_nanos();

            let (exit_code, reason) = match status {
                Ok(s) => {
                    #[cfg(unix)]
                    {
                        use std::os::unix::process::ExitStatusExt;
                        if let Some(sig) = s.signal() {
                            (128 + sig, format!("killed-by-signal-{sig}"))
                        } else {
                            (s.code().unwrap_or(0), String::new())
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        (s.code().unwrap_or(0), String::new())
                    }
                }
                Err(e) => (-1, format!("wait-error: {e}")),
            };

            // Remove io_table entry — fds are dropped here
            io_table_arc.lock().unwrap().remove(&cid);

            let mut cache = cache_arc.lock().unwrap();
            if let Some(entry) = cache.containers.get_mut(&cid) {
                if entry.state == ContainerState::Running {
                    entry.state = ContainerState::Exited;
                    entry.exit_code = exit_code;
                    entry.finished_at_nanos = finished_at;
                    entry.reason = reason;
                    let rec_clone = entry.clone();
                    let filename = format!("{}.json", cid.0);
                    let _ = atomic_write_json(&containers_dir, &filename, &rec_clone);
                }
            }
        });

        Ok(())
    }

    fn stop_container(&self, id: &ContainerId, grace_seconds: i64) -> Result<()> {
        let rec = {
            let cache = self.cache.lock().unwrap();
            match cache.containers.get(id) {
                Some(r) => r.clone(),
                None => return Ok(()), // idempotent: already gone
            }
        };

        match rec.state {
            ContainerState::Created | ContainerState::Exited => return Ok(()), // no-op
            ContainerState::Running => {}
            ContainerState::Unknown => return Ok(()),
        }

        // SIGTERM→SIGKILL law (seam-contract §Container plane):
        //   grace > 0  → graceful: SIGTERM, wait up to grace for exit, then SIGKILL.
        //   grace == 0 → forced:   immediate SIGKILL.
        // The detached reaper thread (start_container) is the SINGLE source of
        // truth for the terminal exit_code — it calls wait() and records the real
        // status (128+15 for SIGTERM, 128+9 for SIGKILL). We only deliver signals
        // and wait for the reaper to land the Exited state; we must NOT guess the
        // code here (a just-signalled process is a zombie until reaped, so a
        // liveness probe reads it as alive and would wrongly escalate SIGTERM→SIGKILL,
        // mis-tagging a graceful 143 stop as a forced 137).
        if rec.pid > 0 {
            if grace_seconds > 0 {
                unsafe {
                    libc::kill(rec.pid as libc::pid_t, libc::SIGTERM);
                }
                let grace = std::time::Duration::from_secs(grace_seconds as u64);
                if !self.wait_until_exited(id, grace) {
                    unsafe {
                        libc::kill(rec.pid as libc::pid_t, libc::SIGKILL);
                    }
                    self.wait_until_exited(id, std::time::Duration::from_secs(5));
                }
            } else {
                unsafe {
                    libc::kill(rec.pid as libc::pid_t, libc::SIGKILL);
                }
                self.wait_until_exited(id, std::time::Duration::from_secs(5));
            }
            return Ok(());
        }

        // Defensive: a Running record with no backing process (pid 0) has no
        // reaper to fire, so record the terminal state directly here.
        let finished_at = now_nanos();
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(entry) = cache.containers.get_mut(id) {
                if entry.state == ContainerState::Running {
                    entry.state = ContainerState::Exited;
                    entry.finished_at_nanos = finished_at;
                    entry.exit_code = if grace_seconds > 0 { 128 + 15 } else { 128 + 9 };
                    entry.reason = "stopped".to_string();
                    let rec_clone = entry.clone();
                    drop(cache);
                    let filename = format!("{}.json", id.0);
                    atomic_write_json(&self.containers_dir, &filename, &rec_clone)?;
                }
            }
        }
        Ok(())
    }

    fn remove_container(&self, id: &ContainerId) -> Result<()> {
        // CRI law: RemoveContainer force-removes even a Running container — it
        // is not a FailedPrecondition. If still Running, force-stop first
        // (grace=0 → SIGKILL via the same stop path), then remove.
        let is_running = {
            let cache = self.cache.lock().unwrap();
            match cache.containers.get(id) {
                None => return Ok(()), // idempotent: already gone
                Some(r) => r.state == ContainerState::Running,
            }
        };
        if is_running {
            // Reuse stop_container with grace=0 (forced SIGKILL + reap by the
            // detached reaper thread); idempotent and leak-free.
            self.stop_container(id, 0)?;
        }
        {
            let mut cache = self.cache.lock().unwrap();
            cache.containers.remove(id);
        }
        // Clean up io_table entry if present (drops any retained log/io fds).
        self.io_table.lock().unwrap().remove(id);
        let filename = format!("{}.json", id.0);
        let path = self.containers_dir.join(&filename);
        let _ = fs::remove_file(path);
        Ok(())
    }

    fn container_status(&self, id: &ContainerId) -> Result<ContainerStatus> {
        let cache = self.cache.lock().unwrap();
        let rec = cache
            .containers
            .get(id)
            .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))?;
        let log_dir = cache
            .sandboxes
            .get(&rec.sandbox)
            .map(|s| s.config.log_directory.clone())
            .unwrap_or_default();
        Ok(rec_to_status(rec, &log_dir))
    }

    fn list_containers(&self, filter: &ContainerFilter) -> Result<Vec<ContainerStatus>> {
        let cache = self.cache.lock().unwrap();
        let mut out = Vec::new();
        for r in cache.containers.values() {
            if container_matches(r, filter) {
                let log_dir = cache
                    .sandboxes
                    .get(&r.sandbox)
                    .map(|s| s.config.log_directory.clone())
                    .unwrap_or_default();
                out.push(rec_to_status(r, &log_dir));
            }
        }
        Ok(out)
    }

    fn container_stats(&self, id: &ContainerId) -> Result<ContainerStatsRec> {
        let rec = {
            let cache = self.cache.lock().unwrap();
            cache
                .containers
                .get(id)
                .cloned()
                .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))?
        };

        let ts = now_nanos();
        if rec.state != ContainerState::Running || rec.pid == 0 {
            return Ok(ContainerStatsRec {
                id: id.clone(),
                timestamp_nanos: ts,
                cpu_usage_core_nanos: 0,
                memory_working_set_bytes: 0,
            });
        }

        let (cpu, mem) = read_proc_stats(rec.pid);
        Ok(ContainerStatsRec {
            id: id.clone(),
            timestamp_nanos: ts,
            cpu_usage_core_nanos: cpu,
            memory_working_set_bytes: mem,
        })
    }

    fn list_container_stats(&self, filter: &ContainerFilter) -> Result<Vec<ContainerStatsRec>> {
        let ids: Vec<ContainerId> = {
            let cache = self.cache.lock().unwrap();
            cache
                .containers
                .values()
                .filter(|r| container_matches(r, filter))
                .map(|r| r.id.clone())
                .collect()
        };
        ids.iter().map(|id| self.container_stats(id)).collect()
    }

    // ---- exec plane ----

    fn exec_sync(
        &self,
        id: &ContainerId,
        cmd: &[String],
        timeout_seconds: i64,
    ) -> Result<ExecResult> {
        let rec = {
            let cache = self.cache.lock().unwrap();
            cache
                .containers
                .get(id)
                .cloned()
                .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))?
        };

        if rec.state != ContainerState::Running {
            return Err(BackendError::FailedPrecondition(format!(
                "container {} is not Running (state={:?}); exec_sync requires Running",
                id.0, rec.state
            )));
        }

        if cmd.is_empty() {
            return Err(BackendError::InvalidArgument(
                "exec_sync: empty command".to_string(),
            ));
        }

        let program = &cmd[0];
        let mut command = std::process::Command::new(program);
        command.args(&cmd[1..]);
        if !rec.config.working_dir.is_empty() {
            command.current_dir(&rec.config.working_dir);
        }
        for (k, v) in &rec.config.envs {
            command.env(k, v);
        }
        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());

        // Own process group so a timeout can SIGKILL the whole tree. critest's
        // "execSync with timeout" wraps the command in a shell that forks a child
        // (e.g. `sleep`); killing only the immediate child leaves the grandchild
        // holding the stdout pipe, so the timeout read would block and exec_sync
        // would never return. With the child as group leader, kill(-pgid) reaps all.
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }

        // §D: join the container child's namespaces so the exec process sees the
        // container's synthesized /etc/resolv.conf (mnt) and hostname (uts), plus
        // the sandbox netns. Order: mnt + uts first, then net (registered last so
        // it runs last in pre_exec). Without this, critest's `cat /etc/resolv.conf`
        // and `hostname` via ExecSync read the HOST values and fail.
        #[cfg(target_os = "linux")]
        {
            join_container_mnt_uts(&mut command, id, rec.pid)?;

            let netns_path_opt: Option<String> = {
                let cache = self.cache.lock().unwrap();
                cache
                    .sandboxes
                    .get(&rec.sandbox)
                    .and_then(|s| s.netns_path.clone())
            };
            if let Some(ref path_str) = netns_path_opt {
                let ns_fd = lightr_cri_net::netns::join_netns(std::path::Path::new(path_str))
                    .map_err(|e| {
                        BackendError::Internal(format!("exec_sync join_netns {path_str}: {e}"))
                    })?;
                unsafe {
                    use std::os::unix::io::IntoRawFd;
                    use std::os::unix::process::CommandExt;
                    let raw_fd = ns_fd.into_raw_fd();
                    command.pre_exec(move || {
                        let rc = libc::setns(raw_fd, libc::CLONE_NEWNET);
                        libc::close(raw_fd);
                        if rc != 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                        Ok(())
                    });
                }
            }
        }

        let mut child = command
            .spawn()
            .map_err(|e| BackendError::Internal(format!("exec_sync spawn: {e}")))?;

        if timeout_seconds > 0 {
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_secs(timeout_seconds as u64);

            loop {
                match child
                    .try_wait()
                    .map_err(|e| BackendError::Internal(format!("try_wait: {e}")))?
                {
                    Some(status) => {
                        let stdout = read_child_output(&mut child, true);
                        let stderr = read_child_output(&mut child, false);
                        let exit_code = exit_code_from_status(&status);
                        return Ok(ExecResult {
                            exit_code,
                            stdout,
                            stderr,
                        });
                    }
                    None => {
                        if std::time::Instant::now() >= deadline {
                            // Timeout: SIGKILL the whole process group (the child is
                            // its own group leader) so a forked grandchild can't keep
                            // the stdout pipe open and wedge the call; then reap so no
                            // zombie survives.
                            #[cfg(unix)]
                            unsafe {
                                libc::kill(-(child.id() as i32), libc::SIGKILL);
                            }
                            let _ = child.kill();
                            let _ = child.wait();
                            return Err(BackendError::Internal("exec timeout".to_string()));
                        }
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                }
            }
        } else {
            let output = child
                .wait_with_output()
                .map_err(|e| BackendError::Internal(format!("exec_sync wait: {e}")))?;
            let exit_code = exit_code_from_status(&output.status);
            Ok(ExecResult {
                exit_code,
                stdout: output.stdout,
                stderr: output.stderr,
            })
        }
    }

    // ---- image plane ----

    fn pull_image(&self, image_ref: &str) -> Result<PulledImage> {
        if image_ref.is_empty() || image_ref.chars().any(|c| c.is_ascii_whitespace()) {
            return Err(BackendError::InvalidArgument(format!(
                "image_ref {:?} is empty or contains whitespace",
                image_ref
            )));
        }

        let root_hex = blake3::hash(image_ref.as_bytes()).to_hex().to_string();
        let total_size = image_ref.len() as u64;
        let short = &root_hex[..16];
        let img_id = format!("sha-{short}");

        let disk_rec = ImageDiskRecord {
            id: img_id.clone(),
            ref_name: image_ref.to_string(),
            size: total_size,
        };
        let filename = format!("{img_id}.json");
        atomic_write_json(&self.images_dir, &filename, &disk_rec)?;

        {
            let mut cache = self.cache.lock().unwrap();
            cache.images.insert(img_id.clone(), disk_rec);
        }

        Ok(PulledImage {
            ref_name: image_ref.to_string(),
            root_hex,
            total_size,
        })
    }

    fn image_status(&self, image_ref: &str) -> Result<Option<ImageRecord>> {
        let cache = self.cache.lock().unwrap();
        let rec = cache
            .images
            .values()
            .find(|r| r.ref_name == image_ref)
            .map(|r| ImageRecord {
                id: r.id.clone(),
                ref_name: r.ref_name.clone(),
                size: r.size,
            });
        Ok(rec)
    }

    fn list_images(&self) -> Result<Vec<ImageRecord>> {
        let cache = self.cache.lock().unwrap();
        Ok(cache
            .images
            .values()
            .map(|r| ImageRecord {
                id: r.id.clone(),
                ref_name: r.ref_name.clone(),
                size: r.size,
            })
            .collect())
    }

    fn remove_image(&self, image_ref: &str) -> Result<()> {
        // Find the image record
        let img_id = {
            let cache = self.cache.lock().unwrap();
            cache
                .images
                .values()
                .find(|r| r.ref_name == image_ref)
                .map(|r| r.id.clone())
        };
        let img_id = match img_id {
            None => return Ok(()), // idempotent: not-found → Ok (CRI law)
            Some(id) => id,
        };

        // Check if any non-Exited container references this image
        {
            let cache = self.cache.lock().unwrap();
            for c in cache.containers.values() {
                if c.config.image_ref == image_ref && c.state != ContainerState::Exited {
                    return Err(BackendError::InUse(format!(
                        "image {image_ref} referenced by container {}",
                        c.id.0
                    )));
                }
            }
        }

        {
            let mut cache = self.cache.lock().unwrap();
            cache.images.remove(&img_id);
        }
        let filename = format!("{img_id}.json");
        let path = self.images_dir.join(&filename);
        let _ = fs::remove_file(path);
        Ok(())
    }

    fn image_fs_info(&self) -> Result<FsInfo> {
        let cache = self.cache.lock().unwrap();
        let used_bytes: u64 = cache.images.values().map(|r| r.size).sum();
        let inodes_used = cache.images.len() as u64;
        drop(cache);

        Ok(FsInfo {
            timestamp_nanos: now_nanos(),
            mountpoint: self.state_root.display().to_string(),
            used_bytes,
            inodes_used,
        })
    }

    // ---- v1.1 streaming methods ----

    /// Open an exec session: spawn `cmd` in the container's execution context
    /// (cwd, env; netns setns on Linux if netns_path is recorded — WP-B wires
    /// this; fake: run on host when netns_path is None).
    ///
    /// tty=true → openpty: child's stdio = slave, StreamSession.stdout = pty
    /// master clone, StreamSession.pty_master = pty master clone; stderr=None.
    /// tty=false → pipe pairs: stdout/stderr/stdin Files.
    /// stdin=false → no stdin pipe.
    ///
    /// The returned ExitWaiter waits the child and returns 128+sig or code.
    fn open_exec(
        &self,
        id: &ContainerId,
        cmd: &[String],
        tty: bool,
        stdin: bool,
    ) -> Result<StreamSession> {
        let rec = {
            let cache = self.cache.lock().unwrap();
            cache
                .containers
                .get(id)
                .cloned()
                .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))?
        };

        if rec.state != ContainerState::Running {
            return Err(BackendError::FailedPrecondition(format!(
                "container {} is not Running (state={:?}); open_exec requires Running",
                id.0, rec.state
            )));
        }

        if cmd.is_empty() {
            return Err(BackendError::InvalidArgument(
                "open_exec: empty command".to_string(),
            ));
        }

        let program = &cmd[0];
        let mut command = std::process::Command::new(program);
        command.args(&cmd[1..]);
        if !rec.config.working_dir.is_empty() {
            command.current_dir(&rec.config.working_dir);
        }
        for (k, v) in &rec.config.envs {
            command.env(k, v);
        }

        // §D: on Linux, if the container's sandbox has a recorded netns_path, setns
        // into it in pre_exec (async-signal-safe: open fd in parent, move OwnedFd into
        // closure, call setns — no alloc in pre_exec).  When netns_path is None (host_network
        // or CNI unavailable), exec runs on the host network as before.
        #[cfg(target_os = "linux")]
        {
            // Join the container child's mnt + uts namespaces first (so exec sees
            // the container's /etc/resolv.conf and hostname), then the netns below.
            // Registration order == pre_exec run order: mnt+uts before net.
            join_container_mnt_uts(&mut command, id, rec.pid)?;

            let netns_path_opt: Option<String> = {
                let cache = self.cache.lock().unwrap();
                cache
                    .sandboxes
                    .get(&rec.sandbox)
                    .and_then(|s| s.netns_path.clone())
            };
            if let Some(ref path_str) = netns_path_opt {
                // Open the netns fd in the parent (before fork) and move OwnedFd
                // into the pre_exec closure — async-signal-safe (r1-cni.md "Join at spawn").
                let ns_fd = lightr_cri_net::netns::join_netns(std::path::Path::new(path_str))
                    .map_err(|e| {
                        BackendError::Internal(format!("open_exec join_netns {path_str}: {e}"))
                    })?;
                unsafe {
                    use std::os::unix::io::IntoRawFd;
                    use std::os::unix::process::CommandExt;
                    let raw_fd = ns_fd.into_raw_fd();
                    command.pre_exec(move || {
                        let rc = libc::setns(raw_fd, libc::CLONE_NEWNET);
                        // Close the fd regardless of setns result (we own it).
                        libc::close(raw_fd);
                        if rc != 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                        Ok(())
                    });
                }
            }
        }

        if tty {
            use nix::pty::openpty;
            let pty =
                openpty(None, None).map_err(|e| BackendError::Internal(format!("openpty: {e}")))?;

            // OwnedFd → std::fs::File
            let master_file: std::fs::File = pty.master.into();
            let slave_file: std::fs::File = pty.slave.into();

            let slave_stdin = dup_file(&slave_file).map_err(BackendError::Io)?;
            let slave_stdout = dup_file(&slave_file).map_err(BackendError::Io)?;
            let slave_stderr = slave_file;

            use std::os::unix::process::CommandExt;
            command.stdin(slave_stdin);
            command.stdout(slave_stdout);
            command.stderr(slave_stderr);

            unsafe {
                command.pre_exec(|| {
                    libc::setsid();
                    Ok(())
                });
            }

            let child = command
                .spawn()
                .map_err(|e| BackendError::Internal(format!("open_exec spawn: {e}")))?;

            // stdout carries the pty stream; pty_master enables TIOCSWINSZ resize
            let stdout_fd = dup_file(&master_file).map_err(BackendError::Io)?;

            Ok(StreamSession {
                stdin: None, // tty: no separate stdin (write to master)
                stdout: Some(stdout_fd),
                stderr: None,
                pty_master: Some(master_file),
                waiter: Box::new(ChildWaiter { child }),
            })
        } else {
            use std::process::Stdio;
            command.stdout(Stdio::piped());
            command.stderr(Stdio::piped());
            if stdin {
                command.stdin(Stdio::piped());
            } else {
                command.stdin(Stdio::null());
            }

            let mut child = command
                .spawn()
                .map_err(|e| BackendError::Internal(format!("open_exec spawn: {e}")))?;

            use std::os::unix::io::FromRawFd;

            let stdout = child.stdout.take().map(|s| {
                use std::os::unix::io::IntoRawFd;
                unsafe { std::fs::File::from_raw_fd(s.into_raw_fd()) }
            });
            let stderr = child.stderr.take().map(|s| {
                use std::os::unix::io::IntoRawFd;
                unsafe { std::fs::File::from_raw_fd(s.into_raw_fd()) }
            });
            let stdin_file = child.stdin.take().map(|s| {
                use std::os::unix::io::IntoRawFd;
                unsafe { std::fs::File::from_raw_fd(s.into_raw_fd()) }
            });

            Ok(StreamSession {
                stdin: stdin_file,
                stdout,
                stderr,
                pty_master: None,
                waiter: Box::new(ChildWaiter { child }),
            })
        }
    }

    /// Attach to the container's live stdio using the held pipe/pty fds from
    /// the io_table (populated by start_container).
    ///
    /// Fake limitation: if the container was started in a previous process
    /// (post-crash, fds lost), returns BackendError::Internal("attach
    /// unavailable after restart"). Document this in tests.
    ///
    /// tty containers: returns a dup of the pty master in both stdout and
    /// pty_master; no separate stderr.
    /// pipe containers: returns duped read-ends of stdout/stderr plus the
    /// write-end of stdin (if held).
    fn open_attach(&self, id: &ContainerId) -> Result<StreamSession> {
        // Verify the container is Running
        {
            let cache = self.cache.lock().unwrap();
            let rec = cache
                .containers
                .get(id)
                .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))?;
            if rec.state != ContainerState::Running {
                return Err(BackendError::FailedPrecondition(format!(
                    "container {} is not Running (state={:?}); open_attach requires Running",
                    id.0, rec.state
                )));
            }
        }

        let io = self.io_table.lock().unwrap();
        let entry = io.get(id).ok_or_else(|| {
            BackendError::Internal("attach unavailable after restart".to_string())
        })?;

        if let Some(master) = &entry.pty_master {
            // tty mode: dup the master for both stdout and pty_master
            let stdout = dup_file(master).map_err(BackendError::Io)?;
            let pty_master = dup_file(master).map_err(BackendError::Io)?;

            // Waiter: attach sessions don't own the child; return a no-op waiter
            struct AttachWaiter;
            impl ExitWaiter for AttachWaiter {
                fn wait(self: Box<Self>) -> Result<i32> {
                    Ok(0)
                }
            }

            Ok(StreamSession {
                stdin: None,
                stdout: Some(stdout),
                stderr: None,
                pty_master: Some(pty_master),
                waiter: Box::new(AttachWaiter),
            })
        } else {
            // pipe mode (non-tty): output fan-out. The per-stream tee threads
            // are still the SOLE readers of the container's read-ends; here we
            // register a fresh OS pipe per stream as an attacher sink, so the
            // tee copies the container's raw bytes into our pipe in addition to
            // the CRI log. There is NO second reader of the container fd, so no
            // race with the log tee (the deferral reason is removed). The
            // StreamSession's stdout/stderr are the READ-ends of those pipes
            // (the streaming server reads them); stdin is a dup of the running
            // container's stdin write-end so the attacher can type into it.
            let fanout = entry.fanout.clone().ok_or_else(|| {
                BackendError::Internal(
                    "pipe-mode container has no output fan-out (started before fan-out support?)"
                        .to_string(),
                )
            })?;

            // stdout sink: register the write-end, hand back the read-end.
            let stdout_attach = if entry.stdout_rd.is_some() {
                let (rd, wr) = make_pipe().map_err(BackendError::Io)?;
                fanout.stdout_sinks.lock().unwrap().push(wr);
                Some(rd)
            } else {
                None
            };

            // stderr sink: same, only if the container has a stderr stream.
            let stderr_attach = if entry.stderr_rd.is_some() {
                let (rd, wr) = make_pipe().map_err(BackendError::Io)?;
                fanout.stderr_sinks.lock().unwrap().push(wr);
                Some(rd)
            } else {
                None
            };

            // stdin: dup the running container's stdin write-end (if it kept a
            // stdin pipe, i.e. config.stdin=true) so the attacher feeds the
            // LIVE process — exactly the pipe-mode attach contract.
            let stdin_attach = match &entry.stdin_wr {
                Some(w) => Some(dup_file(w).map_err(BackendError::Io)?),
                None => None,
            };

            drop(io);

            // Waiter: an attach session does not own the child. It completes
            // when the container leaves Running (process exit) so the streaming
            // server tears the session down. Subscriptions self-clean: once the
            // attacher drops the read-ends, the next tee broadcast hits a broken
            // pipe and prunes the sink (see FanOut::broadcast). No leak.
            struct AttachWaiter {
                cache: Arc<Mutex<Cache>>,
                id: ContainerId,
            }
            impl ExitWaiter for AttachWaiter {
                fn wait(self: Box<Self>) -> Result<i32> {
                    loop {
                        let code = {
                            let cache = self.cache.lock().unwrap();
                            match cache.containers.get(&self.id) {
                                Some(r) if r.state == ContainerState::Running => None,
                                Some(r) => Some(r.exit_code),
                                None => Some(0), // removed underneath us
                            }
                        };
                        if let Some(c) = code {
                            return Ok(c);
                        }
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                }
            }

            Ok(StreamSession {
                stdin: stdin_attach,
                stdout: stdout_attach,
                stderr: stderr_attach,
                pty_master: None,
                waiter: Box::new(AttachWaiter {
                    cache: Arc::clone(&self.cache),
                    id: id.clone(),
                }),
            })
        }
    }

    /// §D probe-truthful: reflects whether CNI is wired and available.
    /// On macOS / unprivileged (cni_available None) → false (host-network behavior).
    fn network_ready(&self) -> bool {
        lightr_cri_net::cni_available().is_some()
    }
}

// ---------------------------------------------------------------------------
// §D helpers: CNI setup and teardown (Linux only; no-op on macOS / unprivileged)
// ---------------------------------------------------------------------------

/// Find the lexicographically first .conflist in the given directory.
fn first_conflist(conf_dir: &std::path::Path) -> Option<std::path::PathBuf> {
    let mut entries: Vec<std::path::PathBuf> = conf_dir
        .read_dir()
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .and_then(|x| x.to_str())
                .map(|x| x == "conflist")
                .unwrap_or(false)
        })
        .collect();
    entries.sort();
    entries.into_iter().next()
}

/// Create a netns and run CNI ADD for a new sandbox.
/// Returns (pod_ip, netns_path_string) on success.
fn cni_setup(
    id: &SandboxId,
    cni_env: &lightr_cri_net::CniEnv,
    port_mappings: &[PortMapping],
) -> std::io::Result<(Option<String>, String)> {
    // Name the netns after the sandbox id (safe chars: [a-z0-9_.-]).
    // Use only the first 32 chars of the id to stay within IFNAMSIZ-like limits.
    let ns_name = format!("lightr-{}", &id.0[..id.0.len().min(24)]);
    let ns_path = lightr_cri_net::netns::create(&ns_name)?;
    let ns_path_str = ns_path.to_string_lossy().into_owned();

    let conflist_path = first_conflist(&cni_env.conf_dir).ok_or_else(|| {
        std::io::Error::other(format!(
            "no .conflist found in {}",
            cni_env.conf_dir.display()
        ))
    })?;

    let result = lightr_cri_net::chain::add(&conflist_path, &id.0, &ns_path, port_mappings)
        .map_err(|e| std::io::Error::other(format!("CNI ADD: {e}")))?;

    Ok((result.ip, ns_path_str))
}

/// Invoke CNI DEL and then tear down the netns.
/// Fail-closed: DEL errors are logged but teardown continues (§D law).
fn cni_teardown(id: &SandboxId, netns_path: &str, _ip: &Option<String>) {
    let ns_path = std::path::Path::new(netns_path);

    // Attempt CNI DEL (no port_mappings needed for teardown — omit for simplicity;
    // the portmap plugin handles missing mappings gracefully on DEL).
    if let Some(cni_env) = lightr_cri_net::cni_available() {
        if let Some(conflist_path) = first_conflist(&cni_env.conf_dir) {
            if let Err(e) = lightr_cri_net::chain::del(&conflist_path, &id.0, ns_path, &[]) {
                eprintln!(
                    "[lightr-cri-fake] CNI DEL for sandbox {} failed (continuing teardown): {e}",
                    id.0
                );
            }
        }
    }

    // umount2 + unlink — LAW: umount first (containerd#6143).
    if let Err(e) = lightr_cri_net::netns::teardown(ns_path) {
        eprintln!(
            "[lightr-cri-fake] netns teardown for {} failed: {e}",
            netns_path
        );
    }
}

// ---------------------------------------------------------------------------
// §D namespace helpers: resolv.conf synthesis + UTS/mount namespace setup
// ---------------------------------------------------------------------------

/// Synthesize a `resolv.conf` from the sandbox DnsConfig and write it to
/// `{state_root}/resolv-{sandbox_id}.conf`.  Returns the absolute path so
/// `install_container_pre_exec` can bind-mount it.
///
/// The file is written unconditionally on every `start_container` call so
/// dns changes in a re-run sandbox are reflected (crash-only: overwrite is
/// atomic via tmpfile+rename already used for state files).
fn write_sandbox_resolv_conf(
    state_root: &Path,
    sandbox: &SandboxId,
    dns: &lightr_cri_backend::DnsConfig,
) -> std::io::Result<PathBuf> {
    let mut content = String::new();
    for s in &dns.servers {
        content.push_str(&format!("nameserver {s}\n"));
    }
    if !dns.searches.is_empty() {
        content.push_str(&format!("search {}\n", dns.searches.join(" ")));
    }
    if !dns.options.is_empty() {
        content.push_str(&format!("options {}\n", dns.options.join(" ")));
    }
    let filename = format!("resolv-{}.conf", sandbox.0);
    atomic_write(state_root, &filename, content.as_bytes())?;
    Ok(state_root.join(filename))
}

/// Register a `pre_exec` closure on `cmd` that:
///   1. Always: `setsid()` so the child is a session leader.
///   2. Linux only, when `hostname` is non-empty: `unshare(CLONE_NEWUTS)` +
///      `sethostname(hostname)`.
///   3. Linux only, when `resolv_conf_src` is Some: `unshare(CLONE_NEWNS)` +
///      bind-mount `resolv_conf_src` over `/etc/resolv.conf` in the child's
///      private mount namespace.
///
/// All operations use only async-signal-safe syscalls (no alloc after fork).
/// The `hostname` and `resolv_conf_src` values are moved into the closure
/// before `fork` (parent side — safe).
///
/// On non-Linux platforms this function registers only the `setsid` closure.
fn install_container_pre_exec(
    cmd: &mut std::process::Command,
    hostname: String,
    resolv_conf_src: Option<PathBuf>,
) {
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;

        // Convert hostname and path to bytes once in the parent (safe: before fork).
        // We use raw C strings so pre_exec does not need to allocate.
        let hostname_bytes: Option<Vec<u8>> = if hostname.is_empty() {
            None
        } else {
            let mut b = hostname.into_bytes();
            b.push(0); // NUL-terminate for C
            Some(b)
        };

        #[cfg(target_os = "linux")]
        let resolv_src_bytes: Option<Vec<u8>> = resolv_conf_src.map(|p| {
            let mut b = p.as_os_str().as_encoded_bytes().to_vec();
            b.push(0);
            b
        });

        #[cfg(not(target_os = "linux"))]
        let _ = resolv_conf_src; // unused on non-Linux

        cmd.pre_exec(move || {
            // 1. setsid — always.
            libc::setsid();

            // 2. UTS namespace: set hostname (Linux only).
            #[cfg(target_os = "linux")]
            if let Some(ref hb) = hostname_bytes {
                // unshare(CLONE_NEWUTS): enter a private UTS namespace.
                if libc::unshare(libc::CLONE_NEWUTS) == 0 {
                    // sethostname(ptr, len-1) — exclude the NUL terminator.
                    let len = hb.len() - 1; // length without NUL
                    libc::sethostname(hb.as_ptr() as *const libc::c_char, len);
                }
                // If unshare fails (e.g. missing CAP_SYS_ADMIN in non-root
                // CI) we continue rather than abort the child — the hostname
                // will be the host's, and the critest DNS/hostname spec will
                // fail in that environment as expected.
            }
            #[cfg(not(target_os = "linux"))]
            let _ = &hostname_bytes;

            // 3. Mount namespace: bind-mount resolv.conf (Linux only).
            #[cfg(target_os = "linux")]
            if let Some(ref src_bytes) = resolv_src_bytes {
                // Target: "/etc/resolv.conf\0"
                const TARGET: &[u8] = b"/etc/resolv.conf\0";
                // unshare(CLONE_NEWNS): enter a private mount namespace.
                if libc::unshare(libc::CLONE_NEWNS) == 0 {
                    // make the mount tree private so the bind-mount doesn't
                    // propagate to the host (MS_REC | MS_PRIVATE).
                    libc::mount(
                        std::ptr::null(),
                        c"/".as_ptr(),
                        std::ptr::null(),
                        libc::MS_REC | libc::MS_PRIVATE,
                        std::ptr::null(),
                    );
                    // bind-mount the synthesized resolv.conf.
                    libc::mount(
                        src_bytes.as_ptr() as *const libc::c_char,
                        TARGET.as_ptr() as *const libc::c_char,
                        std::ptr::null(),
                        libc::MS_BIND,
                        std::ptr::null(),
                    );
                }
                // Same as hostname: best-effort — if unshare fails the child
                // inherits the host resolv.conf.
            }

            Ok(())
        });
    }
    // On non-Unix there is no pre_exec — no-op.
    #[cfg(not(unix))]
    {
        let _ = cmd;
        let _ = hostname;
        let _ = resolv_conf_src;
    }
}

/// Register a `pre_exec` closure on `cmd` that joins the container child's
/// PRIVATE mount + UTS namespaces (the ones `install_container_pre_exec`
/// unshared) so an exec process sees the container's synthesized
/// `/etc/resolv.conf` and `hostname` instead of the host values.
///
/// The container's mnt/uts namespaces are not bind-mounted to a pinned path
/// (only the netns is, via CNI); they live behind the live pause-loop PID.
/// We therefore open `/proc/<container_pid>/ns/{mnt,uts}` in the PARENT
/// (before fork) as `OwnedFd` — reusing `netns::join_netns`, which is just an
/// `O_RDONLY` open returning an `OwnedFd` — and `setns()` them in `pre_exec`
/// (async-signal-safe: fds opened pre-fork, only `setns`/`close` after fork).
///
/// Order matters: mnt + uts are joined BEFORE the caller registers the netns
/// join, so the full namespace set (mnt, uts, net) is entered before exec.
///
/// Fail-closed: if either ns file can't be opened (container process gone) we
/// return a `FailedPrecondition` "not Running" error rather than silently
/// running on the host namespaces.
#[cfg(target_os = "linux")]
fn join_container_mnt_uts(
    cmd: &mut std::process::Command,
    id: &ContainerId,
    container_pid: u32,
) -> Result<()> {
    if container_pid == 0 {
        return Err(BackendError::FailedPrecondition(format!(
            "container {} is not Running (no live pid); exec requires Running",
            id.0
        )));
    }

    let mnt_path = format!("/proc/{container_pid}/ns/mnt");
    let uts_path = format!("/proc/{container_pid}/ns/uts");

    // Open the ns fds in the parent (before fork) and move the OwnedFds into the
    // pre_exec closure — reusing the same open→OwnedFd→setns pattern as the
    // netns join. If the process is gone, fail closed with "not Running".
    let mnt_fd = lightr_cri_net::netns::join_netns(std::path::Path::new(&mnt_path)).map_err(|_| {
        BackendError::FailedPrecondition(format!(
            "container {} is not Running (mnt namespace unavailable); exec requires Running",
            id.0
        ))
    })?;
    let uts_fd = lightr_cri_net::netns::join_netns(std::path::Path::new(&uts_path)).map_err(|_| {
        BackendError::FailedPrecondition(format!(
            "container {} is not Running (uts namespace unavailable); exec requires Running",
            id.0
        ))
    })?;

    unsafe {
        use std::os::unix::io::IntoRawFd;
        use std::os::unix::process::CommandExt;
        let mnt_raw = mnt_fd.into_raw_fd();
        let uts_raw = uts_fd.into_raw_fd();
        cmd.pre_exec(move || {
            // Join mnt first, then uts. Both before the netns join (registered
            // later by the caller). We own both fds; close them regardless.
            let rc_mnt = libc::setns(mnt_raw, libc::CLONE_NEWNS);
            let rc_uts = libc::setns(uts_raw, libc::CLONE_NEWUTS);
            libc::close(mnt_raw);
            libc::close(uts_raw);
            if rc_mnt != 0 || rc_uts != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// start_container pipe-mode helper (extracted to avoid excessive nesting)
// ---------------------------------------------------------------------------

impl FakeBackend {
    #[allow(clippy::too_many_arguments)]
    fn start_container_pipe_mode(
        &self,
        id: &ContainerId,
        _rec: ContainerRecord,
        mut cmd: std::process::Command,
        sandbox_log_dir: String,
        log_shared: Arc<Mutex<Option<fs::File>>>,
    ) -> Result<()> {
        // Persist start intent (crash-only)
        let started_at = now_nanos();
        {
            let mut cache = self.cache.lock().unwrap();
            let entry = cache
                .containers
                .get_mut(id)
                .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))?;
            entry.state = ContainerState::Running;
            entry.started_at_nanos = started_at;
            entry.pid = 0;
            entry.reason = "starting".to_string();
            let rec_clone = entry.clone();
            drop(cache);
            let filename = format!("{}.json", id.0);
            atomic_write_json(&self.containers_dir, &filename, &rec_clone)?;
        }

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let mut cache = self.cache.lock().unwrap();
                if let Some(entry) = cache.containers.get_mut(id) {
                    entry.state = ContainerState::Exited;
                    entry.finished_at_nanos = now_nanos();
                    entry.exit_code = -1;
                    entry.reason = "spawn-failed".to_string();
                    entry.message = e.to_string();
                    let rec_clone = entry.clone();
                    drop(cache);
                    let filename = format!("{}.json", id.0);
                    let _ = atomic_write_json(&self.containers_dir, &filename, &rec_clone);
                }
                return Err(BackendError::Internal(format!(
                    "spawn container {}: {e}",
                    id.0
                )));
            }
        };

        let child_pid = child.id();

        // Extract pipe ends and set up tee threads
        use std::os::unix::io::{FromRawFd, IntoRawFd};

        let stdout_pipe = child.stdout.take();
        let stderr_pipe = child.stderr.take();
        let stdin_pipe = child.stdin.take();

        let stdout_rd_for_table: Option<std::fs::File>;
        let stderr_rd_for_table: Option<std::fs::File>;
        let stdin_wr_for_table: Option<std::fs::File>;

        // One fan-out per container, shared by both stream tee threads (writers)
        // and open_attach (registers attacher sinks). The tee threads remain the
        // SOLE readers of the container's read-ends.
        let fanout = Arc::new(FanOut::default());

        if let Some(stdout) = stdout_pipe {
            let raw = stdout.into_raw_fd();
            let f_for_tee = unsafe { std::fs::File::from_raw_fd(raw) };
            // dup for io_table (so tee thread can run independently)
            let f_for_table = dup_file(&f_for_tee).map_err(BackendError::Io)?;
            spawn_tee_thread(
                "stdout",
                f_for_tee,
                Arc::clone(&log_shared),
                Some(Arc::clone(&fanout)),
            );
            stdout_rd_for_table = Some(f_for_table);
        } else {
            stdout_rd_for_table = None;
        }

        if let Some(stderr) = stderr_pipe {
            let raw = stderr.into_raw_fd();
            let f_for_tee = unsafe { std::fs::File::from_raw_fd(raw) };
            let f_for_table = dup_file(&f_for_tee).map_err(BackendError::Io)?;
            spawn_tee_thread(
                "stderr",
                f_for_tee,
                Arc::clone(&log_shared),
                Some(Arc::clone(&fanout)),
            );
            stderr_rd_for_table = Some(f_for_table);
        } else {
            stderr_rd_for_table = None;
        }

        if let Some(stdin) = stdin_pipe {
            let raw = stdin.into_raw_fd();
            let f = unsafe { std::fs::File::from_raw_fd(raw) };
            stdin_wr_for_table = Some(f);
        } else {
            stdin_wr_for_table = None;
        }

        // Store io_table entry
        {
            let mut io = self.io_table.lock().unwrap();
            io.insert(
                id.clone(),
                ContainerIo {
                    pty_master: None,
                    stdout_rd: stdout_rd_for_table,
                    stderr_rd: stderr_rd_for_table,
                    stdin_wr: stdin_wr_for_table,
                    fanout: Some(fanout),
                },
            );
        }

        // Persist real pid
        {
            let mut cache = self.cache.lock().unwrap();
            let entry = cache
                .containers
                .get_mut(id)
                .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))?;
            entry.pid = child_pid;
            entry.reason = String::new();
            let rec_clone = entry.clone();
            drop(cache);
            let filename = format!("{}.json", id.0);
            atomic_write_json(&self.containers_dir, &filename, &rec_clone)?;
        }

        // Spawn reaper thread
        let containers_dir = self.containers_dir.clone();
        let cid = id.clone();
        let cache_arc = Arc::clone(&self.cache);
        let io_table_arc = Arc::clone(&self.io_table);
        let _ = sandbox_log_dir; // captured for completeness; log_shared keeps the file open

        std::thread::spawn(move || {
            let status = child.wait();
            let finished_at = now_nanos();

            let (exit_code, reason) = match status {
                Ok(s) => {
                    #[cfg(unix)]
                    {
                        use std::os::unix::process::ExitStatusExt;
                        if let Some(sig) = s.signal() {
                            (128 + sig, format!("killed-by-signal-{sig}"))
                        } else {
                            (s.code().unwrap_or(0), String::new())
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        (s.code().unwrap_or(0), String::new())
                    }
                }
                Err(e) => (-1, format!("wait-error: {e}")),
            };

            // Remove io_table entry — fds dropped here
            io_table_arc.lock().unwrap().remove(&cid);

            let mut cache = cache_arc.lock().unwrap();
            if let Some(entry) = cache.containers.get_mut(&cid) {
                if entry.state == ContainerState::Running {
                    entry.state = ContainerState::Exited;
                    entry.exit_code = exit_code;
                    entry.finished_at_nanos = finished_at;
                    entry.reason = reason;
                    let rec_clone = entry.clone();
                    let filename = format!("{}.json", cid.0);
                    let _ = atomic_write_json(&containers_dir, &filename, &rec_clone);
                }
            }
        });

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers for exec_sync
// ---------------------------------------------------------------------------

fn exit_code_from_status(status: &std::process::ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            return 128 + sig;
        }
    }
    status.code().unwrap_or(0)
}

fn read_child_output(child: &mut std::process::Child, stdout: bool) -> Vec<u8> {
    use std::io::Read;
    if stdout {
        if let Some(mut out) = child.stdout.take() {
            let mut buf = Vec::new();
            let _ = out.read_to_end(&mut buf);
            return buf;
        }
    } else if let Some(mut err) = child.stderr.take() {
        let mut buf = Vec::new();
        let _ = err.read_to_end(&mut buf);
        return buf;
    }
    Vec::new()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp_backend() -> (TempDir, FakeBackend) {
        let dir = TempDir::new().unwrap();
        let backend = FakeBackend::open(dir.path()).unwrap();
        (dir, backend)
    }

    fn minimal_sandbox_cfg(name: &str) -> SandboxConfig {
        SandboxConfig {
            name: name.to_string(),
            uid: "uid-1".to_string(),
            namespace: "ns".to_string(),
            attempt: 0,
            labels: Default::default(),
            annotations: Default::default(),
            log_directory: String::new(),
            hostname: String::new(),
            host_network: false,
            dns: None,
            port_mappings: vec![],
        }
    }

    fn minimal_container_cfg(name: &str) -> ContainerConfig {
        ContainerConfig {
            name: name.to_string(),
            attempt: 0,
            image_ref: "busybox:latest".to_string(),
            command: vec!["/bin/sleep".to_string()],
            args: vec!["10".to_string()],
            working_dir: String::new(),
            envs: vec![],
            mounts: vec![],
            labels: Default::default(),
            annotations: Default::default(),
            log_path: String::new(),
            tty: false,
            stdin: false,
        }
    }

    // ---- sandbox lifecycle ----

    #[test]
    fn sandbox_run_stop_remove() {
        let (_dir, backend) = tmp_backend();
        let cfg = minimal_sandbox_cfg("test-sb");
        let id = backend.run_sandbox(cfg).unwrap();

        let status = backend.sandbox_status(&id).unwrap();
        assert_eq!(status.state, SandboxState::Ready);

        backend.stop_sandbox(&id).unwrap();
        let status2 = backend.sandbox_status(&id).unwrap();
        assert_eq!(status2.state, SandboxState::NotReady);

        backend.remove_sandbox(&id).unwrap();
        let result = backend.sandbox_status(&id);
        assert!(matches!(result, Err(BackendError::NotFound(_))));
    }

    #[test]
    fn sandbox_stop_idempotent() {
        let (_dir, backend) = tmp_backend();
        let id = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        backend.stop_sandbox(&id).unwrap();
        backend.stop_sandbox(&id).unwrap(); // second stop is no-op
        backend.stop_sandbox(&id).unwrap(); // third stop too
    }

    #[test]
    fn sandbox_remove_idempotent() {
        let (_dir, backend) = tmp_backend();
        let id = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        backend.remove_sandbox(&id).unwrap();
        backend.remove_sandbox(&id).unwrap(); // second remove is no-op
    }

    #[test]
    fn sandbox_stop_nonexistent_is_ok() {
        let (_dir, backend) = tmp_backend();
        let id = SandboxId("not-there".to_string());
        backend.stop_sandbox(&id).unwrap(); // idempotent
    }

    #[test]
    fn list_sandboxes_filter_state() {
        let (_dir, backend) = tmp_backend();
        let id1 = backend.run_sandbox(minimal_sandbox_cfg("a")).unwrap();
        let id2 = backend.run_sandbox(minimal_sandbox_cfg("b")).unwrap();
        backend.stop_sandbox(&id1).unwrap();

        let filter = SandboxFilter {
            state: Some(SandboxState::Ready),
            ..Default::default()
        };
        let ready: Vec<_> = backend.list_sandboxes(&filter).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, id2);

        let filter2 = SandboxFilter {
            state: Some(SandboxState::NotReady),
            ..Default::default()
        };
        let not_ready: Vec<_> = backend.list_sandboxes(&filter2).unwrap();
        assert_eq!(not_ready.len(), 1);
        assert_eq!(not_ready[0].id, id1);
    }

    #[test]
    fn list_sandboxes_filter_label() {
        let (_dir, backend) = tmp_backend();
        let mut cfg = minimal_sandbox_cfg("labeled");
        cfg.labels.insert("env".to_string(), "test".to_string());
        let id = backend.run_sandbox(cfg).unwrap();
        backend
            .run_sandbox(minimal_sandbox_cfg("unlabeled"))
            .unwrap();

        let mut sel = BTreeMap::new();
        sel.insert("env".to_string(), "test".to_string());
        let filter = SandboxFilter {
            label_selector: sel,
            ..Default::default()
        };
        let result = backend.list_sandboxes(&filter).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, id);
    }

    // ---- container lifecycle ----

    #[test]
    fn container_create_not_found_sandbox() {
        let (_dir, backend) = tmp_backend();
        let err = backend
            .create_container(&SandboxId("ghost".to_string()), minimal_container_cfg("c"))
            .unwrap_err();
        assert!(matches!(err, BackendError::NotFound(_)));
    }

    #[test]
    fn container_created_state() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let cid = backend
            .create_container(&sb, minimal_container_cfg("c"))
            .unwrap();
        let status = backend.container_status(&cid).unwrap();
        assert_eq!(status.state, ContainerState::Created);
        assert_eq!(status.started_at_nanos, 0);
        assert_eq!(status.finished_at_nanos, 0);
    }

    #[test]
    fn container_start_only_from_created() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        // Starting again from Running must fail
        let err = backend.start_container(&cid).unwrap_err();
        assert!(matches!(err, BackendError::FailedPrecondition(_)));

        backend.stop_container(&cid, 0).unwrap();

        // Starting from Exited must fail
        let err2 = backend.start_container(&cid).unwrap_err();
        assert!(matches!(err2, BackendError::FailedPrecondition(_)));

        backend.remove_container(&cid).unwrap();
    }

    #[test]
    fn container_stop_idempotent() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let cid = backend
            .create_container(&sb, minimal_container_cfg("c"))
            .unwrap();

        // stop from Created is a no-op
        backend.stop_container(&cid, 0).unwrap();
        let status = backend.container_status(&cid).unwrap();
        assert_eq!(status.state, ContainerState::Created);

        // Start, stop, stop again
        let mut cfg2 = minimal_container_cfg("c2");
        cfg2.command = vec!["/bin/sleep".to_string()];
        cfg2.args = vec!["60".to_string()];
        let cid2 = backend.create_container(&sb, cfg2).unwrap();
        backend.start_container(&cid2).unwrap();
        backend.stop_container(&cid2, 0).unwrap();
        backend.stop_container(&cid2, 0).unwrap(); // idempotent
        backend.remove_container(&cid2).unwrap();
    }

    #[test]
    fn container_remove_while_running_force_removes() {
        // CRI law: RemoveContainer must succeed on a Running container —
        // it force-stops (SIGKILL) then removes. No FailedPrecondition.
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        // Direct remove of a Running container succeeds (force-remove).
        backend.remove_container(&cid).unwrap();
        // And it is gone: a second remove is a no-op (idempotent).
        backend.remove_container(&cid).unwrap();
    }

    #[test]
    fn container_remove_idempotent() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let cid = backend
            .create_container(&sb, minimal_container_cfg("c"))
            .unwrap();
        backend.remove_container(&cid).unwrap();
        backend.remove_container(&cid).unwrap(); // second remove is no-op
    }

    #[test]
    fn sandbox_remove_cascades_containers() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        // remove_sandbox should stop+remove the container
        backend.remove_sandbox(&sb).unwrap();

        let err = backend.container_status(&cid).unwrap_err();
        assert!(matches!(err, BackendError::NotFound(_)));
        let err2 = backend.sandbox_status(&sb).unwrap_err();
        assert!(matches!(err2, BackendError::NotFound(_)));
    }

    // ---- illegal transitions ----

    #[test]
    fn start_nonexistent_container() {
        let (_dir, backend) = tmp_backend();
        let err = backend
            .start_container(&ContainerId("ghost".to_string()))
            .unwrap_err();
        assert!(matches!(err, BackendError::NotFound(_)));
    }

    #[test]
    fn container_status_not_found() {
        let (_dir, backend) = tmp_backend();
        let err = backend
            .container_status(&ContainerId("ghost".to_string()))
            .unwrap_err();
        assert!(matches!(err, BackendError::NotFound(_)));
    }

    // ---- exec ----

    #[test]
    fn exec_sync_requires_running() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let cid = backend
            .create_container(&sb, minimal_container_cfg("c"))
            .unwrap();
        let cmd = vec!["/bin/echo".to_string(), "hi".to_string()];
        let err = backend.exec_sync(&cid, &cmd, 0).unwrap_err();
        assert!(matches!(err, BackendError::FailedPrecondition(_)));
    }

    #[test]
    fn exec_sync_runs_and_captures() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        let cmd = vec!["/bin/echo".to_string(), "hello".to_string()];
        let result = backend.exec_sync(&cid, &cmd, 5).unwrap();
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stdout.trim_ascii_end(), b"hello");

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
        backend.remove_sandbox(&sb).unwrap();
    }

    #[test]
    fn exec_sync_captures_exit_code() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        let cmd = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "exit 42".to_string(),
        ];
        let result = backend.exec_sync(&cid, &cmd, 5).unwrap();
        assert_eq!(result.exit_code, 42);

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
    }

    #[test]
    fn exec_sync_timeout() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        let cmd = vec!["/bin/sleep".to_string(), "60".to_string()];
        let err = backend.exec_sync(&cid, &cmd, 1).unwrap_err();
        assert!(matches!(err, BackendError::Internal(ref m) if m.contains("timeout")));

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
    }

    // ---- images ----

    #[test]
    fn pull_image_and_list() {
        let (_dir, backend) = tmp_backend();
        let pulled = backend.pull_image("nginx:1.25").unwrap();
        assert_eq!(pulled.ref_name, "nginx:1.25");
        assert!(!pulled.root_hex.is_empty());

        let images = backend.list_images().unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].ref_name, "nginx:1.25");
    }

    #[test]
    fn image_status_found_and_not_found() {
        let (_dir, backend) = tmp_backend();
        backend.pull_image("alpine:3").unwrap();

        let status = backend.image_status("alpine:3").unwrap();
        assert!(status.is_some());

        let missing = backend.image_status("not:there").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn pull_image_invalid_ref() {
        let (_dir, backend) = tmp_backend();
        let err = backend.pull_image("").unwrap_err();
        assert!(matches!(err, BackendError::InvalidArgument(_)));

        let err2 = backend.pull_image("has space").unwrap_err();
        assert!(matches!(err2, BackendError::InvalidArgument(_)));

        let err3 = backend.pull_image("has\ttab").unwrap_err();
        assert!(matches!(err3, BackendError::InvalidArgument(_)));
    }

    #[test]
    fn remove_image_not_found_is_ok() {
        // CRI law: removing a missing image is idempotent — not-found → Ok
        let (_dir, backend) = tmp_backend();
        backend.remove_image("not:there").unwrap(); // must succeed
                                                    // double-remove also Ok
        backend.remove_image("not:there").unwrap();
    }

    #[test]
    fn remove_image_in_use_by_running_container() {
        let (_dir, backend) = tmp_backend();
        backend.pull_image("busy:latest").unwrap();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.image_ref = "busy:latest".to_string();
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        let err = backend.remove_image("busy:latest").unwrap_err();
        assert!(matches!(err, BackendError::InUse(_)));

        backend.stop_container(&cid, 0).unwrap();
        // After container is Exited, image can be removed
        backend.remove_image("busy:latest").unwrap();
    }

    #[test]
    fn remove_image_in_use_by_created_container() {
        let (_dir, backend) = tmp_backend();
        backend.pull_image("busy:latest").unwrap();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.image_ref = "busy:latest".to_string();
        backend.create_container(&sb, cfg).unwrap();

        // Container is in Created state, image should be InUse
        let err = backend.remove_image("busy:latest").unwrap_err();
        assert!(matches!(err, BackendError::InUse(_)));
    }

    #[test]
    fn image_fs_info() {
        let (_dir, backend) = tmp_backend();
        backend.pull_image("img1:v1").unwrap();
        backend.pull_image("img2:v2").unwrap();
        let info = backend.image_fs_info().unwrap();
        assert_eq!(info.inodes_used, 2);
        assert!(info.used_bytes > 0);
        assert!(!info.mountpoint.is_empty());
    }

    // ---- filters ----

    #[test]
    fn list_containers_filter_sandbox() {
        let (_dir, backend) = tmp_backend();
        let sb1 = backend.run_sandbox(minimal_sandbox_cfg("sb1")).unwrap();
        let sb2 = backend.run_sandbox(minimal_sandbox_cfg("sb2")).unwrap();
        backend
            .create_container(&sb1, minimal_container_cfg("c1"))
            .unwrap();
        backend
            .create_container(&sb2, minimal_container_cfg("c2"))
            .unwrap();

        let filter = ContainerFilter {
            sandbox: Some(sb1.clone()),
            ..Default::default()
        };
        let result = backend.list_containers(&filter).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].sandbox, sb1);
    }

    #[test]
    fn list_containers_filter_state() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        backend
            .create_container(&sb, minimal_container_cfg("c1"))
            .unwrap();
        let mut cfg2 = minimal_container_cfg("c2");
        cfg2.command = vec!["/bin/sleep".to_string()];
        cfg2.args = vec!["60".to_string()];
        let cid2 = backend.create_container(&sb, cfg2).unwrap();
        backend.start_container(&cid2).unwrap();

        let filter = ContainerFilter {
            state: Some(ContainerState::Running),
            ..Default::default()
        };
        let running = backend.list_containers(&filter).unwrap();
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].id, cid2);

        backend.stop_container(&cid2, 0).unwrap();
        backend.remove_container(&cid2).unwrap();
    }

    #[test]
    fn list_containers_filter_label() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("labeled");
        cfg.labels.insert("tier".to_string(), "backend".to_string());
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend
            .create_container(&sb, minimal_container_cfg("unlabeled"))
            .unwrap();

        let mut sel = BTreeMap::new();
        sel.insert("tier".to_string(), "backend".to_string());
        let filter = ContainerFilter {
            label_selector: sel,
            ..Default::default()
        };
        let result = backend.list_containers(&filter).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, cid);
    }

    // ---- atomicity (reopen mid-state) ----

    #[test]
    fn reopen_rebuilds_sandbox_state() {
        let dir = TempDir::new().unwrap();
        let id = {
            let backend = FakeBackend::open(dir.path()).unwrap();
            let id = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
            backend.stop_sandbox(&id).unwrap();
            id
        };
        // Re-open from disk
        let backend2 = FakeBackend::open(dir.path()).unwrap();
        let status = backend2.sandbox_status(&id).unwrap();
        assert_eq!(status.state, SandboxState::NotReady);
    }

    #[test]
    fn reopen_rebuilds_container_state() {
        let dir = TempDir::new().unwrap();
        let (sb_id, cid) = {
            let backend = FakeBackend::open(dir.path()).unwrap();
            let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
            let cid = backend
                .create_container(&sb, minimal_container_cfg("c"))
                .unwrap();
            (sb, cid)
        };
        let backend2 = FakeBackend::open(dir.path()).unwrap();
        let status = backend2.container_status(&cid).unwrap();
        assert_eq!(status.state, ContainerState::Created);
        assert_eq!(status.sandbox, sb_id);
    }

    #[test]
    fn reopen_rebuilds_image_state() {
        let dir = TempDir::new().unwrap();
        {
            let backend = FakeBackend::open(dir.path()).unwrap();
            backend.pull_image("redis:7").unwrap();
        }
        let backend2 = FakeBackend::open(dir.path()).unwrap();
        let images = backend2.list_images().unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].ref_name, "redis:7");
    }

    #[test]
    fn reopen_running_container_pid_alive_stays_running() {
        let dir = TempDir::new().unwrap();
        let cid = {
            let backend = FakeBackend::open(dir.path()).unwrap();
            let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
            let mut cfg = minimal_container_cfg("c");
            cfg.command = vec!["/bin/sleep".to_string()];
            cfg.args = vec!["300".to_string()];
            let cid = backend.create_container(&sb, cfg).unwrap();
            backend.start_container(&cid).unwrap();
            cid
        };
        // Re-open — pid is still alive, so state stays Running
        let backend2 = FakeBackend::open(dir.path()).unwrap();
        let status = backend2.container_status(&cid).unwrap();
        assert_eq!(status.state, ContainerState::Running);
        // Clean up
        backend2.stop_container(&cid, 0).unwrap();
        backend2.remove_container(&cid).unwrap();
    }

    // ---- stats ----

    #[test]
    fn stats_not_found() {
        let (_dir, backend) = tmp_backend();
        let err = backend
            .container_stats(&ContainerId("ghost".to_string()))
            .unwrap_err();
        assert!(matches!(err, BackendError::NotFound(_)));
    }

    #[test]
    fn stats_not_running_returns_zeros() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let cid = backend
            .create_container(&sb, minimal_container_cfg("c"))
            .unwrap();
        let stats = backend.container_stats(&cid).unwrap();
        assert_eq!(stats.cpu_usage_core_nanos, 0);
        assert_eq!(stats.memory_working_set_bytes, 0);
        assert!(stats.timestamp_nanos > 0);
    }

    // ---- blake3 determinism ----

    #[test]
    fn pull_image_deterministic_hash() {
        let (dir1, backend1) = tmp_backend();
        let (dir2, backend2) = tmp_backend();
        let _ = dir1;
        let _ = dir2;
        let p1 = backend1.pull_image("same:ref").unwrap();
        let p2 = backend2.pull_image("same:ref").unwrap();
        assert_eq!(p1.root_hex, p2.root_hex);
        assert_eq!(p1.total_size, p2.total_size);
    }

    // ---- v1.1 WP-A: CRI log tee (§C) ----

    /// Log file created + format correct after a run (tty=false, short-lived cmd)
    #[test]
    fn log_file_created_and_format_correct() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("logs");
        fs::create_dir_all(&log_dir).unwrap();

        let backend = FakeBackend::open(dir.path()).unwrap();

        let mut sb_cfg = minimal_sandbox_cfg("sb");
        sb_cfg.log_directory = log_dir.to_str().unwrap().to_string();
        let sb = backend.run_sandbox(sb_cfg).unwrap();

        let mut cfg = minimal_container_cfg("log-test");
        cfg.command = vec!["/bin/sh".to_string()];
        cfg.args = vec!["-c".to_string(), "echo hello-log".to_string()];
        cfg.log_path = "test.log".to_string();
        cfg.tty = false;
        cfg.stdin = false;

        let cid = backend.create_container(&sb, cfg).unwrap();

        // Log file must exist immediately after start (kubelet ReopenContainerLog law)
        backend.start_container(&cid).unwrap();
        let log_path = log_dir.join("test.log");
        assert!(
            log_path.exists(),
            "log file must exist after start_container"
        );

        // Wait for process to finish and tee thread to flush
        std::thread::sleep(std::time::Duration::from_millis(500));

        let contents = fs::read_to_string(&log_path).unwrap();
        // Must have at least one CRI-format line: <ts> stdout F hello-log\n
        let mut found = false;
        for line in contents.lines() {
            // Format: <RFC3339Nano> stdout F hello-log
            let parts: Vec<&str> = line.splitn(4, ' ').collect();
            if parts.len() >= 4
                && parts[1] == "stdout"
                && parts[2] == "F"
                && parts[3].contains("hello-log")
            {
                found = true;
            }
        }
        assert!(
            found,
            "CRI-format log line not found; contents: {contents:?}"
        );

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
        backend.remove_sandbox(&sb).unwrap();
    }

    /// Log file is created (empty) immediately even if the process emits nothing
    #[test]
    fn log_file_created_even_if_no_output() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("logs2");
        fs::create_dir_all(&log_dir).unwrap();

        let backend = FakeBackend::open(dir.path()).unwrap();

        let mut sb_cfg = minimal_sandbox_cfg("sb");
        sb_cfg.log_directory = log_dir.to_str().unwrap().to_string();
        let sb = backend.run_sandbox(sb_cfg).unwrap();

        let mut cfg = minimal_container_cfg("silent");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        cfg.log_path = "silent.log".to_string();

        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        let log_path = log_dir.join("silent.log");
        assert!(
            log_path.exists(),
            "empty log file must exist immediately after start"
        );

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
        backend.remove_sandbox(&sb).unwrap();
    }

    // ---- v1.1 WP-A: open_exec (§B) ----

    /// open_exec echo + exit code 0 (tty=false)
    #[test]
    fn open_exec_echo_and_exit_code_zero() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        let cmd = vec!["/bin/echo".to_string(), "exec-hello".to_string()];
        let mut session = backend.open_exec(&cid, &cmd, false, false).unwrap();

        // Read stdout
        use std::io::Read;
        let mut out = Vec::new();
        if let Some(mut f) = session.stdout.take() {
            f.read_to_end(&mut out).unwrap();
        }
        let exit_code = session.waiter.wait().unwrap();

        assert_eq!(exit_code, 0);
        assert!(
            out.starts_with(b"exec-hello"),
            "stdout: {:?}",
            String::from_utf8_lossy(&out)
        );

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
        backend.remove_sandbox(&sb).unwrap();
    }

    /// open_exec exit code 7
    #[test]
    fn open_exec_exit_code_7() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        let cmd = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "exit 7".to_string(),
        ];
        let session = backend.open_exec(&cid, &cmd, false, false).unwrap();
        let exit_code = session.waiter.wait().unwrap();
        assert_eq!(exit_code, 7);

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
        backend.remove_sandbox(&sb).unwrap();
    }

    /// open_exec on non-Running container → FailedPrecondition
    #[test]
    fn open_exec_non_running_fails_precondition() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let cid = backend
            .create_container(&sb, minimal_container_cfg("c"))
            .unwrap();
        // Container is in Created state
        let cmd = vec!["/bin/echo".to_string(), "hi".to_string()];
        let result = backend.open_exec(&cid, &cmd, false, false);
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("expected Err(FailedPrecondition), got Ok"),
        };
        assert!(
            matches!(err, BackendError::FailedPrecondition(_)),
            "expected FailedPrecondition, got: {err}"
        );
    }

    /// open_exec with tty=true: a line written via the slave is readable from master
    #[test]
    fn open_exec_tty_writes_line() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        // echo a line — the output goes to the pty slave, readable via master
        let cmd = vec!["/bin/echo".to_string(), "pty-hello".to_string()];
        let mut session = backend.open_exec(&cid, &cmd, true, false).unwrap();
        // stdout == pty master clone
        assert!(
            session.pty_master.is_some(),
            "pty_master must be Some for tty=true"
        );
        assert!(session.stdout.is_some(), "stdout must be Some for tty=true");

        // Read a few bytes from stdout (pty master)
        use std::io::Read;
        let mut buf = [0u8; 256];
        let mut total = Vec::new();
        // Give child time to write and pty to flush
        std::thread::sleep(std::time::Duration::from_millis(200));
        if let Some(mut f) = session.stdout.take() {
            // Non-blocking: read what's available
            use std::os::unix::io::AsRawFd;
            let fd = f.as_raw_fd();
            unsafe {
                let flags = libc::fcntl(fd, libc::F_GETFL);
                libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
            let _ = f.read(&mut buf).map(|n| total.extend_from_slice(&buf[..n]));
        }
        let _ = session.waiter.wait();

        // pty output includes CR-LF; check for our string somewhere in output
        let s = String::from_utf8_lossy(&total);
        assert!(
            s.contains("pty-hello"),
            "expected 'pty-hello' in pty output, got: {s:?}"
        );

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
        backend.remove_sandbox(&sb).unwrap();
    }

    /// open_attach on a non-Running container → FailedPrecondition
    #[test]
    fn open_attach_non_running_fails_precondition() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let cid = backend
            .create_container(&sb, minimal_container_cfg("c"))
            .unwrap();
        let err = match backend.open_attach(&cid) {
            Err(e) => e,
            Ok(_) => panic!("expected Err(FailedPrecondition), got Ok"),
        };
        assert!(matches!(err, BackendError::FailedPrecondition(_)));
    }

    /// open_attach after restart (no io_table entry) → Internal error
    #[test]
    fn open_attach_unavailable_after_restart() {
        let dir = TempDir::new().unwrap();
        let cid = {
            let backend = FakeBackend::open(dir.path()).unwrap();
            let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
            let mut cfg = minimal_container_cfg("c");
            cfg.command = vec!["/bin/sleep".to_string()];
            cfg.args = vec!["300".to_string()];
            let cid = backend.create_container(&sb, cfg).unwrap();
            backend.start_container(&cid).unwrap();
            cid
            // backend dropped here — io_table is lost
        };
        // Re-open: container is still Running (pid alive), but io_table is empty
        let backend2 = FakeBackend::open(dir.path()).unwrap();
        let status = backend2.container_status(&cid).unwrap();
        assert_eq!(status.state, ContainerState::Running);
        let err = match backend2.open_attach(&cid) {
            Err(e) => e,
            Ok(_) => panic!("expected Err(Internal), got Ok"),
        };
        assert!(
            matches!(&err, BackendError::Internal(m) if m.contains("attach unavailable after restart")),
            "expected Internal(attach unavailable after restart), got: {err}"
        );
        backend2.stop_container(&cid, 0).unwrap();
        backend2.remove_container(&cid).unwrap();
    }

    // ---- Fix 2: empty command default entrypoint ----

    /// When both command and args are empty, start_container must substitute
    /// a pause-equivalent default (`/bin/sh -c while true; do sleep ...`) so
    /// the container starts running and critest can exec into it.
    #[test]
    fn empty_command_defaults_to_pause_loop() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();

        // Explicitly empty command + args — the default entrypoint kicks in.
        let cfg = ContainerConfig {
            name: "no-cmd".to_string(),
            attempt: 0,
            image_ref: "fake:latest".to_string(),
            command: vec![], // empty
            args: vec![],    // empty
            working_dir: String::new(),
            envs: vec![],
            mounts: vec![],
            labels: Default::default(),
            annotations: Default::default(),
            log_path: String::new(),
            tty: false,
            stdin: false,
        };
        let cid = backend.create_container(&sb, cfg).unwrap();
        // Must not fail with "empty command" — the fake substitutes the pause loop.
        backend.start_container(&cid).unwrap();

        // Container must be Running.
        let status = backend.container_status(&cid).unwrap();
        assert_eq!(
            status.state,
            ContainerState::Running,
            "container with empty command must enter Running via default entrypoint"
        );

        // exec_sync into the running container must work.
        let result = backend
            .exec_sync(&cid, &["/bin/echo".to_string(), "ok".to_string()], 5)
            .unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.starts_with(b"ok"));

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
        backend.remove_sandbox(&sb).unwrap();
    }

    // ---- Fix 3/4: resolv.conf + hostname namespace (Linux-only, probe-gated) ----

    /// resolv.conf synthesis: when the sandbox has DnsConfig, exec_sync
    /// `cat /etc/resolv.conf` must contain the configured nameserver/search/options.
    ///
    /// This test requires Linux (mount namespace + bind-mount) and CAP_SYS_ADMIN.
    /// It SKIPs cleanly on macOS or when running unprivileged.
    #[test]
    #[cfg(target_os = "linux")]
    fn resolv_conf_synthesized_in_container() {
        // Probe: need CAP_SYS_ADMIN for unshare(CLONE_NEWNS).
        // Use a lightweight check: try unshare on a throwaway thread.
        let priv_ok =
            std::thread::spawn(|| -> bool { unsafe { libc::unshare(libc::CLONE_NEWNS) == 0 } })
                .join()
                .unwrap_or(false);
        if !priv_ok {
            eprintln!(
                "SKIP resolv_conf_synthesized_in_container: \
                 CLONE_NEWNS requires CAP_SYS_ADMIN (probe-truthful)"
            );
            return;
        }

        let (_dir, backend) = tmp_backend();

        let mut sb_cfg = minimal_sandbox_cfg("sb-dns");
        sb_cfg.dns = Some(DnsConfig {
            servers: vec!["10.10.10.10".to_string()],
            searches: vec!["google.com".to_string()],
            options: vec!["ndots:8".to_string()],
        });
        let sb = backend.run_sandbox(sb_cfg).unwrap();

        // Container with empty command — uses the pause-loop default.
        let cfg = ContainerConfig {
            name: "dns-test".to_string(),
            attempt: 0,
            image_ref: "fake:latest".to_string(),
            command: vec![],
            args: vec![],
            working_dir: String::new(),
            envs: vec![],
            mounts: vec![],
            labels: Default::default(),
            annotations: Default::default(),
            log_path: String::new(),
            tty: false,
            stdin: false,
        };
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        // exec cat /etc/resolv.conf inside the container's namespace.
        let result = backend
            .exec_sync(
                &cid,
                &["/bin/cat".to_string(), "/etc/resolv.conf".to_string()],
                5,
            )
            .unwrap();
        assert_eq!(result.exit_code, 0, "cat /etc/resolv.conf must succeed");

        let content = String::from_utf8_lossy(&result.stdout);
        assert!(
            content.contains("nameserver 10.10.10.10"),
            "resolv.conf must contain nameserver; got: {content:?}"
        );
        assert!(
            content.contains("search google.com"),
            "resolv.conf must contain search; got: {content:?}"
        );
        assert!(
            content.contains("options ndots:8"),
            "resolv.conf must contain options; got: {content:?}"
        );

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
        backend.remove_sandbox(&sb).unwrap();
    }

    /// hostname namespace: when the sandbox has a hostname set, the container's
    /// `hostname` command must return it.
    ///
    /// Requires Linux + CAP_SYS_ADMIN for unshare(CLONE_NEWUTS).
    /// SKIPs cleanly on macOS or unprivileged.
    #[test]
    #[cfg(target_os = "linux")]
    fn hostname_set_in_container_uts_namespace() {
        // Probe: need CAP_SYS_ADMIN for unshare(CLONE_NEWUTS).
        let priv_ok =
            std::thread::spawn(|| -> bool { unsafe { libc::unshare(libc::CLONE_NEWUTS) == 0 } })
                .join()
                .unwrap_or(false);
        if !priv_ok {
            eprintln!(
                "SKIP hostname_set_in_container_uts_namespace: \
                 CLONE_NEWUTS requires CAP_SYS_ADMIN (probe-truthful)"
            );
            return;
        }

        let (_dir, backend) = tmp_backend();

        let mut sb_cfg = minimal_sandbox_cfg("sb-hostname");
        sb_cfg.hostname = "critest-hostname".to_string();
        let sb = backend.run_sandbox(sb_cfg).unwrap();

        let cfg = ContainerConfig {
            name: "uts-test".to_string(),
            attempt: 0,
            image_ref: "fake:latest".to_string(),
            command: vec![],
            args: vec![],
            working_dir: String::new(),
            envs: vec![],
            mounts: vec![],
            labels: Default::default(),
            annotations: Default::default(),
            log_path: String::new(),
            tty: false,
            stdin: false,
        };
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        // exec hostname inside the container.
        let result = backend
            .exec_sync(&cid, &["/bin/hostname".to_string()], 5)
            .unwrap();
        assert_eq!(result.exit_code, 0, "hostname must succeed");

        let got = String::from_utf8_lossy(&result.stdout);
        let got = got.trim();
        assert_eq!(
            got, "critest-hostname",
            "hostname must return the sandbox hostname; got: {got:?}"
        );

        backend.stop_container(&cid, 0).unwrap();
        backend.remove_container(&cid).unwrap();
        backend.remove_sandbox(&sb).unwrap();
    }
}
