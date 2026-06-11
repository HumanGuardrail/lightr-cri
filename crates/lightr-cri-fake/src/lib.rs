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
}

impl FakeBackend {
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
    let cpu_nanos = if clk_tck > 0 {
        if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
            // fields are space-separated; utime=14th field (0-indexed=13), stime=15th (0-indexed=14)
            let fields: Vec<&str> = stat.split_whitespace().collect();
            if fields.len() > 14 {
                let utime: u64 = fields[13].parse().unwrap_or(0);
                let stime: u64 = fields[14].parse().unwrap_or(0);
                let ticks = utime + stime;
                ticks * (1_000_000_000 / clk_tck)
            } else {
                0
            }
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

fn rec_to_status(rec: &ContainerRecord) -> ContainerStatus {
    ContainerStatus {
        id: rec.id.clone(),
        sandbox: rec.sandbox.clone(),
        config: rec.config.clone(),
        state: rec.state,
        created_at_nanos: rec.created_at_nanos,
        started_at_nanos: rec.started_at_nanos,
        finished_at_nanos: rec.finished_at_nanos,
        exit_code: rec.exit_code,
        reason: rec.reason.clone(),
        message: rec.message.clone(),
    }
}

fn sandbox_rec_to_status(rec: &SandboxRecord) -> SandboxStatus {
    SandboxStatus {
        id: rec.id.clone(),
        config: rec.config.clone(),
        state: rec.state,
        created_at_nanos: rec.created_at_nanos,
    }
}

// ---------------------------------------------------------------------------
// CriBackend impl
// ---------------------------------------------------------------------------

impl CriBackend for FakeBackend {
    // ---- sandbox plane ----

    fn run_sandbox(&self, cfg: SandboxConfig) -> Result<SandboxId> {
        let id = SandboxId(new_id("sb-"));
        let rec = SandboxRecord {
            id: id.clone(),
            config: cfg,
            state: SandboxState::Ready,
            created_at_nanos: now_nanos(),
        };
        let filename = format!("{}.json", id.0);
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

        // Collect containers belonging to this sandbox
        let container_ids: Vec<ContainerId> = {
            let cache = self.cache.lock().unwrap();
            if !cache.sandboxes.contains_key(id) {
                return Ok(()); // already gone
            }
            cache
                .containers
                .values()
                .filter(|c| &c.sandbox == id)
                .map(|c| c.id.clone())
                .collect()
        };

        // Stop+remove each container
        for cid in &container_ids {
            self.stop_container(cid, 0)?;
            self.remove_container(cid)?;
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

        // Build command
        let mut cmd_iter = rec.config.command.iter().chain(rec.config.args.iter());
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

        // setsid so the child survives parent's death
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            unsafe {
                cmd.pre_exec(|| {
                    libc::setsid();
                    Ok(())
                });
            }
        }

        // Crash-only law (cold-critic fix 2026-06-11): persist the START
        // INTENT before spawning. If we die between spawn and the pid write,
        // reopen sees Running+pid=0 → Exited/-1 "lost-start-window" instead
        // of a phantom Created that could double-start.
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
                // Spawn failed: persist the truth (Exited) before erroring.
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

        // Persist the real pid (intent → running)
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

        std::thread::spawn(move || {
            // Wait for child to exit
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

            let mut cache = cache_arc.lock().unwrap();
            if let Some(entry) = cache.containers.get_mut(&cid) {
                if entry.state == ContainerState::Running {
                    entry.state = ContainerState::Exited;
                    entry.exit_code = exit_code;
                    entry.finished_at_nanos = finished_at;
                    entry.reason = reason;
                    let rec_clone = entry.clone();
                    drop(cache);
                    let filename = format!("{}.json", cid.0);
                    let _ = atomic_write_json(&containers_dir, &filename, &rec_clone);
                }
            }
        });

        Ok(())
    }

    fn stop_container(&self, id: &ContainerId, _grace_seconds: i64) -> Result<()> {
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

        // Send SIGTERM/SIGKILL to the process
        if rec.pid > 0 {
            unsafe {
                libc::kill(rec.pid as libc::pid_t, libc::SIGTERM);
            }
            // Give a brief moment, then SIGKILL
            std::thread::sleep(std::time::Duration::from_millis(100));
            if pid_alive(rec.pid) {
                unsafe {
                    libc::kill(rec.pid as libc::pid_t, libc::SIGKILL);
                }
            }
        }

        let finished_at = now_nanos();
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(entry) = cache.containers.get_mut(id) {
                if entry.state == ContainerState::Running {
                    entry.state = ContainerState::Exited;
                    entry.finished_at_nanos = finished_at;
                    entry.exit_code = 128 + 15; // SIGTERM
                    entry.reason = "stopped".to_string();
                    let rec_clone = entry.clone();
                    drop(cache);
                    let filename = format!("{}.json", id.0);
                    atomic_write_json(&self.containers_dir, &filename, &rec_clone)?;
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    fn remove_container(&self, id: &ContainerId) -> Result<()> {
        {
            let cache = self.cache.lock().unwrap();
            match cache.containers.get(id) {
                None => return Ok(()), // idempotent: already gone
                Some(r) if r.state == ContainerState::Running => {
                    return Err(BackendError::FailedPrecondition(format!(
                        "container {} is Running; stop it first",
                        id.0
                    )));
                }
                _ => {}
            }
        }
        {
            let mut cache = self.cache.lock().unwrap();
            cache.containers.remove(id);
        }
        let filename = format!("{}.json", id.0);
        let path = self.containers_dir.join(&filename);
        let _ = fs::remove_file(path);
        Ok(())
    }

    fn container_status(&self, id: &ContainerId) -> Result<ContainerStatus> {
        let cache = self.cache.lock().unwrap();
        cache
            .containers
            .get(id)
            .map(rec_to_status)
            .ok_or_else(|| BackendError::NotFound(format!("container {}", id.0)))
    }

    fn list_containers(&self, filter: &ContainerFilter) -> Result<Vec<ContainerStatus>> {
        let cache = self.cache.lock().unwrap();
        Ok(cache
            .containers
            .values()
            .filter(|r| container_matches(r, filter))
            .map(rec_to_status)
            .collect())
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
                            // Kill on timeout
                            let _ = child.kill();
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
            None => return Err(BackendError::NotFound(format!("image {image_ref}"))),
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
    fn container_remove_while_running_refused() {
        let (_dir, backend) = tmp_backend();
        let sb = backend.run_sandbox(minimal_sandbox_cfg("sb")).unwrap();
        let mut cfg = minimal_container_cfg("c");
        cfg.command = vec!["/bin/sleep".to_string()];
        cfg.args = vec!["60".to_string()];
        let cid = backend.create_container(&sb, cfg).unwrap();
        backend.start_container(&cid).unwrap();

        let err = backend.remove_container(&cid).unwrap_err();
        assert!(matches!(err, BackendError::FailedPrecondition(_)));

        backend.stop_container(&cid, 0).unwrap();
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
    fn remove_image_not_found() {
        let (_dir, backend) = tmp_backend();
        let err = backend.remove_image("not:there").unwrap_err();
        assert!(matches!(err, BackendError::NotFound(_)));
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
}
