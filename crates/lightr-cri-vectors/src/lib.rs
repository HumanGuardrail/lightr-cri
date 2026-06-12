//! WP-E: conformance-vector runner (build-spec-r0 §6, extended for v1.1).
//!
//! FROZEN laws:
//! - Vector JSON shape per spec §6 (`$N` = result of step N;
//!   `expect_err` = exact BackendError variant name; `reopen_backend` step
//!   for crash-recovery scripts).
//! - Runs against `&dyn CriBackend` ONLY — never imports backend internals.
//!   These vectors are the shared integration artifact with hugr-lightr.
//! - A vector failure names the vector + step index + expected/actual.
//!
//! v1.1 additions (WP-E):
//! - `open_exec` step: drives a StreamSession (write stdin if present, read
//!   stdout to EOF, call waiter.wait() for exit code).
//! - `assert_log_exists` / `assert_log_format`: read the CRI log file at
//!   `sandbox.log_directory + "/" + container.log_path`; validate format.
//! - `sandbox_status_ip`: check sandbox_status().ip against expect_ip_present.

use std::io::Read as _;
use std::path::Path;
use std::time::{Duration, Instant};

use lightr_cri_backend::{
    BackendError, ContainerConfig, ContainerId, ContainerState, CriBackend, SandboxConfig,
    SandboxId, SandboxState,
};
use serde::Deserialize;

#[derive(Debug, Default)]
pub struct VectorReport {
    pub passed: usize,
    pub failed: Vec<String>,
}

/// Factory so each vector runs ISOLATED and crash-recovery vectors can drop
/// and reopen the same state (`reopen_backend` step). The fake rotates state
/// roots per `fresh()`; the real backend will do the same at integration.
pub trait BackendFactory {
    /// Fresh, isolated state for a new vector (no leakage between vectors).
    fn fresh(&self) -> Box<dyn CriBackend>;
    /// Reopen the state of the most recent `fresh()` (crash-recovery law).
    fn reopen(&self) -> Box<dyn CriBackend>;
}

// ── Vector JSON shape ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct Vector {
    name: String,
    steps: Vec<Step>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Step {
    RunSandbox {
        cfg: SandboxConfig,
        #[serde(default)]
        expect_err: Option<String>,
    },
    StopSandbox {
        id: String,
        #[serde(default)]
        expect_err: Option<String>,
    },
    RemoveSandbox {
        id: String,
        #[serde(default)]
        expect_err: Option<String>,
    },
    SandboxStatus {
        id: String,
        #[serde(default)]
        expect_state: Option<SandboxState>,
        #[serde(default)]
        expect_err: Option<String>,
    },
    CreateContainer {
        sandbox: String,
        cfg: ContainerConfig,
        #[serde(default)]
        expect_err: Option<String>,
    },
    StartContainer {
        id: String,
        #[serde(default)]
        expect_err: Option<String>,
    },
    StopContainer {
        id: String,
        #[serde(default)]
        grace_seconds: i64,
        #[serde(default)]
        expect_err: Option<String>,
    },
    RemoveContainer {
        id: String,
        #[serde(default)]
        expect_err: Option<String>,
    },
    AssertStatus {
        id: String,
        state: ContainerState,
        #[serde(default)]
        exit_code: Option<i32>,
        #[serde(default)]
        expect_err: Option<String>,
    },
    WaitExited {
        id: String,
        timeout_seconds: u64,
        #[serde(default)]
        expect_err: Option<String>,
    },
    ExecSync {
        id: String,
        cmd: Vec<String>,
        #[serde(default)]
        expect_exit_code: Option<i32>,
        #[serde(default)]
        expect_stdout: Option<String>,
        #[serde(default)]
        expect_err: Option<String>,
    },
    PullImage {
        #[serde(rename = "ref")]
        image_ref: String,
        /// When present, the step result is store_as_result (root_hex).
        #[serde(default)]
        store_as_result: bool,
        #[serde(default)]
        expect_err: Option<String>,
    },
    ImageStatus {
        #[serde(rename = "ref")]
        image_ref: String,
        #[serde(default)]
        expect_present: Option<bool>,
        #[serde(default)]
        expect_err: Option<String>,
    },
    ListImages {
        #[serde(default)]
        expect_count: Option<usize>,
        #[serde(default)]
        expect_err: Option<String>,
    },
    RemoveImage {
        #[serde(rename = "ref")]
        image_ref: String,
        #[serde(default)]
        expect_err: Option<String>,
    },
    ReopenBackend {},

    // ── v1.1 steps ──────────────────────────────────────────────────────────
    /// Open a streaming exec session on a running container.
    /// Drives the StreamSession: writes nothing (stdin unsupported in vectors),
    /// reads stdout to EOF, calls waiter.wait() for the exit code.
    /// NOTE: requires WP-A's open_exec implementation. Vectors using this step
    /// will fail pre-WP-A-merge with BackendError::Internal("v1.1 not
    /// implemented") — that is expected. Parser/decode unit tests pass now.
    OpenExec {
        id: String,
        cmd: Vec<String>,
        #[serde(default)]
        tty: bool,
        #[serde(default)]
        stdin: bool,
        #[serde(default)]
        expect_exit_code: Option<i32>,
        #[serde(default)]
        expect_stdout_contains: Option<String>,
        #[serde(default)]
        expect_err: Option<String>,
    },

    /// Assert that the CRI log file exists at
    /// `sandbox.log_directory + "/" + container.log_path`.
    /// Requires sandbox_id and container_id to look up the paths.
    /// NOTE: requires WP-A's log-tee implementation.
    AssertLogExists {
        sandbox_id: String,
        container_id: String,
        #[serde(default)]
        expect_err: Option<String>,
    },

    /// Assert that every line in the CRI log file matches the CRI log format:
    /// `<RFC3339Nano> <stdout|stderr> <F|P> <data>`
    /// NOTE: requires WP-A's log-tee implementation.
    AssertLogFormat {
        sandbox_id: String,
        container_id: String,
        #[serde(default)]
        expect_err: Option<String>,
    },

    /// Assert sandbox_status().ip presence/absence.
    /// `expect_ip_present: true` → ip must be Some(_).
    /// `expect_ip_present: false` → ip must be None.
    SandboxStatusIp {
        id: String,
        expect_ip_present: bool,
        #[serde(default)]
        expect_err: Option<String>,
    },
}

// ── CRI log format validation ────────────────────────────────────────────────

/// Validate a single CRI log line: `<RFC3339Nano> <stdout|stderr> <F|P> <data>`
/// Returns Ok(()) if valid, Err(description) if not.
/// Empty data ("") is allowed (F tag with no content).
fn validate_cri_log_line(line: &str) -> Result<(), String> {
    // Split into exactly 4 parts: timestamp stream tag data
    // The data part may contain spaces, so split on the first 3 whitespace runs only.
    let mut parts = line.splitn(4, char::is_whitespace);
    let timestamp = parts.next().unwrap_or("");
    let stream = parts.next().unwrap_or("");
    let tag = parts.next().unwrap_or("");
    // data (4th part) may be absent for empty lines — that's fine

    // Validate timestamp: rough RFC3339 check — must contain 'T' and end with 'Z' or offset
    if timestamp.is_empty() {
        return Err(format!("missing timestamp in line: {:?}", line));
    }
    if !timestamp.contains('T') {
        return Err(format!(
            "timestamp {:?} does not look like RFC3339 (missing 'T')",
            timestamp
        ));
    }
    // Accept 'Z' suffix or numeric offset (+HH:MM / -HH:MM)
    let ends_ok = timestamp.ends_with('Z')
        || timestamp.ends_with('z')
        || timestamp
            .chars()
            .rev()
            .nth(5)
            .map(|c| c == '+' || c == '-')
            .unwrap_or(false)
        || timestamp
            .chars()
            .rev()
            .nth(2)
            .map(|c| c == '+' || c == '-')
            .unwrap_or(false);
    if !ends_ok {
        return Err(format!(
            "timestamp {:?} does not end with 'Z' or UTC offset",
            timestamp
        ));
    }

    // Validate stream
    if stream != "stdout" && stream != "stderr" {
        return Err(format!("stream {:?} must be 'stdout' or 'stderr'", stream));
    }

    // Validate tag
    if tag != "F" && tag != "P" {
        return Err(format!("tag {:?} must be 'F' or 'P'", tag));
    }

    Ok(())
}

// ── $N substitution ──────────────────────────────────────────────────────────

fn subst(s: &str, results: &[Option<String>]) -> String {
    if let Some(rest) = s.strip_prefix('$') {
        if let Ok(idx) = rest.parse::<usize>() {
            if let Some(Some(val)) = results.get(idx) {
                return val.clone();
            }
        }
    }
    s.to_string()
}

// ── BackendError variant-name matching ───────────────────────────────────────

fn variant_name(e: &BackendError) -> &'static str {
    match e {
        BackendError::NotFound(_) => "NotFound",
        BackendError::AlreadyExists(_) => "AlreadyExists",
        BackendError::InvalidArgument(_) => "InvalidArgument",
        BackendError::FailedPrecondition(_) => "FailedPrecondition",
        BackendError::InUse(_) => "InUse",
        BackendError::Internal(_) => "Internal",
        BackendError::Io(_) => "Io",
    }
}

// ── Single vector execution ──────────────────────────────────────────────────

/// Run one vector. Returns Ok(()) on pass, Err(message) on first failure.
fn run_vector(factory: &dyn BackendFactory, vector: &Vector) -> Result<(), String> {
    let mut backend: Box<dyn CriBackend> = factory.fresh();
    // results[i] = the String result of step i (None if step produced no id)
    let mut results: Vec<Option<String>> = Vec::new();

    for (step_idx, step) in vector.steps.iter().enumerate() {
        let step_result = execute_step(&mut backend, factory, step, &results);
        match step_result {
            StepOutcome::Ok(val) => {
                results.push(val);
            }
            StepOutcome::Fail(msg) => {
                return Err(format!(
                    "vector '{}' step {}: {}",
                    vector.name, step_idx, msg
                ));
            }
        }
    }
    Ok(())
}

enum StepOutcome {
    Ok(Option<String>),
    Fail(String),
}

/// Macro-like helper: check expect_err; return StepOutcome.
fn check_err_expectation<T>(
    result: lightr_cri_backend::Result<T>,
    expect_err: &Option<String>,
    step_name: &str,
    value_extractor: impl FnOnce(T) -> Option<String>,
) -> StepOutcome {
    match (result, expect_err) {
        (Ok(val), None) => StepOutcome::Ok(value_extractor(val)),
        (Ok(_), Some(expected)) => StepOutcome::Fail(format!(
            "{step_name}: expected error '{expected}' but call succeeded"
        )),
        (Err(e), None) => StepOutcome::Fail(format!("{step_name}: unexpected error: {e}")),
        (Err(e), Some(expected)) => {
            let actual = variant_name(&e);
            if actual == expected.as_str() {
                StepOutcome::Ok(None)
            } else {
                StepOutcome::Fail(format!(
                    "{step_name}: expected error '{expected}', got '{actual}': {e}"
                ))
            }
        }
    }
}

fn execute_step(
    backend: &mut Box<dyn CriBackend>,
    factory: &dyn BackendFactory,
    step: &Step,
    results: &[Option<String>],
) -> StepOutcome {
    match step {
        Step::RunSandbox { cfg, expect_err } => {
            let result = backend.run_sandbox(cfg.clone());
            check_err_expectation(result, expect_err, "run_sandbox", |id| Some(id.0))
        }

        Step::StopSandbox { id, expect_err } => {
            let sid = SandboxId(subst(id, results));
            let result = backend.stop_sandbox(&sid);
            check_err_expectation(result, expect_err, "stop_sandbox", |_| None)
        }

        Step::RemoveSandbox { id, expect_err } => {
            let sid = SandboxId(subst(id, results));
            let result = backend.remove_sandbox(&sid);
            check_err_expectation(result, expect_err, "remove_sandbox", |_| None)
        }

        Step::SandboxStatus {
            id,
            expect_state,
            expect_err,
        } => {
            let sid = SandboxId(subst(id, results));
            match backend.sandbox_status(&sid) {
                Ok(status) => {
                    if expect_err.is_some() {
                        return StepOutcome::Fail(format!(
                            "sandbox_status: expected error '{}' but call succeeded",
                            expect_err.as_ref().unwrap()
                        ));
                    }
                    if let Some(expected) = expect_state {
                        if status.state != *expected {
                            return StepOutcome::Fail(format!(
                                "sandbox_status: expected state {:?}, got {:?}",
                                expected, status.state
                            ));
                        }
                    }
                    StepOutcome::Ok(None)
                }
                Err(e) => match expect_err {
                    None => StepOutcome::Fail(format!("sandbox_status: unexpected error: {e}")),
                    Some(expected) => {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            StepOutcome::Ok(None)
                        } else {
                            StepOutcome::Fail(format!(
                                "sandbox_status: expected error '{expected}', got '{actual}': {e}"
                            ))
                        }
                    }
                },
            }
        }

        Step::CreateContainer {
            sandbox,
            cfg,
            expect_err,
        } => {
            let sid = SandboxId(subst(sandbox, results));
            let result = backend.create_container(&sid, cfg.clone());
            check_err_expectation(result, expect_err, "create_container", |id| Some(id.0))
        }

        Step::StartContainer { id, expect_err } => {
            let cid = ContainerId(subst(id, results));
            let result = backend.start_container(&cid);
            check_err_expectation(result, expect_err, "start_container", |_| None)
        }

        Step::StopContainer {
            id,
            grace_seconds,
            expect_err,
        } => {
            let cid = ContainerId(subst(id, results));
            let result = backend.stop_container(&cid, *grace_seconds);
            check_err_expectation(result, expect_err, "stop_container", |_| None)
        }

        Step::RemoveContainer { id, expect_err } => {
            let cid = ContainerId(subst(id, results));
            let result = backend.remove_container(&cid);
            check_err_expectation(result, expect_err, "remove_container", |_| None)
        }

        Step::AssertStatus {
            id,
            state,
            exit_code,
            expect_err,
        } => {
            let cid = ContainerId(subst(id, results));
            match backend.container_status(&cid) {
                Ok(status) => {
                    if expect_err.is_some() {
                        return StepOutcome::Fail(format!(
                            "assert_status: expected error '{}' but call succeeded",
                            expect_err.as_ref().unwrap()
                        ));
                    }
                    if status.state != *state {
                        return StepOutcome::Fail(format!(
                            "assert_status: expected state {:?}, got {:?}",
                            state, status.state
                        ));
                    }
                    if let Some(expected_code) = exit_code {
                        if status.exit_code != *expected_code {
                            return StepOutcome::Fail(format!(
                                "assert_status: expected exit_code {}, got {}",
                                expected_code, status.exit_code
                            ));
                        }
                    }
                    StepOutcome::Ok(None)
                }
                Err(e) => match expect_err {
                    None => StepOutcome::Fail(format!("assert_status: unexpected error: {e}")),
                    Some(expected) => {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            StepOutcome::Ok(None)
                        } else {
                            StepOutcome::Fail(format!(
                                "assert_status: expected error '{expected}', got '{actual}': {e}"
                            ))
                        }
                    }
                },
            }
        }

        Step::WaitExited {
            id,
            timeout_seconds,
            expect_err,
        } => {
            let cid = ContainerId(subst(id, results));
            let deadline = Instant::now() + Duration::from_secs(*timeout_seconds);
            loop {
                match backend.container_status(&cid) {
                    Ok(status) => {
                        if status.state == ContainerState::Exited {
                            if expect_err.is_some() {
                                return StepOutcome::Fail(format!(
                                    "wait_exited: expected error '{}' but container exited",
                                    expect_err.as_ref().unwrap()
                                ));
                            }
                            return StepOutcome::Ok(None);
                        }
                        if Instant::now() >= deadline {
                            return StepOutcome::Fail(format!(
                                "wait_exited: timeout after {}s — state was {:?}",
                                timeout_seconds, status.state
                            ));
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(e) => match expect_err {
                        None => {
                            return StepOutcome::Fail(format!("wait_exited: unexpected error: {e}"))
                        }
                        Some(expected) => {
                            let actual = variant_name(&e);
                            if actual == expected.as_str() {
                                return StepOutcome::Ok(None);
                            } else {
                                return StepOutcome::Fail(format!(
                                    "wait_exited: expected error '{expected}', got '{actual}': {e}"
                                ));
                            }
                        }
                    },
                }
            }
        }

        Step::ExecSync {
            id,
            cmd,
            expect_exit_code,
            expect_stdout,
            expect_err,
        } => {
            let cid = ContainerId(subst(id, results));
            let result = backend.exec_sync(&cid, cmd, 30);
            match result {
                Ok(exec_result) => {
                    if expect_err.is_some() {
                        return StepOutcome::Fail(format!(
                            "exec_sync: expected error '{}' but call succeeded",
                            expect_err.as_ref().unwrap()
                        ));
                    }
                    if let Some(expected_code) = expect_exit_code {
                        if exec_result.exit_code != *expected_code {
                            return StepOutcome::Fail(format!(
                                "exec_sync: expected exit_code {}, got {}",
                                expected_code, exec_result.exit_code
                            ));
                        }
                    }
                    if let Some(expected_out) = expect_stdout {
                        let actual_out = String::from_utf8_lossy(&exec_result.stdout).into_owned();
                        let actual_trimmed = actual_out.trim_end_matches('\n');
                        let expected_trimmed = expected_out.trim_end_matches('\n');
                        if actual_trimmed != expected_trimmed {
                            return StepOutcome::Fail(format!(
                                "exec_sync: expected stdout {:?}, got {:?}",
                                expected_out, actual_out
                            ));
                        }
                    }
                    StepOutcome::Ok(None)
                }
                Err(e) => match expect_err {
                    None => StepOutcome::Fail(format!("exec_sync: unexpected error: {e}")),
                    Some(expected) => {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            StepOutcome::Ok(None)
                        } else {
                            StepOutcome::Fail(format!(
                                "exec_sync: expected error '{expected}', got '{actual}': {e}"
                            ))
                        }
                    }
                },
            }
        }

        Step::PullImage {
            image_ref,
            store_as_result,
            expect_err,
        } => {
            let result = backend.pull_image(image_ref);
            check_err_expectation(result, expect_err, "pull_image", |pulled| {
                if *store_as_result {
                    Some(pulled.root_hex)
                } else {
                    None
                }
            })
        }

        Step::ImageStatus {
            image_ref,
            expect_present,
            expect_err,
        } => {
            let result = backend.image_status(image_ref);
            match result {
                Ok(maybe_record) => {
                    if expect_err.is_some() {
                        return StepOutcome::Fail(format!(
                            "image_status: expected error '{}' but call succeeded",
                            expect_err.as_ref().unwrap()
                        ));
                    }
                    if let Some(expected) = expect_present {
                        let present = maybe_record.is_some();
                        if present != *expected {
                            return StepOutcome::Fail(format!(
                                "image_status: expected present={}, got present={}",
                                expected, present
                            ));
                        }
                    }
                    StepOutcome::Ok(None)
                }
                Err(e) => match expect_err {
                    None => StepOutcome::Fail(format!("image_status: unexpected error: {e}")),
                    Some(expected) => {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            StepOutcome::Ok(None)
                        } else {
                            StepOutcome::Fail(format!(
                                "image_status: expected error '{expected}', got '{actual}': {e}"
                            ))
                        }
                    }
                },
            }
        }

        Step::ListImages {
            expect_count,
            expect_err,
        } => {
            let result = backend.list_images();
            match result {
                Ok(images) => {
                    if expect_err.is_some() {
                        return StepOutcome::Fail(format!(
                            "list_images: expected error '{}' but call succeeded",
                            expect_err.as_ref().unwrap()
                        ));
                    }
                    if let Some(expected_count) = expect_count {
                        if images.len() != *expected_count {
                            return StepOutcome::Fail(format!(
                                "list_images: expected {} images, got {}",
                                expected_count,
                                images.len()
                            ));
                        }
                    }
                    StepOutcome::Ok(None)
                }
                Err(e) => match expect_err {
                    None => StepOutcome::Fail(format!("list_images: unexpected error: {e}")),
                    Some(expected) => {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            StepOutcome::Ok(None)
                        } else {
                            StepOutcome::Fail(format!(
                                "list_images: expected error '{expected}', got '{actual}': {e}"
                            ))
                        }
                    }
                },
            }
        }

        Step::RemoveImage {
            image_ref,
            expect_err,
        } => {
            let result = backend.remove_image(image_ref);
            check_err_expectation(result, expect_err, "remove_image", |_| None)
        }

        Step::ReopenBackend {} => {
            *backend = factory.reopen();
            StepOutcome::Ok(None)
        }

        // ── v1.1 steps ───────────────────────────────────────────────────────
        Step::OpenExec {
            id,
            cmd,
            tty,
            stdin,
            expect_exit_code,
            expect_stdout_contains,
            expect_err,
        } => {
            let cid = ContainerId(subst(id, results));
            let result = backend.open_exec(&cid, cmd, *tty, *stdin);
            match result {
                Err(e) => match expect_err {
                    None => StepOutcome::Fail(format!("open_exec: unexpected error: {e}")),
                    Some(expected) => {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            StepOutcome::Ok(None)
                        } else {
                            StepOutcome::Fail(format!(
                                "open_exec: expected error '{expected}', got '{actual}': {e}"
                            ))
                        }
                    }
                },
                Ok(mut session) => {
                    if let Some(expected_err) = expect_err {
                        return StepOutcome::Fail(format!(
                            "open_exec: expected error '{expected_err}' but call succeeded"
                        ));
                    }
                    // Drop stdin handle (we don't write to it in vectors)
                    drop(session.stdin.take());

                    // Read stdout to EOF
                    let stdout_bytes = if let Some(mut stdout_file) = session.stdout.take() {
                        let mut buf = Vec::new();
                        if let Err(e) = stdout_file.read_to_end(&mut buf) {
                            return StepOutcome::Fail(format!("open_exec: read stdout: {e}"));
                        }
                        buf
                    } else {
                        Vec::new()
                    };

                    // Drain stderr (ignore content)
                    drop(session.stderr.take());
                    drop(session.pty_master.take());

                    // Wait for exit code
                    let exit_code = match session.waiter.wait() {
                        Ok(code) => code,
                        Err(e) => {
                            return StepOutcome::Fail(format!("open_exec: waiter.wait(): {e}"));
                        }
                    };

                    if let Some(expected_code) = expect_exit_code {
                        if exit_code != *expected_code {
                            return StepOutcome::Fail(format!(
                                "open_exec: expected exit_code {}, got {}",
                                expected_code, exit_code
                            ));
                        }
                    }

                    if let Some(needle) = expect_stdout_contains {
                        let stdout_str = String::from_utf8_lossy(&stdout_bytes);
                        if !stdout_str.contains(needle.as_str()) {
                            return StepOutcome::Fail(format!(
                                "open_exec: stdout {:?} does not contain {:?}",
                                stdout_str, needle
                            ));
                        }
                    }

                    StepOutcome::Ok(None)
                }
            }
        }

        Step::AssertLogExists {
            sandbox_id,
            container_id,
            expect_err,
        } => {
            let sid = SandboxId(subst(sandbox_id, results));
            let cid = ContainerId(subst(container_id, results));

            // Look up sandbox to get log_directory
            let sandbox_status = match backend.sandbox_status(&sid) {
                Ok(s) => s,
                Err(e) => {
                    if let Some(expected) = expect_err {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            return StepOutcome::Ok(None);
                        }
                        return StepOutcome::Fail(format!(
                            "assert_log_exists: sandbox_status error: expected '{expected}', got '{actual}': {e}"
                        ));
                    }
                    return StepOutcome::Fail(format!(
                        "assert_log_exists: sandbox_status error: {e}"
                    ));
                }
            };

            // Look up container to get log_path
            let container_status = match backend.container_status(&cid) {
                Ok(s) => s,
                Err(e) => {
                    if let Some(expected) = expect_err {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            return StepOutcome::Ok(None);
                        }
                        return StepOutcome::Fail(format!(
                            "assert_log_exists: container_status error: expected '{expected}', got '{actual}': {e}"
                        ));
                    }
                    return StepOutcome::Fail(format!(
                        "assert_log_exists: container_status error: {e}"
                    ));
                }
            };

            if expect_err.is_some() {
                return StepOutcome::Fail(
                    "assert_log_exists: expected error but lookups succeeded".to_string(),
                );
            }

            let log_dir = &sandbox_status.config.log_directory;
            let log_path = &container_status.config.log_path;

            if log_dir.is_empty() || log_path.is_empty() {
                return StepOutcome::Fail(format!(
                    "assert_log_exists: log_directory={:?} or log_path={:?} is empty",
                    log_dir, log_path
                ));
            }

            let full_path = std::path::Path::new(log_dir).join(log_path);
            if !full_path.exists() {
                return StepOutcome::Fail(format!(
                    "assert_log_exists: log file {:?} does not exist",
                    full_path
                ));
            }

            StepOutcome::Ok(None)
        }

        Step::AssertLogFormat {
            sandbox_id,
            container_id,
            expect_err,
        } => {
            let sid = SandboxId(subst(sandbox_id, results));
            let cid = ContainerId(subst(container_id, results));

            let sandbox_status = match backend.sandbox_status(&sid) {
                Ok(s) => s,
                Err(e) => {
                    if let Some(expected) = expect_err {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            return StepOutcome::Ok(None);
                        }
                        return StepOutcome::Fail(format!(
                            "assert_log_format: sandbox_status error: expected '{expected}', got '{actual}': {e}"
                        ));
                    }
                    return StepOutcome::Fail(format!(
                        "assert_log_format: sandbox_status error: {e}"
                    ));
                }
            };

            let container_status = match backend.container_status(&cid) {
                Ok(s) => s,
                Err(e) => {
                    if let Some(expected) = expect_err {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            return StepOutcome::Ok(None);
                        }
                        return StepOutcome::Fail(format!(
                            "assert_log_format: container_status error: expected '{expected}', got '{actual}': {e}"
                        ));
                    }
                    return StepOutcome::Fail(format!(
                        "assert_log_format: container_status error: {e}"
                    ));
                }
            };

            if expect_err.is_some() {
                return StepOutcome::Fail(
                    "assert_log_format: expected error but lookups succeeded".to_string(),
                );
            }

            let log_dir = &sandbox_status.config.log_directory;
            let log_path = &container_status.config.log_path;

            if log_dir.is_empty() || log_path.is_empty() {
                return StepOutcome::Fail(format!(
                    "assert_log_format: log_directory={:?} or log_path={:?} is empty",
                    log_dir, log_path
                ));
            }

            let full_path = std::path::Path::new(log_dir).join(log_path);
            let content = match std::fs::read_to_string(&full_path) {
                Ok(c) => c,
                Err(e) => {
                    return StepOutcome::Fail(format!(
                        "assert_log_format: read {:?}: {e}",
                        full_path
                    ));
                }
            };

            // Validate each non-empty line
            for (line_no, line) in content.lines().enumerate() {
                if line.is_empty() {
                    continue;
                }
                if let Err(msg) = validate_cri_log_line(line) {
                    return StepOutcome::Fail(format!(
                        "assert_log_format: line {}: {msg}",
                        line_no + 1
                    ));
                }
            }

            StepOutcome::Ok(None)
        }

        Step::SandboxStatusIp {
            id,
            expect_ip_present,
            expect_err,
        } => {
            let sid = SandboxId(subst(id, results));
            match backend.sandbox_status(&sid) {
                Ok(status) => {
                    if let Some(expected_err) = expect_err {
                        return StepOutcome::Fail(format!(
                            "sandbox_status_ip: expected error '{expected_err}' but call succeeded"
                        ));
                    }
                    let ip_present = status.ip.is_some();
                    if ip_present != *expect_ip_present {
                        return StepOutcome::Fail(format!(
                            "sandbox_status_ip: expected ip_present={}, got ip_present={} (ip={:?})",
                            expect_ip_present, ip_present, status.ip
                        ));
                    }
                    StepOutcome::Ok(None)
                }
                Err(e) => match expect_err {
                    None => StepOutcome::Fail(format!("sandbox_status_ip: unexpected error: {e}")),
                    Some(expected) => {
                        let actual = variant_name(&e);
                        if actual == expected.as_str() {
                            StepOutcome::Ok(None)
                        } else {
                            StepOutcome::Fail(format!(
                                "sandbox_status_ip: expected error '{expected}', got '{actual}': {e}"
                            ))
                        }
                    }
                },
            }
        }
    }
}

// ── Public entry point ───────────────────────────────────────────────────────

pub fn run_vectors(factory: &dyn BackendFactory, dir: &Path) -> std::io::Result<VectorReport> {
    let mut report = VectorReport::default();

    let mut entries: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .collect();
    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let path = entry.path();
        let raw = std::fs::read_to_string(&path)?;
        let vector: Vector = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(e) => {
                report
                    .failed
                    .push(format!("parse error in {}: {e}", path.display()));
                continue;
            }
        };
        match run_vector(factory, &vector) {
            Ok(()) => report.passed += 1,
            Err(msg) => report.failed.push(msg),
        }
    }

    Ok(report)
}

// ── Parser / substitution unit tests ────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subst_replaces_dollar_n() {
        let results = vec![Some("sandbox-abc".to_string()), Some("ctr-xyz".to_string())];
        assert_eq!(subst("$0", &results), "sandbox-abc");
        assert_eq!(subst("$1", &results), "ctr-xyz");
        assert_eq!(subst("plain", &results), "plain");
    }

    #[test]
    fn subst_out_of_range_returns_literal() {
        let results: Vec<Option<String>> = vec![];
        assert_eq!(subst("$0", &results), "$0");
    }

    #[test]
    fn subst_none_slot_returns_literal() {
        let results: Vec<Option<String>> = vec![None];
        assert_eq!(subst("$0", &results), "$0");
    }

    #[test]
    fn variant_name_coverage() {
        assert_eq!(
            variant_name(&BackendError::NotFound("x".into())),
            "NotFound"
        );
        assert_eq!(
            variant_name(&BackendError::AlreadyExists("x".into())),
            "AlreadyExists"
        );
        assert_eq!(
            variant_name(&BackendError::InvalidArgument("x".into())),
            "InvalidArgument"
        );
        assert_eq!(
            variant_name(&BackendError::FailedPrecondition("x".into())),
            "FailedPrecondition"
        );
        assert_eq!(variant_name(&BackendError::InUse("x".into())), "InUse");
        assert_eq!(
            variant_name(&BackendError::Internal("x".into())),
            "Internal"
        );
        assert_eq!(
            variant_name(&BackendError::Io(std::io::Error::other("e"))),
            "Io"
        );
    }

    #[test]
    fn parse_run_sandbox_step() {
        let json = r#"{
            "name": "test",
            "steps": [
                {"op": "run_sandbox", "cfg": {"name": "s1", "uid": "u1", "namespace": "ns", "attempt": 0}}
            ]
        }"#;
        let v: Vector = serde_json::from_str(json).expect("parse");
        assert_eq!(v.name, "test");
        assert_eq!(v.steps.len(), 1);
    }

    #[test]
    fn parse_expect_err_field() {
        let json = r#"{
            "name": "t",
            "steps": [
                {"op": "remove_container", "id": "$1", "expect_err": "FailedPrecondition"}
            ]
        }"#;
        let v: Vector = serde_json::from_str(json).expect("parse");
        match &v.steps[0] {
            Step::RemoveContainer { expect_err, .. } => {
                assert_eq!(expect_err.as_deref(), Some("FailedPrecondition"));
            }
            _ => panic!("wrong step variant"),
        }
    }

    #[test]
    fn parse_wait_exited_step() {
        let json = r#"{
            "name": "t",
            "steps": [
                {"op": "wait_exited", "id": "$1", "timeout_seconds": 5}
            ]
        }"#;
        let v: Vector = serde_json::from_str(json).expect("parse");
        match &v.steps[0] {
            Step::WaitExited {
                timeout_seconds, ..
            } => {
                assert_eq!(*timeout_seconds, 5);
            }
            _ => panic!("wrong step variant"),
        }
    }

    #[test]
    fn parse_exec_sync_step() {
        let json = r#"{
            "name": "t",
            "steps": [
                {"op": "exec_sync", "id": "$1", "cmd": ["/bin/echo", "hi"],
                 "expect_exit_code": 0, "expect_stdout": "hi"}
            ]
        }"#;
        let v: Vector = serde_json::from_str(json).expect("parse");
        match &v.steps[0] {
            Step::ExecSync {
                cmd,
                expect_exit_code,
                expect_stdout,
                ..
            } => {
                assert_eq!(cmd, &["/bin/echo", "hi"]);
                assert_eq!(*expect_exit_code, Some(0));
                assert_eq!(expect_stdout.as_deref(), Some("hi"));
            }
            _ => panic!("wrong step variant"),
        }
    }

    // ── v1.1 parser unit tests ────────────────────────────────────────────────

    #[test]
    fn parse_open_exec_step() {
        let json = r#"{
            "name": "t",
            "steps": [
                {
                    "op": "open_exec",
                    "id": "$1",
                    "cmd": ["/bin/echo", "hi"],
                    "expect_exit_code": 0,
                    "expect_stdout_contains": "hi"
                }
            ]
        }"#;
        let v: Vector = serde_json::from_str(json).expect("parse");
        match &v.steps[0] {
            Step::OpenExec {
                cmd,
                expect_exit_code,
                expect_stdout_contains,
                tty,
                stdin,
                ..
            } => {
                assert_eq!(cmd, &["/bin/echo", "hi"]);
                assert_eq!(*expect_exit_code, Some(0));
                assert_eq!(expect_stdout_contains.as_deref(), Some("hi"));
                assert!(!tty);
                assert!(!stdin);
            }
            _ => panic!("wrong step variant"),
        }
    }

    #[test]
    fn parse_open_exec_with_tty() {
        let json = r#"{
            "name": "t",
            "steps": [
                {"op": "open_exec", "id": "$1", "cmd": ["/bin/sh"], "tty": true, "stdin": true}
            ]
        }"#;
        let v: Vector = serde_json::from_str(json).expect("parse");
        match &v.steps[0] {
            Step::OpenExec { tty, stdin, .. } => {
                assert!(*tty);
                assert!(*stdin);
            }
            _ => panic!("wrong step variant"),
        }
    }

    #[test]
    fn parse_assert_log_exists_step() {
        let json = r#"{
            "name": "t",
            "steps": [
                {"op": "assert_log_exists", "sandbox_id": "$0", "container_id": "$1"}
            ]
        }"#;
        let v: Vector = serde_json::from_str(json).expect("parse");
        assert!(matches!(v.steps[0], Step::AssertLogExists { .. }));
    }

    #[test]
    fn parse_assert_log_format_step() {
        let json = r#"{
            "name": "t",
            "steps": [
                {"op": "assert_log_format", "sandbox_id": "$0", "container_id": "$1"}
            ]
        }"#;
        let v: Vector = serde_json::from_str(json).expect("parse");
        assert!(matches!(v.steps[0], Step::AssertLogFormat { .. }));
    }

    #[test]
    fn parse_sandbox_status_ip_step() {
        let json = r#"{
            "name": "t",
            "steps": [
                {"op": "sandbox_status_ip", "id": "$0", "expect_ip_present": false}
            ]
        }"#;
        let v: Vector = serde_json::from_str(json).expect("parse");
        match &v.steps[0] {
            Step::SandboxStatusIp {
                expect_ip_present, ..
            } => {
                assert!(!expect_ip_present);
            }
            _ => panic!("wrong step variant"),
        }
    }

    // ── CRI log format validator unit tests ───────────────────────────────────

    #[test]
    fn cri_log_line_valid_full_stdout() {
        // Full line, stdout
        assert!(
            validate_cri_log_line("2026-06-12T10:00:00.000000000Z stdout F hello world").is_ok()
        );
    }

    #[test]
    fn cri_log_line_valid_partial_stderr() {
        // Partial line, stderr
        assert!(
            validate_cri_log_line("2026-06-12T10:00:00.123456789Z stderr P some partial data")
                .is_ok()
        );
    }

    #[test]
    fn cri_log_line_valid_with_offset() {
        // UTC offset instead of Z
        assert!(validate_cri_log_line("2026-06-12T10:00:00+00:00 stdout F data").is_ok());
    }

    #[test]
    fn cri_log_line_invalid_stream() {
        let err = validate_cri_log_line("2026-06-12T10:00:00.000Z stdin F data");
        assert!(err.is_err(), "expected error for invalid stream");
    }

    #[test]
    fn cri_log_line_invalid_tag() {
        let err = validate_cri_log_line("2026-06-12T10:00:00.000Z stdout X data");
        assert!(err.is_err(), "expected error for invalid tag");
    }

    #[test]
    fn cri_log_line_missing_timestamp_t() {
        let err = validate_cri_log_line("2026-06-12 10:00:00Z stdout F data");
        assert!(err.is_err(), "expected error for missing T separator");
    }

    #[test]
    fn cri_log_line_empty_is_skipped_by_validator_directly() {
        // The validator itself is not called on empty lines (the executor skips
        // them), but validate_cri_log_line("") should return an error.
        let err = validate_cri_log_line("");
        assert!(err.is_err(), "empty line should return error");
    }

    #[test]
    fn parse_all_example_vectors() {
        // Verify the two frozen examples parse without error.
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
        let vectors_dir = std::path::Path::new(&manifest_dir).join("../../vectors");
        if !vectors_dir.exists() {
            return; // skip if not in repo context
        }
        for entry in std::fs::read_dir(&vectors_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().map(|e| e == "json").unwrap_or(false) {
                let raw = std::fs::read_to_string(&path).unwrap();
                let result: Result<Vector, _> = serde_json::from_str(&raw);
                assert!(
                    result.is_ok(),
                    "Failed to parse {}: {:?}",
                    path.display(),
                    result.err()
                );
            }
        }
    }

    // ── Integration test: vectors_pass_on_fake ────────────────────────────
    // This test is gated on lightr-cri-fake (dev-dep). It will only PASS
    // once WP-1 lands; it MUST COMPILE now.
    #[cfg(test)]
    mod fake_integration {
        use super::*;
        use lightr_cri_fake::FakeBackend;
        use tempfile::TempDir;

        struct FakeFactory {
            base: std::path::PathBuf,
            counter: std::sync::Mutex<u64>,
            current: std::sync::Mutex<std::path::PathBuf>,
        }

        impl BackendFactory for FakeFactory {
            fn fresh(&self) -> Box<dyn CriBackend> {
                let mut c = self.counter.lock().unwrap();
                *c += 1;
                let root = self.base.join(format!("vec-{}", *c));
                *self.current.lock().unwrap() = root.clone();
                Box::new(FakeBackend::open(&root).expect("FakeBackend::open"))
            }
            fn reopen(&self) -> Box<dyn CriBackend> {
                let root = self.current.lock().unwrap().clone();
                Box::new(FakeBackend::open(&root).expect("FakeBackend::reopen"))
            }
        }

        #[test]
        fn vectors_pass_on_fake() {
            let tmp = TempDir::new().expect("tempdir");
            let factory = FakeFactory {
                base: tmp.path().to_path_buf(),
                counter: std::sync::Mutex::new(0),
                current: std::sync::Mutex::new(tmp.path().to_path_buf()),
            };

            let manifest_dir =
                std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
            let vectors_dir = std::path::Path::new(&manifest_dir).join("../../vectors");

            let report = run_vectors(&factory, &vectors_dir).expect("run_vectors io error");

            if !report.failed.is_empty() {
                for failure in &report.failed {
                    eprintln!("FAILED: {failure}");
                }
                panic!("{} vector(s) failed (see above)", report.failed.len());
            }
        }
    }
}
