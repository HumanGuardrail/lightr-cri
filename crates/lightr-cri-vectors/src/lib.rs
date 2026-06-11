//! WP-4: conformance-vector runner (build-spec-r0 §6).
//!
//! FROZEN laws:
//! - Vector JSON shape per spec §6 (`$N` = result of step N;
//!   `expect_err` = exact BackendError variant name; `reopen_backend` step
//!   for crash-recovery scripts).
//! - Runs against `&dyn CriBackend` ONLY — never imports backend internals.
//!   These vectors are the shared integration artifact with hugr-lightr.
//! - A vector failure names the vector + step index + expected/actual.

use std::path::Path;
use std::time::{Duration, Instant};

use lightr_cri_backend::{
    BackendError, CriBackend, ContainerConfig, ContainerState,
    ContainerId, SandboxConfig, SandboxId, SandboxState,
};
use serde::Deserialize;

#[derive(Debug, Default)]
pub struct VectorReport {
    pub passed: usize,
    pub failed: Vec<String>,
}

/// Factory so each vector runs ISOLATED and crash-recovery vectors can drop
/// + reopen the same state (`reopen_backend` step). The fake rotates state
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
fn run_vector(
    factory: &dyn BackendFactory,
    vector: &Vector,
) -> Result<(), String> {
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
        (Err(e), None) => StepOutcome::Fail(format!(
            "{step_name}: unexpected error: {e}"
        )),
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
            check_err_expectation(result, expect_err, "run_sandbox", |id| {
                Some(id.0)
            })
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
            check_err_expectation(result, expect_err, "create_container", |id| {
                Some(id.0)
            })
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
                            return StepOutcome::Fail(format!(
                                "wait_exited: unexpected error: {e}"
                            ))
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
                        let actual_out =
                            String::from_utf8_lossy(&exec_result.stdout).into_owned();
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
                report.failed.push(format!(
                    "parse error in {}: {e}",
                    path.display()
                ));
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
        assert_eq!(variant_name(&BackendError::NotFound("x".into())), "NotFound");
        assert_eq!(variant_name(&BackendError::AlreadyExists("x".into())), "AlreadyExists");
        assert_eq!(variant_name(&BackendError::InvalidArgument("x".into())), "InvalidArgument");
        assert_eq!(variant_name(&BackendError::FailedPrecondition("x".into())), "FailedPrecondition");
        assert_eq!(variant_name(&BackendError::InUse("x".into())), "InUse");
        assert_eq!(variant_name(&BackendError::Internal("x".into())), "Internal");
        assert_eq!(
            variant_name(&BackendError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "e"
            ))),
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
            Step::WaitExited { timeout_seconds, .. } => {
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

    #[test]
    fn parse_all_example_vectors() {
        // Verify the two frozen examples parse without error.
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
        let vectors_dir = std::path::Path::new(&manifest_dir)
            .join("../../vectors");
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
            let vectors_dir =
                std::path::Path::new(&manifest_dir).join("../../vectors");

            let report = run_vectors(&factory, &vectors_dir).expect("run_vectors io error");

            if !report.failed.is_empty() {
                for failure in &report.failed {
                    eprintln!("FAILED: {failure}");
                }
                panic!(
                    "{} vector(s) failed (see above)",
                    report.failed.len()
                );
            }
        }
    }
}
