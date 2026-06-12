//! WP-5: Acceptance tests A1–A8.
//! Each test is probe-gated: missing crictl or binary → loud SKIP, test passes.
//! Unix-only (cfg guard at item level).

use lightr_cri_acceptance::{crictl, find_server_bin, have, skip, ServerHandle};
use std::path::{Path, PathBuf};

// ── probe helpers ────────────────────────────────────────────────────────────

/// Returns true when we are on a Unix platform (cfg check for the rest).
fn is_unix() -> bool {
    cfg!(unix)
}

/// Common probes: platform, crictl in PATH, binary exists.
macro_rules! common_probes {
    ($item:expr) => {{
        if !is_unix() {
            skip($item, "non-Unix platform");
            return;
        }
        if !have("crictl") {
            skip($item, "crictl not in PATH");
            return;
        }
        let bin = match find_server_bin() {
            Some(b) => b,
            None => {
                skip(
                    $item,
                    "lightr-cri binary not found (build first or set LIGHTR_CRI_BIN)",
                );
                return;
            }
        };
        bin
    }};
}

// ── JSON helpers ─────────────────────────────────────────────────────────────

/// Write a minimal pod sandbox config JSON to a temp file and return its path.
fn write_pod_json(dir: &Path, name: &str, uid: &str) -> PathBuf {
    let json = serde_json::json!({
        "metadata": {
            "name": name,
            "uid": uid,
            "namespace": "acceptance",
            "attempt": 0
        },
        "logDirectory": "/tmp",
        "linux": {}
    });
    let path = dir.join("pod.json");
    std::fs::write(&path, json.to_string()).expect("write pod.json");
    path
}

/// Write a minimal container config JSON to a temp file.
fn write_container_json(dir: &Path, name: &str, image: &str, cmd: &[&str]) -> PathBuf {
    let command: Vec<serde_json::Value> = cmd
        .iter()
        .map(|s| serde_json::Value::String(s.to_string()))
        .collect();
    let json = serde_json::json!({
        "metadata": {
            "name": name,
            "attempt": 0
        },
        "image": {
            "image": image
        },
        "command": command,
        "linux": {}
    });
    let path = dir.join(format!("{name}-container.json"));
    std::fs::write(&path, json.to_string()).expect("write container.json");
    path
}

fn ctl(sock: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    crictl(sock, args)
}

fn trim_output(b: &[u8]) -> String {
    String::from_utf8_lossy(b).trim().to_string()
}

fn tempfile_dir(tag: &str) -> PathBuf {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let d = std::env::temp_dir().join(format!("lightr-cri-{tag}-{id}"));
    std::fs::create_dir_all(&d).expect("create temp dir");
    d
}

fn cleanup_dir(d: &PathBuf) {
    let _ = std::fs::remove_dir_all(d);
}

// ── A1: version ──────────────────────────────────────────────────────────────

#[test]
fn a1_version() {
    let bin = common_probes!("A1");
    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let out = ctl(&srv.socket, &["version"]).expect("crictl version");
    assert!(
        out.status.success(),
        "A1: crictl version exit {}\nstdout: {}\nstderr: {}",
        out.status,
        trim_output(&out.stdout),
        trim_output(&out.stderr),
    );
    let stdout = trim_output(&out.stdout);
    assert!(
        stdout.to_lowercase().contains("lightr"),
        "A1: stdout does not contain 'lightr': {stdout}",
    );
}

// ── A2: sandbox lifecycle ─────────────────────────────────────────────────────

#[test]
fn a2_sandbox_lifecycle() {
    let bin = common_probes!("A2");
    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let tmpdir = tempfile_dir("a2");

    let pod_json = write_pod_json(&tmpdir, "a2-pod", "a2-uid-0001");

    // runp
    let out = ctl(&srv.socket, &["runp", pod_json.to_str().unwrap()]).expect("crictl runp");
    assert!(
        out.status.success(),
        "A2: runp failed\n{}",
        trim_output(&out.stderr)
    );
    let pod_id = trim_output(&out.stdout);
    assert!(!pod_id.is_empty(), "A2: runp returned empty id");

    // pods
    let out = ctl(&srv.socket, &["pods"]).expect("crictl pods");
    assert!(out.status.success(), "A2: pods failed");
    let pods_out = trim_output(&out.stdout);
    let prefix = &pod_id[..pod_id.len().min(12)];
    assert!(
        pods_out.contains(prefix),
        "A2: pods output does not contain id prefix {prefix}: {pods_out}",
    );

    // stopp
    let out = ctl(&srv.socket, &["stopp", &pod_id]).expect("crictl stopp");
    assert!(
        out.status.success(),
        "A2: stopp failed\n{}",
        trim_output(&out.stderr)
    );

    // rmp
    let out = ctl(&srv.socket, &["rmp", &pod_id]).expect("crictl rmp");
    assert!(
        out.status.success(),
        "A2: rmp failed\n{}",
        trim_output(&out.stderr)
    );

    // pods no longer lists it
    let out = ctl(&srv.socket, &["pods"]).expect("crictl pods");
    let pods_out = trim_output(&out.stdout);
    assert!(
        !pods_out.contains(prefix),
        "A2: pod {prefix} still listed after rmp: {pods_out}",
    );

    cleanup_dir(&tmpdir);
}

// ── A3: instant pull ──────────────────────────────────────────────────────────

#[test]
fn a3_instant_pull() {
    let bin = common_probes!("A3");
    let srv = ServerHandle::spawn(&bin).expect("spawn server");

    let image_ref = "ref/a3-test-image";
    let t0 = std::time::Instant::now();
    let out = ctl(&srv.socket, &["pull", image_ref]).expect("crictl pull");
    let elapsed = t0.elapsed();

    assert!(
        out.status.success(),
        "A3: pull failed\nstdout: {}\nstderr: {}",
        trim_output(&out.stdout),
        trim_output(&out.stderr),
    );
    // Machine-class ceiling (budget.sh doctrine): the law under test is
    // "pull is resolve-only" — a byte-moving pull is 10s+. On shared-docker
    // (Docker-on-Mac hosts) crictl spawn + virtualization can exceed 1s
    // under load without violating the law.
    let ceiling_secs =
        if std::env::var("LIGHTR_BUDGET_CLASS").as_deref() == Ok("shared-docker") {
            3
        } else {
            1
        };
    assert!(
        elapsed < std::time::Duration::from_secs(ceiling_secs),
        "A3: pull took {elapsed:?} (must be <{ceiling_secs} s — lazy CAS law)",
    );

    // images lists it
    let out = ctl(&srv.socket, &["images"]).expect("crictl images");
    assert!(out.status.success(), "A3: images list failed");
    let images_out = trim_output(&out.stdout);
    assert!(
        images_out.contains(image_ref),
        "A3: images output does not list {image_ref}: {images_out}",
    );
}

// ── A4: container lifecycle ───────────────────────────────────────────────────

#[test]
fn a4_container_lifecycle() {
    let bin = common_probes!("A4");
    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let tmpdir = tempfile_dir("a4");

    let pod_json = write_pod_json(&tmpdir, "a4-pod", "a4-uid-0001");
    let ctr_json = write_container_json(&tmpdir, "a4-ctr", "ref/sleep", &["/bin/sleep", "30"]);

    // runp
    let out = ctl(&srv.socket, &["runp", pod_json.to_str().unwrap()]).expect("runp");
    assert!(
        out.status.success(),
        "A4: runp failed\n{}",
        trim_output(&out.stderr)
    );
    let pod_id = trim_output(&out.stdout);

    // create
    let out = ctl(
        &srv.socket,
        &[
            "create",
            &pod_id,
            ctr_json.to_str().unwrap(),
            pod_json.to_str().unwrap(),
        ],
    )
    .expect("create");
    assert!(
        out.status.success(),
        "A4: create failed\n{}",
        trim_output(&out.stderr)
    );
    let ctr_id = trim_output(&out.stdout);
    assert!(!ctr_id.is_empty(), "A4: create returned empty id");

    // start
    let out = ctl(&srv.socket, &["start", &ctr_id]).expect("start");
    assert!(
        out.status.success(),
        "A4: start failed\n{}",
        trim_output(&out.stderr)
    );

    // ps shows Running
    let out = ctl(&srv.socket, &["ps"]).expect("ps");
    assert!(out.status.success(), "A4: ps failed");
    let ps_out = trim_output(&out.stdout);
    let prefix = &ctr_id[..ctr_id.len().min(12)];
    assert!(
        ps_out.contains(prefix),
        "A4: running container {prefix} not in ps: {ps_out}"
    );
    assert!(
        ps_out.to_lowercase().contains("running"),
        "A4: container not Running in ps: {ps_out}"
    );

    // stop (timeout 0)
    let out = ctl(&srv.socket, &["stop", "--timeout", "0", &ctr_id]).expect("stop");
    assert!(
        out.status.success(),
        "A4: stop failed\n{}",
        trim_output(&out.stderr)
    );

    // ps -a shows Exited
    let out = ctl(&srv.socket, &["ps", "-a"]).expect("ps -a");
    assert!(out.status.success(), "A4: ps -a failed");
    let ps_out = trim_output(&out.stdout);
    assert!(
        ps_out.to_lowercase().contains("exited"),
        "A4: container not Exited in ps -a: {ps_out}"
    );

    cleanup_dir(&tmpdir);
}

// ── A5: exec sync ─────────────────────────────────────────────────────────────

#[test]
fn a5_exec_sync() {
    let bin = common_probes!("A5");
    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let tmpdir = tempfile_dir("a5");

    let pod_json = write_pod_json(&tmpdir, "a5-pod", "a5-uid-0001");
    let ctr_json = write_container_json(&tmpdir, "a5-ctr", "ref/sleep", &["/bin/sleep", "30"]);

    let out = ctl(&srv.socket, &["runp", pod_json.to_str().unwrap()]).expect("runp");
    assert!(out.status.success(), "A5: runp failed");
    let pod_id = trim_output(&out.stdout);

    let out = ctl(
        &srv.socket,
        &[
            "create",
            &pod_id,
            ctr_json.to_str().unwrap(),
            pod_json.to_str().unwrap(),
        ],
    )
    .expect("create");
    assert!(
        out.status.success(),
        "A5: create failed\n{}",
        trim_output(&out.stderr)
    );
    let ctr_id = trim_output(&out.stdout);

    let out = ctl(&srv.socket, &["start", &ctr_id]).expect("start");
    assert!(
        out.status.success(),
        "A5: start failed\n{}",
        trim_output(&out.stderr)
    );

    // exec --sync /bin/echo hi
    let out = ctl(&srv.socket, &["exec", "--sync", &ctr_id, "/bin/echo", "hi"]).expect("exec");
    assert!(
        out.status.success(),
        "A5: exec --sync exit {}\nstdout: {}\nstderr: {}",
        out.status,
        trim_output(&out.stdout),
        trim_output(&out.stderr),
    );
    let stdout = trim_output(&out.stdout);
    assert!(
        stdout.contains("hi"),
        "A5: exec stdout does not contain 'hi': {stdout}"
    );

    cleanup_dir(&tmpdir);
}

// ── A6: idempotency + cascade ─────────────────────────────────────────────────

#[test]
fn a6_idempotency_cascade() {
    let bin = common_probes!("A6");
    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let tmpdir = tempfile_dir("a6");

    let pod_json = write_pod_json(&tmpdir, "a6-pod", "a6-uid-0001");
    let ctr_json = write_container_json(&tmpdir, "a6-ctr", "ref/sleep", &["/bin/sleep", "30"]);

    let out = ctl(&srv.socket, &["runp", pod_json.to_str().unwrap()]).expect("runp");
    assert!(out.status.success(), "A6: runp failed");
    let pod_id = trim_output(&out.stdout);

    let out = ctl(
        &srv.socket,
        &[
            "create",
            &pod_id,
            ctr_json.to_str().unwrap(),
            pod_json.to_str().unwrap(),
        ],
    )
    .expect("create");
    assert!(
        out.status.success(),
        "A6: create failed\n{}",
        trim_output(&out.stderr)
    );
    let ctr_id = trim_output(&out.stdout);

    let out = ctl(&srv.socket, &["start", &ctr_id]).expect("start");
    assert!(out.status.success(), "A6: start failed");

    // stop twice — both must succeed (idempotent)
    let out = ctl(&srv.socket, &["stop", "--timeout", "0", &ctr_id]).expect("stop 1");
    assert!(
        out.status.success(),
        "A6: first stop failed\n{}",
        trim_output(&out.stderr)
    );
    let out = ctl(&srv.socket, &["stop", "--timeout", "0", &ctr_id]).expect("stop 2");
    assert!(
        out.status.success(),
        "A6: second stop failed\n{}",
        trim_output(&out.stderr)
    );

    // rm
    let out = ctl(&srv.socket, &["rm", &ctr_id]).expect("rm");
    assert!(
        out.status.success(),
        "A6: rm failed\n{}",
        trim_output(&out.stderr)
    );

    // rm again — graceful (crictl may return non-zero for "not found")
    let _ = ctl(&srv.socket, &["rm", &ctr_id]);

    // Create another container to test cascade via rmp
    let ctr_json2 = write_container_json(&tmpdir, "a6-ctr2", "ref/sleep", &["/bin/sleep", "30"]);
    let out = ctl(
        &srv.socket,
        &[
            "create",
            &pod_id,
            ctr_json2.to_str().unwrap(),
            pod_json.to_str().unwrap(),
        ],
    )
    .expect("create2");
    assert!(
        out.status.success(),
        "A6: create2 failed\n{}",
        trim_output(&out.stderr)
    );
    let ctr_id2 = trim_output(&out.stdout);
    let out = ctl(&srv.socket, &["start", &ctr_id2]).expect("start2");
    assert!(out.status.success(), "A6: start2 failed");

    // stopp + rmp — should cascade remove ctr_id2
    let out = ctl(&srv.socket, &["stopp", &pod_id]).expect("stopp");
    assert!(
        out.status.success(),
        "A6: stopp failed\n{}",
        trim_output(&out.stderr)
    );
    let out = ctl(&srv.socket, &["rmp", &pod_id]).expect("rmp");
    assert!(
        out.status.success(),
        "A6: rmp failed\n{}",
        trim_output(&out.stderr)
    );

    // ps -a should not list ctr_id2 anymore
    let out = ctl(&srv.socket, &["ps", "-a"]).expect("ps -a");
    let ps_out = trim_output(&out.stdout);
    let prefix2 = &ctr_id2[..ctr_id2.len().min(12)];
    assert!(
        !ps_out.contains(prefix2),
        "A6: container {prefix2} still listed after rmp cascade: {ps_out}",
    );

    cleanup_dir(&tmpdir);
}

// ── A7: stats ─────────────────────────────────────────────────────────────────

#[test]
fn a7_stats() {
    let bin = common_probes!("A7");
    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let tmpdir = tempfile_dir("a7");

    let pod_json = write_pod_json(&tmpdir, "a7-pod", "a7-uid-0001");
    let ctr_json = write_container_json(&tmpdir, "a7-ctr", "ref/sleep", &["/bin/sleep", "30"]);

    let out = ctl(&srv.socket, &["runp", pod_json.to_str().unwrap()]).expect("runp");
    assert!(out.status.success(), "A7: runp failed");
    let pod_id = trim_output(&out.stdout);

    let out = ctl(
        &srv.socket,
        &[
            "create",
            &pod_id,
            ctr_json.to_str().unwrap(),
            pod_json.to_str().unwrap(),
        ],
    )
    .expect("create");
    assert!(
        out.status.success(),
        "A7: create failed\n{}",
        trim_output(&out.stderr)
    );
    let ctr_id = trim_output(&out.stdout);

    let out = ctl(&srv.socket, &["start", &ctr_id]).expect("start");
    assert!(out.status.success(), "A7: start failed");

    // stats
    let out = ctl(&srv.socket, &["stats", &ctr_id]).expect("stats");
    assert!(
        out.status.success(),
        "A7: stats exit {}\nstdout: {}\nstderr: {}",
        out.status,
        trim_output(&out.stdout),
        trim_output(&out.stderr),
    );
    let stats_out = trim_output(&out.stdout);
    assert!(!stats_out.is_empty(), "A7: stats returned empty output");

    cleanup_dir(&tmpdir);
}

// ── A8: crash-only ────────────────────────────────────────────────────────────

#[test]
fn a8_crash_only() {
    let bin = common_probes!("A8");
    // A8 requires Linux /proc for pid verification.
    if !cfg!(target_os = "linux") {
        skip(
            "A8",
            "non-Linux: /proc-based pid verification not available",
        );
        return;
    }

    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let tmpdir = tempfile_dir("a8");
    let socket = srv.socket.clone();
    let state_dir = srv.state_dir.clone();
    // Take the tmpdir path before consuming srv so we can pass it to restart.
    let srv_tmpdir = srv.tmpdir.clone();

    let pod_json = write_pod_json(&tmpdir, "a8-pod", "a8-uid-0001");
    let ctr_json = write_container_json(&tmpdir, "a8-ctr", "ref/sleep", &["/bin/sleep", "60"]);

    let out = ctl(&socket, &["runp", pod_json.to_str().unwrap()]).expect("runp");
    assert!(out.status.success(), "A8: runp failed");
    let pod_id = trim_output(&out.stdout);

    let out = ctl(
        &socket,
        &[
            "create",
            &pod_id,
            ctr_json.to_str().unwrap(),
            pod_json.to_str().unwrap(),
        ],
    )
    .expect("create");
    assert!(
        out.status.success(),
        "A8: create failed\n{}",
        trim_output(&out.stderr)
    );
    let ctr_id = trim_output(&out.stdout);

    let out = ctl(&socket, &["start", &ctr_id]).expect("start");
    assert!(out.status.success(), "A8: start failed");

    // Capture current pods + ps output
    let pods_before = trim_output(&ctl(&socket, &["pods"]).expect("pods").stdout);
    let ps_before = trim_output(&ctl(&socket, &["ps"]).expect("ps").stdout);

    // Read container state JSON to find the pid (fake backend writes pid into state JSON).
    let ctr_state_path = state_dir.join("containers").join(format!("{ctr_id}.json"));
    let ctr_json_val: serde_json::Value = std::fs::read_to_string(&ctr_state_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(serde_json::Value::Null);
    let host_pid: Option<u32> = ctr_json_val
        .get("pid")
        .or_else(|| ctr_json_val.get("host_pid"))
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);

    // SIGKILL the server — crash-only law.
    // Use `kill` shell command to avoid adding libc as a dep.
    let server_pid = srv.child.id();
    let mut srv = std::mem::ManuallyDrop::new(srv);
    let kill_status = std::process::Command::new("kill")
        .arg("-9")
        .arg(server_pid.to_string())
        .status()
        .expect("kill command");
    assert!(
        kill_status.success(),
        "A8: kill -9 failed for server pid {server_pid}"
    );
    // Wait for the child to reap
    let _ = srv.child.wait();

    // If we found a pid, that host process must still be alive.
    if let Some(pid) = host_pid {
        let proc_dir = PathBuf::from(format!("/proc/{pid}"));
        assert!(
            proc_dir.exists(),
            "A8: container host process (pid {pid}) died when server was killed — crash-only violated",
        );
    }

    // Restart server on SAME socket + state.
    let srv2 =
        ServerHandle::restart(&bin, socket.clone(), state_dir, srv_tmpdir).expect("restart server");

    // Re-derive pods + ps after restart.
    let pods_after = trim_output(&ctl(&socket, &["pods"]).expect("pods after").stdout);
    let ps_after = trim_output(&ctl(&socket, &["ps"]).expect("ps after").stdout);

    let pod_prefix = &pod_id[..pod_id.len().min(12)];
    assert!(
        pods_before.contains(pod_prefix),
        "A8: pod {pod_prefix} not in pre-crash pods: {pods_before}",
    );
    assert!(
        pods_after.contains(pod_prefix),
        "A8: pod {pod_prefix} not in post-restart pods: {pods_after}",
    );

    let ctr_prefix = &ctr_id[..ctr_id.len().min(12)];
    assert!(
        ps_before.contains(ctr_prefix),
        "A8: container {ctr_prefix} not in pre-crash ps: {ps_before}",
    );
    assert!(
        ps_after.contains(ctr_prefix),
        "A8: container {ctr_prefix} not in post-restart ps: {ps_after}",
    );

    // Cleanup
    let _ = ctl(&socket, &["stopp", &pod_id]);
    let _ = ctl(&socket, &["rmp", &pod_id]);
    cleanup_dir(&tmpdir);
    drop(srv2);
}
