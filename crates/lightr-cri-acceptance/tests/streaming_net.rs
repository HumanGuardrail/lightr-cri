//! WP-F: B-items (B1–B7).
//!
//! All tests are probe-gated.  On macOS (or any host missing crictl/caps/kubelet)
//! every test SKIPs loudly and exits green.  On Linux CI with
//! LIGHTR_CRI_REQUIRE_PROBES=1 a missing probe becomes a hard FAIL so the suite
//! cannot silently evaporate.
//!
//! Tests reuse ServerHandle / crictl / find_server_bin / skip / have from lib.rs.

use lightr_cri_acceptance::{
    crictl, find_server_bin, have, have_cap_sys_admin, skip, skip_by_design, ServerHandle,
};
use std::path::{Path, PathBuf};

// ── shared helpers ────────────────────────────────────────────────────────────

fn trim(b: &[u8]) -> String {
    String::from_utf8_lossy(b).trim().to_string()
}

fn tmpdir(tag: &str) -> PathBuf {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let d = std::env::temp_dir().join(format!("lightr-b-{tag}-{id}"));
    std::fs::create_dir_all(&d).expect("create tmpdir");
    d
}

fn ctl(sock: &Path, args: &[&str]) -> std::process::Output {
    crictl(sock, args).expect("crictl invocation")
}

/// Write a minimal pod sandbox config.  Returns (path, json-string).
/// `host_network`: if true, injects `"hostNetwork": true` into linux.
fn write_pod_json(dir: &Path, name: &str, uid: &str, host_network: bool) -> PathBuf {
    let linux = if host_network {
        serde_json::json!({ "securityContext": { "namespaceOptions": { "network": 2 } } })
    } else {
        serde_json::json!({})
    };
    let json = serde_json::json!({
        "metadata": { "name": name, "uid": uid, "namespace": "acceptance-b", "attempt": 0 },
        "logDirectory": "/tmp",
        "linux": linux
    });
    let p = dir.join(format!("{name}-pod.json"));
    std::fs::write(&p, json.to_string()).expect("write pod.json");
    p
}

/// Write a minimal container config.
fn write_ctr_json(
    dir: &Path,
    name: &str,
    image: &str,
    cmd: &[&str],
    port_mappings: Option<serde_json::Value>,
) -> PathBuf {
    let command: Vec<serde_json::Value> = cmd
        .iter()
        .map(|s| serde_json::Value::String(s.to_string()))
        .collect();
    let mut json = serde_json::json!({
        "metadata": { "name": name, "attempt": 0 },
        "image": { "image": image },
        "command": command,
        // CRI ContainerConfig.log_path is RELATIVE to the sandbox logDirectory;
        // crictl joins sandbox.logDirectory + this and refuses `logs` if empty
        // ("the container has not set log path"). Give every container a stable
        // per-container relative path so the fake tees to logDirectory/<name>.log.
        "logPath": format!("{name}.log"),
        "linux": {}
    });
    if let Some(pm) = port_mappings {
        json["portMappings"] = pm;
    }
    let p = dir.join(format!("{name}-ctr.json"));
    std::fs::write(&p, json.to_string()).expect("write ctr.json");
    p
}

/// Common B-item probes.  Returns the server binary path or has already SKIPped.
macro_rules! b_probes {
    ($item:expr) => {{
        if !cfg!(unix) {
            skip($item, "non-Unix platform");
            return;
        }
        if !have("crictl") {
            skip($item, "crictl not in PATH");
            return;
        }
        match find_server_bin() {
            Some(b) => b,
            None => {
                skip(
                    $item,
                    "lightr-cri binary not found (build first or set LIGHTR_CRI_BIN)",
                );
                return;
            }
        }
    }};
}

// ── B1: streaming exec (SPDY path, crictl exec without --sync) ────────────────

#[test]
fn b1_streaming_exec() {
    let bin = b_probes!("B1");

    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let td = tmpdir("b1");

    let pod_json = write_pod_json(&td, "b1-pod", "b1-uid-0001", false);
    let ctr_json = write_ctr_json(&td, "b1-ctr", "ref/sleep", &["/bin/sleep", "60"], None);

    let out = ctl(&srv.socket, &["runp", pod_json.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "B1: runp failed\n{}",
        trim(&out.stderr)
    );
    let pod_id = trim(&out.stdout);

    let out = ctl(
        &srv.socket,
        &[
            "create",
            &pod_id,
            ctr_json.to_str().unwrap(),
            pod_json.to_str().unwrap(),
        ],
    );
    assert!(
        out.status.success(),
        "B1: create failed\n{}",
        trim(&out.stderr)
    );
    let ctr_id = trim(&out.stdout);

    let out = ctl(&srv.socket, &["start", &ctr_id]);
    assert!(
        out.status.success(),
        "B1: start failed\n{}",
        trim(&out.stderr)
    );

    // crictl exec WITHOUT --sync → streaming server (SPDY path).
    // crictl exec -it is the interactive form; for a non-interactive command
    // we use crictl exec (no --sync) which still goes through SPDY streaming.
    let out = ctl(&srv.socket, &["exec", &ctr_id, "/bin/echo", "hi"]);
    assert!(
        out.status.success(),
        "B1: streaming exec exit {}\nstdout: {}\nstderr: {}",
        out.status,
        trim(&out.stdout),
        trim(&out.stderr),
    );
    let stdout = trim(&out.stdout);
    assert!(
        stdout.contains("hi"),
        "B1: streaming exec stdout does not contain 'hi': {stdout}",
    );

    let _ = std::fs::remove_dir_all(&td);
}

// ── B2: attach ────────────────────────────────────────────────────────────────
//
// Driving `crictl attach` interactively from a non-terminal test process is
// not reliably achievable: crictl's attach path uses raw terminal mode and
// reads from /dev/stdin which may be a pipe with no reliable EOF signal in a
// test harness.  The interaction model requires bidirectional streaming over
// SPDY with stdin fed in real time — we cannot write to crictl's stdin while
// also reading its stdout in a deterministic way from std::process::Command
// without PTY plumbing (which is out of scope for an acceptance test).
//
// BLOCKED: crictl attach cannot be driven deterministically from a Rust test
// without PTY orchestration (crictl enters raw terminal mode on its own stdin
// pipe — there is no `--input-file` flag).  B2 is structurally limited by the
// crictl CLI, not by the server.  The attach SPDY path is exercised by the
// WP-E vector suite (attach round-trip via open_attach handles).  This item
// documents the limitation and SKIPs loudly so CI stays probe-truthful.

#[test]
fn b2_attach() {
    let _bin = b_probes!("B2");
    skip_by_design(
        "B2",
        "crictl attach requires an interactive PTY on crictl's own stdin; cannot be \
         driven deterministically from a Rust test process (no --input-file flag).",
        "critest attach validation (A10 gate) + WP-E SPDY/WS attach vector suite",
    );
}

// ── B3: portforward ───────────────────────────────────────────────────────────

#[test]
fn b3_portforward() {
    let bin = b_probes!("B3");

    // Additional probes: need a tiny HTTP tool inside the synthetic image.
    // We use busybox httpd if busybox is in PATH, else nc if available.
    // If neither exists, SKIP with reason.
    let http_tool = if have("busybox") {
        "busybox"
    } else if have("nc") || have("ncat") || have("netcat") {
        "nc"
    } else {
        skip("B3", "neither busybox nor nc/ncat/netcat in PATH — cannot start an HTTP responder inside the container without external image pull");
        return;
    };

    // Need curl to test the forwarded port from the runner side.
    if !have("curl") {
        skip("B3", "curl not in PATH");
        return;
    }

    // Need Linux for network namespaces (portforward is meaningless on macOS fake).
    if !cfg!(target_os = "linux") {
        skip(
            "B3",
            "non-Linux: portforward requires kernel network namespaces",
        );
        return;
    }

    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let td = tmpdir("b3");

    let pod_json = write_pod_json(&td, "b3-pod", "b3-uid-0001", false);

    // Container command: start a minimal HTTP responder on port 8080.
    // busybox httpd: `busybox httpd -f -p 8080 -h /tmp`
    // nc loop: `sh -c 'while true; do echo -e "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK" | nc -l -p 8080; done'`
    let cmd: Vec<&str> = if http_tool == "busybox" {
        // Create a minimal index.html first via sh then start httpd.
        vec![
            "/bin/sh",
            "-c",
            "echo B3-OK > /tmp/index.html && busybox httpd -f -p 8080 -h /tmp",
        ]
    } else {
        vec![
            "/bin/sh", "-c",
            "while true; do printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 5\\r\\n\\r\\nB3-OK' | nc -l 8080; done",
        ]
    };

    let ctr_json = write_ctr_json(&td, "b3-ctr", "ref/httpd", &cmd, None);

    let out = ctl(&srv.socket, &["runp", pod_json.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "B3: runp failed\n{}",
        trim(&out.stderr)
    );
    let pod_id = trim(&out.stdout);

    let out = ctl(
        &srv.socket,
        &[
            "create",
            &pod_id,
            ctr_json.to_str().unwrap(),
            pod_json.to_str().unwrap(),
        ],
    );
    assert!(
        out.status.success(),
        "B3: create failed\n{}",
        trim(&out.stderr)
    );
    let ctr_id = trim(&out.stdout);
    let _ = ctr_id; // used above

    let out = ctl(&srv.socket, &["start", &ctr_id]);
    assert!(
        out.status.success(),
        "B3: start failed\n{}",
        trim(&out.stderr)
    );

    // Give the responder a moment to bind.
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Choose a local port for the forward.
    let local_port = 18080u16;

    // Spawn `crictl port-forward <pod-id> <local>:<container>` in background.
    let ep = format!("unix://{}", srv.socket.display());
    let mut pf_child = std::process::Command::new("crictl")
        .arg("--runtime-endpoint")
        .arg(&ep)
        .arg("--image-endpoint")
        .arg(&ep)
        .arg("port-forward")
        .arg(&pod_id)
        .arg(format!("{local_port}:8080"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn crictl port-forward");

    // Give the tunnel a moment to establish.
    std::thread::sleep(std::time::Duration::from_millis(500));

    // curl through the forwarded port.
    let curl_out = std::process::Command::new("curl")
        .arg("-sf")
        .arg("--max-time")
        .arg("5")
        .arg(format!("http://127.0.0.1:{local_port}/"))
        .output()
        .expect("curl");

    let _ = pf_child.kill();
    let _ = pf_child.wait();

    assert!(
        curl_out.status.success(),
        "B3: curl through port-forward failed (exit {})\nstdout: {}\nstderr: {}",
        curl_out.status,
        trim(&curl_out.stdout),
        trim(&curl_out.stderr),
    );
    let body = trim(&curl_out.stdout);
    assert!(
        body.contains("B3-OK") || body.contains("OK"),
        "B3: unexpected curl response body: {body}",
    );

    let _ = std::fs::remove_dir_all(&td);
}

// ── B4: CNI — non-hostNetwork pod gets a routable IP ─────────────────────────

#[test]
fn b4_cni_ip() {
    let bin = b_probes!("B4");

    // CNI probe: need SYS_ADMIN + NET_ADMIN caps AND conflist + binaries present.
    if !have_cap_sys_admin() {
        skip(
            "B4",
            "CAP_SYS_ADMIN+CAP_NET_ADMIN not in CapEff — CNI netns requires privileges",
        );
        return;
    }

    // Re-use lightr-cri-net's cni_available logic via the same env/path checks.
    // We can't call it directly (different crate), so duplicate the probe here.
    let cni_conf =
        std::env::var("LIGHTR_CNI_CONF").unwrap_or_else(|_| "/etc/cni/net.d".to_string());
    let cni_bin = std::env::var("LIGHTR_CNI_BIN").unwrap_or_else(|_| "/opt/cni/bin".to_string());

    let has_conflist = std::path::Path::new(&cni_conf)
        .read_dir()
        .ok()
        .map(|rd| {
            rd.filter_map(|e| e.ok()).any(|e| {
                e.path()
                    .extension()
                    .and_then(|x| x.to_str())
                    .map(|x| x == "conflist")
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);

    if !has_conflist {
        skip(
            "B4",
            "no .conflist in /etc/cni/net.d (or LIGHTR_CNI_CONF) — CNI not configured",
        );
        return;
    }
    if !std::path::Path::new(&cni_bin).is_dir() {
        skip(
            "B4",
            "/opt/cni/bin (or LIGHTR_CNI_BIN) not a directory — CNI plugins absent",
        );
        return;
    }

    // Need curl to verify the pod IP is reachable.
    if !have("curl") {
        skip("B4", "curl not in PATH");
        return;
    }

    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let td = tmpdir("b4");

    // Non-hostNetwork pod — CNI must assign an IP.
    let pod_json = write_pod_json(&td, "b4-pod", "b4-uid-0001", false);

    // Container: busybox httpd or nc loop on port 80.
    let http_tool = if have("busybox") {
        "busybox"
    } else if have("nc") || have("ncat") {
        "nc"
    } else {
        skip(
            "B4",
            "neither busybox nor nc in PATH — cannot start HTTP responder in container",
        );
        return;
    };

    let cmd: Vec<&str> = if http_tool == "busybox" {
        vec![
            "/bin/sh",
            "-c",
            "echo B4-OK > /tmp/index.html && busybox httpd -f -p 80 -h /tmp",
        ]
    } else {
        vec![
            "/bin/sh", "-c",
            "while true; do printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 5\\r\\n\\r\\nB4-OK' | nc -l 80; done",
        ]
    };

    let pm = serde_json::json!([{ "containerPort": 80, "protocol": "TCP" }]);
    let ctr_json = write_ctr_json(&td, "b4-ctr", "ref/httpd", &cmd, Some(pm));

    let out = ctl(&srv.socket, &["runp", pod_json.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "B4: runp failed\n{}",
        trim(&out.stderr)
    );
    let pod_id = trim(&out.stdout);

    // PodSandboxStatus must have a non-empty ip field.
    let out = ctl(&srv.socket, &["inspectp", "-o", "json", &pod_id]);
    assert!(
        out.status.success(),
        "B4: inspectp failed\n{}",
        trim(&out.stderr)
    );
    let status_json: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("B4: inspectp output not valid JSON");
    let ip = status_json
        .pointer("/status/network/ip")
        .or_else(|| status_json.pointer("/Status/Network/Ip"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    assert!(
        !ip.is_empty(),
        "B4: PodSandboxStatus.network.ip is empty — CNI ADD did not assign an IP\nfull status: {status_json}",
    );

    // Start a container and give the HTTP responder time to bind.
    let out = ctl(
        &srv.socket,
        &[
            "create",
            &pod_id,
            ctr_json.to_str().unwrap(),
            pod_json.to_str().unwrap(),
        ],
    );
    assert!(
        out.status.success(),
        "B4: create failed\n{}",
        trim(&out.stderr)
    );
    let ctr_id = trim(&out.stdout);

    let out = ctl(&srv.socket, &["start", &ctr_id]);
    assert!(
        out.status.success(),
        "B4: start failed\n{}",
        trim(&out.stderr)
    );

    std::thread::sleep(std::time::Duration::from_millis(400));

    // Curl the pod IP from the runner's netns.
    let curl_out = std::process::Command::new("curl")
        .arg("-sf")
        .arg("--max-time")
        .arg("5")
        .arg(format!("http://{ip}:80/"))
        .output()
        .expect("curl pod ip");
    assert!(
        curl_out.status.success(),
        "B4: curl http://{ip}:80/ failed (exit {})\nstdout: {}\nstderr: {}",
        curl_out.status,
        trim(&curl_out.stdout),
        trim(&curl_out.stderr),
    );

    // Teardown: rmp → netns must be gone.
    let _ = ctl(&srv.socket, &["stop", "--timeout", "0", &ctr_id]);
    let _ = ctl(&srv.socket, &["rm", &ctr_id]);
    let _ = ctl(&srv.socket, &["stopp", &pod_id]);

    let out = ctl(&srv.socket, &["rmp", &pod_id]);
    assert!(
        out.status.success(),
        "B4: rmp failed\n{}",
        trim(&out.stderr)
    );

    // PodSandboxStatus after rmp should have no ip (or inspectp returns error).
    let status_out = ctl(&srv.socket, &["inspectp", "-o", "json", &pod_id]);
    if status_out.status.success() {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&status_out.stdout) {
            let ip_after = v
                .pointer("/status/network/ip")
                .or_else(|| v.pointer("/Status/Network/Ip"))
                .and_then(|x| x.as_str())
                .unwrap_or("");
            assert!(
                ip_after.is_empty(),
                "B4: ip still present after rmp — netns/CNI DEL did not clean up: {ip_after}",
            );
        }
    }
    // inspectp returning non-zero after rmp is also acceptable (sandbox gone).

    let _ = std::fs::remove_dir_all(&td);
}

// ── B5: hostPort DNAT 127.0.0.1:12000 → pod ─────────────────────────────────

#[test]
fn b5_hostport() {
    let bin = b_probes!("B5");

    if !have_cap_sys_admin() {
        skip(
            "B5",
            "CAP_SYS_ADMIN+CAP_NET_ADMIN not in CapEff — hostPort DNAT requires iptables/NET_ADMIN",
        );
        return;
    }

    let cni_conf =
        std::env::var("LIGHTR_CNI_CONF").unwrap_or_else(|_| "/etc/cni/net.d".to_string());
    let cni_bin = std::env::var("LIGHTR_CNI_BIN").unwrap_or_else(|_| "/opt/cni/bin".to_string());
    let has_conflist = std::path::Path::new(&cni_conf)
        .read_dir()
        .ok()
        .map(|rd| {
            rd.filter_map(|e| e.ok()).any(|e| {
                e.path()
                    .extension()
                    .and_then(|x| x.to_str())
                    .map(|x| x == "conflist")
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    if !has_conflist {
        skip(
            "B5",
            "CNI conflist absent — hostPort requires CNI portmap plugin",
        );
        return;
    }
    if !std::path::Path::new(&cni_bin).is_dir() {
        skip(
            "B5",
            "CNI bin dir absent — hostPort requires portmap binary",
        );
        return;
    }
    if !have("curl") {
        skip("B5", "curl not in PATH");
        return;
    }

    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let td = tmpdir("b5");

    // Non-hostNetwork pod with a hostPort mapping: 127.0.0.1:12000 → container:80.
    let linux_ns =
        serde_json::json!({ "securityContext": { "namespaceOptions": { "network": 0 } } });
    let json = serde_json::json!({
        "metadata": { "name": "b5-pod", "uid": "b5-uid-0001", "namespace": "acceptance-b", "attempt": 0 },
        "logDirectory": "/tmp",
        "linux": linux_ns
    });
    let pod_path = td.join("b5-pod.json");
    std::fs::write(&pod_path, json.to_string()).expect("write pod json");

    // Container: HTTP responder on port 80 with hostPort binding.
    let http_tool = if have("busybox") {
        "busybox"
    } else if have("nc") || have("ncat") {
        "nc"
    } else {
        skip(
            "B5",
            "neither busybox nor nc in PATH — cannot start HTTP responder in container",
        );
        return;
    };
    let cmd: Vec<&str> = if http_tool == "busybox" {
        vec![
            "/bin/sh",
            "-c",
            "echo B5-OK > /tmp/index.html && busybox httpd -f -p 80 -h /tmp",
        ]
    } else {
        vec![
            "/bin/sh", "-c",
            "while true; do printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 5\\r\\n\\r\\nB5-OK' | nc -l 80; done",
        ]
    };

    // hostPort mapping: containerPort=80 hostPort=12000 hostIp=127.0.0.1.
    let pm = serde_json::json!([{
        "containerPort": 80,
        "hostPort": 12000,
        "hostIp": "127.0.0.1",
        "protocol": "TCP"
    }]);
    let ctr_json = write_ctr_json(&td, "b5-ctr", "ref/httpd", &cmd, Some(pm));

    let out = ctl(&srv.socket, &["runp", pod_path.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "B5: runp failed\n{}",
        trim(&out.stderr)
    );
    let pod_id = trim(&out.stdout);

    let out = ctl(
        &srv.socket,
        &[
            "create",
            &pod_id,
            ctr_json.to_str().unwrap(),
            pod_path.to_str().unwrap(),
        ],
    );
    assert!(
        out.status.success(),
        "B5: create failed\n{}",
        trim(&out.stderr)
    );
    let ctr_id = trim(&out.stdout);

    let out = ctl(&srv.socket, &["start", &ctr_id]);
    assert!(
        out.status.success(),
        "B5: start failed\n{}",
        trim(&out.stderr)
    );

    std::thread::sleep(std::time::Duration::from_millis(400));

    // Curl 127.0.0.1:12000 — must be DNAT'd to the pod's port 80.
    let curl_out = std::process::Command::new("curl")
        .arg("-sf")
        .arg("--max-time")
        .arg("5")
        .arg("http://127.0.0.1:12000/")
        .output()
        .expect("curl hostPort");
    assert!(
        curl_out.status.success(),
        "B5: curl http://127.0.0.1:12000/ failed (exit {})\nstdout: {}\nstderr: {}",
        curl_out.status,
        trim(&curl_out.stdout),
        trim(&curl_out.stderr),
    );
    let body = trim(&curl_out.stdout);
    assert!(
        body.contains("B5-OK") || body.contains("OK"),
        "B5: unexpected body from hostPort: {body}",
    );

    let _ = ctl(&srv.socket, &["stop", "--timeout", "0", &ctr_id]);
    let _ = ctl(&srv.socket, &["rm", &ctr_id]);
    let _ = ctl(&srv.socket, &["stopp", &pod_id]);
    let _ = ctl(&srv.socket, &["rmp", &pod_id]);
    let _ = std::fs::remove_dir_all(&td);
}

// ── B6: CRI log files ─────────────────────────────────────────────────────────

#[test]
fn b6_logs() {
    let bin = b_probes!("B6");

    let srv = ServerHandle::spawn(&bin).expect("spawn server");
    let td = tmpdir("b6");

    let pod_json = write_pod_json(&td, "b6-pod", "b6-uid-0001", false);
    // Container that emits a known string then exits.
    let ctr_json = write_ctr_json(
        &td,
        "b6-ctr",
        "ref/echo",
        &["/bin/sh", "-c", "echo B6-LOG-LINE"],
        None,
    );

    let out = ctl(&srv.socket, &["runp", pod_json.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "B6: runp failed\n{}",
        trim(&out.stderr)
    );
    let pod_id = trim(&out.stdout);

    let out = ctl(
        &srv.socket,
        &[
            "create",
            &pod_id,
            ctr_json.to_str().unwrap(),
            pod_json.to_str().unwrap(),
        ],
    );
    assert!(
        out.status.success(),
        "B6: create failed\n{}",
        trim(&out.stderr)
    );
    let ctr_id = trim(&out.stdout);

    let out = ctl(&srv.socket, &["start", &ctr_id]);
    assert!(
        out.status.success(),
        "B6: start failed\n{}",
        trim(&out.stderr)
    );

    // Give the container time to run and write its output.
    std::thread::sleep(std::time::Duration::from_millis(500));

    // `crictl logs <id>` must show the emitted line.
    let out = ctl(&srv.socket, &["logs", &ctr_id]);
    assert!(
        out.status.success(),
        "B6: crictl logs exit {}\nstdout: {}\nstderr: {}",
        out.status,
        trim(&out.stdout),
        trim(&out.stderr),
    );
    let logs = trim(&out.stdout) + &trim(&out.stderr); // crictl may write to stderr
    assert!(
        logs.contains("B6-LOG-LINE"),
        "B6: crictl logs output does not contain 'B6-LOG-LINE': {logs}",
    );

    // Verify the on-disk CRI log file format.
    // The CRI log format is: <RFC3339nano-timestamp> <stream> <partial-flag> <log-line>\n
    // e.g.  2006-01-02T15:04:05.999999999Z stdout F actual log line
    // We locate the log file via inspectp or from state_dir.
    let status_out = ctl(&srv.socket, &["inspect", "-o", "json", &ctr_id]);
    if status_out.status.success() {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&status_out.stdout) {
            let log_path = v
                .pointer("/status/logPath")
                .or_else(|| v.pointer("/Status/LogPath"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            if !log_path.is_empty() {
                let log_content = std::fs::read_to_string(&log_path).unwrap_or_default();
                assert!(
                    !log_content.is_empty(),
                    "B6: CRI log file at {log_path} is empty",
                );
                // First non-empty line must match CRI log format:
                // <timestamp> <stream> <F|P> <message>
                // Regex-free check: split on space, verify 4 fields, stream ∈ {stdout,stderr}.
                let first_line = log_content.lines().find(|l| !l.is_empty()).unwrap_or("");
                let parts: Vec<&str> = first_line.splitn(4, ' ').collect();
                assert!(
                    parts.len() >= 4,
                    "B6: CRI log line has fewer than 4 space-separated fields: {first_line:?}",
                );
                assert!(
                    parts[1] == "stdout" || parts[1] == "stderr",
                    "B6: CRI log line stream field is {:?}, expected 'stdout' or 'stderr': {first_line:?}",
                    parts[1],
                );
                assert!(
                    parts[2] == "F" || parts[2] == "P",
                    "B6: CRI log line partial flag is {:?}, expected 'F' or 'P': {first_line:?}",
                    parts[2],
                );
                assert!(
                    log_content.contains("B6-LOG-LINE"),
                    "B6: CRI log file at {log_path} does not contain 'B6-LOG-LINE':\n{log_content}",
                );
            }
        }
    }

    let _ = ctl(&srv.socket, &["stop", "--timeout", "0", &ctr_id]);
    let _ = ctl(&srv.socket, &["rm", &ctr_id]);
    let _ = ctl(&srv.socket, &["stopp", &pod_id]);
    let _ = ctl(&srv.socket, &["rmp", &pod_id]);
    let _ = std::fs::remove_dir_all(&td);
}

// ── B7: kubelet-smoke (R1 exit headline) ──────────────────────────────────────

#[test]
fn b7_kubelet_smoke() {
    let _bin = b_probes!("B7");

    // Locate ci/kubelet-smoke.sh from workspace root (WP-G's deliverable).
    let smoke_sh = {
        let start = std::env::var("CARGO_MANIFEST_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
        let mut cur = start.as_path();
        let mut found: Option<std::path::PathBuf> = None;
        loop {
            let candidate = cur.join("ci/kubelet-smoke.sh");
            if candidate.exists() {
                found = Some(candidate);
                break;
            }
            match cur.parent() {
                Some(p) => cur = p,
                None => break,
            }
        }
        found
    };

    let smoke_sh = match smoke_sh {
        Some(p) => p,
        None => {
            skip(
                "B7",
                "ci/kubelet-smoke.sh not present — kubelet-smoke harness (WP-G) not yet delivered",
            );
            return;
        }
    };

    // Additional probes: need kubelet binary and SYS_ADMIN caps.
    if !have("kubelet") {
        skip("B7", "kubelet not in PATH — install kubelet v1.33.13 (see docs/research/r1-kubelet-smoke.md)");
        return;
    }
    if !have_cap_sys_admin() {
        skip("B7", "CAP_SYS_ADMIN+CAP_NET_ADMIN not in CapEff — kubelet smoke requires privileged container");
        return;
    }

    // Shell out to the harness script and assert exit 0.
    let status = std::process::Command::new("bash")
        .arg(&smoke_sh)
        .status()
        .expect("failed to execute ci/kubelet-smoke.sh");

    assert!(
        status.success(),
        "B7: ci/kubelet-smoke.sh exited with {status} — kubelet smoke FAILED",
    );
}
