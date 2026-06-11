//! A10: critest scoped — probes critest in PATH, runs against a harness server,
//! asserts exit success.  Will be red on Linux until integration; SKIPs cleanly
//! on macOS (no critest, no crictl).

use lightr_cri_acceptance::{critest_cmd, find_server_bin, have, skip, ServerHandle};

#[test]
fn critest_scoped() {
    // Probe: Unix only
    if !cfg!(unix) {
        skip("A10/critest_scoped", "non-Unix platform");
        return;
    }
    // Probe: critest in PATH
    if !have("critest") {
        skip("A10/critest_scoped", "critest not in PATH");
        return;
    }
    // Probe: crictl in PATH (server harness relies on it indirectly)
    if !have("crictl") {
        skip("A10/critest_scoped", "crictl not in PATH");
        return;
    }
    // Probe: binary exists
    let bin = match find_server_bin() {
        Some(b) => b,
        None => {
            skip("A10/critest_scoped", "lightr-cri binary not found (build first or set LIGHTR_CRI_BIN)");
            return;
        }
    };

    let srv = ServerHandle::spawn(&bin).expect("spawn server for critest");

    // Locate the skips file (ci/critest-skips.txt from workspace root).
    let skips_file = {
        // Walk up from CARGO_MANIFEST_DIR to find the workspace root.
        let start = std::env::var("CARGO_MANIFEST_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
        let mut cur = start.as_path();
        let mut found = None;
        loop {
            let candidate = cur.join("ci/critest-skips.txt");
            if candidate.exists() {
                found = Some(candidate);
                break;
            }
            match cur.parent() {
                Some(p) => cur = p,
                None => break,
            }
        }
        found.unwrap_or_else(|| {
            // If not found, use a non-existent path; critest_cmd handles missing file gracefully.
            start.join("ci/critest-skips.txt")
        })
    };

    let mut cmd = critest_cmd(&srv.socket, &skips_file);
    let status = cmd.status().expect("failed to run critest");
    assert!(status.success(), "A10: critest exited with {status}");
}
