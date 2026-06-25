//! bin `lightr-cri` — the in-repo conformance driver: wires the **fake** backend
//! into the generic server composition root (`lightr_cri_server::run_blocking`).
//!
//! The server wiring itself lives in `lib.rs`, backend-agnostic. This binary is
//! the only thing that knows about `FakeBackend`; the integrated runtime builds
//! the real `LightrBackend` and calls the same `run_blocking` — a
//! backend-construction swap, never a copy-paste of the wiring (contract-swap law).
//!
//! Compiled only with the `fake-bin` feature (default); a consumer that wants
//! the wiring without the fake depends on this crate with `default-features = false`.
//!
//! FROZEN args (clap-free): `--socket PATH` (default /run/lightr/cri.sock),
//! `--state PATH` (default per fake backend law).

use std::path::PathBuf;
use std::sync::Arc;

use lightr_cri_fake::FakeBackend;

fn parse_args() -> Option<(PathBuf, Option<PathBuf>)> {
    let mut args = std::env::args().skip(1);
    let mut socket: PathBuf = PathBuf::from("/run/lightr/cri.sock");
    let mut state: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => {
                let val = args.next()?;
                socket = PathBuf::from(val);
            }
            "--state" => {
                let val = args.next()?;
                state = Some(PathBuf::from(val));
            }
            other => {
                eprintln!("Usage: lightr-cri [--socket PATH] [--state PATH]");
                eprintln!("Unknown argument: {other}");
                std::process::exit(1);
            }
        }
    }
    Some((socket, state))
}

/// Resolve the state root: use the provided path if any, otherwise honour
/// LIGHTR_CRI_STATE env var, otherwise fall back to $TMPDIR/lightr-cri-fake.
fn resolve_state(state: Option<PathBuf>) -> PathBuf {
    if let Some(p) = state {
        return p;
    }
    if let Ok(v) = std::env::var("LIGHTR_CRI_STATE") {
        if !v.is_empty() {
            return PathBuf::from(v);
        }
    }
    std::env::temp_dir().join("lightr-cri-fake")
}

fn main() {
    let (socket_path, state_arg) = match parse_args() {
        Some(v) => v,
        None => {
            eprintln!("Usage: lightr-cri [--socket PATH] [--state PATH]");
            std::process::exit(1);
        }
    };

    let state_path = resolve_state(state_arg);

    // Open the fake backend (R1 conformance driver; the lightr backend arrives
    // at integration via the SAME run_blocking entry — see lib.rs).
    let backend = match FakeBackend::open(&state_path) {
        Ok(b) => Arc::new(b),
        Err(e) => {
            eprintln!(
                "lightr-cri: failed to open backend at {}: {e}",
                state_path.display()
            );
            std::process::exit(1);
        }
    };

    std::process::exit(lightr_cri_server::run_blocking(backend, socket_path));
}
