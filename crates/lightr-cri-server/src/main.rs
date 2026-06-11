//! WP-3: bin `lightr-cri` (build-spec-r0 §5).
//!
//! FROZEN laws:
//! - Args, clap-free: `--socket PATH` (default /run/lightr/cri.sock),
//!   `--state PATH` (default per fake backend law).
//! - tokio rt, UnixListener, serve RuntimeService + ImageService.
//! - SIGTERM graceful; exit codes 0/1; one stderr line per lifecycle event.
//! - The process holds NO state: restartable at any instant (crash-only).

fn main() {
    // WP-3 replaces this stub with the real entrypoint.
    eprintln!("lightr-cri: R0 scaffold stub (WP-3)");
    std::process::exit(2);
}
