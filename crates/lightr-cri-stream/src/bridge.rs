//! Bridges between the sync `StreamSession` (`std::fs::File` handles, contract
//! §B) and the async tokio world, plus the pty `TIOCSWINSZ` resize ioctl.
//!
//! The contract keeps the backend sync; the streamer is async. We adopt each
//! `std::fs::File` into a `tokio::fs::File` via `from_std`, which is the
//! contract-blessed bridge ("`File` → `AsyncFd`/`from_std`").

use std::os::unix::io::AsRawFd;

use tokio::fs::File as TokioFile;

/// Adopt a sync `std::fs::File` into a tokio async `File`.
pub fn adopt(file: std::fs::File) -> TokioFile {
    TokioFile::from_std(file)
}

// ── pty resize (TIOCSWINSZ) ──────────────────────────────────────────────────
//
// We declare the ioctl FFI directly rather than pull in `libc`/`nix` (neither
// is a dependency of this crate). `winsize` and the `TIOCSWINSZ` request code
// are stable per-platform kernel ABI.

#[repr(C)]
struct Winsize {
    ws_row: u16,
    ws_col: u16,
    ws_xpixel: u16,
    ws_ypixel: u16,
}

// TIOCSWINSZ differs by platform:
//   Linux:  0x5414
//   macOS/BSD: _IOW('t', 103, struct winsize) = 0x80087467
#[cfg(target_os = "linux")]
const TIOCSWINSZ: std::os::raw::c_ulong = 0x5414;
#[cfg(not(target_os = "linux"))]
const TIOCSWINSZ: std::os::raw::c_ulong = 0x8008_7467;

extern "C" {
    fn ioctl(fd: std::os::raw::c_int, request: std::os::raw::c_ulong, ...) -> std::os::raw::c_int;
}

/// Apply a terminal resize to the pty master via `TIOCSWINSZ`.
///
/// `width`/`height` are columns/rows (client-go `TerminalSize` is
/// Width=cols, Height=rows). Returns `false` if the ioctl fails (e.g. the
/// handle is not a pty); callers treat resize as best-effort.
pub fn set_winsize(pty_master: &std::fs::File, width: u16, height: u16) -> bool {
    let ws = Winsize {
        ws_row: height,
        ws_col: width,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: `pty_master` is a valid open fd for the call's duration; `ws`
    // outlives the syscall; TIOCSWINSZ expects a `*const winsize`.
    let rc = unsafe { ioctl(pty_master.as_raw_fd(), TIOCSWINSZ, &ws as *const Winsize) };
    rc == 0
}
