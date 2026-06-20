//! Network namespace lifecycle — containerd bind-mount pattern.
//!
//! Laws (r1-cni.md "Netns lifecycle"):
//! - Create: mkdir /run/netns, touch the file, on a DEDICATED std::thread:
//!   unshare(CLONE_NEWNET) then bind-mount /proc/self/task/<gettid>/ns/net → path;
//!   thread exits, mount pins the ns.
//! - Teardown LAW: umount2(path, MNT_DETACH) THEN unlink (never skip — containerd#6143).
//! - Sweep: for each entry under dir, if no process holds it, umount+unlink.
//! - join_netns: open the file O_RDONLY, return OwnedFd (caller passes into pre_exec).

use std::fs;
use std::io;
use std::os::unix::io::{FromRawFd, IntoRawFd, OwnedFd};
use std::path::{Path, PathBuf};

const NETNS_DIR: &str = "/run/netns";

/// Create a new named network namespace pinned at `/run/netns/<name>`.
///
/// Returns the path on success.  The operation runs on a dedicated OS thread
/// so that `unshare(CLONE_NEWNET)` only affects that thread and not the caller.
pub fn create(name: &str) -> io::Result<PathBuf> {
    let dir = Path::new(NETNS_DIR);
    fs::create_dir_all(dir)?;

    let path = dir.join(name);
    // Touch the file so it exists as a mount point.
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;

    let path_clone = path.clone();
    let result = std::thread::spawn(move || -> io::Result<()> { create_on_thread(&path_clone) })
        .join()
        .map_err(|_| io::Error::other("netns create thread panicked"))?;

    result?;
    Ok(path)
}

/// Performed on the dedicated thread: unshare, then bind-mount the thread-local netns.
#[cfg(target_os = "linux")]
fn create_on_thread(path: &Path) -> io::Result<()> {
    use nix::mount::{mount, MsFlags};
    use nix::sched::{unshare, CloneFlags};
    use nix::unistd::gettid;

    // Create a new network namespace for THIS thread only.
    unshare(CloneFlags::CLONE_NEWNET)
        .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e.to_string()))?;

    // Bind-mount the thread-local ns file onto the pinned path.
    let tid = gettid();
    let ns_src = format!("/proc/self/task/{}/ns/net", tid);

    mount(
        Some(ns_src.as_str()),
        path,
        None::<&str>,
        MsFlags::MS_BIND,
        None::<&str>,
    )
    .map_err(|e| io::Error::other(format!("bind-mount netns: {e}")))?;

    // Bring the loopback interface UP inside the new netns. A fresh netns has
    // `lo` DOWN by default, so dialing 127.0.0.1 inside it (the port-forward
    // in-netns dial) would never connect → timeout. This thread is in the new
    // netns (post-unshare), so a child process forked here runs in it too.
    // (`ip` = iproute2, installed on the runner; non-fatal if it can't run.)
    let _ = std::process::Command::new("ip")
        .args(["link", "set", "lo", "up"])
        .status();

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn create_on_thread(_path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "netns create is Linux-only",
    ))
}

/// Tear down a pinned netns: `umount2(path, MNT_DETACH)` then `unlink`.
///
/// LAW: umount first, unlink second.  Reversing the order causes EBUSY on the
/// mount point and leaves the kernel ns referenced indefinitely (containerd#6143).
pub fn teardown(path: &Path) -> io::Result<()> {
    umount_detach(path)?;
    fs::remove_file(path)
}

#[cfg(target_os = "linux")]
fn umount_detach(path: &Path) -> io::Result<()> {
    use nix::mount::{umount2, MntFlags};
    umount2(path, MntFlags::MNT_DETACH).map_err(|e| io::Error::other(format!("umount2: {e}")))
}

#[cfg(not(target_os = "linux"))]
fn umount_detach(_path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "umount2 is Linux-only",
    ))
}

/// Sweep `dir` for orphaned netns files (no process holds an open fd to them)
/// and clean each one up.  Returns the number of entries removed.
///
/// Heuristic (matches containerd): try `umount2(MNT_DETACH)`; EINVAL → stale
/// touch file (never mounted) → just unlink; EBUSY or any other error → skip
/// (another process may still hold it).
pub fn sweep(dir: &Path) -> io::Result<usize> {
    let mut removed = 0usize;
    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if matches!(sweep_one(&path), Ok(true)) {
            removed += 1;
        }
    }
    Ok(removed)
}

#[cfg(target_os = "linux")]
fn sweep_one(path: &Path) -> io::Result<bool> {
    use nix::errno::Errno;
    use nix::mount::{umount2, MntFlags};

    match umount2(path, MntFlags::MNT_DETACH) {
        Ok(()) => {
            fs::remove_file(path)?;
            Ok(true)
        }
        Err(Errno::EINVAL) => {
            // Not mounted — stale file; just remove.
            fs::remove_file(path)?;
            Ok(true)
        }
        Err(_) => {
            // EBUSY or other — still in use; leave it.
            Ok(false)
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn sweep_one(_path: &Path) -> io::Result<bool> {
    Ok(false)
}

/// Open a pinned netns file `O_RDONLY` for use in a `pre_exec` setns call.
///
/// The returned `OwnedFd` should be moved into the closure passed to
/// `Command::pre_exec`; the `setns(fd, CLONE_NEWNET)` call is async-signal-safe
/// and allocates nothing.
pub fn join_netns(path: &Path) -> io::Result<OwnedFd> {
    let file = fs::OpenOptions::new().read(true).open(path)?;
    let raw = file.into_raw_fd();
    // SAFETY: `file` is open and valid; we consume it via into_raw_fd so
    // the OwnedFd is the sole owner.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

/// Dial `127.0.0.1:port` (or `host:port`) from INSIDE the sandbox network
/// namespace pinned at `netns_path` (contract §B, AMENDED 2026-06-19:
/// portforward enters the sandbox netns).
///
/// `setns(CLONE_NEWNET)` mutates the CALLING THREAD's network namespace, so it
/// MUST NOT run on a tokio worker thread (it would corrupt every other async
/// task time-sharing that thread). This runs the `setns` + blocking
/// `TcpStream::connect` on a DEDICATED `std::thread` that we own end-to-end:
/// the thread joins the ns, connects, and the connected `std::net::TcpStream`
/// is moved back across the `.join()` boundary. The thread then exits, so its
/// mutated netns is discarded with it — the caller's threads are never touched.
///
/// The returned stream is a blocking `std::net::TcpStream`; the async caller
/// sets it non-blocking and adopts it with `tokio::net::TcpStream::from_std`.
///
/// `host_network` sandboxes have no `netns_path` and must NOT call this — they
/// keep the ordinary host-netns dial.
pub fn dial_in_netns(netns_path: &str, host: &str, port: u16) -> io::Result<std::net::TcpStream> {
    let netns_path = netns_path.to_string();
    let host = host.to_string();
    std::thread::spawn(move || -> io::Result<std::net::TcpStream> {
        enter_netns(&netns_path)?;
        // Now inside the sandbox netns: 127.0.0.1 routes to the pod loopback.
        std::net::TcpStream::connect((host.as_str(), port))
    })
    .join()
    .map_err(|_| io::Error::other("dial_in_netns thread panicked"))?
}

/// setns(CLONE_NEWNET) the calling thread into the pinned netns. Linux-only;
/// the OwnedFd from `join_netns` is closed when this returns (success or not).
#[cfg(target_os = "linux")]
fn enter_netns(netns_path: &str) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let ns_fd = join_netns(Path::new(netns_path))?;
    // SAFETY: ns_fd is a valid, open O_RDONLY netns fd owned by this thread.
    let rc = unsafe { libc::setns(ns_fd.as_raw_fd(), libc::CLONE_NEWNET) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    // ns_fd's Drop closes the fd; the thread keeps the netns until it exits.
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn enter_netns(_netns_path: &str) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "setns is Linux-only",
    ))
}
