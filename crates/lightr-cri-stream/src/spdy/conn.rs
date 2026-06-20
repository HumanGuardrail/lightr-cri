//! SPDY/3.1 connection driver for exec/attach and portforward.
//!
//! Runs over a single upgraded byte stream. The server is passive: the client
//! opens streams (SYN_STREAM); the server SYN_REPLYs each, then multiplexes
//! DATA frames. Header blocks use the connection-scoped zlib codecs with the
//! SPDY dictionary (`zlib`/`dict`).
//!
//! Exec/attach: streams identified by the `streamType` header
//! (stdin|stdout|stderr|error|resize) drive the `StreamSession`. The v4
//! `metav1.Status` exit lands on the error stream.
//! PortForward: stream PAIRS (`streamType` data|error, `port`, `requestID`)
//! dial the backend and pipe; RST both on dial failure. The dial enters the
//! sandbox netns when one is recorded (contract §B AMENDED 2026-06-19): see
//! `dial_pf` — `setns` + connect on a dedicated thread, host-netns otherwise.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};

use super::frame::{self, Frame, ParseResult};
use super::headers::{self, get};
use super::zlib::{HeaderCompressor, HeaderDecompressor};
use crate::bridge;
use crate::channel as ch;
use crate::status;
use crate::{SessionFactory, StreamParams, StreamVerb};

/// Abort-on-drop ownership of a set of spawned tasks.
///
/// Plain `JoinHandle`s do NOT abort their task when dropped — a `run_exec`
/// future cancelled by an outer session-duration cap would drop its handles
/// and leave the read-loop + output pumps RUNNING, holding the socket open
/// forever (the 57-min hang the cap could not bound). Registering every
/// spawned task's `AbortHandle` here makes the cap real: when `run_exec`
/// returns OR its future is dropped, this guard's `Drop` aborts them all, the
/// last `Writer` Arc clone and the read half `rd` are released, and the
/// connection tears down. On the normal path the tasks have already finished
/// (or are explicitly aborted) before the guard drops, so the aborts are
/// no-ops.
/// Shared list of every spawned task's `AbortHandle`. The read-loop pushes
/// the output pumps' handles as the client opens stdout/stderr streams; the
/// function pushes the read-loop and reaper handles up front. A `std::sync`
/// mutex (never held across an `.await`) keeps it cheap to touch from `Drop`.
type AbortList = Arc<std::sync::Mutex<Vec<tokio::task::AbortHandle>>>;

struct TaskGuard {
    handles: AbortList,
}

impl TaskGuard {
    fn new(handles: AbortList) -> Self {
        Self { handles }
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Ok(list) = self.handles.lock() {
            for h in list.iter() {
                h.abort();
            }
        }
    }
}

/// Shared write side: the upgraded stream + the (server→client) header
/// compressor, behind one mutex so frame writes are atomic.
struct Writer<W> {
    out: W,
    comp: HeaderCompressor,
}

impl<W: AsyncWrite + Unpin> Writer<W> {
    async fn write_frame(&mut self, f: &Frame) -> std::io::Result<()> {
        let bytes = f.serialize();
        self.out.write_all(&bytes).await?;
        self.out.flush().await
    }

    /// SYN_REPLY acknowledging a stream the client opened.
    ///
    /// The reply carries a header block with zero name/value pairs — k8s exec
    /// streams attach no reply headers — but it MUST still be a *well-formed*
    /// zlib-compressed SPDY header block, not literally empty: an empty
    /// `header_block` would leave the client's inflate stream un-advanced and
    /// desynchronize every subsequent block. `serialize_headers(&[])` emits the
    /// valid 4-byte count=0 block (`00 00 00 00`); the connection-scoped
    /// compressor turns it into a proper Z_SYNC_FLUSH-terminated zlib segment
    /// the client decodes against the SPDY dictionary. (The compressed bytes
    /// are non-empty even for a zero-pair block.)
    async fn syn_reply(&mut self, stream_id: u32) -> std::io::Result<()> {
        let block = headers::serialize_headers(&[]);
        debug_assert_eq!(block, [0, 0, 0, 0], "empty header block is count=0");
        let compressed = self.comp.compress(&block);
        debug_assert!(
            !compressed.is_empty(),
            "compressed SYN_REPLY header block must be a non-empty zlib segment"
        );
        self.write_frame(&Frame::SynReply {
            stream_id,
            flags: 0,
            header_block: compressed,
        })
        .await
    }

    async fn data(&mut self, stream_id: u32, flags: u8, data: Vec<u8>) -> std::io::Result<()> {
        self.write_frame(&Frame::Data {
            stream_id,
            flags,
            data,
        })
        .await
    }

    async fn rst(&mut self, stream_id: u32, status: u32) -> std::io::Result<()> {
        self.write_frame(&Frame::RstStream { stream_id, status })
            .await
    }
}

/// Read one full frame from `buf`/`stream`, growing `buf` as needed.
async fn read_frame<R: AsyncRead + Unpin>(
    stream: &mut R,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<Frame>> {
    loop {
        match frame::parse_frame(buf) {
            ParseResult::Frame(f, n) => {
                buf.drain(..n);
                return Ok(Some(f));
            }
            ParseResult::Error(e) => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
            }
            ParseResult::NeedMore => {
                let mut tmp = [0u8; 16384];
                let n = stream.read(&mut tmp).await?;
                if n == 0 {
                    return Ok(None);
                }
                buf.extend_from_slice(&tmp[..n]);
            }
        }
    }
}

/// Drive an exec/attach SPDY session.
pub async fn run_exec<S>(
    stream: S,
    verb: StreamVerb,
    params: StreamParams,
    factory: Arc<dyn SessionFactory>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut rd, wr) = tokio::io::split(stream);
    let writer = Arc::new(Mutex::new(Writer {
        out: wr,
        comp: HeaderCompressor::new(),
    }));
    let mut decomp = HeaderDecompressor::new();
    let mut buf = Vec::with_capacity(16384);

    // Open the session up front (blocking factory off the async path).
    let f2 = factory.clone();
    let p2 = params.clone();
    let session = match tokio::task::spawn_blocking(move || f2.open_session(verb, &p2)).await {
        Ok(Ok(s)) => Some(s),
        _ => None,
    };
    let mut session = session;

    // stream_id → role (mutated inside the read-loop task it is moved into)
    let roles: HashMap<u32, u8> = HashMap::new();
    // role → stream_id (for output routing)
    let by_role: HashMap<u8, u32> = HashMap::new();

    // output pump tasks spawned once we know stdout/stderr stream ids. These
    // are moved into the read-loop task below and (re)bound `mut` there.
    let stdin = session
        .as_mut()
        .and_then(|s| s.stdin.take())
        .map(bridge::adopt);
    let pty_master = session.as_mut().and_then(|s| s.pty_master.take());
    // hold raw output files until their stream is opened
    let stdout_raw = session.as_mut().and_then(|s| s.stdout.take());
    let stderr_raw = session.as_mut().and_then(|s| s.stderr.take());
    // Whether the SESSION actually provides each output file. A pump is spawned
    // ONLY when the client opens the role AND the session has the file (read-loop
    // SynStream arm). The drain-readiness gate below must size itself to the
    // pumps that WILL spawn — counting a stderr pump that can never exist (e.g.
    // `stderr: None`, stderr merged onto the pty, or a stdout-only session) makes
    // the gate wait out its full backstop, stalling completion and starving the
    // stdout bytes / the terminal Status (the spdy_exec_e2e regression).
    let has_stdout_file = stdout_raw.is_some();
    let has_stderr_file = stderr_raw.is_some();
    let waiter = session.map(|s| s.waiter);

    // Routing/output state that BOTH the concurrent read-loop and the
    // completion path read: the read-loop discovers which stream ids the
    // client opened for stdout/stderr/error and spawns the output pumps; the
    // completion path (driven by child-exit, below) needs those ids to FIN the
    // right streams and to await the right pumps. Shared behind a mutex.
    let by_role = Arc::new(Mutex::new(by_role));
    let output_tasks = Arc::new(Mutex::new(Vec::<tokio::task::JoinHandle<()>>::new()));

    // Abort-on-drop ownership of EVERY task this function spawns (read-loop,
    // output pumps, reaper). The guard lives on this future's stack: if the
    // future is dropped (e.g. an outer session-duration cap fires), the guard
    // drops too and aborts all registered tasks — releasing the read half `rd`
    // and the last `Writer` Arc clones so the socket actually closes. Without
    // this, dropped `JoinHandle`s leave the tasks running and the cap cannot
    // tear the session down. The read-loop pushes each output pump's handle as
    // streams open (shared `abort_list`), so pumps are covered too.
    let abort_list: AbortList = Arc::new(std::sync::Mutex::new(Vec::new()));
    let _guard = TaskGuard::new(abort_list.clone());
    // Fired by the read-loop after every `by_role` insert, so the completion
    // path can wait for the client's output/error stream ids to land instead
    // of racing a fast-exiting child (see the fast-exit barrier below).
    let role_added = Arc::new(Notify::new());

    // Fired when the CLIENT closes its side. This is the detach signal feeding
    // the completion select's detach arm. It MUST resolve on EVERY client-close
    // path, not just transport teardown:
    //   - the read-loop ending on EOF/RST/GoAway (whole connection gone), AND
    //   - the client half-closing its WRITE side after it is done — for attach
    //     a detaching client sends FLAG_FIN on the streams it writes (stdin,
    //     and it half-closes its readable error/stdout streams) WITHOUT tearing
    //     the transport down, so the read-loop keeps spinning and the EOF path
    //     never fires. A live-container attach then never completes (RACE 2).
    // So `client_closed` is notified both at loop-end AND inline when the
    // client FLAG_FINs a stream during an attach (see the Data arm below).
    let client_closed = Arc::new(Notify::new());

    // ── concurrent read-loop ──────────────────────────────────────────────
    //
    // The bug this fixes: completion used to be triggered by THIS loop ending,
    // and the loop only ends on transport close. Real client-go keeps its
    // write half open and blocks on the server's terminal Status → deadlock.
    // So the loop now runs CONCURRENTLY with completion (driven by output
    // drain / detach) and is NOT itself what completes the session. The loop
    // keeps draining stdin + resize + ping for as long as the client stays
    // connected, and signals `client_closed` when the client tears down.
    let mut read_loop = {
        let writer = writer.clone();
        let by_role = by_role.clone();
        let output_tasks = output_tasks.clone();
        let role_added = role_added.clone();
        let client_closed = client_closed.clone();
        let abort_list = abort_list.clone();
        // `verb` is `Copy`; the read-loop needs it to decide whether a client
        // FLAG_FIN is a detach (attach) — see the Data arm's RACE 2 handling.
        let is_attach = verb == StreamVerb::Attach;
        tokio::spawn(async move {
            let mut roles = roles;
            let mut stdin = stdin;
            let mut stdout_raw = stdout_raw;
            let mut stderr_raw = stderr_raw;
            loop {
                let f = match read_frame(&mut rd, &mut buf).await {
                    Ok(Some(f)) => f,
                    // Transport close (EOF/RST). After the server has signalled
                    // completion this is the client's benign conn.Close; before,
                    // it is an aborted client. Either way the read-loop simply
                    // ends — completion is owned by the reaper, not here.
                    Ok(None) | Err(_) => break,
                };
                match f {
                    Frame::SynStream {
                        stream_id,
                        header_block,
                        ..
                    } => {
                        let hdr = decomp
                            .decompress(&header_block)
                            .ok()
                            .and_then(|b| headers::deserialize_headers(&b))
                            .unwrap_or_default();
                        let role = match get(&hdr, "streamType") {
                            Some("stdin") => ch::STDIN,
                            Some("stdout") => ch::STDOUT,
                            Some("stderr") => ch::STDERR,
                            Some("error") => ch::ERROR,
                            Some("resize") => ch::RESIZE,
                            _ => 0xff,
                        };
                        {
                            let mut w = writer.lock().await;
                            let _ = w.syn_reply(stream_id).await;
                        }
                        roles.insert(stream_id, role);
                        by_role.lock().await.insert(role, stream_id);
                        // Wake the completion barrier: a new role id just landed.
                        role_added.notify_waiters();

                        // When an output stream opens, spawn its pump.
                        if role == ch::STDOUT {
                            if let Some(file) = stdout_raw.take() {
                                let h = spawn_output(
                                    writer.clone(),
                                    stream_id,
                                    bridge::adopt(file),
                                );
                                // Register for abort-on-drop BEFORE handing the
                                // handle to the drain list, so a cap that fires
                                // mid-stream aborts this pump too.
                                if let Ok(mut l) = abort_list.lock() {
                                    l.push(h.abort_handle());
                                }
                                output_tasks.lock().await.push(h);
                                // The pump is now registered in `output_tasks`.
                                // Re-notify so the completion path's drain gate
                                // (which waits for the pumps, not just the role
                                // ids) observes it. Without this second notify a
                                // reaper-first completion for a fast TTY exec
                                // (`echo` via the pty master) can snapshot an
                                // EMPTY `output_tasks` between the by_role insert
                                // and this push, draining nothing and tearing the
                                // session down before the pty-master "hello" is
                                // forwarded → client times out (the tty=true flake).
                                role_added.notify_waiters();
                            }
                        } else if role == ch::STDERR {
                            if let Some(file) = stderr_raw.take() {
                                let h = spawn_output(
                                    writer.clone(),
                                    stream_id,
                                    bridge::adopt(file),
                                );
                                if let Ok(mut l) = abort_list.lock() {
                                    l.push(h.abort_handle());
                                }
                                output_tasks.lock().await.push(h);
                                role_added.notify_waiters();
                            }
                        }
                    }
                    Frame::Data {
                        stream_id,
                        flags,
                        data,
                    } => {
                        match roles.get(&stream_id).copied() {
                            Some(ch::STDIN) => {
                                if let Some(f) = stdin.as_mut() {
                                    let _ = f.write_all(&data).await;
                                    let _ = f.flush().await;
                                }
                            }
                            Some(ch::RESIZE) => {
                                if let (Some(sz), Some(pty)) =
                                    (ch::parse_resize(&data), pty_master.as_ref())
                                {
                                    bridge::set_winsize(pty, sz.width, sz.height);
                                }
                            }
                            _ => {}
                        }
                        if flags & frame::FLAG_FIN != 0 {
                            // client half-closed this stream
                            if roles.get(&stream_id).copied() == Some(ch::STDIN) {
                                // drop stdin so the process sees EOF
                                stdin = None;
                            }
                            // RACE 2: a client half-closing its write side is a
                            // detach handshake — for attach to a LIVE container
                            // there is no reaper signal and the container does
                            // not exit, so this FLAG_FIN (the client closing the
                            // streams it owns as it detaches) is what must drive
                            // completion. The transport stays up, so the EOF
                            // path at loop-end does NOT fire; signal detach
                            // inline. For exec this is harmless: the reaper wins
                            // first and client-go keeps its write half open
                            // until it reads the Status, so it does not FLAG_FIN
                            // early.
                            if is_attach {
                                client_closed.notify_waiters();
                            }
                        }
                    }
                    Frame::Ping { id } => {
                        let mut w = writer.lock().await;
                        let _ = w.write_frame(&Frame::Ping { id }).await;
                    }
                    Frame::RstStream { .. } | Frame::GoAway { .. } => break,
                    Frame::Settings { .. }
                    | Frame::WindowUpdate { .. }
                    | Frame::Headers { .. } => {}
                    Frame::SynReply { .. } => {}
                }
            }
            // The loop only exits on a client-initiated close (EOF/RST/GoAway).
            // Signal it so the completion path treats this as a detach.
            client_closed.notify_waiters();
        })
    };
    // Own the read-loop for abort-on-drop. Dropping `run_exec` (session cap)
    // now aborts this task, releasing the read half `rd` and its `Writer` Arc
    // clone — the socket can finally close.
    if let Ok(mut l) = abort_list.lock() {
        l.push(read_loop.abort_handle());
    }

    // ── completion driven by OUTPUT-DRAIN or DETACH, not transport close ──
    //
    // Completion TRIGGER (robust for BOTH verbs, mirroring ws.rs run_session):
    //   - PRIMARY: all output pumps drain to EOF. For exec this coincides with
    //     process exit; for attach it is container/pty exit.
    //   - OR: the client closes its side (detach). For attach this is the
    //     common case (the container outlives the session); for exec it is an
    //     aborted client.
    // Whichever fires first completes the session.
    //
    // The exit CODE for the exec Status still comes from the reaper, run as a
    // concurrent task so it never gates the trigger: for exec it resolves on
    // process exit (≈ output-drain); for attach `AttachWaiter` returns Ok(0)
    // instantly → Status{Success}, which is the "container exit code if known,
    // else Success" semantics. The read-loop keeps draining stdin/resize/ping
    // concurrently throughout.
    // The FULL set of stream roles the client WILL open for this session,
    // derived from the request flags + verb. client-go's createStreams (v2.go,
    // inherited by v3/v4) opens, in order: error (always), stdin (if
    // params.stdin), stdout (always), stderr (only when NOT tty). The
    // fast-exit barrier must wait for ALL of these to land in `by_role` before
    // completion is allowed to tear the connection down — otherwise a fast exec
    // (`echo`) can complete + the TaskGuard can reset a client SYN_STREAM that
    // is still in flight, surfacing as client-go's "failed to open streamer"
    // (RACE 1). The set is the same for exec and attach.
    let expected_roles: Vec<u8> = {
        let mut v = vec![ch::ERROR, ch::STDOUT];
        if params.stdin {
            v.push(ch::STDIN);
        }
        if !params.tty {
            v.push(ch::STDERR);
        }
        v
    };

    if let Some(waiter) = waiter {
        let is_attach = verb == StreamVerb::Attach;
        let mut reaper = tokio::task::spawn_blocking(move || waiter.wait());
        // Own the reaper too: if `run_exec` is dropped before the reaper
        // resolves, abort it so it cannot linger past the cap. (A blocking
        // wait already in progress can't be force-cancelled, but the handle is
        // released; the attach path also explicitly aborts it below.)
        if let Ok(mut l) = abort_list.lock() {
            l.push(reaper.abort_handle());
        }

        // Arm the detach signal ONCE, up front, and reuse the SAME future in
        // both selects below. `Notify::notified()` only observes a
        // `notify_waiters()` that fires AFTER the future is registered, so a
        // fresh `notified()` per select would miss a detach that raced an
        // earlier phase and could hang (attach with a long-lived container).
        // Pinning one future latches the next notify across both phases.
        let detached = client_closed.notified();
        tokio::pin!(detached);

        // 0. Fast-exit barrier. A fast command (e.g. `echo`) can make the
        //    output pumps drain — and the reaper fire — BEFORE the read-loop
        //    has processed all of the client's SYN_STREAM frames. If completion
        //    proceeds now, the TaskGuard tears the connection down while a
        //    still-in-flight client SYN_STREAM (stderr/stdin) is reset, which
        //    client-go reports as "failed to open streamer" (RACE 1). So WAIT
        //    until EVERY role the client WILL open (`expected_roles`, derived
        //    above from tty/stdin/verb) is present in `by_role`, bounded by a
        //    ~5s backstop. For attach the client may detach before opening the
        //    full set, so race the barrier against `client_closed` and bail
        //    straight to teardown on detach. Deadlock-free: conformant
        //    client-go opens its whole stream set via SYN_STREAM synchronously
        //    at stream() start, BEFORE it blocks on the server, so the barrier
        //    resolves in ms.
        let barrier = async {
            loop {
                // Register for the next notify BEFORE re-checking, so an insert
                // racing between check and await is not lost.
                let notified = role_added.notified();
                {
                    let g = by_role.lock().await;
                    if expected_roles.iter().all(|r| g.contains_key(r)) {
                        break;
                    }
                }
                notified.await;
            }
        };
        let mut detached_fired = false;
        if is_attach {
            // ATTACH: a client may detach before it opens its full stream set, so
            // race the barrier against the detach signal and bail to teardown.
            tokio::select! {
                _ = tokio::time::timeout(std::time::Duration::from_secs(5), barrier) => {}
                _ = &mut detached => { detached_fired = true; }
            }
        } else {
            // EXEC: the client closing its WRITE half (the TCP half-close after
            // it finishes sending SYN_STREAMs / stdin) is NOT a detach — client-go
            // keeps its READ half OPEN to receive stdout and the terminal Status.
            // Short-circuiting on that loop-end EOF here skips the output drain and
            // truncates stdout (the spdy_exec_e2e regression). So exec NEVER bails
            // on detach; it always runs reaper + drain. `detached` is left unpolled
            // so it stays valid for the completion select's safety arm below.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), barrier).await;
        }

        // 0b. Drain-readiness gate (TTY-exec output-delivery fix). The barrier
        //     above only proves the client's stream IDS have landed in
        //     `by_role`; it does NOT prove the read-loop has finished SPAWNING
        //     and REGISTERING the output pumps in `output_tasks` (those are
        //     pushed AFTER the by_role insert). For a fast TTY exec the reaper
        //     can already be resolved here, so the completion select's `drain`
        //     would `mem::take` an EMPTY `output_tasks`, await nothing, and tear
        //     the session down BEFORE the pty-master pump forwards the child's
        //     buffered output → client-go "Timeout occurred". So wait until one
        //     output pump per expected output role is registered. The pty slave
        //     closes on child exit → master EOFs → the pump (now guaranteed
        //     present) is awaited to completion by `drain`, delivering every
        //     byte before the Status. Bounded by the same backstop and raced
        //     against detach so a client that bails early still tears down.
        //     Skip when already detached (nothing left to drain for the client).
        if !detached_fired {
            let expected_pumps = {
                // Count ONLY pumps that will actually spawn: one per output role
                // the client opens (stdout always; stderr only when NOT tty —
                // matching `expected_roles` / client-go's createStreams) that the
                // session ALSO has a file for. A session with `stderr: None`
                // (stderr merged onto the pty, or stdout-only) yields a single
                // pump; gating on a fixed 2 would hang on the absent stderr pump.
                let stdout = has_stdout_file as usize;
                let stderr = (!params.tty && has_stderr_file) as usize;
                stdout + stderr
            };
            let pump_gate = async {
                loop {
                    let notified = role_added.notified();
                    if output_tasks.lock().await.len() >= expected_pumps {
                        break;
                    }
                    notified.await;
                }
            };
            tokio::pin!(pump_gate);
            if is_attach {
                tokio::select! {
                    _ = tokio::time::timeout(std::time::Duration::from_secs(5), pump_gate) => {}
                    _ = &mut detached => { detached_fired = true; }
                }
            } else {
                // EXEC: as above, never bail on detach — wait for the pumps to
                // register (bounded) so the drain has them, keeping `detached`
                // valid for the completion select.
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), pump_gate).await;
            }
        }

        // 1. COMPLETION SELECT — the FIRST of three triggers wins (mirroring
        //    ws.rs run_session's drive-off-drain shape, extended for SPDY's
        //    concurrent reaper):
        //      - REAPER resolves: process/exec exit. EXEC's normal path — gives
        //        the real exit code for the Status. The exec client-go keeps
        //        its write half OPEN until it reads the Status, so it never
        //        detaches early; the reaper fires first (closing-first here
        //        would deadlock exec — a previously-fixed bug, so the reaper
        //        trigger is preserved).
        //      - DRAIN: all output pumps reach EOF. For exec this coincides
        //        with process exit; for attach it is container/pty exit. We
        //        AWAIT (not abort) the pumps so a fast-exiting process's tail
        //        bytes are not truncated before the Status.
        //      - DETACH: the client closed its side. For attach this is the
        //        normal completion path (the container outlives the session);
        //        for exec it is an aborted client.
        //    The reaper arm is included ONLY for exec: for attach the
        //    `AttachWaiter` never fires while the container lives, so racing it
        //    would just sit idle (and we must not block on it) — attach
        //    completes via DRAIN (container exit) or DETACH instead.
        //
        //    `detached` may already be resolved from the barrier phase; a
        //    completed `Notified` panics on re-poll, so skip the select then.
        let mut reaped: Option<i32> = None;
        if !detached_fired {
            let drain = async {
                let tasks = std::mem::take(&mut *output_tasks.lock().await);
                for t in tasks {
                    let _ = t.await;
                }
            };
            // `drain` is a fresh async block (Unpin not guaranteed) → pin it.
            // `reaper` is a `JoinHandle`, which is `Unpin`, so `&mut reaper`
            // polls fine without pinning and the handle survives the select for
            // the abort/await below.
            tokio::pin!(drain);
            if is_attach {
                tokio::select! {
                    _ = &mut drain => {}
                    _ = &mut detached => {}
                }
            } else {
                tokio::select! {
                    // EXEC: reaper-first is the dominant path; drain coincides.
                    r = &mut reaper => {
                        reaped = Some(r.unwrap_or(Ok(-1)).unwrap_or(-1));
                        // The process exited; let any tail output flush before
                        // the Status so client-go sees every byte first.
                        let _ = drain.await;
                    }
                    _ = &mut drain => {}
                    _ = &mut detached => {
                        // The client closed its WRITE side (half-close) — but for
                        // exec it keeps its READ side open for stdout + the Status.
                        // STILL drain every output byte before the Status so a
                        // half-closing client receives the full stdout; the exit
                        // code is awaited from the reaper below.
                        let _ = drain.await;
                    }
                }
            }
        }

        // Exit code for the Status. Exec: the reaper's real code → Success /
        // Failure+NonZeroExitCode (already captured if the reaper arm won;
        // otherwise await it now that output drained). Attach has no meaningful
        // exit code (the container outlives the session) → Status{Success}
        // (exit 0) and do NOT block on the reaper, which would hang until the
        // container itself exits; abort it instead.
        let exit = if is_attach {
            reaper.abort();
            0
        } else if let Some(code) = reaped {
            code
        } else {
            reaper.await.unwrap_or(Ok(-1)).unwrap_or(-1)
        };

        // Snapshot the stream ids the client actually opened. We FIN only
        // streams the client opened to read: for a TTY exec there is no
        // stderr stream, so `STDERR` is simply absent and gets no FIN.
        let by_role = by_role.lock().await;
        let stdout_id = by_role.get(&ch::STDOUT).copied();
        let stderr_id = by_role.get(&ch::STDERR).copied();
        let error_id = by_role.get(&ch::ERROR).copied();
        drop(by_role);

        let mut w = writer.lock().await;
        // 2. FLAG_FIN on stdout (empty DATA frame, fin flag set).
        if let Some(id) = stdout_id {
            let _ = w.data(id, frame::FLAG_FIN, Vec::new()).await;
        }
        // 3. FLAG_FIN on stderr (only if the client opened one — absent for TTY).
        if let Some(id) = stderr_id {
            let _ = w.data(id, frame::FLAG_FIN, Vec::new()).await;
        }
        // 4 + 5. v4 metav1.Status on the error stream, then its FLAG_FIN — the
        //        final completion gate. The empty DATA + fin in one frame both
        //        delivers the Status client-go's errorstream.go decodes and
        //        closes the error stream.
        if let Some(id) = error_id {
            let json = status::exit_status_json(exit);
            let _ = w.data(id, frame::FLAG_FIN, json).await;
        }
        drop(w);
    }

    // GRACEFUL TEARDOWN — decouple teardown from completion (the last tty=true
    // EPIPE flake). Completion has been signalled: the Status + FLAG_FINs are on
    // the wire. But for a FAST tty exec (`echo` exits in ms) the server reaches
    // here while client-go may STILL be writing its SYN_STREAM/stdin frames to
    // OPEN the session. If we abort the read-loop / drop the socket now, those
    // in-flight client writes hit a closed socket → EPIPE → "failed to open
    // streamer" (RACE 3 — the residual window the barrier + drain gates above
    // narrowed but could not close, since they gate completion, not teardown).
    //
    // So do NOT abort immediately. Keep the read-loop ALIVE so it keeps DRAINING
    // the client's inbound frames (consuming its handshake/stdin writes — never
    // EPIPE), and WAIT for the CLIENT to close its side: the read-loop ends on
    // EOF/RST/GoAway once client-go has read the Status off the error stream and
    // closed cleanly. We await the read-loop handle directly — it resolves on
    // exactly that client-close. BOUNDED by a 5s grace so a misbehaving client
    // can never hang teardown.
    //
    // This does NOT reintroduce the old Status-before-close deadlock: the Status
    // was ALREADY emitted above (lines emitting FLAG_FIN + metav1.Status), so we
    // wait-for-close AFTER the Status, not before it. Both exec and attach reach
    // here after completion; for attach the detach path already drove completion,
    // and the read-loop has typically ended (or will within grace), so detach
    // still completes. After the grace wait — or the moment the client closes —
    // we wind the read-loop down explicitly and `_guard` drops at function end,
    // aborting the whole registered set (a no-op if the read-loop already ended,
    // but the SAME drop runs if `run_exec`'s future is cancelled before reaching
    // here (session cap), which is what makes the cap actually tear down).
    // If the grace elapses (Err) the read-loop is still running → abort + reap.
    // If it returns Ok the read-loop already finished (client closed) — do NOT
    // poll the JoinHandle again (awaiting a completed handle panics).
    if tokio::time::timeout(std::time::Duration::from_secs(5), &mut read_loop)
        .await
        .is_err()
    {
        read_loop.abort();
        let _ = read_loop.await;
    }
}

/// Pump a session output file → DATA frames on `stream_id` until EOF.
fn spawn_output<W>(
    writer: Arc<Mutex<Writer<W>>>,
    stream_id: u32,
    mut file: tokio::fs::File,
) -> tokio::task::JoinHandle<()>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            match file.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let mut w = writer.lock().await;
                    if w.data(stream_id, 0, buf[..n].to_vec()).await.is_err() {
                        break;
                    }
                }
            }
        }
    })
}

/// Drive a SPDY portforward session. Streams arrive in PAIRS keyed by
/// `requestID`; the `data` stream carries bytes to/from the dialed TCP socket,
/// the `error` stream carries plaintext failures. RST both on dial failure.
pub async fn run_portforward<S>(stream: S, dial_host: String, netns_path: Option<String>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut rd, wr) = tokio::io::split(stream);
    let writer = Arc::new(Mutex::new(Writer {
        out: wr,
        comp: HeaderCompressor::new(),
    }));
    let mut decomp = HeaderDecompressor::new();
    let mut buf = Vec::with_capacity(16384);

    struct Pair {
        port: u16,
        data_stream: Option<u32>,
        error_stream: Option<u32>,
        tcp: Option<Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>>,
    }
    // requestID → pair
    let mut pairs: HashMap<String, Pair> = HashMap::new();
    // data stream_id → requestID (route DATA frames)
    let mut data_route: HashMap<u32, String> = HashMap::new();
    let mut tcp_tasks = Vec::new();

    loop {
        let f = match read_frame(&mut rd, &mut buf).await {
            Ok(Some(f)) => f,
            Ok(None) | Err(_) => break,
        };
        match f {
            Frame::SynStream {
                stream_id,
                header_block,
                ..
            } => {
                let hdr = decomp
                    .decompress(&header_block)
                    .ok()
                    .and_then(|b| headers::deserialize_headers(&b))
                    .unwrap_or_default();
                let stype = get(&hdr, "streamType").unwrap_or("").to_string();
                let port: u16 = get(&hdr, "port").and_then(|p| p.parse().ok()).unwrap_or(0);
                // requestID may be absent — fall back to the port as the key.
                let req_id = get(&hdr, "requestID")
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("port-{port}"));

                {
                    let mut w = writer.lock().await;
                    let _ = w.syn_reply(stream_id).await;
                }

                let entry = pairs.entry(req_id.clone()).or_insert(Pair {
                    port,
                    data_stream: None,
                    error_stream: None,
                    tcp: None,
                });
                if entry.port == 0 {
                    entry.port = port;
                }
                if stype == "error" {
                    entry.error_stream = Some(stream_id);
                } else {
                    // default/"data" stream
                    entry.data_stream = Some(stream_id);
                    data_route.insert(stream_id, req_id.clone());
                    // Dial now and start the TCP→SPDY pump. When the sandbox
                    // has a netns (contract §B AMENDED 2026-06-19) the connect
                    // happens INSIDE the sandbox netns; otherwise host-netns.
                    match dial_pf(dial_host.as_str(), entry.port, netns_path.as_deref()).await {
                        Ok(sock) => {
                            let (rd_half, wr_half) = sock.into_split();
                            entry.tcp = Some(Arc::new(Mutex::new(wr_half)));
                            tcp_tasks.push(spawn_tcp_to_spdy(writer.clone(), stream_id, rd_half));
                        }
                        Err(_) => {
                            let mut w = writer.lock().await;
                            if let Some(eid) = entry.error_stream {
                                let _ = w.data(eid, frame::FLAG_FIN, b"dial failed".to_vec()).await;
                            }
                            let _ = w.rst(stream_id, frame::RST_REFUSED_STREAM).await;
                            if let Some(eid) = entry.error_stream {
                                let _ = w.rst(eid, frame::RST_REFUSED_STREAM).await;
                            }
                        }
                    }
                }
            }
            Frame::Data {
                stream_id,
                flags,
                data,
            } => {
                if let Some(req) = data_route.get(&stream_id) {
                    if let Some(pair) = pairs.get(req) {
                        if let Some(tcp) = pair.tcp.clone() {
                            let mut g = tcp.lock().await;
                            let _ = g.write_all(&data).await;
                            let _ = g.flush().await;
                            // Propagate the client's CloseWrite (FLAG_FIN) to the
                            // backend as a TCP half-close: shut down the write
                            // half so the backend sees EOF. Do NOT drop the pair —
                            // the backend→client read direction (the TCP→SPDY pump
                            // holding the read half) must keep flowing.
                            if flags & frame::FLAG_FIN != 0 {
                                let _ = g.shutdown().await;
                            }
                        }
                    }
                }
            }
            Frame::Ping { id } => {
                let mut w = writer.lock().await;
                let _ = w.write_frame(&Frame::Ping { id }).await;
            }
            Frame::RstStream { .. } | Frame::GoAway { .. } => break,
            _ => {}
        }
    }
    // BOUNDED drain of the TCP→SPDY pumps. We await (do NOT abort) so in-flight
    // backend bytes are flushed to the client instead of being truncated
    // mid-transfer — the common case is a half-close where the backend EOFs
    // promptly and each pump returns on its own. But a backend that keeps its
    // connection open and never sends EOF after the client disconnects would
    // make an unconditional await hang forever, so we cap the drain with a
    // deadline and abort any pump still running past it.
    let drain_abort: Vec<_> = tcp_tasks.iter().map(|t| t.abort_handle()).collect();
    let drain = async {
        for t in tcp_tasks {
            let _ = t.await;
        }
    };
    if tokio::time::timeout(std::time::Duration::from_secs(3), drain)
        .await
        .is_err()
    {
        // Deadline hit: a non-closing backend kept a pump alive. Abort the
        // stragglers so run_portforward cannot block indefinitely.
        for h in drain_abort {
            h.abort();
        }
    }
}

/// Dial `host:port` for a forwarded connection. With `netns_path = Some`, the
/// connect runs INSIDE the sandbox network namespace (contract §B AMENDED
/// 2026-06-19): `lightr_cri_net::netns::dial_in_netns` does the `setns` +
/// blocking `std::net` connect on a DEDICATED thread (never a tokio worker —
/// `setns` mutates the calling thread's netns), and hands the connected
/// `std::TcpStream` back; we adopt it as a tokio stream off the async runtime
/// via `spawn_blocking` so the cross-thread `join()` never parks a worker.
/// `None` (host_network) keeps the ordinary host-netns dial unchanged.
async fn dial_pf(host: &str, port: u16, netns_path: Option<&str>) -> std::io::Result<TcpStream> {
    match netns_path {
        None => TcpStream::connect((host, port)).await,
        Some(ns) => {
            let ns = ns.to_string();
            let host = host.to_string();
            // The dedicated dial thread blocks on .join(); run it off-runtime.
            let std_sock = tokio::task::spawn_blocking(move || {
                lightr_cri_net::netns::dial_in_netns(&ns, &host, port)
            })
            .await
            .map_err(|e| std::io::Error::other(format!("dial_in_netns join: {e}")))??;
            std_sock.set_nonblocking(true)?;
            TcpStream::from_std(std_sock)
        }
    }
}

/// Pump the TCP read half → DATA frames on the data stream.
fn spawn_tcp_to_spdy<W>(
    writer: Arc<Mutex<Writer<W>>>,
    stream_id: u32,
    mut rd: tokio::net::tcp::OwnedReadHalf,
) -> tokio::task::JoinHandle<()>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            match rd.read(&mut buf).await {
                Ok(0) | Err(_) => {
                    let mut w = writer.lock().await;
                    let _ = w.data(stream_id, frame::FLAG_FIN, Vec::new()).await;
                    break;
                }
                Ok(n) => {
                    let mut w = writer.lock().await;
                    if w.data(stream_id, 0, buf[..n].to_vec()).await.is_err() {
                        break;
                    }
                }
            }
        }
    })
}
