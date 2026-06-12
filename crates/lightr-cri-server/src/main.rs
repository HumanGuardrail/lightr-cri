//! WP-D: bin `lightr-cri` — wire the streaming server (build-spec-r1 §3 WP-D).
//!
//! FROZEN laws (carry-over from R0 + R1 additions):
//! - Args, clap-free: `--socket PATH` (default /run/lightr/cri.sock),
//!   `--state PATH` (default per fake backend law).
//! - tokio rt, UnixListener, serve RuntimeService + ImageService.
//! - Stream server: `lightr_cri_stream::serve(127.0.0.1:0, factory)` where
//!   the factory maps (verb, params) → a StreamSession from the backend
//!   (Exec/Attach → open_exec/open_attach; PortForward → stream crate dials
//!   params.dial_target itself). ServerHandle (registry + base_url) is passed
//!   into RuntimeShell via `with_stream`.
//! - Both servers run on the same tokio runtime.
//! - SIGTERM graceful: shut down the gRPC UDS server, then the stream server.
//! - The process holds NO state: restartable at any instant (crash-only).
//!   Stream tokens are ephemeral — lost on restart — clients retry.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use lightr_cri_fake::FakeBackend;
use lightr_cri_proto::v1::{
    image_service_server::ImageServiceServer, runtime_service_server::RuntimeServiceServer,
};
use lightr_cri_shell::{ImageShell, RuntimeShell};
use lightr_cri_stream::{SessionFactory, StreamParams, StreamSession, StreamVerb};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;

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

/// Session factory that dispatches (verb, params) → backend open_exec/open_attach.
/// PortForward never calls this — the stream crate dials params.dial_target itself.
struct BackendFactory {
    backend: Arc<FakeBackend>,
}

impl SessionFactory for BackendFactory {
    fn open_session(
        &self,
        verb: StreamVerb,
        params: &StreamParams,
    ) -> Result<StreamSession, String> {
        use lightr_cri_backend::{ContainerId, CriBackend};
        match verb {
            StreamVerb::Exec => {
                let cid = params
                    .container
                    .as_deref()
                    .ok_or("exec: missing container id")?;
                self.backend
                    .open_exec(
                        &ContainerId(cid.to_string()),
                        &params.cmd,
                        params.tty,
                        params.stdin,
                    )
                    .map_err(|e| e.to_string())
            }
            StreamVerb::Attach => {
                let cid = params
                    .container
                    .as_deref()
                    .ok_or("attach: missing container id")?;
                self.backend
                    .open_attach(&ContainerId(cid.to_string()))
                    .map_err(|e| e.to_string())
            }
            StreamVerb::PortForward => {
                // PortForward: the stream crate dials params.dial_target itself;
                // this factory is not called for portforward sessions.
                Err(
                    "portforward: factory should not be called (stream crate dials directly)"
                        .to_string(),
                )
            }
        }
    }
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

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("lightr-cri: failed to build tokio runtime: {e}");
            std::process::exit(1);
        }
    };

    let exit_code = rt.block_on(async move {
        // Open the fake backend (R0; the lightr backend arrives at integration).
        let backend = match FakeBackend::open(&state_path) {
            Ok(b) => Arc::new(b),
            Err(e) => {
                eprintln!(
                    "lightr-cri: failed to open backend at {}: {e}",
                    state_path.display()
                );
                return 1;
            }
        };

        // Start the streaming server on an ephemeral 127.0.0.1 port.
        // The factory bridges (verb, params) → backend open_exec/open_attach.
        // Crash-only: tokens are ephemeral — lost on restart — clients retry.
        let stream_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let factory = Arc::new(BackendFactory {
            backend: Arc::clone(&backend),
        });
        let stream_handle = match lightr_cri_stream::serve(
            stream_addr,
            factory as Arc<dyn lightr_cri_stream::SessionFactory>,
        )
        .await
        {
            Ok(h) => h,
            Err(e) => {
                eprintln!("lightr-cri: failed to start stream server: {e}");
                return 1;
            }
        };
        eprintln!("stream server {}", stream_handle.base_url());

        // Remove stale socket file if present.
        if socket_path.exists() {
            if let Err(e) = std::fs::remove_file(&socket_path) {
                eprintln!(
                    "lightr-cri: failed to remove stale socket {}: {e}",
                    socket_path.display()
                );
                return 1;
            }
        }

        // Ensure parent directory exists.
        if let Some(parent) = socket_path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    eprintln!(
                        "lightr-cri: failed to create socket directory {}: {e}",
                        parent.display()
                    );
                    return 1;
                }
            }
        }

        let listener = match UnixListener::bind(&socket_path) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("lightr-cri: bind {} failed: {e}", socket_path.display());
                return 1;
            }
        };

        eprintln!("listening {}", socket_path.display());

        let incoming = UnixListenerStream::new(listener);

        let shutdown = async {
            let mut sigterm =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(_) => {
                        // fallback: only ctrl_c
                        tokio::signal::ctrl_c().await.ok();
                        return;
                    }
                };
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
        };

        let result = tonic::transport::Server::builder()
            .add_service(RuntimeServiceServer::new(RuntimeShell::with_stream(
                Arc::clone(&backend),
                &stream_handle,
            )))
            .add_service(ImageServiceServer::new(ImageShell::new(Arc::clone(
                &backend,
            ))))
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await;

        // Shut down the stream server after the gRPC server has stopped.
        stream_handle.shutdown().await;

        match result {
            Ok(()) => {
                eprintln!("shutdown");
                0
            }
            Err(e) => {
                eprintln!("lightr-cri: serve error: {e}");
                1
            }
        }
    });

    std::process::exit(exit_code);
}
