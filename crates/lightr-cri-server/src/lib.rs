//! The `lightr-cri` server composition root — **backend-agnostic**.
//!
//! This is THE swap seam for integration. The whole server (gRPC UDS +
//! streaming) is wired here generically over any `B: CriBackend`. The binary
//! in this repo drives the in-memory `FakeBackend` (R1 conformance); the
//! integrated runtime constructs the real `LightrBackend` and calls the SAME
//! `run_blocking`/`serve` entry points — a backend-construction change, **never
//! a copy-paste of this wiring** (CLAUDE.md contract-swap law).
//!
//! FROZEN laws (carry-over from R0 + R1):
//! - tokio rt, UnixListener, serve RuntimeService + ImageService.
//! - Stream server: `lightr_cri_stream::serve(127.0.0.1:0, factory)`; the
//!   factory maps (verb, params) → a StreamSession from the backend
//!   (Exec/Attach → open_exec/open_attach; PortForward → stream crate dials
//!   params.dial_target itself). ServerHandle is passed into RuntimeShell.
//! - Both servers run on the same tokio runtime.
//! - SIGTERM graceful: shut down the gRPC UDS server, then the stream server.
//! - The process holds NO state: restartable at any instant (crash-only).
//!   Stream tokens are ephemeral — lost on restart — clients retry.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use lightr_cri_backend::CriBackend;
use lightr_cri_proto::v1::{
    image_service_server::ImageServiceServer, runtime_service_server::RuntimeServiceServer,
};
use lightr_cri_shell::{ImageShell, RuntimeShell};
use lightr_cri_stream::{SessionFactory, StreamParams, StreamSession, StreamVerb};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;

/// Session factory that dispatches (verb, params) → backend open_exec/open_attach.
/// Generic over the backend so the real `LightrBackend` drops in unchanged.
/// PortForward never calls this — the stream crate dials params.dial_target itself.
pub struct BackendFactory<B: CriBackend> {
    backend: Arc<B>,
}

impl<B: CriBackend> BackendFactory<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

impl<B: CriBackend> SessionFactory for BackendFactory<B> {
    fn open_session(
        &self,
        verb: StreamVerb,
        params: &StreamParams,
    ) -> Result<StreamSession, String> {
        use lightr_cri_backend::ContainerId;
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

/// Build a multi-thread tokio runtime and run the server to completion,
/// returning the process exit code. This is the entry the integrated runtime
/// calls after constructing the real backend:
///
/// ```ignore
/// let backend = Arc::new(LightrBackend::new(home)?);
/// std::process::exit(lightr_cri_server::run_blocking(backend, socket_path));
/// ```
pub fn run_blocking<B: CriBackend>(backend: Arc<B>, socket_path: PathBuf) -> i32 {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("lightr-cri: failed to build tokio runtime: {e}");
            return 1;
        }
    };
    rt.block_on(serve(backend, socket_path))
}

/// Async server core: start the streaming server, bind the UDS, serve the two
/// CRI services, and tear both down on SIGTERM. Backend-agnostic by design.
/// Returns the process exit code (0 = clean shutdown).
pub async fn serve<B: CriBackend>(backend: Arc<B>, socket_path: PathBuf) -> i32 {
    // Start the streaming server on an ephemeral 127.0.0.1 port.
    // The factory bridges (verb, params) → backend open_exec/open_attach.
    // Crash-only: tokens are ephemeral — lost on restart — clients retry.
    let stream_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let factory = Arc::new(BackendFactory::new(Arc::clone(&backend)));
    let stream_handle =
        match lightr_cri_stream::serve(stream_addr, factory as Arc<dyn SessionFactory>).await {
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
        .add_service(ImageServiceServer::new(ImageShell::new(Arc::clone(&backend))))
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
}
