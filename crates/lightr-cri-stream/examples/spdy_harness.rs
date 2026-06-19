//! Diagnostic harness: boot the SPDY/3.1 stream server with a fixed fixture
//! exec session and print `URL TOKEN` on stdout, then serve forever. Used by
//! the client-go SPDY oracle (a real `remotecommand.NewSPDYExecutor`) to
//! reproduce the critest interop failure without the full CRI lifecycle.
//!
//! Not part of the crate's shipped surface — examples are dev-only.

use std::io::Write as _;
use std::sync::Arc;

use lightr_cri_backend::{ExitWaiter, StreamSession};
use lightr_cri_stream::{serve, SessionFactory, StreamParams, StreamVerb};

struct CodeWaiter(i32);
impl ExitWaiter for CodeWaiter {
    fn wait(self: Box<Self>) -> lightr_cri_backend::Result<i32> {
        Ok(self.0)
    }
}

fn seeded_temp_file(bytes: &[u8]) -> std::fs::File {
    use std::io::{Seek, SeekFrom};
    let path = std::env::temp_dir().join(format!("spdy-harness-{}.tmp", std::process::id()));
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    f.write_all(bytes).unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    let _ = std::fs::remove_file(&path);
    f
}

#[tokio::main]
async fn main() {
    let factory: Arc<dyn SessionFactory> = Arc::new(|_v: StreamVerb, _p: &StreamParams| {
        Ok(StreamSession {
            stdin: None,
            stdout: Some(seeded_temp_file(b"hello from exec\n")),
            stderr: None,
            pty_master: None,
            waiter: Box::new(CodeWaiter(0)),
        })
    });

    let handle = serve("127.0.0.1:0".parse().unwrap(), factory)
        .await
        .unwrap();
    let token = handle
        .registry()
        .mint(
            StreamVerb::Exec,
            StreamParams {
                container: Some("c0".into()),
                sandbox: None,
                cmd: vec!["echo".into()],
                tty: false,
                stdin: false,
                ports: vec![],
                dial_target: None,
                netns_path: None,
            },
        )
        .unwrap();

    println!("{} {}", handle.base_url(), token);
    std::io::stdout().flush().unwrap();

    // Serve until killed.
    std::future::pending::<()>().await;
}
