# Seam Contract v1.1 — ADDITIVE deltas over v1

- **Status:** FROZEN 2026-06-12 (owner mandate "bora pra cima, mantendo rigor").
- v1 stays in force; this file lists only additions. All additions are
  serde-default-compatible (old state files load unchanged). The fake and the
  future lightr backend implement both versions with one trait.

## §A Vocabulary additions

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DnsConfig {
    pub servers: Vec<String>,
    pub searches: Vec<String>,
    pub options: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Protocol { Tcp, Udp, Sctp }

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PortMapping {
    pub protocol: Protocol,
    pub container_port: i32,
    pub host_port: i32,
    #[serde(default)]
    pub host_ip: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthConfig {
    pub username: String,
    pub password: String,
    pub auth: String,
    pub server_address: String,
}
```

`SandboxConfig` gains (all `#[serde(default)]`): `hostname: String`,
`host_network: bool`, `dns: Option<DnsConfig>`,
`port_mappings: Vec<PortMapping>`.
`SandboxStatus` gains: `ip: Option<String>` (host-routable pod IP; None when
host_network or no CNI), `netns_path: Option<String>`.
`ContainerConfig` gains: `tty: bool`, `stdin: bool`.

## §B Streaming sessions (struct-of-handles — lead decision)

Object-safe trait sessions were rejected: handles bridge cleanly to tokio
(`File` → `AsyncFd`/`from_std`) and keep the backend sync.

```rust
/// Exit waiter — consumed once.
pub trait ExitWaiter: Send {
    fn wait(self: Box<Self>) -> Result<i32>;
}

/// Live I/O of an exec or attach session. tty=true → stdout carries the
/// pty stream, stderr is None, pty_master enables TIOCSWINSZ resize.
pub struct StreamSession {
    pub stdin: Option<std::fs::File>,
    pub stdout: Option<std::fs::File>,
    pub stderr: Option<std::fs::File>,
    pub pty_master: Option<std::fs::File>,
    pub waiter: Box<dyn ExitWaiter>,
}
```

New `CriBackend` methods (default impls return
`Err(BackendError::Internal("v1.1 not implemented"))` so v1-only backends
still compile):

```rust
fn open_exec(&self, id: &ContainerId, cmd: &[String], tty: bool, stdin: bool)
    -> Result<StreamSession>;
/// Attach to the container's live stdio (spawned with held pipes/pty).
fn open_attach(&self, id: &ContainerId) -> Result<StreamSession>;
/// Auth-aware pull; default delegates to pull_image (auth ignored = fake-honest).
fn pull_image_with_auth(&self, image_ref: &str, _auth: Option<&AuthConfig>)
    -> Result<PulledImage> { self.pull_image(image_ref) }
```

PortForward needs no backend method: the streamer dials
`SandboxStatus.ip:port` (CNI IPs are host^W runner-netns-routable by design)
or `127.0.0.1:port` for host_network sandboxes.

## §C Container log law (behavioral, no new method)

From v1.1 on, `start_container` MUST tee the process stdout/stderr to the
CRI log file at `sandbox.log_directory + "/" + container.log_path`
(creating parent dirs), in CRI format: each line
`<RFC3339Nano> <stdout|stderr> <P|F> <data>` (F = full line, P = partial).
An empty file MUST exist from start even if the process emits nothing
(kubelet ReopenContainerLog law). Crash-only: append-only writes, no
buffering beyond line granularity.

## §D Networking law (behavioral)

`run_sandbox` with `host_network=false` and CNI available: create a pinned
netns (bind-mount law, research/r1-cni.md), invoke the CNI chain (ADD
forward / DEL reverse, portMappings via runtimeConfig), record `ip` +
`netns_path` in the persisted sandbox record BEFORE returning (crash-only).
`remove_sandbox`: CNI DEL then umount2+unlink the netns — idempotent,
fail-closed on DEL errors (log + continue teardown). Containers of the
sandbox join via setns(pre_exec). `host_network=true`: no netns, ip=None.
resolv.conf: when `dns` is Some, synthesize and bind-mount/copy into the
container's /etc/resolv.conf view (fake: place file + env contract per
build-spec-r1 §4).
Probe-truthful: without CAP_SYS_ADMIN/NET_ADMIN (macOS, unprivileged), CNI
is absent — sandboxes fall back to host_network-only behavior and
`Status.NetworkReady=false` with reason; never fake an IP.

## §E Versioning

v1.1 is additive; v1 vectors run unchanged. New vectors cover §B/§C/§D.
Next planned: v1.2 (events journal for evented PLEG, pod-level stats) — R2,
deliberately NOT in v1.1 (kubelet 1.33 never calls them; alpha gates off).
