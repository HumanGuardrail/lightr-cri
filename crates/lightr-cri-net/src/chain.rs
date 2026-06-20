//! CNI chain invocation — per research/r1-cni.md "Invocation contract".
//!
//! Laws:
//! - Build per-plugin stdin config: plugin object + injected cniVersion + name (from conflist)
//!   + (for ADD, after first) prevResult + runtimeConfig (only declared capabilities;
//!     portmap gets `{"portMappings":[...]}` from `port_mappings`).
//! - Iterate plugins[] FORWARD for ADD, REVERSE for DEL.
//! - Exec binary from bin_dir (default /opt/cni/bin, override CNI_PATH env).
//! - Env: CNI_COMMAND, CNI_CONTAINERID, CNI_NETNS, CNI_IFNAME=eth0, CNI_PATH.
//! - Feed stdin, read stdout. Non-zero exit → parse error JSON → CniError(msg).
//! - Thread prevResult forward.  Final result: first IPv4 from ips[].
//! - DEL: ignore most errors (log + continue, fail-closed teardown).
//!
//! Pure functions `derive_plugin_config`, `parse_result`, `parse_error` are
//! host-testable without any privilege.

use lightr_cri_backend::PortMapping;
use serde_json::Value;
use std::io;
use std::path::Path;
use std::process::{Command, Stdio};

/// Minimal CNI result returned from a successful ADD.
pub struct CniResult {
    pub ip: Option<String>,
}

/// CNI invocation error (plugin stderr / error JSON message).
#[derive(Debug)]
pub struct CniError(pub String);

impl std::fmt::Display for CniError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CniError {}

impl From<io::Error> for CniError {
    fn from(e: io::Error) -> Self {
        CniError(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Invoke the CNI ADD command for the conflist at `conflist_path`.
///
/// Returns the pod IP (if any) extracted from the final plugin's result.
pub fn add(
    conflist_path: &Path,
    container_id: &str,
    netns_path: &Path,
    port_mappings: &[PortMapping],
) -> Result<CniResult, CniError> {
    let conflist = load_conflist(conflist_path)?;
    let cni_version = conflist_version(&conflist);
    let net_name = conflist_name(&conflist);
    let plugins = conflist_plugins(&conflist)?;
    let bin_dir = bin_dir_from_env();

    let mut prev_result: Option<Value> = None;

    for plugin in &plugins {
        let plugin_type = plugin_type(plugin)?;
        let stdin = derive_plugin_config(
            plugin,
            &cni_version,
            &net_name,
            prev_result.as_ref(),
            port_mappings,
        );
        let stdout = exec_plugin(
            &bin_dir,
            plugin_type,
            "ADD",
            container_id,
            netns_path.to_str().unwrap_or(""),
            &stdin,
        )?;
        let result = parse_result(&stdout)
            .map_err(|e| CniError(format!("parse result from {plugin_type}: {e}")))?;
        prev_result = Some(result);
    }

    // Extract the first IPv4 from the last plugin's result.
    let ip = prev_result.as_ref().and_then(extract_first_ipv4);

    Ok(CniResult { ip })
}

/// Invoke the CNI DEL command for the conflist at `conflist_path`.
///
/// Iterates plugins in REVERSE order.  Per spec, individual plugin errors are
/// logged and skipped (fail-closed teardown: attempt all plugins even if one fails).
pub fn del(
    conflist_path: &Path,
    container_id: &str,
    netns_path: &Path,
    port_mappings: &[PortMapping],
) -> Result<(), CniError> {
    let conflist = load_conflist(conflist_path)?;
    let cni_version = conflist_version(&conflist);
    let net_name = conflist_name(&conflist);
    let plugins = conflist_plugins(&conflist)?;
    let bin_dir = bin_dir_from_env();

    let mut last_err: Option<CniError> = None;

    for plugin in plugins.iter().rev() {
        let plugin_type = match plugin_type(plugin) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("[lightr-cri-net] del: skip plugin (no type): {e}");
                continue;
            }
        };
        // DEL does not use prevResult; portMappings capability still required for portmap.
        let stdin = derive_plugin_config(plugin, &cni_version, &net_name, None, port_mappings);
        match exec_plugin(
            &bin_dir,
            plugin_type,
            "DEL",
            container_id,
            netns_path.to_str().unwrap_or(""),
            &stdin,
        ) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("[lightr-cri-net] del: plugin {plugin_type} error (continuing): {e}");
                last_err = Some(e);
            }
        }
    }

    // Fail-closed: surface the last error if all attempts are done.
    match last_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Pure (host-testable) helpers
// ---------------------------------------------------------------------------

/// Build the per-plugin stdin JSON config.
///
/// Rules (CNI spec 1.0/1.1):
/// 1. Start with the plugin's own object (as parsed from the conflist `plugins[]` entry).
/// 2. Inject top-level `cniVersion` and `name` from the conflist.
/// 3. For ADD only, if `prev_result` is Some, inject it as `prevResult`.
/// 4. Inject `runtimeConfig` only for capabilities declared by the plugin:
///    - `portMappings` capability → `runtimeConfig.portMappings = port_mappings` (serialised).
pub fn derive_plugin_config(
    plugin: &Value,
    cni_version: &str,
    net_name: &str,
    prev_result: Option<&Value>,
    port_mappings: &[PortMapping],
) -> String {
    let mut obj = match plugin.as_object().cloned() {
        Some(m) => m,
        None => serde_json::Map::new(),
    };

    // Inject top-level fields.
    obj.insert(
        "cniVersion".to_string(),
        Value::String(cni_version.to_string()),
    );
    obj.insert("name".to_string(), Value::String(net_name.to_string()));

    // prevResult threading.
    if let Some(prev) = prev_result {
        obj.insert("prevResult".to_string(), prev.clone());
    }

    // runtimeConfig — only inject capabilities the plugin declares.
    let has_portmappings_cap = plugin
        .get("capabilities")
        .and_then(|c| c.get("portMappings"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if std::env::var_os("LIGHTR_CNI_TRACE").is_some() {
        eprintln!(
            "[lightr-cri-net][TRACE] derive: type={:?} has_portmappings_cap={} port_mappings.len()={}",
            plugin.get("type"),
            has_portmappings_cap,
            port_mappings.len()
        );
    }

    if has_portmappings_cap && !port_mappings.is_empty() {
        // CRI spec: host_port == 0 means "no host mapping" — omit those entries.
        // Passing host_port=0 to the portmap CNI plugin produces:
        //   "CNI ADD: portmap: Invalid host port number: 0"
        // (observed in critest v1.33 conformance run).
        let pm_values: Vec<Value> = port_mappings
            .iter()
            .filter(|pm| pm.host_port > 0)
            .map(serialize_port_mapping)
            .collect();
        if !pm_values.is_empty() {
            let mut runtime_cfg = serde_json::Map::new();
            runtime_cfg.insert("portMappings".to_string(), Value::Array(pm_values));
            obj.insert("runtimeConfig".to_string(), Value::Object(runtime_cfg));
        }
    }

    serde_json::to_string(&Value::Object(obj)).unwrap_or_else(|_| "{}".to_string())
}

fn serialize_port_mapping(pm: &PortMapping) -> Value {
    use lightr_cri_backend::Protocol;
    let proto = match pm.protocol {
        Protocol::Tcp => "tcp",
        Protocol::Udp => "udp",
        Protocol::Sctp => "sctp",
    };
    serde_json::json!({
        "hostPort": pm.host_port,
        "containerPort": pm.container_port,
        "protocol": proto,
        "hostIP": pm.host_ip,
    })
}

/// Parse a successful CNI result JSON (stdout from a plugin).
///
/// Returns the raw `Value` for prevResult threading; callers that need the IP
/// use `extract_first_ipv4`.
pub fn parse_result(stdout: &str) -> Result<Value, String> {
    let v: Value = serde_json::from_str(stdout).map_err(|e| e.to_string())?;
    Ok(v)
}

/// Parse a CNI error JSON (stdout on non-zero exit).
///
/// CNI spec: `{ "cniVersion": "...", "code": <N>, "msg": "...", "details": "..." }`.
/// Returns the `msg` field (or a fallback).
pub fn parse_cni_error(stdout: &str) -> String {
    if let Ok(v) = serde_json::from_str::<Value>(stdout) {
        if let Some(msg) = v.get("msg").and_then(|m| m.as_str()) {
            return msg.to_string();
        }
        if let Some(msg) = v.get("details").and_then(|m| m.as_str()) {
            return msg.to_string();
        }
    }
    stdout.to_string()
}

/// Extract the first IPv4 address from a CNI result's `ips[]` array,
/// stripping the prefix length (e.g. `"10.88.0.5/16"` → `"10.88.0.5"`).
pub fn extract_first_ipv4(result: &Value) -> Option<String> {
    let ips = result.get("ips")?.as_array()?;
    for entry in ips {
        let addr = entry.get("address").and_then(|a| a.as_str())?;
        // Only consider IPv4 (no colon).
        if !addr.contains(':') {
            let ip = addr.split('/').next().unwrap_or(addr);
            return Some(ip.to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Private execution helpers
// ---------------------------------------------------------------------------

fn load_conflist(path: &Path) -> Result<Value, CniError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| CniError(format!("read conflist {}: {e}", path.display())))?;
    serde_json::from_str(&text)
        .map_err(|e| CniError(format!("parse conflist {}: {e}", path.display())))
}

fn conflist_version(conflist: &Value) -> String {
    conflist
        .get("cniVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("1.0.0")
        .to_string()
}

fn conflist_name(conflist: &Value) -> String {
    conflist
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn conflist_plugins(conflist: &Value) -> Result<Vec<Value>, CniError> {
    conflist
        .get("plugins")
        .and_then(|p| p.as_array())
        .map(|arr| arr.to_vec())
        .ok_or_else(|| CniError("conflist missing 'plugins' array".to_string()))
}

fn plugin_type(plugin: &Value) -> Result<&str, CniError> {
    plugin
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| CniError("plugin object missing 'type' field".to_string()))
}

fn bin_dir_from_env() -> String {
    std::env::var("CNI_PATH").unwrap_or_else(|_| "/opt/cni/bin".to_string())
}

/// True when `LIGHTR_CNI_TRACE` is set to a non-empty, non-"0" value.
///
/// When on, [`exec_plugin`] logs the EXACT stdin JSON sent to each plugin plus
/// the plugin's stdout and stderr, so a portmap no-op (e.g. empty
/// `runtimeConfig.portMappings` or a `prevResult` with no usable container IP →
/// portmap silently returns success and creates no DNAT rule, per portmap
/// v1.9.1 `cmdAdd`) is conclusively visible in CI logs. The CI sets the env var;
/// nothing in this crate sets it.
fn cni_trace_on() -> bool {
    match std::env::var("LIGHTR_CNI_TRACE") {
        Ok(v) => !v.is_empty() && v != "0",
        Err(_) => false,
    }
}

fn exec_plugin(
    bin_dir: &str,
    plugin_type: &str,
    command: &str,
    container_id: &str,
    netns: &str,
    stdin_json: &str,
) -> Result<String, CniError> {
    let binary = Path::new(bin_dir).join(plugin_type);

    let trace = cni_trace_on();
    if trace {
        // Log the EXACT stdin JSON before exec. For portmap this shows whether
        // runtimeConfig.portMappings is present (keys/casing/types) and whether
        // prevResult carries interfaces[]+ips[] with the sandbox container IP.
        eprintln!(
            "[lightr-cri-net][TRACE] exec {} CNI_COMMAND={} CNI_IFNAME=eth0 CNI_NETNS={} CNI_CONTAINERID={}\n[lightr-cri-net][TRACE] {} stdin: {}",
            binary.display(),
            command,
            netns,
            container_id,
            plugin_type,
            stdin_json,
        );
    }

    let mut child = Command::new(&binary)
        .env("CNI_COMMAND", command)
        .env("CNI_CONTAINERID", container_id)
        .env("CNI_NETNS", netns)
        .env("CNI_IFNAME", "eth0")
        .env("CNI_PATH", bin_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| CniError(format!("spawn {}: {e}", binary.display())))?;

    // Write stdin, then DROP the handle so the plugin sees EOF and proceeds.
    // (`wait_with_output` also takes stdin, but dropping here is explicit and
    // avoids any chance of the plugin blocking on a still-open stdin pipe.)
    {
        use std::io::Write;
        if let Some(mut s) = child.stdin.take() {
            s.write_all(stdin_json.as_bytes())
                .map_err(|e| CniError(format!("write stdin to {plugin_type}: {e}")))?;
        }
    }

    let output = child
        .wait_with_output()
        .map_err(|e| CniError(format!("wait for {plugin_type}: {e}")))?;

    if trace {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!(
            "[lightr-cri-net][TRACE] {} exit={:?}\n[lightr-cri-net][TRACE] {} stdout: {}\n[lightr-cri-net][TRACE] {} stderr: {}",
            plugin_type,
            output.status.code(),
            plugin_type,
            stdout.trim_end(),
            plugin_type,
            stderr.trim_end(),
        );
    }

    if output.status.success() {
        String::from_utf8(output.stdout)
            .map_err(|e| CniError(format!("non-utf8 stdout from {plugin_type}: {e}")))
    } else {
        // Parse error JSON from stdout (CNI spec).
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let msg = if !stdout.is_empty() {
            parse_cni_error(&stdout)
        } else {
            stderr.to_string()
        };
        Err(CniError(format!("{plugin_type}: {msg}")))
    }
}

// ---------------------------------------------------------------------------
// Unit tests — host-testable, no privilege, no filesystem writes
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── derive_plugin_config ────────────────────────────────────────────────

    #[test]
    fn derive_injects_cni_version_and_name() {
        let plugin = json!({"type": "bridge", "bridge": "cni0"});
        let out = derive_plugin_config(&plugin, "1.0.0", "lightr", None, &[]);
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["cniVersion"], "1.0.0");
        assert_eq!(v["name"], "lightr");
        assert_eq!(v["bridge"], "cni0");
    }

    #[test]
    fn derive_threads_prev_result() {
        let plugin = json!({"type": "host-local"});
        let prev = json!({"ips": [{"address": "10.88.0.5/16"}]});
        let out = derive_plugin_config(&plugin, "1.0.0", "net", Some(&prev), &[]);
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["prevResult"]["ips"][0]["address"], "10.88.0.5/16");
    }

    #[test]
    fn derive_no_prev_result_omitted() {
        let plugin = json!({"type": "bridge"});
        let out = derive_plugin_config(&plugin, "1.0.0", "net", None, &[]);
        let v: Value = serde_json::from_str(&out).unwrap();
        assert!(v.get("prevResult").is_none());
    }

    #[test]
    fn derive_portmap_runtime_config() {
        use lightr_cri_backend::{PortMapping, Protocol};
        let plugin = json!({"type": "portmap", "capabilities": {"portMappings": true}});
        let pm = PortMapping {
            protocol: Protocol::Tcp,
            container_port: 80,
            host_port: 12000,
            host_ip: "".to_string(),
        };
        let out = derive_plugin_config(&plugin, "1.0.0", "net", None, &[pm]);
        let v: Value = serde_json::from_str(&out).unwrap();
        let pmaps = &v["runtimeConfig"]["portMappings"];
        assert!(pmaps.is_array());
        let first = &pmaps[0];
        assert_eq!(first["hostPort"], 12000);
        assert_eq!(first["containerPort"], 80);
        assert_eq!(first["protocol"], "tcp");
    }

    /// host_port == 0 means "no host mapping" per CRI spec — must be omitted
    /// from the portmap runtimeConfig to avoid "Invalid host port number: 0".
    #[test]
    fn derive_portmap_omits_host_port_zero() {
        use lightr_cri_backend::{PortMapping, Protocol};
        let plugin = json!({"type": "portmap", "capabilities": {"portMappings": true}});
        // Mix: one mapping with host_port=0 (omit), one with host_port=8080 (keep).
        let pm_zero = PortMapping {
            protocol: Protocol::Tcp,
            container_port: 80,
            host_port: 0,
            host_ip: "".to_string(),
        };
        let pm_real = PortMapping {
            protocol: Protocol::Tcp,
            container_port: 443,
            host_port: 8443,
            host_ip: "".to_string(),
        };
        let out = derive_plugin_config(&plugin, "1.0.0", "net", None, &[pm_zero, pm_real]);
        let v: Value = serde_json::from_str(&out).unwrap();
        let pmaps = v["runtimeConfig"]["portMappings"].as_array().unwrap();
        // Only the real mapping must appear.
        assert_eq!(pmaps.len(), 1, "host_port=0 must be filtered out");
        assert_eq!(pmaps[0]["hostPort"], 8443);
        assert_eq!(pmaps[0]["containerPort"], 443);
    }

    /// When ALL port mappings have host_port == 0, runtimeConfig must be absent
    /// (no empty portMappings array).
    #[test]
    fn derive_portmap_all_zero_omits_runtime_config() {
        use lightr_cri_backend::{PortMapping, Protocol};
        let plugin = json!({"type": "portmap", "capabilities": {"portMappings": true}});
        let pm_zero = PortMapping {
            protocol: Protocol::Tcp,
            container_port: 80,
            host_port: 0,
            host_ip: "".to_string(),
        };
        let out = derive_plugin_config(&plugin, "1.0.0", "net", None, &[pm_zero]);
        let v: Value = serde_json::from_str(&out).unwrap();
        assert!(
            v.get("runtimeConfig").is_none(),
            "runtimeConfig must be absent when all host_ports are 0"
        );
    }

    #[test]
    fn derive_no_portmap_cap_skips_runtime_config() {
        use lightr_cri_backend::{PortMapping, Protocol};
        // Plugin does not declare portMappings capability.
        let plugin = json!({"type": "bridge"});
        let pm = PortMapping {
            protocol: Protocol::Tcp,
            container_port: 80,
            host_port: 12000,
            host_ip: "".to_string(),
        };
        let out = derive_plugin_config(&plugin, "1.0.0", "net", None, &[pm]);
        let v: Value = serde_json::from_str(&out).unwrap();
        assert!(v.get("runtimeConfig").is_none());
    }

    #[test]
    fn derive_portmap_cap_false_skips_runtime_config() {
        use lightr_cri_backend::{PortMapping, Protocol};
        let plugin = json!({"type": "portmap", "capabilities": {"portMappings": false}});
        let pm = PortMapping {
            protocol: Protocol::Tcp,
            container_port: 80,
            host_port: 8080,
            host_ip: "0.0.0.0".to_string(),
        };
        let out = derive_plugin_config(&plugin, "1.0.0", "net", None, &[pm]);
        let v: Value = serde_json::from_str(&out).unwrap();
        assert!(v.get("runtimeConfig").is_none());
    }

    // ── extract_first_ipv4 ──────────────────────────────────────────────────

    #[test]
    fn extract_ipv4_strips_prefix() {
        let result = json!({"ips": [{"address": "10.88.0.5/16", "version": "4"}]});
        assert_eq!(extract_first_ipv4(&result), Some("10.88.0.5".to_string()));
    }

    #[test]
    fn extract_ipv4_skips_ipv6() {
        let result = json!({"ips": [
            {"address": "fd00::1/64"},
            {"address": "10.1.2.3/24"}
        ]});
        assert_eq!(extract_first_ipv4(&result), Some("10.1.2.3".to_string()));
    }

    #[test]
    fn extract_ipv4_empty_ips() {
        let result = json!({"ips": []});
        assert_eq!(extract_first_ipv4(&result), None);
    }

    #[test]
    fn extract_ipv4_no_ips_key() {
        let result = json!({"cniVersion": "1.0.0"});
        assert_eq!(extract_first_ipv4(&result), None);
    }

    #[test]
    fn extract_ipv4_no_prefix() {
        let result = json!({"ips": [{"address": "192.168.1.100"}]});
        assert_eq!(
            extract_first_ipv4(&result),
            Some("192.168.1.100".to_string())
        );
    }

    // ── parse_result ────────────────────────────────────────────────────────

    #[test]
    fn parse_result_valid_json() {
        let json = r#"{"cniVersion":"1.0.0","ips":[{"address":"10.88.0.2/16"}]}"#;
        let v = parse_result(json).unwrap();
        assert_eq!(v["cniVersion"], "1.0.0");
    }

    #[test]
    fn parse_result_invalid_json() {
        assert!(parse_result("{broken").is_err());
    }

    // ── parse_cni_error ─────────────────────────────────────────────────────

    #[test]
    fn parse_error_extracts_msg() {
        let json =
            r#"{"cniVersion":"1.0.0","code":7,"msg":"container already exists","details":""}"#;
        assert_eq!(parse_cni_error(json), "container already exists");
    }

    #[test]
    fn parse_error_falls_back_to_details() {
        let json = r#"{"code":100,"details":"something went wrong"}"#;
        assert_eq!(parse_cni_error(json), "something went wrong");
    }

    #[test]
    fn parse_error_falls_back_to_raw() {
        let raw = "some non-json output";
        assert_eq!(parse_cni_error(raw), raw);
    }

    // ── conflist helpers ────────────────────────────────────────────────────

    #[test]
    fn conflist_helpers_extract_fields() {
        let conflist = json!({
            "cniVersion": "1.0.0",
            "name": "lightr",
            "plugins": [
                {"type": "bridge"},
                {"type": "portmap", "capabilities": {"portMappings": true}}
            ]
        });
        assert_eq!(conflist_version(&conflist), "1.0.0");
        assert_eq!(conflist_name(&conflist), "lightr");
        let plugins = conflist_plugins(&conflist).unwrap();
        assert_eq!(plugins.len(), 2);
        assert_eq!(plugin_type(&plugins[0]).unwrap(), "bridge");
        assert_eq!(plugin_type(&plugins[1]).unwrap(), "portmap");
    }

    #[test]
    fn conflist_missing_plugins_returns_err() {
        let conflist = json!({"cniVersion": "1.0.0", "name": "test"});
        assert!(conflist_plugins(&conflist).is_err());
    }

    // ── full ADD config derivation with prevResult chaining ─────────────────

    #[test]
    fn derive_chain_bridge_then_portmap() {
        use lightr_cri_backend::{PortMapping, Protocol};

        let bridge_plugin = json!({"type": "bridge", "bridge": "cni0", "isGateway": true, "ipMasq": true, "ipam": {"type": "host-local", "ranges": [[{"subnet": "10.88.0.0/16"}]]}});
        let portmap_plugin = json!({"type": "portmap", "capabilities": {"portMappings": true}});
        let pm = PortMapping {
            protocol: Protocol::Tcp,
            container_port: 80,
            host_port: 12000,
            host_ip: "".to_string(),
        };
        let prev = json!({"ips": [{"address": "10.88.0.5/16"}]});

        // Bridge plugin (first — no prevResult)
        let bridge_cfg = derive_plugin_config(
            &bridge_plugin,
            "1.0.0",
            "lightr",
            None,
            std::slice::from_ref(&pm),
        );
        let bv: Value = serde_json::from_str(&bridge_cfg).unwrap();
        assert_eq!(bv["type"], "bridge");
        assert!(bv.get("prevResult").is_none());
        // bridge does not declare portMappings cap, so no runtimeConfig
        assert!(bv.get("runtimeConfig").is_none());

        // Portmap plugin (second — has prevResult)
        let portmap_cfg =
            derive_plugin_config(&portmap_plugin, "1.0.0", "lightr", Some(&prev), &[pm]);
        let pv: Value = serde_json::from_str(&portmap_cfg).unwrap();
        assert_eq!(pv["type"], "portmap");
        assert_eq!(pv["prevResult"]["ips"][0]["address"], "10.88.0.5/16");
        assert_eq!(pv["runtimeConfig"]["portMappings"][0]["hostPort"], 12000);
    }
}
