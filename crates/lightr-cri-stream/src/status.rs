//! v4 exit/error delivery — `metav1.Status` JSON on channel 3 / the error
//! stream (r1-streaming.md item 5).
//!
//! Exit-zero (Success):
//!   `{"metadata":{},"status":"Success"}`
//! Non-zero exit (Failure):
//!   `{"metadata":{},"status":"Failure","message":"...","reason":"NonZeroExitCode",
//!     "details":{"causes":[{"type":"ExitCode","message":"<bare decimal>"}]}}`
//!
//! These mirror client-go's `remotecommand` v4 protocol; the exit code is a
//! BARE DECIMAL string (e.g. "1", not "0x01"), and `metadata` is an empty
//! object (kubernetes `metav1.Status` always emits `metadata`).

use serde::Serialize;

#[derive(Serialize)]
struct Cause {
    #[serde(rename = "type")]
    typ: String,
    message: String,
}

#[derive(Serialize)]
struct Details {
    causes: Vec<Cause>,
}

#[derive(Serialize)]
struct Status {
    metadata: Metadata,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Details>,
}

/// Empty `metav1.ObjectMeta` — serializes as `{}`.
#[derive(Serialize)]
struct Metadata {}

/// Build the `metav1.Status` JSON bytes for a finished exec/attach session.
///
/// `exit_code == 0` → Success; non-zero → Failure with the
/// `NonZeroExitCode` reason and an `ExitCode` cause carrying the bare
/// decimal exit code.
pub fn exit_status_json(exit_code: i32) -> Vec<u8> {
    let status = if exit_code == 0 {
        Status {
            metadata: Metadata {},
            status: "Success".to_string(),
            message: None,
            reason: None,
            details: None,
        }
    } else {
        Status {
            metadata: Metadata {},
            status: "Failure".to_string(),
            message: Some(format!(
                "command terminated with non-zero exit code: error executing command, exit status {exit_code}"
            )),
            reason: Some("NonZeroExitCode".to_string()),
            details: Some(Details {
                causes: vec![Cause {
                    typ: "ExitCode".to_string(),
                    // bare decimal — client-go parses this with strconv.Atoi
                    message: exit_code.to_string(),
                }],
            }),
        }
    };
    serde_json::to_vec(&status).expect("status serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_exact_bytes() {
        let bytes = exit_status_json(0);
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            r#"{"metadata":{},"status":"Success"}"#
        );
    }

    #[test]
    fn failure_shape_and_bare_decimal() {
        let bytes = exit_status_json(1);
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["status"], "Failure");
        assert_eq!(v["reason"], "NonZeroExitCode");
        assert_eq!(v["metadata"], serde_json::json!({}));
        let cause = &v["details"]["causes"][0];
        assert_eq!(cause["type"], "ExitCode");
        // bare decimal string, not hex
        assert_eq!(cause["message"], "1");
    }

    #[test]
    fn failure_high_exit_code_decimal() {
        let bytes = exit_status_json(255);
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["details"]["causes"][0]["message"], "255");
    }

    #[test]
    fn key_order_is_metadata_first() {
        // metav1.Status field order: metadata, then status — kubelet/client-go
        // tolerate any order, but pin the canonical layout we emit.
        let bytes = exit_status_json(0);
        let s = std::str::from_utf8(&bytes).unwrap();
        assert!(s.starts_with(r#"{"metadata":{},"status":"#));
    }
}
