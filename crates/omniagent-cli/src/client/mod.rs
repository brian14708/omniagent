//! Resilient client to the `OmniAgent` control plane.
//!
//! [`PhoenixSocket`] owns one reconnecting WebSocket; [`DaemonSupervisor`]
//! multiplexes agent sessions over it. See [`conn`] for the delivery and
//! reconnection model.

mod conn;
mod supervisor;

pub use conn::{ChannelHandle, ControlChannelHandle, PhoenixSocket, SocketHandle};
pub use supervisor::{
    DEFAULT_REVIEW_TIMEOUT_SECS, DaemonSupervisor, SessionSpec, SessionSummary, WorkspacePolicy,
};

use serde_json::{Value, json};

use crate::protocol::ServerCommand;

/// Connection parameters for the control-plane socket.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub server_url: String,
    pub token: String,
}

/// Builds the Phoenix client WebSocket URL, upgrading the scheme and appending
/// the auth token as a query parameter (`http`->`ws`, `https`->`wss`).
pub fn websocket_url(server_url: &str, token: &str) -> String {
    let trimmed = server_url.trim_end_matches('/');
    let (scheme, rest) = trimmed
        .strip_prefix("https://")
        .map(|rest| ("wss", rest))
        .or_else(|| trimmed.strip_prefix("http://").map(|rest| ("ws", rest)))
        .unwrap_or(("ws", trimmed));
    let base = format!("{scheme}://{rest}");
    format!(
        "{base}/client/websocket?token={}",
        urlencoding::encode(token)
    )
}

/// Serializes a Phoenix v2 (map) channel frame.
pub fn phoenix_message(
    reference: Option<&str>,
    join_ref: Option<&str>,
    topic: &str,
    event: &str,
    payload: &Value,
) -> String {
    json!({
        "join_ref": join_ref,
        "ref": reference,
        "topic": topic,
        "event": event,
        "payload": payload
    })
    .to_string()
}

pub fn next_ref() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Decodes a server->client command frame into a [`ServerCommand`].
pub fn decode_command(event: &str, payload: &Value) -> Option<ServerCommand> {
    match event {
        "pty_input" => Some(ServerCommand::PtyInput {
            data: payload.get("data")?.as_str()?.to_string(),
        }),
        "pty_resize" => Some(ServerCommand::PtyResize {
            rows: payload.get("rows")?.as_u64()?.try_into().ok()?,
            cols: payload.get("cols")?.as_u64()?.try_into().ok()?,
        }),
        "review_decision" => Some(ServerCommand::ReviewDecision {
            id: payload.get("id")?.as_str()?.to_string(),
            decision: payload.get("decision").cloned().unwrap_or(Value::Null),
        }),
        "file_request" => Some(ServerCommand::FileRequest {
            path: payload
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "diff_request" => Some(ServerCommand::DiffRequest {
            path: payload
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "dir_request" => Some(ServerCommand::ListDir {
            path: payload
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "shutdown" => Some(ServerCommand::Shutdown),
        "spawn_agent" => Some(ServerCommand::SpawnAgent {
            argv: payload
                .get("argv")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default(),
            cwd: payload
                .get("cwd")
                .and_then(Value::as_str)
                .map(str::to_string),
            name: payload
                .get("name")
                .and_then(Value::as_str)
                .map(str::to_string),
        }),
        _ => None,
    }
}

/// Decodes `chunk` as UTF-8, carrying an incomplete trailing multi-byte
/// sequence in `carry` across calls so a codepoint split at a chunk boundary is
/// preserved instead of being mangled into replacement characters. Genuinely
/// invalid bytes are replaced with U+FFFD, matching `from_utf8_lossy`.
pub fn decode_streaming(carry: &mut Vec<u8>, chunk: &[u8]) -> String {
    carry.extend_from_slice(chunk);
    let mut out = String::new();
    loop {
        match std::str::from_utf8(carry) {
            Ok(text) => {
                out.push_str(text);
                carry.clear();
                break;
            }
            Err(err) => {
                let valid = err.valid_up_to();
                if valid > 0 {
                    out.push_str(
                        std::str::from_utf8(&carry[..valid])
                            .expect("valid_up_to prefix is valid utf-8"),
                    );
                }
                if let Some(len) = err.error_len() {
                    out.push('\u{FFFD}');
                    carry.drain(..valid + len);
                } else {
                    carry.drain(..valid);
                    break;
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::decode_streaming;

    #[test]
    fn passes_through_valid_utf8() {
        let mut carry = Vec::new();
        assert_eq!(decode_streaming(&mut carry, b"hello"), "hello");
        assert!(carry.is_empty());
    }

    #[test]
    fn reassembles_two_byte_codepoint_split_across_chunks() {
        let mut carry = Vec::new();
        assert_eq!(decode_streaming(&mut carry, &[0xC3]), "");
        assert_eq!(decode_streaming(&mut carry, &[0xA9]), "é");
        assert!(carry.is_empty());
    }

    #[test]
    fn reassembles_four_byte_codepoint_split_across_three_chunks() {
        let mut carry = Vec::new();
        assert_eq!(decode_streaming(&mut carry, &[0xF0, 0x9F]), "");
        assert_eq!(decode_streaming(&mut carry, &[0xA6]), "");
        assert_eq!(decode_streaming(&mut carry, &[0x80]), "🦀");
        assert!(carry.is_empty());
    }

    #[test]
    fn replaces_genuinely_invalid_bytes() {
        let mut carry = Vec::new();
        assert_eq!(
            decode_streaming(&mut carry, &[b'a', 0xFF, b'b']),
            "a\u{FFFD}b"
        );
        assert!(carry.is_empty());
    }
}
