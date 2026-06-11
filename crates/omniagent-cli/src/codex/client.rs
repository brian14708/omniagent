//! Minimal async JSON-RPC client over a child process's stdio, for driving
//! `codex app-server`.
//!
//! The app-server speaks JSON-RPC-2.0-*shaped* messages but **omits** the
//! `"jsonrpc"` field — one JSON object per line over stdin/stdout. We frame on
//! newlines and disambiguate inbound objects purely by field presence:
//!
//! - `method` **and** `id`  → a server→client *request* (an approval we must answer)
//! - `method` only          → a notification (turn / item / error / token-usage event)
//! - `id` + `result`/`error`→ a reply to one of our requests
//!
//! Outbound requests use monotonic integer ids; a reader task correlates replies
//! back to the awaiting caller and fans notifications / server-requests out over
//! unbounded channels the worker bridges consume.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::{mpsc, oneshot};

/// How long a request waits for its reply before giving up.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A server→client notification — an event within a turn (agent message deltas,
/// item lifecycle, token usage, errors, …). `params` is passed through verbatim.
#[derive(Debug, Clone)]
pub struct Notification {
    pub method: String,
    pub params: Value,
}

/// A server→client request we must answer (a command/patch approval prompt).
///
/// `id` is kept as raw JSON and echoed back verbatim in the response — codex ids
/// may be numbers or strings, so we never coerce them.
#[derive(Debug, Clone)]
pub struct ServerRequest {
    pub id: Value,
    pub method: String,
    pub params: Value,
}

/// How an inbound frame is classified before dispatch.
enum Inbound {
    Response { id: i64, result: Result<Value> },
    Notification(Notification),
    Request(ServerRequest),
    Ignore,
}

/// Classifies one decoded inbound JSON frame by field presence (see module docs).
fn classify(value: &Value) -> Inbound {
    let method = value.get("method").and_then(Value::as_str);
    let id = value.get("id");
    match (method, id) {
        (Some(method), Some(id)) => Inbound::Request(ServerRequest {
            id: id.clone(),
            method: method.to_string(),
            params: value.get("params").cloned().unwrap_or(Value::Null),
        }),
        (Some(method), None) => Inbound::Notification(Notification {
            method: method.to_string(),
            params: value.get("params").cloned().unwrap_or(Value::Null),
        }),
        (None, Some(id)) => {
            let Some(id) = id.as_i64() else {
                return Inbound::Ignore;
            };
            let result = value.get("error").map_or_else(
                || Ok(value.get("result").cloned().unwrap_or(Value::Null)),
                |err| Err(anyhow!("codex rpc error: {err}")),
            );
            Inbound::Response { id, result }
        }
        (None, None) => Inbound::Ignore,
    }
}

/// JSON-RPC client bound to a `codex app-server` child's stdin/stdout.
pub struct CodexClient {
    stdin: AsyncMutex<ChildStdin>,
    next_id: AtomicI64,
    pending: Mutex<HashMap<i64, oneshot::Sender<Result<Value>>>>,
}

impl CodexClient {
    /// Wraps the child's stdio, starts the reader task, and returns the client
    /// alongside the notification and server-request streams the worker bridges.
    #[must_use]
    pub fn spawn(
        stdin: ChildStdin,
        stdout: ChildStdout,
    ) -> (
        std::sync::Arc<Self>,
        mpsc::UnboundedReceiver<Notification>,
        mpsc::UnboundedReceiver<ServerRequest>,
    ) {
        let client = std::sync::Arc::new(Self {
            stdin: AsyncMutex::new(stdin),
            next_id: AtomicI64::new(1),
            pending: Mutex::new(HashMap::new()),
        });
        let (notif_tx, notif_rx) = mpsc::unbounded_channel();
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        tokio::spawn(read_loop(
            std::sync::Arc::clone(&client),
            stdout,
            notif_tx,
            req_tx,
        ));
        (client, notif_rx, req_rx)
    }

    /// Sends a request and awaits its reply (or a timeout / disconnect error).
    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("codex pending lock")
            .insert(id, tx);

        let frame = json!({ "id": id, "method": method, "params": params });
        if let Err(err) = self.write_value(&frame).await {
            self.pending.lock().expect("codex pending lock").remove(&id);
            return Err(err);
        }

        match tokio::time::timeout(REQUEST_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(anyhow!("codex request reply channel closed")),
            Err(_) => {
                self.pending.lock().expect("codex pending lock").remove(&id);
                Err(anyhow!("codex request '{method}' timed out"))
            }
        }
    }

    /// Sends a fire-and-forget notification (no reply expected).
    pub async fn notify(&self, method: &str, params: Value) {
        let frame = json!({ "method": method, "params": params });
        let _ = self.write_value(&frame).await;
    }

    /// Answers a server→client request, echoing its id verbatim.
    pub async fn respond(&self, id: &Value, result: Value) {
        let frame = json!({ "id": id, "result": result });
        let _ = self.write_value(&frame).await;
    }

    async fn write_value(&self, value: &Value) -> Result<()> {
        let mut line = serde_json::to_string(value)?;
        line.push('\n');
        let mut guard = self.stdin.lock().await;
        guard.write_all(line.as_bytes()).await?;
        guard.flush().await?;
        drop(guard);
        Ok(())
    }

    /// Performs the initialize handshake, then sends the `initialized`
    /// notification (a notification, *not* a request — awaiting a reply would
    /// hang). Any request before this completes errors `"Not initialized"`.
    pub async fn initialize(&self) -> Result<Value> {
        let result = self
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "omniagent",
                        "title": "OmniAgent",
                        "version": env!("CARGO_PKG_VERSION"),
                    }
                }),
            )
            .await?;
        self.notify("initialized", json!({})).await;
        Ok(result)
    }

    /// Starts a thread and returns its id.
    pub async fn thread_start(&self, model: Option<&str>, cwd: &Path) -> Result<String> {
        let mut params = serde_json::Map::new();
        params.insert("cwd".to_string(), json!(cwd.display().to_string()));
        if let Some(model) = model {
            params.insert("model".to_string(), json!(model));
        }
        let result = self.request("thread/start", Value::Object(params)).await?;
        result
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .ok_or_else(|| anyhow!("codex thread/start response missing thread.id"))
    }

    /// Drives a turn with a single text input.
    pub async fn turn_start(&self, thread_id: &str, text: &str) -> Result<Value> {
        self.request(
            "turn/start",
            json!({
                "threadId": thread_id,
                "input": [ { "type": "text", "text": text } ],
            }),
        )
        .await
    }

    /// Interrupts an in-progress turn.
    pub async fn turn_interrupt(&self, thread_id: &str, turn_id: &str) -> Result<Value> {
        self.request(
            "turn/interrupt",
            json!({ "threadId": thread_id, "turnId": turn_id }),
        )
        .await
    }
}

/// Reads newline-delimited frames for the child's lifetime, dispatching each to
/// the awaiting request, the notification stream, or the server-request stream.
/// On EOF, fails every in-flight request so callers don't hang.
async fn read_loop(
    client: std::sync::Arc<CodexClient>,
    stdout: ChildStdout,
    notif_tx: mpsc::UnboundedSender<Notification>,
    req_tx: mpsc::UnboundedSender<ServerRequest>,
) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            tracing::debug!(line = %line, "codex: skipping non-JSON frame");
            continue;
        };
        match classify(&value) {
            Inbound::Response { id, result } => {
                let waiter = client
                    .pending
                    .lock()
                    .expect("codex pending lock")
                    .remove(&id);
                if let Some(tx) = waiter {
                    let _ = tx.send(result);
                }
            }
            Inbound::Notification(note) => {
                let _ = notif_tx.send(note);
            }
            Inbound::Request(req) => {
                let _ = req_tx.send(req);
            }
            Inbound::Ignore => {}
        }
    }
    // EOF: the app-server is gone. Fail any in-flight requests; dropping the
    // senders closes the bridge receivers so their loops exit.
    let waiters: Vec<_> = client
        .pending
        .lock()
        .expect("codex pending lock")
        .drain()
        .map(|(_, tx)| tx)
        .collect();
    for tx in waiters {
        let _ = tx.send(Err(anyhow!("codex app-server closed")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_response_with_result() {
        let value = json!({ "id": 7, "result": { "ok": true } });
        match classify(&value) {
            Inbound::Response { id, result } => {
                assert_eq!(id, 7);
                assert_eq!(result.unwrap(), json!({ "ok": true }));
            }
            _ => panic!("expected response"),
        }
    }

    #[test]
    fn classifies_response_with_error() {
        let value = json!({ "id": 7, "error": { "code": -32001, "message": "busy" } });
        match classify(&value) {
            Inbound::Response { id, result } => {
                assert_eq!(id, 7);
                assert!(result.is_err());
            }
            _ => panic!("expected response"),
        }
    }

    #[test]
    fn classifies_notification() {
        let value = json!({ "method": "turn/started", "params": { "turn": { "id": "t1" } } });
        match classify(&value) {
            Inbound::Notification(note) => {
                assert_eq!(note.method, "turn/started");
                assert_eq!(note.params["turn"]["id"], "t1");
            }
            _ => panic!("expected notification"),
        }
    }

    #[test]
    fn classifies_server_request_with_method_and_id() {
        let value = json!({
            "id": "req-1",
            "method": "item/commandExecution/requestApproval",
            "params": { "command": ["ls"] }
        });
        match classify(&value) {
            Inbound::Request(req) => {
                assert_eq!(req.id, json!("req-1"));
                assert_eq!(req.method, "item/commandExecution/requestApproval");
            }
            _ => panic!("expected server request"),
        }
    }

    #[test]
    fn ignores_frames_without_method_or_id() {
        assert!(matches!(classify(&json!({ "foo": 1 })), Inbound::Ignore));
    }
}
