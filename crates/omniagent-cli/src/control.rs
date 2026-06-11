//! Local control plane for the omniagent daemon.
//!
//! The daemon listens on a Unix-domain socket; the `omniagent` CLI connects to
//! it to spawn, list, and stop sessions. The wire format is one JSON value per
//! line (newline-delimited), request then response.
//!
//! A `server_config` request is reserved as a passthrough so the CLI can later
//! relay server-configuration calls through the daemon's authenticated
//! control-plane connection; the concrete config operations are not implemented
//! yet.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::client::SessionSummary;
use crate::session::omniagent_data_dir;

/// A request from the CLI to the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    /// Health check.
    Ping,
    /// Launch a new session.
    Spawn(SpawnRequest),
    /// List live sessions.
    List,
    /// Stop a session by server session id.
    Stop { session_id: String },
    /// Reserved: relay a server-configuration call through the daemon.
    ServerConfig { payload: serde_json::Value },
}

/// Parameters for spawning a session over the control socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnRequest {
    /// Selected agent: a canonical name (`claude`/`codex`/`gemini`). Drives the
    /// launch command, the backend, and proxy config injection.
    pub agent: String,
    /// Extra args appended to the agent's resolved launch command.
    #[serde(default)]
    pub custom_command: Vec<String>,
    pub cwd: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub no_review: bool,
    #[serde(default)]
    pub review_timeout_secs: Option<u64>,
    #[serde(default)]
    pub proxy_port: u16,
    /// Drive codex via `codex app-server` (native, structured) instead of a PTY.
    #[serde(default)]
    pub app_server: bool,
    /// Optional model override passed to `codex app-server` `thread/start`.
    #[serde(default)]
    pub model: Option<String>,
}

/// A response from the daemon to the CLI.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlResponse {
    Pong,
    Spawned(SessionSummary),
    Sessions(Vec<SessionSummary>),
    Stopped { found: bool },
    Error { message: String },
}

/// Path to the daemon control socket: `$XDG_RUNTIME_DIR/omniagent/daemon.sock`
/// when `XDG_RUNTIME_DIR` is set (the canonical location for runtime sockets),
/// otherwise `<data dir>/daemon.sock`.
#[must_use]
pub fn socket_path() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .map_or_else(omniagent_data_dir, |base| base.join("omniagent"))
        .join("daemon.sock")
}

/// Send a single request to the daemon and read its response.
pub async fn send_request(request: &ControlRequest) -> Result<ControlResponse> {
    let path = socket_path();
    let stream = UnixStream::connect(&path).await.with_context(|| {
        format!(
            "failed to reach daemon control socket at {} (is `omniagent daemon` running?)",
            path.display()
        )
    })?;
    let (read_half, mut write_half) = stream.into_split();

    let mut line = serde_json::to_string(request)?;
    line.push('\n');
    write_half.write_all(line.as_bytes()).await?;
    write_half.flush().await?;

    let mut reader = BufReader::new(read_half);
    let mut response_line = String::new();
    reader
        .read_line(&mut response_line)
        .await
        .context("failed to read daemon response")?;
    if response_line.trim().is_empty() {
        return Err(anyhow!("daemon closed the connection without responding"));
    }
    serde_json::from_str(response_line.trim()).context("invalid daemon response")
}

/// Bind the control socket, replacing any stale socket file.
pub fn bind_listener() -> Result<(UnixListener, PathBuf)> {
    let path = socket_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    // A leftover socket file from a previous run blocks bind; clear it.
    if path.exists() {
        let _ = std::fs::remove_file(&path);
    }
    let listener = UnixListener::bind(&path)
        .with_context(|| format!("failed to bind control socket at {}", path.display()))?;
    Ok((listener, path))
}

/// Serve one control connection: read a request, run `handler`, write the reply.
pub async fn serve_connection<F, Fut>(stream: UnixStream, handler: F)
where
    F: FnOnce(ControlRequest) -> Fut,
    Fut: std::future::Future<Output = ControlResponse>,
{
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    if reader.read_line(&mut line).await.is_err() || line.trim().is_empty() {
        return;
    }
    let response = match serde_json::from_str::<ControlRequest>(line.trim()) {
        Ok(request) => handler(request).await,
        Err(err) => ControlResponse::Error {
            message: format!("invalid request: {err}"),
        },
    };
    if let Ok(mut encoded) = serde_json::to_string(&response) {
        encoded.push('\n');
        let _ = write_half.write_all(encoded.as_bytes()).await;
        let _ = write_half.flush().await;
    }
}
