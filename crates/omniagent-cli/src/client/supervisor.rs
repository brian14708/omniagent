//! Daemon supervisor: multiplexes agent sessions over one resilient socket.
//!
//! Each session pairs a [`ChannelHandle`] (its slice of the shared WebSocket)
//! with a local agent [`WorkerHandle`], a recording proxy, and the trace /
//! review stores. The supervisor owns their lifecycles so a single
//! long-running daemon can host many sessions and survive network outages
//! transparently (reconnection lives in [`super::conn`]).

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Notify, broadcast};

use crate::agent::AgentHandle;
use crate::agent_log;
use crate::agents::{self, AgentInfo};
use crate::atif;
use crate::cast::CastRecorder;
use crate::config::{ConfigStore, path_within_roots};
use crate::files;
use crate::protocol::{RegisterSessionRequest, ServerCommand};
use crate::record::TraceStore;
use crate::review::{ReviewDecision, ReviewEvent, ReviewStore};
use crate::session::omniagent_data_dir;
use crate::upload::{self, UploadConfig, UploadedArtifact};
use crate::worker::WorkerHandle;
use crate::{proxy, start_proxy};

/// Default PTY geometry for a spawned agent.
const DEFAULT_PTY_ROWS: u16 = 40;
const DEFAULT_PTY_COLS: u16 = 120;

/// Default seconds to wait for a human review decision before timing out.
pub const DEFAULT_REVIEW_TIMEOUT_SECS: u64 = 30;

use super::{ChannelHandle, ClientConfig, PhoenixSocket, SocketHandle, decode_streaming};

/// Everything needed to launch one supervised agent session.
#[derive(Debug, Clone)]
pub struct SessionSpec {
    /// Agent command and arguments (e.g. `["claude"]`).
    pub argv: Vec<String>,
    /// Working directory for the agent.
    pub cwd: PathBuf,
    /// Optional human-readable name.
    pub name: Option<String>,
    /// Resume this existing server session id instead of creating a new one.
    pub session_id: Option<String>,
    /// Address the recording proxy binds to.
    pub bind: IpAddr,
    /// Proxy port (0 picks a free port).
    pub proxy_port: u16,
    /// Disable the human review gate.
    pub no_review: bool,
    /// Seconds to wait for a review decision (0 = wait forever).
    pub review_timeout_secs: u64,
    /// Optional JSONL trace archive path.
    pub trace_path: Option<PathBuf>,
}

/// A snapshot of a live session for control-plane listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub name: Option<String>,
    pub cwd: String,
    pub argv: Vec<String>,
    pub proxy_url: String,
}

/// A live session tracked by the supervisor.
struct SessionRecord {
    summary: SessionSummary,
    topic: String,
    worker: WorkerHandle,
}

/// Whether the daemon restricts spawns to the allowed-workspaces allowlist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspacePolicy {
    /// Spawn in any cwd (daemon `--full-access`).
    FullAccess,
    /// Spawn only under a configured allowed workspace; empty list denies all.
    Restricted,
}

/// Owns the socket and the set of live sessions.
pub struct DaemonSupervisor {
    socket: PhoenixSocket,
    sessions: Arc<Mutex<HashMap<String, SessionRecord>>>,
    idle: Arc<Notify>,
    /// Where (and how) to upload session artifacts at close. Derived from the
    /// same connection credentials the socket uses.
    upload: UploadConfig,
    /// Whether spawns are confined to the allowed-workspaces allowlist.
    policy: WorkspacePolicy,
}

impl DaemonSupervisor {
    /// Start the supervisor; the socket connects in the background.
    #[must_use]
    pub fn start(config: ClientConfig, policy: WorkspacePolicy) -> Self {
        let upload = UploadConfig {
            server_url: config.server_url.clone(),
            token: config.token.clone(),
        };
        Self {
            socket: PhoenixSocket::start(config),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            idle: Arc::new(Notify::new()),
            upload,
            policy,
        }
    }

    /// Open the daemon control channel so the server can request new sessions.
    pub async fn open_control_channel(
        &self,
        metadata: serde_json::Value,
    ) -> Result<crate::client::ControlChannelHandle> {
        self.socket.handle().open_control_channel(metadata).await
    }

    /// Enforces the allowed-workspaces policy for a spawn `cwd`.
    ///
    /// Under [`WorkspacePolicy::Restricted`], the canonicalized cwd must fall
    /// within a configured allowed workspace (an empty allowlist denies all).
    fn authorize_workspace(&self, cwd: &Path) -> Result<()> {
        if self.policy == WorkspacePolicy::FullAccess {
            return Ok(());
        }
        let canonical = std::fs::canonicalize(cwd)
            .with_context(|| format!("cannot resolve cwd {}", cwd.display()))?;
        let roots = ConfigStore::default().workspace_roots()?;
        if path_within_roots(&canonical, &roots) {
            Ok(())
        } else {
            Err(anyhow!(
                "cwd {0} is not an allowed workspace — add it with `omniagent workspaces add {0}` or run the daemon with --full-access",
                canonical.display()
            ))
        }
    }

    /// Launch a new session: register with the server, start its proxy, spawn the
    /// agent, and wire up the bidirectional bridges. Returns once the server has
    /// confirmed the registration.
    pub async fn spawn_session(&self, spec: SessionSpec) -> Result<SessionSummary> {
        self.authorize_workspace(&spec.cwd)?;

        let handle = self.socket.handle();
        let cwd_string = spec.cwd.display().to_string();

        let opened = handle
            .open_session(RegisterSessionRequest {
                session_id: spec.session_id.clone(),
                name: spec.name.clone(),
                cwd: cwd_string.clone(),
                argv: spec.argv.clone(),
                metadata: serde_json::Map::new(),
            })
            .await?;
        let channel = opened.handle;
        let session_id = opened.registered.id.clone();

        // Recording stores forward to the channel (and so into its outbox).
        let traces = build_trace_store(spec.trace_path.as_deref(), channel.clone())?;
        let reviews = build_review_store(spec.no_review, spec.review_timeout_secs, &channel);

        let proxy_addr =
            start_proxy(spec.bind, spec.proxy_port, traces.clone(), reviews.clone()).await?;
        let proxy_url = crate::proxy_base_url(spec.bind, proxy_addr);
        let env = proxy::agent_env(&proxy_url);

        // Identify the agent (for ATIF support) and, for claude-code, inject a
        // known `--session-id` so we can locate its transcript deterministically
        // at close. The server still registers the user's original argv.
        //
        // Detection runs on the original argv (it matches the bare program name
        // `claude`); `resolve_argv` then expands that name into its configured
        // launch command (e.g. `pnpm dlx @anthropic-ai/claude-code`) before the
        // session id is appended.
        let agent_info = agents::detect_agent(&spec.argv);
        let agent_commands = ConfigStore::default().agent_commands().unwrap_or_default();
        let resolved_argv = agents::resolve_argv(&spec.argv, &agent_commands);
        let (spawn_argv, native_session_id) = agents::prepare_argv(agent_info, &resolved_argv);
        let started_at = SystemTime::now();

        let agent = AgentHandle::spawn(
            &spawn_argv,
            &env,
            Some(spec.cwd.as_path()),
            DEFAULT_PTY_ROWS,
            DEFAULT_PTY_COLS,
        )?;
        let worker = WorkerHandle::new(agent);

        // Record the terminal to a `.cast` file under the data dir — always,
        // since every session has a PTY. Best-effort; never blocks startup.
        let recording_path = omniagent_data_dir()
            .join("recordings")
            .join(format!("{session_id}.cast"));
        let recorder = match CastRecorder::spawn(
            recording_path,
            &worker,
            DEFAULT_PTY_ROWS,
            DEFAULT_PTY_COLS,
        ) {
            Ok(recorder) => Some(recorder),
            Err(err) => {
                tracing::warn!(error = %err, "failed to start terminal recording");
                None
            }
        };

        spawn_review_decision_bridge(&channel, reviews.clone());
        spawn_worker_bridge(&channel, worker.clone(), spec.cwd.clone());

        let summary = SessionSummary {
            session_id: session_id.clone(),
            name: spec.name.clone().or_else(|| opened.registered.name.clone()),
            cwd: cwd_string,
            argv: spec.argv.clone(),
            proxy_url,
        };

        self.sessions.lock().expect("sessions lock").insert(
            session_id.clone(),
            SessionRecord {
                summary: summary.clone(),
                topic: channel.topic(),
                worker: worker.clone(),
            },
        );

        // Reap the session when its agent exits: finalize and upload its
        // artifacts (recording + ATIF trajectory), announce `session_close`,
        // then remove it.
        let sessions = Arc::clone(&self.sessions);
        let socket = self.socket.handle();
        let idle = Arc::clone(&self.idle);
        let reap_id = session_id.clone();
        let finalize = FinalizeCtx {
            channel: channel.clone(),
            traces: Arc::clone(&traces),
            recorder,
            agent: agent_info,
            upload: self.upload.clone(),
            session_id: session_id.clone(),
            cwd: spec.cwd.clone(),
            native_session_id,
            started_at,
        };
        tokio::spawn(async move {
            let exit_code = worker.wait_exit().await;
            finalize_session(finalize, exit_code).await;
            remove_session(&sessions, &socket, &idle, &reap_id);
        });

        Ok(summary)
    }

    /// List currently-live sessions.
    #[must_use]
    pub fn list_sessions(&self) -> Vec<SessionSummary> {
        self.sessions
            .lock()
            .expect("sessions lock")
            .values()
            .map(|record| record.summary.clone())
            .collect()
    }

    /// Stop a session's agent; the reaper removes it once the worker exits.
    pub async fn stop_session(&self, session_id: &str) -> bool {
        let worker = self
            .sessions
            .lock()
            .expect("sessions lock")
            .get(session_id)
            .map(|record| record.worker.clone());
        match worker {
            Some(worker) => {
                worker.shutdown().await;
                true
            }
            None => false,
        }
    }

    /// Shut down every live session.
    pub async fn shutdown_all(&self) {
        let workers: Vec<WorkerHandle> = self
            .sessions
            .lock()
            .expect("sessions lock")
            .values()
            .map(|record| record.worker.clone())
            .collect();
        for worker in workers {
            worker.shutdown().await;
        }
    }
}

fn remove_session(
    sessions: &Arc<Mutex<HashMap<String, SessionRecord>>>,
    socket: &SocketHandle,
    idle: &Arc<Notify>,
    session_id: &str,
) {
    let now_empty = {
        let mut guard = sessions.lock().expect("sessions lock");
        if let Some(record) = guard.remove(session_id) {
            socket.close_session(&record.topic);
        }
        guard.is_empty()
    };
    if now_empty {
        idle.notify_waiters();
    }
}

/// Inputs the reaper needs to finalize and upload a session's artifacts.
struct FinalizeCtx {
    channel: ChannelHandle,
    traces: Arc<TraceStore>,
    recorder: Option<CastRecorder>,
    agent: Option<AgentInfo>,
    upload: UploadConfig,
    session_id: String,
    /// Agent working directory — used to locate claude's native transcript.
    cwd: PathBuf,
    /// Session id we injected into a claude invocation, if any (deterministic
    /// transcript discovery).
    native_session_id: Option<String>,
    /// When the agent was spawned — bounds native-log discovery by recency.
    started_at: SystemTime,
}

/// Finalizes recordings, uploads artifacts, and emits the `session_close`
/// event. Best-effort throughout: failing to record, build, or upload any one
/// artifact is logged and never blocks teardown.
async fn finalize_session(ctx: FinalizeCtx, exit_code: i32) {
    // Collect each artifact's bytes (cheap, sequential), then upload them
    // concurrently below so a slow upload doesn't serialize the others.
    let mut pending: Vec<(&'static str, Vec<u8>)> = Vec::new();

    // 1. Terminal recording — always captured when a PTY exists.
    if let Some(recorder) = ctx.recorder {
        let path = recorder.finalize().await;
        match tokio::fs::read(&path).await {
            Ok(bytes) if !bytes.is_empty() => pending.push(("recording", bytes)),
            Ok(_) => {}
            Err(err) => tracing::warn!(error = %err, "failed to read terminal recording"),
        }
    }

    let spans = ctx.traces.snapshot();

    // Locate the agent's native session log once: it feeds both the ATIF
    // trajectory (parsed below) and the raw `session_log` artifact (uploaded
    // verbatim), so we don't scan the log directory twice.
    let native_log = ctx
        .agent
        .filter(|info| info.supports_atif)
        .and_then(|agent| {
            agent_log::locate_native_log(
                agent,
                &ctx.cwd,
                ctx.native_session_id.as_deref(),
                ctx.started_at,
            )
            .map(|path| (agent, path))
        });

    // 2. ATIF trajectory — only for agents that support it. Prefer the agent's
    // native session log; fall back to reconstructing from the recorded spans.
    if let Some(agent) = ctx.agent.filter(|info| info.supports_atif) {
        let trajectory = native_log
            .as_ref()
            .and_then(|(agent, path)| agent_log::parse_native_log(*agent, path))
            .or_else(|| atif::build_trajectory(agent, &ctx.session_id, &spans));
        if let Some(mut trajectory) = trajectory {
            // Link each agent step to its raw request span (by provider response
            // id) so the trajectory joins to the `raw_requests` artifact.
            atif::link_raw_requests(&mut trajectory, &spans);
            match serde_json::to_vec_pretty(&trajectory) {
                Ok(bytes) => pending.push(("trajectory", bytes)),
                Err(err) => tracing::warn!(error = %err, "failed to serialize trajectory"),
            }
        }
    }

    // 3. Raw native session log — uploaded verbatim. Lossless and higher fidelity
    // than the parsed trajectory; for claude it also preserves subagent (`Task`)
    // sidechains that the flat ATIF model flattens away.
    if let Some((_, path)) = &native_log {
        match tokio::fs::read(path).await {
            Ok(bytes) if !bytes.is_empty() => pending.push(("session_log", bytes)),
            Ok(_) => {}
            Err(err) => tracing::warn!(error = %err, "failed to read native session log"),
        }
    }

    // 4. Raw requests — the exact intercepted spans as JSONL, for any session
    // that made LLM calls (independent of agent support).
    if let Some(bytes) = spans_to_jsonl(&spans) {
        pending.push(("raw_requests", bytes));
    }

    // Upload all artifacts concurrently over a single shared HTTP client.
    let client = reqwest::Client::new();
    let uploads = pending.into_iter().map(|(kind, bytes)| {
        let client = &client;
        let upload = &ctx.upload;
        let session_id = &ctx.session_id;
        async move {
            (
                kind,
                upload::upload_artifact(client, upload, session_id, kind, bytes).await,
            )
        }
    });

    let mut artifacts: Vec<serde_json::Value> = Vec::new();
    for (kind, result) in futures_util::future::join_all(uploads).await {
        match result {
            Ok(uploaded) => artifacts.push(artifact_summary(kind, &uploaded)),
            // `{err:#}` renders anyhow's full cause chain on one line (e.g. the
            // underlying reqwest connection-reset/timeout), not just the top
            // context — otherwise the real reason for a failed upload is lost.
            Err(err) => tracing::warn!(
                error = format!("{err:#}"),
                kind,
                "failed to upload artifact"
            ),
        }
    }

    // 5. Announce session close, delivered before the channel is left.
    let payload = json!({
        "exit_code": exit_code,
        "agent": {
            "name": ctx.agent.map(|info| info.name),
            "supported": ctx.agent.is_some(),
        },
        "artifacts": artifacts,
    });
    if let Err(err) = ctx.channel.send_now("session_close", payload).await {
        tracing::debug!(error = %err, "session_close not delivered (socket down)");
    }
}

/// Serializes recorded spans to JSONL (one span per line), or `None` if there
/// are no spans or none serialize.
fn spans_to_jsonl(spans: &[crate::record::LlmSpan]) -> Option<Vec<u8>> {
    if spans.is_empty() {
        return None;
    }
    let mut out = String::new();
    for span in spans {
        if let Ok(line) = serde_json::to_string(span) {
            out.push_str(&line);
            out.push('\n');
        }
    }
    (!out.is_empty()).then(|| out.into_bytes())
}

/// Builds the per-artifact summary embedded in the `session_close` event.
fn artifact_summary(kind: &str, uploaded: &UploadedArtifact) -> serde_json::Value {
    json!({
        "kind": kind,
        "id": uploaded.id,
        "key": uploaded.key,
        "size": uploaded.size,
    })
}

// ---------------------------------------------------------------------------
// Per-session store wiring (forwards events into the channel outbox)
// ---------------------------------------------------------------------------

fn build_trace_store(
    trace_path: Option<&std::path::Path>,
    channel: ChannelHandle,
) -> Result<Arc<TraceStore>> {
    let store = match trace_path {
        Some(path) => TraceStore::with_sink(path)
            .with_context(|| format!("failed to open trace file {}", path.display()))?,
        None => TraceStore::new(),
    }
    .with_forwarder(Arc::new(move |span| {
        if let Ok(payload) = serde_json::to_value(span) {
            channel.push("trace_span", payload);
        }
    }));
    Ok(Arc::new(store))
}

fn build_review_store(
    no_review: bool,
    review_timeout_secs: u64,
    channel: &ChannelHandle,
) -> Arc<ReviewStore> {
    let review_timeout =
        (review_timeout_secs > 0).then(|| Duration::from_secs(review_timeout_secs));
    Arc::new(
        ReviewStore::new(!no_review, review_timeout).with_forwarder(Arc::new({
            let channel = channel.clone();
            move |event| {
                if let ReviewEvent::Upsert { item } = event
                    && let Ok(payload) = serde_json::to_value(item.as_ref())
                {
                    channel.push("review_item", payload);
                }
            }
        })),
    )
}

// ---------------------------------------------------------------------------
// Bidirectional bridges between the channel and the worker
// ---------------------------------------------------------------------------

fn spawn_worker_bridge(channel: &ChannelHandle, worker: WorkerHandle, workspace: PathBuf) {
    spawn_terminal_output_bridge(channel, worker.clone());
    spawn_command_bridge(channel, worker, workspace);
}

/// Forwards PTY output (and the final exit) from the worker into the channel.
///
/// A single task owns the output stream so `pty_exit` is always sequenced
/// *after* the trailing `pty_output`: when the worker exits we drain whatever
/// output is still buffered in the broadcast before announcing the exit, so the
/// "agent exited" banner never races ahead of the agent's final lines. (The
/// output broadcast does not reliably close on exit — late attachers may keep a
/// sender alive — so we can't rely on a `Closed` to mark the end of output.)
fn spawn_terminal_output_bridge(channel: &ChannelHandle, worker: WorkerHandle) {
    let output_channel = channel.clone();
    tokio::spawn(async move {
        let mut terminal = worker.attach();
        // Carry an incomplete trailing UTF-8 sequence across chunks so a
        // multi-byte codepoint split at a chunk boundary isn't corrupted.
        let mut carry: Vec<u8> = Vec::new();
        let push_output = |carry: &mut Vec<u8>, bytes: &[u8]| {
            let data = decode_streaming(carry, bytes);
            if !data.is_empty() {
                output_channel.push("pty_output", json!({ "data": data }));
            }
        };
        if !terminal.backlog.is_empty() {
            let backlog = std::mem::take(&mut terminal.backlog);
            push_output(&mut carry, &backlog);
        }

        let exit = worker.wait_exit();
        tokio::pin!(exit);
        let exit_code = loop {
            tokio::select! {
                // Prefer draining output over noticing the exit so buffered
                // chunks are always pushed (with lower sequences) first.
                biased;
                chunk = terminal.output.recv() => match chunk {
                    Ok(chunk) => push_output(&mut carry, &chunk),
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break worker.wait_exit().await,
                },
                code = &mut exit => break code,
            }
        };

        // Process exited: flush any output still buffered in the broadcast
        // before announcing the exit.
        loop {
            match terminal.output.try_recv() {
                Ok(chunk) => push_output(&mut carry, &chunk),
                Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
        output_channel.push("pty_exit", json!({ "exit_code": exit_code }));
    });
}

/// Applies server->client commands (input, resize, file/diff, shutdown).
fn spawn_command_bridge(channel: &ChannelHandle, worker: WorkerHandle, workspace: PathBuf) {
    let mut commands = channel.subscribe_commands();
    let channel = channel.clone();
    tokio::spawn(async move {
        loop {
            // A broadcast Lagged means we fell behind under a command burst; skip
            // the gap and keep serving rather than tearing the bridge down (which
            // would silently stop delivering input/resize/shutdown for the rest of
            // the session). Only a Closed channel ends the loop.
            let command = match commands.recv().await {
                Ok(command) => command,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            };
            match command {
                ServerCommand::PtyInput { data } => worker.send_input(Bytes::from(data)),
                ServerCommand::PtyResize { rows, cols } => worker.resize(rows, cols),
                ServerCommand::ReviewDecision { .. } | ServerCommand::SpawnAgent { .. } => {
                    // Handled elsewhere: review-decision bridge / daemon control channel.
                }
                ServerCommand::FileRequest { path } => {
                    let workspace = workspace.clone();
                    let payload = tokio::task::spawn_blocking(move || {
                        match files::read_file(&workspace, &path) {
                            Ok(text) => json!({"path": path, "ok": true, "text": text}),
                            Err(err) => {
                                json!({"path": path, "ok": false, "error": err.to_string()})
                            }
                        }
                    })
                    .await
                    .unwrap_or_else(|_| json!({"ok": false, "error": "file task panicked"}));
                    channel.push("file_response", payload);
                }
                ServerCommand::DiffRequest { path } => {
                    let workspace = workspace.clone();
                    let payload = tokio::task::spawn_blocking(move || {
                        let filter = (!path.trim().is_empty()).then_some(path.as_str());
                        let diff = files::git_diff(&workspace, filter);
                        json!({"path": path, "ok": true, "diff": diff})
                    })
                    .await
                    .unwrap_or_else(|_| json!({"ok": false, "error": "diff task panicked"}));
                    channel.push("diff_response", payload);
                }
                ServerCommand::ListDir { path } => {
                    let workspace = workspace.clone();
                    let payload = tokio::task::spawn_blocking(move || {
                        match files::list_dir(&workspace, &path) {
                            Ok(entries) => json!({"path": path, "ok": true, "entries": entries}),
                            Err(err) => {
                                json!({"path": path, "ok": false, "error": err.to_string()})
                            }
                        }
                    })
                    .await
                    .unwrap_or_else(|_| json!({"ok": false, "error": "dir task panicked"}));
                    channel.push("dir_response", payload);
                }
                ServerCommand::Shutdown => {
                    worker.shutdown().await;
                    break;
                }
            }
        }
    });
}

/// Routes review decisions from the server into the review store.
fn spawn_review_decision_bridge(channel: &ChannelHandle, reviews: Arc<ReviewStore>) {
    let mut commands = channel.subscribe_commands();
    tokio::spawn(async move {
        loop {
            // Ignore Lagged (skip the gap, keep serving) so a command burst can't
            // silently kill the bridge and strand every later review at the
            // server-side timeout. Only Closed ends the loop.
            let command = match commands.recv().await {
                Ok(command) => command,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            };
            if let ServerCommand::ReviewDecision { id, decision } = command {
                let _ = reviews.decide(&id, ReviewDecision::from_server_value(&decision));
            }
        }
    });
}
