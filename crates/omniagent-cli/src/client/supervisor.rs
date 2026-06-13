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

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::OffsetDateTime;
use tokio::sync::{Notify, broadcast, mpsc};
use tokio::task::JoinHandle;

use crate::agent::AgentHandle;
use crate::agent_log;
use crate::cast::CastRecorder;
use crate::codex::client::{Notification, ServerRequest};
use crate::codex::{self, CodexWorkerHandle};
use crate::config::{ConfigStore, path_within_roots};
use crate::executor::action::{ActionType, ExecutorAction, ScriptRequest};
use crate::executor::{self, AgentInfo, Backend};
use crate::files;
use crate::protocol::{RegisterSessionRequest, ServerCommand};
use crate::record::{Provider, TraceStore};
use crate::review::{ReviewDecision, ReviewEvent, ReviewItem, ReviewPhase, ReviewStore};
use crate::session::omniagent_data_dir;
use crate::upload::{self, UploadConfig, UploadedArtifact};
use crate::worker::WorkerHandle;
use crate::workspace::{self, WorktreeMode};
use crate::{proxy, start_proxy};

/// Default PTY geometry for a spawned agent.
const DEFAULT_PTY_ROWS: u16 = 40;
const DEFAULT_PTY_COLS: u16 = 120;

/// Default seconds to wait for a human review decision before timing out.
pub const DEFAULT_REVIEW_TIMEOUT_SECS: u64 = 30;

/// How long [`DaemonSupervisor::shutdown_all`] waits for in-flight sessions to
/// finalize and upload their artifacts before giving up, so a single stuck
/// upload cannot hang daemon shutdown indefinitely.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

use super::{ChannelHandle, ClientConfig, PhoenixSocket, SocketHandle, decode_streaming};

/// Everything needed to launch one supervised agent session.
#[derive(Debug, Clone)]
pub struct SessionSpec {
    /// Selected agent: a canonical name (`claude`/`codex`/`gemini`). Drives the
    /// launch command, the backend, and proxy config injection.
    pub agent: String,
    /// Extra args appended to the agent's resolved launch command.
    pub custom_command: Vec<String>,
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
    /// Drive codex via `codex app-server` (native, structured) instead of a PTY.
    pub app_server: bool,
    /// Optional model override passed to codex's `thread/start`.
    pub model: Option<String>,
    /// Allowed workspace root the agent runs under (preferred over `cwd`).
    pub workspace: Option<PathBuf>,
    /// Branch to use, or the new branch name when creating a worktree.
    pub branch: Option<String>,
    /// Create (or reuse) an isolated `git worktree` for `branch`.
    pub create_worktree: bool,
    /// Spawn in this existing linked worktree.
    pub worktree: Option<PathBuf>,
    /// Base ref a newly-created worktree branches from.
    pub base_branch: Option<String>,
    /// Setup script to run before the agent (overrides the config default when
    /// `Some`). Used by `serve-session` to carry devcontainer `postStart` steps.
    pub setup_script: Option<String>,
    /// Cleanup script to run after the agent exits (overrides the config default
    /// when `Some`).
    pub cleanup_script: Option<String>,
    /// Initial PTY size as `(rows, cols)` from the browser terminal. Falls back
    /// to the default when absent (daemon-driven sessions correct via resize).
    pub pty_size: Option<(u16, u16)>,
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
    /// The daemon's control channel, once registered with the control plane. Used
    /// to re-advertise the workspace allowlist after the CLI edits it.
    control: Mutex<Option<crate::client::ControlChannelHandle>>,
}

/// Validates a control-panel-supplied workspace name as a single safe path
/// component, rejecting empties, separators, and `.`/`..` so creation can never
/// escape `<data dir>/workspaces/`.
fn sanitized_workspace_name(name: &str) -> Result<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        bail!("workspace name is empty");
    }
    let mut components = Path::new(trimmed).components();
    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(part)), None) => Ok(part.to_string_lossy().into_owned()),
        _ => bail!("workspace name must be a single path component: {trimmed:?}"),
    }
}

/// Picks the worktree resolution mode from a spec: an explicit existing
/// worktree wins, then an isolated-worktree request (defaulting the branch name
/// when unset), otherwise spawn in place.
fn select_mode(spec: &SessionSpec) -> WorktreeMode {
    if let Some(path) = &spec.worktree {
        return WorktreeMode::Existing(path.clone());
    }
    if spec.create_worktree {
        let branch = spec
            .branch
            .clone()
            .filter(|b| !b.trim().is_empty())
            .unwrap_or_else(|| {
                format!(
                    "omniagent/{}",
                    &uuid::Uuid::new_v4().simple().to_string()[..8]
                )
            });
        return WorktreeMode::Create {
            branch,
            base: spec.base_branch.clone().filter(|b| !b.trim().is_empty()),
        };
    }
    WorktreeMode::InPlace
}

/// Builds the session metadata map registered with the control plane: backend
/// kind, model, and the resolved workspace/branch/worktree for the console.
fn session_metadata(
    spec: &SessionSpec,
    resolved: &workspace::ResolvedWorkspace,
    kind: Option<&str>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut metadata = serde_json::Map::new();
    if let Some(kind) = kind {
        metadata.insert("kind".to_string(), json!(kind));
    }
    if let Some(model) = &spec.model {
        metadata.insert("model".to_string(), json!(model));
    }
    metadata.insert(
        "workspace".to_string(),
        json!(resolved.origin_root.display().to_string()),
    );
    if let Some(branch) = &spec.branch {
        metadata.insert("branch".to_string(), json!(branch));
    }
    if let Some(worktree) = &resolved.created_worktree {
        metadata.insert(
            "worktree".to_string(),
            json!(worktree.display().to_string()),
        );
    }
    metadata
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
            control: Mutex::new(None),
        }
    }

    /// Open the daemon control channel so the server can request new sessions.
    pub async fn open_control_channel(
        &self,
        metadata: serde_json::Value,
    ) -> Result<crate::client::ControlChannelHandle> {
        let handle = self.socket.handle().open_control_channel(metadata).await?;
        *self.control.lock().expect("control lock") = Some(handle.clone());
        Ok(handle)
    }

    /// Re-advertise the daemon's workspace allowlist (and other metadata) to the
    /// control plane after the CLI edited it, so the console's pickers update
    /// without a daemon restart. Errors if the control channel isn't registered
    /// yet (the new allowlist still applies on the next (re)registration).
    pub async fn refresh_workspaces(&self, metadata: serde_json::Value) -> Result<()> {
        let handle = self.control.lock().expect("control lock").clone();
        let handle = handle.ok_or_else(|| {
            anyhow!(
                "daemon is not registered with the control plane yet; allowlist applies on connect"
            )
        })?;
        handle.refresh_metadata(metadata).await
    }

    /// Create a new project workspace under the local data dir
    /// (`<data dir>/workspaces/<name>`), initialize it as a git repo, and add it
    /// to the allowlist. Returns the created (canonical) path. Driven by the
    /// control panel via the `create_workspace` command; the caller re-advertises
    /// the updated allowlist afterward.
    pub fn create_workspace(name: &str) -> Result<PathBuf> {
        let name = sanitized_workspace_name(name)?;
        let root = omniagent_data_dir().join("workspaces").join(&name);
        if root.exists() {
            bail!("workspace {} already exists", root.display());
        }
        std::fs::create_dir_all(&root)
            .with_context(|| format!("failed to create workspace {}", root.display()))?;
        // Default to a git repo so worktree-based sessions work out of the box.
        crate::git::git(&root, &["init"])
            .ok_or_else(|| anyhow!("git init failed in {}", root.display()))?;
        ConfigStore::default().add_workspace(&root)
    }

    /// Enforces the allowed-workspaces policy by the resolved **origin repo**.
    ///
    /// Under [`WorkspacePolicy::Restricted`], the origin root must fall within a
    /// configured allowed workspace (an empty allowlist denies all). Auto-created
    /// worktrees live outside the allowlist but authorize via their origin repo.
    fn authorize_workspace(&self, origin_root: &Path) -> Result<()> {
        if self.policy == WorkspacePolicy::FullAccess {
            return Ok(());
        }
        let roots = ConfigStore::default().workspace_roots()?;
        if path_within_roots(origin_root, &roots) {
            Ok(())
        } else {
            Err(anyhow!(
                "{0} is not an allowed workspace — add it with `omniagent workspaces add {0}` or run the daemon with --full-access",
                origin_root.display()
            ))
        }
    }

    /// Resolves a spec's workspace selection to an effective cwd: authorizes by
    /// the origin repo first, then performs any side-effecting `git worktree
    /// add`. Mutates `spec.cwd` to the resolved directory.
    fn resolve_workspace(&self, spec: &mut SessionSpec) -> Result<workspace::ResolvedWorkspace> {
        let root = spec.workspace.clone().unwrap_or_else(|| spec.cwd.clone());
        let mode = select_mode(spec);
        let origin = workspace::resolve_origin(&root)
            .with_context(|| format!("cannot resolve workspace {}", root.display()))?;
        self.authorize_workspace(&origin)?;
        let resolved = workspace::resolve(&root, &mode)?;
        spec.cwd.clone_from(&resolved.cwd);
        Ok(resolved)
    }
}

/// Runs a session's leading setup scripts to completion and returns the trailing
/// (cleanup) chain for the reaper. On setup failure, announces the aborted
/// session, closes its channel, and returns the error.
async fn prepare_session_chain(
    socket: &SocketHandle,
    channel: &ChannelHandle,
    cwd: &Path,
    env: &[(String, String)],
    agent_info: Option<AgentInfo>,
    resolved: &workspace::ResolvedWorkspace,
    chain: ExecutorAction,
) -> Result<Option<ExecutorAction>> {
    match run_setup_scripts(channel, cwd, env, &chain).await {
        Ok(cleanup) => Ok(cleanup),
        Err(err) => {
            abort_before_agent(channel, agent_info, resolved, &err).await;
            socket.close_session(&channel.topic());
            Err(err)
        }
    }
}

/// Runs the chain's leading [`ActionType::Script`] steps (setup) to
/// completion, streaming their output to the session's terminal. Returns the
/// remaining chain after the [`ActionType::CodingAgent`] step (the cleanup
/// scripts the reaper will run). A non-zero setup exit is an error: the
/// caller aborts the session before the agent starts.
async fn run_setup_scripts(
    channel: &ChannelHandle,
    cwd: &Path,
    env: &[(String, String)],
    chain: &ExecutorAction,
) -> Result<Option<ExecutorAction>> {
    let mut step = Some(chain);
    while let Some(node) = step {
        match &node.typ {
            ActionType::Script(req) => {
                let code = run_script_action(channel, cwd, env, req).await?;
                if code != 0 {
                    return Err(anyhow!("{} script failed with exit code {code}", req.label));
                }
                step = node.next();
            }
            // The agent step itself is spawned by the caller; everything after
            // it (cleanup) runs in the reaper.
            ActionType::CodingAgent => return Ok(node.next().cloned()),
        }
    }
    Ok(None)
}

/// Resolves a session's setup/cleanup scripts: an explicit pair on the spec
/// (e.g. devcontainer lifecycle from `serve-session`) wins; otherwise fall back
/// to the daemon's configured defaults.
fn resolve_session_scripts(spec: &SessionSpec) -> (Option<String>, Option<String>) {
    if spec.setup_script.is_some() || spec.cleanup_script.is_some() {
        (spec.setup_script.clone(), spec.cleanup_script.clone())
    } else {
        ConfigStore::default()
            .session_scripts()
            .unwrap_or((None, None))
    }
}

impl DaemonSupervisor {
    /// Launch a new session: register with the server, start its proxy, spawn the
    /// agent, and wire up the bidirectional bridges. Returns once the server has
    /// confirmed the registration; the session is reaped in the background when
    /// its agent exits.
    pub async fn spawn_session(&self, mut spec: SessionSpec) -> Result<SessionSummary> {
        let resolved = self.resolve_workspace(&mut spec)?;
        let spawned =
            prepare_and_spawn(&self.socket.handle(), &self.upload, spec, resolved).await?;
        let SpawnedSession {
            summary,
            topic,
            worker,
            fs_watcher,
            finalize,
            ..
        } = spawned;

        self.sessions.lock().expect("sessions lock").insert(
            summary.session_id.clone(),
            SessionRecord {
                summary: summary.clone(),
                topic,
                worker: worker.clone(),
            },
        );

        // Reap the session when its agent exits: finalize and upload its
        // artifacts (recording + ATIF trajectory), announce `session_close`,
        // then remove it.
        let sessions = Arc::clone(&self.sessions);
        let socket = self.socket.handle();
        let idle = Arc::clone(&self.idle);
        let reap_id = summary.session_id.clone();
        tokio::spawn(async move {
            let exit_code = worker.wait_exit().await;
            fs_watcher.abort();
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

    /// Shut down every live session and wait for their reapers to finish
    /// finalizing and uploading artifacts.
    ///
    /// Signaling a worker only makes its agent exit; the per-session reap task
    /// spawned in [`Self::spawn_session`] is what awaits that exit and then
    /// uploads the session's artifacts. If we returned as soon as the workers
    /// were signaled, the daemon's runtime would be dropped mid-upload and
    /// cancel those tasks (surfacing as "task NNN was cancelled" /
    /// "background task failed"), losing the session-log and raw-request
    /// artifacts. So we wait for the session map to drain — each reaper removes
    /// its session and notifies `idle` once finalize completes — bounded by
    /// [`SHUTDOWN_DRAIN_TIMEOUT`] so a single stuck upload can't hang shutdown.
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

        let drained = async {
            loop {
                let notified = self.idle.notified();
                tokio::pin!(notified);
                // Register the waiter before checking emptiness: `notify_waiters`
                // wakes only already-registered waiters and stores no permit, so
                // a reaper that drains the map between the check and the await
                // would otherwise be missed and leave us waiting forever.
                notified.as_mut().enable();
                if self.sessions.lock().expect("sessions lock").is_empty() {
                    break;
                }
                notified.await;
            }
        };
        if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, drained)
            .await
            .is_err()
        {
            tracing::warn!("timed out waiting for sessions to finalize during shutdown");
        }
    }
}

/// A session brought up by [`prepare_and_spawn`]: enough state for the daemon to
/// track and reap it, or for `serve-session` to await it.
struct SpawnedSession {
    summary: SessionSummary,
    topic: String,
    worker: WorkerHandle,
    /// FS-watch task pushing `fs_change` events; aborted when the session ends.
    fs_watcher: JoinHandle<()>,
    /// Terminal output bridge (PTY backends only); `None` for codex app-server.
    /// `serve-session` awaits it so the trailing `pty_output`/`pty_exit` are
    /// pushed before finalize. The daemon lets it run detached.
    output_bridge: Option<JoinHandle<()>>,
    finalize: FinalizeCtx,
}

/// Shared session bring-up used by both the daemon and `serve-session`: resolve
/// the executor/argv, register the channel, start the proxy, run setup scripts,
/// spawn the backend, and wire the bridges. Workspace resolution/authorization
/// is the caller's responsibility (`spec.cwd`/`resolved` are already set).
async fn prepare_and_spawn(
    socket: &SocketHandle,
    upload: &UploadConfig,
    spec: SessionSpec,
    resolved: workspace::ResolvedWorkspace,
) -> Result<SpawnedSession> {
    let cwd_string = spec.cwd.display().to_string();

    // The user's explicit selection resolves to an executor that drives the
    // backend choice, argv transforms, native-log capability, and the console's
    // renderer label.
    let executor = executor::resolve_executor(&spec.agent);
    let agent_info = executor.info();
    let backend = executor.backend(spec.app_server);

    // Resolve the launch command (configured command + the user's extra args)
    // once, so registration and either backend share one argv.
    let agent_commands = ConfigStore::default().agent_commands().unwrap_or_default();
    let resolved_argv = executor::launch_argv(&spec.agent, &spec.custom_command, &agent_commands);

    let metadata = session_metadata(&spec, &resolved, executor.metadata_kind(spec.app_server));

    let opened = socket
        .open_session(RegisterSessionRequest {
            session_id: spec.session_id.clone(),
            name: spec.name.clone(),
            cwd: cwd_string.clone(),
            argv: resolved_argv.clone(),
            metadata,
        })
        .await?;
    let channel = opened.handle;
    let session_id = opened.registered.id.clone();

    // Recording stores forward to the channel (and so into its outbox).
    let traces = build_trace_store(spec.trace_path.as_deref(), channel.clone())?;
    let reviews = build_review_store(spec.no_review, spec.review_timeout_secs, &channel);

    let proxy_addr = start_proxy(
        spec.bind,
        spec.proxy_port,
        traces.clone(),
        reviews.clone(),
        spec.model.clone(),
    )
    .await?;
    let proxy_url = crate::proxy_base_url(spec.bind, proxy_addr);
    let env = proxy::agent_env(&proxy_url);
    let started_at = SystemTime::now();

    // Build the session's step chain and run any leading setup scripts before
    // the agent; the remaining (cleanup) chain is deferred to the reaper. A
    // non-zero setup exit aborts the session before the agent starts.
    let (setup_script, cleanup_script) = resolve_session_scripts(&spec);
    let chain = ExecutorAction::session_chain(setup_script, cleanup_script);
    let cleanup_chain = prepare_session_chain(
        socket, &channel, &spec.cwd, &env, agent_info, &resolved, chain,
    )
    .await?;

    // Choose the backend (codex app-server vs PTY) and wire its bridges.
    let (worker, recorder, native_session_id, output_bridge) = spawn_backend(
        backend,
        executor.as_ref(),
        &resolved_argv,
        &proxy_url,
        &env,
        &channel,
        &reviews,
        &spec,
        &session_id,
    )
    .await?;

    // Review decisions flow back the same way for both backends.
    spawn_review_decision_bridge(&channel, reviews.clone());

    // Push the git changed-files list to the control plane whenever the
    // workspace changes, so the UI's Changes panel stays live.
    let fs_watcher = super::fs_watch::spawn_fs_watcher(channel.clone(), spec.cwd.clone());

    let summary = SessionSummary {
        session_id: session_id.clone(),
        name: spec.name.clone().or_else(|| opened.registered.name.clone()),
        cwd: cwd_string,
        argv: resolved_argv,
        proxy_url,
    };

    let finalize = FinalizeCtx {
        channel: channel.clone(),
        traces: Arc::clone(&traces),
        recorder,
        agent: agent_info,
        upload: upload.clone(),
        session_id,
        cwd: spec.cwd.clone(),
        native_session_id,
        started_at,
        created_worktree: resolved.created_worktree.clone(),
        origin_root: resolved.origin_root.clone(),
        cleanup_chain,
        env,
    };

    Ok(SpawnedSession {
        summary,
        topic: channel.topic(),
        worker,
        fs_watcher,
        output_bridge,
        finalize,
    })
}

/// Run a single session to completion against a fresh connection, blocking until
/// the agent exits and its artifacts are finalized/uploaded. Used by `omniagent
/// serve-session` inside an oad sandbox. The cwd IS the workspace (no allowlist
/// or git-worktree machinery — the sandbox is the isolation unit). Returns the
/// agent's exit code.
pub async fn run_serve_session(config: ClientConfig, spec: SessionSpec) -> Result<i32> {
    let socket = PhoenixSocket::start(config.clone());
    let handle = socket.handle();
    let upload = UploadConfig {
        server_url: config.server_url,
        token: config.token,
    };
    let resolved = workspace::ResolvedWorkspace {
        cwd: spec.cwd.clone(),
        origin_root: spec.cwd.clone(),
        created_worktree: None,
    };

    let spawned = prepare_and_spawn(&handle, &upload, spec, resolved).await?;
    let SpawnedSession {
        worker,
        fs_watcher,
        output_bridge,
        finalize,
        ..
    } = spawned;
    let channel = finalize.channel.clone();

    let exit_code = worker.wait_exit().await;
    // Let the terminal bridge push the final output + `pty_exit` before finalize.
    if let Some(bridge) = output_bridge {
        let _ = tokio::time::timeout(Duration::from_secs(5), bridge).await;
    }
    fs_watcher.abort();
    finalize_session(finalize, exit_code).await;
    // Flush the outbox (trailing pty_output/pty_exit) to the wire before the
    // socket is dropped and the process exits.
    channel.drain_to_wire(Duration::from_secs(10)).await;
    drop(socket);
    Ok(exit_code)
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
    /// An auto-created worktree to prune on close, if any.
    created_worktree: Option<PathBuf>,
    /// Origin repo the worktree belongs to (the `git worktree remove` anchor).
    origin_root: PathBuf,
    /// Trailing chain steps (cleanup scripts) to run after the agent exits,
    /// before artifacts are collected.
    cleanup_chain: Option<ExecutorAction>,
    /// Agent environment (proxy base-URL overrides) — reused for cleanup scripts.
    env: Vec<(String, String)>,
}

/// Finalizes recordings, uploads artifacts, and emits the `session_close`
/// event. Best-effort throughout: failing to record, build, or upload any one
/// artifact is logged and never blocks teardown.
async fn finalize_session(ctx: FinalizeCtx, exit_code: i32) {
    // Run trailing cleanup scripts before collecting artifacts (the cwd, and any
    // auto-created worktree, still exist — the worktree is pruned last below).
    // Best-effort: a failing cleanup script is logged, not fatal.
    let mut step = ctx.cleanup_chain.as_ref();
    while let Some(node) = step {
        if let ActionType::Script(req) = &node.typ
            && let Err(err) = run_script_action(&ctx.channel, &ctx.cwd, &ctx.env, req).await
        {
            tracing::warn!(error = %err, label = req.label, "cleanup script failed to run");
        }
        step = node.next();
    }

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

    // Locate the agent's native session log so it can be uploaded verbatim as
    // the raw `session_log` artifact.
    let native_log = ctx
        .agent
        .filter(|info| info.has_native_log)
        .and_then(|agent| {
            agent_log::locate_native_log(
                agent,
                &ctx.cwd,
                ctx.native_session_id.as_deref(),
                ctx.started_at,
            )
            .map(|path| (agent, path))
        });

    // 2. Raw native session log — uploaded verbatim. Lossless; for claude it
    // preserves subagent (`Task`) sidechains.
    if let Some((_, path)) = &native_log {
        match tokio::fs::read(path).await {
            Ok(bytes) if !bytes.is_empty() => pending.push(("session_log", bytes)),
            Ok(_) => {}
            Err(err) => tracing::warn!(error = %err, "failed to read native session log"),
        }
    }

    // 3. Raw requests — the exact intercepted spans as JSONL, for any session
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

    // Prune an auto-created worktree last — after all artifacts (including the
    // native session log living under it) have been collected above.
    if let Some(worktree) = &ctx.created_worktree {
        workspace::remove_worktree(&ctx.origin_root, worktree);
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

fn spawn_worker_bridge(
    channel: &ChannelHandle,
    worker: WorkerHandle,
    workspace: PathBuf,
) -> JoinHandle<()> {
    let output_bridge = spawn_terminal_output_bridge(channel, worker.clone());
    spawn_command_bridge(channel, worker, workspace);
    output_bridge
}

/// Records a PTY session's terminal to a `.cast` file under the data dir.
/// Best-effort: a failure is logged and yields `None` rather than blocking startup.
fn start_cast_recorder(session_id: &str, worker: &WorkerHandle) -> Option<CastRecorder> {
    let recording_path = omniagent_data_dir()
        .join("recordings")
        .join(format!("{session_id}.cast"));
    match CastRecorder::spawn(recording_path, worker, DEFAULT_PTY_ROWS, DEFAULT_PTY_COLS) {
        Ok(recorder) => Some(recorder),
        Err(err) => {
            tracing::warn!(error = %err, "failed to start terminal recording");
            None
        }
    }
}

/// Forwards PTY output (and the final exit) from the worker into the channel.
///
/// A single task owns the output stream so `pty_exit` is always sequenced
/// *after* the trailing `pty_output`: when the worker exits we drain whatever
/// output is still buffered in the broadcast before announcing the exit, so the
/// "agent exited" banner never races ahead of the agent's final lines. (The
/// output broadcast does not reliably close on exit — late attachers may keep a
/// sender alive — so we can't rely on a `Closed` to mark the end of output.)
fn spawn_terminal_output_bridge(channel: &ChannelHandle, worker: WorkerHandle) -> JoinHandle<()> {
    let channel = channel.clone();
    tokio::spawn(async move {
        let exit_code = pump_terminal_output(&channel, &worker).await;
        channel.push("pty_exit", json!({ "exit_code": exit_code }));
    })
}

/// Drains a worker's PTY output to the channel as `pty_output` events until the
/// worker exits, returning its exit code. Does **not** emit `pty_exit` — the
/// caller decides whether the exit ends the session (the agent) or is just the
/// end of an intermediate step (a setup/cleanup script).
async fn pump_terminal_output(channel: &ChannelHandle, worker: &WorkerHandle) -> i32 {
    let mut terminal = worker.attach();
    // Carry an incomplete trailing UTF-8 sequence across chunks so a multi-byte
    // codepoint split at a chunk boundary isn't corrupted.
    let mut carry: Vec<u8> = Vec::new();
    let push_output = |carry: &mut Vec<u8>, bytes: &[u8]| {
        let data = decode_streaming(carry, bytes);
        if !data.is_empty() {
            channel.push("pty_output", json!({ "data": data }));
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

    // Process exited: flush any output still buffered in the broadcast.
    loop {
        match terminal.output.try_recv() {
            Ok(chunk) => push_output(&mut carry, &chunk),
            Err(broadcast::error::TryRecvError::Lagged(_)) => {}
            Err(_) => break,
        }
    }
    exit_code
}

/// Spawns the session's agent on the chosen backend and wires its bridges,
/// returning the worker handle, an optional terminal recorder (PTY only), and any
/// native session id we injected (claude). Codex-native drives `codex app-server`
/// over JSON-RPC and renders structured events; everything else runs in a PTY.
#[allow(clippy::too_many_arguments)]
async fn spawn_backend(
    backend: Backend,
    executor: &dyn executor::Executor,
    resolved_argv: &[String],
    proxy_url: &str,
    env: &[(String, String)],
    channel: &ChannelHandle,
    reviews: &Arc<ReviewStore>,
    spec: &SessionSpec,
    session_id: &str,
) -> Result<(
    WorkerHandle,
    Option<CastRecorder>,
    Option<String>,
    Option<JoinHandle<()>>,
)> {
    match backend {
        Backend::CodexAppServer => {
            let app_argv = executor.app_server_argv(resolved_argv, proxy_url);
            let (codex_worker, events) =
                CodexWorkerHandle::spawn(&app_argv, env, &spec.cwd, spec.model.as_deref()).await?;
            spawn_codex_event_bridge(channel, codex_worker.clone(), events.notifications);
            spawn_codex_approval_bridge(reviews.clone(), codex_worker.clone(), events.approvals);
            spawn_codex_command_bridge(channel, codex_worker.clone(), spec.cwd.clone());
            Ok((WorkerHandle::new_codex(codex_worker), None, None, None))
        }
        Backend::Pty => {
            // For claude-code, inject a known `--session-id` so its transcript is
            // locatable at close; other agents are spawned unchanged.
            let (spawn_argv, native_session_id) = executor.pty_argv(resolved_argv, proxy_url);
            // Wait briefly for the browser's first resize so the agent opens at the
            // visible terminal's size. claude prints to scrollback (not the alt
            // screen), so anything drawn at the wrong width before a later resize
            // stays wrong — opening at the right size avoids that. Falls back to
            // the spec size (or default) if no resize arrives in time.
            let fallback = spec
                .pty_size
                .unwrap_or((DEFAULT_PTY_ROWS, DEFAULT_PTY_COLS));
            let (rows, cols) = await_initial_pty_size(channel, fallback).await;
            let agent = AgentHandle::spawn(&spawn_argv, env, Some(spec.cwd.as_path()), rows, cols)?;
            let worker = WorkerHandle::new_pty(agent);
            let recorder = start_cast_recorder(session_id, &worker);
            let output_bridge = spawn_worker_bridge(channel, worker.clone(), spec.cwd.clone());
            Ok((worker, recorder, native_session_id, Some(output_bridge)))
        }
    }
}

const INITIAL_RESIZE_WAIT: Duration = Duration::from_millis(750);

/// Waits briefly for the browser's first `pty_resize` so the agent PTY opens at
/// the visible terminal's size. Returns `fallback` if none arrives in time
/// (e.g. the session isn't being viewed yet — a later resize then reflows it).
async fn await_initial_pty_size(channel: &ChannelHandle, fallback: (u16, u16)) -> (u16, u16) {
    let mut commands = channel.subscribe_commands();

    let wait = async {
        loop {
            match commands.recv().await {
                Ok(ServerCommand::PtyResize { rows, cols }) if rows > 0 && cols > 0 => {
                    return (rows, cols);
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return fallback,
            }
        }
    };

    tokio::time::timeout(INITIAL_RESIZE_WAIT, wait)
        .await
        .unwrap_or(fallback)
}

/// streaming its output to the session's terminal, and returns its exit code.
/// Used for setup (before the agent) and cleanup (after the agent) steps.
async fn run_script_action(
    channel: &ChannelHandle,
    cwd: &Path,
    env: &[(String, String)],
    req: &ScriptRequest,
) -> Result<i32> {
    let argv = vec!["sh".to_string(), "-lc".to_string(), req.script.clone()];
    let agent = AgentHandle::spawn(&argv, env, Some(cwd), DEFAULT_PTY_ROWS, DEFAULT_PTY_COLS)
        .with_context(|| format!("failed to spawn {} script", req.label))?;
    let worker = WorkerHandle::new_pty(agent);
    Ok(pump_terminal_output(channel, &worker).await)
}

/// Announces a session that failed before its agent started (e.g. a setup script
/// errored), then prunes any auto-created worktree. The caller closes the channel
/// and returns the error.
async fn abort_before_agent(
    channel: &ChannelHandle,
    agent: Option<AgentInfo>,
    resolved: &workspace::ResolvedWorkspace,
    err: &anyhow::Error,
) {
    let payload = json!({
        "exit_code": -1,
        "agent": {
            "name": agent.map(|info| info.name),
            "supported": agent.is_some(),
        },
        "artifacts": [],
        "error": format!("{err:#}"),
    });
    if let Err(send_err) = channel.send_now("session_close", payload).await {
        tracing::debug!(error = %send_err, "session_close (abort) not delivered (socket down)");
    }
    if let Some(worktree) = &resolved.created_worktree {
        workspace::remove_worktree(&resolved.origin_root, worktree);
    }
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
                ServerCommand::Shutdown => {
                    worker.shutdown().await;
                    break;
                }
                // Handled elsewhere (review-decision bridge / daemon control /
                // codex command bridge) or not applicable to a PTY session.
                ServerCommand::ReviewDecision { .. }
                | ServerCommand::SpawnAgent { .. }
                | ServerCommand::CreateWorkspace { .. }
                | ServerCommand::CodexInput { .. }
                | ServerCommand::CodexInterrupt => {}
                workspace_request @ ServerCommand::DiffRequest { .. } => {
                    handle_workspace_request(&channel, &workspace, &workspace_request).await;
                }
            }
        }
    });
}

/// Serves the workspace diff read command shared by the PTY and codex command
/// bridges. Returns `true` if `command` was a workspace request.
async fn handle_workspace_request(
    channel: &ChannelHandle,
    workspace: &Path,
    command: &ServerCommand,
) -> bool {
    match command {
        ServerCommand::DiffRequest { path } => {
            let workspace = workspace.to_path_buf();
            let path = path.clone();
            let payload = tokio::task::spawn_blocking(move || {
                let filter = (!path.trim().is_empty()).then_some(path.as_str());
                let diff = files::git_diff(&workspace, filter);
                json!({"path": path, "ok": true, "diff": diff})
            })
            .await
            .unwrap_or_else(|_| json!({"ok": false, "error": "diff task panicked"}));
            channel.push("diff_response", payload);
            true
        }
        _ => false,
    }
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

// ---------------------------------------------------------------------------
// Codex app-server bridges (structured conversation instead of a PTY)
// ---------------------------------------------------------------------------

/// Forwards codex notifications to the channel as structured conversation
/// events, tracking the active turn id so an interrupt can target it.
fn spawn_codex_event_bridge(
    channel: &ChannelHandle,
    worker: CodexWorkerHandle,
    mut notifications: mpsc::UnboundedReceiver<Notification>,
) {
    let channel = channel.clone();
    tokio::spawn(async move {
        // Codex sends reasoning text only via ephemeral deltas; accumulate it per
        // item so the durable reasoning item can be backfilled on completion and
        // survive replay.
        let mut reasoning: HashMap<String, String> = HashMap::new();
        while let Some(note) = notifications.recv().await {
            match note.method.as_str() {
                "turn/started" => worker.set_active_turn(codex::turn_started_id(&note.params)),
                "turn/completed" => worker.set_active_turn(None),
                _ => {}
            }
            if let Some((item_id, delta)) = codex::reasoning_delta(&note) {
                reasoning.entry(item_id).or_default().push_str(&delta);
            }
            if let Some((event, mut payload)) = codex::map_notification(&note) {
                codex::backfill_reasoning(&mut payload, &mut reasoning);
                channel.push(event, payload);
            }
        }
    });
}

/// Turns codex approval requests into review-gate items and routes the human
/// decision back as the codex approval response. Reuses the existing
/// [`ReviewStore`] so codex approvals surface in the console's Reviews pane.
fn spawn_codex_approval_bridge(
    reviews: Arc<ReviewStore>,
    worker: CodexWorkerHandle,
    mut approvals: mpsc::UnboundedReceiver<ServerRequest>,
) {
    tokio::spawn(async move {
        while let Some(req) = approvals.recv().await {
            // One task per request so a slow human decision on one approval
            // doesn't block surfacing the next.
            let reviews = Arc::clone(&reviews);
            let worker = worker.clone();
            tokio::spawn(async move {
                let item = approval_review_item(&req);
                let decision = reviews.prompt(item).await;
                worker
                    .respond_approval(&req.id, codex::map_decision(&decision))
                    .await;
            });
        }
    });
}

/// First present key from `keys` read as a string, else empty (codex mixes
/// `snake_case` and `camelCase` across payloads).
fn json_str(value: &serde_json::Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .unwrap_or_default()
        .to_string()
}

/// Builds a review item from a codex approval request. The id encodes the rpc id
/// so the returned decision round-trips to the right request; codex isn't an
/// LLM-proxy call, so the proxy-specific fields stay empty.
fn approval_review_item(req: &ServerRequest) -> ReviewItem {
    let p = &req.params;
    let (method, path) = if req.method.contains("fileChange") {
        // The fileChange approval references the item by id (codex mixes
        // snake/camel spellings across payloads, so accept both).
        let id = json_str(p, &["itemId", "item_id"]);
        ("file_change", id)
    } else {
        // For a command approval, show the command itself (falling back to the
        // cwd) rather than the cwd alone — that's what the human is approving.
        let command = p
            .get("command")
            .and_then(serde_json::Value::as_array)
            .map(|parts| {
                parts
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| json_str(p, &["cwd"]));
        ("command_execution", command)
    };
    ReviewItem {
        id: codex::approval_review_id(req),
        sequence: 0,
        phase: ReviewPhase::Request,
        attempt: 1,
        // Codex runs on OpenAI; the review UI is provider-labelled but otherwise
        // provider-agnostic.
        provider: Provider::OpenAI,
        model: None,
        method: method.to_string(),
        path,
        streaming: false,
        request_base_url: String::new(),
        upstream_base_url: String::new(),
        request_headers: std::collections::BTreeMap::new(),
        request: req.params.clone(),
        response_headers: std::collections::BTreeMap::new(),
        response: serde_json::Value::Null,
        usage: crate::record::Usage::default(),
        status: None,
        latency_ms: None,
        started_at: OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        error: None,
    }
}

/// Applies server->client commands for a codex session: user turns, interrupt,
/// shutdown, and the shared workspace file/diff/dir reads.
fn spawn_codex_command_bridge(
    channel: &ChannelHandle,
    worker: CodexWorkerHandle,
    workspace: PathBuf,
) {
    let mut commands = channel.subscribe_commands();
    let channel = channel.clone();
    tokio::spawn(async move {
        loop {
            let command = match commands.recv().await {
                Ok(command) => command,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            };
            match command {
                ServerCommand::CodexInput { text } => worker.start_turn(&text).await,
                ServerCommand::CodexInterrupt => worker.interrupt().await,
                ServerCommand::Shutdown => {
                    worker.shutdown().await;
                    break;
                }
                // Handled by the review-decision bridge / daemon control channel,
                // or not applicable to a codex session.
                ServerCommand::ReviewDecision { .. }
                | ServerCommand::SpawnAgent { .. }
                | ServerCommand::CreateWorkspace { .. }
                | ServerCommand::PtyInput { .. }
                | ServerCommand::PtyResize { .. } => {}
                workspace_request @ ServerCommand::DiffRequest { .. } => {
                    handle_workspace_request(&channel, &workspace, &workspace_request).await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::sanitized_workspace_name;

    #[test]
    fn accepts_a_plain_name_and_trims_it() {
        assert_eq!(
            sanitized_workspace_name("  my-project  ").unwrap(),
            "my-project"
        );
    }

    #[test]
    fn rejects_empty_separators_and_traversal() {
        for bad in [
            "",
            "  ",
            ".",
            "..",
            "a/b",
            "/abs",
            "../escape",
            "foo/../bar",
        ] {
            assert!(
                sanitized_workspace_name(bad).is_err(),
                "should reject {bad:?}"
            );
        }
    }
}
