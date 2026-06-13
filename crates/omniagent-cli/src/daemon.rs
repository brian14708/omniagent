//! The persistent `omniagent daemon`: a long-running supervisor that hosts many
//! agent sessions over one resilient control-plane connection and accepts control
//! commands on a local Unix socket.
//!
//! It is structured to be supervised by systemd later: it runs in the
//! foreground, logs to stderr, and shuts every session down cleanly on SIGTERM
//! or Ctrl-C.

use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::{Value, json};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::broadcast;

use crate::client::{
    ClientConfig, DEFAULT_REVIEW_TIMEOUT_SECS, DaemonSupervisor, SessionSpec, WorkspacePolicy,
};
use crate::control::{self, ControlRequest, ControlResponse, SpawnRequest};
use crate::protocol::ServerCommand;

/// Run the daemon until interrupted.
pub async fn run_daemon(
    bind: IpAddr,
    config: ClientConfig,
    persist_traces: bool,
    policy: WorkspacePolicy,
) -> Result<()> {
    let supervisor = Arc::new(DaemonSupervisor::start(config, policy));
    spawn_control_listener(Arc::clone(&supervisor), bind, persist_traces);

    let (listener, path) = control::bind_listener()?;
    println!("omniagent daemon: control socket {}", path.display());
    if policy == WorkspacePolicy::FullAccess {
        println!(
            "omniagent daemon: full-access mode — spawns are NOT confined to allowed workspaces"
        );
    } else {
        println!(
            "omniagent daemon: restricted to allowed workspaces (manage with `omniagent workspaces`)"
        );
    }
    println!("omniagent daemon: ready (start a session from the omniagent web app)");

    let mut sigterm = signal(SignalKind::terminate())?;

    let accept_supervisor = Arc::clone(&supervisor);
    let accept = async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let supervisor = Arc::clone(&accept_supervisor);
                    tokio::spawn(async move {
                        control::serve_connection(stream, |request| {
                            handle_request(supervisor, bind, persist_traces, request)
                        })
                        .await;
                    });
                }
                Err(err) => tracing::warn!(error = %err, "control socket accept failed"),
            }
        }
    };

    tokio::select! {
        _ = accept => {}
        _ = tokio::signal::ctrl_c() => println!("\nomniagent daemon: interrupted"),
        _ = sigterm.recv() => println!("omniagent daemon: received SIGTERM"),
    }

    println!("omniagent daemon: shutting down sessions");
    supervisor.shutdown_all().await;
    let _ = std::fs::remove_file(&path);
    Ok(())
}

async fn handle_request(
    supervisor: Arc<DaemonSupervisor>,
    bind: IpAddr,
    persist_traces: bool,
    request: ControlRequest,
) -> ControlResponse {
    match request {
        ControlRequest::Ping => ControlResponse::Pong,
        ControlRequest::List => ControlResponse::Sessions(supervisor.list_sessions()),
        ControlRequest::Stop { session_id } => ControlResponse::Stopped {
            found: supervisor.stop_session(&session_id).await,
        },
        ControlRequest::RefreshWorkspaces => {
            match supervisor.refresh_workspaces(control_metadata()).await {
                Ok(()) => ControlResponse::WorkspacesRefreshed,
                Err(err) => ControlResponse::Error {
                    message: format!("{err:#}"),
                },
            }
        }
        ControlRequest::ServerConfig { .. } => ControlResponse::Error {
            message: "server_config passthrough is not implemented yet".to_string(),
        },
        ControlRequest::Spawn(spawn) => {
            spawn_session(&supervisor, bind, persist_traces, *spawn).await
        }
    }
}

async fn spawn_session(
    supervisor: &Arc<DaemonSupervisor>,
    bind: IpAddr,
    persist_traces: bool,
    spawn: SpawnRequest,
) -> ControlResponse {
    let spec = SessionSpec {
        agent: spawn.agent,
        custom_command: spawn.custom_command,
        cwd: PathBuf::from(spawn.cwd),
        name: spawn.name,
        session_id: spawn.session_id,
        bind,
        proxy_port: spawn.proxy_port,
        no_review: spawn.no_review,
        review_timeout_secs: spawn
            .review_timeout_secs
            .unwrap_or(DEFAULT_REVIEW_TIMEOUT_SECS),
        // Each session gets a fresh trace archive when persistence is enabled.
        trace_path: persist_traces.then(crate::default_trace_path),
        app_server: spawn.app_server,
        model: spawn.model,
        workspace: spawn.workspace.map(PathBuf::from),
        branch: spawn.branch,
        create_worktree: spawn.create_worktree,
        worktree: spawn.worktree.map(PathBuf::from),
        base_branch: spawn.base_branch,
        setup_script: None,
        cleanup_script: None,
        pty_size: None,
    };
    match supervisor.spawn_session(spec).await {
        Ok(summary) => ControlResponse::Spawned(summary),
        Err(err) => ControlResponse::Error {
            message: format!("{err:#}"),
        },
    }
}

/// Open the daemon control channel and spawn agents on `spawn_agent` commands
/// from the server. Retries registration until the control plane is reachable; the
/// channel rejoins automatically across reconnects.
fn spawn_control_listener(supervisor: Arc<DaemonSupervisor>, bind: IpAddr, persist_traces: bool) {
    tokio::spawn(async move {
        let metadata = control_metadata();
        let control = loop {
            match supervisor.open_control_channel(metadata.clone()).await {
                Ok(handle) => {
                    tracing::debug!("registered daemon control channel with control plane");
                    break handle;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "daemon control registration failed; retrying");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        };

        let mut commands = control.subscribe_commands();
        loop {
            match commands.recv().await {
                Ok(ServerCommand::SpawnAgent {
                    agent,
                    custom_command,
                    cwd,
                    name,
                    app_server,
                    model,
                    workspace,
                    branch,
                    create_worktree,
                    worktree,
                    base_branch,
                }) => {
                    let cwd = cwd.map_or_else(
                        || std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
                        PathBuf::from,
                    );
                    let spec = SessionSpec {
                        agent,
                        custom_command,
                        cwd,
                        name,
                        session_id: None,
                        bind,
                        proxy_port: 0,
                        no_review: false,
                        review_timeout_secs: DEFAULT_REVIEW_TIMEOUT_SECS,
                        trace_path: persist_traces.then(crate::default_trace_path),
                        app_server,
                        model,
                        workspace: workspace.map(PathBuf::from),
                        branch,
                        create_worktree,
                        worktree: worktree.map(PathBuf::from),
                        base_branch,
                        setup_script: None,
                        cleanup_script: None,
                        pty_size: None,
                    };
                    let supervisor = Arc::clone(&supervisor);
                    tokio::spawn(async move {
                        match supervisor.spawn_session(spec).await {
                            Ok(summary) => {
                                tracing::debug!(session = %summary.session_id, "spawned agent via control channel");
                            }
                            Err(err) => {
                                tracing::error!(error = %err, "control-channel spawn failed");
                            }
                        }
                    });
                }
                Ok(ServerCommand::CreateWorkspace { name }) => {
                    let supervisor = Arc::clone(&supervisor);
                    tokio::spawn(async move {
                        match DaemonSupervisor::create_workspace(&name) {
                            Ok(path) => {
                                tracing::info!(path = %path.display(), "created workspace via control channel");
                                // Re-advertise so the new workspace appears in the
                                // console's pickers immediately.
                                if let Err(err) =
                                    supervisor.refresh_workspaces(control_metadata()).await
                                {
                                    tracing::warn!(error = %err, "workspace created but re-advertise failed");
                                }
                            }
                            Err(err) => {
                                tracing::error!(error = %err, "create workspace failed");
                            }
                        }
                    });
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

/// Metadata advertised to the control plane so the UI can label this daemon and
/// populate its workspace/branch pickers.
fn control_metadata() -> Value {
    let cwd = std::env::current_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    json!({
        "hostname": hostname(),
        "pid": std::process::id(),
        "cwd": cwd,
        "agents": ["claude", "codex", "gemini"],
        "workspaces": advertised_workspaces(),
    })
}

/// Detects each allowed workspace root, yielding the classified info (path, kind,
/// branches, worktrees) the console uses to drive its spawn pickers.
fn advertised_workspaces() -> Vec<Value> {
    crate::config::ConfigStore::default()
        .workspace_roots()
        .unwrap_or_default()
        .iter()
        .map(|root| {
            crate::workspace::detect(root)
                .ok()
                .and_then(|ws| serde_json::to_value(&ws).ok())
                .unwrap_or_else(|| json!({ "path": root, "kind": "plain" }))
        })
        .collect()
}

/// Best-effort host name: `HOSTNAME`/`HOST` env, else the `hostname` command,
/// else `"daemon"`.
fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::env::var("HOST").ok())
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .and_then(|out| String::from_utf8(out.stdout).ok())
                .map(|s| s.trim().to_string())
        })
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "daemon".to_string())
}
