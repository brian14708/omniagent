//! Supervises a `codex app-server` child and exposes its thread/turn lifecycle.
//!
//! Unlike the PTY [`crate::agent::AgentHandle`], this worker is not a byte pump:
//! it spawns `codex app-server --listen stdio://`, performs the JSON-RPC
//! handshake, opens a thread, and hands the supervisor the notification /
//! approval streams to bridge to the control plane. It shares the same lifecycle
//! contract the reaper relies on — [`wait_exit`](CodexWorkerHandle::wait_exit)
//! resolves when the child dies, and [`shutdown`](CodexWorkerHandle::shutdown)
//! kills it (causing stdout EOF → `wait_exit` to fire).

use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio::sync::{mpsc, watch};

use super::client::{CodexClient, Notification, ServerRequest};

/// The notification and approval streams a freshly spawned worker emits, handed
/// to the supervisor to wire into the channel.
pub struct CodexEvents {
    pub notifications: mpsc::UnboundedReceiver<Notification>,
    pub approvals: mpsc::UnboundedReceiver<ServerRequest>,
}

struct CodexWorkerInner {
    client: Arc<CodexClient>,
    thread_id: String,
    /// Id of the in-progress turn, tracked from `turn/started`/`turn/completed`
    /// so an interrupt can target it.
    active_turn: Mutex<Option<String>>,
    exit_rx: watch::Receiver<Option<i32>>,
    pid: Option<u32>,
}

/// Cloneable handle to a supervised `codex app-server` session.
#[derive(Clone)]
pub struct CodexWorkerHandle {
    inner: Arc<CodexWorkerInner>,
}

impl CodexWorkerHandle {
    /// Spawns `codex app-server` (already including `app-server --listen
    /// stdio://` in `argv` — see [`app_server_argv`]), performs the handshake and
    /// opens a thread. `extra_env` carries the recording-proxy base URLs so codex
    /// still routes its LLM calls through the proxy.
    pub async fn spawn(
        argv: &[String],
        extra_env: &[(String, String)],
        cwd: &Path,
        model: Option<&str>,
    ) -> Result<(Self, CodexEvents)> {
        let program = argv.first().context("codex command must not be empty")?;
        let mut command = Command::new(program);
        command.args(&argv[1..]);
        for (key, value) in extra_env {
            command.env(key, value);
        }
        // codex app-server logs its own (very verbose) span tracing to stderr,
        // which we inherit. Quiet it to errors unless the operator set RUST_LOG.
        if std::env::var_os("RUST_LOG").is_none() {
            command.env("RUST_LOG", "error");
        }
        command
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(false);
        // Run in a fresh process group so shutdown can signal the whole group
        // (codex's command-execution children included), mirroring the PTY path.
        #[cfg(unix)]
        command.process_group(0);

        let mut child = command
            .spawn()
            .with_context(|| format!("failed to spawn codex app-server ({program})"))?;
        let pid = child.id();
        let stdin = child
            .stdin
            .take()
            .context("codex app-server stdin missing")?;
        let stdout = child
            .stdout
            .take()
            .context("codex app-server stdout missing")?;

        let (client, notifications, approvals) = CodexClient::spawn(stdin, stdout);

        let (exit_tx, exit_rx) = watch::channel::<Option<i32>>(None);
        tokio::spawn(async move {
            let code = child
                .wait()
                .await
                .ok()
                .and_then(|status| status.code())
                .unwrap_or(-1);
            let _ = exit_tx.send(Some(code));
        });

        client
            .initialize()
            .await
            .context("codex app-server initialize failed")?;
        let thread_id = client
            .thread_start(model, cwd)
            .await
            .context("codex app-server thread/start failed")?;

        let inner = Arc::new(CodexWorkerInner {
            client,
            thread_id,
            active_turn: Mutex::new(None),
            exit_rx,
            pid,
        });
        Ok((
            Self { inner },
            CodexEvents {
                notifications,
                approvals,
            },
        ))
    }

    /// Records the in-progress turn id (set on `turn/started`, cleared on
    /// `turn/completed`).
    pub fn set_active_turn(&self, turn: Option<String>) {
        *self
            .inner
            .active_turn
            .lock()
            .expect("codex active turn lock") = turn;
    }

    #[must_use]
    pub fn active_turn(&self) -> Option<String> {
        self.inner
            .active_turn
            .lock()
            .expect("codex active turn lock")
            .clone()
    }

    /// Submits a user turn (best-effort: a failure is logged, not surfaced).
    pub async fn start_turn(&self, text: &str) {
        if let Err(err) = self
            .inner
            .client
            .turn_start(&self.inner.thread_id, text)
            .await
        {
            tracing::warn!(error = %err, "codex turn/start failed");
        }
    }

    /// Interrupts the in-progress turn, if any.
    pub async fn interrupt(&self) {
        let Some(turn) = self.active_turn() else {
            return;
        };
        if let Err(err) = self
            .inner
            .client
            .turn_interrupt(&self.inner.thread_id, &turn)
            .await
        {
            tracing::warn!(error = %err, "codex turn/interrupt failed");
        }
    }

    /// Answers a codex approval request with a decision string.
    pub async fn respond_approval(&self, id: &Value, decision: &str) {
        self.inner
            .client
            .respond(id, json!({ "decision": decision }))
            .await;
    }

    /// Resolves once the app-server child exits, yielding its exit code.
    pub async fn wait_exit(&self) -> i32 {
        let mut rx = self.inner.exit_rx.clone();
        loop {
            let current = *rx.borrow();
            if let Some(code) = current {
                return code;
            }
            if rx.changed().await.is_err() {
                return 0;
            }
        }
    }

    /// Terminates the app-server process group. Idempotent and safe after the
    /// child has already exited.
    ///
    /// Killing the process group cancels any in-flight turn, so we do NOT first
    /// await a `turn/interrupt` RPC here — that request is bound by a 30s timeout
    /// and a wedged child (not reading stdin) would stall teardown for the full
    /// window, serialized across every session in `shutdown_all`.
    pub async fn shutdown(&self) {
        if self.inner.exit_rx.borrow().is_some() {
            return;
        }
        let pid = self.inner.pid;
        let _ = tokio::task::spawn_blocking(move || terminate(pid)).await;
    }
}

/// Builds the argv to launch the codex app-server from the resolved codex
/// command: appends the `app-server` subcommand and stdio transport, then config
/// overrides that route codex's model calls through omniagent's recording proxy.
///
/// Codex ignores `OPENAI_BASE_URL` for its built-in providers (verified against
/// codex 0.139), so — unlike the PTY agents — env-var redirection does not put
/// its LLM traffic through the proxy. Built-in provider ids are reserved and
/// can't be overridden, but a *custom* provider can, so we inject one whose
/// `base_url` points at the proxy and select it. Codex then POSTs
/// `/v1/responses` to the proxy (Responses wire API), which forwards upstream
/// with the real `OPENAI_API_KEY` like every other agent.
#[must_use]
pub fn app_server_argv(resolved: &[String], proxy_url: &str) -> Vec<String> {
    let mut argv = resolved.to_vec();
    if !argv.iter().any(|arg| arg == "app-server") {
        argv.push("app-server".to_string());
    }
    if !argv.iter().any(|arg| arg == "--listen") {
        argv.push("--listen".to_string());
        argv.push("stdio://".to_string());
    }
    let base_url = format!("{}/v1", proxy_url.trim_end_matches('/'));
    for override_ in [
        "model_provider=omniagent".to_string(),
        "model_providers.omniagent.name=\"omniagent\"".to_string(),
        format!("model_providers.omniagent.base_url=\"{base_url}\""),
        "model_providers.omniagent.wire_api=\"responses\"".to_string(),
        "model_providers.omniagent.env_key=\"OPENAI_API_KEY\"".to_string(),
        // Surface the model's reasoning summary so the console can render it —
        // codex emits reasoning deltas only when this is enabled (off by default).
        "model_reasoning_summary=\"detailed\"".to_string(),
    ] {
        argv.push("-c".to_string());
        argv.push(override_);
    }
    argv
}

/// Terminates the app-server process group (SIGTERM, then SIGKILL after a short
/// grace). The child was spawned in its own group, so the group id is its pid.
#[cfg(unix)]
fn terminate(pid: Option<u32>) {
    use rustix::process::{Pid, Signal};

    let Some(pid) = pid
        .and_then(|pid| i32::try_from(pid).ok())
        .and_then(Pid::from_raw)
    else {
        return;
    };
    signal_group(pid, Signal::TERM);
    for _ in 0..10 {
        if group_gone(pid) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    signal_group(pid, Signal::KILL);
}

#[cfg(unix)]
fn signal_group(pid: rustix::process::Pid, signal: rustix::process::Signal) {
    match rustix::process::kill_process_group(pid, signal) {
        Ok(()) | Err(rustix::io::Errno::SRCH) => {}
        Err(err) => tracing::debug!(error = %err, "failed to signal codex process group"),
    }
}

#[cfg(unix)]
fn group_gone(pid: rustix::process::Pid) -> bool {
    matches!(
        rustix::process::test_kill_process_group(pid),
        Err(rustix::io::Errno::SRCH)
    )
}

#[cfg(not(unix))]
fn terminate(_pid: Option<u32>) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_string()).collect()
    }

    const PROXY: &str = "http://127.0.0.1:7000";

    fn provider_overrides() -> Vec<String> {
        vec![
            "-c".to_string(),
            "model_provider=omniagent".to_string(),
            "-c".to_string(),
            "model_providers.omniagent.name=\"omniagent\"".to_string(),
            "-c".to_string(),
            "model_providers.omniagent.base_url=\"http://127.0.0.1:7000/v1\"".to_string(),
            "-c".to_string(),
            "model_providers.omniagent.wire_api=\"responses\"".to_string(),
            "-c".to_string(),
            "model_providers.omniagent.env_key=\"OPENAI_API_KEY\"".to_string(),
            "-c".to_string(),
            "model_reasoning_summary=\"detailed\"".to_string(),
        ]
    }

    #[test]
    fn app_server_argv_appends_subcommand_transport_and_provider() {
        let mut expected = argv(&["codex", "app-server", "--listen", "stdio://"]);
        expected.extend(provider_overrides());
        assert_eq!(app_server_argv(&argv(&["codex"]), PROXY), expected);
    }

    #[test]
    fn app_server_argv_keeps_existing_subcommand_and_listen() {
        // app-server present but --listen absent → transport still appended.
        let mut expected = argv(&["codex", "app-server", "--listen", "stdio://"]);
        expected.extend(provider_overrides());
        assert_eq!(
            app_server_argv(&argv(&["codex", "app-server"]), PROXY),
            expected
        );

        // An explicit --listen is preserved (not duplicated).
        let out = app_server_argv(
            &argv(&["codex", "app-server", "--listen", "ws://127.0.0.1:0"]),
            PROXY,
        );
        assert_eq!(
            out[..4],
            argv(&["codex", "app-server", "--listen", "ws://127.0.0.1:0"])[..]
        );
        assert!(out.iter().any(|a| a == "model_provider=omniagent"));
    }

    #[test]
    fn app_server_argv_preserves_resolved_launcher() {
        let out = app_server_argv(&argv(&["pnpm", "dlx", "codex"]), PROXY);
        assert_eq!(
            out[..6],
            argv(&["pnpm", "dlx", "codex", "app-server", "--listen", "stdio://"])[..]
        );
    }

    #[test]
    fn app_server_argv_points_provider_base_url_at_the_proxy() {
        let out = app_server_argv(&argv(&["codex"]), "http://10.0.0.5:9999/");
        assert!(
            out.iter()
                .any(|a| a == "model_providers.omniagent.base_url=\"http://10.0.0.5:9999/v1\""),
            "base_url should be the proxy url + /v1, with a trailing slash trimmed"
        );
    }
}
