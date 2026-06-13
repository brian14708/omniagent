//! Coding-agent executor abstraction.
//!
//! Each supervised session is launched through an [`Executor`]: the trait
//! captures everything agent-specific about *launching and identifying* an agent
//! — its canonical name and native-log capability ([`AgentInfo`]), which backend
//! it runs on ([`Backend`]), and how its launch argv is transformed for the proxy
//! (claude's `--session-id` injection, codex's provider overrides). This keeps
//! the supervisor free of `agent == "codex"` string checks and makes adding an
//! agent a single [`Executor`] impl plus a [`resolve_executor`] arm.
//!
//! Note omniagent agents are *interactive* (a PTY, or the codex app-server over
//! JSON-RPC), so this trait is about launch identity rather than headless
//! prompt execution.

use std::collections::BTreeMap;

use crate::codex;

pub mod action;

/// A recognized coding agent and its capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentInfo {
    /// Canonical, harbor-compatible agent name (e.g. `"claude-code"`).
    pub name: &'static str,
    /// Whether this agent writes a native on-disk session log we know how to
    /// locate (and therefore upload as the `session_log` artifact).
    pub has_native_log: bool,
}

/// Which backend drives a session's agent process.
///
/// Non-exhaustive so a future structured backend (e.g. `Acp`, for the
/// Gemini/Qwen/Copilot Agent-Client-Protocol family) is an additive change
/// rather than a breaking one.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Interactive PTY (claude / gemini / codex-TUI / custom commands).
    Pty,
    /// Native, structured `codex app-server` over JSON-RPC stdio.
    CodexAppServer,
}

/// Launch-time behavior for one coding agent.
pub trait Executor: Send + Sync {
    /// Capability handle (canonical name + native-log flag), or `None` for an
    /// unrecognized/custom command (recorded, but no native session log).
    fn info(&self) -> Option<AgentInfo>;

    /// Backend selection. `app_server` is the user's opt-in to the native codex
    /// app-server; only [`CodexExecutor`] honors it today.
    fn backend(&self, app_server: bool) -> Backend;

    /// Transforms the resolved launch argv for a PTY spawn, returning any native
    /// session id we injected (claude, for deterministic transcript discovery).
    fn pty_argv(&self, resolved_argv: &[String], proxy_url: &str) -> (Vec<String>, Option<String>);

    /// Argv for the codex app-server backend. Unused unless [`Self::backend`]
    /// returns [`Backend::CodexAppServer`].
    fn app_server_argv(&self, _resolved: &[String], _proxy_url: &str) -> Vec<String> {
        Vec::new()
    }

    /// Optional session-metadata `kind` label for the console's renderer choice.
    fn metadata_kind(&self, _app_server: bool) -> Option<&'static str> {
        None
    }
}

/// Resolves the user's selector (`claude`/`codex`/`gemini`) to an executor; any
/// other selector is a custom command run in a bare PTY with no native log.
#[must_use]
pub fn resolve_executor(agent: &str) -> Box<dyn Executor> {
    match agent {
        "claude" => Box::new(ClaudeExecutor),
        "codex" => Box::new(CodexExecutor),
        "gemini" => Box::new(GeminiExecutor),
        _ => Box::new(CustomExecutor),
    }
}

/// Builds the argv to spawn for the selected agent: its configured launch command
/// (e.g. `pnpm dlx @anthropic-ai/claude-code`) followed by the user's `extra`
/// args.
///
/// `commands` is the daemon's `agent_commands` map (see [`crate::config`]), keyed
/// by bare agent name. An agent with no configured command falls back to running
/// the bare name itself (`codex` → `["codex"]`), so an operator who has the agent
/// on `PATH` needs no configuration. Agent-agnostic: the per-agent argv transform
/// happens later in [`Executor::pty_argv`] / [`Executor::app_server_argv`].
#[must_use]
pub fn launch_argv(
    agent: &str,
    extra: &[String],
    commands: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let mut argv = commands
        .get(agent)
        .filter(|command| !command.is_empty())
        .cloned()
        .unwrap_or_else(|| vec![agent.to_string()]);
    argv.extend_from_slice(extra);
    argv
}

// ---------------------------------------------------------------------------
// claude-code
// ---------------------------------------------------------------------------

pub struct ClaudeExecutor;

/// Flags that mean the user is selecting or continuing a session themselves, so
/// we must not impose our own `--session-id`.
const CLAUDE_SESSION_FLAGS: [&str; 6] = [
    "--session-id",
    "-r",
    "--resume",
    "-c",
    "--continue",
    "--from-pr",
];

fn has_session_flag(argv: &[String]) -> bool {
    argv.iter().any(|arg| {
        CLAUDE_SESSION_FLAGS.iter().any(|flag| {
            // Match the bare flag (`--session-id`) and, for long flags, the
            // `=`-joined form (`--session-id=<id>`) that exact matching misses —
            // otherwise we'd inject a second `--session-id` alongside the user's.
            arg == flag
                || (flag.starts_with("--")
                    && arg
                        .strip_prefix(flag)
                        .is_some_and(|rest| rest.starts_with('=')))
        })
    })
}

impl Executor for ClaudeExecutor {
    fn info(&self) -> Option<AgentInfo> {
        Some(AgentInfo {
            name: "claude-code",
            has_native_log: true,
        })
    }

    fn backend(&self, _app_server: bool) -> Backend {
        Backend::Pty
    }

    /// Injects `--session-id <uuid>` so the on-disk transcript filename is known
    /// up front (deterministic log discovery at close). Skipped when the user
    /// already passed a session-affecting flag (see [`CLAUDE_SESSION_FLAGS`]).
    fn pty_argv(
        &self,
        resolved_argv: &[String],
        _proxy_url: &str,
    ) -> (Vec<String>, Option<String>) {
        if has_session_flag(resolved_argv) {
            return (resolved_argv.to_vec(), None);
        }
        let id = uuid::Uuid::new_v4().to_string();
        let mut prepared = resolved_argv.to_vec();
        prepared.push("--session-id".to_string());
        prepared.push(id.clone());
        (prepared, Some(id))
    }
}

// ---------------------------------------------------------------------------
// codex
// ---------------------------------------------------------------------------

pub struct CodexExecutor;

impl Executor for CodexExecutor {
    fn info(&self) -> Option<AgentInfo> {
        Some(AgentInfo {
            name: "codex",
            has_native_log: true,
        })
    }

    fn backend(&self, app_server: bool) -> Backend {
        if app_server {
            Backend::CodexAppServer
        } else {
            Backend::Pty
        }
    }

    /// Codex ignores `OPENAI_BASE_URL` for its built-in providers, so the PTY TUI
    /// gets the proxy provider overrides injected as argv (env redirection alone
    /// wouldn't route it through the proxy). No native session id.
    fn pty_argv(&self, resolved_argv: &[String], proxy_url: &str) -> (Vec<String>, Option<String>) {
        (codex::worker::tui_argv(resolved_argv, proxy_url), None)
    }

    fn app_server_argv(&self, resolved: &[String], proxy_url: &str) -> Vec<String> {
        codex::worker::app_server_argv(resolved, proxy_url)
    }

    fn metadata_kind(&self, app_server: bool) -> Option<&'static str> {
        app_server.then_some("codex-app-server")
    }
}

// ---------------------------------------------------------------------------
// gemini
// ---------------------------------------------------------------------------

pub struct GeminiExecutor;

impl Executor for GeminiExecutor {
    fn info(&self) -> Option<AgentInfo> {
        Some(AgentInfo {
            name: "gemini-cli",
            has_native_log: true,
        })
    }

    fn backend(&self, _app_server: bool) -> Backend {
        Backend::Pty
    }

    fn pty_argv(
        &self,
        resolved_argv: &[String],
        _proxy_url: &str,
    ) -> (Vec<String>, Option<String>) {
        (resolved_argv.to_vec(), None)
    }
}

// ---------------------------------------------------------------------------
// custom / unrecognized
// ---------------------------------------------------------------------------

pub struct CustomExecutor;

impl Executor for CustomExecutor {
    fn info(&self) -> Option<AgentInfo> {
        None
    }

    fn backend(&self, _app_server: bool) -> Backend {
        Backend::Pty
    }

    fn pty_argv(
        &self,
        resolved_argv: &[String],
        _proxy_url: &str,
    ) -> (Vec<String>, Option<String>) {
        (resolved_argv.to_vec(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn resolves_known_agents_with_capabilities() {
        assert_eq!(
            resolve_executor("claude").info().unwrap().name,
            "claude-code"
        );
        assert_eq!(resolve_executor("codex").info().unwrap().name, "codex");
        assert_eq!(
            resolve_executor("gemini").info().unwrap().name,
            "gemini-cli"
        );
        assert!(resolve_executor("claude").info().unwrap().has_native_log);
        assert!(resolve_executor("codex").info().unwrap().has_native_log);
        assert!(resolve_executor("gemini").info().unwrap().has_native_log);
    }

    #[test]
    fn custom_agent_is_unsupported() {
        // The resolved program name is not a selector, so it is not recognized.
        assert!(resolve_executor("claude-code").info().is_none());
        assert!(resolve_executor("bash").info().is_none());
        assert!(resolve_executor("").info().is_none());
    }

    #[test]
    fn codex_backend_follows_app_server_flag() {
        assert_eq!(CodexExecutor.backend(true), Backend::CodexAppServer);
        assert_eq!(CodexExecutor.backend(false), Backend::Pty);
        // Only codex honors the flag.
        assert_eq!(ClaudeExecutor.backend(true), Backend::Pty);
        assert_eq!(GeminiExecutor.backend(true), Backend::Pty);
        assert_eq!(CustomExecutor.backend(true), Backend::Pty);
    }

    #[test]
    fn codex_metadata_kind_only_for_app_server() {
        assert_eq!(CodexExecutor.metadata_kind(true), Some("codex-app-server"));
        assert_eq!(CodexExecutor.metadata_kind(false), None);
        assert_eq!(ClaudeExecutor.metadata_kind(true), None);
    }

    fn agent_commands() -> BTreeMap<String, Vec<String>> {
        BTreeMap::from([(
            "claude".to_string(),
            argv(&["pnpm", "dlx", "@anthropic-ai/claude-code"]),
        )])
    }

    #[test]
    fn launch_argv_uses_configured_command_and_appends_extra() {
        let resolved = launch_argv("claude", &argv(&["--model", "opus"]), &agent_commands());
        assert_eq!(
            resolved,
            argv(&[
                "pnpm",
                "dlx",
                "@anthropic-ai/claude-code",
                "--model",
                "opus"
            ])
        );
    }

    #[test]
    fn launch_argv_falls_back_to_bare_name_when_unconfigured() {
        let commands = agent_commands();
        // codex has no config entry → run the bare selector name.
        assert_eq!(launch_argv("codex", &[], &commands), argv(&["codex"]));
        // extra args are appended to the fallback too.
        assert_eq!(
            launch_argv("codex", &argv(&["--search"]), &commands),
            argv(&["codex", "--search"])
        );
        // an empty configured command is ignored in favor of the bare name.
        let empty = BTreeMap::from([("gemini".to_string(), Vec::new())]);
        assert_eq!(launch_argv("gemini", &[], &empty), argv(&["gemini"]));
    }

    #[test]
    fn resolution_then_session_injection_for_claude() {
        // The realistic spawn order: resolve the launch command, then transform
        // it for the PTY spawn (claude session id onto the resolved command).
        let resolved = launch_argv("claude", &[], &agent_commands());
        let (prepared, id) = ClaudeExecutor.pty_argv(&resolved, "http://127.0.0.1:9000");
        let id = id.expect("claude still gets a session id after resolution");
        assert_eq!(
            prepared,
            argv(&[
                "pnpm",
                "dlx",
                "@anthropic-ai/claude-code",
                "--session-id",
                &id
            ])
        );
    }

    #[test]
    fn injects_session_id_for_claude() {
        let (prepared, id) =
            ClaudeExecutor.pty_argv(&argv(&["claude", "--model", "opus"]), "http://p");
        let id = id.expect("a session id is chosen");
        assert_eq!(
            prepared,
            vec!["claude", "--model", "opus", "--session-id", &id]
        );
        assert!(uuid::Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn skips_injection_when_user_sets_a_session_flag() {
        for flag in [
            "--session-id",
            "-r",
            "--resume",
            "-c",
            "--continue",
            "--from-pr",
        ] {
            let (prepared, id) = ClaudeExecutor.pty_argv(&argv(&["claude", flag, "x"]), "http://p");
            assert_eq!(prepared, argv(&["claude", flag, "x"]));
            assert!(id.is_none(), "no id injected when {flag} present");
        }
    }

    #[test]
    fn skips_injection_for_equals_joined_session_flags() {
        for arg in ["--session-id=abc", "--resume=x", "--continue=y"] {
            let (prepared, id) = ClaudeExecutor.pty_argv(&argv(&["claude", arg]), "http://p");
            assert_eq!(
                prepared,
                argv(&["claude", arg]),
                "must not inject for {arg}"
            );
            assert!(id.is_none(), "no id injected when {arg} present");
        }
    }

    #[test]
    fn codex_pty_argv_injects_proxy_overrides_without_id() {
        let (prepared, id) = CodexExecutor.pty_argv(&argv(&["codex"]), "http://127.0.0.1:9000");
        assert!(id.is_none());
        // The provider overrides route the TUI through the proxy base URL.
        assert!(prepared.iter().any(|a| a == "model_provider=omniagent"));
        assert!(
            prepared
                .iter()
                .any(|a| a.contains("http://127.0.0.1:9000/v1"))
        );
    }

    #[test]
    fn other_agents_leave_argv_unchanged() {
        let (prepared, id) = GeminiExecutor.pty_argv(&argv(&["gemini"]), "http://p");
        assert_eq!(prepared, argv(&["gemini"]));
        assert!(id.is_none());

        let (prepared, id) = CustomExecutor.pty_argv(&argv(&["bash"]), "http://p");
        assert_eq!(prepared, argv(&["bash"]));
        assert!(id.is_none());
    }
}
