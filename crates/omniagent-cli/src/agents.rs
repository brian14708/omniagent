//! Agent identification and capability detection.
//!
//! The user selects which agent a session supervises by its canonical name
//! (`claude`/`codex`/`gemini`); we don't infer it from the spawned `argv`. The
//! name drives (a) whether omniagent recognizes the agent and (b) whether we can
//! build an ATIF trajectory for it. This mirrors harbor's `AgentName` registry
//! plus the `SUPPORTS_ATIF` capability flag declared on `BaseAgent`: an agent
//! without that capability simply produces no `trajectory.json`.

use std::collections::BTreeMap;

/// A recognized coding agent and its capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentInfo {
    /// Canonical, harbor-compatible agent name (e.g. `"claude-code"`).
    pub name: &'static str,
    /// Whether an ATIF trajectory can be produced for this agent.
    pub supports_atif: bool,
}

/// Looks up an agent by the selector the user picked (`claude`/`codex`/`gemini`).
///
/// Returns `None` for an unrecognized selector: the session is still recorded and
/// its terminal recording uploaded, but no ATIF trajectory is built (mirroring
/// harbor, where an agent without `SUPPORTS_ATIF` produces no `trajectory.json`).
#[must_use]
pub fn agent_info(agent: &str) -> Option<AgentInfo> {
    match agent {
        "claude" => Some(AgentInfo {
            name: "claude-code",
            supports_atif: true,
        }),
        "codex" => Some(AgentInfo {
            name: "codex",
            supports_atif: true,
        }),
        "gemini" => Some(AgentInfo {
            name: "gemini-cli",
            supports_atif: true,
        }),
        _ => None,
    }
}

/// Builds the argv to spawn for the selected agent: its configured launch command
/// (e.g. `pnpm dlx @anthropic-ai/claude-code`) followed by the user's `extra`
/// args.
///
/// `commands` is the daemon's `agent_commands` map (see [`crate::config`]), keyed
/// by bare agent name. An agent with no configured command falls back to running
/// the bare name itself (`codex` → `["codex"]`), so an operator who has the agent
/// on `PATH` needs no configuration.
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

/// Prepares the argv to actually spawn, returning it alongside the agent's
/// native session id when we set one.
///
/// For claude-code we inject `--session-id <uuid>` so the on-disk transcript
/// filename is known up front (deterministic log discovery at close). We skip
/// injection when the user already passed a session-affecting flag (see
/// [`CLAUDE_SESSION_FLAGS`]). All other agents are returned unchanged with no id.
#[must_use]
pub fn prepare_argv(agent: Option<AgentInfo>, argv: &[String]) -> (Vec<String>, Option<String>) {
    let is_claude = matches!(agent, Some(info) if info.name == "claude-code");
    if is_claude && !has_session_flag(argv) {
        let id = uuid::Uuid::new_v4().to_string();
        let mut prepared = argv.to_vec();
        prepared.push("--session-id".to_string());
        prepared.push(id.clone());
        (prepared, Some(id))
    } else {
        (argv.to_vec(), None)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn looks_up_known_agents_by_name() {
        assert_eq!(agent_info("claude").unwrap().name, "claude-code");
        assert_eq!(agent_info("codex").unwrap().name, "codex");
        assert_eq!(agent_info("gemini").unwrap().name, "gemini-cli");
    }

    #[test]
    fn unknown_agent_is_unsupported() {
        // The resolved program name is not a selector, so it is not recognized.
        assert_eq!(agent_info("claude-code"), None);
        assert_eq!(agent_info("bash"), None);
        assert_eq!(agent_info(""), None);
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
        // The realistic spawn order: resolve the launch command for the selected
        // agent, then inject the claude session id onto the resolved command.
        let agent = agent_info("claude");
        let resolved = launch_argv("claude", &[], &agent_commands());
        let (prepared, id) = prepare_argv(agent, &resolved);
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
    fn known_agents_support_atif() {
        assert!(agent_info("claude").unwrap().supports_atif);
        assert!(agent_info("codex").unwrap().supports_atif);
        assert!(agent_info("gemini").unwrap().supports_atif);
    }

    #[test]
    fn injects_session_id_for_claude() {
        let claude = agent_info("claude");
        let (prepared, id) = prepare_argv(claude, &argv(&["claude", "--model", "opus"]));
        let id = id.expect("a session id is chosen");
        assert_eq!(
            prepared,
            vec!["claude", "--model", "opus", "--session-id", &id]
        );
        assert!(uuid::Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn skips_injection_when_user_sets_a_session_flag() {
        let claude = agent_info("claude");
        for flag in [
            "--session-id",
            "-r",
            "--resume",
            "-c",
            "--continue",
            "--from-pr",
        ] {
            let (prepared, id) = prepare_argv(claude, &argv(&["claude", flag, "x"]));
            assert_eq!(prepared, argv(&["claude", flag, "x"]));
            assert!(id.is_none(), "no id injected when {flag} present");
        }
    }

    #[test]
    fn skips_injection_for_equals_joined_session_flags() {
        let claude = agent_info("claude");
        for arg in ["--session-id=abc", "--resume=x", "--continue=y"] {
            let (prepared, id) = prepare_argv(claude, &argv(&["claude", arg]));
            assert_eq!(
                prepared,
                argv(&["claude", arg]),
                "must not inject for {arg}"
            );
            assert!(id.is_none(), "no id injected when {arg} present");
        }
    }

    #[test]
    fn leaves_other_agents_unchanged() {
        let codex = agent_info("codex");
        let (prepared, id) = prepare_argv(codex, &argv(&["codex"]));
        assert_eq!(prepared, argv(&["codex"]));
        assert!(id.is_none());

        let (prepared, id) = prepare_argv(None, &argv(&["bash"]));
        assert_eq!(prepared, argv(&["bash"]));
        assert!(id.is_none());
    }
}
