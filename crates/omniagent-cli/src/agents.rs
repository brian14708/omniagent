//! Agent identification and capability detection.
//!
//! The agent a session supervises is otherwise opaque — it is just an `argv`
//! the user supplied and we spawn verbatim. At session close we need to know
//! (a) whether it is an agent omniagent recognizes and (b) whether we can build
//! an ATIF trajectory for it. This mirrors harbor's `AgentName` registry plus
//! the `SUPPORTS_ATIF` capability flag declared on `BaseAgent`: an agent without
//! that capability simply produces no `trajectory.json`.

use std::collections::BTreeMap;
use std::path::Path;

/// A recognized coding agent and its capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentInfo {
    /// Canonical, harbor-compatible agent name (e.g. `"claude-code"`).
    pub name: &'static str,
    /// Whether an ATIF trajectory can be produced for this agent.
    pub supports_atif: bool,
}

/// Detects the agent from its command line by matching the program basename.
///
/// Returns `None` for an unrecognized binary: the session is still recorded and
/// its terminal recording uploaded, but no ATIF trajectory is built (mirroring
/// harbor, where an agent without `SUPPORTS_ATIF` produces no `trajectory.json`).
#[must_use]
pub fn detect_agent(argv: &[String]) -> Option<AgentInfo> {
    let program = argv.first()?;
    let stem = Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(program.as_str());
    let stem = stem.strip_suffix(".exe").unwrap_or(stem);
    match stem {
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

/// Expands a bare agent name (e.g. `claude`) into its configured launch command
/// (e.g. `pnpm dlx @anthropic-ai/claude-code`), preserving any trailing args.
///
/// `commands` is the daemon's `agent_commands` map (see [`crate::config`]),
/// keyed by bare agent name. Only an exact, bare program name is expanded: an
/// argv whose program is an explicit path (`/usr/local/bin/claude`) or an
/// already-resolved command is returned unchanged, so a user who has the agent
/// installed keeps control. Detection ([`detect_agent`]) must be run on the
/// original argv before calling this, since it matches the basename `claude`
/// rather than the resolved `pnpm`.
#[must_use]
pub fn resolve_argv(argv: &[String], commands: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    let Some(program) = argv.first() else {
        return argv.to_vec();
    };
    match commands.get(program) {
        Some(command) if !command.is_empty() => {
            let mut resolved = command.clone();
            resolved.extend_from_slice(&argv[1..]);
            resolved
        }
        _ => argv.to_vec(),
    }
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
    fn detects_known_agents_by_basename() {
        assert_eq!(
            detect_agent(&argv(&["claude"])).unwrap().name,
            "claude-code"
        );
        assert_eq!(
            detect_agent(&argv(&["codex", "--flag"])).unwrap().name,
            "codex"
        );
        assert_eq!(detect_agent(&argv(&["gemini"])).unwrap().name, "gemini-cli");
    }

    #[test]
    fn matches_through_absolute_paths_and_exe_suffix() {
        assert_eq!(
            detect_agent(&argv(&["/usr/local/bin/claude"]))
                .unwrap()
                .name,
            "claude-code"
        );
        assert_eq!(detect_agent(&argv(&["codex.exe"])).unwrap().name, "codex");
    }

    #[test]
    fn unknown_agent_is_unsupported() {
        assert_eq!(detect_agent(&argv(&["bash"])), None);
        assert_eq!(detect_agent(&[]), None);
    }

    fn agent_commands() -> BTreeMap<String, Vec<String>> {
        BTreeMap::from([(
            "claude".to_string(),
            argv(&["pnpm", "dlx", "@anthropic-ai/claude-code"]),
        )])
    }

    #[test]
    fn resolves_bare_name_to_configured_command_keeping_args() {
        let resolved = resolve_argv(&argv(&["claude", "--model", "opus"]), &agent_commands());
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
    fn leaves_unmapped_or_explicit_programs_unchanged() {
        let commands = agent_commands();
        // an unmapped name (no config entry) is spawned verbatim
        assert_eq!(resolve_argv(&argv(&["codex"]), &commands), argv(&["codex"]));
        // an explicit path is not a bare name, so it is never expanded
        assert_eq!(
            resolve_argv(&argv(&["/usr/local/bin/claude"]), &commands),
            argv(&["/usr/local/bin/claude"])
        );
        // empty argv stays empty
        assert!(resolve_argv(&[], &commands).is_empty());
    }

    #[test]
    fn detection_then_resolution_preserves_session_injection() {
        // The realistic spawn order: detect on the original argv, resolve the
        // program, then inject the claude session id onto the resolved command.
        let original = argv(&["claude"]);
        let agent = detect_agent(&original);
        let resolved = resolve_argv(&original, &agent_commands());
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
        assert!(detect_agent(&argv(&["claude"])).unwrap().supports_atif);
        assert!(detect_agent(&argv(&["codex"])).unwrap().supports_atif);
        assert!(detect_agent(&argv(&["gemini"])).unwrap().supports_atif);
    }

    #[test]
    fn injects_session_id_for_claude() {
        let claude = detect_agent(&argv(&["claude"]));
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
        let claude = detect_agent(&argv(&["claude"]));
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
        let claude = detect_agent(&argv(&["claude"]));
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
        let codex = detect_agent(&argv(&["codex"]));
        let (prepared, id) = prepare_argv(codex, &argv(&["codex"]));
        assert_eq!(prepared, argv(&["codex"]));
        assert!(id.is_none());

        let (prepared, id) = prepare_argv(None, &argv(&["bash"]));
        assert_eq!(prepared, argv(&["bash"]));
        assert!(id.is_none());
    }
}
