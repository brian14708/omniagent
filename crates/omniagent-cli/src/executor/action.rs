//! Chainable execution steps for one session.
//!
//! A session can run a small ordered chain of steps around its coding agent —
//! `setup script(s) → coding agent → cleanup script` — modeled as a singly
//! linked list of [`ExecutorAction`]s (mirroring vibe-kanban's `ExecutorAction`,
//! narrowed to omniagent's needs). The supervisor walks the chain: leading
//! [`ActionType::Script`] steps run to completion inline before the long-lived
//! [`ActionType::CodingAgent`] step, and any trailing scripts run after the agent
//! exits. The chain is backend-agnostic — it spawns whatever worker the agent's
//! executor produces — so a future structured backend needs no changes here.

/// A shell command run as a session step, streamed to the session's terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptRequest {
    /// The shell snippet, run via `sh -lc <script>` in the session's cwd.
    pub script: String,
    /// Short label for logs/telemetry (e.g. `"setup"` / `"cleanup"`).
    pub label: &'static str,
}

/// One step in a session's chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionType {
    /// Run a shell script to completion (streamed to the terminal).
    Script(ScriptRequest),
    /// Spawn the interactive coding agent (dispatched via the session's
    /// [`Executor`](super::Executor)).
    CodingAgent,
}

/// A step plus the rest of the chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorAction {
    pub typ: ActionType,
    pub next: Option<Box<Self>>,
}

impl ExecutorAction {
    #[must_use]
    pub const fn new(typ: ActionType) -> Self {
        Self { typ, next: None }
    }

    /// Appends `action` at the tail of the chain.
    #[must_use]
    pub fn append(mut self, action: Self) -> Self {
        match self.next {
            Some(next) => self.next = Some(Box::new(next.append(action))),
            None => self.next = Some(Box::new(action)),
        }
        self
    }

    /// The next step, if any.
    #[must_use]
    pub fn next(&self) -> Option<&Self> {
        self.next.as_deref()
    }

    /// Builds the standard chain from optional setup/cleanup scripts:
    /// `setup? → CodingAgent → cleanup?`.
    #[must_use]
    pub fn session_chain(setup: Option<String>, cleanup: Option<String>) -> Self {
        let agent = Self::new(ActionType::CodingAgent);
        let agent = match cleanup.filter(|s| !s.trim().is_empty()) {
            Some(script) => agent.append(Self::new(ActionType::Script(ScriptRequest {
                script,
                label: "cleanup",
            }))),
            None => agent,
        };
        match setup.filter(|s| !s.trim().is_empty()) {
            Some(script) => Self::new(ActionType::Script(ScriptRequest {
                script,
                label: "setup",
            }))
            .append(agent),
            None => agent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(action: &ExecutorAction) -> Vec<String> {
        let mut out = Vec::new();
        let mut cur = Some(action);
        while let Some(node) = cur {
            out.push(match &node.typ {
                ActionType::Script(s) => s.label.to_string(),
                ActionType::CodingAgent => "agent".to_string(),
            });
            cur = node.next();
        }
        out
    }

    #[test]
    fn chain_with_setup_and_cleanup() {
        let chain = ExecutorAction::session_chain(
            Some("npm i".to_string()),
            Some("rm -rf tmp".to_string()),
        );
        assert_eq!(labels(&chain), ["setup", "agent", "cleanup"]);
    }

    #[test]
    fn chain_without_scripts_is_just_the_agent() {
        let chain = ExecutorAction::session_chain(None, None);
        assert_eq!(labels(&chain), ["agent"]);
    }

    #[test]
    fn blank_scripts_are_ignored() {
        let chain = ExecutorAction::session_chain(Some("   ".to_string()), Some(String::new()));
        assert_eq!(labels(&chain), ["agent"]);
    }

    #[test]
    fn setup_only_and_cleanup_only() {
        let setup_only = ExecutorAction::session_chain(Some("a".to_string()), None);
        assert_eq!(labels(&setup_only), ["setup", "agent"]);
        let cleanup_only = ExecutorAction::session_chain(None, Some("b".to_string()));
        assert_eq!(labels(&cleanup_only), ["agent", "cleanup"]);
    }
}
