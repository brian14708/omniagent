//! Wire types for the CLI ↔ control-plane Phoenix channel protocol.
//!
//! Client→server events are mostly built inline as JSON. Two worth documenting:
//!
//! `pty_output_batch` coalesces a contiguous run of terminal-output chunks into
//! one frame (sent as a request; the reply's `last_client_sequence` acks and
//! trims the outbox). Each element keeps its own sequence for idempotent replay:
//!
//! ```json
//! { "events": [ {"data": "…", "sequence": 41}, {"data": "…", "sequence": 42} ] }
//! ```
//!
//! `session_close` is emitted once when a session's agent exits (after its
//! artifacts have been uploaded over HTTP):
//!
//! ```json
//! {
//!   "exit_code": 0,
//!   "agent": { "name": "claude-code", "supported": true },
//!   "artifacts": [ { "kind": "recording", "id": "…", "key": "…", "size": 1234 } ]
//! }
//! ```
//!
//! It is best-effort (non-replayable) and delivered before the channel leaves.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSessionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub cwd: String,
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredSession {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub argv: Vec<String>,
    #[serde(default)]
    pub last_client_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerCommand {
    PtyInput {
        data: String,
    },
    PtyResize {
        rows: u16,
        cols: u16,
    },
    ReviewDecision {
        id: String,
        decision: serde_json::Value,
    },
    FileRequest {
        path: String,
    },
    DiffRequest {
        path: String,
    },
    ListDir {
        path: String,
    },
    Shutdown,
    /// Daemon control: start a new agent session.
    SpawnAgent {
        /// Selected agent: a canonical name (`claude`/`codex`/`gemini`).
        agent: String,
        /// Extra args appended to the agent's resolved launch command.
        #[serde(default)]
        custom_command: Vec<String>,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        name: Option<String>,
        /// Drive codex via `codex app-server` (native, structured) instead of a PTY.
        #[serde(default)]
        app_server: bool,
        /// Optional model override applied to every request from this session.
        #[serde(default)]
        model: Option<String>,
        /// Allowed workspace root the agent runs under (preferred over `cwd`).
        #[serde(default)]
        workspace: Option<String>,
        /// Branch to use, or the new branch name when creating a worktree.
        #[serde(default)]
        branch: Option<String>,
        /// Create (or reuse) an isolated `git worktree` for `branch`.
        #[serde(default)]
        create_worktree: bool,
        /// Spawn in this existing linked worktree.
        #[serde(default)]
        worktree: Option<String>,
        /// Base ref a newly-created worktree branches from.
        #[serde(default)]
        base_branch: Option<String>,
    },
    /// Codex app-server: submit a user turn.
    CodexInput {
        text: String,
    },
    /// Codex app-server: interrupt the in-progress turn.
    CodexInterrupt,
    /// Daemon control: create a new project workspace under the local data dir,
    /// initialize it as a git repo, and add it to the allowlist.
    CreateWorkspace {
        name: String,
    },
}
