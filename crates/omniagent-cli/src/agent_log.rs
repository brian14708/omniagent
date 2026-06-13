//! Locates an agent's *native* on-disk session log so it can be uploaded
//! verbatim as the raw `session_log` artifact.
//!
//! Because omniagent runs the agent on the host against the user's real config,
//! we do not pin the agent's config dir (that would relocate its auth); we
//! *honor* `CLAUDE_CONFIG_DIR` / `CODEX_HOME` and locate the log by either the
//! session id we injected (claude — deterministic) or recency (codex).

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::executor::AgentInfo;

/// Tolerance applied when matching a log file's mtime against the session start,
/// absorbing small clock differences between spawn and first write.
const MTIME_SKEW: Duration = Duration::from_secs(5);

/// Locates the agent's native on-disk session log, or `None` if the agent has
/// no native log here or it can't be found. The located log is uploaded
/// verbatim as the raw `session_log` artifact.
#[must_use]
pub fn locate_native_log(
    agent: AgentInfo,
    cwd: &Path,
    native_session_id: Option<&str>,
    since: SystemTime,
) -> Option<PathBuf> {
    match agent.name {
        "claude-code" => locate_claude_log(cwd, native_session_id, since),
        "codex" => locate_codex_log(since),
        _ => None,
    }
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

/// `$CLAUDE_CONFIG_DIR` if set, else `~/.claude`.
fn claude_base() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("CLAUDE_CONFIG_DIR") {
        return Some(PathBuf::from(dir));
    }
    home_dir().map(|home| home.join(".claude"))
}

/// `$CODEX_HOME` if set, else `~/.codex`.
fn codex_base() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("CODEX_HOME") {
        return Some(PathBuf::from(dir));
    }
    home_dir().map(|home| home.join(".codex"))
}

/// Claude encodes a project's cwd by replacing `/` and `.` with `-`.
fn encode_project_dir(cwd: &Path) -> String {
    cwd.to_string_lossy()
        .chars()
        .map(|c| if c == '/' || c == '.' { '-' } else { c })
        .collect()
}

fn locate_claude_log(
    cwd: &Path,
    native_session_id: Option<&str>,
    since: SystemTime,
) -> Option<PathBuf> {
    let dir = claude_base()?
        .join("projects")
        .join(encode_project_dir(cwd));

    // Deterministic when we injected the session id.
    if let Some(id) = native_session_id {
        let path = dir.join(format!("{id}.jsonl"));
        if path.is_file() {
            return Some(path);
        }
    }
    // Otherwise the newest transcript written during this session.
    newest_file(&dir, since, false, |path| {
        path.extension().is_some_and(|ext| ext == "jsonl")
    })
}

fn locate_codex_log(since: SystemTime) -> Option<PathBuf> {
    let sessions = codex_base()?.join("sessions");
    newest_file(&sessions, since, true, |path| {
        path.extension().is_some_and(|ext| ext == "jsonl")
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("rollout-"))
    })
}

/// Returns the most-recently-modified file under `root` (optionally recursing)
/// that satisfies `pred` and was modified no earlier than `since` (minus skew).
fn newest_file(
    root: &Path,
    since: SystemTime,
    recurse: bool,
    pred: impl Fn(&Path) -> bool,
) -> Option<PathBuf> {
    let cutoff = since.checked_sub(MTIME_SKEW).unwrap_or(since);
    let mut best: Option<(SystemTime, PathBuf)> = None;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_dir() {
                if recurse {
                    stack.push(path);
                }
                continue;
            }
            if !pred(&path) {
                continue;
            }
            let Ok(modified) = meta.modified() else {
                continue;
            };
            if modified < cutoff {
                continue;
            }
            if best.as_ref().is_none_or(|(best_t, _)| modified > *best_t) {
                best = Some((modified, path));
            }
        }
    }
    best.map(|(_, path)| path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_project_dir_like_claude() {
        assert_eq!(
            encode_project_dir(Path::new("/home/brianli/oa/omniagent")),
            "-home-brianli-oa-omniagent"
        );
        assert_eq!(
            encode_project_dir(Path::new("/home/u/.config/app")),
            "-home-u--config-app"
        );
    }
}
