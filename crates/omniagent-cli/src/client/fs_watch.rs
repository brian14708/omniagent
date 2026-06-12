//! Per-session filesystem watcher that pushes the git changed-files list to the
//! control plane whenever the workspace actually changes.
//!
//! The control plane (`LiveView`) has no view of the filesystem, so without this
//! it can only guess when to refresh the "Changes" panel. Here we watch the
//! session's worktree with a debounced `notify` watcher and, on any relevant
//! change, recompute `git status` and push an ephemeral `fs_change` event — the
//! same delivery path the on-demand `diff_response` uses.

use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use notify_debouncer_full::DebounceEventResult;
use notify_debouncer_full::new_debouncer;
use notify_debouncer_full::notify::RecursiveMode;
use serde_json::json;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::JoinHandle;
use tracing::warn;

use super::ChannelHandle;
use crate::files::{self, ChangedFile};

/// Debounce window: coalesce the burst of writes an editor/agent makes into one
/// recompute.
const DEBOUNCE: Duration = Duration::from_millis(250);

/// Build/dependency dirs that are never interesting and can churn heavily. Git
/// status would ignore them anyway; skipping here avoids waking the watcher.
const ALWAYS_SKIP: &[&str] = &[
    "node_modules",
    "target",
    "_build",
    "deps",
    ".elixir_ls",
    "dist",
    ".next",
];

/// Spawns the watcher task for `workspace`, pushing `fs_change` over `channel`.
/// Abort the returned handle to stop watching (drops the watcher).
pub fn spawn_fs_watcher(channel: ChannelHandle, workspace: PathBuf) -> JoinHandle<()> {
    tokio::spawn(async move { run_watcher(channel, workspace).await })
}

async fn run_watcher(channel: ChannelHandle, workspace: PathBuf) {
    // Last list we pushed, so identical recomputes (a save that doesn't change
    // git status, a touch, a repeated save) don't spam the UI.
    let mut last: Option<Vec<ChangedFile>> = None;

    // Prime any already-attached viewer before the first edit arrives.
    push_changes(&channel, &workspace, &mut last).await;

    let gitignore = build_gitignore(&workspace);
    let (tx, mut rx) = unbounded_channel::<Vec<PathBuf>>();

    let mut debouncer = match new_debouncer(DEBOUNCE, None, move |res: DebounceEventResult| {
        if let Ok(events) = res {
            let paths: Vec<PathBuf> = events.into_iter().flat_map(|e| e.event.paths).collect();
            if !paths.is_empty() {
                let _ = tx.send(paths);
            }
        }
    }) {
        Ok(debouncer) => debouncer,
        Err(err) => {
            warn!(?err, "fs watcher: failed to create debouncer");
            return;
        }
    };

    if let Err(err) = debouncer.watch(&workspace, RecursiveMode::Recursive) {
        warn!(?err, "fs watcher: failed to watch workspace");
        return;
    }

    // Holding `debouncer` here keeps the watcher alive; when this task is
    // aborted the future drops and the watcher stops.
    while let Some(paths) = rx.recv().await {
        if paths
            .iter()
            .any(|p| is_relevant(p, &workspace, gitignore.as_ref()))
        {
            push_changes(&channel, &workspace, &mut last).await;
        }
    }
}

async fn push_changes(
    channel: &ChannelHandle,
    workspace: &Path,
    last: &mut Option<Vec<ChangedFile>>,
) {
    let ws = workspace.to_path_buf();
    match tokio::task::spawn_blocking(move || files::git_diff(&ws, None)).await {
        Ok(diff) => {
            if last.as_ref() == Some(&diff.files) {
                return;
            }
            channel.push("fs_change", json!({ "files": &diff.files }));
            *last = Some(diff.files);
        }
        Err(err) => warn!(?err, "fs watcher: git status task failed"),
    }
}

/// Loads the workspace's root `.gitignore` as a perf filter. Best-effort: a
/// missing or malformed file just means no extra filtering.
fn build_gitignore(workspace: &Path) -> Option<Gitignore> {
    let mut builder = GitignoreBuilder::new(workspace);
    builder.add(workspace.join(".gitignore"));
    builder.build().ok()
}

/// Decides whether a changed path should trigger a `git status` recompute.
fn is_relevant(path: &Path, workspace: &Path, gitignore: Option<&Gitignore>) -> bool {
    let Ok(rel) = path.strip_prefix(workspace) else {
        return false;
    };

    // Inside `.git`, only HEAD / logs/HEAD matter — they signal commits and
    // checkouts, which change `git status`. Everything else (index, locks,
    // objects) is git's own churn and would create a feedback loop.
    if let Some(Component::Normal(first)) = rel.components().next()
        && first == ".git"
    {
        // Compare path-wise (not string-wise) so separators match on every
        // platform.
        let tail: PathBuf = rel.components().skip(1).collect();
        return tail == Path::new("HEAD") || tail == Path::new("logs/HEAD");
    }

    // Heavy build/dependency dirs anywhere in the path.
    for component in rel.components() {
        if let Component::Normal(name) = component
            && ALWAYS_SKIP.iter().any(|skip| name == *skip)
        {
            return false;
        }
    }

    // Gitignored paths are noise — `git status` won't report them anyway.
    if let Some(gitignore) = gitignore
        && gitignore.matched(rel, false).is_ignore()
    {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ignore_target() -> Gitignore {
        let mut builder = GitignoreBuilder::new("/ws");
        builder.add_line(None, "ignored.txt").unwrap();
        builder.build().unwrap()
    }

    #[test]
    fn accepts_a_source_file() {
        assert!(is_relevant(
            Path::new("/ws/src/main.rs"),
            Path::new("/ws"),
            None
        ));
    }

    #[test]
    fn rejects_git_internals_but_accepts_head() {
        let ws = Path::new("/ws");
        assert!(!is_relevant(Path::new("/ws/.git/index"), ws, None));
        assert!(!is_relevant(Path::new("/ws/.git/objects/ab/cd"), ws, None));
        assert!(is_relevant(Path::new("/ws/.git/HEAD"), ws, None));
        assert!(is_relevant(Path::new("/ws/.git/logs/HEAD"), ws, None));
    }

    #[test]
    fn rejects_build_dirs_and_gitignored() {
        let ws = Path::new("/ws");
        assert!(!is_relevant(Path::new("/ws/node_modules/x/y.js"), ws, None));
        assert!(!is_relevant(Path::new("/ws/target/debug/bin"), ws, None));
        let gi = ignore_target();
        assert!(!is_relevant(Path::new("/ws/ignored.txt"), ws, Some(&gi)));
        assert!(is_relevant(Path::new("/ws/kept.txt"), ws, Some(&gi)));
    }

    #[test]
    fn rejects_paths_outside_workspace() {
        assert!(!is_relevant(
            Path::new("/other/file.rs"),
            Path::new("/ws"),
            None
        ));
    }
}
