//! Shared `git` shell-out helper.
//!
//! Centralizes the one way the CLI shells out to `git` (no `git2`/`gix`
//! dependency). Both the workspace file/diff viewer ([`crate::files`]) and the
//! workspace abstraction ([`crate::workspace`]) run git through here.

use std::path::Path;
use std::process::Command;

/// Runs `git -C <root> <args...>`, returning stdout on success (exit 0), or
/// `None` on spawn failure or a non-zero exit. The trailing newline is kept.
#[must_use]
pub fn git(root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Like [`git`], but returns the first non-empty trimmed line of stdout.
#[must_use]
pub fn git_line(root: &Path, args: &[&str]) -> Option<String> {
    git(root, args).and_then(|out| {
        out.lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .map(ToString::to_string)
    })
}
