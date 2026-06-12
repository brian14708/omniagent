//! Shared `git` shell-out helper.
//!
//! Centralizes the one way the CLI shells out to `git` (no `git2`/`gix`
//! dependency). Both the workspace file/diff viewer ([`crate::files`]) and the
//! workspace abstraction ([`crate::workspace`]) run git through here.

use std::path::Path;
use std::process::Command;

/// Runs `git -C <root> <args...>`, returning the raw output (the trailing
/// newline is kept) when its exit code satisfies `accept`, else `None`.
fn run(root: &Path, args: &[&str], accept: impl Fn(Option<i32>) -> bool) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .ok()?;
    accept(output.status.code()).then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Runs `git -C <root> <args...>`, returning stdout on success (exit 0), or
/// `None` on spawn failure or a non-zero exit. The trailing newline is kept.
#[must_use]
pub fn git(root: &Path, args: &[&str]) -> Option<String> {
    run(root, args, |code| code == Some(0))
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

/// Like [`git`], but also accepts exit code 1, which `git diff` uses to signal
/// "differences found" rather than an error. Returns `None` only on spawn
/// failure or an exit code greater than 1.
#[must_use]
pub fn git_diff(root: &Path, args: &[&str]) -> Option<String> {
    run(root, args, |code| matches!(code, Some(0 | 1)))
}
