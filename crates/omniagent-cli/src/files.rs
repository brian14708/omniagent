//! Workspace diff access for the central UI's diff requests.
//!
//! All paths are sandboxed to the agent's workspace root: the requested path is
//! joined onto the canonical root, canonicalized, and rejected unless it stays
//! within the root. This blocks `..` traversal and symlink escapes.

use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::git::git;

#[derive(Debug)]
pub enum FsError {
    Forbidden,
    NotFound,
    Io(String),
}

impl std::fmt::Display for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Forbidden => f.write_str("path outside workspace"),
            Self::NotFound => f.write_str("not found"),
            Self::Io(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for FsError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChangedFile {
    pub path: String,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct DiffResult {
    pub files: Vec<ChangedFile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff: Option<String>,
}

/// Resolves `rel` under `root`, ensuring the result stays inside `root`.
fn resolve_within(root: &Path, rel: &str) -> Result<PathBuf, FsError> {
    let rel = rel.trim_start_matches('/');
    // Reject obvious traversal before touching the filesystem.
    if Path::new(rel)
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(FsError::Forbidden);
    }
    let root = root
        .canonicalize()
        .map_err(|e| FsError::Io(e.to_string()))?;
    let joined = if rel.is_empty() {
        root.clone()
    } else {
        root.join(rel)
    };
    let canonical = joined.canonicalize().map_err(|_| FsError::NotFound)?;
    if !canonical.starts_with(&root) {
        return Err(FsError::Forbidden);
    }
    Ok(canonical)
}

/// Collects the agent's changed files (and optionally one file's unified diff)
/// from git. Degrades to an empty result when the workspace is not a repo.
pub fn git_diff(root: &Path, path: Option<&str>) -> DiffResult {
    let files = git_status(root);
    let diff = match path {
        Some(p) if !p.is_empty() => git_file_diff(root, p),
        _ => None,
    };
    DiffResult { files, diff }
}

fn git_status(root: &Path) -> Vec<ChangedFile> {
    // `core.quotePath=false` keeps non-ASCII paths literal; git would otherwise
    // C-quote them (e.g. `"caf\303\251.txt"`), and that quoted string would
    // never match when fed back to `git diff` as a pathspec.
    let Some(out) = git(
        root,
        &["-c", "core.quotePath=false", "status", "--porcelain"],
    ) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for line in out.lines() {
        if line.len() < 4 {
            continue;
        }
        let status = line[..2].trim().to_string();
        let mut path = line[3..].to_string();
        // Renames render as "old -> new"; keep the new path.
        if let Some((_, new)) = path.split_once(" -> ") {
            path = new.to_string();
        }
        files.push(ChangedFile { path, status });
    }
    files
}

fn git_file_diff(root: &Path, path: &str) -> Option<String> {
    // Reject traversal and absolute paths: the `--no-index` fallback below
    // treats `path` as a literal filesystem path, so an absolute path or one
    // escaping the workspace would let the diff viewer read arbitrary files.
    if Path::new(path).components().any(|c| {
        matches!(
            c,
            std::path::Component::ParentDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_)
        )
    }) {
        return None;
    }
    // Prefer diff against HEAD (covers staged + unstaged); fall back to the
    // worktree diff for repos without a HEAD commit yet. `--no-ext-diff`/
    // `--no-color` defeat any external diff driver or colour the user has
    // configured globally, so we always get plain unified-diff text.
    let tracked = git(
        root,
        &["diff", "--no-ext-diff", "--no-color", "HEAD", "--", path],
    )
    .filter(|d| !d.trim().is_empty())
    .or_else(|| {
        git(root, &["diff", "--no-ext-diff", "--no-color", "--", path])
            .filter(|d| !d.trim().is_empty())
    });
    if tracked.is_some() {
        return tracked;
    }
    // Untracked/new files don't appear in `git diff`; diff against the null
    // device so the viewer shows the whole file as additions. `--no-index`
    // returns exit code 1 when the files differ, so go through `git_diff`.
    //
    // Unlike the pathspec-confined branches above, `--no-index` reads `path`
    // straight from the filesystem, so the component check at the top isn't
    // enough — a symlinked directory component could still escape the workspace.
    // Apply the same canonicalized containment guard the rest of this module
    // uses before handing the path to git.
    if resolve_within(root, path).is_err() {
        return None;
    }
    let null = if cfg!(windows) { "NUL" } else { "/dev/null" };
    crate::git::git_diff(
        root,
        &[
            "diff",
            "--no-ext-diff",
            "--no-color",
            "--no-index",
            "--",
            null,
            path,
        ],
    )
    .filter(|d| !d.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_root() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("oa-files-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn diff_shows_untracked_file_as_additions() {
        let root = temp_root();
        fs::write(root.join("new.txt"), b"hello\nworld\n").unwrap();

        let diff = git_diff(&root, Some("new.txt"))
            .diff
            .expect("untracked file should yield an additions diff");
        assert!(diff.contains("+hello"), "diff was: {diff}");
        assert!(diff.contains("+world"), "diff was: {diff}");

        fs::remove_dir_all(&root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn diff_rejects_symlinked_dir_escape() {
        use std::os::unix::fs::symlink;

        // A symlinked directory component whose target is outside the workspace
        // must not let the `--no-index` fallback read files from outside it.
        let root = temp_root();
        let outside = temp_root();
        fs::write(outside.join("secret.txt"), b"top secret\n").unwrap();
        symlink(&outside, root.join("link")).unwrap();

        assert!(
            git_file_diff(&root, "link/secret.txt").is_none(),
            "symlinked-dir escape should be rejected"
        );

        fs::remove_dir_all(&root).unwrap();
        fs::remove_dir_all(&outside).unwrap();
    }
}
