//! Workspace file access for the web UI's File and Diff views.
//!
//! All paths are sandboxed to the agent's workspace root: the requested path is
//! joined onto the canonical root, canonicalized, and rejected unless it stays
//! within the root. This blocks `..` traversal and symlink escapes.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Serialize;

/// Largest file the viewer will return inline.
const MAX_FILE_BYTES: u64 = 512 * 1024;

#[derive(Debug)]
pub enum FsError {
    Forbidden,
    NotFound,
    TooLarge,
    NotText,
    Io(String),
}

#[derive(Debug, Serialize)]
pub struct Entry {
    pub name: String,
    pub dir: bool,
}

#[derive(Debug, Serialize)]
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

/// Lists directory entries under `root`/`rel`, directories first then by name.
pub fn list_dir(root: &Path, rel: &str) -> Result<Vec<Entry>, FsError> {
    let dir = resolve_within(root, rel)?;
    if !dir.is_dir() {
        return Err(FsError::NotFound);
    }
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(|e| FsError::Io(e.to_string()))? {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name().to_string_lossy().to_string();
        if name == ".git" {
            continue;
        }
        let dir = entry.file_type().is_ok_and(|t| t.is_dir());
        entries.push(Entry { name, dir });
    }
    entries.sort_by(|a, b| b.dir.cmp(&a.dir).then_with(|| a.name.cmp(&b.name)));
    Ok(entries)
}

/// Reads a text file under `root`/`rel`, capped at [`MAX_FILE_BYTES`].
pub fn read_file(root: &Path, rel: &str) -> Result<String, FsError> {
    let file = resolve_within(root, rel)?;
    let meta = file.metadata().map_err(|_| FsError::NotFound)?;
    if !meta.is_file() {
        return Err(FsError::NotFound);
    }
    if meta.len() > MAX_FILE_BYTES {
        return Err(FsError::TooLarge);
    }
    let bytes = std::fs::read(&file).map_err(|e| FsError::Io(e.to_string()))?;
    String::from_utf8(bytes).map_err(|_| FsError::NotText)
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
    let Some(out) = git(root, &["status", "--porcelain"]) else {
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
    if Path::new(path)
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return None;
    }
    // Prefer diff against HEAD (covers staged + unstaged); fall back to the
    // worktree diff for repos without a HEAD commit yet.
    let head = git(root, &["diff", "HEAD", "--", path]).filter(|d| !d.trim().is_empty());
    head.or_else(|| git(root, &["diff", "--", path]))
}

fn git(root: &Path, args: &[&str]) -> Option<String> {
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
