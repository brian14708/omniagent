//! Workspace file access for the central UI's file and diff requests.
//!
//! All paths are sandboxed to the agent's workspace root: the requested path is
//! joined onto the canonical root, canonicalized, and rejected unless it stays
//! within the root. This blocks `..` traversal and symlink escapes.

use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::git::git;

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

impl std::fmt::Display for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Forbidden => f.write_str("path outside workspace"),
            Self::NotFound => f.write_str("not found"),
            Self::TooLarge => f.write_str("file too large"),
            Self::NotText => f.write_str("not a text file"),
            Self::Io(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for FsError {}

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

/// One entry in a directory listing returned to the file browser.
#[derive(Debug, Serialize)]
pub struct DirEntry {
    pub name: String,
    pub dir: bool,
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

/// Lists the entries of a directory under `root`/`rel` (empty `rel` = root).
/// Directories sort before files, each alphabetically; `.git` is hidden.
pub fn list_dir(root: &Path, rel: &str) -> Result<Vec<DirEntry>, FsError> {
    let dir = resolve_within(root, rel)?;
    let meta = dir.metadata().map_err(|_| FsError::NotFound)?;
    if !meta.is_dir() {
        return Err(FsError::NotFound);
    }
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(|e| FsError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| FsError::Io(e.to_string()))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == ".git" {
            continue;
        }
        // `file_type()` avoids a stat for most platforms; fall back to false
        // (treat as a file) when it can't be determined.
        let is_dir = entry.file_type().is_ok_and(|t| t.is_dir());
        entries.push(DirEntry { name, dir: is_dir });
    }
    entries.sort_by(|a, b| b.dir.cmp(&a.dir).then_with(|| a.name.cmp(&b.name)));
    Ok(entries)
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
    fn list_dir_sorts_dirs_first_then_alpha_and_hides_git() {
        let root = temp_root();
        fs::create_dir_all(root.join(".git")).unwrap();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::create_dir_all(root.join("assets")).unwrap();
        fs::write(root.join("README.md"), b"hi").unwrap();
        fs::write(root.join("Cargo.toml"), b"[package]").unwrap();

        let entries = list_dir(&root, "").unwrap();
        let names: Vec<_> = entries.iter().map(|e| e.name.as_str()).collect();
        // Directories first (alpha), then files (alpha); `.git` excluded.
        assert_eq!(names, vec!["assets", "src", "Cargo.toml", "README.md"]);
        assert!(entries[0].dir && entries[1].dir);
        assert!(!entries[2].dir && !entries[3].dir);

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn list_dir_lists_a_subdirectory() {
        let root = temp_root();
        fs::create_dir_all(root.join("src/inner")).unwrap();
        fs::write(root.join("src/main.rs"), b"fn main() {}").unwrap();

        let entries = list_dir(&root, "src").unwrap();
        let names: Vec<_> = entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["inner", "main.rs"]);

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn list_dir_rejects_parent_traversal() {
        let root = temp_root();
        fs::create_dir_all(root.join("sub")).unwrap();
        assert!(matches!(list_dir(&root, "../"), Err(FsError::Forbidden)));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn list_dir_errors_on_a_file_path() {
        let root = temp_root();
        fs::write(root.join("a.txt"), b"x").unwrap();
        assert!(matches!(list_dir(&root, "a.txt"), Err(FsError::NotFound)));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn list_dir_errors_on_missing_dir() {
        let root = temp_root();
        assert!(matches!(list_dir(&root, "nope"), Err(FsError::NotFound)));
        fs::remove_dir_all(&root).unwrap();
    }
}
