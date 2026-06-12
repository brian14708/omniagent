//! Workspace abstraction: classify a path (plain dir / git repo / git worktree),
//! expose its git info, and resolve a spawn selection to an effective working
//! directory — optionally creating an isolated `git worktree` so concurrent
//! agents don't collide.
//!
//! All git access goes through [`crate::git`] (shell-outs; no `git2`/`gix`).

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow};
use serde::Serialize;

use crate::git::{git, git_line};
use crate::session::omniagent_data_dir;

/// How a path relates to git.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceKind {
    /// Not inside a git working tree.
    Plain,
    /// A git repository's main working tree.
    GitRepo,
    /// A linked git worktree (created via `git worktree add`).
    GitWorktree,
}

/// One entry from `git worktree list`.
#[derive(Debug, Clone, Serialize)]
pub struct WorktreeEntry {
    pub path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// Git details for a workspace that lives inside a repository.
#[derive(Debug, Clone, Serialize)]
pub struct GitInfo {
    /// Canonical working-tree root for the selected path.
    pub toplevel: PathBuf,
    /// Canonical git common dir (shared across a repo's worktrees).
    pub common_dir: PathBuf,
    /// Current branch, or `None` when HEAD is detached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_branch: Option<String>,
    /// Local branches (`refs/heads/*`).
    pub branches: Vec<String>,
    /// All worktrees of the repository (main worktree listed first).
    pub worktrees: Vec<WorktreeEntry>,
}

/// A classified workspace path.
#[derive(Debug, Clone, Serialize)]
pub struct Workspace {
    /// Canonical input path.
    pub path: PathBuf,
    pub kind: WorkspaceKind,
    /// Git details, `None` for [`WorkspaceKind::Plain`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git: Option<GitInfo>,
}

/// How a spawn selects its effective working directory.
#[derive(Debug, Clone)]
pub enum WorktreeMode {
    /// Run in the selected path as-is.
    InPlace,
    /// Run in an existing linked worktree.
    Existing(PathBuf),
    /// Create (or reuse) an isolated worktree for `branch`, off `base`.
    Create {
        branch: String,
        base: Option<String>,
    },
}

/// The outcome of resolving a spawn selection.
#[derive(Debug, Clone)]
pub struct ResolvedWorkspace {
    /// Effective working directory to spawn the agent in.
    pub cwd: PathBuf,
    /// Canonical main-repo toplevel — the anchor for allowlist authorization.
    pub origin_root: PathBuf,
    /// Set to the worktree path iff this resolution created it (for cleanup).
    pub created_worktree: Option<PathBuf>,
}

/// Classifies `path`. Plain directories (and unreadable git output) yield
/// [`WorkspaceKind::Plain`]; never errors on a non-repo.
///
/// # Errors
///
/// Returns an error only if `path` cannot be canonicalized.
pub fn detect(path: &Path) -> Result<Workspace> {
    let canonical = std::fs::canonicalize(path)
        .with_context(|| format!("cannot resolve path {}", path.display()))?;

    let Some(toplevel) = git_line(&canonical, &["rev-parse", "--show-toplevel"]) else {
        return Ok(Workspace {
            path: canonical,
            kind: WorkspaceKind::Plain,
            git: None,
        });
    };
    let toplevel = canonicalize_or(&toplevel);

    let git_dir =
        git_line(&canonical, &["rev-parse", "--absolute-git-dir"]).map(|d| canonicalize_or(&d));
    let common_dir = git_line(
        &canonical,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )
    .map_or_else(
        || git_dir.clone().unwrap_or_else(|| toplevel.join(".git")),
        |d| canonicalize_or(&d),
    );

    // A linked worktree's git dir differs from the shared common dir.
    let kind = match &git_dir {
        Some(dir) if *dir != common_dir => WorkspaceKind::GitWorktree,
        _ => WorkspaceKind::GitRepo,
    };

    let current_branch =
        git_line(&canonical, &["rev-parse", "--abbrev-ref", "HEAD"]).filter(|b| b != "HEAD");
    let branches = list_branches(&canonical);
    let worktrees = list_worktrees(&canonical);

    Ok(Workspace {
        path: canonical,
        kind,
        git: Some(GitInfo {
            toplevel,
            common_dir,
            current_branch,
            branches,
            worktrees,
        }),
    })
}

/// The canonical main-repo toplevel for `root` (the authorization anchor), or
/// the canonical directory itself when `root` is not in a repo.
///
/// # Errors
///
/// Returns an error if `root` cannot be canonicalized.
pub fn resolve_origin(root: &Path) -> Result<PathBuf> {
    Ok(origin_root(&detect(root)?))
}

/// Resolves a spawn selection to an effective cwd, creating a worktree when
/// asked. For [`WorktreeMode::Create`], an existing worktree for the same branch
/// is reused (and reported with `created_worktree = None`).
///
/// # Errors
///
/// Returns an error if detection fails, a worktree cannot be created, or a
/// `Create`/`Existing` request targets a non-git path.
pub fn resolve(root: &Path, mode: &WorktreeMode) -> Result<ResolvedWorkspace> {
    let ws = detect(root)?;
    let origin = origin_root(&ws);
    match mode {
        WorktreeMode::InPlace => Ok(ResolvedWorkspace {
            cwd: ws.path,
            origin_root: origin,
            created_worktree: None,
        }),
        WorktreeMode::Existing(path) => {
            let cwd = std::fs::canonicalize(path)
                .with_context(|| format!("cannot resolve worktree {}", path.display()))?;
            Ok(ResolvedWorkspace {
                cwd,
                origin_root: origin,
                created_worktree: None,
            })
        }
        WorktreeMode::Create { branch, base } => {
            let git = ws.git.as_ref().ok_or_else(|| {
                anyhow!(
                    "cannot create a worktree: {} is not a git repository",
                    ws.path.display()
                )
            })?;
            create_worktree(&origin, git, branch, base.as_deref())
        }
    }
}

/// Best-effort removal of an auto-created worktree. Non-force, so a dirty
/// worktree is preserved (the branch always persists); failures are logged.
pub fn remove_worktree(origin_root: &Path, worktree: &Path) {
    match run_git(
        origin_root,
        &["worktree", "remove", &worktree.to_string_lossy()],
    ) {
        Ok(_) => tracing::debug!(worktree = %worktree.display(), "removed session worktree"),
        Err(stderr) => tracing::warn!(
            worktree = %worktree.display(),
            error = stderr.trim(),
            "could not remove session worktree (left in place)"
        ),
    }
}

/// Creates (or reuses) an isolated worktree for `branch`.
fn create_worktree(
    origin: &Path,
    git: &GitInfo,
    branch: &str,
    base: Option<&str>,
) -> Result<ResolvedWorkspace> {
    // Reuse an existing worktree already checked out to this branch.
    if let Some(existing) = git
        .worktrees
        .iter()
        .find(|w| w.branch.as_deref() == Some(branch))
    {
        return Ok(ResolvedWorkspace {
            cwd: existing.path.clone(),
            origin_root: origin.to_path_buf(),
            created_worktree: None,
        });
    }

    let dest = unique_dest(origin, branch);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let dest_str = dest.to_string_lossy().into_owned();

    // Try to create a fresh branch; if it already exists, check it out instead.
    let mut args = vec!["worktree", "add", "-b", branch, &dest_str];
    if let Some(base) = base {
        args.push(base);
    }
    if let Err(stderr) = run_git(origin, &args) {
        run_git(origin, &["worktree", "add", &dest_str, branch])
            .map_err(|retry| anyhow!("git worktree add failed: {}", first_line(&retry, &stderr)))?;
    }

    let cwd = std::fs::canonicalize(&dest).unwrap_or(dest);
    Ok(ResolvedWorkspace {
        cwd: cwd.clone(),
        origin_root: origin.to_path_buf(),
        created_worktree: Some(cwd),
    })
}

/// `<data>/worktrees/<repo>/<branch>`, uniquified if the path is already taken.
fn unique_dest(origin: &Path, branch: &str) -> PathBuf {
    let repo = origin
        .file_name()
        .map_or_else(|| "repo".to_string(), |n| sanitize(&n.to_string_lossy()));
    let base = omniagent_data_dir()
        .join("worktrees")
        .join(repo)
        .join(sanitize(branch));
    if !base.exists() {
        return base;
    }
    for n in 2..100 {
        let candidate = base.with_file_name(format!("{}-{n}", sanitize(branch)));
        if !candidate.exists() {
            return candidate;
        }
    }
    base
}

/// The main-repo toplevel: the first worktree entry, else the git toplevel, else
/// the canonical path itself (plain dirs).
fn origin_root(ws: &Workspace) -> PathBuf {
    ws.git.as_ref().map_or_else(
        || ws.path.clone(),
        |git| {
            git.worktrees
                .first()
                .map_or_else(|| git.toplevel.clone(), |w| w.path.clone())
        },
    )
}

fn list_branches(root: &Path) -> Vec<String> {
    git(
        root,
        &["for-each-ref", "--format=%(refname:short)", "refs/heads"],
    )
    .map(|out| {
        out.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(ToString::to_string)
            .collect()
    })
    .unwrap_or_default()
}

fn list_worktrees(root: &Path) -> Vec<WorktreeEntry> {
    let Some(out) = git(root, &["worktree", "list", "--porcelain"]) else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    let mut path: Option<PathBuf> = None;
    let mut branch: Option<String> = None;
    let mut bare = false;
    let mut flush = |path: &mut Option<PathBuf>, branch: &mut Option<String>, bare: &mut bool| {
        if let Some(p) = path.take()
            && !*bare
        {
            entries.push(WorktreeEntry {
                path: canonicalize_or(&p.to_string_lossy()),
                branch: branch.take(),
            });
        }
        *branch = None;
        *bare = false;
    };
    for line in out.lines() {
        if let Some(rest) = line.strip_prefix("worktree ") {
            flush(&mut path, &mut branch, &mut bare);
            path = Some(PathBuf::from(rest.trim()));
        } else if let Some(rest) = line.strip_prefix("branch ") {
            branch = Some(rest.trim().trim_start_matches("refs/heads/").to_string());
        } else if line.trim() == "bare" {
            bare = true;
        }
    }
    flush(&mut path, &mut branch, &mut bare);
    entries
}

/// Runs a mutating git command, returning stdout on success or stderr on error.
fn run_git(root: &Path, args: &[&str]) -> std::result::Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).into_owned())
    }
}

fn canonicalize_or(path: &str) -> PathBuf {
    let p = PathBuf::from(path.trim());
    std::fs::canonicalize(&p).unwrap_or(p)
}

fn first_line<'a>(primary: &'a str, fallback: &'a str) -> String {
    let pick = if primary.trim().is_empty() {
        fallback
    } else {
        primary
    };
    pick.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("unknown error")
        .to_string()
}

/// Maps a string to a filesystem-safe directory segment.
fn sanitize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_dash = false;
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    let trimmed = out.trim_matches('-');
    let capped: String = trimmed.chars().take(60).collect();
    let result = capped.trim_matches('-').to_string();
    if result.is_empty() {
        "wt".to_string()
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn run(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }

    fn temp_repo() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("oa-ws-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        run(&dir, &["init", "-q", "-b", "main"]);
        run(&dir, &["config", "user.email", "t@t"]);
        run(&dir, &["config", "user.name", "t"]);
        std::fs::write(dir.join("README.md"), b"hi").unwrap();
        run(&dir, &["add", "."]);
        run(&dir, &["commit", "-qm", "init"]);
        std::fs::canonicalize(&dir).unwrap()
    }

    #[test]
    fn detect_classifies_plain_and_repo() {
        let plain = std::env::temp_dir().join(format!("oa-plain-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&plain).unwrap();
        assert_eq!(detect(&plain).unwrap().kind, WorkspaceKind::Plain);
        std::fs::remove_dir_all(&plain).unwrap();

        let repo = temp_repo();
        let ws = detect(&repo).unwrap();
        assert_eq!(ws.kind, WorkspaceKind::GitRepo);
        let git = ws.git.unwrap();
        assert_eq!(git.current_branch.as_deref(), Some("main"));
        assert!(git.branches.contains(&"main".to_string()));
        std::fs::remove_dir_all(&repo).unwrap();
    }

    #[test]
    fn resolve_create_makes_worktree_anchored_to_origin() {
        let repo = temp_repo();
        let resolved = resolve(
            &repo,
            &WorktreeMode::Create {
                branch: "feature/x".to_string(),
                base: None,
            },
        )
        .unwrap();
        assert_eq!(resolved.origin_root, repo);
        assert!(resolved.created_worktree.is_some());
        assert!(resolved.cwd.exists());
        // The created cwd is itself a linked worktree of the same repo.
        let wt = detect(&resolved.cwd).unwrap();
        assert_eq!(wt.kind, WorkspaceKind::GitWorktree);
        assert_eq!(resolve_origin(&resolved.cwd).unwrap(), repo);

        remove_worktree(&resolved.origin_root, &resolved.cwd);
        std::fs::remove_dir_all(&repo).unwrap();
    }

    #[test]
    fn resolve_create_reuses_existing_branch_worktree() {
        let repo = temp_repo();
        let mode = WorktreeMode::Create {
            branch: "dev".to_string(),
            base: None,
        };
        let first = resolve(&repo, &mode).unwrap();
        let second = resolve(&repo, &mode).unwrap();
        assert_eq!(first.cwd, second.cwd);
        assert!(second.created_worktree.is_none());

        remove_worktree(&first.origin_root, &first.cwd);
        std::fs::remove_dir_all(&repo).unwrap();
    }

    #[test]
    fn sanitize_maps_slashes_and_caps() {
        assert_eq!(sanitize("feature/new-thing"), "feature-new-thing");
        assert_eq!(sanitize("  "), "wt");
        assert_eq!(sanitize("UPPER"), "upper");
    }
}
