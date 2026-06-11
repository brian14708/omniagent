//! Daemon configuration, persisted as JSON at
//! `$XDG_CONFIG_HOME/omniagent/config.json` (falling back to
//! `~/.config/omniagent/config.json`).
//!
//! This holds everything the daemon needs to run: the control-plane credentials
//! (`omniagent login`), the allowed-workspaces allowlist (`omniagent
//! workspaces`), and the access policy. The file is written owner-only because
//! it stores the long-lived API token.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// On-disk daemon configuration.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// `OmniAgent` control-plane URL the daemon connects to.
    #[serde(default)]
    pub server_url: Option<String>,
    /// Long-lived CLI API token.
    #[serde(default)]
    pub token: Option<String>,
    /// Allow spawning agents in any directory, bypassing the allowlist.
    #[serde(default)]
    pub full_access: bool,
    /// Canonical absolute directories the daemon may spawn agents under.
    #[serde(default)]
    pub allowed_workspaces: Vec<String>,
    /// Default launch commands keyed by bare agent name. A session spawned with
    /// a bare name (e.g. `claude`) is expanded to its mapped command (e.g.
    /// `pnpm dlx @anthropic-ai/claude-code`) before spawning, so the daemon host
    /// needs only the package runner rather than a globally installed agent
    /// binary. Editing the file overrides or extends this map.
    #[serde(default = "default_agent_commands")]
    pub agent_commands: BTreeMap<String, Vec<String>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_url: None,
            token: None,
            full_access: false,
            allowed_workspaces: Vec::new(),
            agent_commands: default_agent_commands(),
        }
    }
}

/// Built-in agent launch commands, used when the config file omits the field.
fn default_agent_commands() -> BTreeMap<String, Vec<String>> {
    let cmd = |parts: &[&str]| parts.iter().map(|s| (*s).to_string()).collect();
    BTreeMap::from([
        (
            "claude".to_string(),
            cmd(&["pnpm", "dlx", "@anthropic-ai/claude-code"]),
        ),
        ("codex".to_string(), cmd(&["codex"])),
        ("gemini".to_string(), cmd(&["gemini"])),
    ])
}

/// Reads and writes the daemon [`Config`] file.
#[derive(Debug, Clone)]
pub struct ConfigStore {
    path: PathBuf,
}

impl ConfigStore {
    #[must_use]
    pub fn default() -> Self {
        Self {
            path: omniagent_config_dir().join("config.json"),
        }
    }

    /// Loads the config, returning the defaults when the file is absent.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn load(&self) -> Result<Config> {
        match fs::read_to_string(&self.path) {
            Ok(body) => serde_json::from_str(&body)
                .with_context(|| format!("failed to parse {}", self.path.display())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
            Err(err) => Err(err).with_context(|| format!("failed to read {}", self.path.display())),
        }
    }

    /// Stores the control-plane credentials, preserving the rest of the config.
    ///
    /// # Errors
    ///
    /// Returns an error if the config cannot be read or written.
    pub fn set_credentials(&self, server_url: String, token: String) -> Result<()> {
        let mut config = self.load()?;
        config.server_url = Some(server_url);
        config.token = Some(token);
        self.save(&config)
    }

    /// The configured `(server_url, token)`, if both are present.
    ///
    /// # Errors
    ///
    /// Returns an error if the config cannot be read.
    pub fn credentials(&self) -> Result<Option<(String, String)>> {
        let config = self.load()?;
        Ok(match (config.server_url, config.token) {
            (Some(url), Some(token)) => Some((url, token)),
            _ => None,
        })
    }

    /// Adds `path` (canonicalized) to the allowlist, returning the canonical
    /// path. A path already present is left as-is.
    ///
    /// # Errors
    ///
    /// Returns an error if `path` cannot be canonicalized or the config cannot
    /// be written.
    pub fn add_workspace(&self, path: &Path) -> Result<PathBuf> {
        let canonical = fs::canonicalize(path)
            .with_context(|| format!("cannot resolve workspace path {}", path.display()))?;
        let entry = canonical.to_string_lossy().to_string();
        let mut config = self.load()?;
        if !config.allowed_workspaces.contains(&entry) {
            config.allowed_workspaces.push(entry);
            config.allowed_workspaces.sort();
            self.save(&config)?;
        }
        Ok(canonical)
    }

    /// Removes a workspace from the allowlist. Returns whether one was removed.
    ///
    /// # Errors
    ///
    /// Returns an error if the config cannot be read or written.
    pub fn remove_workspace(&self, path: &Path) -> Result<bool> {
        let target = fs::canonicalize(path).map_or_else(
            |_| path.to_string_lossy().to_string(),
            |p| p.to_string_lossy().to_string(),
        );
        let mut config = self.load()?;
        let before = config.allowed_workspaces.len();
        config.allowed_workspaces.retain(|entry| entry != &target);
        let removed = config.allowed_workspaces.len() != before;
        if removed {
            self.save(&config)?;
        }
        Ok(removed)
    }

    /// Lists the allowed-workspace entries (canonical paths).
    ///
    /// # Errors
    ///
    /// Returns an error if the config cannot be read.
    pub fn list_workspaces(&self) -> Result<Vec<String>> {
        Ok(self.load()?.allowed_workspaces)
    }

    /// The allowed-workspace roots as canonical paths, loaded fresh so changes
    /// made while the daemon runs take effect on the next spawn.
    ///
    /// # Errors
    ///
    /// Returns an error if the config cannot be read.
    pub fn workspace_roots(&self) -> Result<Vec<PathBuf>> {
        Ok(self
            .list_workspaces()?
            .into_iter()
            .map(PathBuf::from)
            .collect())
    }

    /// The configured agent launch-command map, loaded fresh so edits made
    /// while the daemon runs take effect on the next spawn.
    ///
    /// # Errors
    ///
    /// Returns an error if the config cannot be read.
    pub fn agent_commands(&self) -> Result<BTreeMap<String, Vec<String>>> {
        Ok(self.load()?.agent_commands)
    }

    fn save(&self, config: &Config) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let body = serde_json::to_string_pretty(config)?;
        fs::write(&self.path, body)
            .with_context(|| format!("failed to write {}", self.path.display()))?;
        // The config stores the long-lived API token, so keep it owner-only.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&self.path, fs::Permissions::from_mode(0o600))
                .with_context(|| format!("failed to chmod {}", self.path.display()))?;
        }
        Ok(())
    }
}

/// Base config directory per the XDG Base Directory spec:
/// `$XDG_CONFIG_HOME/omniagent` (absolute `$XDG_CONFIG_HOME` only), else
/// `$HOME/.config/omniagent`, else `./omniagent`.
#[must_use]
pub fn omniagent_config_dir() -> PathBuf {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("omniagent")
}

/// Returns whether `cwd` is one of `roots` or nested under one of them.
///
/// Both `cwd` and `roots` are expected to be canonical absolute paths.
/// `Path::starts_with` matches whole path components, so a root `/a/projects`
/// permits `/a/projects/sub` but not the sibling `/a/projects-evil`.
#[must_use]
pub fn path_within_roots(cwd: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| cwd.starts_with(root))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_within_roots_covers_subdirs_only() {
        let roots = vec![PathBuf::from("/home/u/projects")];
        assert!(path_within_roots(Path::new("/home/u/projects"), &roots));
        assert!(path_within_roots(Path::new("/home/u/projects/foo"), &roots));
        // sibling sharing a name prefix must not match
        assert!(!path_within_roots(
            Path::new("/home/u/projects-evil"),
            &roots
        ));
        // empty allowlist denies everything
        assert!(!path_within_roots(Path::new("/home/u/projects"), &[]));
    }

    #[test]
    fn credentials_and_workspaces_round_trip() {
        let dir = std::env::temp_dir().join(format!("oa-cfg-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let store = ConfigStore {
            path: dir.join("config.json"),
        };

        assert!(store.credentials().unwrap().is_none());
        store
            .set_credentials("http://localhost:4000".into(), "tok".into())
            .unwrap();
        assert_eq!(
            store.credentials().unwrap(),
            Some(("http://localhost:4000".into(), "tok".into()))
        );

        let ws = dir.join("workspace");
        fs::create_dir_all(&ws).unwrap();
        let canonical = store.add_workspace(&ws).unwrap();
        // setting credentials and adding a workspace coexist in one file
        assert!(store.credentials().unwrap().is_some());
        assert_eq!(
            store.list_workspaces().unwrap(),
            vec![canonical.to_string_lossy().to_string()]
        );
        let roots = store.workspace_roots().unwrap();
        assert!(path_within_roots(&canonical.join("sub"), &roots));

        assert!(store.remove_workspace(&ws).unwrap());
        assert!(store.list_workspaces().unwrap().is_empty());

        let _ = fs::remove_dir_all(&dir);
    }
}
