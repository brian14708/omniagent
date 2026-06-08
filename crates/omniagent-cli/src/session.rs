//! Data-directory helper for omniagent runtime artifacts (trace archives,
//! terminal recordings). Distinct from [`crate::config`], which holds durable
//! daemon configuration under `$XDG_CONFIG_HOME`.

use std::path::PathBuf;

/// Base data directory for omniagent runtime artifacts, per the XDG Base
/// Directory spec: `$XDG_DATA_HOME/omniagent` (absolute `$XDG_DATA_HOME` only),
/// else `$HOME/.local/share/omniagent`, else `./omniagent`.
pub fn omniagent_data_dir() -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("omniagent")
}
