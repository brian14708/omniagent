//! Immutable snapshots: the fork source for `create --from-snapshot`.
//!
//! A snapshot is captured from a running sandbox (see the `snapshot` handler).
//! It pairs a gVisor checkpoint image (under
//! [`OadPaths::snapshot_checkpoint_dir`]) with a manifest of the container
//! specs needed to rebuild bundles when forking. Both live under the persistent
//! cache tree so snapshots survive sandbox deletion.

use std::io;

use oad_core::{ContainerSpec, OadPaths};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::registry::write_json_atomic;

/// On-disk description of a snapshot, written alongside its checkpoint image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub name: String,
    /// Pause image the source sandbox was booted with.
    pub pause_image: String,
    /// User container specs (excluding the reserved `pause` container).
    pub containers: Vec<ContainerSpec>,
    pub created_at: OffsetDateTime,
}

impl SnapshotManifest {
    /// Builds a manifest stamped with the current UTC time.
    #[must_use]
    pub fn new(name: String, pause_image: String, containers: Vec<ContainerSpec>) -> Self {
        Self {
            name,
            pause_image,
            containers,
            created_at: OffsetDateTime::now_utc(),
        }
    }

    /// Container names captured in the snapshot, with `pause` first.
    #[must_use]
    pub fn container_names(&self) -> Vec<String> {
        oad_core::container_names(&self.containers)
    }

    /// Creation time formatted as RFC 3339, or the empty string if unformattable.
    #[must_use]
    pub fn created_at_rfc3339(&self) -> String {
        self.created_at.format(&Rfc3339).unwrap_or_default()
    }
}

/// Writes a snapshot manifest, creating the snapshot directory as needed.
pub async fn write_manifest(paths: &OadPaths, manifest: &SnapshotManifest) -> io::Result<()> {
    write_json_atomic(&paths.snapshot_manifest(&manifest.name), manifest).await
}

/// Reads a single snapshot's manifest.
pub async fn read_manifest(paths: &OadPaths, name: &str) -> io::Result<SnapshotManifest> {
    let body = tokio::fs::read(paths.snapshot_manifest(name)).await?;
    serde_json::from_slice(&body).map_err(io::Error::other)
}

/// Reports whether a snapshot with `name` already exists.
pub async fn exists(paths: &OadPaths, name: &str) -> bool {
    tokio::fs::try_exists(paths.snapshot_manifest(name))
        .await
        .unwrap_or(false)
}

/// Atomically reserves a snapshot directory for creation.
///
/// Returns `Ok(false)` if any directory already exists for the snapshot name.
pub async fn reserve(paths: &OadPaths, name: &str) -> io::Result<bool> {
    tokio::fs::create_dir_all(paths.snapshots_dir()).await?;
    match tokio::fs::create_dir(paths.snapshot_dir(name)).await {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            if read_manifest(paths, name).await.is_ok() {
                return Ok(false);
            }
            delete(paths, name).await?;
            match tokio::fs::create_dir(paths.snapshot_dir(name)).await {
                Ok(()) => Ok(true),
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(false),
                Err(err) => Err(err),
            }
        }
        Err(err) => Err(err),
    }
}

/// Lists all stored snapshot manifests. Unreadable entries are skipped.
pub async fn list(paths: &OadPaths) -> Vec<SnapshotManifest> {
    let mut out = Vec::new();
    let Ok(mut entries) = tokio::fs::read_dir(paths.snapshots_dir()).await else {
        return out;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Some(name) = entry.file_name().to_str()
            && let Ok(manifest) = read_manifest(paths, name).await
        {
            out.push(manifest);
        }
    }
    out
}

/// Removes a snapshot's directory (manifest + checkpoint image).
///
/// Returns `Ok(false)` if the snapshot did not exist.
pub async fn delete(paths: &OadPaths, name: &str) -> io::Result<bool> {
    let dir = paths.snapshot_dir(name);
    match tokio::fs::remove_dir_all(&dir).await {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reserve_is_atomic_for_existing_snapshot_dir() {
        let temp = tempfile::tempdir().unwrap();
        let paths = OadPaths::new(temp.path());
        let manifest =
            SnapshotManifest::new("golden".to_string(), "pause:latest".to_string(), Vec::new());

        assert!(reserve(&paths, "golden").await.unwrap());
        write_manifest(&paths, &manifest).await.unwrap();
        assert!(!reserve(&paths, "golden").await.unwrap());
        assert!(paths.snapshot_dir("golden").is_dir());
    }

    #[tokio::test]
    async fn reserve_reclaims_stale_directory_without_manifest() {
        let temp = tempfile::tempdir().unwrap();
        let paths = OadPaths::new(temp.path());

        assert!(reserve(&paths, "golden").await.unwrap());
        assert!(reserve(&paths, "golden").await.unwrap());
    }

    #[tokio::test]
    async fn write_manifest_uses_final_manifest_path_only() {
        let temp = tempfile::tempdir().unwrap();
        let paths = OadPaths::new(temp.path());
        let manifest =
            SnapshotManifest::new("golden".to_string(), "pause:latest".to_string(), Vec::new());

        write_manifest(&paths, &manifest).await.unwrap();

        let read = read_manifest(&paths, "golden").await.unwrap();
        assert_eq!(read.name, "golden");
        assert!(paths.snapshot_manifest("golden").is_file());
        let tmp_entries = std::fs::read_dir(paths.snapshot_dir("golden"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".manifest.json")
            })
            .count();
        assert_eq!(tmp_entries, 0);
    }
}
