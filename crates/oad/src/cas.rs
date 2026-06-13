//! The daemon's interface to the content-addressed store (CAS).
//!
//! When the daemon is configured with a CAS ([`oad_core::CasConfig`]) it can:
//! - **publish** a captured snapshot — chunk each container's `checkpoint.img`
//!   with `FastCDC`, upload it, and store a portable [`SnapshotDescriptor`]; and
//! - **materialize** a snapshot it does not hold locally — fetch the descriptor,
//!   reassemble the checkpoint images, and write the local manifest so a fork can
//!   proceed.
//!
//! Only the checkpoint (the CRIU memory state) travels through the CAS; a node
//! rebuilds the rootfs deterministically from the OCI registry as it does today.
//!
//! The publish/materialize logic lives in store-generic free functions so it can
//! be exercised against an [`oad_cas::FsChunkStore`] in tests; [`Cas`] wraps the
//! production [`oad_cas::S3ChunkStore`].

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use oad_cas::{ChunkStore, ChunkerParams, S3ChunkStore, S3Config};
use oad_core::{CasConfig, OadPaths};
use oad_runtime::RUNSC_CHECKPOINT_IMAGE;

use crate::snapshots::{ContainerCheckpoint, SnapshotDescriptor, SnapshotManifest};

/// The daemon's CAS handle: an object store plus chunking parameters.
pub struct Cas {
    store: S3ChunkStore,
    params: ChunkerParams,
    zstd_level: i32,
}

/// Summary of a published snapshot, returned to the control plane so it can
/// register the descriptor and reference-count the chunks.
#[derive(Debug, Clone)]
pub struct PublishOutcome {
    /// Object key of the stored [`SnapshotDescriptor`].
    pub descriptor_key: String,
    /// Total uncompressed size of all checkpoint images.
    pub total_bytes: u64,
    /// Bytes actually uploaded (after deduplication).
    pub uploaded_bytes: u64,
    /// Distinct chunk hashes (hex) the snapshot references.
    pub chunk_hashes: Vec<String>,
}

impl Cas {
    /// Builds a CAS handle from configuration, constructing its own HTTP client
    /// for object-store transfers.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built or the CAS
    /// configuration is invalid (e.g. a malformed endpoint URL).
    pub fn from_config(cas: &CasConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .build()
            .context("failed to build CAS HTTP client")?;
        let store = S3ChunkStore::new(
            S3Config {
                endpoint: cas.endpoint.clone(),
                region: cas.region.clone(),
                bucket: cas.bucket.clone(),
                access_key_id: cas.access_key_id.clone(),
                secret_access_key: cas.secret_access_key.clone(),
                prefix: cas.prefix.clone(),
            },
            http,
        )
        .map_err(|err| anyhow!("invalid CAS config: {err}"))?;
        Ok(Self {
            store,
            params: ChunkerParams {
                min: cas.chunk_min,
                avg: cas.chunk_avg,
                max: cas.chunk_max,
            },
            zstd_level: cas.zstd_level,
        })
    }

    /// Publishes the snapshot described by `manifest` to the object store.
    ///
    /// # Errors
    ///
    /// Returns an error if a checkpoint image is missing, an upload fails, or the
    /// descriptor cannot be serialized or stored.
    pub async fn publish_snapshot(
        &self,
        paths: &OadPaths,
        manifest: &SnapshotManifest,
    ) -> Result<PublishOutcome> {
        publish_snapshot(&self.store, self.params, self.zstd_level, paths, manifest).await
    }

    /// Materializes a snapshot from the object store into the local snapshot
    /// store (checkpoint images + manifest), so a fork can proceed on a node that
    /// did not capture it.
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor is absent, a chunk cannot be fetched or
    /// verified, or the local manifest cannot be written.
    pub async fn materialize_snapshot(&self, paths: &OadPaths, name: &str) -> Result<()> {
        materialize_snapshot(&self.store, paths, name).await
    }
}

/// Chunks and uploads every container's checkpoint image for `manifest`, then
/// stores a [`SnapshotDescriptor`]. Generic over the store for testability.
async fn publish_snapshot<S: ChunkStore + ?Sized>(
    store: &S,
    params: ChunkerParams,
    zstd_level: i32,
    paths: &OadPaths,
    manifest: &SnapshotManifest,
) -> Result<PublishOutcome> {
    let checkpoint_root = paths.snapshot_checkpoint_dir(&manifest.name);
    let mut checkpoints = Vec::new();
    let mut unique: BTreeSet<String> = BTreeSet::new();
    let mut total_bytes: u64 = 0;
    let mut uploaded_bytes: u64 = 0;

    for container in manifest.container_names() {
        let image = checkpoint_image_path(&checkpoint_root, &container)
            .await
            .with_context(|| format!("locating checkpoint image for {container}"))?;
        let outcome = oad_cas::chunk_and_upload(&image, store, params, zstd_level)
            .await
            .with_context(|| format!("uploading checkpoint for {container}"))?;
        total_bytes += outcome.recipe.total_len;
        uploaded_bytes += outcome.bytes_uploaded;
        for hash in outcome.recipe.unique_chunk_hashes() {
            unique.insert(hash.to_hex());
        }
        checkpoints.push(ContainerCheckpoint {
            container,
            recipe: outcome.recipe,
        });
    }

    let descriptor = SnapshotDescriptor {
        name: manifest.name.clone(),
        pause_image: manifest.pause_image.clone(),
        containers: manifest.containers.clone(),
        network: manifest.network.clone(),
        created_at: manifest.created_at,
        checkpoints,
    };
    let descriptor_key = oad_cas::descriptor_object_key(&manifest.name);
    let body = serde_json::to_vec(&descriptor).context("serializing snapshot descriptor")?;
    store
        .put_object(&descriptor_key, &body)
        .await
        .context("storing snapshot descriptor")?;

    Ok(PublishOutcome {
        descriptor_key,
        total_bytes,
        uploaded_bytes,
        chunk_hashes: unique.into_iter().collect(),
    })
}

/// Fetches the descriptor for `name`, reassembles each container's checkpoint
/// image into the local snapshot store, and writes the local manifest.
async fn materialize_snapshot<S: ChunkStore + ?Sized>(
    store: &S,
    paths: &OadPaths,
    name: &str,
) -> Result<()> {
    let descriptor_key = oad_cas::descriptor_object_key(name);
    let body = store
        .get_object(&descriptor_key)
        .await
        .with_context(|| format!("fetching descriptor {descriptor_key}"))?;
    let descriptor: SnapshotDescriptor =
        serde_json::from_slice(&body).context("parsing snapshot descriptor")?;

    let checkpoint_root = paths.snapshot_checkpoint_dir(name);
    for checkpoint in &descriptor.checkpoints {
        let dest = checkpoint_root
            .join(&checkpoint.container)
            .join(RUNSC_CHECKPOINT_IMAGE);
        oad_cas::materialize(&checkpoint.recipe, &dest, store)
            .await
            .with_context(|| format!("materializing checkpoint for {}", checkpoint.container))?;
    }

    // Write the local manifest last: its presence is what `fork_from_snapshot`
    // treats as "snapshot is available locally", so it must only appear once the
    // checkpoint images are fully in place.
    let manifest = SnapshotManifest {
        name: descriptor.name,
        pause_image: descriptor.pause_image,
        containers: descriptor.containers,
        network: descriptor.network,
        created_at: descriptor.created_at,
    };
    crate::snapshots::write_manifest(paths, &manifest)
        .await
        .context("writing local snapshot manifest")?;
    Ok(())
}

/// Resolves a container's checkpoint image, preferring the per-container layout
/// (`<root>/<container>/checkpoint.img`) and falling back to the legacy
/// single-image layout (`<root>/checkpoint.img`).
async fn checkpoint_image_path(root: &Path, container: &str) -> Result<PathBuf> {
    let per_container = root.join(container).join(RUNSC_CHECKPOINT_IMAGE);
    if tokio::fs::try_exists(&per_container).await? {
        return Ok(per_container);
    }
    let legacy = root.join(RUNSC_CHECKPOINT_IMAGE);
    if tokio::fs::try_exists(&legacy).await? {
        return Ok(legacy);
    }
    Err(anyhow!(
        "no checkpoint image at {} or {}",
        per_container.display(),
        legacy.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use oad_cas::FsChunkStore;
    use oad_core::SandboxNetworkSpec;

    fn params() -> ChunkerParams {
        ChunkerParams {
            min: 2 * 1024,
            avg: 8 * 1024,
            max: 32 * 1024,
        }
    }

    #[tokio::test]
    async fn publish_then_materialize_on_a_cold_node_round_trips() {
        let store_dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(store_dir.path().join("cas"));

        // Build node: a snapshot with a single (pause) checkpoint image.
        let build_dir = tempfile::tempdir().unwrap();
        let build_paths = OadPaths::new(build_dir.path());
        let manifest = SnapshotManifest::new(
            "ws-v1".to_string(),
            "pause:latest".to_string(),
            Vec::new(),
            SandboxNetworkSpec::default(),
        );
        // container_names() yields just ["pause"] with no user containers.
        let pause_image = build_paths
            .snapshot_checkpoint_dir("ws-v1")
            .join("pause")
            .join(RUNSC_CHECKPOINT_IMAGE);
        tokio::fs::create_dir_all(pause_image.parent().unwrap())
            .await
            .unwrap();
        let checkpoint_bytes = vec![7u8; 100 * 1024];
        tokio::fs::write(&pause_image, &checkpoint_bytes)
            .await
            .unwrap();

        let outcome = publish_snapshot(&store, params(), 3, &build_paths, &manifest)
            .await
            .unwrap();
        assert_eq!(outcome.descriptor_key, "descriptors/ws-v1.json");
        assert!(!outcome.chunk_hashes.is_empty());

        // Cold node: a fresh base dir with nothing local.
        let cold_dir = tempfile::tempdir().unwrap();
        let cold_paths = OadPaths::new(cold_dir.path());
        assert!(!snapshots_exists(&cold_paths, "ws-v1").await);

        materialize_snapshot(&store, &cold_paths, "ws-v1")
            .await
            .unwrap();

        // The manifest and checkpoint image now exist locally and match.
        assert!(snapshots_exists(&cold_paths, "ws-v1").await);
        let restored = tokio::fs::read(
            cold_paths
                .snapshot_checkpoint_dir("ws-v1")
                .join("pause")
                .join(RUNSC_CHECKPOINT_IMAGE),
        )
        .await
        .unwrap();
        assert_eq!(restored, checkpoint_bytes);
        let manifest = crate::snapshots::read_manifest(&cold_paths, "ws-v1")
            .await
            .unwrap();
        assert_eq!(manifest.name, "ws-v1");
        assert_eq!(manifest.pause_image, "pause:latest");
    }

    #[tokio::test]
    async fn materialize_missing_descriptor_errors() {
        let store_dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(store_dir.path().join("cas"));
        let cold_dir = tempfile::tempdir().unwrap();
        let cold_paths = OadPaths::new(cold_dir.path());

        assert!(
            materialize_snapshot(&store, &cold_paths, "missing")
                .await
                .is_err()
        );
    }

    async fn snapshots_exists(paths: &OadPaths, name: &str) -> bool {
        crate::snapshots::exists(paths, name).await
    }
}
