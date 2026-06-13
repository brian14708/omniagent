//! Publishing snapshot artifacts to the content-addressed store (CAS).
//!
//! When the daemon is configured with a CAS ([`oad_core::CasConfig`]), each
//! captured snapshot's per-container checkpoint images are chunked with
//! `FastCDC`, uploaded to the object store, and described by a
//! [`SnapshotDescriptor`] so any node can later materialize and fork from them.
//! Only the checkpoint (the CRIU memory state) is published; a node rebuilds the
//! rootfs deterministically from the OCI registry as it does today.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use oad_cas::{ChunkStore, ChunkerParams, S3ChunkStore, S3Config};
use oad_core::{CasConfig, OadPaths};
use oad_runtime::RUNSC_CHECKPOINT_IMAGE;

use crate::snapshots::{ContainerCheckpoint, SnapshotDescriptor, SnapshotManifest};

/// Publishes snapshot checkpoint images to a content-addressed object store.
pub struct CasPublisher {
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

impl CasPublisher {
    /// Builds a publisher from CAS configuration, constructing its own HTTP
    /// client for object-store transfers.
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

    /// Chunks and uploads every container's checkpoint image for the snapshot
    /// described by `manifest`, then stores a [`SnapshotDescriptor`].
    ///
    /// # Errors
    ///
    /// Returns an error if a checkpoint image is missing, an upload fails, or
    /// the descriptor cannot be serialized or stored.
    pub async fn publish_snapshot(
        &self,
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
            let outcome =
                oad_cas::chunk_and_upload(&image, &self.store, self.params, self.zstd_level)
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
        self.store
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
