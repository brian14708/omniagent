use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt;
use utoipa::ToSchema;
use uuid::Uuid;

pub const PAUSE_CONTAINER: &str = "pause";
const MAX_ID_SEGMENT_LEN: usize = 128;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("value cannot be empty")]
    Empty,
    #[error("value is too long; maximum length is {max} bytes")]
    TooLong { max: usize },
    #[error("{0:?} is reserved and cannot be used as a path segment")]
    ReservedPathSegment(String),
    #[error("value contains unsupported character {0:?}")]
    UnsupportedCharacter(char),
    #[error("{0:?} is a reserved container name")]
    ReservedContainerName(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SandboxId(String);

impl SandboxId {
    #[must_use]
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Creates a sandbox id from a string.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] if the value is empty, is `.` or `..`, or
    /// contains characters other than ASCII alphanumerics, `-`, `_`, or `.`.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_id_segment(&value)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SandboxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<SandboxId> for String {
    fn from(value: SandboxId) -> Self {
        value.0
    }
}

impl TryFrom<String> for SandboxId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl FromStr for SandboxId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SandboxStatus {
    Pending,
    Running,
    Stopping,
    Stopped,
    Suspended,
    Error,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SandboxRecord {
    /// Unique sandbox identifier.
    #[schema(value_type = String, example = "5f3c1b9e-1c2d-4a8b-9f0e-7a6b5c4d3e2f")]
    pub id: SandboxId,
    pub status: SandboxStatus,
    /// Names of all containers in the sandbox, including the reserved `pause` container.
    pub containers: Vec<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    /// Most recent error encountered for this sandbox, if any.
    pub last_error: Option<String>,
    /// Name of the snapshot this sandbox was forked from, if any.
    #[serde(default)]
    pub origin_snapshot: Option<String>,
}

impl SandboxRecord {
    #[must_use]
    pub fn new_pending(id: SandboxId, containers: Vec<String>) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            id,
            status: SandboxStatus::Pending,
            containers,
            created_at: now,
            updated_at: now,
            last_error: None,
            origin_snapshot: None,
        }
    }

    pub fn set_status(&mut self, status: SandboxStatus) {
        self.status = status;
        self.updated_at = OffsetDateTime::now_utc();
    }

    pub fn set_error(&mut self, error: impl Into<String>) {
        self.status = SandboxStatus::Error;
        self.last_error = Some(error.into());
        self.updated_at = OffsetDateTime::now_utc();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ContainerSpec {
    /// Container name. Must be unique within the sandbox and may not be the
    /// reserved name `pause`.
    #[schema(example = "web")]
    pub name: String,
    /// OCI image reference to run.
    #[schema(example = "registry.example/web:latest")]
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: Vec<EnvVar>,
}

impl ContainerSpec {
    #[must_use]
    pub fn argv(&self) -> Vec<String> {
        self.command.iter().chain(&self.args).cloned().collect()
    }
}

/// Persisted per-sandbox spec for forking.
///
/// Holds everything needed to rebuild the sandbox's bundles when forking from a
/// snapshot of it. Written at creation time (alongside `state.json`) and copied
/// into the snapshot store on snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxSpec {
    /// Pause image the sandbox was booted with.
    pub pause_image: String,
    /// User container specs (excluding the reserved `pause` container).
    pub containers: Vec<ContainerSpec>,
    /// Network policy and shaping applied to this sandbox.
    #[serde(default)]
    pub network: SandboxNetworkSpec,
    /// runsc network mode resolved at create time. Persisted so resume/restore
    /// replays the same mode the checkpoint was taken with, instead of
    /// recomputing it from daemon config that may have changed. `None` for
    /// specs written before this field existed (resume falls back to the
    /// recomputed mode in that case).
    #[serde(default)]
    pub network_mode: Option<NetworkMode>,
}

/// runsc networking mode for a sandbox, mirrored in `oad-core` (rather than
/// `oad-runtime`) so it can be serialized into the persisted [`SandboxSpec`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkMode {
    Sandbox,
    Host,
}

/// Full container list for a sandbox: the reserved `pause` container first,
/// followed by each user container in order.
///
/// This pause-first ordering is the canonical sandbox layout.
#[must_use]
pub fn container_names(containers: &[ContainerSpec]) -> Vec<String> {
    std::iter::once(PAUSE_CONTAINER.to_string())
        .chain(containers.iter().map(|c| c.name.clone()))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct SandboxNetworkSpec {
    pub egress: EgressPolicy,
    pub shaping: TrafficShapingPolicy,
    pub l7: L7EgressPolicy,
    pub udp: UdpEgressPolicy,
}

impl Default for SandboxNetworkSpec {
    fn default() -> Self {
        Self {
            egress: EgressPolicy::AllowAll,
            shaping: TrafficShapingPolicy::default(),
            l7: L7EgressPolicy::default(),
            udp: UdpEgressPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum EgressPolicy {
    #[default]
    AllowAll,
    DenyAll,
    Rules {
        #[serde(default)]
        rules: Vec<EgressRule>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EgressRule {
    pub destination: EgressDestination,
    #[serde(default)]
    pub protocol: Protocol,
    #[serde(default)]
    pub ports: Vec<PortRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EgressDestination {
    Cidr { cidr: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    #[default]
    All,
    Tcp,
    Udp,
    Icmp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PortRange {
    pub start: u16,
    pub end: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct TrafficShapingPolicy {
    pub upload_bps: Option<u64>,
    pub download_bps: Option<u64>,
    pub burst_bytes: Option<u64>,
    pub latency_ms: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct L7EgressPolicy {
    pub transparent_tcp: bool,
}

impl Default for L7EgressPolicy {
    fn default() -> Self {
        Self {
            transparent_tcp: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct UdpEgressPolicy {
    pub dns_redirect: bool,
    pub block_quic: bool,
    pub allow: Vec<EgressRule>,
}

impl Default for UdpEgressPolicy {
    fn default() -> Self {
        Self {
            dns_redirect: true,
            block_quic: true,
            allow: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub base_dir: PathBuf,
    pub pause_image: String,
    pub network_namespace: Option<PathBuf>,
    pub network: NetworkRuntimeConfig,
    pub observability: ObservabilityConfig,
    /// Host directories/files bind-mounted (read-only by default) into every user
    /// container the daemon creates — fresh or forked. Used to supply static
    /// assets such as the `omniagent` binary without baking them into images or
    /// persisting them per-sandbox.
    pub static_mounts: Vec<MountSpec>,
}

/// A host path bind-mounted into a container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountSpec {
    /// Absolute host path to mount.
    pub source: PathBuf,
    /// Absolute in-container destination path.
    pub destination: String,
    /// Mount read-only (the default; static assets should not be writable).
    #[serde(default = "mount_read_only_default")]
    pub read_only: bool,
}

const fn mount_read_only_default() -> bool {
    true
}

/// Daemon-level toggle for egress observability.
///
/// The OTLP exporter itself is configured through the canonical
/// `OTEL_EXPORTER_OTLP_*` environment variables (endpoint, protocol, headers,
/// timeout) read by the OpenTelemetry SDK; this only controls whether the
/// daemon emits spans at all.
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    pub enabled: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone)]
pub struct NetworkRuntimeConfig {
    pub enabled: bool,
    pub backend: ManagedNetworkBackend,
    pub sandbox_cidr: String,
    pub envoy_listener: String,
    pub dns_listener: String,
    pub dns_upstream: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ManagedNetworkBackend {
    Hostinet,
    Netstack,
}

impl Default for NetworkRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            backend: ManagedNetworkBackend::Netstack,
            sandbox_cidr: "10.90.0.0/16".to_string(),
            envoy_listener: "0.0.0.0:15001".to_string(),
            dns_listener: "0.0.0.0:15053".to_string(),
            dns_upstream: "1.1.1.1:53".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub bind: String,
    pub bearer_token: String,
}

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub http: HttpConfig,
    pub runtime: RuntimeConfig,
    /// When set, the daemon self-registers with an `OmniAgent` control plane and
    /// heartbeats for liveness.
    pub control_plane: Option<ControlPlaneConfig>,
}

/// How the daemon registers itself with the `OmniAgent` control plane so the
/// control plane can discover it and call its `/v1` API directly.
#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    /// Control-plane base URL (e.g. `http://127.0.0.1:4000`).
    pub url: String,
    /// Bearer token authenticating oad → control plane registration calls.
    pub register_token: String,
    /// Base URL the control plane should call back to reach this oad's `/v1` API.
    pub advertise_url: String,
    /// Optional human-readable instance name shown in the console.
    pub name: Option<String>,
    /// In-container path to the `omniagent` binary (supplied via a static mount),
    /// advertised so the control plane knows what to exec for `serve-session`.
    pub omniagent_path: String,
}

#[derive(Debug, Clone)]
pub struct OadPaths {
    base_dir: PathBuf,
}

impl OadPaths {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    #[must_use]
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    #[must_use]
    pub fn sandboxes_dir(&self) -> PathBuf {
        self.base_dir.join("sandboxes")
    }

    /// Root of the persistent, sandbox-independent cache. Survives sandbox
    /// deletion so pulled layers and built rootfs images can be reused.
    #[must_use]
    pub fn cache_dir(&self) -> PathBuf {
        self.base_dir.join("cache")
    }

    /// Cache of downloaded OCI content blobs, keyed by digest.
    #[must_use]
    pub fn layer_cache_dir(&self) -> PathBuf {
        self.cache_dir().join("layers")
    }

    /// Cache of recently resolved OCI manifests, keyed by image reference.
    #[must_use]
    pub fn manifest_cache_dir(&self) -> PathBuf {
        self.cache_dir().join("manifests")
    }

    /// Cache of built EROFS rootfs images, keyed by ordered layer digests.
    #[must_use]
    pub fn rootfs_cache_dir(&self) -> PathBuf {
        self.cache_dir().join("rootfs")
    }

    #[must_use]
    pub fn sandbox_dir(&self, id: &SandboxId) -> PathBuf {
        self.sandboxes_dir().join(id.as_str())
    }

    #[must_use]
    pub fn state_file(&self, id: &SandboxId) -> PathBuf {
        self.sandbox_dir(id).join("state.json")
    }

    #[must_use]
    pub fn bundles_dir(&self, id: &SandboxId) -> PathBuf {
        self.sandbox_dir(id).join("bundles")
    }

    #[must_use]
    pub fn bundle_dir(&self, id: &SandboxId, container: &str) -> PathBuf {
        self.bundles_dir(id).join(container)
    }

    #[must_use]
    pub fn rootfs_dir(&self, id: &SandboxId, container: &str) -> PathBuf {
        self.bundle_dir(id, container).join("rootfs")
    }

    #[must_use]
    pub fn rootfs_overlay_dir(&self, id: &SandboxId, container: &str) -> PathBuf {
        self.bundle_dir(id, container).join("rootfs-overlay")
    }

    /// Per-sandbox gVisor checkpoint image directory (runsc `-image-path`),
    /// written by `suspend` and read by `resume`.
    #[must_use]
    pub fn checkpoint_dir(&self, id: &SandboxId) -> PathBuf {
        self.sandbox_dir(id).join("checkpoint")
    }

    /// Per-sandbox spec manifest (pause image + container specs), written at
    /// creation so a later snapshot can rebuild bundles for forks.
    #[must_use]
    pub fn sandbox_spec(&self, id: &SandboxId) -> PathBuf {
        self.sandbox_dir(id).join("spec.json")
    }

    #[must_use]
    pub fn network_dir(&self) -> PathBuf {
        self.base_dir.join("network")
    }

    #[must_use]
    pub fn sandbox_network_state(&self, id: &SandboxId) -> PathBuf {
        self.sandbox_dir(id).join("network.json")
    }

    #[must_use]
    pub fn sandbox_resolv_conf(&self, id: &SandboxId) -> PathBuf {
        self.sandbox_dir(id).join("resolv.conf")
    }

    #[must_use]
    pub fn envoy_config(&self) -> PathBuf {
        self.network_dir().join("envoy.json")
    }

    #[must_use]
    pub fn envoy_log(&self) -> PathBuf {
        self.network_dir().join("envoy.log")
    }

    #[must_use]
    pub fn envoy_access_log_socket(&self) -> PathBuf {
        self.network_dir().join("envoy-access.sock")
    }

    /// Root of the immutable, sandbox-independent snapshot store. Lives under
    /// the persistent cache so forkable snapshots survive sandbox deletion.
    #[must_use]
    pub fn snapshots_dir(&self) -> PathBuf {
        self.cache_dir().join("snapshots")
    }

    #[must_use]
    pub fn snapshot_dir(&self, name: &str) -> PathBuf {
        self.snapshots_dir().join(name)
    }

    /// Checkpoint image directory for a snapshot (the fork source image-path).
    #[must_use]
    pub fn snapshot_checkpoint_dir(&self, name: &str) -> PathBuf {
        self.snapshot_dir(name).join("checkpoint")
    }

    /// Manifest of the container specs needed to rebuild bundles for a fork.
    #[must_use]
    pub fn snapshot_manifest(&self, name: &str) -> PathBuf {
        self.snapshot_dir(name).join("manifest.json")
    }

    #[must_use]
    pub fn rootfs_staging_dir(&self, id: &SandboxId, container: &str) -> PathBuf {
        self.bundle_dir(id, container).join("rootfs-staging")
    }

    #[must_use]
    pub fn rootfs_erofs(&self, id: &SandboxId, container: &str) -> PathBuf {
        self.bundle_dir(id, container).join("rootfs.erofs")
    }

    #[must_use]
    pub fn config_json(&self, id: &SandboxId, container: &str) -> PathBuf {
        self.bundle_dir(id, container).join("config.json")
    }

    #[must_use]
    pub fn runsc_state_dir(&self, id: &SandboxId) -> PathBuf {
        self.sandbox_dir(id).join("runsc-state")
    }

    #[must_use]
    pub fn pidfiles_dir(&self, id: &SandboxId) -> PathBuf {
        self.sandbox_dir(id).join("pidfiles")
    }

    #[must_use]
    pub fn pidfile(&self, id: &SandboxId, container: &str) -> PathBuf {
        self.pidfiles_dir(id).join(format!("{container}.pid"))
    }

    #[must_use]
    pub fn logs_dir(&self, id: &SandboxId) -> PathBuf {
        self.sandbox_dir(id).join("logs")
    }

    #[must_use]
    pub fn container_log(&self, id: &SandboxId, container: &str) -> PathBuf {
        self.logs_dir(id).join(format!("{container}.jsonl"))
    }
}

/// Validates a user-supplied container name.
///
/// # Errors
///
/// Returns [`ValidationError`] if the value is empty, contains unsupported
/// characters, or is the reserved name `pause`.
pub fn validate_container_name(value: &str) -> Result<(), ValidationError> {
    validate_id_segment(value)?;
    if value == PAUSE_CONTAINER {
        return Err(ValidationError::ReservedContainerName(value.to_string()));
    }
    Ok(())
}

/// Validates a user-supplied snapshot name.
///
/// # Errors
///
/// Returns [`ValidationError`] if the value is empty, too long, is `.` or `..`,
/// or contains characters other than ASCII alphanumerics, `-`, `_`, or `.`.
pub fn validate_snapshot_name(value: &str) -> Result<(), ValidationError> {
    validate_id_segment(value)
}

/// Writes `body` to `path` through a same-directory temporary file, then
/// atomically renames it into place.
///
/// # Errors
///
/// Returns any I/O error raised while creating the parent directory, writing or
/// syncing the temporary file, renaming it, or syncing the parent directory.
pub async fn write_atomic_file(path: &Path, body: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let tmp = temp_path(path);
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .await?;
        file.write_all(body).await?;
        file.flush().await?;
        drop(file);

        publish_atomic_file(&tmp, path).await
    }
    .await;

    if result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result
}

/// Publishes an already-written temporary file to `path` by syncing it,
/// renaming it into place, and syncing the parent directory.
///
/// # Errors
///
/// Returns any I/O error raised while creating the destination parent
/// directory, syncing the temporary file, renaming it, or syncing the parent
/// directory.
pub async fn publish_atomic_file(tmp: &Path, path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    sync_file(tmp).await?;
    tokio::fs::rename(tmp, path).await?;
    if let Some(parent) = path.parent() {
        sync_dir(parent).await?;
    }
    Ok(())
}

/// Opens `path` and flushes its contents to disk via `fsync`.
///
/// # Errors
///
/// Returns any I/O error raised while opening or syncing the file.
pub async fn sync_file(path: &Path) -> io::Result<()> {
    let file = tokio::fs::File::open(path).await?;
    file.sync_all().await
}

/// Opens directory `path` and flushes its entries to disk via `fsync`, making
/// prior renames into it durable.
///
/// # Errors
///
/// Returns any I/O error raised while opening or syncing the directory.
pub async fn sync_dir(path: &Path) -> io::Result<()> {
    let dir = tokio::fs::File::open(path).await?;
    dir.sync_all().await
}

/// Builds a process-unique sibling temp path for `path`, suitable for an atomic
/// publish-by-rename. Uniqueness comes from the PID and a monotonic counter.
#[must_use]
pub fn temp_path(path: &Path) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    path.with_file_name(format!(
        ".{file_name}.{}.{nanos}.{sequence}.tmp",
        std::process::id(),
    ))
}

fn validate_id_segment(value: &str) -> Result<(), ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::Empty);
    }
    if matches!(value, "." | "..") {
        return Err(ValidationError::ReservedPathSegment(value.to_string()));
    }
    if value.len() > MAX_ID_SEGMENT_LEN {
        return Err(ValidationError::TooLong {
            max: MAX_ID_SEGMENT_LEN,
        });
    }
    for ch in value.chars() {
        if !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.') {
            return Err(ValidationError::UnsupportedCharacter(ch));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_managed_network_backend_supports_checkpoints() {
        assert_eq!(
            NetworkRuntimeConfig::default().backend,
            ManagedNetworkBackend::Netstack
        );
    }

    #[test]
    fn sandboxes_dir_is_under_base_dir() {
        let paths = OadPaths::new("/run/omniagent");

        assert_eq!(
            paths.sandboxes_dir(),
            PathBuf::from("/run/omniagent/sandboxes")
        );
    }

    #[test]
    fn rejects_reserved_container_names() {
        assert!(matches!(
            validate_container_name(PAUSE_CONTAINER),
            Err(ValidationError::ReservedContainerName(_))
        ));
    }

    #[test]
    fn rejects_overlong_path_segments() {
        let value = "a".repeat(MAX_ID_SEGMENT_LEN + 1);
        assert!(matches!(
            SandboxId::new(value),
            Err(ValidationError::TooLong {
                max: MAX_ID_SEGMENT_LEN
            })
        ));
    }

    #[test]
    fn rejects_dot_path_segments() {
        for value in [".", ".."] {
            assert!(matches!(
                SandboxId::new(value),
                Err(ValidationError::ReservedPathSegment(_))
            ));
            assert!(matches!(
                validate_container_name(value),
                Err(ValidationError::ReservedPathSegment(_))
            ));
            assert!(matches!(
                validate_snapshot_name(value),
                Err(ValidationError::ReservedPathSegment(_))
            ));
        }
    }

    #[tokio::test]
    async fn write_atomic_file_creates_parent_and_cleans_temp() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("nested").join("state.json");

        write_atomic_file(&path, b"state\n").await.unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"state\n");
        let tmp_entries = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".state.json")
            })
            .count();
        assert_eq!(tmp_entries, 0);
    }
}
