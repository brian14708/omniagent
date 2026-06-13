use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use oad_core::{ContainerSpec, EnvVar, ResourceSpec, SandboxNetworkSpec, SandboxRecord};
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSandboxRequest {
    /// Optional sandbox id. When omitted, the daemon generates a UUID.
    #[serde(default)]
    pub id: Option<String>,
    /// Containers to launch in the sandbox. Must contain at least one entry
    /// unless `from_snapshot` is set, in which case the containers come from
    /// the snapshot and this field is ignored.
    #[serde(default)]
    pub containers: Vec<ContainerSpec>,
    /// When set, fork the sandbox from this snapshot instead of booting fresh
    /// containers.
    #[serde(default)]
    pub from_snapshot: Option<String>,
    /// Optional per-sandbox network policy. When omitted, the daemon default is used
    /// for fresh sandboxes and snapshot forks inherit the snapshot policy.
    #[serde(default)]
    pub network: Option<SandboxNetworkSpec>,
    /// Optional CPU/memory limits. On a snapshot fork this overrides the limits
    /// on every container restored from the snapshot, so resources can be set
    /// per session without rebaking the snapshot. When omitted, containers keep
    /// whatever limits their spec carries (typically none).
    #[serde(default)]
    pub resources: Option<ResourceSpec>,
}

/// Request to capture a snapshot of a running sandbox. The snapshot's
/// containers and pause image are taken from the source sandbox.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSnapshotRequest {
    /// Snapshot name; used as the fork source and as a path segment. When
    /// omitted, the daemon generates a unique name and returns it in the response.
    #[serde(default)]
    pub name: Option<String>,
}

/// Metadata describing a stored snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SnapshotInfo {
    pub name: String,
    /// Container names captured in the snapshot, including `pause`.
    pub containers: Vec<String>,
    /// Creation time, RFC 3339.
    pub created_at: String,
    /// Present when the snapshot was published to the content-addressed store,
    /// making it portable across nodes. Absent in legacy single-node mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cas: Option<CasSnapshotInfo>,
}

/// Details of a snapshot published to the content-addressed object store.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CasSnapshotInfo {
    /// Object key of the stored snapshot descriptor.
    pub descriptor_key: String,
    /// Total uncompressed size of the snapshot's checkpoint images, in bytes.
    pub total_bytes: u64,
    /// Bytes uploaded to the store after deduplication, in bytes.
    pub uploaded_bytes: u64,
    /// Distinct chunk hashes (hex `blake3`) the snapshot references, so the
    /// control plane can reference-count them.
    pub chunk_hashes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SnapshotResponse {
    pub snapshot: SnapshotInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListSnapshotsResponse {
    pub snapshots: Vec<SnapshotInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SandboxResponse {
    pub sandbox: SandboxRecord,
}

/// Managed-network addresses assigned to a sandbox.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SandboxNetworkResponse {
    pub sandbox_id: String,
    /// Address of the host-side gateway interface reachable from the sandbox.
    pub host_gateway_ip: String,
    /// Address assigned to the sandbox-side interface.
    pub sandbox_ip: String,
}

/// Request to run a one-off command inside a running container.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExecRequest {
    /// Container to exec in. Defaults to the first non-pause container.
    #[serde(default)]
    pub container: Option<String>,
    /// Command and arguments to execute. Must not be empty.
    pub command: Vec<String>,
    /// Additional environment variables for the executed process.
    #[serde(default)]
    pub env: Vec<EnvVar>,
    /// Working directory for the executed process.
    #[serde(default)]
    pub cwd: Option<String>,
}

/// Captured result of an [`ExecRequest`].
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExecResponse {
    pub sandbox_id: String,
    pub container: String,
    /// Exit code of the executed command (-1 if terminated by a signal).
    pub exit_code: i32,
    #[serde(
        serialize_with = "serialize_base64",
        deserialize_with = "deserialize_base64"
    )]
    pub stdout: Vec<u8>,
    #[serde(
        serialize_with = "serialize_base64",
        deserialize_with = "deserialize_base64"
    )]
    pub stderr: Vec<u8>,
}

/// Default PTY row count used when a client does not request a specific size.
pub const DEFAULT_PTY_ROWS: u16 = 40;
/// Default PTY column count used when a client does not request a specific size.
pub const DEFAULT_PTY_COLS: u16 = 120;

/// Request to start a long-running command inside a running container.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StartBackgroundExecRequest {
    /// Container to exec in. Defaults to the first non-pause container.
    #[serde(default)]
    pub container: Option<String>,
    /// Command and arguments to execute. Must not be empty.
    pub command: Vec<String>,
    /// Additional environment variables for the executed process.
    #[serde(default)]
    pub env: Vec<EnvVar>,
    /// Working directory for the executed process.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Attach the process to a pseudo-terminal instead of separate stdout and
    /// stderr pipes.
    #[serde(default)]
    pub pty: bool,
    /// Initial PTY row count. Ignored unless `pty` is true.
    #[serde(default)]
    pub rows: Option<u16>,
    /// Initial PTY column count. Ignored unless `pty` is true.
    #[serde(default)]
    pub cols: Option<u16>,
}

/// Lifecycle state for a background exec session.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackgroundExecStatus {
    Running,
    Exited,
    Failed,
}

/// Metadata for a background exec session owned by the daemon.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackgroundExecInfo {
    pub id: String,
    pub sandbox_id: String,
    pub container: String,
    pub command: Vec<String>,
    /// Whether the session is backed by a pseudo-terminal.
    #[serde(default)]
    pub pty: bool,
    pub status: BackgroundExecStatus,
    /// Exit code when the process has exited (-1 if terminated by a signal).
    #[serde(default)]
    pub exit_code: Option<i32>,
    /// Most recent fatal error for the session, if any.
    #[serde(default)]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackgroundExecResponse {
    pub exec: BackgroundExecInfo,
}

/// Request to write bytes to a background exec session's stdin.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackgroundExecStdinRequest {
    #[serde(
        default,
        serialize_with = "serialize_base64",
        deserialize_with = "deserialize_base64"
    )]
    pub data: Vec<u8>,
    /// Close stdin after writing `data`.
    #[serde(default)]
    pub close: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackgroundExecStdinResponse {
    pub accepted: bool,
}

/// Stream event emitted by a background exec session.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackgroundExecEvent {
    pub sequence: u64,
    pub exec_id: String,
    #[serde(flatten)]
    pub event: BackgroundExecEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackgroundExecEventKind {
    Stdout {
        #[serde(
            serialize_with = "serialize_base64",
            deserialize_with = "deserialize_base64"
        )]
        data: Vec<u8>,
    },
    Stderr {
        #[serde(
            serialize_with = "serialize_base64",
            deserialize_with = "deserialize_base64"
        )]
        data: Vec<u8>,
    },
    Exited {
        exit_code: i32,
    },
    Failed {
        message: String,
    },
}

impl BackgroundExecEventKind {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Exited { .. } | Self::Failed { .. })
    }
}

fn serialize_base64<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&STANDARD.encode(bytes))
}

fn deserialize_base64<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let encoded = String::deserialize(deserializer)?;
    STANDARD.decode(encoded).map_err(D::Error::custom)
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ErrorBody {
    /// Machine-readable error code, e.g. `not_found`.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

impl ErrorResponse {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                code: code.into(),
                message: message.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_minimal_create_request() {
        let req: CreateSandboxRequest = serde_json::from_str(
            r#"{"containers":[{"name":"web","image":"registry.example/web:latest"}]}"#,
        )
        .unwrap();

        assert_eq!(req.containers[0].name, "web");
        assert!(req.containers[0].command.is_empty());
        assert!(req.network.is_none());
    }

    #[test]
    fn background_exec_stdin_uses_base64_bytes() {
        let req: BackgroundExecStdinRequest =
            serde_json::from_str(r#"{"data":"aGkK","close":true}"#).unwrap();

        assert_eq!(req.data, b"hi\n");
        assert!(req.close);

        let encoded = serde_json::to_value(&req).unwrap();
        assert_eq!(encoded["data"], "aGkK");
    }

    #[test]
    fn background_exec_event_flattens_type_tag() {
        let event = BackgroundExecEvent {
            sequence: 7,
            exec_id: "exec-1".to_string(),
            event: BackgroundExecEventKind::Stdout {
                data: b"hello".to_vec(),
            },
        };

        let encoded = serde_json::to_value(&event).unwrap();

        assert_eq!(encoded["sequence"], 7);
        assert_eq!(encoded["exec_id"], "exec-1");
        assert_eq!(encoded["type"], "stdout");
        assert_eq!(encoded["data"], "aGVsbG8=");
    }
}
