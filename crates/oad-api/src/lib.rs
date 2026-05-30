use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use oad_core::{ContainerSpec, EnvVar, SandboxRecord};
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};
use utoipa::{IntoParams, ToSchema};

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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListSandboxesResponse {
    pub sandboxes: Vec<SandboxRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LogsResponse {
    pub sandbox_id: String,
    pub container: String,
    pub lines: Vec<String>,
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

#[derive(Debug, Clone, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct LogsQuery {
    /// Container to read logs from. Defaults to the first non-pause container.
    pub container: Option<String>,
    /// Maximum number of trailing log lines to return (capped at 5000, default 200).
    pub tail: Option<usize>,
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
    }
}
