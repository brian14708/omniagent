//! Uploads session artifacts to the control plane over HTTP.
//!
//! At session close the CLI POSTs each artifact's raw bytes to
//! `POST {server}/api/sessions/{id}/artifacts` with a bearer token and an
//! `X-Artifact-Kind` header. The control plane is the S3 client: it streams the
//! body into `RustFS` (S3-compatible object storage) and records an `artifacts`
//! row, returning the stored object's id, key and size.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::time::Duration;

/// Caps how long a single artifact upload can block session teardown.
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(30);

/// Where to upload artifacts and how to authenticate. Mirrors the connection
/// fields of [`crate::client::ClientConfig`].
#[derive(Debug, Clone)]
pub struct UploadConfig {
    pub server_url: String,
    pub token: String,
}

/// The control plane's response describing a stored artifact.
#[derive(Debug, Clone, Deserialize)]
pub struct UploadedArtifact {
    pub id: String,
    pub key: String,
    #[serde(default)]
    pub size: u64,
}

/// Uploads `bytes` as an artifact of `kind` (`"trajectory"`, `"recording"`,
/// `"raw_requests"`, `"session_log"`) for `session_id`.
///
/// The body is sent as `application/octet-stream` so the control plane's request
/// parser passes it through untouched; the server derives the stored content
/// type from `kind`.
///
/// # Errors
///
/// Returns an error if the request cannot be sent or the server responds with a
/// non-success status or an unparseable body.
pub async fn upload_artifact(
    client: &reqwest::Client,
    config: &UploadConfig,
    session_id: &str,
    kind: &str,
    bytes: Vec<u8>,
) -> Result<UploadedArtifact> {
    let base = http_base_url(&config.server_url);
    let url = format!("{base}/api/sessions/{session_id}/artifacts");

    let response = client
        .post(&url)
        .bearer_auth(&config.token)
        .header("X-Artifact-Kind", kind)
        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
        .timeout(UPLOAD_TIMEOUT)
        .body(bytes)
        .send()
        .await
        .with_context(|| format!("failed to upload {kind} artifact for session {session_id}"))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("artifact upload failed ({status}): {body}");
    }

    response
        .json::<UploadedArtifact>()
        .await
        .context("invalid artifact upload response")
}

/// Normalizes `server_url` into an HTTP base URL with no trailing slash.
///
/// A scheme-less host (e.g. `localhost:4000`) is given an `http://` prefix —
/// reqwest rejects schemeless URLs (it parses `localhost:` as the scheme), so
/// without this every upload fails. This mirrors [`crate::client::websocket_url`],
/// which defaults the same configs to the insecure (`ws`) scheme.
fn http_base_url(server_url: &str) -> String {
    let trimmed = server_url.trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

#[cfg(test)]
mod tests {
    use super::http_base_url;

    #[test]
    fn prefixes_schemeless_host() {
        assert_eq!(http_base_url("localhost:4000"), "http://localhost:4000");
    }

    #[test]
    fn preserves_explicit_scheme_and_trims_slash() {
        assert_eq!(
            http_base_url("https://api.example.com/"),
            "https://api.example.com"
        );
        assert_eq!(
            http_base_url("http://localhost:4000"),
            "http://localhost:4000"
        );
    }
}
