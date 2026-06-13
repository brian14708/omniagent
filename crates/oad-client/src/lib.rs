//! Reusable HTTP client for the oad sandbox daemon's `/v1` API.
//!
//! Request and response bodies reuse the daemon's own DTO crates (`oad-api`
//! and `oad-core`) so the client can never drift from the server contract.
//! This crate is the shared client used by both `oadctl` and the omniagent
//! runner; keep it free of CLI/binary concerns.

use anyhow::{Context, Result, anyhow};
use oad_api::{
    BackgroundExecEvent, BackgroundExecResizeRequest, BackgroundExecResizeResponse,
    BackgroundExecResponse, BackgroundExecStdinRequest, BackgroundExecStdinResponse,
    CreateSandboxRequest, CreateSnapshotRequest, ErrorResponse, ExecRequest, ExecResponse,
    ListBackgroundExecsResponse, ListSandboxesResponse, ListSnapshotsResponse, LogsResponse,
    SandboxNetworkResponse, SandboxResponse, SnapshotResponse, StartBackgroundExecRequest,
};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;

/// Client bound to a single daemon base URL and bearer token.
#[derive(Clone)]
pub struct OadClient {
    base_url: String,
    token: String,
    http: Client,
}

impl OadClient {
    /// Builds a client for `base_url`, authenticating with `token`.
    ///
    /// `token` may be empty for endpoints that do not require auth (e.g.
    /// `/healthz`); authenticated endpoints will return `401` in that case.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be constructed.
    pub fn new(base_url: &str, token: String) -> Result<Self> {
        let http = Client::builder()
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            token,
            http,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// Sends a request with the bearer token attached, mapping transport
    /// failures to a friendly "is the daemon running?" hint.
    async fn send(&self, request: RequestBuilder) -> Result<Response> {
        request
            .bearer_auth(&self.token)
            .send()
            .await
            .with_context(|| {
                format!(
                    "request to {} failed; is the daemon running?",
                    self.base_url
                )
            })
    }

    /// Reads a response body, decoding it as `T` on success or surfacing the
    /// daemon's structured [`ErrorResponse`] (falling back to raw text).
    async fn read_json<T: DeserializeOwned>(resp: Response) -> Result<T> {
        let status = resp.status();
        let bytes = resp.bytes().await.context("failed to read response body")?;
        if status.is_success() {
            return serde_json::from_slice(&bytes)
                .context("failed to decode response from oad daemon");
        }
        Err(Self::decode_error(status, &bytes))
    }

    fn decode_error(status: StatusCode, bytes: &[u8]) -> anyhow::Error {
        if let Ok(parsed) = serde_json::from_slice::<ErrorResponse>(bytes) {
            anyhow!(
                "oad returned {} {}: {}",
                status.as_u16(),
                parsed.error.code,
                parsed.error.message
            )
        } else {
            let body = String::from_utf8_lossy(bytes);
            anyhow!("oad returned {}: {}", status.as_u16(), body.trim())
        }
    }

    /// `GET /healthz` — liveness probe (no auth required).
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn health(&self) -> Result<serde_json::Value> {
        let resp = self.send(self.http.get(self.url("/healthz"))).await?;
        Self::read_json(resp).await
    }

    /// `POST /v1/sandboxes` — create and start a sandbox.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn create(&self, request: &CreateSandboxRequest) -> Result<SandboxResponse> {
        let resp = self
            .send(self.http.post(self.url("/v1/sandboxes")).json(request))
            .await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/sandboxes` — list all known sandboxes.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn list(&self) -> Result<ListSandboxesResponse> {
        let resp = self.send(self.http.get(self.url("/v1/sandboxes"))).await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/sandboxes/{id}` — fetch a single sandbox.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn get(&self, id: &str) -> Result<SandboxResponse> {
        let resp = self
            .send(self.http.get(self.url(&format!("/v1/sandboxes/{id}"))))
            .await?;
        Self::read_json(resp).await
    }

    /// `DELETE /v1/sandboxes/{id}` — stop and delete a sandbox.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn delete(&self, id: &str) -> Result<SandboxResponse> {
        let resp = self
            .send(self.http.delete(self.url(&format!("/v1/sandboxes/{id}"))))
            .await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/sandboxes/{id}/logs` — read recent container log lines.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn logs(
        &self,
        id: &str,
        container: Option<&str>,
        tail: Option<usize>,
    ) -> Result<LogsResponse> {
        let mut query: Vec<(&str, String)> = Vec::new();
        if let Some(container) = container {
            query.push(("container", container.to_string()));
        }
        if let Some(tail) = tail {
            query.push(("tail", tail.to_string()));
        }
        let resp = self
            .send(
                self.http
                    .get(self.url(&format!("/v1/sandboxes/{id}/logs")))
                    .query(&query),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/sandboxes/{id}/network` — managed-network addresses.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, managed networking is disabled,
    /// the daemon rejects the request, or the response body cannot be decoded.
    pub async fn network(&self, id: &str) -> Result<SandboxNetworkResponse> {
        let resp = self
            .send(
                self.http
                    .get(self.url(&format!("/v1/sandboxes/{id}/network"))),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `POST /v1/sandboxes/{id}/exec` — run a command inside a container.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn exec(&self, id: &str, request: &ExecRequest) -> Result<ExecResponse> {
        let resp = self
            .send(
                self.http
                    .post(self.url(&format!("/v1/sandboxes/{id}/exec")))
                    .json(request),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `POST /v1/sandboxes/{id}/execs` — start a background exec session.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn start_exec(
        &self,
        id: &str,
        request: &StartBackgroundExecRequest,
    ) -> Result<BackgroundExecResponse> {
        let resp = self
            .send(
                self.http
                    .post(self.url(&format!("/v1/sandboxes/{id}/execs")))
                    .json(request),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/sandboxes/{id}/execs` — list background exec sessions.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn list_execs(&self, id: &str) -> Result<ListBackgroundExecsResponse> {
        let resp = self
            .send(
                self.http
                    .get(self.url(&format!("/v1/sandboxes/{id}/execs"))),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/sandboxes/{id}/execs/{exec_id}` — get session metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn get_exec(&self, id: &str, exec_id: &str) -> Result<BackgroundExecResponse> {
        let resp = self
            .send(
                self.http
                    .get(self.url(&format!("/v1/sandboxes/{id}/execs/{exec_id}"))),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `DELETE /v1/sandboxes/{id}/execs/{exec_id}` — request session kill.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn kill_exec(&self, id: &str, exec_id: &str) -> Result<BackgroundExecResponse> {
        let resp = self
            .send(
                self.http
                    .delete(self.url(&format!("/v1/sandboxes/{id}/execs/{exec_id}"))),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `POST /v1/sandboxes/{id}/execs/{exec_id}/stdin` — write attached stdin.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn write_exec_stdin(
        &self,
        id: &str,
        exec_id: &str,
        request: &BackgroundExecStdinRequest,
    ) -> Result<BackgroundExecStdinResponse> {
        let resp = self
            .send(
                self.http
                    .post(self.url(&format!("/v1/sandboxes/{id}/execs/{exec_id}/stdin")))
                    .json(request),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `POST /v1/sandboxes/{id}/execs/{exec_id}/resize` — resize a PTY exec.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the exec is not PTY-backed, the
    /// daemon rejects the request, or the response body cannot be decoded.
    pub async fn resize_exec(
        &self,
        id: &str,
        exec_id: &str,
        request: &BackgroundExecResizeRequest,
    ) -> Result<BackgroundExecResizeResponse> {
        let resp = self
            .send(
                self.http
                    .post(self.url(&format!("/v1/sandboxes/{id}/execs/{exec_id}/resize")))
                    .json(request),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/sandboxes/{id}/execs/{exec_id}/events` — open the SSE stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or the daemon rejects it.
    pub async fn exec_events(&self, id: &str, exec_id: &str, from: u64) -> Result<Response> {
        let resp = self
            .send(
                self.http
                    .get(self.url(&format!("/v1/sandboxes/{id}/execs/{exec_id}/events")))
                    .query(&[("from", from.to_string())]),
            )
            .await?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        let bytes = resp.bytes().await.context("failed to read response body")?;
        Err(Self::decode_error(status, &bytes))
    }

    /// `POST /v1/sandboxes/{id}/suspend` — checkpoint and tear down a sandbox.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn suspend(&self, id: &str) -> Result<SandboxResponse> {
        let resp = self
            .send(
                self.http
                    .post(self.url(&format!("/v1/sandboxes/{id}/suspend"))),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `POST /v1/sandboxes/{id}/resume` — restore a suspended sandbox.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn resume(&self, id: &str) -> Result<SandboxResponse> {
        let resp = self
            .send(
                self.http
                    .post(self.url(&format!("/v1/sandboxes/{id}/resume"))),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `POST /v1/sandboxes/{id}/snapshot` — snapshot a running sandbox.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn snapshot(
        &self,
        id: &str,
        request: &CreateSnapshotRequest,
    ) -> Result<SnapshotResponse> {
        let resp = self
            .send(
                self.http
                    .post(self.url(&format!("/v1/sandboxes/{id}/snapshot")))
                    .json(request),
            )
            .await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/snapshots` — list all snapshots.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the daemon rejects it, or the
    /// response body cannot be decoded.
    pub async fn list_snapshots(&self) -> Result<ListSnapshotsResponse> {
        let resp = self.send(self.http.get(self.url("/v1/snapshots"))).await?;
        Self::read_json(resp).await
    }

    /// `DELETE /v1/snapshots/{name}` — delete a snapshot (204 No Content).
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or the daemon rejects it.
    pub async fn delete_snapshot(&self, name: &str) -> Result<()> {
        let resp = self
            .send(self.http.delete(self.url(&format!("/v1/snapshots/{name}"))))
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let bytes = resp.bytes().await.context("failed to read response body")?;
        Err(Self::decode_error(status, &bytes))
    }
}

/// Splits the next complete SSE frame (terminated by a blank line) off the
/// front of `buffer`, returning it without the trailing delimiter.
#[must_use]
pub fn take_sse_frame(buffer: &mut String) -> Option<String> {
    let idx = buffer.find("\n\n")?;
    let frame = buffer[..idx].to_string();
    buffer.drain(..idx + 2);
    Some(frame)
}

/// Parses one SSE frame into a [`BackgroundExecEvent`], joining `data:` lines
/// and ignoring comments/empty lines. Returns `Ok(None)` for frames with no
/// data lines (e.g. keep-alive comments).
///
/// # Errors
///
/// Returns an error when the joined data payload is not valid JSON.
pub fn parse_sse_event(frame: &str) -> Result<Option<BackgroundExecEvent>> {
    let mut data_lines = Vec::new();
    for line in frame.lines() {
        if line.starts_with(':') || line.is_empty() {
            continue;
        }
        let Some(rest) = line.strip_prefix("data:") else {
            continue;
        };
        data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
    }
    if data_lines.is_empty() {
        return Ok(None);
    }
    let data = data_lines.join("\n");
    serde_json::from_str(&data)
        .map(Some)
        .with_context(|| format!("failed to parse SSE event payload: {data}"))
}
