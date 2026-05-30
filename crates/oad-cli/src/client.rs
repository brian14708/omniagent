//! Thin HTTP client over the oad daemon's `/v1` API.
//!
//! Request and response bodies reuse the daemon's own DTO crates (`oad-api`
//! and `oad-core`) so the client can never drift from the server contract.

use anyhow::{Context, Result, anyhow};
use oad_api::{
    CreateSandboxRequest, CreateSnapshotRequest, ErrorResponse, ExecRequest, ExecResponse,
    ListSandboxesResponse, ListSnapshotsResponse, LogsResponse, SandboxResponse, SnapshotResponse,
};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;

/// Client bound to a single daemon base URL and bearer token.
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
    pub async fn health(&self) -> Result<serde_json::Value> {
        let resp = self.send(self.http.get(self.url("/healthz"))).await?;
        Self::read_json(resp).await
    }

    /// `POST /v1/sandboxes` — create and start a sandbox.
    pub async fn create(&self, request: &CreateSandboxRequest) -> Result<SandboxResponse> {
        let resp = self
            .send(self.http.post(self.url("/v1/sandboxes")).json(request))
            .await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/sandboxes` — list all known sandboxes.
    pub async fn list(&self) -> Result<ListSandboxesResponse> {
        let resp = self.send(self.http.get(self.url("/v1/sandboxes"))).await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/sandboxes/{id}` — fetch a single sandbox.
    pub async fn get(&self, id: &str) -> Result<SandboxResponse> {
        let resp = self
            .send(self.http.get(self.url(&format!("/v1/sandboxes/{id}"))))
            .await?;
        Self::read_json(resp).await
    }

    /// `DELETE /v1/sandboxes/{id}` — stop and delete a sandbox.
    pub async fn delete(&self, id: &str) -> Result<SandboxResponse> {
        let resp = self
            .send(self.http.delete(self.url(&format!("/v1/sandboxes/{id}"))))
            .await?;
        Self::read_json(resp).await
    }

    /// `GET /v1/sandboxes/{id}/logs` — read recent container log lines.
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

    /// `POST /v1/sandboxes/{id}/exec` — run a command inside a container.
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

    /// `POST /v1/sandboxes/{id}/suspend` — checkpoint and tear down a sandbox.
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
    pub async fn list_snapshots(&self) -> Result<ListSnapshotsResponse> {
        let resp = self.send(self.http.get(self.url("/v1/snapshots"))).await?;
        Self::read_json(resp).await
    }

    /// `DELETE /v1/snapshots/{name}` — delete a snapshot (204 No Content).
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
