//! Self-registration with the `OmniAgent` control plane.
//!
//! When `[control_plane]` is configured, the daemon announces itself to the
//! control plane over HTTP on a heartbeat interval so the control plane can
//! discover live oad instances and call their `/v1` API directly. The payload
//! carries this daemon's own bearer token (so the control plane can authenticate
//! to `/v1`) and the in-container path of the `omniagent` binary (supplied via a
//! static mount). On shutdown the daemon best-effort deregisters.

use std::sync::Arc;
use std::time::Duration;

use oad_core::ControlPlaneConfig;
use serde_json::json;
use tokio::task::JoinHandle;
use tracing::warn;

/// Heartbeat cadence; the control plane expires instances after a few missed
/// beats.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Drives registration + heartbeat for one control plane.
pub struct Registration {
    cp: ControlPlaneConfig,
    /// This daemon's own `/v1` bearer token, handed to the control plane.
    api_token: String,
    instance_id: String,
    http: reqwest::Client,
}

impl Registration {
    /// Builds a registration client. `api_token` is the daemon's `/v1` bearer
    /// token. `instance_id` should be stable for the process lifetime.
    #[must_use]
    pub fn new(cp: ControlPlaneConfig, api_token: String, instance_id: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            cp,
            api_token,
            instance_id,
            http,
        }
    }

    fn register_url(&self) -> String {
        format!("{}/api/oad/register", self.cp.url.trim_end_matches('/'))
    }

    fn payload(&self) -> serde_json::Value {
        json!({
            "instance_id": self.instance_id,
            "name": self.cp.name,
            "advertise_url": self.cp.advertise_url,
            "api_token": self.api_token,
            "version": env!("CARGO_PKG_VERSION"),
            "capabilities": {
                "omniagent_path": self.cp.omniagent_path,
                "control_plane_url": self.cp.url,
                "static_mount": true,
            },
        })
    }

    /// Sends one registration heartbeat.
    async fn beat(&self) {
        match self
            .http
            .post(self.register_url())
            .bearer_auth(&self.cp.register_token)
            .json(&self.payload())
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => warn!(status = %resp.status(), "control-plane registration rejected"),
            Err(err) => warn!(error = %err, "control-plane registration failed"),
        }
    }

    /// Spawns the heartbeat loop, returning its task handle. Abort it on shutdown
    /// and call [`Registration::deregister`].
    #[must_use]
    pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
            loop {
                ticker.tick().await;
                self.beat().await;
            }
        })
    }

    /// Best-effort deregistration on shutdown.
    pub async fn deregister(&self) {
        let url = format!("{}/{}", self.register_url(), self.instance_id);
        let _ = self
            .http
            .delete(url)
            .bearer_auth(&self.cp.register_token)
            .send()
            .await;
    }
}
