//! Human-in-the-loop review gate for intercepted LLM calls.
//!
//! When enabled, the proxy publishes a pending response item here and waits on
//! a one-shot decision from the central UI before it releases the response.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::record::{HeaderSnapshot, Provider, Usage};

pub type ReviewForwarder = Arc<dyn Fn(&ReviewEvent) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewPhase {
    Request,
    Response,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewItem {
    pub id: String,
    pub sequence: u64,
    pub phase: ReviewPhase,
    pub attempt: u32,
    pub provider: Provider,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub method: String,
    pub path: String,
    pub streaming: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_base_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub upstream_base_url: String,
    #[serde(default, skip_serializing_if = "HeaderSnapshot::is_empty")]
    pub request_headers: HeaderSnapshot,
    pub request: serde_json::Value,
    #[serde(default, skip_serializing_if = "HeaderSnapshot::is_empty")]
    pub response_headers: HeaderSnapshot,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub response: serde_json::Value,
    #[serde(skip_serializing_if = "Usage::is_empty")]
    pub usage: Usage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    pub started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ReviewDecision {
    Approve {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },
    Reject {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    Retry {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },
}

impl ReviewDecision {
    #[must_use]
    pub const fn approve() -> Self {
        Self::Approve { model: None }
    }

    #[must_use]
    pub fn from_server_value(value: &serde_json::Value) -> Self {
        let action = value
            .get("action")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("approve");
        match action {
            "reject" => Self::Reject {
                message: value
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string),
            },
            "retry" => Self::Retry {
                model: value
                    .get("model")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string),
            },
            _ => Self::Approve {
                model: value
                    .get("model")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReviewEvent {
    Upsert { item: Box<ReviewItem> },
    Remove { id: String },
}

pub struct ReviewStore {
    enabled: AtomicBool,
    /// How long `prompt` waits for a human decision before auto-approving.
    /// `None` waits forever.
    timeout: Option<Duration>,
    items: RwLock<BTreeMap<String, ReviewItem>>,
    sequence: AtomicU64,
    waiters: Mutex<BTreeMap<String, oneshot::Sender<ReviewDecision>>>,
    forwarder: Option<ReviewForwarder>,
}

impl ReviewStore {
    #[must_use]
    pub fn new(enabled: bool, timeout: Option<Duration>) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
            timeout,
            items: RwLock::new(BTreeMap::new()),
            sequence: AtomicU64::new(0),
            waiters: Mutex::new(BTreeMap::new()),
            forwarder: None,
        }
    }

    #[must_use]
    pub fn with_forwarder(mut self, forwarder: ReviewForwarder) -> Self {
        self.forwarder = Some(forwarder);
        self
    }

    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub async fn prompt(&self, mut item: ReviewItem) -> ReviewDecision {
        if !self.enabled() {
            return ReviewDecision::approve();
        }

        item.sequence = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let id = item.id.clone();
        let (tx, rx) = oneshot::channel();

        if let Ok(mut waiters) = self.waiters.lock() {
            waiters.insert(id.clone(), tx);
        }
        if let Ok(mut items) = self.items.write() {
            items.insert(id.clone(), item.clone());
        }
        let event = ReviewEvent::Upsert {
            item: Box::new(item),
        };
        self.forward(&event);

        let decision = match self.timeout {
            // Auto-approve if no human decides within the window, so unattended
            // runs are not blocked indefinitely.
            Some(timeout) => match tokio::time::timeout(timeout, rx).await {
                Ok(received) => received,
                Err(_elapsed) => {
                    self.discard(&id);
                    return ReviewDecision::approve();
                }
            },
            None => rx.await,
        };

        decision.unwrap_or_else(|_| {
            self.discard(&id);
            ReviewDecision::Reject {
                message: Some("review channel closed".to_string()),
            }
        })
    }

    pub fn decide(&self, id: &str, decision: ReviewDecision) -> bool {
        let sender = self
            .waiters
            .lock()
            .ok()
            .and_then(|mut waiters| waiters.remove(id));
        let Some(sender) = sender else {
            return false;
        };
        self.remove(id);
        sender.send(decision).is_ok()
    }

    /// Drop a pending waiter and its item without delivering a decision.
    fn discard(&self, id: &str) {
        if let Ok(mut waiters) = self.waiters.lock() {
            waiters.remove(id);
        }
        self.remove(id);
    }

    fn remove(&self, id: &str) {
        if let Ok(mut items) = self.items.write() {
            items.remove(id);
        }
        let event = ReviewEvent::Remove { id: id.to_string() };
        self.forward(&event);
    }

    fn forward(&self, event: &ReviewEvent) {
        if let Some(forwarder) = &self.forwarder {
            forwarder(event);
        }
    }
}
