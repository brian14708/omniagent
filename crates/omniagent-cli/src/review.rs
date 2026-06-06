//! Human-in-the-loop review gate for intercepted LLM calls.
//!
//! When enabled, the proxy publishes a pending response item here and waits on
//! a one-shot decision from the web UI before it releases the response.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, oneshot};

use crate::record::{HeaderSnapshot, Provider, Usage};

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
}

#[derive(Debug, Clone, Serialize)]
pub struct ReviewList {
    pub enabled: bool,
    pub items: Vec<ReviewItem>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReviewEvent {
    Upsert { item: Box<ReviewItem> },
    Remove { id: String },
    Reset { items: Vec<ReviewItem> },
}

#[derive(Debug)]
pub struct ReviewStore {
    enabled: AtomicBool,
    /// How long `prompt` waits for a human decision before auto-approving.
    /// `None` waits forever.
    timeout: Option<Duration>,
    items: RwLock<BTreeMap<String, ReviewItem>>,
    sequence: AtomicU64,
    tx: broadcast::Sender<ReviewEvent>,
    waiters: Mutex<BTreeMap<String, oneshot::Sender<ReviewDecision>>>,
}

impl ReviewStore {
    #[must_use]
    pub fn new(enabled: bool, timeout: Option<Duration>) -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            enabled: AtomicBool::new(enabled),
            timeout,
            items: RwLock::new(BTreeMap::new()),
            sequence: AtomicU64::new(0),
            tx,
            waiters: Mutex::new(BTreeMap::new()),
        }
    }

    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Toggles review gating at runtime. The startup screen sets this per the
    /// chosen mode before the agent is launched.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    #[must_use]
    pub fn list(&self) -> ReviewList {
        let mut items = self
            .items
            .read()
            .map(|items| items.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        items.sort_by_key(|item| item.sequence);
        ReviewList {
            enabled: self.enabled(),
            items,
        }
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ReviewEvent> {
        self.tx.subscribe()
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
        let _ = self.tx.send(ReviewEvent::Upsert {
            item: Box::new(item),
        });

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
        let _ = self.tx.send(ReviewEvent::Remove { id: id.to_string() });
    }
}
