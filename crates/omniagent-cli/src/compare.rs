//! Multi-model side-by-side comparison.
//!
//! Replays a previously captured request against several models on the *same*
//! provider/endpoint and collects their responses so the web UI can show them
//! side-by-side. This is an analysis tool only — it never alters the live
//! agent's response.
//!
//! Stored [`crate::record::LlmSpan`]s have their request headers redacted, so a
//! span cannot be replayed from the trace store alone (the auth header is gone).
//! Instead the proxy retains the *raw* request (real forwarded headers + body)
//! here, keyed by the recorded span id, in a bounded in-memory ring. These raw
//! requests are **never serialized** to the UI; only the parsed request body and
//! the comparison responses are.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use axum::body::Bytes;
use axum::http::HeaderMap;
use serde::Serialize;
use time::format_description::well_known::Rfc3339;
use tokio::sync::broadcast;

use crate::record::{Provider, Usage};

/// How many raw captured requests to retain for replay before evicting the
/// oldest. Bounds how long auth headers linger in memory.
const CAPTURE_CAPACITY: usize = 256;

/// A raw request retained for replay. **Server-only — never serialized.**
#[derive(Clone)]
pub struct CapturedRequest {
    pub method: reqwest::Method,
    pub path_and_query: String,
    /// Original (un-forwarded) request headers, including the auth header.
    pub headers: HeaderMap,
    pub body: Bytes,
    pub provider: Provider,
    pub upstream_base_url: String,
    /// Parsed request body, reused as the base for per-model overrides.
    pub request_json: serde_json::Value,
}

/// State of one model's response within a comparison run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VariantState {
    Pending,
    Done,
    Error,
}

/// One model's response to the shared request.
#[derive(Debug, Clone, Serialize)]
pub struct CompareVariant {
    pub model: String,
    pub state: VariantState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub response: serde_json::Value,
    #[serde(skip_serializing_if = "Usage::is_empty")]
    pub usage: Usage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl CompareVariant {
    /// A not-yet-started variant for the given model.
    #[must_use]
    pub fn pending(model: String) -> Self {
        Self {
            model,
            state: VariantState::Pending,
            status: None,
            response: serde_json::Value::Null,
            usage: Usage::default(),
            latency_ms: None,
            error: None,
        }
    }
}

/// One comparison: a shared request fanned out across several models.
#[derive(Debug, Clone, Serialize)]
pub struct CompareRun {
    pub id: String,
    pub sequence: u64,
    pub created_at: String,
    pub provider: Provider,
    pub path: String,
    pub source_span_id: String,
    /// The shared request body (no secrets) shown for context in the UI.
    pub request: serde_json::Value,
    pub variants: Vec<CompareVariant>,
}

/// Snapshot returned by `GET /api/compare`.
#[derive(Debug, Clone, Serialize)]
pub struct CompareList {
    pub default_models: Vec<String>,
    pub runs: Vec<CompareRun>,
}

/// Live update pushed over the comparison SSE stream.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CompareEvent {
    Reset { runs: Vec<CompareRun> },
    Upsert { run: Box<CompareRun> },
}

/// Bounded, insertion-ordered map of captured raw requests.
#[derive(Default)]
struct CaptureRing {
    order: VecDeque<String>,
    map: HashMap<String, CapturedRequest>,
}

impl CaptureRing {
    fn insert(&mut self, span_id: String, req: CapturedRequest) {
        if self.map.insert(span_id.clone(), req).is_none() {
            self.order.push_back(span_id);
            while self.order.len() > CAPTURE_CAPACITY {
                if let Some(evicted) = self.order.pop_front() {
                    self.map.remove(&evicted);
                }
            }
        }
    }

    fn get(&self, span_id: &str) -> Option<CapturedRequest> {
        self.map.get(span_id).cloned()
    }
}

/// In-memory, broadcast-backed store of comparison runs plus the bounded ring of
/// raw requests retained for replay.
pub struct CompareStore {
    runs: RwLock<BTreeMap<String, CompareRun>>,
    captured: Mutex<CaptureRing>,
    sequence: AtomicU64,
    tx: broadcast::Sender<CompareEvent>,
    default_models: Vec<String>,
    client: reqwest::Client,
}

impl CompareStore {
    #[must_use]
    pub fn new(default_models: Vec<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_mins(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let (tx, _) = broadcast::channel(256);
        Self {
            runs: RwLock::new(BTreeMap::new()),
            captured: Mutex::new(CaptureRing::default()),
            sequence: AtomicU64::new(0),
            tx,
            default_models,
            client,
        }
    }

    #[must_use]
    pub fn default_models(&self) -> &[String] {
        &self.default_models
    }

    pub const fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Retains a raw request for later replay, keyed by its recorded span id.
    pub fn capture(&self, span_id: String, req: CapturedRequest) {
        if let Ok(mut ring) = self.captured.lock() {
            ring.insert(span_id, req);
        }
    }

    /// Returns a clone of the raw request for `span_id`, if still retained.
    pub fn captured(&self, span_id: &str) -> Option<CapturedRequest> {
        self.captured.lock().ok().and_then(|ring| ring.get(span_id))
    }

    #[must_use]
    pub fn list(&self) -> CompareList {
        let mut runs = self
            .runs
            .read()
            .map(|runs| runs.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        runs.sort_by_key(|run| run.sequence);
        CompareList {
            default_models: self.default_models.clone(),
            runs,
        }
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<CompareEvent> {
        self.tx.subscribe()
    }

    /// Creates a run with one pending variant per model, stores it, broadcasts
    /// it, and returns its id.
    pub fn create_run(
        &self,
        source_span_id: &str,
        captured: &CapturedRequest,
        path: String,
        models: &[String],
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let run = CompareRun {
            id: id.clone(),
            sequence,
            created_at: time::OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_default(),
            provider: captured.provider,
            path,
            source_span_id: source_span_id.to_string(),
            request: captured.request_json.clone(),
            variants: models
                .iter()
                .map(|model| CompareVariant::pending(model.clone()))
                .collect(),
        };
        self.upsert(run);
        id
    }

    /// Replaces the variant at `index` in `run_id` and rebroadcasts the run.
    pub fn complete_variant(&self, run_id: &str, index: usize, variant: CompareVariant) {
        let updated = {
            let Ok(mut runs) = self.runs.write() else {
                return;
            };
            let Some(run) = runs.get_mut(run_id) else {
                return;
            };
            if let Some(slot) = run.variants.get_mut(index) {
                *slot = variant;
            }
            run.clone()
        };
        let _ = self.tx.send(CompareEvent::Upsert {
            run: Box::new(updated),
        });
    }

    fn upsert(&self, run: CompareRun) {
        if let Ok(mut runs) = self.runs.write() {
            runs.insert(run.id.clone(), run.clone());
        }
        let _ = self.tx.send(CompareEvent::Upsert { run: Box::new(run) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn captured() -> CapturedRequest {
        CapturedRequest {
            method: reqwest::Method::POST,
            path_and_query: "/v1/messages".to_string(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
            provider: Provider::Anthropic,
            upstream_base_url: "https://api.anthropic.com".to_string(),
            request_json: serde_json::json!({"model": "a"}),
        }
    }

    #[test]
    fn capture_ring_evicts_oldest_past_capacity() {
        let store = CompareStore::new(vec![]);
        for i in 0..(CAPTURE_CAPACITY + 10) {
            store.capture(format!("span-{i}"), captured());
        }
        // The first 10 were evicted; the most recent CAPACITY remain.
        assert!(store.captured("span-0").is_none());
        assert!(store.captured("span-9").is_none());
        assert!(store.captured("span-10").is_some());
        assert!(
            store
                .captured(&format!("span-{}", CAPTURE_CAPACITY + 9))
                .is_some()
        );
    }

    #[test]
    fn create_run_and_complete_variant() {
        let store = CompareStore::new(vec![]);
        let cap = captured();
        let models = vec!["m1".to_string(), "m2".to_string()];
        let run_id = store.create_run("span-1", &cap, "/v1/messages".to_string(), &models);

        let run = store.list().runs.into_iter().next().unwrap();
        assert_eq!(run.variants.len(), 2);
        assert!(
            run.variants
                .iter()
                .all(|v| v.state == VariantState::Pending)
        );

        let mut variant = CompareVariant::pending("m2".to_string());
        variant.state = VariantState::Done;
        variant.status = Some(200);
        store.complete_variant(&run_id, 1, variant);

        let run = store.list().runs.into_iter().next().unwrap();
        assert_eq!(run.variants[0].state, VariantState::Pending);
        assert_eq!(run.variants[1].state, VariantState::Done);
        assert_eq!(run.variants[1].status, Some(200));
    }

    #[test]
    fn list_reports_default_models() {
        let store = CompareStore::new(vec!["x".to_string(), "y".to_string()]);
        assert_eq!(store.list().default_models, vec!["x", "y"]);
    }
}
