//! Recorded LLM-traffic data model and in-memory store.
//!
//! The model is deliberately close to the OpenTelemetry `GenAI` span /
//! `LangSmith` `RunTree` shape: each intercepted request/response pair becomes one
//! [`LlmSpan`] carrying the provider, model, raw request/response JSON, token
//! usage, status and timing. [`TraceStore`] keeps spans in memory and fans new
//! ones out over a `broadcast` channel, mirroring the sequenced-event pattern
//! used by the daemon's background-exec store.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::broadcast;

/// LLM provider whose wire protocol was intercepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Anthropic,
    #[serde(rename = "openai")]
    OpenAI,
    Gemini,
}

/// Token accounting extracted from a provider response, when present.
///
/// Providers spell their usage fields differently (`prompt_tokens` vs
/// `input_tokens` vs `promptTokenCount`, cached tokens nested under
/// `*_tokens_details`, …); [`Usage::from_value`] folds the known variants into
/// this single shape so downstream code and the UI stay provider-agnostic.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
// Field names are the serialized JSON contract the trace UI reads; the shared
// `_tokens` suffix is intentional, not the accidental repetition this lint flags.
#[expect(
    clippy::struct_field_names,
    reason = "token field names are the serialized trace/UI contract"
)]
pub struct Usage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
    /// Tokens spent on provider-side reasoning, when reported separately.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    /// Tokens served from the provider's prompt cache (a read hit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<u64>,
    /// Tokens written into the provider's prompt cache (a cache-creation cost).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_creation_tokens: Option<u64>,
}

impl Usage {
    /// Returns `true` when no token count was populated.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.input_tokens.is_none()
            && self.output_tokens.is_none()
            && self.total_tokens.is_none()
            && self.reasoning_tokens.is_none()
            && self.cache_read_tokens.is_none()
            && self.cache_creation_tokens.is_none()
    }

    /// Extracts usage from a provider usage object, accepting every field
    /// spelling we have seen across Anthropic, `OpenAI` and Gemini.
    ///
    /// `value` is the usage container itself — Anthropic/`OpenAI` `usage`,
    /// Gemini `usageMetadata`, or the message object when usage is inlined.
    #[must_use]
    pub fn from_value(value: &serde_json::Value) -> Self {
        Self {
            input_tokens: first_u64(
                value,
                &[
                    "input_tokens",
                    "prompt_tokens",
                    "promptTokenCount",
                    "inputTokens",
                ],
            ),
            output_tokens: first_u64(
                value,
                &[
                    "output_tokens",
                    "completion_tokens",
                    "candidatesTokenCount",
                    "outputTokens",
                ],
            ),
            total_tokens: first_u64(
                value,
                &[
                    "total_tokens",
                    "totalTokenCount",
                    "totalTokens",
                    "total_tokens_count",
                ],
            ),
            reasoning_tokens: first_u64(value, &["reasoning_tokens", "reasoningTokens"])
                .or_else(|| nested_reasoning_tokens(value)),
            cache_read_tokens: first_u64(
                value,
                &[
                    "cache_read_input_tokens",
                    "cached_tokens",
                    "cachedContentTokenCount",
                    "cacheReadInputTokens",
                ],
            )
            .or_else(|| nested_cached_tokens(value)),
            cache_creation_tokens: first_u64(
                value,
                &["cache_creation_input_tokens", "cacheWriteInputTokens"],
            ),
        }
    }

    /// Folds another reading into this one, keeping any non-`None` field. Used
    /// to merge usage scattered across multiple streamed events.
    pub const fn merge_from(&mut self, other: &Self) {
        if other.input_tokens.is_some() {
            self.input_tokens = other.input_tokens;
        }
        if other.output_tokens.is_some() {
            self.output_tokens = other.output_tokens;
        }
        if other.total_tokens.is_some() {
            self.total_tokens = other.total_tokens;
        }
        if other.reasoning_tokens.is_some() {
            self.reasoning_tokens = other.reasoning_tokens;
        }
        if other.cache_read_tokens.is_some() {
            self.cache_read_tokens = other.cache_read_tokens;
        }
        if other.cache_creation_tokens.is_some() {
            self.cache_creation_tokens = other.cache_creation_tokens;
        }
    }
}

/// Returns the first present, integer-valued key from `keys` under `value`.
fn first_u64(value: &serde_json::Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_u64))
}

/// `OpenAI` nests cached prompt tokens under `*_tokens_details.cached_tokens`.
fn nested_cached_tokens(value: &serde_json::Value) -> Option<u64> {
    ["input_tokens_details", "prompt_tokens_details"]
        .iter()
        .find_map(|key| {
            value
                .get(*key)
                .and_then(|d| d.get("cached_tokens"))
                .and_then(serde_json::Value::as_u64)
        })
}

/// `OpenAI` can nest reasoning counts under `output_tokens_details`.
fn nested_reasoning_tokens(value: &serde_json::Value) -> Option<u64> {
    ["output_tokens_details", "completion_tokens_details"]
        .iter()
        .find_map(|key| {
            value
                .get(*key)
                .and_then(|d| d.get("reasoning_tokens"))
                .and_then(serde_json::Value::as_u64)
        })
}

/// Redacted HTTP headers captured for debugging a proxied exchange.
pub type HeaderSnapshot = BTreeMap<String, Vec<String>>;

/// One structured event from a streamed response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    /// SSE event name; bare `data:` frames use `message`.
    pub event: String,
    /// Parsed `data:` payload.
    pub data: serde_json::Value,
}

/// One intercepted LLM request/response, the unit of "record & visualization".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmSpan {
    /// Stable unique id (UUID v4).
    pub id: String,
    /// Monotonic sequence assigned by the [`TraceStore`], starting at 1.
    pub sequence: u64,
    pub provider: Provider,
    /// Model name parsed from the request body or URL, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// HTTP method the agent used.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub method: String,
    /// Base URL the agent called on the local proxy.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_base_url: String,
    /// Upstream provider base URL the proxy forwarded to.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub upstream_base_url: String,
    /// Request path the agent called (e.g. `/v1/messages`).
    pub path: String,
    /// Whether the response was a streamed (SSE) body.
    pub streaming: bool,
    /// Request headers with credentials redacted.
    #[serde(default, skip_serializing_if = "HeaderSnapshot::is_empty")]
    pub request_headers: HeaderSnapshot,
    /// Parsed request body (or `{"raw": "..."}` when not JSON).
    pub request: serde_json::Value,
    /// Response headers with credentials redacted.
    #[serde(default, skip_serializing_if = "HeaderSnapshot::is_empty")]
    pub response_headers: HeaderSnapshot,
    /// Parsed response body (or `{"raw": "..."}` for streamed/non-JSON bodies).
    pub response: serde_json::Value,
    /// Structured SSE events observed while reconstructing a streamed body.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stream_events: Vec<StreamEvent>,
    #[serde(default, skip_serializing_if = "Usage::is_empty")]
    pub usage: Usage,
    /// Upstream HTTP status code (0 if the request never reached upstream).
    pub status: u16,
    /// RFC 3339 start timestamp.
    pub started_at: String,
    /// End-to-end latency in milliseconds.
    pub latency_ms: u64,
    /// Transport-level error, if the upstream call failed outright.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Fields known before the span is sequenced and stored.
#[derive(Debug, Clone)]
pub struct SpanDraft {
    pub provider: Provider,
    pub model: Option<String>,
    pub method: String,
    pub request_base_url: String,
    pub upstream_base_url: String,
    pub path: String,
    pub streaming: bool,
    pub request_headers: HeaderSnapshot,
    pub request: serde_json::Value,
    pub response_headers: HeaderSnapshot,
    pub response: serde_json::Value,
    pub stream_events: Vec<StreamEvent>,
    pub usage: Usage,
    pub status: u16,
    pub started_at: OffsetDateTime,
    pub latency_ms: u64,
    pub error: Option<String>,
}

impl SpanDraft {
    /// Convenience constructor capturing the request side at start time.
    #[must_use]
    pub fn new(provider: Provider, path: impl Into<String>) -> Self {
        Self {
            provider,
            model: None,
            method: String::new(),
            request_base_url: String::new(),
            upstream_base_url: String::new(),
            path: path.into(),
            streaming: false,
            request_headers: HeaderSnapshot::new(),
            request: serde_json::Value::Null,
            response_headers: HeaderSnapshot::new(),
            response: serde_json::Value::Null,
            stream_events: Vec::new(),
            usage: Usage::default(),
            status: 0,
            started_at: OffsetDateTime::now_utc(),
            latency_ms: 0,
            error: None,
        }
    }
}

/// In-memory, broadcast-backed store of [`LlmSpan`]s.
///
/// When built with [`TraceStore::with_sink`] each recorded span is also
/// appended to a JSONL file so a run's traffic survives the process.
#[derive(Debug)]
pub struct TraceStore {
    spans: RwLock<Vec<LlmSpan>>,
    sequence: AtomicU64,
    tx: broadcast::Sender<LlmSpan>,
    /// Optional append-only JSONL sink; one [`LlmSpan`] per line.
    sink: Option<Mutex<fs::File>>,
}

impl Default for TraceStore {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            spans: RwLock::new(Vec::new()),
            sequence: AtomicU64::new(0),
            tx,
            sink: None,
        }
    }
}

impl TraceStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a store that appends every recorded span to `path` as JSONL,
    /// creating the file (and any missing parent directories) if absent.
    ///
    /// Existing spans already in the file are left untouched and are *not*
    /// loaded into memory: each run starts with an empty store and continues
    /// appending to the archive.
    ///
    /// # Errors
    ///
    /// Returns an [`std::io::Error`] if the parent directory cannot be created
    /// or the file cannot be opened for appending.
    pub fn with_sink(path: &Path) -> std::io::Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            sink: Some(Mutex::new(file)),
            ..Self::default()
        })
    }

    /// Finalizes a [`SpanDraft`] into a sequenced [`LlmSpan`], stores it, and
    /// broadcasts it to live subscribers. Returns the assigned span id so callers
    /// can correlate out-of-band state (e.g. retained raw requests) with it.
    pub fn record(&self, draft: SpanDraft) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let started_at = draft
            .started_at
            .format(&Rfc3339)
            .unwrap_or_else(|_| String::new());
        let id = uuid::Uuid::new_v4().to_string();
        let span = LlmSpan {
            id: id.clone(),
            sequence,
            provider: draft.provider,
            model: draft.model,
            method: draft.method,
            request_base_url: draft.request_base_url,
            upstream_base_url: draft.upstream_base_url,
            path: draft.path,
            streaming: draft.streaming,
            request_headers: draft.request_headers,
            request: draft.request,
            response_headers: draft.response_headers,
            response: draft.response,
            stream_events: draft.stream_events,
            usage: draft.usage,
            status: draft.status,
            started_at,
            latency_ms: draft.latency_ms,
            error: draft.error,
        };
        // Store (and persist) before broadcasting: a subscriber that recovers
        // from a broadcast lag via `list_since` must find a span it just saw
        // announced. The reverse order leaves a window where the recovered
        // range omits the span and it is never re-delivered.
        if let Ok(mut spans) = self.spans.write() {
            spans.push(span.clone());
        }
        self.persist(&span);
        // A send error only means nobody is currently subscribed.
        let _ = self.tx.send(span);
        id
    }

    /// Appends `span` to the JSONL sink, if one is configured. Persistence is
    /// best-effort: a serialization or write failure is logged and swallowed so
    /// recording never breaks the proxy path.
    fn persist(&self, span: &LlmSpan) {
        let Some(sink) = &self.sink else { return };
        match serde_json::to_string(span) {
            Ok(line) => {
                if let Ok(mut file) = sink.lock()
                    && let Err(err) = writeln!(file, "{line}")
                {
                    tracing::warn!(error = %err, "failed to persist span");
                }
            }
            Err(err) => tracing::warn!(error = %err, "failed to serialize span for persistence"),
        }
    }

    /// Returns a snapshot of all recorded spans.
    #[must_use]
    pub fn list(&self) -> Vec<LlmSpan> {
        self.spans.read().map(|s| s.clone()).unwrap_or_default()
    }

    /// Returns recorded spans with `sequence >= from`.
    #[must_use]
    pub fn list_since(&self, from: u64) -> Vec<LlmSpan> {
        self.spans
            .read()
            .map(|spans| {
                spans
                    .iter()
                    .filter(|s| s.sequence >= from)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Subscribes to spans recorded from now on.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<LlmSpan> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_assign_increasing_sequences() {
        let store = TraceStore::new();
        store.record(SpanDraft::new(Provider::OpenAI, "/v1/chat/completions"));
        store.record(SpanDraft::new(Provider::Anthropic, "/v1/messages"));
        let spans = store.list();
        assert_eq!(spans[0].sequence, 1);
        assert_eq!(spans[1].sequence, 2);
        assert_eq!(spans.len(), 2);
        assert_eq!(store.list_since(2).len(), 1);
    }

    #[test]
    fn record_returns_the_stored_span_id() {
        let store = TraceStore::new();
        let id = store.record(SpanDraft::new(Provider::OpenAI, "/v1/chat/completions"));
        assert!(!id.is_empty());
        assert_eq!(store.list()[0].id, id);
    }

    #[test]
    fn span_round_trips_through_json() {
        let store = TraceStore::new();
        let mut draft = SpanDraft::new(Provider::Anthropic, "/v1/messages");
        draft.model = Some("claude-opus-4-8".to_string());
        draft.streaming = true;
        draft.status = 200;
        draft.usage = Usage {
            input_tokens: Some(12),
            output_tokens: Some(34),
            ..Usage::default()
        };
        draft.request = serde_json::json!({"model": "claude-opus-4-8"});
        draft.response = serde_json::json!({"stop_reason": "end_turn"});
        store.record(draft);
        let span = store.list().pop().expect("recorded span");

        let encoded = serde_json::to_string(&span).unwrap();
        let decoded: LlmSpan = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded.provider, Provider::Anthropic);
        assert_eq!(decoded.model.as_deref(), Some("claude-opus-4-8"));
        assert_eq!(decoded.usage.output_tokens, Some(34));
        assert!(decoded.streaming);
        assert_eq!(decoded.sequence, 1);
    }

    #[test]
    fn usage_is_omitted_when_empty() {
        let store = TraceStore::new();
        store.record(SpanDraft::new(Provider::Gemini, "/v1beta/x"));
        let span = store.list().pop().expect("recorded span");
        let value = serde_json::to_value(&span).unwrap();
        assert!(value.get("usage").is_none());
        assert!(value.get("model").is_none());
    }

    #[test]
    fn provider_serializes_to_stable_lowercase_labels() {
        assert_eq!(serde_json::to_value(Provider::OpenAI).unwrap(), "openai");
        assert_eq!(
            serde_json::to_value(Provider::Anthropic).unwrap(),
            "anthropic"
        );
        assert_eq!(serde_json::to_value(Provider::Gemini).unwrap(), "gemini");
    }

    #[test]
    fn usage_from_value_reads_provider_field_variants() {
        let openai = serde_json::json!({
            "prompt_tokens": 100,
            "completion_tokens": 20,
            "prompt_tokens_details": {"cached_tokens": 80},
        });
        let u = Usage::from_value(&openai);
        assert_eq!(u.input_tokens, Some(100));
        assert_eq!(u.output_tokens, Some(20));
        assert_eq!(u.cache_read_tokens, Some(80));

        let anthropic = serde_json::json!({
            "input_tokens": 7,
            "output_tokens": 9,
            "cache_read_input_tokens": 3,
            "cache_creation_input_tokens": 4,
        });
        let u = Usage::from_value(&anthropic);
        assert_eq!(u.cache_read_tokens, Some(3));
        assert_eq!(u.cache_creation_tokens, Some(4));

        let gemini = serde_json::json!({
            "promptTokenCount": 12,
            "candidatesTokenCount": 5,
            "cachedContentTokenCount": 2,
        });
        let u = Usage::from_value(&gemini);
        assert_eq!(u.input_tokens, Some(12));
        assert_eq!(u.output_tokens, Some(5));
        assert_eq!(u.cache_read_tokens, Some(2));
    }

    #[test]
    fn usage_merge_keeps_populated_fields() {
        let mut acc = Usage {
            input_tokens: Some(15),
            ..Usage::default()
        };
        acc.merge_from(&Usage {
            output_tokens: Some(42),
            ..Usage::default()
        });
        assert_eq!(acc.input_tokens, Some(15));
        assert_eq!(acc.output_tokens, Some(42));
    }

    /// A unique scratch path under the OS temp dir (reusing `uuid`, already a
    /// dependency, to avoid clashes between concurrent test runs).
    fn temp_trace_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("omniagent-traces-{}.jsonl", uuid::Uuid::new_v4()))
    }

    #[test]
    fn records_appended_to_sink_as_jsonl() {
        let path = temp_trace_path();
        {
            let store = TraceStore::with_sink(&path).expect("open sink");
            store.record(SpanDraft::new(Provider::OpenAI, "/v1/chat/completions"));
            store.record(SpanDraft::new(Provider::Anthropic, "/v1/messages"));
        }

        let contents = std::fs::read_to_string(&path).expect("read trace file");
        let spans: Vec<LlmSpan> = contents
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line is an LlmSpan"))
            .collect();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].sequence, 1);
        assert_eq!(spans[0].provider, Provider::OpenAI);
        assert_eq!(spans[1].sequence, 2);
        assert_eq!(spans[1].provider, Provider::Anthropic);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn with_sink_appends_across_stores() {
        let path = temp_trace_path();
        {
            let first = TraceStore::with_sink(&path).expect("open sink");
            first.record(SpanDraft::new(Provider::Gemini, "/v1beta/x"));
        }
        {
            // A fresh store reopening the same path must append, not truncate.
            let second = TraceStore::with_sink(&path).expect("reopen sink");
            second.record(SpanDraft::new(Provider::OpenAI, "/v1/chat/completions"));
            second.record(SpanDraft::new(Provider::Anthropic, "/v1/messages"));
        }

        let contents = std::fs::read_to_string(&path).expect("read trace file");
        assert_eq!(contents.lines().count(), 3);

        let _ = std::fs::remove_file(&path);
    }
}
