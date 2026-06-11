//! Multi-provider recording proxy.
//!
//! A single axum app accepts the LLM HTTP traffic of the supervised agent
//! (whose `*_BASE_URL` env vars we point here), forwards it transparently to
//! the real provider with `reqwest`, and streams the response back to the agent
//! byte-for-byte while teeing a copy into a [`TraceStore`] span. The agent's own
//! auth headers are forwarded as-is, so no credentials live in the proxy.
//!
//! Unlike a single OpenAI-compatible gateway, the three providers speak
//! different wire formats, so requests are classified by path and each
//! provider has its own model/usage extraction (Anthropic `/v1/messages`,
//! `OpenAI` `/v1/chat/completions`, Gemini `:generateContent`).

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use futures_util::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::record::{HeaderSnapshot, Provider, SpanDraft, TraceStore, Usage};
use crate::review::{ReviewDecision, ReviewItem, ReviewPhase, ReviewStore};

/// Default upstreams, overridable via each provider's base-URL environment var.
const DEFAULT_ANTHROPIC: &str = "https://api.anthropic.com";
const DEFAULT_OPENAI: &str = "https://api.openai.com";
const DEFAULT_GEMINI: &str = "https://generativelanguage.googleapis.com";

/// Known model/API endpoints. Mirroring claude-tap's path allowlist avoids this
/// local proxy becoming a blind forwarder for crawlers or unrelated agent
/// startup traffic.
const ALLOWED_PATH_PREFIXES: &[&str] = &[
    // Anthropic API.
    "/v1/messages",
    "/v1/complete",
    // OpenAI-compatible APIs.
    "/v1/responses",
    "/responses",
    "/v1/chat/completions",
    "/chat/completions",
    "/v1/completions",
    "/completions",
    "/v1/models",
    "/models",
    "/v1/embeddings",
    "/embeddings",
    "/v1/files",
    "/files",
    // Gemini public and Code Assist-style APIs.
    "/v1beta/models",
    "/v1alpha/models",
    "/v1internal",
    // Kimi/OpenRouter-compatible auxiliary endpoints used by some coding CLIs.
    "/search",
    "/fetch",
    "/usages",
    "/feedback",
];

/// Shared proxy state: the trace store, an HTTP client, and resolved upstreams.
#[derive(Clone)]
pub struct ProxyState {
    pub traces: Arc<TraceStore>,
    reviews: Arc<ReviewStore>,
    client: reqwest::Client,
    anthropic_upstream: String,
    openai_upstream: String,
    gemini_upstream: String,
    /// Model to force on every request from this session (None = pass through).
    model_override: Option<String>,
}

impl ProxyState {
    /// Builds proxy state, resolving upstream hosts from the environment.
    #[must_use]
    pub fn new(
        traces: Arc<TraceStore>,
        reviews: Arc<ReviewStore>,
        model_override: Option<String>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_mins(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            traces,
            reviews,
            client,
            anthropic_upstream: upstream_from_env(&["ANTHROPIC_BASE_URL"], DEFAULT_ANTHROPIC),
            openai_upstream: upstream_from_env(
                &[
                    "OPENAI_BASE_URL",
                    "OPENROUTER_BASE_URL",
                    "CUSTOM_BASE_URL",
                    "MOONSHOT_BASE_URL",
                    "KIMI_BASE_URL",
                ],
                DEFAULT_OPENAI,
            ),
            gemini_upstream: upstream_from_env(
                &[
                    "GOOGLE_GEMINI_BASE_URL",
                    "GOOGLE_VERTEX_BASE_URL",
                    "GEMINI_API_BASE_URL",
                ],
                DEFAULT_GEMINI,
            ),
            model_override,
        }
    }

    fn upstream_for(&self, provider: Provider) -> &str {
        match provider {
            Provider::Anthropic => &self.anthropic_upstream,
            Provider::OpenAI => &self.openai_upstream,
            Provider::Gemini => &self.gemini_upstream,
        }
    }
}

struct ReviewContext {
    method: Method,
    reqwest_method: reqwest::Method,
    path_and_query: String,
    headers: HeaderMap,
    body: Bytes,
    provider: Provider,
    upstream_base_url: String,
    request: serde_json::Value,
}

impl ReviewContext {
    fn draft(&self) -> SpanDraft {
        let path = path_from_path_and_query(&self.path_and_query);
        let mut draft = SpanDraft::new(self.provider, path);
        draft.method = self.method.as_str().to_string();
        draft.request_base_url = request_base_url(&self.headers);
        draft.upstream_base_url.clone_from(&self.upstream_base_url);
        draft.request_headers = capture_headers(&self.headers);
        draft.request = self.request.clone();
        draft.model = request_model(self.provider, &draft.request, &draft.path);
        draft
    }

    fn apply_model_override(&mut self, model: &str) {
        apply_model_override(
            self.provider,
            &mut self.request,
            &mut self.body,
            &mut self.path_and_query,
            model,
        );
    }
}

struct ReviewAttempt {
    status: reqwest::StatusCode,
    forwarded_headers: HeaderMap,
    response_bytes: Bytes,
    completed: SpanDraft,
}

fn upstream_from_env(vars: &[&str], default: &str) -> String {
    vars.iter()
        .find_map(|var| std::env::var(var).ok().filter(|v| !v.is_empty()))
        .unwrap_or_else(|| default.to_string())
        .trim_end_matches('/')
        .to_string()
}

fn upstream_url(provider: Provider, base_url: &str, path_and_query: &str) -> String {
    let path = match provider {
        Provider::Anthropic => path_and_query,
        Provider::OpenAI => strip_duplicate_prefix(base_url, path_and_query, "/v1"),
        Provider::Gemini => strip_duplicate_prefix(base_url, path_and_query, "/v1beta"),
    };
    format!("{base_url}{path}")
}

fn strip_duplicate_prefix<'a>(base_url: &str, path: &'a str, prefix: &str) -> &'a str {
    let base_path = base_url
        .trim_end_matches('/')
        .rsplit_once("://")
        .map_or(base_url, |(_, rest)| rest)
        .split_once('/')
        .map_or("", |(_, path)| path);
    let prefix = prefix.trim_start_matches('/');
    if base_path == prefix || base_path.ends_with(&format!("/{prefix}")) {
        path.strip_prefix(&format!("/{prefix}")).unwrap_or(path)
    } else {
        path
    }
}

/// Environment overrides that point a spawned agent's LLM clients at this proxy.
///
/// `base` is the proxy's root URL (e.g. `http://127.0.0.1:54321`). `OpenAI`
/// clients expect the `/v1` suffix; Anthropic and Gemini take the bare root.
/// Several coding CLIs use `OpenAI`-compatible provider variables for Kimi,
/// `OpenRouter`, Moonshot, and custom gateways, so they are pointed at the same
/// proxy too. Gemini is nudged into API-key mode because its base-URL override
/// is ignored under cached OAuth (`gemini-cli` #15430).
#[must_use]
pub fn agent_env(base: &str) -> Vec<(String, String)> {
    let base = base.trim_end_matches('/').to_string();
    let mut env = vec![
        ("ANTHROPIC_BASE_URL".to_string(), base.clone()),
        ("OPENAI_BASE_URL".to_string(), format!("{base}/v1")),
        ("OPENROUTER_BASE_URL".to_string(), format!("{base}/v1")),
        ("CUSTOM_BASE_URL".to_string(), format!("{base}/v1")),
        ("MOONSHOT_BASE_URL".to_string(), format!("{base}/v1")),
        ("KIMI_BASE_URL".to_string(), base.clone()),
        ("GOOGLE_GEMINI_BASE_URL".to_string(), base.clone()),
        ("GOOGLE_VERTEX_BASE_URL".to_string(), base.clone()),
        ("GEMINI_API_BASE_URL".to_string(), base),
        // Nudge agents that probe these toward API-key auth over OAuth.
        ("GOOGLE_GENAI_USE_VERTEXAI".to_string(), "false".to_string()),
    ];
    // Provide a placeholder Anthropic token only when one isn't already set, so
    // a configured key continues to flow through untouched.
    if std::env::var_os("ANTHROPIC_AUTH_TOKEN").is_none()
        && std::env::var_os("ANTHROPIC_API_KEY").is_none()
    {
        env.push((
            "ANTHROPIC_AUTH_TOKEN".to_string(),
            "omniagent-placeholder".to_string(),
        ));
    }
    env
}

/// Builds the proxy router. Every request is handled by [`proxy_handler`].
pub fn router(state: ProxyState) -> Router {
    Router::new()
        .route("/{*path}", any(proxy_handler))
        .route("/", any(proxy_handler))
        // The `Bytes` extractor otherwise caps bodies at axum's 2 MiB default,
        // which rejects ordinary LLM requests (long contexts, inline images)
        // with 413 before they ever reach the upstream. This proxy only fronts
        // the single supervised agent on loopback, so the limit is pure
        // breakage with no upside.
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(state)
}

/// Classifies the target provider from the request path.
fn classify(path: &str) -> Provider {
    if path.starts_with("/v1beta")
        || path.starts_with("/v1alpha")
        || path.starts_with("/v1internal")
        || path.contains(":generateContent")
        || path.contains(":streamGenerateContent")
    {
        Provider::Gemini
    } else if path.starts_with("/v1/messages") || path.starts_with("/v1/complete") {
        Provider::Anthropic
    } else {
        // Default for `/v1/chat/completions`, `/v1/responses`, `/v1/models`, ...
        Provider::OpenAI
    }
}

fn is_allowed_path(path: &str) -> bool {
    let clean = path.split('?').next().unwrap_or(path).trim_end_matches('/');
    ALLOWED_PATH_PREFIXES.iter().any(|prefix| {
        clean == *prefix
            || clean
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/') || rest.starts_with(':'))
    })
}

/// Forwards one request upstream and records the exchange.
#[expect(
    clippy::too_many_lines,
    reason = "the proxy handler keeps request preparation and streaming trace teeing together"
)]
async fn proxy_handler(
    State(state): State<ProxyState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_string();
    if !is_allowed_path(&path) {
        tracing::debug!(method = %method, path = %path, "blocked non-API proxy path");
        return error_response(StatusCode::NOT_FOUND, "not found");
    }

    let mut path_and_query = uri
        .path_and_query()
        .map_or_else(|| path.clone(), |pq| pq.as_str().to_string());
    let provider = classify(&path);
    let upstream_base_url = state.upstream_for(provider).to_string();

    let mut body = body;
    let mut request = parse_body(&body);

    // Force the session's configured model on every request (None = pass
    // through untouched). This makes model selection uniform across PTY agents
    // and codex regardless of each agent's own CLI flags. It can rewrite the
    // body and, for Gemini, the URL path, so the upstream URL is resolved after.
    if let Some(model) = clean_model(state.model_override.as_deref()) {
        apply_model_override(
            provider,
            &mut request,
            &mut body,
            &mut path_and_query,
            model,
        );
    }

    let url = upstream_url(provider, &upstream_base_url, &path_and_query);

    let mut draft = SpanDraft::new(provider, path.clone());
    draft.method = method.as_str().to_string();
    draft.request_base_url = request_base_url(&headers);
    draft.upstream_base_url = upstream_base_url.clone();
    draft.request_headers = capture_headers(&headers);
    draft.model = request_model(
        provider,
        &request,
        &path_from_path_and_query(&path_and_query),
    );
    draft.request = request;

    if state.reviews.enabled() {
        return reviewed_proxy_handler(
            state,
            method,
            path_and_query,
            headers,
            body,
            provider,
            upstream_base_url,
            draft,
        )
        .await;
    }

    let Ok(reqwest_method) = reqwest::Method::from_bytes(method.as_str().as_bytes()) else {
        return error_response(StatusCode::BAD_REQUEST, "invalid method");
    };

    let upstream = state
        .client
        .request(reqwest_method, &url)
        .headers(forward_request_headers(&headers))
        .body(body);

    let started = Instant::now();
    let resp = match upstream.send().await {
        Ok(resp) => resp,
        Err(err) => {
            draft.error = Some(err.to_string());
            draft.latency_ms = elapsed_ms(started);
            state.traces.record(draft);
            return error_response(StatusCode::BAD_GATEWAY, "upstream request failed");
        }
    };

    let status = resp.status();
    draft.status = status.as_u16();
    let streaming = is_streaming(resp.headers());
    draft.streaming = streaming;
    draft.response_headers = capture_headers(resp.headers());

    let mut response_headers = forward_response_headers(resp.headers());
    let traces = Arc::clone(&state.traces);

    // Tee: forward each chunk to the agent while accumulating a copy. The span
    // is recorded once the upstream body completes.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    tokio::spawn(async move {
        let mut acc: Vec<u8> = Vec::new();
        let mut stream = resp.bytes_stream();
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    acc.extend_from_slice(&chunk);
                    if tx.send(Ok(chunk)).await.is_err() {
                        break; // Agent hung up; keep accumulating is pointless.
                    }
                }
                Err(err) => {
                    let message = err.to_string();
                    let _ = tx.send(Err(std::io::Error::other(message.clone()))).await;
                    draft.error.get_or_insert(message);
                    break;
                }
            }
        }
        finalize_span(&traces, draft, &acc, provider, streaming, started);
    });

    let body = Body::from_stream(ReceiverStream::new(rx));
    let mut builder = Response::builder().status(status);
    if let Some(h) = builder.headers_mut() {
        response_headers.remove(header::CONTENT_LENGTH);
        *h = response_headers;
    }
    builder
        .body(body)
        .unwrap_or_else(|_| error_response(StatusCode::INTERNAL_SERVER_ERROR, "proxy error"))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the proxy router passes request context components explicitly"
)]
async fn reviewed_proxy_handler(
    state: ProxyState,
    method: Method,
    path_and_query: String,
    headers: HeaderMap,
    body: Bytes,
    provider: Provider,
    upstream_base_url: String,
    initial_draft: SpanDraft,
) -> Response {
    let Ok(reqwest_method) = reqwest::Method::from_bytes(method.as_str().as_bytes()) else {
        return error_response(StatusCode::BAD_REQUEST, "invalid method");
    };
    let mut ctx = ReviewContext {
        method,
        reqwest_method,
        path_and_query,
        headers,
        body,
        provider,
        upstream_base_url,
        request: initial_draft.request.clone(),
    };

    let mut attempt: u32 = 1;
    loop {
        let outcome = match run_review_attempt(&state, &ctx).await {
            Ok(outcome) => outcome,
            Err(response) => return response,
        };

        match state
            .reviews
            .prompt(review_item(
                &outcome.completed,
                ReviewPhase::Response,
                attempt,
            ))
            .await
        {
            ReviewDecision::Approve { .. } => {
                return approve_review_response(&state, &ctx, outcome);
            }
            ReviewDecision::Reject { message } => {
                return reject_review_response(&state, outcome.completed, message.as_deref());
            }
            ReviewDecision::Retry { model } => {
                record_regenerated_attempt(&state, outcome.completed);
                if let Some(model) = clean_model(model.as_deref()) {
                    ctx.apply_model_override(model);
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// Sends one buffered upstream request and normalizes the response into a
/// completed [`SpanDraft`]. On transport/body failure, returns the draft with
/// its `error`/`latency_ms` populated (the caller decides whether to record it).
/// This is the shared fan-out primitive for review attempts and comparisons.
async fn execute_buffered(
    client: &reqwest::Client,
    mut draft: SpanDraft,
    reqwest_method: reqwest::Method,
    url: &str,
    headers: &HeaderMap,
    body: Bytes,
    provider: Provider,
) -> Result<ReviewAttempt, SpanDraft> {
    let started = Instant::now();
    let resp = match client
        .request(reqwest_method, url)
        .headers(forward_request_headers(headers))
        .body(body)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            draft.error = Some(err.to_string());
            draft.latency_ms = elapsed_ms(started);
            return Err(draft);
        }
    };

    let status = resp.status();
    let streaming = is_streaming(resp.headers());
    let response_headers = capture_headers(resp.headers());
    let forwarded_headers = forward_response_headers(resp.headers());
    let response_bytes = match resp.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => {
            draft.status = status.as_u16();
            draft.streaming = streaming;
            draft.response_headers = response_headers;
            draft.error = Some(err.to_string());
            draft.latency_ms = elapsed_ms(started);
            return Err(draft);
        }
    };

    draft.status = status.as_u16();
    draft.streaming = streaming;
    draft.response_headers = response_headers;
    let completed = complete_span_draft(draft, &response_bytes, provider, streaming, started);
    Ok(ReviewAttempt {
        status,
        forwarded_headers,
        response_bytes,
        completed,
    })
}

async fn run_review_attempt(
    state: &ProxyState,
    ctx: &ReviewContext,
) -> Result<ReviewAttempt, Response> {
    let draft = ctx.draft();
    let url = upstream_url(ctx.provider, &ctx.upstream_base_url, &ctx.path_and_query);
    match execute_buffered(
        &state.client,
        draft,
        ctx.reqwest_method.clone(),
        &url,
        &ctx.headers,
        ctx.body.clone(),
        ctx.provider,
    )
    .await
    {
        Ok(attempt) => Ok(attempt),
        Err(errored) => {
            state.traces.record(errored);
            Err(error_response(
                StatusCode::BAD_GATEWAY,
                "upstream request failed",
            ))
        }
    }
}

fn approve_review_response(
    state: &ProxyState,
    _ctx: &ReviewContext,
    outcome: ReviewAttempt,
) -> Response {
    let status = outcome.status;
    let forwarded_headers = outcome.forwarded_headers;
    let response_bytes = outcome.response_bytes;
    state.traces.record(outcome.completed);
    buffered_response(status, forwarded_headers, response_bytes)
}

fn reject_review_response(
    state: &ProxyState,
    mut completed: SpanDraft,
    message: Option<&str>,
) -> Response {
    let message = review_rejection_message(message);
    // The agent receives a 499, so record the span with that status rather than
    // the upstream's real code; the trace then reflects what the agent saw.
    completed.status = 499;
    completed.error = Some(message.clone());
    state.traces.record(completed);
    error_response(review_rejected_status(), &message)
}

fn record_regenerated_attempt(state: &ProxyState, mut completed: SpanDraft) {
    completed.error = Some("regenerated by operator".to_string());
    state.traces.record(completed);
}

fn clean_model(model: Option<&str>) -> Option<&str> {
    model.map(str::trim).filter(|m| !m.is_empty())
}

fn review_rejection_message(message: Option<&str>) -> String {
    clean_model(message).map_or_else(|| "rejected by operator".to_string(), ToString::to_string)
}

fn review_rejected_status() -> StatusCode {
    StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST)
}

fn buffered_response(status: reqwest::StatusCode, mut headers: HeaderMap, body: Bytes) -> Response {
    headers.remove(header::CONTENT_LENGTH);
    let mut builder = Response::builder().status(status);
    if let Some(out) = builder.headers_mut() {
        *out = headers;
    }
    builder
        .body(Body::from(body))
        .unwrap_or_else(|_| error_response(StatusCode::INTERNAL_SERVER_ERROR, "proxy error"))
}

/// Records the completed exchange, extracting usage from the response body.
/// Returns the recorded span's id.
fn finalize_span(
    traces: &Arc<TraceStore>,
    draft: SpanDraft,
    body: &[u8],
    provider: Provider,
    streaming: bool,
    started: Instant,
) -> String {
    let draft = complete_span_draft(draft, body, provider, streaming, started);
    traces.record(draft)
}

fn complete_span_draft(
    mut draft: SpanDraft,
    body: &[u8],
    provider: Provider,
    streaming: bool,
    started: Instant,
) -> SpanDraft {
    draft.latency_ms = elapsed_ms(started);
    if streaming {
        // Fold the streamed deltas back into the single response object the
        // agent effectively received, so the trace shows real content and the
        // usage scan sees the terminal token counts.
        let (snapshot, usage, events) = crate::sse::reconstruct(provider, body);
        draft.usage = usage;
        draft.stream_events = events;
        if draft.model.is_none() {
            draft.model = snapshot.as_ref().and_then(response_model);
        }
        draft.response = snapshot.unwrap_or_else(|| {
            serde_json::json!({
                "streaming": true,
                "raw": String::from_utf8_lossy(body),
            })
        });
    } else {
        let value = parse_body(body);
        draft.usage = extract_usage(provider, &value);
        if draft.model.is_none() {
            draft.model = response_model(&value);
        }
        draft.response = value;
    }
    draft
}

/// Parses bytes as JSON, falling back to a `{ "raw": ... }` wrapper.
fn parse_body(body: &[u8]) -> serde_json::Value {
    if body.is_empty() {
        return serde_json::Value::Null;
    }
    serde_json::from_slice::<serde_json::Value>(body)
        .unwrap_or_else(|_| serde_json::json!({ "raw": String::from_utf8_lossy(body).to_string() }))
}

fn review_item(draft: &SpanDraft, phase: ReviewPhase, attempt: u32) -> ReviewItem {
    ReviewItem {
        id: Uuid::new_v4().to_string(),
        sequence: 0,
        phase,
        attempt,
        provider: draft.provider,
        model: draft.model.clone(),
        method: draft.method.clone(),
        path: draft.path.clone(),
        streaming: draft.streaming,
        request_base_url: draft.request_base_url.clone(),
        upstream_base_url: draft.upstream_base_url.clone(),
        request_headers: draft.request_headers.clone(),
        request: draft.request.clone(),
        response_headers: draft.response_headers.clone(),
        response: draft.response.clone(),
        usage: draft.usage.clone(),
        status: (draft.status != 0).then_some(draft.status),
        latency_ms: (draft.latency_ms != 0).then_some(draft.latency_ms),
        started_at: draft
            .started_at
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        error: draft.error.clone(),
    }
}

fn apply_model_override(
    provider: Provider,
    request: &mut serde_json::Value,
    body: &mut Bytes,
    path_and_query: &mut String,
    model: &str,
) {
    // `parse_body` represents a non-JSON body as a `{"raw": "..."}` wrapper;
    // inserting a model into that and re-encoding would replace the real
    // upstream body (e.g. a multipart `/v1/files` upload) with a corrupt JSON
    // object, so only rewrite a genuine structured request here.
    let is_raw_wrapper = request
        .as_object()
        .is_some_and(|obj| obj.len() == 1 && obj.contains_key("raw"));
    if let Some(obj) = request.as_object_mut()
        && !is_raw_wrapper
    {
        if obj.contains_key("model_id") {
            obj.insert("model_id".to_string(), serde_json::json!(model));
        } else if obj.contains_key("model") || provider != Provider::Gemini {
            obj.insert("model".to_string(), serde_json::json!(model));
        }
        if let Ok(encoded) = serde_json::to_vec(request) {
            *body = Bytes::from(encoded);
        }
    }

    if provider == Provider::Gemini {
        rewrite_gemini_model_path(path_and_query, model);
    }
}

fn rewrite_gemini_model_path(path_and_query: &mut String, model: &str) {
    let clean_model = model.strip_prefix("models/").unwrap_or(model);
    let Some((prefix, rest)) = path_and_query.split_once("models/") else {
        return;
    };
    let suffix_at = rest.find([':', '/', '?']).unwrap_or(rest.len());
    let suffix = &rest[suffix_at..];
    *path_and_query = format!("{prefix}models/{clean_model}{suffix}");
}

fn path_from_path_and_query(path_and_query: &str) -> String {
    path_and_query
        .split_once('?')
        .map_or(path_and_query, |(path, _)| path)
        .to_string()
}

/// Extracts the model name from a request body or, for Gemini, the URL path.
fn request_model(provider: Provider, request: &serde_json::Value, path: &str) -> Option<String> {
    if let Some(model) = request
        .get("model")
        .or_else(|| request.get("model_id"))
        .and_then(|m| m.as_str())
    {
        return Some(model.to_string());
    }
    if provider == Provider::Gemini {
        // `/v1beta/models/{model}:generateContent`
        let after = path.split("models/").nth(1)?;
        let model = after.split(':').next()?;
        if !model.is_empty() {
            return Some(model.to_string());
        }
    }
    None
}

fn response_model(value: &serde_json::Value) -> Option<String> {
    value
        .get("model")
        .and_then(|m| m.as_str())
        .or_else(|| value.get("model_id").and_then(|m| m.as_str()))
        .map(ToString::to_string)
}

/// Extracts token usage from a fully-parsed (non-streaming) response by
/// locating the provider's usage container and normalizing its fields.
fn extract_usage(provider: Provider, value: &serde_json::Value) -> Usage {
    let container = match provider {
        Provider::Gemini => value.get("usageMetadata"),
        Provider::Anthropic | Provider::OpenAI => value.get("usage"),
    };
    container.map(Usage::from_value).unwrap_or_default()
}

/// Copies request headers to forward upstream, dropping hop-by-hop headers and
/// `accept-encoding` (so upstream returns an uncompressed, parseable body).
fn forward_request_headers(headers: &HeaderMap) -> HeaderMap {
    let mut out = filter_headers(
        headers,
        &[
            "host",
            "content-length",
            "accept-encoding",
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "trailers",
            "transfer-encoding",
            "upgrade",
        ],
    );
    // Keep response bodies identity-encoded so the trace recorder can parse the
    // exact bytes it is teeing back to the agent.
    out.insert(
        header::ACCEPT_ENCODING,
        HeaderValue::from_static("identity"),
    );
    out
}

/// Copies response headers back to the agent, dropping length/encoding headers
/// that no longer apply to the re-streamed body.
fn forward_response_headers(headers: &HeaderMap) -> HeaderMap {
    filter_headers(
        headers,
        &[
            "content-length",
            "content-encoding",
            "transfer-encoding",
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "trailers",
            "upgrade",
        ],
    )
}

/// Clones `headers`, omitting any whose (lowercased) name is in `drop`.
fn filter_headers(headers: &HeaderMap, drop: &[&str]) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        let lname = name.as_str().to_ascii_lowercase();
        if drop.contains(&lname.as_str()) {
            continue;
        }
        out.insert(name.clone(), value.clone());
    }
    out
}

fn capture_headers(headers: &HeaderMap) -> HeaderSnapshot {
    let mut out = HeaderSnapshot::new();
    for (name, value) in headers {
        let value = value
            .to_str()
            .map_or_else(|_| "<non-utf8>".to_string(), ToString::to_string);
        out.entry(name.as_str().to_ascii_lowercase())
            .or_default()
            .push(redact_header_value(name.as_str(), &value));
    }
    out
}

fn request_base_url(headers: &HeaderMap) -> String {
    headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|host| !host.is_empty())
        .map_or_else(String::new, |host| format!("http://{host}"))
}

fn redact_header_value(name: &str, value: &str) -> String {
    if is_sensitive_header(name) {
        redact_secret(value)
    } else {
        value.to_string()
    }
}

fn is_sensitive_header(name: &str) -> bool {
    const SENSITIVE: &[&str] = &[
        "authorization",
        "x-api-key",
        "api-key",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "set-cookie2",
        "x-amz-security-token",
        "cosy-key",
        "cosy-machinetoken",
        "cosy-machine-token",
        "cosy-machineid",
        "cosy-machine-id",
        "cosy-machinetype",
        "cosy-machine-type",
        "cosy-user",
    ];
    let name = name.to_ascii_lowercase();
    SENSITIVE.contains(&name.as_str())
        || name.ends_with("-api-key")
        || name.contains("token")
        || name.contains("secret")
}

fn redact_secret(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "<empty>".to_string();
    }

    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let first = parts.next().unwrap_or_default();
    let Some(rest) = parts.next() else {
        return redact_token(trimmed);
    };
    format!("{first} {}", redact_token(rest.trim()))
}

fn redact_token(value: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= 8 {
        return format!("<redacted:{}>", chars.len());
    }

    let prefix = chars[..4].iter().collect::<String>();
    let suffix = chars[chars.len() - 4..].iter().collect::<String>();
    format!("{prefix}...{suffix} ({})", chars.len())
}

fn is_streaming(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("event-stream"))
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn error_response(status: StatusCode, message: &str) -> Response {
    let body = serde_json::json!({ "error": { "message": message } }).to_string();
    let mut resp = (status, body).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_providers_by_path() {
        assert_eq!(classify("/v1/messages"), Provider::Anthropic);
        assert_eq!(classify("/v1/complete"), Provider::Anthropic);
        assert_eq!(classify("/v1/chat/completions"), Provider::OpenAI);
        assert_eq!(classify("/v1/responses"), Provider::OpenAI);
        assert_eq!(
            classify("/v1beta/models/gemini-2.0-flash:generateContent"),
            Provider::Gemini
        );
    }

    #[test]
    fn extracts_model_from_gemini_path() {
        let model = request_model(
            Provider::Gemini,
            &serde_json::Value::Null,
            "/v1beta/models/gemini-2.0-flash:streamGenerateContent",
        );
        assert_eq!(model.as_deref(), Some("gemini-2.0-flash"));
    }

    #[test]
    fn builds_upstream_urls_from_provider_base_urls() {
        assert_eq!(
            upstream_url(
                Provider::Anthropic,
                "https://api.anthropic.com",
                "/v1/messages"
            ),
            "https://api.anthropic.com/v1/messages"
        );
        assert_eq!(
            upstream_url(
                Provider::OpenAI,
                "https://api.openai.com",
                "/v1/chat/completions"
            ),
            "https://api.openai.com/v1/chat/completions"
        );
        assert_eq!(
            upstream_url(
                Provider::OpenAI,
                "https://gateway.example/v1",
                "/v1/chat/completions"
            ),
            "https://gateway.example/v1/chat/completions"
        );
        assert_eq!(
            upstream_url(
                Provider::Gemini,
                "https://generativelanguage.googleapis.com/v1beta",
                "/v1beta/models/gemini-2.0-flash:generateContent"
            ),
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
        );
    }

    #[test]
    fn extracts_openai_and_anthropic_usage() {
        let openai = serde_json::json!({"usage": {"prompt_tokens": 10, "completion_tokens": 20}});
        let u = extract_usage(Provider::OpenAI, &openai);
        assert_eq!(u.input_tokens, Some(10));
        assert_eq!(u.output_tokens, Some(20));

        let anthropic = serde_json::json!({"usage": {"input_tokens": 7, "output_tokens": 9}});
        let u = extract_usage(Provider::Anthropic, &anthropic);
        assert_eq!(u.input_tokens, Some(7));
        assert_eq!(u.output_tokens, Some(9));
    }

    #[test]
    fn captures_headers_with_sensitive_values_redacted() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_static("sk-ant-api03-abcdef1234567890"),
        );
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer sk-proj-abcdef1234567890"),
        );
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

        let captured = capture_headers(&headers);

        assert_eq!(
            captured.get("anthropic-version").unwrap(),
            &vec!["2023-06-01".to_string()]
        );
        assert_eq!(
            captured.get("x-api-key").unwrap(),
            &vec!["sk-a...7890 (29)".to_string()]
        );
        assert_eq!(
            captured.get("authorization").unwrap(),
            &vec!["Bearer sk-p...7890 (24)".to_string()]
        );
    }

    #[test]
    fn redacts_short_and_empty_secrets() {
        assert_eq!(redact_secret(""), "<empty>");
        assert_eq!(redact_secret("short"), "<redacted:5>");
        assert_eq!(redact_secret("Bearer short"), "Bearer <redacted:5>");
    }

    #[test]
    fn model_override_leaves_non_json_body_intact() {
        // A non-JSON body is wrapped as `{"raw": ...}` by `parse_body`; applying
        // a model override must not re-encode that wrapper over the real bytes
        // forwarded upstream (e.g. a multipart upload on a compare/retry).
        let raw = b"--boundary\r\nmultipart upload".to_vec();
        let mut request = parse_body(&raw);
        let mut body = Bytes::from(raw.clone());
        let mut path_and_query = "/v1/files".to_string();
        apply_model_override(
            Provider::OpenAI,
            &mut request,
            &mut body,
            &mut path_and_query,
            "gpt-4o",
        );
        assert_eq!(body.as_ref(), raw.as_slice());
    }

    #[test]
    fn model_override_rewrites_structured_request_body() {
        // The override still applies to a genuine JSON request body.
        let original =
            serde_json::to_vec(&serde_json::json!({"model": "old", "messages": []})).unwrap();
        let mut request = parse_body(&original);
        let mut body = Bytes::from(original);
        let mut path_and_query = "/v1/chat/completions".to_string();
        apply_model_override(
            Provider::OpenAI,
            &mut request,
            &mut body,
            &mut path_and_query,
            "new",
        );
        let sent: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(sent["model"], "new");
    }

    #[test]
    fn model_override_rewrites_gemini_url_path() {
        // Gemini carries the model in the URL, so the override rewrites the path
        // (the proxy handler recomputes the upstream URL from this afterwards).
        let mut request = parse_body(b"{}");
        let mut body = Bytes::from_static(b"{}");
        let mut path_and_query =
            "/v1beta/models/gemini-1.5-flash:generateContent?key=x".to_string();
        apply_model_override(
            Provider::Gemini,
            &mut request,
            &mut body,
            &mut path_and_query,
            "gemini-2.0-pro",
        );
        assert_eq!(
            path_and_query,
            "/v1beta/models/gemini-2.0-pro:generateContent?key=x"
        );
    }

    #[test]
    fn clean_model_ignores_blank_override() {
        // A blank/whitespace model must be treated as "no override" so the proxy
        // forwards the agent's own model untouched.
        assert_eq!(clean_model(Some("")), None);
        assert_eq!(clean_model(Some("   ")), None);
        assert_eq!(clean_model(Some(" opus ")), Some("opus"));
        assert_eq!(clean_model(None), None);
    }
}
