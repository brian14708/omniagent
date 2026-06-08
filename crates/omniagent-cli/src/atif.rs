//! Builds a harbor-compatible ATIF (Agent Trajectory Interchange Format)
//! `trajectory.json` from a session's recorded LLM spans.
//!
//! The data model mirrors harbor's `models/trajectories/*` Pydantic classes and
//! targets `schema_version: "ATIF-v1.7"`. Serialization matches harbor's
//! `to_json_dict(exclude_none=True)`: every optional field is omitted when
//! absent (`skip_serializing_if`).
//!
//! ## Reconstruction strategy
//!
//! Each [`LlmSpan`] is one request/response exchange whose request body carries
//! the *entire running conversation* up to that point. We therefore reconstruct
//! the conversation from the most complete (last) span's request — yielding the
//! system / user / agent / tool-result history — then append that span's
//! response as the final agent turn. Token totals in `final_metrics` are summed
//! across *all* spans so the aggregate reflects the whole session.
//!
//! Per-provider request/response extraction is implemented for Anthropic
//! (`/v1/messages`) and `OpenAI` chat completions; Gemini and any other provider
//! fall back to a response-only agent step. This is the iterative piece called
//! out in the plan — the structure is valid ATIF for every provider, and the
//! fidelity grows as extractors are added.

// ATIF field names (`step_id`, `tool_call_id`, the `*_tokens`/`total_*` metric
// fields) are the harbor-compatible wire contract, not accidental repetition.
#![allow(clippy::struct_field_names)]

use serde::Serialize;

use crate::agents::AgentInfo;
use crate::record::{LlmSpan, Provider, Usage};

/// Current ATIF schema version we emit.
const SCHEMA_VERSION: &str = "ATIF-v1.7";

/// Top-level ATIF trajectory document.
#[derive(Debug, Clone, Serialize)]
pub struct Trajectory {
    pub schema_version: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub agent: Agent,
    pub steps: Vec<Step>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_metrics: Option<FinalMetrics>,
}

/// The agent that produced the trajectory.
#[derive(Debug, Clone, Serialize)]
pub struct Agent {
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
}

/// One conversation step. `model_name`, `reasoning_content`, `tool_calls` and
/// `metrics` are only valid on `source == "agent"` (enforced by harbor's
/// validator), so we only populate them there. `extra` carries custom metadata —
/// we use it to link an agent step to its raw request (the provider response id,
/// and the matched `raw_requests` span id; see [`link_raw_requests`]).
#[derive(Debug, Clone, Serialize)]
pub struct Step {
    pub step_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    pub source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observation: Option<Observation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<serde_json::Value>,
}

/// A tool/function invocation requested by the agent.
#[derive(Debug, Clone, Serialize)]
pub struct ToolCall {
    pub tool_call_id: String,
    pub function_name: String,
    pub arguments: serde_json::Value,
}

/// Tool results observed by the agent, attached to the step that called them.
#[derive(Debug, Clone, Serialize)]
pub struct Observation {
    pub results: Vec<ObservationResult>,
}

/// One tool result, linked back to its [`ToolCall`] by `source_call_id`.
#[derive(Debug, Clone, Serialize)]
pub struct ObservationResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

/// Per-step token accounting.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Metrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<u64>,
}

impl Metrics {
    const fn is_empty(&self) -> bool {
        self.prompt_tokens.is_none()
            && self.completion_tokens.is_none()
            && self.cached_tokens.is_none()
    }

    fn from_usage(usage: &Usage) -> Option<Self> {
        let metrics = Self {
            prompt_tokens: usage.input_tokens,
            completion_tokens: usage.output_tokens,
            cached_tokens: usage.cache_read_tokens,
        };
        (!metrics.is_empty()).then_some(metrics)
    }
}

/// Aggregate token totals across the whole trajectory.
#[derive(Debug, Clone, Default, Serialize)]
pub struct FinalMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_prompt_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_completion_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_cached_tokens: Option<u64>,
    pub total_steps: u32,
}

/// Builds a trajectory from a session's spans, or `None` when there is nothing
/// to serialize (no spans → ATIF requires at least one step).
#[must_use]
pub fn build_trajectory(
    agent: AgentInfo,
    session_id: &str,
    spans: &[LlmSpan],
) -> Option<Trajectory> {
    if spans.is_empty() {
        return None;
    }

    // The last span carries the most complete conversation in its request.
    let last = spans.last()?;
    let model_name = last.model.clone();

    let mut builder = StepBuilder::default();
    builder.extend_from_request(last);
    builder.push_response(last);

    if builder.steps.is_empty() {
        return None;
    }

    let final_metrics = aggregate_metrics(spans, builder.steps.len());

    Some(Trajectory {
        schema_version: SCHEMA_VERSION,
        session_id: (!session_id.is_empty()).then(|| session_id.to_string()),
        agent: Agent {
            name: agent.name.to_string(),
            version: "unknown".to_string(),
            model_name,
        },
        steps: builder.steps,
        final_metrics: Some(final_metrics),
    })
}

/// Sums an iterator of per-step `(prompt, completion, cached)` token triples into
/// trajectory totals, preserving the "absent unless ever present" distinction and
/// saturating rather than overflowing. Shared by the span-reconstruction path
/// (here) and the native-log path ([`crate::agent_log`]).
pub fn sum_token_metrics<I>(items: I, total_steps: usize) -> FinalMetrics
where
    I: IntoIterator<Item = (Option<u64>, Option<u64>, Option<u64>)>,
{
    let mut prompt = 0u64;
    let mut completion = 0u64;
    let mut cached = 0u64;
    let (mut has_prompt, mut has_completion, mut has_cached) = (false, false, false);
    for (p, c, ca) in items {
        if let Some(v) = p {
            prompt = prompt.saturating_add(v);
            has_prompt = true;
        }
        if let Some(v) = c {
            completion = completion.saturating_add(v);
            has_completion = true;
        }
        if let Some(v) = ca {
            cached = cached.saturating_add(v);
            has_cached = true;
        }
    }
    FinalMetrics {
        total_prompt_tokens: has_prompt.then_some(prompt),
        total_completion_tokens: has_completion.then_some(completion),
        total_cached_tokens: has_cached.then_some(cached),
        total_steps: u32::try_from(total_steps).unwrap_or(u32::MAX),
    }
}

/// Sums per-span token usage into the trajectory's `final_metrics`.
fn aggregate_metrics(spans: &[LlmSpan], total_steps: usize) -> FinalMetrics {
    sum_token_metrics(
        spans.iter().map(|span| {
            (
                span.usage.input_tokens,
                span.usage.output_tokens,
                span.usage.cache_read_tokens,
            )
        }),
        total_steps,
    )
}

/// Returns the provider response id from a response body (`id`), if present.
fn response_id(response: &serde_json::Value) -> Option<&str> {
    response
        .get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
}

/// Inserts `key`→`value` into a step's `extra` object, creating it if needed.
fn set_extra_key(step: &mut Step, key: &str, value: serde_json::Value) {
    let extra = step
        .extra
        .get_or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    if let serde_json::Value::Object(obj) = extra {
        obj.insert(key.to_string(), value);
    }
}

/// Tags an agent step with the provider response id that produced it.
fn set_response_id(step: &mut Step, id: &str) {
    set_extra_key(
        step,
        "response_id",
        serde_json::Value::String(id.to_string()),
    );
}

/// Links agent steps to their raw request spans so a consumer can join the
/// trajectory to the `raw_requests` artifact.
///
/// Both the agent's transcript and the recorded proxy span carry the same
/// provider response id (e.g. Anthropic's `message.id` == the response body's
/// `id`). For any step tagged with `extra.response_id`, this stamps
/// `extra.raw_request_id` with the `id` of the span whose response shares that
/// response id. Steps with no match are left as-is.
pub fn link_raw_requests(trajectory: &mut Trajectory, spans: &[LlmSpan]) {
    let by_response: std::collections::HashMap<&str, &str> = spans
        .iter()
        .filter_map(|span| response_id(&span.response).map(|rid| (rid, span.id.as_str())))
        .collect();
    if by_response.is_empty() {
        return;
    }
    for step in &mut trajectory.steps {
        let rid = step
            .extra
            .as_ref()
            .and_then(|extra| extra.get("response_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let Some(rid) = rid else {
            continue;
        };
        if let Some(span_id) = by_response.get(rid.as_str()) {
            set_extra_key(
                step,
                "raw_request_id",
                serde_json::Value::String((*span_id).to_string()),
            );
        }
    }
}

/// Accumulates [`Step`]s while assigning sequential `step_id`s from 1.
#[derive(Default)]
struct StepBuilder {
    steps: Vec<Step>,
}

impl StepBuilder {
    fn next_id(&self) -> u32 {
        u32::try_from(self.steps.len()).unwrap_or(u32::MAX) + 1
    }

    fn push_user(&mut self, source: &'static str, message: String) {
        let step_id = self.next_id();
        self.steps.push(Step {
            step_id,
            timestamp: None,
            source,
            model_name: None,
            message,
            reasoning_content: None,
            tool_calls: None,
            observation: None,
            metrics: None,
            extra: None,
        });
    }

    fn push_agent(
        &mut self,
        model_name: Option<String>,
        message: String,
        tool_calls: Option<Vec<ToolCall>>,
        timestamp: Option<String>,
        metrics: Option<Metrics>,
    ) {
        let step_id = self.next_id();
        self.steps.push(Step {
            step_id,
            timestamp,
            source: "agent",
            model_name,
            message,
            reasoning_content: None,
            tool_calls,
            observation: None,
            metrics,
            extra: None,
        });
    }

    /// Attaches tool results to the most recent agent step (where the matching
    /// `tool_call`s live, per ATIF's `validate_tool_call_references`).
    fn attach_observation(&mut self, results: Vec<ObservationResult>) {
        if results.is_empty() {
            return;
        }
        if let Some(step) = self
            .steps
            .iter_mut()
            .rev()
            .find(|step| step.source == "agent")
        {
            match &mut step.observation {
                Some(obs) => obs.results.extend(results),
                none => *none = Some(Observation { results }),
            }
        }
    }

    /// Reconstructs the conversation history from a span's request body.
    fn extend_from_request(&mut self, span: &LlmSpan) {
        match span.provider {
            Provider::Anthropic => self.extend_anthropic(span),
            Provider::OpenAI => self.extend_openai_chat(span),
            Provider::Gemini => self.extend_gemini(span),
        }
    }

    /// Appends the span's *response* as the final agent step.
    fn push_response(&mut self, span: &LlmSpan) {
        let timestamp = (!span.started_at.is_empty()).then(|| span.started_at.clone());
        let metrics = Metrics::from_usage(&span.usage);
        let (message, tool_calls) = match span.provider {
            Provider::Anthropic => anthropic_response(&span.response),
            Provider::OpenAI => openai_response(&span.response),
            Provider::Gemini => gemini_response(&span.response),
        };
        // Skip an empty trailing agent step only if it carries nothing at all.
        if message.is_empty() && tool_calls.is_none() && metrics.is_none() {
            return;
        }
        self.push_agent(span.model.clone(), message, tool_calls, timestamp, metrics);
        // Tag the step with the provider response id so it links to the raw
        // request span (see `link_raw_requests`).
        if let Some(id) = response_id(&span.response)
            && let Some(step) = self.steps.last_mut()
        {
            set_response_id(step, id);
        }
    }

    // --- Anthropic Messages API ------------------------------------------

    fn extend_anthropic(&mut self, span: &LlmSpan) {
        let req = &span.request;
        if let Some(system) = req.get("system") {
            let text = anthropic_text(system);
            if !text.is_empty() {
                self.push_user("system", text);
            }
        }
        let Some(messages) = req.get("messages").and_then(|v| v.as_array()) else {
            return;
        };
        for message in messages {
            let role = message.get("role").and_then(|v| v.as_str()).unwrap_or("");
            let content = message.get("content").unwrap_or(&serde_json::Value::Null);
            match role {
                "assistant" => {
                    let (text, tool_calls) = anthropic_content(content);
                    self.push_agent(span.model.clone(), text, tool_calls, None, None);
                }
                "user" | "tool" => {
                    let (text, observations) = anthropic_user_content(content);
                    self.attach_observation(observations);
                    if !text.is_empty() {
                        self.push_user("user", text);
                    }
                }
                _ => {}
            }
        }
    }

    // --- OpenAI Chat Completions -----------------------------------------

    fn extend_openai_chat(&mut self, span: &LlmSpan) {
        let Some(messages) = span.request.get("messages").and_then(|v| v.as_array()) else {
            return;
        };
        for message in messages {
            let role = message.get("role").and_then(|v| v.as_str()).unwrap_or("");
            match role {
                "system" | "developer" => {
                    self.push_user("system", openai_text(message.get("content")));
                }
                "user" => {
                    self.push_user("user", openai_text(message.get("content")));
                }
                "assistant" => {
                    let text = openai_text(message.get("content"));
                    let tool_calls = openai_tool_calls(message.get("tool_calls"));
                    self.push_agent(span.model.clone(), text, tool_calls, None, None);
                }
                "tool" => {
                    let result = ObservationResult {
                        source_call_id: message
                            .get("tool_call_id")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        content: Some(openai_text(message.get("content"))),
                    };
                    self.attach_observation(vec![result]);
                }
                _ => {}
            }
        }
    }

    // --- Gemini generateContent ------------------------------------------

    fn extend_gemini(&mut self, span: &LlmSpan) {
        let req = &span.request;
        if let Some(system) = req
            .get("system_instruction")
            .or_else(|| req.get("systemInstruction"))
        {
            let text = gemini_parts_text(system.get("parts"));
            if !text.is_empty() {
                self.push_user("system", text);
            }
        }
        let Some(contents) = req.get("contents").and_then(|v| v.as_array()) else {
            return;
        };
        for content in contents {
            let role = content
                .get("role")
                .and_then(|v| v.as_str())
                .unwrap_or("user");
            let parts = content.get("parts");
            if role == "model" {
                let text = gemini_parts_text(parts);
                let tool_calls = gemini_tool_calls(parts);
                self.push_agent(span.model.clone(), text, tool_calls, None, None);
            } else {
                let observations = gemini_tool_results(parts);
                self.attach_observation(observations);
                let text = gemini_parts_text(parts);
                if !text.is_empty() {
                    self.push_user("user", text);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Provider extraction helpers
// ---------------------------------------------------------------------------

/// Flattens an Anthropic `content` (string or array of text blocks) to text.
fn anthropic_text(value: &serde_json::Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    let Some(blocks) = value.as_array() else {
        return String::new();
    };
    blocks
        .iter()
        .filter_map(|block| block.get("text").and_then(|v| v.as_str()))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Splits an Anthropic assistant `content` into text and `tool_use` calls.
fn anthropic_content(value: &serde_json::Value) -> (String, Option<Vec<ToolCall>>) {
    if let Some(text) = value.as_str() {
        return (text.to_string(), None);
    }
    let Some(blocks) = value.as_array() else {
        return (String::new(), None);
    };
    let mut text = Vec::new();
    let mut calls = Vec::new();
    for block in blocks {
        match block.get("type").and_then(|v| v.as_str()) {
            Some("text") => {
                if let Some(t) = block.get("text").and_then(|v| v.as_str()) {
                    text.push(t.to_string());
                }
            }
            Some("tool_use") => calls.push(ToolCall {
                tool_call_id: block
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                function_name: block
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                arguments: block
                    .get("input")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            }),
            _ => {}
        }
    }
    let calls = (!calls.is_empty()).then_some(calls);
    (text.join("\n"), calls)
}

/// Splits an Anthropic user `content` into plain text and `tool_result` blocks.
fn anthropic_user_content(value: &serde_json::Value) -> (String, Vec<ObservationResult>) {
    if let Some(text) = value.as_str() {
        return (text.to_string(), Vec::new());
    }
    let Some(blocks) = value.as_array() else {
        return (String::new(), Vec::new());
    };
    let mut text = Vec::new();
    let mut results = Vec::new();
    for block in blocks {
        match block.get("type").and_then(|v| v.as_str()) {
            Some("text") => {
                if let Some(t) = block.get("text").and_then(|v| v.as_str()) {
                    text.push(t.to_string());
                }
            }
            Some("tool_result") => results.push(ObservationResult {
                source_call_id: block
                    .get("tool_use_id")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
                content: Some(anthropic_text(
                    block.get("content").unwrap_or(&serde_json::Value::Null),
                )),
            }),
            _ => {}
        }
    }
    (text.join("\n"), results)
}

/// Extracts text and `tool_use` from an Anthropic Messages response body.
fn anthropic_response(response: &serde_json::Value) -> (String, Option<Vec<ToolCall>>) {
    anthropic_content(response.get("content").unwrap_or(&serde_json::Value::Null))
}

/// Flattens an `OpenAI` `content` (string or array of `{type,text}` parts).
fn openai_text(value: Option<&serde_json::Value>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    let Some(parts) = value.as_array() else {
        return String::new();
    };
    parts
        .iter()
        .filter_map(|part| part.get("text").and_then(|v| v.as_str()))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Maps `OpenAI` `tool_calls` into ATIF [`ToolCall`]s, parsing JSON arguments.
fn openai_tool_calls(value: Option<&serde_json::Value>) -> Option<Vec<ToolCall>> {
    let calls = value?.as_array()?;
    let mapped: Vec<ToolCall> = calls
        .iter()
        .map(|call| {
            let function = call.get("function");
            let arguments =
                function
                    .and_then(|f| f.get("arguments"))
                    .map_or(serde_json::Value::Null, |a| {
                        a.as_str()
                            .and_then(|s| serde_json::from_str(s).ok())
                            .unwrap_or_else(|| a.clone())
                    });
            ToolCall {
                tool_call_id: call
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                function_name: function
                    .and_then(|f| f.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                arguments,
            }
        })
        .collect();
    (!mapped.is_empty()).then_some(mapped)
}

/// Extracts the assistant message from an `OpenAI` chat-completions response.
fn openai_response(response: &serde_json::Value) -> (String, Option<Vec<ToolCall>>) {
    let message = response
        .get("choices")
        .and_then(|v| v.as_array())
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"));
    let Some(message) = message else {
        return (String::new(), None);
    };
    (
        openai_text(message.get("content")),
        openai_tool_calls(message.get("tool_calls")),
    )
}

/// Joins the text from Gemini `parts`.
fn gemini_parts_text(parts: Option<&serde_json::Value>) -> String {
    let Some(parts) = parts.and_then(|v| v.as_array()) else {
        return String::new();
    };
    parts
        .iter()
        .filter_map(|part| part.get("text").and_then(|v| v.as_str()))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Maps Gemini `functionCall` parts into ATIF [`ToolCall`]s.
fn gemini_tool_calls(parts: Option<&serde_json::Value>) -> Option<Vec<ToolCall>> {
    let parts = parts?.as_array()?;
    let mapped: Vec<ToolCall> = parts
        .iter()
        .filter_map(|part| part.get("functionCall"))
        .map(|call| {
            let function_name = call
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            ToolCall {
                // Gemini omits a call id, but its `functionResponse` result is
                // keyed by the function name; fall back to the name so the call
                // links to its observation (see `gemini_tool_results`).
                tool_call_id: call
                    .get("id")
                    .and_then(|v| v.as_str())
                    .filter(|id| !id.is_empty())
                    .map_or_else(|| function_name.clone(), str::to_string),
                function_name,
                arguments: call.get("args").cloned().unwrap_or(serde_json::Value::Null),
            }
        })
        .collect();
    (!mapped.is_empty()).then_some(mapped)
}

/// Maps Gemini `functionResponse` parts into [`ObservationResult`]s.
fn gemini_tool_results(parts: Option<&serde_json::Value>) -> Vec<ObservationResult> {
    let Some(parts) = parts.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    parts
        .iter()
        .filter_map(|part| part.get("functionResponse"))
        .map(|resp| ObservationResult {
            source_call_id: resp
                .get("name")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            content: Some(
                resp.get("response")
                    .map_or_else(String::new, ToString::to_string),
            ),
        })
        .collect()
}

/// Extracts text and `functionCall`s from a Gemini `generateContent` response.
fn gemini_response(response: &serde_json::Value) -> (String, Option<Vec<ToolCall>>) {
    let parts = response
        .get("candidates")
        .and_then(|v| v.as_array())
        .and_then(|candidates| candidates.first())
        .and_then(|candidate| candidate.get("content"))
        .and_then(|content| content.get("parts"));
    (gemini_parts_text(parts), gemini_tool_calls(parts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{Provider, SpanDraft, TraceStore};

    fn agent() -> AgentInfo {
        AgentInfo {
            name: "claude-code",
            supports_atif: true,
        }
    }

    fn span(
        provider: Provider,
        request: serde_json::Value,
        response: serde_json::Value,
    ) -> LlmSpan {
        let store = TraceStore::new();
        let mut draft = SpanDraft::new(provider, "/v1/messages");
        draft.model = Some("claude-opus-4-8".to_string());
        draft.request = request;
        draft.response = response;
        draft.usage = Usage {
            input_tokens: Some(100),
            output_tokens: Some(20),
            cache_read_tokens: Some(10),
            ..Usage::default()
        };
        store.record(draft);
        store.snapshot().pop().expect("recorded span")
    }

    #[test]
    fn empty_spans_yield_no_trajectory() {
        assert!(build_trajectory(agent(), "sess", &[]).is_none());
    }

    #[test]
    fn link_raw_requests_joins_step_to_span_by_response_id() {
        // Span whose response carries the same id the transcript step references.
        let span = span(
            Provider::Anthropic,
            serde_json::json!({}),
            serde_json::json!({"id": "msg_xyz", "content": [{"type": "text", "text": "hi"}]}),
        );
        let mut traj = Trajectory {
            schema_version: SCHEMA_VERSION,
            session_id: None,
            agent: Agent {
                name: "claude-code".to_string(),
                version: "unknown".to_string(),
                model_name: None,
            },
            steps: vec![Step {
                step_id: 1,
                timestamp: None,
                source: "agent",
                model_name: None,
                message: "hi".to_string(),
                reasoning_content: None,
                tool_calls: None,
                observation: None,
                metrics: None,
                extra: Some(serde_json::json!({"response_id": "msg_xyz"})),
            }],
            final_metrics: None,
        };

        link_raw_requests(&mut traj, std::slice::from_ref(&span));

        let extra = traj.steps[0].extra.as_ref().unwrap();
        assert_eq!(extra["response_id"], "msg_xyz");
        assert_eq!(extra["raw_request_id"], serde_json::json!(span.id));
    }

    #[test]
    fn link_raw_requests_leaves_unmatched_steps_untouched() {
        let span = span(
            Provider::Anthropic,
            serde_json::json!({}),
            serde_json::json!({"id": "msg_a"}),
        );
        let mut traj = Trajectory {
            schema_version: SCHEMA_VERSION,
            session_id: None,
            agent: Agent {
                name: "claude-code".to_string(),
                version: "unknown".to_string(),
                model_name: None,
            },
            steps: vec![Step {
                step_id: 1,
                timestamp: None,
                source: "agent",
                model_name: None,
                message: "x".to_string(),
                reasoning_content: None,
                tool_calls: None,
                observation: None,
                metrics: None,
                extra: Some(serde_json::json!({"response_id": "msg_other"})),
            }],
            final_metrics: None,
        };
        link_raw_requests(&mut traj, &[span]);
        assert!(
            traj.steps[0]
                .extra
                .as_ref()
                .unwrap()
                .get("raw_request_id")
                .is_none()
        );
    }

    #[test]
    fn anthropic_conversation_becomes_steps() {
        let request = serde_json::json!({
            "model": "claude-opus-4-8",
            "system": "be helpful",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "let me check"},
                    {"type": "tool_use", "id": "tool_1", "name": "read", "input": {"path": "a"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "tool_1", "content": "file body"}
                ]}
            ]
        });
        let response = serde_json::json!({
            "content": [{"type": "text", "text": "done"}]
        });
        let traj = build_trajectory(
            agent(),
            "sess",
            &[span(Provider::Anthropic, request, response)],
        )
        .expect("trajectory");

        assert_eq!(traj.schema_version, "ATIF-v1.7");
        assert_eq!(traj.session_id.as_deref(), Some("sess"));
        // system, user, agent(tool_use)+observation, final agent(done)
        assert_eq!(traj.steps[0].source, "system");
        assert_eq!(traj.steps[1].source, "user");
        assert_eq!(traj.steps[2].source, "agent");
        let calls = traj.steps[2].tool_calls.as_ref().expect("tool calls");
        assert_eq!(calls[0].function_name, "read");
        let obs = traj.steps[2].observation.as_ref().expect("observation");
        assert_eq!(obs.results[0].source_call_id.as_deref(), Some("tool_1"));
        assert_eq!(traj.steps.last().unwrap().message, "done");
        // step_ids are sequential from 1.
        for (i, step) in traj.steps.iter().enumerate() {
            assert_eq!(step.step_id, u32::try_from(i).unwrap() + 1);
        }
        let fm = traj.final_metrics.as_ref().unwrap();
        assert_eq!(fm.total_prompt_tokens, Some(100));
        assert_eq!(fm.total_completion_tokens, Some(20));
    }

    #[test]
    fn openai_response_becomes_agent_step() {
        let request = serde_json::json!({
            "messages": [{"role": "user", "content": "hi"}]
        });
        let response = serde_json::json!({
            "choices": [{"message": {"role": "assistant", "content": "hello"}}]
        });
        let traj = build_trajectory(agent(), "", &[span(Provider::OpenAI, request, response)])
            .expect("trajectory");
        assert!(traj.session_id.is_none());
        assert_eq!(traj.steps[0].source, "user");
        assert_eq!(traj.steps.last().unwrap().source, "agent");
        assert_eq!(traj.steps.last().unwrap().message, "hello");
    }

    #[test]
    fn agent_only_fields_absent_on_non_agent_steps() {
        let request =
            serde_json::json!({"system": "s", "messages": [{"role": "user", "content": "u"}]});
        let response = serde_json::json!({"content": [{"type": "text", "text": "a"}]});
        let traj = build_trajectory(
            agent(),
            "x",
            &[span(Provider::Anthropic, request, response)],
        )
        .expect("trajectory");
        let encoded = serde_json::to_value(&traj).unwrap();
        let steps = encoded["steps"].as_array().unwrap();
        for step in steps {
            if step["source"] != "agent" {
                assert!(step.get("model_name").is_none());
                assert!(step.get("tool_calls").is_none());
                assert!(step.get("metrics").is_none());
            }
        }
    }
}
