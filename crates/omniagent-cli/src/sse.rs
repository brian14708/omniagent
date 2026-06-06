//! Streamed-response reassembly.
//!
//! Providers answer a streaming request with a `text/event-stream` body of
//! incremental deltas rather than one JSON object. Recording the raw byte blob
//! is useless for inspection, so this module folds the deltas back into the
//! single response object the agent effectively received — text, extended
//! thinking, and tool calls included — mirroring the approach in
//! `claude-tap`'s `SSEReassembler`.
//!
//! Three wire protocols are handled:
//!
//! * **Anthropic** `/v1/messages` — typed events (`message_start`,
//!   `content_block_*`, `message_delta`) carrying an Anthropic message whose
//!   `content` is an array of typed blocks.
//! * **`OpenAI`** — either Chat Completions (bare `data:` frames with a
//!   `choices[].delta`) or the Responses API (`response.completed` snapshots).
//! * **Gemini** `:streamGenerateContent` — bare `data:` frames each holding a
//!   `GenerateContentResponse` with `candidates` and `usageMetadata`.
//!
//! Every reconstruction also carries a normalized Anthropic-shape `content`
//! array so the viewer can render any provider through one code path.

use serde_json::{Value, json};

use crate::record::{Provider, StreamEvent, Usage};

/// Reconstructs the full response object and folds usage from an accumulated
/// streamed (SSE) body. Returns `None` when nothing parseable was found, so the
/// caller can fall back to recording the raw text.
///
/// The wire protocol is inferred per-event from its shape rather than the
/// nominal `provider`, since a single agent may speak more than one format
/// (e.g. `OpenAI` Chat Completions and Responses) through the same endpoint.
#[must_use]
pub fn reconstruct(_provider: Provider, body: &[u8]) -> (Option<Value>, Usage, Vec<StreamEvent>) {
    let mut state = Reassembler::new();
    let events = parse_events(body);
    for event in &events {
        state.feed(event.event.as_deref(), &event.data);
    }
    let (snapshot, usage) = state.finish();
    let stream_events = events
        .into_iter()
        .map(|event| StreamEvent {
            event: event.event.unwrap_or_else(|| {
                event
                    .data
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("message")
                    .to_string()
            }),
            data: event.data,
        })
        .collect();
    (snapshot, usage, stream_events)
}

struct ParsedEvent {
    event: Option<String>,
    data: Value,
}

/// Splits an SSE body into `(event, data)` pairs, parsing each `data:` payload
/// as JSON and preserving non-JSON payloads as strings. The `[DONE]` sentinel
/// is skipped because it is protocol framing noise rather than content.
fn parse_events(body: &[u8]) -> Vec<ParsedEvent> {
    let text = String::from_utf8_lossy(body);
    let mut events = Vec::new();
    let mut event: Option<String> = None;
    let mut data_lines: Vec<&str> = Vec::new();

    let mut flush = |event: &mut Option<String>, data_lines: &mut Vec<&str>| {
        if event.is_none() && data_lines.is_empty() {
            return;
        }
        let payload = data_lines.join("\n");
        data_lines.clear();
        let typed = event.take();
        if payload.is_empty() || payload == "[DONE]" {
            return;
        }
        let data = serde_json::from_str::<Value>(&payload).unwrap_or_else(|_| json!(payload));
        events.push(ParsedEvent { event: typed, data });
    };

    for raw in text.lines() {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.trim());
        } else if line.is_empty() {
            flush(&mut event, &mut data_lines);
        }
    }
    flush(&mut event, &mut data_lines);
    events
}

/// Accumulates streamed events into a single response snapshot plus usage.
struct Reassembler {
    snapshot: Option<Value>,
    usage: Usage,
}

impl Reassembler {
    fn new() -> Self {
        Self {
            snapshot: None,
            usage: Usage::default(),
        }
    }

    fn finish(mut self) -> (Option<Value>, Usage) {
        // The snapshot's own usage object is only a fallback. Anthropic's
        // `message_start` carries a placeholder `output_tokens: 1` (the real
        // count arrives later in `message_delta`'s sibling `usage`), so merging
        // the snapshot *last* would clobber the authoritative per-event total
        // back to 1. Seed from the snapshot, then let the accumulated per-event
        // usage win.
        let mut usage = Usage::default();
        if let Some(snapshot) = &self.snapshot {
            for key in ["usage", "usageMetadata"] {
                if let Some(value) = snapshot.get(key) {
                    usage.merge_from(&Usage::from_value(value));
                }
            }
        }
        usage.merge_from(&self.usage);

        // Build the normalized `content` mirror once, from the final `output`.
        // It is a pure derived view read only here, so rebuilding it on every
        // streamed delta would be quadratic; a no-op for non-Responses snapshots.
        self.refresh_responses_content_mirror();

        // Parse any tool-call argument buffer that is now complete. Chat
        // Completions has no per-block stop event, so the end of the stream is
        // the OpenAI counterpart of Anthropic's `content_block_stop` finalize;
        // doing it here (rather than after every delta) avoids prematurely
        // finalizing on a fragment that is only coincidentally valid JSON.
        if let Some(content) = self
            .snapshot
            .as_mut()
            .and_then(Value::as_object_mut)
            .and_then(|obj| obj.get_mut("content"))
            .and_then(Value::as_array_mut)
        {
            for block in content.iter_mut() {
                finalize_partial_json(block);
            }
        }

        (self.snapshot, usage)
    }

    fn feed(&mut self, event: Option<&str>, data: &Value) {
        let event = event.or_else(|| data.get("type").and_then(Value::as_str));
        match event {
            Some("message_start") => self.snapshot = data.get("message").cloned(),
            Some("content_block_start") => self.content_block_start(data),
            Some("content_block_delta") => self.content_block_delta(data),
            Some("content_block_stop") => self.content_block_stop(data),
            Some("message_delta") => self.message_delta(data),
            Some(e) if e.starts_with("response.") => self.response_event(e, data),
            // Bare `data:` frames: OpenAI Chat Completions or Gemini, told apart
            // by shape. Typed events we don't model are ignored.
            _ if data.get("choices").is_some() => self.chat_completion_chunk(data),
            _ if data.get("candidates").is_some() => self.gemini_chunk(data),
            _ => {}
        }
        self.merge_event_usage(data);
    }

    /// Folds any usage object found directly on an event into the running total.
    fn merge_event_usage(&mut self, data: &Value) {
        for key in ["usage", "usageMetadata"] {
            if let Some(value) = data.get(key) {
                self.usage.merge_from(&Usage::from_value(value));
            }
        }
    }

    // --- Anthropic ---------------------------------------------------------

    fn content_block_start(&mut self, data: &Value) {
        let Some(block) = data.get("content_block").cloned() else {
            return;
        };
        let idx = index_of(data);
        let content = self.content_mut();
        grow_to(content, idx);
        content[idx] = block;
    }

    fn content_block_delta(&mut self, data: &Value) {
        let idx = index_of(data);
        let delta = data.get("delta").cloned().unwrap_or(Value::Null);
        let content = self.content_mut();
        grow_to(content, idx);
        // A delta may arrive before its `content_block_start` (or none is sent);
        // seed a typed block from the delta so the block carries a `type`, as
        // claude-tap's `_empty_content_block_for_delta` does.
        if content[idx].get("type").is_none() {
            content[idx] = empty_block_for_delta(&delta);
        }
        let block = &mut content[idx];
        match delta.get("type").and_then(Value::as_str) {
            Some("text_delta") => append_str(block, "text", str_field(&delta, "text")),
            Some("thinking_delta") => {
                append_str(block, "thinking", str_field(&delta, "thinking"));
                if let Some(sig) = delta.get("signature").and_then(Value::as_str) {
                    block["signature"] = json!(sig);
                }
            }
            Some("input_json_delta") => {
                append_str(block, "_partial_json", str_field(&delta, "partial_json"));
            }
            _ => {}
        }
    }

    fn content_block_stop(&mut self, data: &Value) {
        let idx = index_of(data);
        let content = self.content_mut();
        if let Some(block) = content.get_mut(idx) {
            finalize_partial_json(block);
        }
    }

    fn message_delta(&mut self, data: &Value) {
        let Some(snapshot) = self.snapshot.as_mut().and_then(Value::as_object_mut) else {
            return;
        };
        if let Some(delta) = data.get("delta").and_then(Value::as_object) {
            for (key, value) in delta {
                snapshot.insert(key.clone(), value.clone());
            }
        }
    }

    // --- OpenAI Responses --------------------------------------------------

    fn response_event(&mut self, event: &str, data: &Value) {
        // `response.created`/`completed`/`done` wrap the full response object;
        // the terminal events otherwise carry it at the top level.
        if let Some(response) = data.get("response") {
            self.set_response_snapshot(response.clone());
        } else if matches!(event, "response.completed" | "response.done") {
            self.set_response_snapshot(data.clone());
        } else if event == "response.output_item.added" || event == "response.output_item.done" {
            self.response_output_item(data);
        } else if event
            .rsplit_once('.')
            .is_some_and(|(_, suffix)| suffix == "delta")
        {
            self.response_delta(event, data);
        }
    }

    fn set_response_snapshot(&mut self, mut response: Value) {
        let existing_output = self
            .snapshot
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|obj| obj.get("output"))
            .and_then(Value::as_array)
            .filter(|output| !output.is_empty())
            .cloned();
        let existing_content = self
            .snapshot
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|obj| obj.get("content"))
            .and_then(Value::as_array)
            .filter(|content| !content.is_empty())
            .cloned();

        if let Some(obj) = response.as_object_mut() {
            let missing_output = obj
                .get("output")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty);
            if missing_output && let Some(output) = existing_output {
                obj.insert("output".to_string(), Value::Array(output));
            }
            if !obj.contains_key("content")
                && let Some(content) = existing_content
            {
                obj.insert("content".to_string(), Value::Array(content));
            }
        }

        self.snapshot = Some(response);
    }

    /// Accumulates an `OpenAI` Responses `output_item.*` event into the snapshot's
    /// `output` list. This keeps traces useful even when the terminal
    /// `response.completed` payload is skeletal or a compatible gateway omits
    /// the full response object.
    fn response_output_item(&mut self, data: &Value) {
        let Some(item) = data.get("item").cloned() else {
            return;
        };
        let idx = data
            .get("output_index")
            .and_then(Value::as_u64)
            .and_then(|i| usize::try_from(i).ok())
            .unwrap_or_else(|| self.response_output_mut().len());
        let output = self.response_output_mut();
        grow_to(output, idx);
        output[idx] = item;
    }

    /// Handles the common text/refusal/reasoning delta events from the Responses
    /// API by appending into `output[].content[]` at the provided indices.
    fn response_delta(&mut self, event: &str, data: &Value) {
        let text = data
            .get("delta")
            .and_then(Value::as_str)
            .or_else(|| data.get("text").and_then(Value::as_str))
            .or_else(|| data.get("summary_text").and_then(Value::as_str))
            .unwrap_or("");
        if text.is_empty() {
            return;
        }

        let output_index = data
            .get("output_index")
            .and_then(Value::as_u64)
            .and_then(|i| usize::try_from(i).ok())
            .unwrap_or(0);

        if event.contains("reasoning") || event.contains("summary") {
            let summary_index = data
                .get("summary_index")
                .and_then(Value::as_u64)
                .and_then(|i| usize::try_from(i).ok())
                .unwrap_or(0);
            let output = self.response_output_mut();
            grow_to(output, output_index);
            if !output[output_index].is_object() {
                output[output_index] = json!({"type": "reasoning", "summary": []});
            }
            let item = output[output_index]
                .as_object_mut()
                .expect("item is object");
            item.entry("type".to_string())
                .or_insert_with(|| json!("reasoning"));
            let summary = array_entry(item, "summary");
            grow_to(summary, summary_index);
            if !summary[summary_index].is_object() {
                summary[summary_index] = json!({"type": "summary_text", "text": ""});
            }
            append_str(&mut summary[summary_index], "text", text);
            return;
        }

        let content_index = data
            .get("content_index")
            .and_then(Value::as_u64)
            .and_then(|i| usize::try_from(i).ok())
            .unwrap_or(0);

        let block_type = if event.contains("refusal") {
            "refusal"
        } else {
            // `summary`/`reasoning` deltas already returned above, so only
            // refusal and plain output text reach here.
            "output_text"
        };
        let field = if block_type == "refusal" {
            "refusal"
        } else {
            "text"
        };

        let output = self.response_output_mut();
        grow_to(output, output_index);
        if !output[output_index].is_object() {
            output[output_index] = json!({"type": "message", "role": "assistant", "content": []});
        }
        let item = output[output_index]
            .as_object_mut()
            .expect("item is object");
        item.entry("type".to_string())
            .or_insert_with(|| json!("message"));
        item.entry("role".to_string())
            .or_insert_with(|| json!("assistant"));
        let content = array_entry(item, "content");
        grow_to(content, content_index);
        if !content[content_index].is_object() {
            content[content_index] = json!({"type": block_type, field: ""});
        }
        if content[content_index].get("type").is_none() {
            content[content_index]["type"] = json!(block_type);
        }
        append_str(&mut content[content_index], field, text);
    }

    // --- OpenAI Chat Completions -------------------------------------------

    fn chat_completion_chunk(&mut self, data: &Value) {
        let choice = data
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|c| c.first())
            .cloned()
            .unwrap_or(Value::Null);
        let delta = choice.get("delta").cloned().unwrap_or(Value::Null);

        if self.snapshot.is_none() {
            self.snapshot = Some(json!({
                "id": data.get("id").cloned().unwrap_or_else(|| json!("")),
                "object": "chat.completion",
                "model": data.get("model").cloned().unwrap_or_else(|| json!("")),
                "content": [],
            }));
        }

        if let Some(text) = delta.get("content").and_then(Value::as_str)
            && !text.is_empty()
        {
            append_str(self.text_block(), "text", text);
        }
        if let Some(reason) = delta.get("reasoning_content").and_then(Value::as_str)
            && !reason.is_empty()
        {
            append_str(self.thinking_block(), "thinking", reason);
        }
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                self.chat_tool_call_delta(call);
            }
        }
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str)
            && let Some(obj) = self.snapshot.as_mut().and_then(Value::as_object_mut)
        {
            obj.insert("stop_reason".to_string(), json!(reason));
        }
    }

    /// Applies one indexed Chat-Completions tool-call delta to a mirrored
    /// `tool_use` content block, concatenating the streamed argument fragments.
    fn chat_tool_call_delta(&mut self, call: &Value) {
        let idx = index_of(call);
        let block = self.tool_use_block(idx);
        if let Some(id) = call.get("id").and_then(Value::as_str)
            && !id.is_empty()
        {
            block["id"] = json!(id);
        }
        if let Some(func) = call.get("function") {
            if let Some(name) = func.get("name").and_then(Value::as_str)
                && !name.is_empty()
            {
                block["name"] = json!(name);
            }
            if let Some(args) = func.get("arguments").and_then(Value::as_str) {
                append_str(block, "_partial_json", args);
            }
        }
    }

    // --- Gemini ------------------------------------------------------------

    fn gemini_chunk(&mut self, data: &Value) {
        if self.snapshot.is_none() {
            self.snapshot = Some(json!({ "candidates": [], "content": [] }));
        }
        let parts = data
            .get("candidates")
            .and_then(Value::as_array)
            .and_then(|c| c.first())
            .and_then(|c| c.get("content"))
            .and_then(|c| c.get("parts"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for part in &parts {
            if let Some(text) = part.get("text").and_then(Value::as_str) {
                append_str(self.text_block(), "text", text);
            }
        }
        // Preserve the latest finish/safety metadata on the snapshot.
        if let (Some(obj), Some(candidates)) = (
            self.snapshot.as_mut().and_then(Value::as_object_mut),
            data.get("candidates").cloned(),
        ) {
            obj.insert("candidates".to_string(), candidates);
        }
    }

    // --- normalized content helpers ----------------------------------------

    /// Borrows the snapshot's `content` array, creating it (and an empty object
    /// snapshot) if absent.
    fn content_mut(&mut self) -> &mut Vec<Value> {
        let snapshot = self.snapshot.get_or_insert_with(|| json!({}));
        if !snapshot.is_object() {
            *snapshot = json!({});
        }
        let obj = snapshot.as_object_mut().expect("snapshot is an object");
        array_entry(obj, "content")
    }

    /// Borrows the `OpenAI` Responses `output` array, creating a response-shaped
    /// snapshot if necessary.
    fn response_output_mut(&mut self) -> &mut Vec<Value> {
        let snapshot = self.snapshot.get_or_insert_with(|| json!({}));
        if !snapshot.is_object() {
            *snapshot = json!({});
        }
        let obj = snapshot.as_object_mut().expect("snapshot is an object");
        obj.entry("object".to_string())
            .or_insert_with(|| json!("response"));
        array_entry(obj, "output")
    }

    /// Mirrors Responses `output` into the normalized Anthropic-style `content`
    /// array that the lightweight viewer already knows how to render.
    fn refresh_responses_content_mirror(&mut self) {
        let Some(output) = self
            .snapshot
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|obj| obj.get("output"))
            .and_then(Value::as_array)
        else {
            return;
        };
        let mut content = Vec::new();
        for item in output {
            match item.get("type").and_then(Value::as_str) {
                Some("message") => {
                    for block in item
                        .get("content")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        match block.get("type").and_then(Value::as_str) {
                            Some("output_text" | "input_text") => {
                                content.push(json!({
                                    "type": "text",
                                    "text": block.get("text").and_then(Value::as_str).unwrap_or(""),
                                }));
                            }
                            Some("refusal") => {
                                content.push(json!({
                                    "type": "text",
                                    "text": block.get("refusal").and_then(Value::as_str).unwrap_or(""),
                                }));
                            }
                            _ => {}
                        }
                    }
                }
                Some(kind) if kind.ends_with("_call") => {
                    content.push(json!({
                        "type": "tool_use",
                        "id": item.get("call_id").or_else(|| item.get("id")).cloned().unwrap_or_else(|| json!("")),
                        "name": response_tool_name(item),
                        "input": response_tool_input(item),
                    }));
                }
                Some("reasoning") => {
                    let text = item
                        .get("summary")
                        .and_then(Value::as_array)
                        .map(|summary| {
                            summary
                                .iter()
                                .filter_map(|part| {
                                    part.get("text")
                                        .or_else(|| part.get("summary_text"))
                                        .and_then(Value::as_str)
                                })
                                .collect::<Vec<_>>()
                                .join("")
                        })
                        .unwrap_or_default();
                    if !text.is_empty() {
                        content.push(json!({"type": "thinking", "thinking": text}));
                    }
                }
                _ => {}
            }
        }
        if let Some(obj) = self.snapshot.as_mut().and_then(Value::as_object_mut) {
            obj.insert("content".to_string(), Value::Array(content));
        }
    }

    /// Borrows the first `text` block in `content`, appending one if none.
    fn text_block(&mut self) -> &mut Value {
        let content = self.content_mut();
        if !content.iter().any(|b| block_type(b) == "text") {
            content.push(json!({"type": "text", "text": ""}));
        }
        content
            .iter_mut()
            .find(|b| block_type(b) == "text")
            .expect("text block present")
    }

    /// Borrows the first `thinking` block in `content`, prepending one if none.
    fn thinking_block(&mut self) -> &mut Value {
        let content = self.content_mut();
        if !content.iter().any(|b| block_type(b) == "thinking") {
            content.insert(0, json!({"type": "thinking", "thinking": ""}));
        }
        content
            .iter_mut()
            .find(|b| block_type(b) == "thinking")
            .expect("thinking block present")
    }

    /// Borrows the `idx`-th mirrored `tool_use` block, creating blocks as
    /// needed. Tool blocks live after the text/thinking mirrors.
    fn tool_use_block(&mut self, idx: usize) -> &mut Value {
        let content = self.content_mut();
        if let Some(pos) = content
            .iter()
            .enumerate()
            .filter(|(_, b)| block_type(b) == "tool_use")
            .map(|(i, _)| i)
            .nth(idx)
        {
            return &mut content[pos];
        }
        let existing = content
            .iter()
            .filter(|b| block_type(b) == "tool_use")
            .count();
        for _ in existing..=idx {
            content.push(json!({"type": "tool_use", "id": "", "name": "", "input": {}}));
        }
        let pos = content.len() - 1;
        &mut content[pos]
    }
}

/// Reads an event's `index` field (default 0) as a list position.
fn index_of(data: &Value) -> usize {
    data.get("index")
        .and_then(Value::as_u64)
        .and_then(|i| usize::try_from(i).ok())
        .unwrap_or(0)
}

fn str_field<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}

fn block_type(block: &Value) -> &str {
    block.get("type").and_then(Value::as_str).unwrap_or("")
}

fn response_tool_name(item: &Value) -> String {
    if item.get("type").and_then(Value::as_str) == Some("tool_search_call") {
        return "tool_search".to_string();
    }
    if let Some(name) = item.get("name").and_then(Value::as_str)
        && !name.is_empty()
    {
        return name.to_string();
    }
    item.get("type")
        .and_then(Value::as_str)
        .and_then(|kind| kind.strip_suffix("_call"))
        .unwrap_or("")
        .to_string()
}

fn response_tool_input(item: &Value) -> Value {
    if let Some(arguments) = item.get("arguments") {
        if let Some(text) = arguments.as_str() {
            return serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!(text));
        }
        return arguments.clone();
    }

    let Some(obj) = item.as_object() else {
        return json!({});
    };
    let mut input = serde_json::Map::new();
    for (key, value) in obj {
        if matches!(
            key.as_str(),
            "id" | "type" | "status" | "call_id" | "name" | "execution"
        ) {
            continue;
        }
        input.insert(key.clone(), value.clone());
    }
    Value::Object(input)
}

/// Pushes empty objects onto `content` until index `idx` is addressable.
fn grow_to(content: &mut Vec<Value>, idx: usize) {
    while content.len() <= idx {
        content.push(json!({}));
    }
}

/// Borrows `obj[key]` as an array, resetting it to `[]` when absent or holding
/// a non-array value. Folding a streamed body can otherwise meet a provider
/// snapshot whose `content`/`output`/`summary` is a non-array JSON value (a
/// malformed or hostile upstream), where a bare `as_array_mut().expect(...)`
/// would panic the reassembly task and drop the agent's response.
fn array_entry<'a>(obj: &'a mut serde_json::Map<String, Value>, key: &str) -> &'a mut Vec<Value> {
    let slot = obj.entry(key).or_insert_with(|| json!([]));
    if !slot.is_array() {
        *slot = json!([]);
    }
    slot.as_array_mut().expect("slot coerced to an array")
}

/// Builds the typed empty block a `content_block_delta` implies, used when the
/// matching `content_block_start` was missing — mirrors claude-tap's
/// `_empty_content_block_for_delta`.
fn empty_block_for_delta(delta: &Value) -> Value {
    match delta.get("type").and_then(Value::as_str) {
        Some("thinking_delta") => json!({"type": "thinking", "thinking": ""}),
        Some("input_json_delta") => json!({"type": "tool_use", "id": "", "name": "", "input": {}}),
        _ => json!({"type": "text", "text": ""}),
    }
}

/// Appends `text` to a string field on `block`, creating it if absent.
fn append_str(block: &mut Value, key: &str, text: &str) {
    if !block.is_object() {
        *block = json!({});
    }
    let obj = block.as_object_mut().expect("block is an object");
    match obj.get_mut(key) {
        // Extend the existing buffer in place rather than rebuilding it, so a
        // long streamed field stays O(n) over its deltas instead of O(n²).
        Some(Value::String(existing)) => existing.push_str(text),
        _ => {
            obj.insert(key.to_string(), json!(text));
        }
    }
}

/// Parses an accumulated `_partial_json` argument buffer into `input` once it
/// forms valid JSON, leaving the buffer in place until then.
fn finalize_partial_json(block: &mut Value) {
    let Some(partial) = block.get("_partial_json").and_then(Value::as_str) else {
        return;
    };
    if let Ok(parsed) = serde_json::from_str::<Value>(partial)
        && let Some(obj) = block.as_object_mut()
    {
        obj.insert("input".to_string(), parsed);
        obj.remove("_partial_json");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_delta_output_tokens_beats_message_start_placeholder() {
        // Anthropic sends a placeholder `output_tokens: 1` in `message_start`
        // and the real total only in `message_delta`. The reconstruction must
        // report the final count, not let the snapshot's stale usage win.
        let body = concat!(
            "event: message_start\n",
            "data: {\"message\":{\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":25,\"output_tokens\":1}}}\n\n",
            "event: message_delta\n",
            "data: {\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":137}}\n\n",
        );
        let (_, usage, _) = reconstruct(Provider::Anthropic, body.as_bytes());
        assert_eq!(usage.input_tokens, Some(25));
        assert_eq!(usage.output_tokens, Some(137));
    }

    #[test]
    fn reconstructs_anthropic_message_with_text_and_usage() {
        let body = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":15,\"cache_read_input_tokens\":4}}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":42}}\n\n",
        );
        let (snapshot, usage, _) = reconstruct(Provider::Anthropic, body.as_bytes());
        let snapshot = snapshot.expect("snapshot reconstructed");
        assert_eq!(snapshot["content"][0]["text"], "Hello world");
        assert_eq!(snapshot["stop_reason"], "end_turn");
        assert_eq!(usage.input_tokens, Some(15));
        assert_eq!(usage.output_tokens, Some(42));
        assert_eq!(usage.cache_read_tokens, Some(4));
    }

    #[test]
    fn reconstructs_anthropic_tool_use_input() {
        let body = concat!(
            "event: message_start\n",
            "data: {\"message\":{\"role\":\"assistant\",\"content\":[]}}\n\n",
            "event: content_block_start\n",
            "data: {\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"t1\",\"name\":\"get_weather\",\"input\":{}}}\n\n",
            "event: content_block_delta\n",
            "data: {\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\":\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"SF\\\"}\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"index\":0}\n\n",
        );
        let (snapshot, _, _) = reconstruct(Provider::Anthropic, body.as_bytes());
        let snapshot = snapshot.expect("snapshot reconstructed");
        assert_eq!(snapshot["content"][0]["name"], "get_weather");
        assert_eq!(snapshot["content"][0]["input"]["city"], "SF");
        assert!(snapshot["content"][0].get("_partial_json").is_none());
    }

    #[test]
    fn reconstructs_openai_chat_completion_text_and_tool_call() {
        let body = concat!(
            "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"}}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\" there\"}}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"42}\"}}]}}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":3}}\n\n",
            "data: [DONE]\n\n",
        );
        let (snapshot, usage, _) = reconstruct(Provider::OpenAI, body.as_bytes());
        let snapshot = snapshot.expect("snapshot reconstructed");
        let content = snapshot["content"].as_array().expect("content array");
        assert_eq!(content[0]["text"], "Hi there");
        let tool = content
            .iter()
            .find(|b| b["type"] == "tool_use")
            .expect("tool_use block");
        assert_eq!(tool["name"], "lookup");
        assert_eq!(tool["input"]["q"], 42);
        assert_eq!(usage.input_tokens, Some(11));
        assert_eq!(usage.output_tokens, Some(3));
    }

    #[test]
    fn reconstructs_gemini_text_and_usage() {
        let body = concat!(
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"par\"}]}}]}\n\n",
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"tial\"}]}}],\"usageMetadata\":{\"promptTokenCount\":8,\"candidatesTokenCount\":2}}\n\n",
        );
        let (snapshot, usage, _) = reconstruct(Provider::Gemini, body.as_bytes());
        let snapshot = snapshot.expect("snapshot reconstructed");
        assert_eq!(snapshot["content"][0]["text"], "partial");
        assert_eq!(usage.input_tokens, Some(8));
        assert_eq!(usage.output_tokens, Some(2));
    }

    #[test]
    fn reconstructs_openai_responses_snapshot() {
        let body = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":6}}}\n\n",
        );
        let (snapshot, usage, _) = reconstruct(Provider::OpenAI, body.as_bytes());
        let snapshot = snapshot.expect("snapshot reconstructed");
        assert_eq!(snapshot["status"], "completed");
        assert_eq!(usage.input_tokens, Some(5));
        assert_eq!(usage.output_tokens, Some(6));
    }

    #[test]
    fn reconstructs_openai_responses_delta_events_and_stream_event_log() {
        let body = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hel\"}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"lo\"}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"web_search_call\",\"call_id\":\"call_1\",\"action\":{\"query\":\"docs\"}}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r2\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":9,\"output_tokens\":2}}}\n\n",
        );
        let (snapshot, usage, events) = reconstruct(Provider::OpenAI, body.as_bytes());
        let snapshot = snapshot.expect("snapshot reconstructed");
        assert_eq!(snapshot["output"][0]["content"][0]["text"], "Hello");
        assert_eq!(snapshot["content"][0]["text"], "Hello");
        assert_eq!(snapshot["content"][1]["name"], "web_search");
        assert_eq!(snapshot["content"][1]["input"]["action"]["query"], "docs");
        assert_eq!(usage.input_tokens, Some(9));
        assert_eq!(usage.output_tokens, Some(2));
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].event, "response.output_text.delta");
    }

    #[test]
    fn returns_none_for_unparseable_body() {
        let (snapshot, usage, _) = reconstruct(Provider::Anthropic, b"not an sse stream");
        assert!(snapshot.is_none());
        assert!(usage.is_empty());
    }

    #[test]
    fn seeds_typed_block_for_orphan_delta() {
        // A text delta with no preceding content_block_start still yields a
        // typed text block rather than an untyped `{}`.
        let body = concat!(
            "event: message_start\n",
            "data: {\"message\":{\"role\":\"assistant\",\"content\":[]}}\n\n",
            "event: content_block_delta\n",
            "data: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
        );
        let (snapshot, _, _) = reconstruct(Provider::Anthropic, body.as_bytes());
        let snapshot = snapshot.expect("snapshot reconstructed");
        assert_eq!(snapshot["content"][0]["type"], "text");
        assert_eq!(snapshot["content"][0]["text"], "hi");
    }

    #[test]
    fn tolerates_non_array_content_in_snapshot() {
        // A malformed/hostile upstream whose `message_start` carries a non-array
        // `content` must not panic the reassembler when a delta follows; the
        // bad value is coerced to an empty array.
        let body = concat!(
            "event: message_start\n",
            "data: {\"message\":{\"role\":\"assistant\",\"content\":\"oops\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
        );
        let (snapshot, _, _) = reconstruct(Provider::Anthropic, body.as_bytes());
        let snapshot = snapshot.expect("snapshot reconstructed");
        assert_eq!(snapshot["content"][0]["text"], "hi");
    }

    #[test]
    fn tolerates_non_array_output_in_responses_snapshot() {
        // A Responses snapshot whose `output` is a non-array value must not
        // panic when a later `output_item` event accumulates into it.
        let body = concat!(
            "event: response.created\n",
            "data: {\"response\":{\"id\":\"r1\",\"output\":\"oops\"}}\n\n",
            "event: response.output_item.done\n",
            "data: {\"output_index\":0,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n",
        );
        let (snapshot, _, _) = reconstruct(Provider::OpenAI, body.as_bytes());
        let snapshot = snapshot.expect("snapshot reconstructed");
        assert_eq!(snapshot["output"][0]["type"], "message");
    }
}
