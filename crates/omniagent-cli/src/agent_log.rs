//! Parses an agent's *native* on-disk session log into an ATIF [`Trajectory`].
//!
//! This is higher fidelity than reconstructing from proxy spans ([`crate::atif`]):
//! it reflects the exact turns, tool calls/results, reasoning, and per-turn token
//! usage the agent itself recorded. It mirrors harbor's per-agent converters
//! (`agents/installed/claude_code.py`, `codex.py`).
//!
//! Because omniagent runs the agent on the host against the user's real config,
//! we do not pin the agent's config dir (that would relocate its auth); we
//! *honor* `CLAUDE_CONFIG_DIR` / `CODEX_HOME` and locate the log by either the
//! session id we injected (claude — deterministic) or recency (codex).

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::atif::{
    Agent, FinalMetrics, Metrics, Observation, ObservationResult, Step, ToolCall, Trajectory,
};
use crate::executor::AgentInfo;

const SCHEMA_VERSION: &str = "ATIF-v1.7";

/// Tolerance applied when matching a log file's mtime against the session start,
/// absorbing small clock differences between spawn and first write.
const MTIME_SKEW: Duration = Duration::from_secs(5);

/// Locates the agent's native on-disk session log, or `None` if the agent has
/// no native log here or it can't be found. Exposed separately from parsing so
/// the raw log can be uploaded verbatim alongside the (lossy) ATIF trajectory
/// built from it — the raw log preserves detail the conversion drops (e.g.
/// claude's subagent/`Task` sidechains).
#[must_use]
pub fn locate_native_log(
    agent: AgentInfo,
    cwd: &Path,
    native_session_id: Option<&str>,
    since: SystemTime,
) -> Option<PathBuf> {
    match agent.name {
        "claude-code" => locate_claude_log(cwd, native_session_id, since),
        "codex" => locate_codex_log(since),
        _ => None,
    }
}

/// Parses an already-located native session log into an ATIF [`Trajectory`].
#[must_use]
pub fn parse_native_log(agent: AgentInfo, path: &Path) -> Option<Trajectory> {
    match agent.name {
        "claude-code" => parse_claude(path),
        "codex" => parse_codex(path),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Locating logs
// ---------------------------------------------------------------------------

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

/// `$CLAUDE_CONFIG_DIR` if set, else `~/.claude`.
fn claude_base() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("CLAUDE_CONFIG_DIR") {
        return Some(PathBuf::from(dir));
    }
    home_dir().map(|home| home.join(".claude"))
}

/// `$CODEX_HOME` if set, else `~/.codex`.
fn codex_base() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("CODEX_HOME") {
        return Some(PathBuf::from(dir));
    }
    home_dir().map(|home| home.join(".codex"))
}

/// Claude encodes a project's cwd by replacing `/` and `.` with `-`.
fn encode_project_dir(cwd: &Path) -> String {
    cwd.to_string_lossy()
        .chars()
        .map(|c| if c == '/' || c == '.' { '-' } else { c })
        .collect()
}

fn locate_claude_log(
    cwd: &Path,
    native_session_id: Option<&str>,
    since: SystemTime,
) -> Option<PathBuf> {
    let dir = claude_base()?
        .join("projects")
        .join(encode_project_dir(cwd));

    // Deterministic when we injected the session id.
    if let Some(id) = native_session_id {
        let path = dir.join(format!("{id}.jsonl"));
        if path.is_file() {
            return Some(path);
        }
    }
    // Otherwise the newest transcript written during this session.
    newest_file(&dir, since, false, |path| {
        path.extension().is_some_and(|ext| ext == "jsonl")
    })
}

fn locate_codex_log(since: SystemTime) -> Option<PathBuf> {
    let sessions = codex_base()?.join("sessions");
    newest_file(&sessions, since, true, |path| {
        path.extension().is_some_and(|ext| ext == "jsonl")
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("rollout-"))
    })
}

/// Returns the most-recently-modified file under `root` (optionally recursing)
/// that satisfies `pred` and was modified no earlier than `since` (minus skew).
fn newest_file(
    root: &Path,
    since: SystemTime,
    recurse: bool,
    pred: impl Fn(&Path) -> bool,
) -> Option<PathBuf> {
    let cutoff = since.checked_sub(MTIME_SKEW).unwrap_or(since);
    let mut best: Option<(SystemTime, PathBuf)> = None;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_dir() {
                if recurse {
                    stack.push(path);
                }
                continue;
            }
            if !pred(&path) {
                continue;
            }
            let Ok(modified) = meta.modified() else {
                continue;
            };
            if modified < cutoff {
                continue;
            }
            if best.as_ref().is_none_or(|(best_t, _)| modified > *best_t) {
                best = Some((modified, path));
            }
        }
    }
    best.map(|(_, path)| path)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Reads a JSONL file into one [`Value`] per non-empty, parseable line.
fn read_jsonl(path: &Path) -> Option<Vec<Value>> {
    let text = fs::read_to_string(path).ok()?;
    Some(
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect(),
    )
}

fn str_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn u64_field(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

/// Trims a single layer of surrounding double quotes (codex sometimes
/// JSON-encodes `call_id`/`output` values into the string itself).
fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
        .unwrap_or(value)
}

/// Accumulates steps with sequential `step_id`s and resolves tool results back
/// to the agent step that issued the matching call.
#[derive(Default)]
struct Builder {
    steps: Vec<Step>,
    /// `tool_call_id` → index into `steps` of the agent step holding that call.
    call_to_step: HashMap<String, usize>,
}

impl Builder {
    fn push_plain(&mut self, source: &'static str, message: String) {
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
        reasoning_content: Option<String>,
        tool_calls: Option<Vec<ToolCall>>,
        timestamp: Option<String>,
        metrics: Option<Metrics>,
    ) -> usize {
        let step_id = self.next_id();
        let index = self.steps.len();
        if let Some(calls) = &tool_calls {
            for call in calls {
                self.call_to_step.insert(call.tool_call_id.clone(), index);
            }
        }
        self.steps.push(Step {
            step_id,
            timestamp,
            source: "agent",
            model_name,
            message,
            reasoning_content,
            tool_calls,
            observation: None,
            metrics,
            extra: None,
        });
        index
    }

    /// Records the provider response id on a step so it links to its raw request
    /// span ([`crate::atif::link_raw_requests`]).
    fn tag_response_id(&mut self, index: usize, response_id: Option<&str>) {
        if let Some(id) = response_id.filter(|id| !id.is_empty()) {
            self.steps[index].extra = Some(serde_json::json!({ "response_id": id }));
        }
    }

    fn next_id(&self) -> u32 {
        u32::try_from(self.steps.len()).unwrap_or(u32::MAX) + 1
    }

    fn attach_result(&mut self, call_id: &str, result: ObservationResult) {
        let Some(&index) = self.call_to_step.get(call_id) else {
            return;
        };
        let observation = self.steps[index]
            .observation
            .get_or_insert_with(|| Observation {
                results: Vec::new(),
            });
        observation.results.push(result);
    }
}

fn finalize(
    builder: Builder,
    name: &str,
    session_id: Option<String>,
    model: Option<String>,
) -> Option<Trajectory> {
    if builder.steps.is_empty() {
        return None;
    }
    let final_metrics = sum_metrics(&builder.steps);
    Some(Trajectory {
        schema_version: SCHEMA_VERSION,
        session_id,
        agent: Agent {
            name: name.to_string(),
            version: "unknown".to_string(),
            model_name: model,
        },
        steps: builder.steps,
        final_metrics: Some(final_metrics),
    })
}

/// Sums per-step metrics into the trajectory's aggregate totals.
fn sum_metrics(steps: &[Step]) -> FinalMetrics {
    crate::atif::sum_token_metrics(
        steps
            .iter()
            .filter_map(|step| step.metrics.as_ref())
            .map(|metrics| {
                (
                    metrics.prompt_tokens,
                    metrics.completion_tokens,
                    metrics.cached_tokens,
                )
            }),
        steps.len(),
    )
}

// ---------------------------------------------------------------------------
// Claude Code transcript (JSONL)
// ---------------------------------------------------------------------------

fn parse_claude(path: &Path) -> Option<Trajectory> {
    let mut events = read_jsonl(path)?;
    // Keep only message-bearing lines, dedupe by uuid, order by timestamp.
    events.retain(|event| matches!(str_field(event, "type"), Some("user" | "assistant")));
    let mut seen = std::collections::HashSet::new();
    events
        .retain(|event| str_field(event, "uuid").is_none_or(|uuid| seen.insert(uuid.to_string())));
    events.sort_by(|a, b| str_field(a, "timestamp").cmp(&str_field(b, "timestamp")));

    let mut builder = Builder::default();
    // assistant message.id → step index, so streamed parts of one message merge.
    let mut msgid_to_step: HashMap<String, usize> = HashMap::new();
    let mut session_id = None;
    let mut model = None;

    for event in &events {
        if session_id.is_none() {
            session_id = str_field(event, "sessionId").map(str::to_string);
        }
        let Some(message) = event.get("message") else {
            continue;
        };
        match str_field(event, "type") {
            Some("assistant") => {
                if model.is_none() {
                    model = str_field(message, "model").map(str::to_string);
                }
                claude_assistant(&mut builder, &mut msgid_to_step, event, message);
            }
            Some("user") => claude_user(&mut builder, message),
            _ => {}
        }
    }

    finalize(builder, "claude-code", session_id, model)
}

fn claude_assistant(
    builder: &mut Builder,
    msgid_to_step: &mut HashMap<String, usize>,
    event: &Value,
    message: &Value,
) {
    let (text, reasoning, tool_calls) = claude_assistant_content(message.get("content"));
    let metrics = message.get("usage").and_then(claude_metrics);
    let timestamp = str_field(event, "timestamp").map(str::to_string);
    let model = str_field(message, "model").map(str::to_string);
    let msg_id = str_field(message, "id").map(str::to_string);

    // Merge streamed parts that share the assistant message id into one step.
    if let Some(id) = &msg_id
        && let Some(&index) = msgid_to_step.get(id)
    {
        merge_into_agent_step(builder, index, &text, reasoning, tool_calls, metrics);
        return;
    }

    let calls = (!tool_calls.is_empty()).then_some(tool_calls);
    let index = builder.push_agent(model, text, reasoning, calls, timestamp, metrics);
    builder.tag_response_id(index, msg_id.as_deref());
    if let Some(id) = msg_id {
        msgid_to_step.insert(id, index);
    }
}

fn merge_into_agent_step(
    builder: &mut Builder,
    index: usize,
    text: &str,
    reasoning: Option<String>,
    tool_calls: Vec<ToolCall>,
    metrics: Option<Metrics>,
) {
    for call in &tool_calls {
        builder
            .call_to_step
            .insert(call.tool_call_id.clone(), index);
    }
    let step = &mut builder.steps[index];
    if !text.is_empty() {
        if step.message.is_empty() {
            step.message = text.to_string();
        } else {
            step.message.push('\n');
            step.message.push_str(text);
        }
    }
    if let Some(reasoning) = reasoning {
        match &mut step.reasoning_content {
            Some(existing) => {
                existing.push('\n');
                existing.push_str(&reasoning);
            }
            slot => *slot = Some(reasoning),
        }
    }
    if !tool_calls.is_empty() {
        step.tool_calls
            .get_or_insert_with(Vec::new)
            .extend(tool_calls);
    }
    if metrics.is_some() {
        step.metrics = metrics;
    }
}

fn claude_user(builder: &mut Builder, message: &Value) {
    let content = message.get("content");
    // tool_result blocks attach to the step that issued the matching tool_use.
    if let Some(blocks) = content.and_then(Value::as_array) {
        for block in blocks {
            if str_field(block, "type") == Some("tool_result")
                && let Some(call_id) = str_field(block, "tool_use_id")
            {
                builder.attach_result(
                    call_id,
                    ObservationResult {
                        source_call_id: Some(call_id.to_string()),
                        content: Some(claude_text(block.get("content"))),
                    },
                );
            }
        }
    }
    let text = claude_text(content);
    if !text.is_empty() {
        builder.push_plain("user", text);
    }
}

/// Splits assistant content blocks into (text, reasoning, tool calls).
fn claude_assistant_content(content: Option<&Value>) -> (String, Option<String>, Vec<ToolCall>) {
    let Some(blocks) = content.and_then(Value::as_array) else {
        return (
            content.map(claude_value_text).unwrap_or_default(),
            None,
            Vec::new(),
        );
    };
    let mut text = Vec::new();
    let mut reasoning = Vec::new();
    let mut calls = Vec::new();
    for block in blocks {
        match str_field(block, "type") {
            Some("text") => {
                if let Some(t) = str_field(block, "text") {
                    text.push(t.to_string());
                }
            }
            Some("thinking") => {
                if let Some(t) = str_field(block, "thinking") {
                    reasoning.push(t.to_string());
                }
            }
            Some("tool_use") => calls.push(ToolCall {
                tool_call_id: str_field(block, "id").unwrap_or_default().to_string(),
                function_name: str_field(block, "name").unwrap_or_default().to_string(),
                arguments: block.get("input").cloned().unwrap_or(Value::Null),
            }),
            _ => {}
        }
    }
    let reasoning = (!reasoning.is_empty()).then(|| reasoning.join("\n"));
    (text.join("\n"), reasoning, calls)
}

/// Maps a Claude `usage` object to ATIF metrics (harbor's accounting:
/// `prompt = input + cache_read + cache_creation`, `cached = cache_read`).
fn claude_metrics(usage: &Value) -> Option<Metrics> {
    let input = u64_field(usage, "input_tokens");
    let output = u64_field(usage, "output_tokens");
    let cache_read = u64_field(usage, "cache_read_input_tokens");
    let cache_creation = u64_field(usage, "cache_creation_input_tokens");
    if input.is_none() && output.is_none() && cache_read.is_none() && cache_creation.is_none() {
        return None;
    }
    let prompt = input.unwrap_or(0) + cache_read.unwrap_or(0) + cache_creation.unwrap_or(0);
    Some(Metrics {
        prompt_tokens: Some(prompt),
        completion_tokens: output,
        cached_tokens: cache_read,
    })
}

/// Flattens Claude content (string, array of text blocks, or `tool_result`
/// content) into plain text.
fn claude_text(value: Option<&Value>) -> String {
    value.map(claude_value_text).unwrap_or_default()
}

fn claude_value_text(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    let Some(blocks) = value.as_array() else {
        return String::new();
    };
    blocks
        .iter()
        .filter_map(|block| str_field(block, "text").map(str::to_string))
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Codex rollout (JSONL)
// ---------------------------------------------------------------------------

fn parse_codex(path: &Path) -> Option<Trajectory> {
    let events = read_jsonl(path)?;
    let mut builder = Builder::default();
    let mut session_id = None;
    let mut model = None;
    let mut totals: Option<Metrics> = None;

    for event in &events {
        let payload = event.get("payload").unwrap_or(&Value::Null);
        match str_field(event, "type") {
            Some("session_meta") => {
                session_id = str_field(payload, "id").map(str::to_string);
            }
            Some("turn_context") => {
                model = model.or_else(|| str_field(payload, "model").map(str::to_string));
            }
            Some("response_item") => codex_response_item(&mut builder, payload, model.as_deref()),
            Some("event_msg") => {
                if str_field(payload, "type") == Some("token_count")
                    && let Some(usage) = payload
                        .get("info")
                        .and_then(|info| info.get("total_token_usage"))
                {
                    totals = codex_usage(usage).or(totals);
                }
            }
            _ => {}
        }
    }

    let trajectory = finalize(builder, "codex", session_id, model)?;
    Some(match totals {
        Some(metrics) => Trajectory {
            final_metrics: Some(FinalMetrics {
                total_prompt_tokens: metrics.prompt_tokens,
                total_completion_tokens: metrics.completion_tokens,
                total_cached_tokens: metrics.cached_tokens,
                total_steps: u32::try_from(trajectory.steps.len()).unwrap_or(u32::MAX),
            }),
            ..trajectory
        },
        None => trajectory,
    })
}

fn codex_response_item(builder: &mut Builder, payload: &Value, model: Option<&str>) {
    match str_field(payload, "type") {
        Some("message") => {
            let text = codex_message_text(payload.get("content"));
            if text.is_empty() {
                return;
            }
            match str_field(payload, "role") {
                Some("assistant") => {
                    builder.push_agent(model.map(str::to_string), text, None, None, None, None);
                }
                Some("user") => builder.push_plain("user", text),
                _ => builder.push_plain("system", text),
            }
        }
        Some("reasoning") => {
            let summary = codex_reasoning(payload.get("summary"));
            if !summary.is_empty() {
                builder.push_agent(
                    model.map(str::to_string),
                    String::new(),
                    Some(summary),
                    None,
                    None,
                    None,
                );
            }
        }
        Some("function_call" | "custom_tool_call") => {
            let call = ToolCall {
                tool_call_id: unquote(str_field(payload, "call_id").unwrap_or_default())
                    .to_string(),
                function_name: str_field(payload, "name").unwrap_or_default().to_string(),
                arguments: codex_arguments(payload),
            };
            builder.push_agent(
                model.map(str::to_string),
                String::new(),
                None,
                Some(vec![call]),
                None,
                None,
            );
        }
        Some("function_call_output" | "custom_tool_call_output") => {
            let call_id = unquote(str_field(payload, "call_id").unwrap_or_default()).to_string();
            let content = payload.get("output").map(|out| {
                out.as_str()
                    .map_or_else(|| out.to_string(), |s| unquote(s).to_string())
            });
            builder.attach_result(
                &call_id,
                ObservationResult {
                    source_call_id: (!call_id.is_empty()).then_some(call_id.clone()),
                    content,
                },
            );
        }
        _ => {}
    }
}

/// Codex tool `arguments` is a JSON string; parse it, else keep the raw value.
fn codex_arguments(payload: &Value) -> Value {
    match payload.get("arguments").or_else(|| payload.get("input")) {
        Some(Value::String(s)) => {
            serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone()))
        }
        Some(other) => other.clone(),
        None => Value::Null,
    }
}

fn codex_message_text(content: Option<&Value>) -> String {
    let Some(blocks) = content.and_then(Value::as_array) else {
        return String::new();
    };
    blocks
        .iter()
        .filter_map(|block| str_field(block, "text").map(str::to_string))
        .collect::<Vec<_>>()
        .join("\n")
}

fn codex_reasoning(summary: Option<&Value>) -> String {
    let Some(items) = summary.and_then(Value::as_array) else {
        return String::new();
    };
    items
        .iter()
        .filter_map(|item| {
            item.as_str().map(str::to_string).or_else(|| {
                str_field(item, "text")
                    .or_else(|| str_field(item, "summary"))
                    .map(str::to_string)
            })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn codex_usage(usage: &Value) -> Option<Metrics> {
    let input = u64_field(usage, "input_tokens");
    let output = u64_field(usage, "output_tokens");
    let cached = u64_field(usage, "cached_input_tokens");
    if input.is_none() && output.is_none() && cached.is_none() {
        return None;
    }
    Some(Metrics {
        prompt_tokens: input,
        completion_tokens: output,
        cached_tokens: cached,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(name: &'static str) -> AgentInfo {
        AgentInfo {
            name,
            supports_atif: true,
        }
    }

    #[test]
    fn parse_native_log_dispatches_by_agent() {
        let dir = std::env::temp_dir().join(format!("oa-native-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.jsonl");
        fs::write(
            &path,
            r#"{"type":"user","uuid":"u1","timestamp":"2026-06-10T00:00:00Z","sessionId":"s","message":{"role":"user","content":"hi"}}"#,
        )
        .unwrap();

        // claude-code routes to the transcript parser…
        assert!(parse_native_log(agent("claude-code"), &path).is_some());
        // …while an agent without a native parser yields None.
        assert!(parse_native_log(agent("gemini-cli"), &path).is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn encodes_project_dir_like_claude() {
        assert_eq!(
            encode_project_dir(Path::new("/home/brianli/oa/omniagent")),
            "-home-brianli-oa-omniagent"
        );
        assert_eq!(
            encode_project_dir(Path::new("/home/u/.config/app")),
            "-home-u--config-app"
        );
    }

    #[test]
    fn parses_claude_transcript_into_atif() {
        let dir = std::env::temp_dir().join(format!("oa-claude-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.jsonl");
        let lines = [
            r#"{"type":"system","message":{}}"#,
            r#"{"type":"user","uuid":"u1","timestamp":"2026-06-10T00:00:00Z","sessionId":"sess-1","message":{"role":"user","content":"hello"}}"#,
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-06-10T00:00:01Z","sessionId":"sess-1","message":{"id":"m1","model":"claude-opus-4-8","role":"assistant","usage":{"input_tokens":100,"output_tokens":20,"cache_read_input_tokens":10,"cache_creation_input_tokens":5},"content":[{"type":"thinking","thinking":"let me look"},{"type":"text","text":"checking"},{"type":"tool_use","id":"tool_1","name":"read","input":{"path":"a"}}]}}"#,
            r#"{"type":"user","uuid":"u2","timestamp":"2026-06-10T00:00:02Z","sessionId":"sess-1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool_1","content":"file body"}]}}"#,
            r#"{"type":"assistant","uuid":"a2","timestamp":"2026-06-10T00:00:03Z","sessionId":"sess-1","message":{"id":"m2","model":"claude-opus-4-8","role":"assistant","usage":{"output_tokens":7},"content":[{"type":"text","text":"done"}]}}"#,
            // duplicate uuid must be dropped
            r#"{"type":"assistant","uuid":"a2","timestamp":"2026-06-10T00:00:03Z","sessionId":"sess-1","message":{"id":"m2","content":[{"type":"text","text":"dupe"}]}}"#,
        ];
        fs::write(&path, lines.join("\n")).unwrap();

        let traj = parse_claude(&path).expect("trajectory");
        let _ = fs::remove_dir_all(&dir);

        assert_eq!(traj.schema_version, "ATIF-v1.7");
        assert_eq!(traj.session_id.as_deref(), Some("sess-1"));
        assert_eq!(traj.agent.model_name.as_deref(), Some("claude-opus-4-8"));
        // user, agent(tool_use)+observation+reasoning, agent(done)
        assert_eq!(traj.steps.len(), 3);
        assert_eq!(traj.steps[0].source, "user");
        assert_eq!(traj.steps[1].source, "agent");
        assert_eq!(
            traj.steps[1].reasoning_content.as_deref(),
            Some("let me look")
        );
        let calls = traj.steps[1].tool_calls.as_ref().unwrap();
        assert_eq!(calls[0].function_name, "read");
        let obs = traj.steps[1].observation.as_ref().unwrap();
        assert_eq!(obs.results[0].source_call_id.as_deref(), Some("tool_1"));
        assert_eq!(obs.results[0].content.as_deref(), Some("file body"));
        // the agent step is tagged with its provider response id (message.id)
        assert_eq!(traj.steps[1].extra.as_ref().unwrap()["response_id"], "m1");
        // metrics: prompt = 100 + 10 + 5
        assert_eq!(
            traj.steps[1].metrics.as_ref().unwrap().prompt_tokens,
            Some(115)
        );
        assert_eq!(traj.steps[2].message, "done");
        for (i, step) in traj.steps.iter().enumerate() {
            assert_eq!(step.step_id, u32::try_from(i).unwrap() + 1);
        }
        let fm = traj.final_metrics.as_ref().unwrap();
        assert_eq!(fm.total_prompt_tokens, Some(115));
        assert_eq!(fm.total_completion_tokens, Some(27));
    }

    #[test]
    fn parses_codex_rollout_into_atif() {
        let dir = std::env::temp_dir().join(format!("oa-codex-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rollout-x.jsonl");
        let lines = [
            r#"{"type":"session_meta","payload":{"id":"cx-1","cwd":"/x","cli_version":"0.130.0"}}"#,
            r#"{"type":"turn_context","payload":{"model":"gpt-5.5"}}"#,
            r#"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}}"#,
            r#"{"type":"response_item","payload":{"type":"function_call","name":"update_plan","call_id":"call_1","arguments":"{\"a\":1}"}}"#,
            r#"{"type":"response_item","payload":{"type":"function_call_output","call_id":"\"call_1\"","output":"\"done\""}}"#,
            r#"{"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"finished"}]}}"#,
            r#"{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":50,"output_tokens":8,"cached_input_tokens":4}}}}"#,
            r#"{"type":"event_msg","payload":{"type":"token_count","info":null}}"#,
        ];
        fs::write(&path, lines.join("\n")).unwrap();

        let traj = parse_codex(&path).expect("trajectory");
        let _ = fs::remove_dir_all(&dir);

        assert_eq!(traj.session_id.as_deref(), Some("cx-1"));
        assert_eq!(traj.agent.model_name.as_deref(), Some("gpt-5.5"));
        // user, agent(tool call)+obs, agent(finished)
        assert_eq!(traj.steps[0].source, "user");
        let call_step = &traj.steps[1];
        let call = &call_step.tool_calls.as_ref().unwrap()[0];
        assert_eq!(call.function_name, "update_plan");
        assert_eq!(call.arguments, serde_json::json!({"a": 1}));
        // call_id matched despite the output's quote-wrapped id
        let obs = call_step.observation.as_ref().unwrap();
        assert_eq!(obs.results[0].content.as_deref(), Some("done"));
        assert_eq!(traj.steps.last().unwrap().message, "finished");
        let fm = traj.final_metrics.as_ref().unwrap();
        assert_eq!(fm.total_prompt_tokens, Some(50));
        assert_eq!(fm.total_completion_tokens, Some(8));
        assert_eq!(fm.total_cached_tokens, Some(4));
    }
}
