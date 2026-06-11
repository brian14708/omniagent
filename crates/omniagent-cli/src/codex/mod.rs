//! Native codex integration: drive `codex app-server` over JSON-RPC stdio and
//! render its structured conversation, instead of supervising codex's TUI in a
//! PTY.
//!
//! [`client`] is the transport; [`worker`] owns the child process and the
//! thread/turn lifecycle. The pure mapping helpers here translate between codex's
//! protocol and the control-plane channel events / review-gate decisions, kept
//! free of I/O so they are unit-testable.

pub mod client;
pub mod worker;

use std::collections::HashMap;

use serde_json::{Value, json};

use crate::review::ReviewDecision;
use client::{Notification, ServerRequest};
pub use worker::CodexWorkerHandle;

/// Looks up the first present key from `keys` as a string (tolerates codex's
/// mix of `snake_case` and `camelCase` across notification payloads).
fn str_field(value: &Value, keys: &[&str]) -> Value {
    for key in keys {
        if let Some(found) = value.get(*key)
            && !found.is_null()
        {
            return found.clone();
        }
    }
    Value::Null
}

/// Builds a `codex_delta` payload for a streaming item delta.
fn delta(kind: &str, params: &Value) -> (&'static str, Value) {
    (
        "codex_delta",
        json!({
            "kind": kind,
            "item_id": str_field(params, &["item_id", "itemId"]),
            "delta": params.get("delta").cloned().unwrap_or(Value::Null),
        }),
    )
}

/// Maps a codex server→client notification to the control-plane channel event
/// the console renders, or `None` for notifications we don't surface.
///
/// Returns `(event_name, payload)`. `codex_item` / `codex_turn` /
/// `codex_token_usage` / `codex_error` are durable (replayed on reconnect);
/// `codex_delta` is the ephemeral streaming stream reconciled by the matching
/// completed item.
#[must_use]
pub fn map_notification(note: &Notification) -> Option<(&'static str, Value)> {
    let p = &note.params;
    match note.method.as_str() {
        "item/started" => Some((
            "codex_item",
            json!({ "phase": "started", "item": p.get("item").cloned().unwrap_or(Value::Null) }),
        )),
        "item/completed" => Some((
            "codex_item",
            json!({ "phase": "completed", "item": p.get("item").cloned().unwrap_or(Value::Null) }),
        )),
        "item/agentMessage/delta" => Some(delta("agent_message", p)),
        "item/reasoning/textDelta" => Some(delta("reasoning", p)),
        "item/reasoning/summaryTextDelta" => Some(delta("reasoning_summary", p)),
        "item/commandExecution/outputDelta" => Some(delta("command_output", p)),
        "item/fileChange/patchUpdated" => Some((
            "codex_delta",
            json!({
                "kind": "file_change",
                "item_id": str_field(p, &["item_id", "itemId"]),
                "changes": p.get("changes").cloned().unwrap_or(Value::Null),
            }),
        )),
        "turn/started" => Some((
            "codex_turn",
            json!({ "phase": "started", "turn": p.get("turn").cloned().unwrap_or(Value::Null) }),
        )),
        "turn/completed" => Some((
            "codex_turn",
            json!({ "phase": "completed", "turn": p.get("turn").cloned().unwrap_or(Value::Null) }),
        )),
        "turn/diff/updated" => Some((
            "codex_turn",
            json!({ "phase": "diff", "diff": p.get("diff").cloned().unwrap_or(Value::Null) }),
        )),
        "turn/plan/updated" => Some((
            "codex_turn",
            json!({
                "phase": "plan",
                "plan": p.get("plan").cloned().unwrap_or(Value::Null),
                "explanation": p.get("explanation").cloned().unwrap_or(Value::Null),
            }),
        )),
        "thread/tokenUsage/updated" => Some(("codex_token_usage", p.clone())),
        "error" => Some(("codex_error", p.clone())),
        _ => None,
    }
}

/// Extracts the turn id from a `turn/started` notification's params
/// (`{ turn: { id } }`), for interrupt targeting.
#[must_use]
pub fn turn_started_id(params: &Value) -> Option<String> {
    params
        .get("turn")
        .and_then(|turn| turn.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

/// If `note` is a reasoning text/summary delta, returns `(item_id, delta_text)`.
///
/// Codex delivers reasoning text only through these streaming deltas — the
/// reasoning `item` itself arrives with empty `content`/`summary` — so the event
/// bridge accumulates them to persist a replayable reasoning body (see
/// [`backfill_reasoning`]).
#[must_use]
pub fn reasoning_delta(note: &Notification) -> Option<(String, String)> {
    if !matches!(
        note.method.as_str(),
        "item/reasoning/textDelta" | "item/reasoning/summaryTextDelta"
    ) {
        return None;
    }
    let item_id = note
        .params
        .get("item_id")
        .or_else(|| note.params.get("itemId"))
        .and_then(Value::as_str)?
        .to_string();
    let delta = note
        .params
        .get("delta")
        .and_then(Value::as_str)?
        .to_string();
    Some((item_id, delta))
}

/// Backfills a completed reasoning item's body from accumulated delta text when
/// codex left it empty, so the durable (replayed) event carries the reasoning.
/// `buffers` maps item id → accumulated text; the entry is consumed on completion.
pub fn backfill_reasoning(payload: &mut Value, buffers: &mut HashMap<String, String>) {
    let Some(item) = payload.get("item") else {
        return;
    };
    if item.get("type").and_then(Value::as_str) != Some("reasoning") {
        return;
    }
    if payload.get("phase").and_then(Value::as_str) != Some("completed") {
        return;
    }
    let Some(id) = item
        .get("id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
    else {
        return;
    };
    let Some(text) = buffers.remove(&id) else {
        return;
    };
    if text.is_empty() {
        return;
    }
    if let Some(obj) = payload.get_mut("item").and_then(Value::as_object_mut) {
        let already_has_text = obj
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|a| !a.is_empty())
            || obj
                .get("summary")
                .and_then(Value::as_array)
                .is_some_and(|a| !a.is_empty());
        if !already_has_text {
            obj.insert("content".to_string(), json!([text]));
        }
    }
}

/// Maps a review-gate decision to the codex approval decision string.
///
/// Codex approvals accept `accept` / `acceptForSession` / `decline` / `cancel`;
/// the review gate's `Retry` has no codex equivalent (it is an LLM-proxy
/// concept), so it declines — the agent proceeds with the rejection rather than
/// aborting the turn.
#[must_use]
pub const fn map_decision(decision: &ReviewDecision) -> &'static str {
    match decision {
        ReviewDecision::Approve { .. } => "accept",
        ReviewDecision::Reject { .. } | ReviewDecision::Retry { .. } => "decline",
    }
}

/// Stable review-item id for a codex approval request, derived from its rpc id
/// so the returned decision round-trips back to the right request.
#[must_use]
pub fn approval_review_id(req: &ServerRequest) -> String {
    let raw = req
        .id
        .as_str()
        .map_or_else(|| req.id.to_string(), ToString::to_string);
    format!("codex-approval-{raw}")
}
