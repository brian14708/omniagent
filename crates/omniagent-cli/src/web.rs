//! Web control plane: serves the single-page UI, bridges the agent PTY to the
//! browser over a WebSocket (ghostty-web), and streams recorded LLM spans over SSE.
//!
//! The SSE endpoint mirrors the daemon's background-exec event stream: replay
//! everything from `?from=N`, then follow live broadcasts, refilling from the
//! store if a slow consumer lags behind the channel.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::agent::AgentHandle;
use crate::compare::{CompareEvent, CompareList, CompareStore};
use crate::files::{self, FsError};
use crate::record::{LlmSpan, TraceStore};
use crate::review::{ReviewDecision, ReviewEvent, ReviewList, ReviewStore};
use crate::worker::WorkerHandle;

const INDEX_HTML: &str = include_str!("../assets/index.html");
const STYLE_CSS: &str = include_str!("../assets/style.css");

/// The browser UI's ES modules, served under `/app/{file}`. Embedded into the
/// binary so the control plane stays a single static artifact (no build step).
const APP_MODULES: &[(&str, &str)] = &[
    ("main.js", include_str!("../assets/app/main.js")),
    ("pty.js", include_str!("../assets/app/pty.js")),
    ("state.js", include_str!("../assets/app/state.js")),
    ("modes.js", include_str!("../assets/app/modes.js")),
    ("startup.js", include_str!("../assets/app/startup.js")),
    ("format.js", include_str!("../assets/app/format.js")),
    ("parse.js", include_str!("../assets/app/parse.js")),
    ("render.js", include_str!("../assets/app/render.js")),
    ("metrics.js", include_str!("../assets/app/metrics.js")),
    ("traces.js", include_str!("../assets/app/traces.js")),
    ("reviews.js", include_str!("../assets/app/reviews.js")),
    ("compare.js", include_str!("../assets/app/compare.js")),
    ("files.js", include_str!("../assets/app/files.js")),
    ("diff.js", include_str!("../assets/app/diff.js")),
];

/// Everything needed to spawn the agent once the UI requests a launch. Captured
/// at startup (from CLI args + the recording proxy) but not acted on until the
/// startup screen posts `/api/launch`.
pub struct LaunchConfig {
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub proxy_url: String,
    pub rows: u16,
    pub cols: u16,
    /// The mode the UI launched with (set on launch). `None` until launched.
    pub mode: Mutex<Option<String>>,
}

/// State shared by the web handlers.
#[derive(Clone)]
pub struct WebState {
    /// The running agent, set when the UI posts a launch (deferred spawn).
    pub agent: Arc<RwLock<Option<WorkerHandle>>>,
    pub launch: Arc<LaunchConfig>,
    pub workspace: Arc<PathBuf>,
    pub traces: Arc<TraceStore>,
    pub reviews: Arc<ReviewStore>,
    pub compare: Arc<CompareStore>,
}

/// Builds the web control-plane router.
pub fn router(state: WebState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/style.css", get(style_css))
        .route("/app/{file}", get(app_module))
        .route("/ws/pty", get(ws_pty))
        .route("/api/status", get(status))
        .route("/api/launch", post(launch))
        .route("/api/files", get(list_files))
        .route("/api/file", get(read_file))
        .route("/api/diff", get(diff))
        .route("/api/traces", get(list_traces))
        .route("/api/traces/events", get(stream_traces))
        .route("/api/review", get(list_reviews))
        .route("/api/review/events", get(stream_reviews))
        .route("/api/review/{id}/decision", post(decide_review))
        .route("/api/compare", get(list_compare))
        .route("/api/compare/events", get(stream_compare))
        .route("/api/compare/run", post(start_compare))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn style_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STYLE_CSS,
    )
}

async fn app_module(Path(file): Path<String>) -> impl IntoResponse {
    match APP_MODULES.iter().find(|(name, _)| *name == file) {
        Some((_, body)) => (
            [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
            *body,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Current launch state, so the UI can show the startup screen vs the runtime.
#[derive(Debug, Serialize)]
struct StatusResponse {
    launched: bool,
    mode: Option<String>,
    agent_cmd: String,
    cwd: String,
}

async fn status(State(state): State<WebState>) -> Json<StatusResponse> {
    let launched = state.agent.read().await.is_some();
    let mode = state.launch.mode.lock().ok().and_then(|m| m.clone());
    Json(StatusResponse {
        launched,
        mode,
        agent_cmd: state.launch.argv.join(" "),
        cwd: state.workspace.display().to_string(),
    })
}

/// Body of `POST /api/launch`: the mode chosen on the startup screen and whether
/// human review should gate this run.
#[derive(Debug, Deserialize)]
struct LaunchRequest {
    mode: String,
    #[serde(default)]
    review: bool,
}

/// Spawns the agent on demand (deferred spawn). Idempotent: 409 if already
/// launched.
async fn launch(
    State(state): State<WebState>,
    Json(req): Json<LaunchRequest>,
) -> impl IntoResponse {
    let mut slot = state.agent.write().await;
    if slot.is_some() {
        return (StatusCode::CONFLICT, "agent already launched").into_response();
    }
    state.reviews.set_enabled(req.review);
    let env = crate::proxy::agent_env(&state.launch.proxy_url);
    let spawned = AgentHandle::spawn(
        &state.launch.argv,
        &env,
        Some(state.launch.cwd.as_path()),
        state.launch.rows,
        state.launch.cols,
    );
    let worker = match spawned {
        Ok(agent) => WorkerHandle::new(agent),
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to launch agent: {err}"),
            )
                .into_response();
        }
    };
    *slot = Some(worker.clone());
    drop(slot);

    if let Ok(mut mode) = state.launch.mode.lock() {
        *mode = Some(req.mode);
    }
    // Log when the agent process exits so an unattended session leaves a trace;
    // the supervisor still owns shutdown on ctrl-c.
    tokio::spawn(async move {
        let code = worker.wait_exit().await;
        println!("omniagent: agent exited ({code})");
    });
    StatusCode::NO_CONTENT.into_response()
}

/// Maps a filesystem-access error to an HTTP response.
fn fs_error_response(err: FsError) -> axum::response::Response {
    match err {
        FsError::Forbidden => (StatusCode::FORBIDDEN, "path outside workspace").into_response(),
        FsError::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
        FsError::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "file too large").into_response(),
        FsError::NotText => (StatusCode::UNSUPPORTED_MEDIA_TYPE, "not a text file").into_response(),
        FsError::Io(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct PathQuery {
    path: Option<String>,
}

async fn list_files(
    State(state): State<WebState>,
    Query(query): Query<PathQuery>,
) -> impl IntoResponse {
    match files::list_dir(&state.workspace, query.path.as_deref().unwrap_or("")) {
        Ok(entries) => Json(entries).into_response(),
        Err(err) => fs_error_response(err),
    }
}

async fn read_file(
    State(state): State<WebState>,
    Query(query): Query<PathQuery>,
) -> impl IntoResponse {
    let path = query.path.unwrap_or_default();
    match files::read_file(&state.workspace, &path) {
        Ok(text) => (
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            text,
        )
            .into_response(),
        Err(err) => fs_error_response(err),
    }
}

async fn diff(
    State(state): State<WebState>,
    Query(query): Query<PathQuery>,
) -> impl IntoResponse {
    Json(files::git_diff(&state.workspace, query.path.as_deref()))
}

async fn list_traces(State(state): State<WebState>) -> Json<Vec<LlmSpan>> {
    Json(state.traces.list())
}

async fn list_reviews(State(state): State<WebState>) -> Json<ReviewList> {
    Json(state.reviews.list())
}

async fn decide_review(
    State(state): State<WebState>,
    Path(id): Path<String>,
    Json(decision): Json<ReviewDecision>,
) -> impl IntoResponse {
    if state.reviews.decide(&id, decision) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn list_compare(State(state): State<WebState>) -> Json<CompareList> {
    Json(state.compare.list())
}

/// Request to replay a captured span against several models.
#[derive(Debug, Deserialize)]
struct CompareRequest {
    span_id: String,
    #[serde(default)]
    models: Vec<String>,
}

/// Kicks off a comparison run for a captured span. Falls back to the configured
/// default models when none are supplied. Returns the run id, or 404 if the
/// source request is no longer retained.
async fn start_compare(
    State(state): State<WebState>,
    Json(req): Json<CompareRequest>,
) -> impl IntoResponse {
    let mut models: Vec<String> = req
        .models
        .into_iter()
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty())
        .collect();
    if models.is_empty() {
        models = state.compare.default_models().to_vec();
    }
    if models.is_empty() {
        return (StatusCode::BAD_REQUEST, "no models specified").into_response();
    }
    crate::proxy::run_comparison(&state.compare, &req.span_id, models).map_or_else(
        || {
            (
                StatusCode::NOT_FOUND,
                "request no longer available for replay",
            )
                .into_response()
        },
        |id| Json(serde_json::json!({ "id": id })).into_response(),
    )
}

/// A broadcast-backed store whose SSE stream is "emit a full snapshot, then
/// follow live events, re-snapshotting whenever a slow client lags behind the
/// channel." The review and comparison streams share this protocol exactly; the
/// trace stream differs (it replays by sequence) so it is not expressed here.
trait SnapshotSse: Clone + Send + 'static {
    /// The live event type, whose snapshot/reset variant is produced by `reset`.
    type Event: serde::Serialize + Clone + Send + 'static;
    /// SSE `event:` name the browser dispatches on.
    const EVENT: &'static str;

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Self::Event>;
    /// The current full state as a single replace-everything event.
    fn reset(&self) -> Self::Event;
}

impl SnapshotSse for Arc<CompareStore> {
    type Event = CompareEvent;
    const EVENT: &'static str = "compare";
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<CompareEvent> {
        CompareStore::subscribe(self)
    }
    fn reset(&self) -> CompareEvent {
        CompareEvent::Reset {
            runs: self.list().runs,
        }
    }
}

impl SnapshotSse for Arc<ReviewStore> {
    type Event = ReviewEvent;
    const EVENT: &'static str = "review";
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<ReviewEvent> {
        ReviewStore::subscribe(self)
    }
    fn reset(&self) -> ReviewEvent {
        ReviewEvent::Reset {
            items: self.list().items,
        }
    }
}

/// Snapshot-then-follow SSE for a [`SnapshotSse`] store: emit the current state,
/// then live events, recovering from broadcast lag by re-emitting a fresh
/// snapshot.
fn snapshot_sse<S: SnapshotSse>(
    store: S,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let receiver = store.subscribe();
    let reset = store.reset();
    let replay = stream::once(async move { reset });
    let live = stream::unfold((store, receiver), |(store, mut receiver)| async move {
        match receiver.recv().await {
            Ok(event) => Some((event, (store, receiver))),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                Some((store.reset(), (store, receiver)))
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
        }
    });

    let events = replay.chain(live).map(|event| {
        Ok(Event::default()
            .event(S::EVENT)
            .json_data(&event)
            .expect("event serializes to JSON"))
    });

    Sse::new(events).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn stream_compare(
    State(state): State<WebState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    snapshot_sse(state.compare)
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    /// First span sequence to replay before live streaming (default 1).
    from: Option<u64>,
}

/// SSE stream of recorded spans: replay from `from`, then live.
///
/// On reconnect, browsers resend the last delivered span id via the
/// `Last-Event-ID` header; honoring it (over the `?from=` default) avoids
/// replaying — and the client double-counting — the entire history.
async fn stream_traces(
    State(state): State<WebState>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(|seq| seq.saturating_add(1));
    let from = last_event_id.or(query.from).unwrap_or(1);
    let receiver = state.traces.subscribe();
    let replay = state.traces.list_since(from);
    let next = replay
        .last()
        .map_or(from, |span| span.sequence.saturating_add(1));

    let live = stream::unfold(
        (state.traces, receiver, next, VecDeque::<LlmSpan>::new()),
        move |(traces, mut receiver, mut next, mut pending)| async move {
            loop {
                // Drain any spans we recovered from the store after a lag first.
                if let Some(span) = pending.pop_front() {
                    next = span.sequence.saturating_add(1);
                    return Some((span, (traces, receiver, next, pending)));
                }
                match receiver.recv().await {
                    Ok(span) if span.sequence < next => {}
                    Ok(span) => {
                        next = span.sequence.saturating_add(1);
                        return Some((span, (traces, receiver, next, pending)));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Catch up from the store across the whole dropped range;
                        // the next live event resumes after the queue drains.
                        pending.extend(traces.list_since(next));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );

    let events = stream::iter(replay).chain(live).map(|span| {
        Ok(Event::default()
            .id(span.sequence.to_string())
            .event("span")
            .json_data(&span)
            .expect("LlmSpan serializes to JSON"))
    });

    Sse::new(events).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn stream_reviews(
    State(state): State<WebState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    snapshot_sse(state.reviews)
}

async fn ws_pty(ws: WebSocketUpgrade, State(state): State<WebState>) -> impl IntoResponse {
    // The agent is spawned lazily from the startup screen; until then there is
    // nothing to bridge.
    let agent = state.agent.read().await.clone();
    let Some(agent) = agent else {
        return StatusCode::CONFLICT.into_response();
    };
    ws.on_upgrade(move |socket| pty_session(socket, agent))
        .into_response()
}

/// Control frame the browser sends as a text message (keystrokes are binary).
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PtyControl {
    Resize { rows: u16, cols: u16 },
}

/// Bridges one browser WebSocket to the agent PTY for its lifetime.
async fn pty_session(socket: WebSocket, agent: WorkerHandle) {
    let (mut sink, mut source) = socket.split();

    // Prime the terminal with retained output, then follow live output. The
    // snapshot and subscription are taken atomically so no output is lost (or
    // duplicated) in the gap between them.
    let mut terminal = agent.attach();
    if !terminal.backlog.is_empty()
        && sink
            .send(Message::Binary(terminal.backlog.into()))
            .await
            .is_err()
    {
        return;
    }

    let mut send_task = tokio::spawn(async move {
        loop {
            match terminal.output.recv().await {
                Ok(chunk) => {
                    if sink.send(Message::Binary(chunk)).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let input_agent = agent.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(message)) = source.next().await {
            match message {
                Message::Binary(data) => input_agent.send_input(data),
                Message::Text(text) => {
                    if let Ok(PtyControl::Resize { rows, cols }) =
                        serde_json::from_str::<PtyControl>(&text)
                    {
                        input_agent.resize(rows, cols);
                    } else {
                        input_agent.send_input(Bytes::from(text.as_bytes().to_vec()));
                    }
                }
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) => {}
            }
        }
    });

    // When either direction ends, tear down the other.
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
}
