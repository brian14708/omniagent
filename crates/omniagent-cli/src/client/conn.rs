//! Resilient Phoenix Channels connection manager.
//!
//! One [`PhoenixSocket`] owns a single WebSocket to the control plane and
//! multiplexes any number of session channels (topic `client:<client_id>`) over
//! it. A background supervisor task keeps the socket connected across network
//! outages: it reconnects with exponential backoff, re-joins every channel, and
//! replays each channel's buffered client->server events from the last sequence
//! the server acknowledged.
//!
//! ## Delivery model (client -> server)
//!
//! Each channel assigns a monotonic `sequence` to every *replayable* push
//! (`pty_output`, `pty_exit`, `trace_span`, `review_item`, `compare_run`) and
//! keeps it in an in-memory `outbox` until the server acknowledges it. The ack
//! is the server's `last_client_sequence`, returned on the resume reply and on
//! every app-level `heartbeat` reply. Because the server processes a channel's
//! messages strictly in order and persists them idempotently
//! (pty events via a unique `(session, source, type, sequence)` index; trace /
//! review / compare via upsert-by-id), replaying the unacked suffix after a
//! reconnect can never duplicate or corrupt the log.
//!
//! ## Correctness across daemon restarts
//!
//! The server is the durable source of truth for the sequence high-water mark.
//! On every (re)join the channel seeds its counter to
//! `max(local, last_client_sequence)`, so a freshly restarted daemon (local
//! counter 0) continues the sequence *past* everything already persisted instead
//! of restarting at 0 and colliding with old sequences (which the unique index
//! would silently drop). No local on-disk sequence is needed.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::{Notify, broadcast, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::protocol::{RegisterSessionRequest, RegisteredSession, ServerCommand};

use super::{ClientConfig, decode_command, next_ref, phoenix_message, websocket_url};

/// Events that ride the sequenced, replayable outbox.
const REPLAYABLE_EVENTS: [&str; 4] = ["pty_output", "pty_exit", "trace_span", "review_item"];

/// Wire event coalescing a contiguous run of `pty_output` items into one frame.
const PTY_OUTPUT_BATCH: &str = "pty_output_batch";

/// Soft cap on a channel's outbox payload bytes. Past this, the oldest
/// `pty_output` is shed (with a one-time gap marker) so a producer outpacing the
/// link can't grow memory without bound.
const OUTBOX_BYTE_CAP: usize = 8 * 1024 * 1024;

/// Marker emitted into the stream once when output is shed under backpressure.
const TRUNCATION_MARKER: &str = "\r\n[omniagent: output truncated]\r\n";

/// How long an `open_session` call waits for the first successful registration.
const REGISTER_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a single request/reply round-trip waits before giving up.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// App-level heartbeat cadence (liveness + a fallback outbox-trimming ack for
/// channels not covered by a recent `pty_output_batch` reply).
const APP_HEARTBEAT: Duration = Duration::from_secs(15);
/// Phoenix transport heartbeat cadence (keeps the socket from being reaped).
const TRANSPORT_HEARTBEAT: Duration = Duration::from_secs(30);

type WsStream = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;
type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;

/// A queued, sequence-numbered client->server event awaiting server ack.
#[derive(Clone)]
struct Outgoing {
    seq: u64,
    event: String,
    payload: Value,
}

/// Approximate payload weight for outbox accounting. `pty_output` is dominated
/// by its `data` string (the cheap fast path); other events (`trace_span` carries
/// a full serialized LLM span and can be large) are measured by their real
/// serialized size so the byte cap isn't fooled into never shedding.
fn payload_bytes(payload: &Value) -> usize {
    payload.get("data").and_then(Value::as_str).map_or_else(
        || serde_json::to_string(payload).map_or(0, |s| s.len()),
        str::len,
    )
}

/// Per-channel delivery state guarded by a single lock.
#[derive(Default)]
struct ChannelInner {
    /// Highest sequence assigned so far (the high-water mark).
    seq: u64,
    /// Highest sequence the server has acknowledged (outbox is trimmed to this).
    last_acked: u64,
    /// Highest sequence sent on the *current* connection; reset on each (re)join
    /// so the unacked suffix is replayed.
    sent_upto: u64,
    /// Unacked replayable events, ordered by `seq`.
    outbox: VecDeque<Outgoing>,
    /// Approximate total payload bytes held in `outbox` (for the soft cap).
    outbox_bytes: usize,
    /// Whether we are currently shedding output (so we mark the gap only once).
    truncating: bool,
}

/// All state for one multiplexed channel — either an agent session or the
/// daemon's control channel.
struct ChannelState {
    topic: String,
    kind: ChannelKind,
    /// Fan-out of decoded server->client commands to bridges.
    commands: broadcast::Sender<ServerCommand>,
    /// Sequenced delivery state (only used by session channels; a control
    /// channel's outbox stays empty).
    inner: Mutex<ChannelInner>,
}

/// What a channel is for, and the per-kind handshake/ready state.
enum ChannelKind {
    Session(SessionState),
    Control(ControlState),
}

/// Per-session handshake state.
struct SessionState {
    /// Registration request, updated in place with the resolved server session id
    /// so reconnects resume the same session.
    reg: Mutex<RegisterSessionRequest>,
    /// Server session id once registered.
    session_id: Mutex<Option<String>>,
    /// Fulfilled once with the first successful registration result.
    first_register: Mutex<Option<oneshot::Sender<Result<RegisteredSession>>>>,
}

/// Per-control-channel handshake state.
struct ControlState {
    /// Daemon metadata sent on `daemon_register`.
    metadata: Value,
    /// Fulfilled once when the daemon channel is first registered.
    ready: Mutex<Option<oneshot::Sender<Result<()>>>>,
}

/// Shared state owned by the supervisor task and every handle.
struct Shared {
    config: ClientConfig,
    channels: Mutex<HashMap<String, Arc<ChannelState>>>,
    pending: Mutex<HashMap<String, oneshot::Sender<Result<Value>>>>,
    sink: AsyncMutex<Option<WsSink>>,
    /// Signaled when an outbox gains items (kick the flusher).
    dirty: Notify,
    /// Signaled when the channel set changes (kick reconciliation).
    reconcile: Notify,
}

impl Shared {
    fn snapshot(&self) -> Vec<Arc<ChannelState>> {
        self.channels
            .lock()
            .expect("channels lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn channel(&self, topic: &str) -> Option<Arc<ChannelState>> {
        self.channels
            .lock()
            .expect("channels lock poisoned")
            .get(topic)
            .cloned()
    }
}

/// Owns the connection supervisor task; holds it alive for the socket's lifetime.
pub struct PhoenixSocket {
    shared: Arc<Shared>,
    _supervisor: JoinHandle<()>,
}

impl PhoenixSocket {
    /// Connect (lazily — the supervisor task drives reconnection in the
    /// background) and return a socket plus a cloneable handle.
    #[must_use]
    pub fn start(config: ClientConfig) -> Self {
        let shared = Arc::new(Shared {
            config,
            channels: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            sink: AsyncMutex::new(None),
            dirty: Notify::new(),
            reconcile: Notify::new(),
        });
        let supervisor = tokio::spawn(run_supervisor(Arc::clone(&shared)));
        Self {
            shared,
            _supervisor: supervisor,
        }
    }

    /// A cloneable handle for opening and closing sessions.
    #[must_use]
    pub fn handle(&self) -> SocketHandle {
        SocketHandle {
            shared: Arc::clone(&self.shared),
        }
    }
}

/// Cloneable handle used to manage sessions over the socket.
#[derive(Clone)]
pub struct SocketHandle {
    shared: Arc<Shared>,
}

/// Result of opening a session: a per-channel push handle plus the server's
/// registration reply.
pub struct OpenedSession {
    pub handle: ChannelHandle,
    pub registered: RegisteredSession,
}

impl SocketHandle {
    /// Register (or resume, when `reg.session_id` is set) a session and return a
    /// handle once the server confirms it. Waits for the socket to be connected.
    pub async fn open_session(&self, reg: RegisterSessionRequest) -> Result<OpenedSession> {
        let client_id = uuid::Uuid::new_v4().to_string();
        let topic = format!("client:{client_id}");
        let (tx, rx) = oneshot::channel();
        let (commands, _) = broadcast::channel::<ServerCommand>(256);
        let state = Arc::new(ChannelState {
            topic: topic.clone(),
            kind: ChannelKind::Session(SessionState {
                reg: Mutex::new(reg),
                session_id: Mutex::new(None),
                first_register: Mutex::new(Some(tx)),
            }),
            commands,
            inner: Mutex::new(ChannelInner::default()),
        });

        let registered = self.register_channel(&topic, &state, rx, "session").await?;

        Ok(OpenedSession {
            handle: ChannelHandle {
                shared: Arc::clone(&self.shared),
                state,
            },
            registered,
        })
    }

    /// Stop tracking a session and ask the server to leave its channel.
    pub fn close_session(&self, topic: &str) {
        let removed = self
            .shared
            .channels
            .lock()
            .expect("channels lock poisoned")
            .remove(topic);
        if removed.is_some() {
            let shared = Arc::clone(&self.shared);
            let topic = topic.to_string();
            tokio::spawn(async move {
                let _ =
                    send_frame(&shared, &topic, "phx_leave", json!({}), Some(&next_ref())).await;
            });
        }
    }

    /// Open the daemon's control channel (`daemon:<id>`) and register it with the
    /// server. The returned handle streams server->daemon commands (e.g.
    /// `spawn_agent`). Rejoins automatically on reconnect like a session channel.
    pub async fn open_control_channel(&self, metadata: Value) -> Result<ControlChannelHandle> {
        let daemon_id = uuid::Uuid::new_v4().to_string();
        let topic = format!("daemon:{daemon_id}");
        let (tx, rx) = oneshot::channel();
        let (commands, _) = broadcast::channel::<ServerCommand>(256);
        let state = Arc::new(ChannelState {
            topic: topic.clone(),
            kind: ChannelKind::Control(ControlState {
                metadata,
                ready: Mutex::new(Some(tx)),
            }),
            commands,
            inner: Mutex::new(ChannelInner::default()),
        });

        self.register_channel(&topic, &state, rx, "daemon").await?;

        Ok(ControlChannelHandle {
            _shared: Arc::clone(&self.shared),
            state,
        })
    }

    /// Insert a freshly built channel into the map, kick the reconcile loop to
    /// join it, and wait for the server to confirm registration. On timeout the
    /// channel is rolled back out of the map. `what` names the channel kind for
    /// error messages.
    async fn register_channel<T>(
        &self,
        topic: &str,
        state: &Arc<ChannelState>,
        rx: oneshot::Receiver<Result<T>>,
        what: &str,
    ) -> Result<T> {
        self.shared
            .channels
            .lock()
            .expect("channels lock poisoned")
            .insert(topic.to_string(), Arc::clone(state));
        self.shared.reconcile.notify_one();

        match tokio::time::timeout(REGISTER_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(anyhow!("{what} registration cancelled")),
            Err(_) => {
                // Timed out waiting for the server; roll back the channel.
                self.shared
                    .channels
                    .lock()
                    .expect("channels lock poisoned")
                    .remove(topic);
                Err(anyhow!(
                    "timed out registering {what} with the control plane (is it reachable?)"
                ))
            }
        }
    }
}

/// Handle to the daemon control channel; streams server->daemon commands.
#[derive(Clone)]
pub struct ControlChannelHandle {
    _shared: Arc<Shared>,
    state: Arc<ChannelState>,
}

impl ControlChannelHandle {
    /// Subscribe to decoded server->daemon commands (e.g. `spawn_agent`).
    #[must_use]
    pub fn subscribe_commands(&self) -> broadcast::Receiver<ServerCommand> {
        self.state.commands.subscribe()
    }
}

/// Per-session push handle used by the worker bridges and recording stores.
#[derive(Clone)]
pub struct ChannelHandle {
    shared: Arc<Shared>,
    state: Arc<ChannelState>,
}

impl ChannelHandle {
    #[must_use]
    pub fn topic(&self) -> String {
        self.state.topic.clone()
    }

    /// Subscribe to decoded server->client commands for this session.
    #[must_use]
    pub fn subscribe_commands(&self) -> broadcast::Receiver<ServerCommand> {
        self.state.commands.subscribe()
    }

    /// Enqueue an event. Replayable events are buffered and acked; transient
    /// request replies (`file_response`, `diff_response`) are best-effort.
    pub fn push(&self, event: impl Into<String>, payload: Value) {
        let event = event.into();
        if REPLAYABLE_EVENTS.contains(&event.as_str()) {
            self.push_replayable(event, payload);
        } else {
            self.push_ephemeral(event, payload);
        }
    }

    fn push_replayable(&self, event: String, mut payload: Value) {
        let mut inner = self.state.inner.lock().expect("channel inner lock");
        inner.seq += 1;
        let seq = inner.seq;
        if let Value::Object(map) = &mut payload {
            map.insert("sequence".to_string(), json!(seq));
        }
        inner.outbox_bytes += payload_bytes(&payload);
        inner.outbox.push_back(Outgoing {
            seq,
            event,
            payload,
        });
        shed_outbox(&mut inner, OUTBOX_BYTE_CAP);
        drop(inner);
        self.shared.dirty.notify_one();
    }

    fn push_ephemeral(&self, event: String, payload: Value) {
        let shared = Arc::clone(&self.shared);
        let topic = self.state.topic.clone();
        tokio::spawn(async move {
            let _ = send_frame(&shared, &topic, &event, payload, Some(&next_ref())).await;
        });
    }

    /// Sends an event and awaits its write to the socket (no reply expected).
    ///
    /// Unlike [`Self::push`], this resolves only once the frame has reached the
    /// wire, so a terminal event (e.g. `session_close`) can be guaranteed to be
    /// delivered before the channel is left. Best-effort: returns an error if
    /// the socket is disconnected.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket is not connected or the frame fails to
    /// write.
    pub async fn send_now(&self, event: impl Into<String>, payload: Value) -> Result<()> {
        send_frame(
            &self.shared,
            &self.state.topic,
            &event.into(),
            payload,
            Some(&next_ref()),
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Supervisor + connection lifecycle
// ---------------------------------------------------------------------------

async fn run_supervisor(shared: Arc<Shared>) {
    let mut backoff = Backoff::new();
    loop {
        match connect_ws(&shared.config).await {
            Ok((sink, stream)) => {
                *shared.sink.lock().await = Some(sink);
                let disconnect = Arc::new(Notify::new());
                let reader = spawn_reader(Arc::clone(&shared), stream, Arc::clone(&disconnect));

                let mut live: Vec<String> = Vec::new();
                if let Err(err) = handshake_all(&shared, &mut live).await {
                    tracing::warn!(error = %err, "handshake failed; will reconnect");
                    reader.abort();
                    *shared.sink.lock().await = None;
                    tokio::time::sleep(backoff.next()).await;
                    continue;
                }

                backoff.reset();
                tracing::info!("connected to control plane; {} channel(s) live", live.len());
                let flusher = spawn_flusher(Arc::clone(&shared));
                let heartbeat = spawn_heartbeat(Arc::clone(&shared));
                shared.dirty.notify_one();

                loop {
                    tokio::select! {
                        () = disconnect.notified() => break,
                        () = shared.reconcile.notified() => {
                            if let Err(err) = reconcile(&shared, &mut live).await {
                                tracing::warn!(error = %err, "reconcile failed; reconnecting");
                                break;
                            }
                            shared.dirty.notify_one();
                        }
                    }
                }

                flusher.abort();
                heartbeat.abort();
                reader.abort();
                *shared.sink.lock().await = None;
                fail_pending(&shared, "connection lost");
                tracing::warn!("disconnected from control plane; reconnecting");
            }
            Err(err) => {
                tracing::warn!(error = %err, "connect failed; retrying");
            }
        }
        tokio::time::sleep(backoff.next()).await;
    }
}

async fn connect_ws(config: &ClientConfig) -> Result<(WsSink, WsStream)> {
    let url = websocket_url(&config.server_url, &config.token);
    let (socket, _response) = connect_async(url)
        .await
        .context("failed to connect to OmniAgent control-plane socket")?;
    Ok(socket.split())
}

/// Join + register every currently-known channel on a fresh connection.
async fn handshake_all(shared: &Arc<Shared>, live: &mut Vec<String>) -> Result<()> {
    live.clear();
    for state in shared.snapshot() {
        handshake(shared, &state).await?;
        live.push(state.topic.clone());
    }
    Ok(())
}

/// Handshake any channel registered since the last reconcile.
async fn reconcile(shared: &Arc<Shared>, live: &mut Vec<String>) -> Result<()> {
    for state in shared.snapshot() {
        if !live.contains(&state.topic) {
            handshake(shared, &state).await?;
            live.push(state.topic.clone());
        }
    }
    Ok(())
}

/// Join the channel, then run the per-kind registration handshake.
async fn handshake(shared: &Arc<Shared>, state: &Arc<ChannelState>) -> Result<()> {
    let join_ref = next_ref();
    request_with_ref(shared, &state.topic, "phx_join", json!({}), &join_ref)
        .await
        .context("channel join failed")?;

    match &state.kind {
        ChannelKind::Session(session) => handshake_session(shared, state, session).await,
        ChannelKind::Control(control) => handshake_control(shared, state, control).await,
    }
}

/// Register/resume the agent session and seed sequence state.
async fn handshake_session(
    shared: &Arc<Shared>,
    state: &Arc<ChannelState>,
    session: &SessionState,
) -> Result<()> {
    let (event, payload) = {
        let reg = session.reg.lock().expect("reg lock").clone();
        let event = if reg.session_id.is_some() {
            "session_resume"
        } else {
            "session_register"
        };
        (event, serde_json::to_value(reg)?)
    };

    let response = request(shared, &state.topic, event, payload)
        .await
        .context("session register/resume failed")?;
    let registered: RegisteredSession =
        serde_json::from_value(response).context("invalid session register response")?;

    *session.session_id.lock().expect("session id lock") = Some(registered.id.clone());
    session.reg.lock().expect("reg lock").session_id = Some(registered.id.clone());

    let lcs = registered.last_client_sequence;
    {
        let mut inner = state.inner.lock().expect("channel inner lock");
        // Seed past the server's high-water mark — the key restart-correctness
        // invariant — and drop anything the server already has.
        inner.seq = inner.seq.max(lcs);
        inner.last_acked = inner.last_acked.max(lcs);
        while inner.outbox.front().is_some_and(|o| o.seq <= lcs) {
            if let Some(item) = inner.outbox.pop_front() {
                inner.outbox_bytes = inner
                    .outbox_bytes
                    .saturating_sub(payload_bytes(&item.payload));
            }
        }
        // Mirror apply_ack: a fully-drained outbox resets the shed accounting so
        // a later overflow on this connection marks a fresh truncation gap.
        if inner.outbox.is_empty() {
            inner.outbox_bytes = 0;
            inner.truncating = false;
        }
        // Replay the unacked suffix on this connection.
        inner.sent_upto = lcs;
    }

    let waiter = session
        .first_register
        .lock()
        .expect("first register lock")
        .take();
    if let Some(tx) = waiter {
        let _ = tx.send(Ok(registered));
    }
    Ok(())
}

/// Register the daemon control channel (command-only; no sequence/outbox).
async fn handshake_control(
    shared: &Arc<Shared>,
    state: &Arc<ChannelState>,
    control: &ControlState,
) -> Result<()> {
    request(
        shared,
        &state.topic,
        "daemon_register",
        control.metadata.clone(),
    )
    .await
    .context("daemon register failed")?;

    let waiter = control.ready.lock().expect("ready lock").take();
    if let Some(tx) = waiter {
        let _ = tx.send(Ok(()));
    }
    Ok(())
}

/// Drains every channel's outbox suffix to the wire whenever new items appear.
///
/// Contiguous `pty_output` items are coalesced into a single `pty_output_batch`
/// frame sent as a request, and the reply's `last_client_sequence` trims the
/// outbox (piggybacked ack). Other replayable events are sent individually.
fn spawn_flusher(shared: Arc<Shared>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            shared.dirty.notified().await;
            for state in shared.snapshot() {
                // Snapshot the unacked suffix WITHOUT advancing sent_upto here —
                // it is advanced per item only after delivery (advance_sent_upto),
                // so a failed send leaves the undelivered tail eligible to retry
                // instead of being acked-away by the heartbeat.
                let to_send: Vec<Outgoing> = {
                    let inner = state.inner.lock().expect("channel inner lock");
                    inner
                        .outbox
                        .iter()
                        .filter(|o| o.seq > inner.sent_upto)
                        .cloned()
                        .collect()
                };

                let mut run: Vec<Outgoing> = Vec::new();
                let mut healthy = true;
                for item in to_send {
                    if item.event == "pty_output" {
                        run.push(item);
                        continue;
                    }
                    // Preserve ordering: flush the pending pty run before a
                    // non-pty event, then send that event individually. Mark both
                    // delivered (advance sent_upto) only once they reach the wire.
                    let seq = item.seq;
                    if !flush_pty_run(&shared, &state, &mut run).await {
                        healthy = false;
                        break;
                    }
                    if send_frame(
                        &shared,
                        &state.topic,
                        &item.event,
                        item.payload.clone(),
                        Some(&next_ref()),
                    )
                    .await
                    .is_err()
                    {
                        healthy = false;
                        break;
                    }
                    advance_sent_upto(&state, seq);
                }
                if healthy {
                    // A failure here just leaves items in the outbox — sent_upto is
                    // not advanced for them, so the next flush or reconnect resends.
                    let _ = flush_pty_run(&shared, &state, &mut run).await;
                }
            }
        }
    })
}

/// Sends an accumulated `pty_output` run as one `pty_output_batch` request and
/// applies the reply's ack. Returns `false` if the socket is down (the items
/// stay queued for replay). A no-op for an empty run.
async fn flush_pty_run(
    shared: &Arc<Shared>,
    state: &Arc<ChannelState>,
    run: &mut Vec<Outgoing>,
) -> bool {
    if run.is_empty() {
        return true;
    }
    let last_seq = run.last().map(|o| o.seq);
    let payload = build_pty_batch(run);
    // Timeout/disconnect: the run stays eligible (sent_upto isn't advanced) and
    // the next flush or reconnect replay re-sends it.
    let Ok(reply) = request(shared, &state.topic, PTY_OUTPUT_BATCH, payload).await else {
        return false;
    };
    // Delivered: only now mark the run as sent so a failure above never strands it.
    if let Some(seq) = last_seq {
        advance_sent_upto(state, seq);
    }
    if let Some(acked) = reply.get("last_client_sequence").and_then(Value::as_u64) {
        apply_ack(state, acked);
    }
    true
}

/// Advances the per-connection delivered high-water mark (`sent_upto`) once an
/// item or batch has reached the wire. Only delivered sequences are skipped on
/// the next flush, so a failed send leaves the rest of the suffix to be retried
/// rather than being acked-away.
fn advance_sent_upto(state: &Arc<ChannelState>, seq: u64) {
    let mut inner = state.inner.lock().expect("channel inner lock");
    inner.sent_upto = inner.sent_upto.max(seq);
}

/// App-level per-channel heartbeats (carry the ack) + transport heartbeat.
fn spawn_heartbeat(shared: Arc<Shared>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut app = tokio::time::interval(APP_HEARTBEAT);
        let mut transport = tokio::time::interval(TRANSPORT_HEARTBEAT);
        app.tick().await;
        transport.tick().await;
        loop {
            tokio::select! {
                _ = transport.tick() => {
                    let _ = send_frame(&shared, "phoenix", "heartbeat", json!({}), Some(&next_ref())).await;
                }
                _ = app.tick() => {
                    for state in shared.snapshot() {
                        // Only session channels carry a sequence to acknowledge.
                        if !matches!(state.kind, ChannelKind::Session(_)) {
                            continue;
                        }
                        // Skip channels already fully acked (e.g. by recent
                        // pty_output_batch replies) — no idle round-trip needed.
                        // Ack only what has actually been delivered on this
                        // connection (sent_upto), never the produced high-water
                        // (seq) — otherwise the heartbeat would advance the
                        // server's ack past undelivered items and trim them from
                        // the outbox, losing them. Skip when delivered == acked.
                        let high_water = {
                            let inner = state.inner.lock().expect("channel inner lock");
                            if inner.sent_upto <= inner.last_acked {
                                continue;
                            }
                            inner.sent_upto
                        };
                        let payload = json!({ "sequence": high_water });
                        // A failure on one channel shouldn't strand the others.
                        if let Ok(reply) = request(&shared, &state.topic, "heartbeat", payload).await
                            && let Some(acked) =
                                reply.get("last_client_sequence").and_then(Value::as_u64)
                        {
                            apply_ack(&state, acked);
                        }
                    }
                }
            }
        }
    })
}

/// Trim a channel's outbox up to the server-acknowledged sequence.
fn apply_ack(state: &Arc<ChannelState>, acked: u64) {
    let mut inner = state.inner.lock().expect("channel inner lock");
    inner.last_acked = inner.last_acked.max(acked);
    while inner
        .outbox
        .front()
        .is_some_and(|o| o.seq <= inner.last_acked)
    {
        if let Some(item) = inner.outbox.pop_front() {
            inner.outbox_bytes = inner
                .outbox_bytes
                .saturating_sub(payload_bytes(&item.payload));
        }
    }
    // Once the backlog has fully drained, a later overflow marks a fresh gap.
    if inner.outbox.is_empty() {
        inner.outbox_bytes = 0;
        inner.truncating = false;
    }
    drop(inner);
}

/// Sheds the oldest `pty_output` items while the outbox exceeds the byte cap.
/// Non-`pty_output` events (trace/review) are never shed. On the first shed of an
/// episode, emits a one-time truncation marker so the gap is visible downstream.
fn shed_outbox(inner: &mut ChannelInner, cap: usize) {
    let mut dropped_bytes = 0usize;
    while inner.outbox_bytes > cap {
        let Some(pos) = inner.outbox.iter().position(|o| o.event == "pty_output") else {
            break;
        };
        if let Some(item) = inner.outbox.remove(pos) {
            let bytes = payload_bytes(&item.payload);
            inner.outbox_bytes = inner.outbox_bytes.saturating_sub(bytes);
            dropped_bytes += bytes;
        }
    }
    if dropped_bytes > 0 && !inner.truncating {
        inner.truncating = true;
        tracing::warn!(dropped_bytes, "outbox over cap; shedding oldest pty_output");
        inner.seq += 1;
        let seq = inner.seq;
        let payload = json!({ "data": TRUNCATION_MARKER, "sequence": seq });
        inner.outbox_bytes += payload_bytes(&payload);
        inner.outbox.push_back(Outgoing {
            seq,
            event: "pty_output".to_string(),
            payload,
        });
    }
}

/// Packs a run of `pty_output` outbox items into one `pty_output_batch` payload
/// `{"events": [{data, sequence}, ...]}`, preserving order. Drains the run (its
/// items are owned by the flusher and discarded after sending).
fn build_pty_batch(items: &mut Vec<Outgoing>) -> Value {
    let events: Vec<Value> = items.drain(..).map(|item| item.payload).collect();
    json!({ "events": events })
}

/// Reads frames for the lifetime of a connection, routing replies and commands.
fn spawn_reader(
    shared: Arc<Shared>,
    mut stream: WsStream,
    disconnect: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(message) = stream.next().await {
            match message {
                Ok(Message::Text(text)) => route_frame(&shared, &text),
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
        disconnect.notify_one();
    })
}

fn route_frame(shared: &Arc<Shared>, text: &str) {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return;
    };
    let event = value
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if event == "phx_reply" {
        if let Some(reference) = value.get("ref").and_then(Value::as_str) {
            let waiter = shared
                .pending
                .lock()
                .expect("pending lock poisoned")
                .remove(reference);
            if let Some(reply) = waiter {
                let payload = value.get("payload").cloned().unwrap_or(Value::Null);
                let status = payload
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("ok");
                let response = payload.get("response").cloned().unwrap_or(Value::Null);
                let result = if status == "ok" {
                    Ok(response)
                } else {
                    Err(anyhow!("server rejected request: {response}"))
                };
                let _ = reply.send(result);
            }
        }
        return;
    }

    if let Some(command) = decode_command(event, value.get("payload").unwrap_or(&Value::Null)) {
        let topic = value
            .get("topic")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if let Some(state) = shared.channel(topic) {
            let _ = state.commands.send(command);
        }
    }
}

/// Fail any in-flight request waiters so callers don't hang past a disconnect.
fn fail_pending(shared: &Arc<Shared>, reason: &str) {
    let waiters: Vec<_> = shared
        .pending
        .lock()
        .expect("pending lock poisoned")
        .drain()
        .map(|(_, tx)| tx)
        .collect();
    for tx in waiters {
        let _ = tx.send(Err(anyhow!("{reason}")));
    }
}

// ---------------------------------------------------------------------------
// Frame I/O
// ---------------------------------------------------------------------------

async fn send_frame(
    shared: &Arc<Shared>,
    topic: &str,
    event: &str,
    payload: Value,
    reference: Option<&str>,
) -> Result<()> {
    let frame = phoenix_message(reference, None, topic, event, &payload);
    let mut guard = shared.sink.lock().await;
    let sink = guard
        .as_mut()
        .ok_or_else(|| anyhow!("socket not connected"))?;
    let result = sink
        .send(Message::Text(frame.into()))
        .await
        .context("failed to write frame");
    drop(guard);
    result
}

async fn request(shared: &Arc<Shared>, topic: &str, event: &str, payload: Value) -> Result<Value> {
    request_with_ref(shared, topic, event, payload, &next_ref()).await
}

async fn request_with_ref(
    shared: &Arc<Shared>,
    topic: &str,
    event: &str,
    payload: Value,
    reference: &str,
) -> Result<Value> {
    let (tx, rx) = oneshot::channel();
    shared
        .pending
        .lock()
        .expect("pending lock poisoned")
        .insert(reference.to_string(), tx);
    if let Err(err) = send_frame(shared, topic, event, payload, Some(reference)).await {
        shared
            .pending
            .lock()
            .expect("pending lock poisoned")
            .remove(reference);
        return Err(err);
    }
    match tokio::time::timeout(REQUEST_TIMEOUT, rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(anyhow!("request reply channel closed")),
        Err(_) => {
            shared
                .pending
                .lock()
                .expect("pending lock poisoned")
                .remove(reference);
            Err(anyhow!("request '{event}' timed out"))
        }
    }
}

// ---------------------------------------------------------------------------
// Backoff
// ---------------------------------------------------------------------------

/// Full-jitter exponential backoff: 500ms base, ×2, capped at 30s.
struct Backoff {
    current: Duration,
}

impl Backoff {
    const BASE: Duration = Duration::from_millis(500);
    const CAP: Duration = Duration::from_secs(30);

    const fn new() -> Self {
        Self {
            current: Self::BASE,
        }
    }

    const fn reset(&mut self) {
        self.current = Self::BASE;
    }

    fn next(&mut self) -> Duration {
        let ceiling = self.current;
        self.current = (self.current * 2).min(Self::CAP);
        // Equal jitter: wait in [ceiling/2, ceiling] so retries never busy-spin
        // yet still spread out to avoid a thundering herd on the server.
        let ceiling_ms = u64::try_from(ceiling.as_millis().max(2)).unwrap_or(u64::MAX);
        let half = ceiling_ms / 2;
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let jittered = half + (u64::from(nanos) % half.max(1));
        Duration::from_millis(jittered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> Arc<ChannelState> {
        let (commands, _) = broadcast::channel(8);
        Arc::new(ChannelState {
            topic: "client:test".to_string(),
            kind: ChannelKind::Session(SessionState {
                reg: Mutex::new(RegisterSessionRequest {
                    session_id: None,
                    name: None,
                    cwd: "/tmp".to_string(),
                    argv: vec!["claude".to_string()],
                    metadata: serde_json::Map::new(),
                }),
                session_id: Mutex::new(None),
                first_register: Mutex::new(None),
            }),
            commands,
            inner: Mutex::new(ChannelInner::default()),
        })
    }

    fn enqueue(state: &Arc<ChannelState>, event: &str) -> u64 {
        let mut inner = state.inner.lock().unwrap();
        inner.seq += 1;
        let seq = inner.seq;
        inner.outbox.push_back(Outgoing {
            seq,
            event: event.to_string(),
            payload: json!({ "sequence": seq }),
        });
        drop(inner);
        seq
    }

    #[test]
    fn apply_ack_trims_acked_prefix() {
        let state = test_state();
        for _ in 0..5 {
            enqueue(&state, "pty_output");
        }
        apply_ack(&state, 3);
        let inner = state.inner.lock().unwrap();
        let last_acked = inner.last_acked;
        // Only sequences 4 and 5 remain unacked.
        let remaining: Vec<u64> = inner.outbox.iter().map(|o| o.seq).collect();
        drop(inner);
        assert_eq!(last_acked, 3);
        assert_eq!(remaining, vec![4, 5]);
    }

    #[test]
    fn apply_ack_is_monotonic() {
        let state = test_state();
        for _ in 0..5 {
            enqueue(&state, "pty_output");
        }
        apply_ack(&state, 4);
        // A stale, lower ack must not regress or restore trimmed entries.
        apply_ack(&state, 2);
        let inner = state.inner.lock().unwrap();
        let last_acked = inner.last_acked;
        let remaining = inner.outbox.iter().map(|o| o.seq).collect::<Vec<_>>();
        drop(inner);
        assert_eq!(last_acked, 4);
        assert_eq!(remaining, vec![5]);
    }

    #[test]
    fn resume_seeds_sequence_past_server_high_water() {
        // Simulates a restarted daemon: local counter 0, server already at 100.
        let state = test_state();
        let lcs = 100u64;
        {
            let mut inner = state.inner.lock().unwrap();
            inner.seq = inner.seq.max(lcs);
            inner.last_acked = inner.last_acked.max(lcs);
            while inner.outbox.front().is_some_and(|o| o.seq <= lcs) {
                inner.outbox.pop_front();
            }
            inner.sent_upto = lcs;
        }
        // The next assigned sequence continues past the server's mark — no
        // collision with already-persisted events.
        let next = enqueue(&state, "pty_output");
        assert_eq!(next, 101);
    }

    #[test]
    fn resume_drops_events_already_on_server() {
        let state = test_state();
        for _ in 0..5 {
            enqueue(&state, "pty_output"); // seq 1..=5
        }
        let lcs = 3u64;
        {
            let mut inner = state.inner.lock().unwrap();
            inner.seq = inner.seq.max(lcs);
            inner.last_acked = inner.last_acked.max(lcs);
            while inner.outbox.front().is_some_and(|o| o.seq <= lcs) {
                inner.outbox.pop_front();
            }
            inner.sent_upto = lcs;
        }
        let inner = state.inner.lock().unwrap();
        // Server had 1..=3; only 4 and 5 are replayed.
        let remaining = inner.outbox.iter().map(|o| o.seq).collect::<Vec<_>>();
        let sent_upto = inner.sent_upto;
        drop(inner);
        assert_eq!(remaining, vec![4, 5]);
        assert_eq!(sent_upto, 3);
    }

    #[test]
    fn backoff_grows_and_caps() {
        let mut backoff = Backoff::new();
        let mut last_ceiling = Backoff::BASE;
        for _ in 0..10 {
            let _ = backoff.next();
            last_ceiling = backoff.current;
        }
        assert_eq!(last_ceiling, Backoff::CAP);
        backoff.reset();
        assert_eq!(backoff.current, Backoff::BASE);
    }

    #[test]
    fn build_pty_batch_preserves_order_and_payloads() {
        let mut items = vec![
            Outgoing {
                seq: 1,
                event: "pty_output".to_string(),
                payload: json!({ "data": "a", "sequence": 1 }),
            },
            Outgoing {
                seq: 2,
                event: "pty_output".to_string(),
                payload: json!({ "data": "b", "sequence": 2 }),
            },
        ];
        let batch = build_pty_batch(&mut items);
        let events = batch["events"].as_array().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["data"], "a");
        assert_eq!(events[0]["sequence"], 1);
        assert_eq!(events[1]["data"], "b");
    }

    #[test]
    fn shed_outbox_drops_oldest_pty_output_and_marks_gap() {
        let mut inner = ChannelInner::default();
        // Two 100-byte pty_output chunks and one trace_span between them.
        let big = "x".repeat(100);
        for (event, seq) in [("pty_output", 1u64), ("trace_span", 2), ("pty_output", 3)] {
            let payload = if event == "pty_output" {
                json!({ "data": big, "sequence": seq })
            } else {
                json!({ "sequence": seq })
            };
            inner.outbox_bytes += payload_bytes(&payload);
            inner.seq = seq;
            inner.outbox.push_back(Outgoing {
                seq,
                event: event.to_string(),
                payload,
            });
        }

        // Cap at 150 bytes (two 100-byte pty chunks + a ~14-byte trace_span =
        // 214) sheds the oldest pty_output (seq 1); the trace_span and the newer
        // pty_output stay.
        shed_outbox(&mut inner, 150);

        assert!(inner.truncating);
        // trace_span must survive; the oldest pty_output (seq 1) is gone.
        assert!(inner.outbox.iter().any(|o| o.event == "trace_span"));
        assert!(!inner.outbox.iter().any(|o| o.seq == 1));
        // a truncation marker chunk was appended with a fresh sequence
        let marker = inner.outbox.back().unwrap();
        assert_eq!(marker.event, "pty_output");
        assert_eq!(marker.payload["data"], TRUNCATION_MARKER);

        // A second pass while still under cap adds no further marker.
        let before = inner.outbox.len();
        shed_outbox(&mut inner, 150);
        assert_eq!(inner.outbox.len(), before);
    }
}
