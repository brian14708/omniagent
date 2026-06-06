use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use oad_api::{
    BackgroundExecEvent, BackgroundExecEventKind, BackgroundExecInfo, BackgroundExecStatus,
};
use oad_runtime::{ExecProcess, PtyExecProcess};
use portable_pty::PtySize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Child;
use tokio::sync::{Mutex, Notify, broadcast, oneshot};
use tokio::task::JoinHandle;
use tracing::warn;

const EVENT_BUFFER_CAPACITY: usize = 4096;
const READ_BUFFER_BYTES: usize = 8192;
const KILL_STREAM_DRAIN_TIMEOUT: Duration = Duration::from_millis(100);
const KILL_SESSION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
pub struct BackgroundExecStore {
    sessions: Arc<Mutex<HashMap<String, Arc<BackgroundExecSession>>>>,
}

impl BackgroundExecStore {
    pub async fn insert(
        &self,
        info: BackgroundExecInfo,
        process: ExecProcess,
    ) -> Arc<BackgroundExecSession> {
        let exec_id = info.id.clone();
        let (kill_tx, kill_rx) = oneshot::channel();
        let session = Arc::new(BackgroundExecSession::new(info, process.stdin, kill_tx));
        self.sessions
            .lock()
            .await
            .insert(exec_id.clone(), session.clone());

        let stdout_task = spawn_stream_reader(
            session.clone(),
            exec_id.clone(),
            process.stdout,
            StreamName::Stdout,
        );
        let stderr_task = spawn_stream_reader(
            session.clone(),
            exec_id.clone(),
            process.stderr,
            StreamName::Stderr,
        );
        spawn_waiter(
            session.clone(),
            process.child,
            kill_rx,
            stdout_task,
            stderr_task,
        );

        session
    }

    pub async fn insert_pty(
        &self,
        info: BackgroundExecInfo,
        process: PtyExecProcess,
    ) -> Arc<BackgroundExecSession> {
        let exec_id = info.id.clone();
        let (kill_tx, kill_rx) = oneshot::channel();
        let session = Arc::new(BackgroundExecSession::new_pty(
            info,
            process.writer,
            process.master,
            process.killer,
            kill_tx,
        ));
        self.sessions
            .lock()
            .await
            .insert(exec_id.clone(), session.clone());

        let output_task = spawn_pty_reader(session.clone(), exec_id.clone(), process.reader);
        spawn_pty_waiter(session.clone(), process.child, kill_rx, output_task);

        session
    }

    pub async fn get(&self, exec_id: &str) -> Option<Arc<BackgroundExecSession>> {
        self.sessions.lock().await.get(exec_id).cloned()
    }

    pub async fn list_for_sandbox(&self, sandbox_id: &str) -> Vec<BackgroundExecInfo> {
        let sessions = {
            let sessions = self.sessions.lock().await;
            sessions.values().cloned().collect::<Vec<_>>()
        };

        let mut infos = Vec::new();
        for session in sessions {
            let info = session.info().await;
            if info.sandbox_id == sandbox_id {
                infos.push(info);
            }
        }
        infos.sort_by(|left, right| left.id.cmp(&right.id));
        infos
    }

    pub async fn kill_for_sandbox(&self, sandbox_id: &str) {
        let sessions = {
            let sessions = self.sessions.lock().await;
            sessions.values().cloned().collect::<Vec<_>>()
        };

        let mut matching = Vec::new();
        for session in sessions {
            let info = session.info().await;
            if info.sandbox_id == sandbox_id {
                matching.push((info.id.clone(), session));
            }
        }

        for (_, session) in &matching {
            let _ = session.kill().await;
        }
        let mut exec_ids = Vec::with_capacity(matching.len());
        for (exec_id, session) in matching {
            if tokio::time::timeout(KILL_SESSION_TIMEOUT, session.wait_finished())
                .await
                .is_err()
            {
                let info = session.info().await;
                warn!(
                    exec_id = %info.id,
                    sandbox_id = %info.sandbox_id,
                    "timed out waiting for background exec to stop"
                );
            }
            exec_ids.push(exec_id);
        }

        // Drop the finished sessions from the store so their buffers don't
        // accumulate for the daemon's lifetime once their sandbox is gone.
        if !exec_ids.is_empty() {
            let mut sessions = self.sessions.lock().await;
            for exec_id in exec_ids {
                sessions.remove(&exec_id);
            }
        }
    }
}

pub struct BackgroundExecSession {
    info: Mutex<BackgroundExecInfo>,
    control: Mutex<SessionControl>,
    events: Mutex<VecDeque<BackgroundExecEvent>>,
    next_sequence: AtomicU64,
    tx: broadcast::Sender<BackgroundExecEvent>,
    finished: Notify,
}

enum SessionControl {
    Pipes {
        stdin: Option<tokio::process::ChildStdin>,
        kill_tx: Option<oneshot::Sender<()>>,
    },
    Pty {
        writer: Option<Box<dyn Write + Send>>,
        master: Box<dyn portable_pty::MasterPty + Send>,
        killer: Option<Box<dyn portable_pty::ChildKiller + Send + Sync>>,
        kill_tx: Option<oneshot::Sender<()>>,
    },
}

impl BackgroundExecSession {
    fn new(
        info: BackgroundExecInfo,
        stdin: tokio::process::ChildStdin,
        kill_tx: oneshot::Sender<()>,
    ) -> Self {
        let (tx, _) = broadcast::channel(EVENT_BUFFER_CAPACITY);
        Self {
            info: Mutex::new(info),
            control: Mutex::new(SessionControl::Pipes {
                stdin: Some(stdin),
                kill_tx: Some(kill_tx),
            }),
            events: Mutex::new(VecDeque::with_capacity(EVENT_BUFFER_CAPACITY)),
            next_sequence: AtomicU64::new(1),
            tx,
            finished: Notify::new(),
        }
    }

    fn new_pty(
        info: BackgroundExecInfo,
        writer: Box<dyn Write + Send>,
        master: Box<dyn portable_pty::MasterPty + Send>,
        killer: Box<dyn portable_pty::ChildKiller + Send + Sync>,
        kill_tx: oneshot::Sender<()>,
    ) -> Self {
        let (tx, _) = broadcast::channel(EVENT_BUFFER_CAPACITY);
        Self {
            info: Mutex::new(info),
            control: Mutex::new(SessionControl::Pty {
                writer: Some(writer),
                master,
                killer: Some(killer),
                kill_tx: Some(kill_tx),
            }),
            events: Mutex::new(VecDeque::with_capacity(EVENT_BUFFER_CAPACITY)),
            next_sequence: AtomicU64::new(1),
            tx,
            finished: Notify::new(),
        }
    }

    pub async fn info(&self) -> BackgroundExecInfo {
        self.info.lock().await.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BackgroundExecEvent> {
        self.tx.subscribe()
    }

    pub async fn events_since(&self, from_sequence: u64) -> Vec<BackgroundExecEvent> {
        self.events
            .lock()
            .await
            .iter()
            .filter(|event| event.sequence >= from_sequence)
            .cloned()
            .collect()
    }

    pub async fn write_stdin(&self, data: &[u8], close: bool) -> std::io::Result<bool> {
        let mut control = self.control.lock().await;
        match &mut *control {
            SessionControl::Pipes { stdin, .. } => {
                let Some(writer) = stdin.as_mut() else {
                    return Ok(false);
                };

                if !data.is_empty() {
                    writer.write_all(data).await?;
                    writer.flush().await?;
                }
                if close {
                    stdin.take();
                }
            }
            SessionControl::Pty { writer, .. } => {
                let Some(pty_writer) = writer.as_mut() else {
                    return Ok(false);
                };

                if !data.is_empty() {
                    pty_writer.write_all(data)?;
                    pty_writer.flush()?;
                }
                if close {
                    writer.take();
                }
            }
        }
        drop(control);
        Ok(true)
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the PTY master lock must be held while calling resize"
    )]
    pub async fn resize(&self, rows: u16, cols: u16) -> bool {
        let control = self.control.lock().await;
        let SessionControl::Pty { master, .. } = &*control else {
            return false;
        };
        master
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .is_ok()
    }

    pub async fn kill(&self) -> bool {
        let mut control = self.control.lock().await;
        match &mut *control {
            SessionControl::Pipes { stdin, kill_tx } => {
                stdin.take();
                kill_tx
                    .take()
                    .is_some_and(|kill_tx| kill_tx.send(()).is_ok())
            }
            SessionControl::Pty {
                writer,
                killer,
                kill_tx,
                ..
            } => {
                writer.take();
                // `runsc exec` can forward signals to the foreground process,
                // so ask both the waiter and the cloned PTY child killer to
                // terminate. Either one succeeding is enough.
                let killed_waiter = kill_tx
                    .take()
                    .is_some_and(|kill_tx| kill_tx.send(()).is_ok());
                let killed_child = killer
                    .take()
                    .is_some_and(|mut killer| killer.kill().is_ok());
                killed_waiter || killed_child
            }
        }
    }

    pub async fn wait_finished(&self) {
        loop {
            let notified = self.finished.notified();
            if !matches!(self.info.lock().await.status, BackgroundExecStatus::Running) {
                return;
            }
            notified.await;
        }
    }

    // The broadcast send must stay inside the events lock to preserve event
    // ordering, so the drop-tightening lint (which would move it out) does not
    // apply here.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "events must be enqueued and broadcast under one lock to preserve ordering"
    )]
    async fn push_event(&self, exec_id: &str, event: BackgroundExecEventKind) {
        // Assign the sequence, enqueue, and broadcast while holding the events
        // lock so concurrent stdout/stderr readers can't interleave: the
        // sequence order, VecDeque order, and broadcast order all stay
        // consistent. fetch_add outside the lock would let reader B (seq N+1)
        // win the lock ahead of reader A (seq N) and publish out of order.
        let mut events = self.events.lock().await;
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let event = BackgroundExecEvent {
            sequence,
            exec_id: exec_id.to_string(),
            event,
        };
        if events.len() == EVENT_BUFFER_CAPACITY {
            events.pop_front();
        }
        events.push_back(event.clone());
        let _ = self.tx.send(event);
    }

    async fn finish_exited(&self, exec_id: &str, exit_code: i32) {
        self.finish(
            exec_id,
            BackgroundExecStatus::Exited,
            Some(exit_code),
            None,
            BackgroundExecEventKind::Exited { exit_code },
        )
        .await;
    }

    async fn finish_failed(&self, exec_id: &str, message: String) {
        self.finish(
            exec_id,
            BackgroundExecStatus::Failed,
            None,
            Some(message.clone()),
            BackgroundExecEventKind::Failed { message },
        )
        .await;
    }

    /// Transition a still-running session to its terminal state, release its
    /// stdin/kill handles, and emit the terminal event. No-op if the session
    /// has already finished.
    async fn finish(
        &self,
        exec_id: &str,
        status: BackgroundExecStatus,
        exit_code: Option<i32>,
        last_error: Option<String>,
        event: BackgroundExecEventKind,
    ) {
        {
            let mut info = self.info.lock().await;
            if !matches!(info.status, BackgroundExecStatus::Running) {
                return;
            }
            info.status = status;
            info.exit_code = exit_code;
            info.last_error = last_error;
        }
        let mut control = self.control.lock().await;
        match &mut *control {
            SessionControl::Pipes { stdin, kill_tx } => {
                stdin.take();
                kill_tx.take();
            }
            SessionControl::Pty {
                writer,
                killer,
                kill_tx,
                ..
            } => {
                writer.take();
                killer.take();
                kill_tx.take();
            }
        }
        drop(control);
        self.push_event(exec_id, event).await;
        self.finished.notify_waiters();
    }
}

enum StreamName {
    Stdout,
    Stderr,
}

fn spawn_stream_reader<R>(
    session: Arc<BackgroundExecSession>,
    exec_id: String,
    mut reader: R,
    stream_name: StreamName,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buffer = vec![0; READ_BUFFER_BYTES];
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break,
                Ok(n) => {
                    let data = buffer[..n].to_vec();
                    let event = match stream_name {
                        StreamName::Stdout => BackgroundExecEventKind::Stdout { data },
                        StreamName::Stderr => BackgroundExecEventKind::Stderr { data },
                    };
                    session.push_event(&exec_id, event).await;
                }
                Err(err) => {
                    warn!(exec_id, %err, "failed to read background exec stream");
                    break;
                }
            }
        }
    })
}

fn spawn_pty_reader(
    session: Arc<BackgroundExecSession>,
    exec_id: String,
    mut reader: Box<dyn Read + Send>,
) -> JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let handle = tokio::runtime::Handle::current();
        let mut buffer = vec![0; READ_BUFFER_BYTES];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    let data = buffer[..n].to_vec();
                    let event = BackgroundExecEventKind::Stdout { data };
                    handle.block_on(session.push_event(&exec_id, event));
                }
                Err(err) => {
                    warn!(exec_id, %err, "failed to read pty background exec stream");
                    break;
                }
            }
        }
    })
}

fn spawn_waiter(
    session: Arc<BackgroundExecSession>,
    mut child: Child,
    kill_rx: oneshot::Receiver<()>,
    stdout_task: JoinHandle<()>,
    stderr_task: JoinHandle<()>,
) {
    tokio::spawn(async move {
        let exec_id = session.info().await.id;
        let (status, killed) = tokio::select! {
            status = child.wait() => (status, false),
            _ = kill_rx => {
                let status = match child.start_kill() {
                    Ok(()) => child.wait().await,
                    Err(err) => Err(err),
                };
                (status, true)
            }
        };
        if killed {
            drain_or_abort_stream_readers(stdout_task, stderr_task).await;
        } else {
            let _ = stdout_task.await;
            let _ = stderr_task.await;
        }
        finish_child(session, &exec_id, status).await;
    });
}

fn spawn_pty_waiter(
    session: Arc<BackgroundExecSession>,
    mut child: Box<dyn portable_pty::Child + Send + Sync>,
    mut kill_rx: oneshot::Receiver<()>,
    mut output_task: JoinHandle<()>,
) {
    tokio::task::spawn_blocking(move || {
        let handle = tokio::runtime::Handle::current();
        let exec_id = handle.block_on(async { session.info().await.id });
        let mut killed = false;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    let code = i32::try_from(status.exit_code()).unwrap_or(-1);
                    // Bound the wait for the reader to flush. A descendant that
                    // inherited and kept the PTY slave open can prevent the
                    // master from ever reaching EOF, so the reader would never
                    // return and the session would hang in `Running` forever
                    // (mirrors `drain_or_abort_stream_readers` on the pipe
                    // path). After the timeout we give up on a clean drain so
                    // the session can still transition to its terminal state.
                    let drained = handle.block_on(async {
                        tokio::time::timeout(KILL_STREAM_DRAIN_TIMEOUT, &mut output_task)
                            .await
                            .is_ok()
                    });
                    if !drained {
                        output_task.abort();
                    }
                    handle.block_on(session.finish_exited(&exec_id, code));
                    return;
                }
                Ok(None) => {}
                Err(err) => {
                    let message = format!("failed to wait for pty background exec: {err}");
                    output_task.abort();
                    handle.block_on(session.finish_failed(&exec_id, message));
                    return;
                }
            }
            if !killed && kill_rx.try_recv().is_ok() {
                killed = true;
                let _ = child.kill();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
}

async fn drain_or_abort_stream_readers(
    mut stdout_task: JoinHandle<()>,
    mut stderr_task: JoinHandle<()>,
) {
    let mut stdout_done = false;
    let mut stderr_done = false;
    let timeout = tokio::time::sleep(KILL_STREAM_DRAIN_TIMEOUT);
    tokio::pin!(timeout);

    loop {
        if stdout_done && stderr_done {
            return;
        }
        tokio::select! {
            () = &mut timeout => break,
            result = &mut stdout_task, if !stdout_done => {
                let _ = result;
                stdout_done = true;
            }
            result = &mut stderr_task, if !stderr_done => {
                let _ = result;
                stderr_done = true;
            }
        }
    }

    if !stdout_done {
        stdout_task.abort();
    }
    if !stderr_done {
        stderr_task.abort();
    }
    if !stdout_done {
        let _ = stdout_task.await;
    }
    if !stderr_done {
        let _ = stderr_task.await;
    }
}

async fn finish_child(
    session: Arc<BackgroundExecSession>,
    exec_id: &str,
    status: std::io::Result<std::process::ExitStatus>,
) {
    match status {
        Ok(status) => {
            session
                .finish_exited(exec_id, status.code().unwrap_or(-1))
                .await;
        }
        Err(err) => {
            session
                .finish_failed(
                    exec_id,
                    format!("failed to wait for background exec: {err}"),
                )
                .await;
        }
    }
}
