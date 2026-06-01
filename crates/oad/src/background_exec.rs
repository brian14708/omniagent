use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use oad_api::{
    BackgroundExecEvent, BackgroundExecEventKind, BackgroundExecInfo, BackgroundExecStatus,
};
use oad_runtime::ExecProcess;
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
            if session.info().await.sandbox_id == sandbox_id {
                matching.push(session);
            }
        }

        for session in &matching {
            let _ = session.kill().await;
        }
        for session in matching {
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
        }
    }
}

pub struct BackgroundExecSession {
    info: Mutex<BackgroundExecInfo>,
    stdin: Mutex<Option<tokio::process::ChildStdin>>,
    kill_tx: Mutex<Option<oneshot::Sender<()>>>,
    events: Mutex<VecDeque<BackgroundExecEvent>>,
    next_sequence: AtomicU64,
    tx: broadcast::Sender<BackgroundExecEvent>,
    finished: Notify,
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
            stdin: Mutex::new(Some(stdin)),
            kill_tx: Mutex::new(Some(kill_tx)),
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
        let mut stdin = self.stdin.lock().await;
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
        drop(stdin);
        Ok(true)
    }

    pub async fn kill(&self) -> bool {
        self.stdin.lock().await.take();
        self.kill_tx
            .lock()
            .await
            .take()
            .is_some_and(|kill_tx| kill_tx.send(()).is_ok())
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

    async fn push_event(&self, exec_id: &str, event: BackgroundExecEventKind) {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let event = BackgroundExecEvent {
            sequence,
            exec_id: exec_id.to_string(),
            event,
        };
        {
            let mut events = self.events.lock().await;
            if events.len() == EVENT_BUFFER_CAPACITY {
                events.pop_front();
            }
            events.push_back(event.clone());
        }
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
        self.stdin.lock().await.take();
        self.kill_tx.lock().await.take();
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
                .finish_failed(exec_id, format!("failed to wait for process: {err}"))
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oad_api::BackgroundExecInfo;
    use tokio::process::Command;

    fn test_exec_info(id: &str) -> BackgroundExecInfo {
        BackgroundExecInfo {
            id: id.to_string(),
            sandbox_id: "sandbox".to_string(),
            container: "main".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "sleep 1".to_string()],
            status: BackgroundExecStatus::Running,
            exit_code: None,
            last_error: None,
        }
    }

    fn spawn_shell(command: &str) -> ExecProcess {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(command)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("test shell spawns");
        let stdin = child.stdin.take().expect("test shell has stdin");
        let stdout = child.stdout.take().expect("test shell has stdout");
        let stderr = child.stderr.take().expect("test shell has stderr");
        ExecProcess {
            child,
            stdin,
            stdout,
            stderr,
        }
    }

    #[tokio::test]
    async fn kill_finishes_even_when_descendant_keeps_stream_open() {
        let store = BackgroundExecStore::default();
        let session = store
            .insert(test_exec_info("exec"), spawn_shell("sleep 1"))
            .await;

        assert!(session.kill().await);
        tokio::time::timeout(Duration::from_millis(500), session.wait_finished())
            .await
            .expect("kill should not wait for descendant-held stdio EOF");

        let info = session.info().await;
        assert_eq!(info.status, BackgroundExecStatus::Exited);
        assert_eq!(info.exit_code, Some(-1));
    }
}
