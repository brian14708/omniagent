//! Common worker abstraction for the process exposed by `omniagent serve`.
//!
//! The web control plane only needs terminal I/O, resize, exit notification,
//! and shutdown semantics. Keeping those operations behind this handle lets the
//! worker backend evolve independently of the control plane.

use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::sync::broadcast;

use crate::agent::AgentHandle;

/// A point-in-time terminal attachment: retained output first, then live output.
pub struct TerminalAttachment {
    pub backlog: Vec<u8>,
    pub output: broadcast::Receiver<Bytes>,
}

impl TerminalAttachment {
    #[must_use]
    const fn new(backlog: Vec<u8>, output: broadcast::Receiver<Bytes>) -> Self {
        Self { backlog, output }
    }

    /// An attachment with no terminal output, for backends without a PTY (the
    /// codex app-server worker). The sender is dropped immediately, so the
    /// receiver reports `Closed` on first poll — but the terminal bridges and
    /// recorder that consume it are never wired for a non-PTY session.
    #[must_use]
    fn empty() -> Self {
        let (_tx, output) = broadcast::channel(1);
        Self {
            backlog: Vec::new(),
            output,
        }
    }
}

/// Backend contract for a supervised coding-agent worker.
pub trait AgentWorker: Send + Sync + 'static {
    /// Attach a terminal client to the worker's PTY output.
    #[must_use]
    fn terminal_attach(&self) -> TerminalAttachment;

    /// Send raw terminal bytes to the worker's PTY stdin.
    fn write_input(&self, data: Bytes);

    /// Resize the worker's PTY.
    fn resize_pty(&self, rows: u16, cols: u16);

    /// Wait for the worker's command to exit and return its exit code.
    #[must_use]
    fn wait_exit_code(&self) -> BoxFuture<'_, i32>;

    /// Terminate the worker and release any backend-owned resources.
    #[must_use]
    fn shutdown_worker(&self) -> BoxFuture<'_, ()>;
}

/// Cloneable, backend-agnostic handle used by the web server and supervisor.
///
/// Two backends: a PTY-backed agent ([`AgentHandle`], the terminal path used by
/// claude/gemini/codex-TUI) and the structured codex app-server worker
/// ([`crate::codex::CodexWorkerHandle`]). The shared lifecycle methods
/// ([`wait_exit`](Self::wait_exit) / [`shutdown`](Self::shutdown)) drive the
/// supervisor's reaper uniformly; the terminal methods are meaningful only for
/// the PTY backend and are inert for codex (which the supervisor never wires to
/// a terminal bridge).
#[derive(Clone)]
pub enum WorkerHandle {
    Pty(Arc<dyn AgentWorker>),
    Codex(crate::codex::CodexWorkerHandle),
}

impl WorkerHandle {
    #[must_use]
    pub fn new_pty(worker: impl AgentWorker) -> Self {
        Self::Pty(Arc::new(worker))
    }

    #[must_use]
    pub const fn new_codex(handle: crate::codex::CodexWorkerHandle) -> Self {
        Self::Codex(handle)
    }

    /// Attach a terminal client to the worker's PTY output (empty for codex).
    #[must_use]
    pub fn attach(&self) -> TerminalAttachment {
        match self {
            Self::Pty(worker) => worker.terminal_attach(),
            Self::Codex(_) => TerminalAttachment::empty(),
        }
    }

    /// Send raw terminal bytes to the worker's PTY stdin (no-op for codex).
    pub fn send_input(&self, data: Bytes) {
        if let Self::Pty(worker) = self {
            worker.write_input(data);
        }
    }

    /// Resize the worker's PTY (no-op for codex).
    pub fn resize(&self, rows: u16, cols: u16) {
        if let Self::Pty(worker) = self {
            worker.resize_pty(rows, cols);
        }
    }

    /// Wait for the worker to exit and return its exit code.
    #[must_use]
    pub fn wait_exit(&self) -> BoxFuture<'_, i32> {
        match self {
            Self::Pty(worker) => worker.wait_exit_code(),
            Self::Codex(handle) => {
                let handle = handle.clone();
                Box::pin(async move { handle.wait_exit().await })
            }
        }
    }

    /// Terminate the worker and release any backend-owned resources.
    #[must_use]
    pub fn shutdown(&self) -> BoxFuture<'_, ()> {
        match self {
            Self::Pty(worker) => worker.shutdown_worker(),
            Self::Codex(handle) => {
                let handle = handle.clone();
                Box::pin(async move { handle.shutdown().await })
            }
        }
    }
}

impl AgentWorker for AgentHandle {
    fn terminal_attach(&self) -> TerminalAttachment {
        let (backlog, output) = self.attach();
        TerminalAttachment::new(backlog, output)
    }

    fn write_input(&self, data: Bytes) {
        self.send_input(data);
    }

    fn resize_pty(&self, rows: u16, cols: u16) {
        self.resize(rows, cols);
    }

    fn wait_exit_code(&self) -> BoxFuture<'_, i32> {
        Box::pin(async move { self.wait_exit().await })
    }

    fn shutdown_worker(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.shutdown() })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone)]
    struct FakeWorker {
        events: Arc<Mutex<Vec<String>>>,
        output: broadcast::Sender<Bytes>,
    }

    impl FakeWorker {
        fn new() -> Self {
            let (output, _) = broadcast::channel(1);
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
                output,
            }
        }

        fn push_event(&self, event: String) {
            self.events
                .lock()
                .expect("fake worker event log lock is healthy")
                .push(event);
        }
    }

    impl AgentWorker for FakeWorker {
        fn terminal_attach(&self) -> TerminalAttachment {
            self.push_event("attach".to_string());
            TerminalAttachment::new(b"backlog".to_vec(), self.output.subscribe())
        }

        fn write_input(&self, data: Bytes) {
            self.push_event(format!("input:{}", String::from_utf8_lossy(&data)));
        }

        fn resize_pty(&self, rows: u16, cols: u16) {
            self.push_event(format!("resize:{rows}x{cols}"));
        }

        fn wait_exit_code(&self) -> BoxFuture<'_, i32> {
            Box::pin(async { 42 })
        }

        fn shutdown_worker(&self) -> BoxFuture<'_, ()> {
            Box::pin(async move {
                self.push_event("shutdown".to_string());
            })
        }
    }

    #[tokio::test]
    async fn worker_handle_forwards_backend_operations() {
        let fake = FakeWorker::new();
        let events = fake.events.clone();
        let worker = WorkerHandle::new_pty(fake);

        let attachment = worker.attach();
        assert_eq!(attachment.backlog, b"backlog");
        worker.send_input(Bytes::from_static(b"abc"));
        worker.resize(12, 80);
        assert_eq!(worker.wait_exit().await, 42);
        worker.shutdown().await;

        let events = events
            .lock()
            .expect("fake worker event log lock is healthy")
            .clone();
        assert_eq!(events, ["attach", "input:abc", "resize:12x80", "shutdown",]);
    }
}
