//! Common worker abstraction for the process exposed by `omniagent serve`.
//!
//! The web control plane only needs terminal I/O, resize, exit notification,
//! and shutdown semantics.  Keeping those operations behind this handle lets
//! the local PTY backend and the remote oad PTY backend evolve independently.

use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio::sync::broadcast;

use crate::agent::AgentHandle;
use crate::oad_agent::OadAgentHandle;

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
#[derive(Clone)]
pub struct WorkerHandle {
    inner: Arc<dyn AgentWorker>,
}

impl WorkerHandle {
    #[must_use]
    pub fn new(worker: impl AgentWorker) -> Self {
        Self {
            inner: Arc::new(worker),
        }
    }

    /// Attach a terminal client to the worker's PTY output.
    #[must_use]
    pub fn attach(&self) -> TerminalAttachment {
        self.inner.terminal_attach()
    }

    /// Send raw terminal bytes to the worker's PTY stdin.
    pub fn send_input(&self, data: Bytes) {
        self.inner.write_input(data);
    }

    /// Resize the worker's PTY.
    pub fn resize(&self, rows: u16, cols: u16) {
        self.inner.resize_pty(rows, cols);
    }

    /// Wait for the worker's command to exit and return its exit code.
    #[must_use]
    pub fn wait_exit(&self) -> BoxFuture<'_, i32> {
        self.inner.wait_exit_code()
    }

    /// Terminate the worker and release any backend-owned resources.
    #[must_use]
    pub fn shutdown(&self) -> BoxFuture<'_, ()> {
        self.inner.shutdown_worker()
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

impl AgentWorker for OadAgentHandle {
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
        Box::pin(async move { self.shutdown().await })
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
        let worker = WorkerHandle::new(fake);

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
