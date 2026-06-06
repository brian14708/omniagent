use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use oad_api::{
    BackgroundExecEventKind, BackgroundExecResizeRequest, BackgroundExecStdinRequest,
};
use oad_cli::client::{OadClient, parse_sse_event, take_sse_frame};
use tokio::sync::{broadcast, mpsc, watch};

const BACKLOG_CAP: usize = 256 * 1024;
const OUTPUT_CHANNEL_CAP: usize = 2048;

#[derive(Clone)]
pub struct OadAgentHandle {
    client: OadClient,
    sandbox_id: String,
    exec_id: String,
    keep_sandbox: bool,
    control_tx: mpsc::UnboundedSender<OadControl>,
    output_tx: broadcast::Sender<Bytes>,
    backlog: Arc<Mutex<VecDeque<u8>>>,
    exit_rx: watch::Receiver<Option<i32>>,
    exit_tx: watch::Sender<Option<i32>>,
    cleanup: Arc<tokio::sync::Mutex<bool>>,
}

#[derive(Debug)]
enum OadControl {
    Input(Bytes),
    Resize { rows: u16, cols: u16 },
}

impl OadAgentHandle {
    #[must_use]
    pub fn new(client: OadClient, sandbox_id: String, exec_id: String, keep_sandbox: bool) -> Self {
        let (output_tx, _) = broadcast::channel::<Bytes>(OUTPUT_CHANNEL_CAP);
        let (control_tx, control_rx) = mpsc::unbounded_channel::<OadControl>();
        let backlog = Arc::new(Mutex::new(VecDeque::new()));
        let (exit_tx, exit_rx) = watch::channel::<Option<i32>>(None);
        let handle = Self {
            client,
            sandbox_id,
            exec_id,
            keep_sandbox,
            control_tx,
            output_tx,
            backlog,
            exit_rx,
            exit_tx,
            cleanup: Arc::new(tokio::sync::Mutex::new(false)),
        };
        handle.spawn_event_pump();
        handle.spawn_control_pump(control_rx);
        handle
    }

    fn spawn_event_pump(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(err) = this.pump_events().await {
                this.push_output(Bytes::from(format!(
                    "\r\nomniagent: oad event stream failed: {err:#}\r\n"
                )));
                let _ = this.exit_tx.send(Some(-1));
            }
        });
    }

    fn spawn_control_pump(&self, mut control_rx: mpsc::UnboundedReceiver<OadControl>) {
        let client = self.client.clone();
        let sandbox_id = self.sandbox_id.clone();
        let exec_id = self.exec_id.clone();
        tokio::spawn(async move {
            while let Some(control) = control_rx.recv().await {
                let result = match control {
                    OadControl::Input(data) => client
                        .write_exec_stdin(
                            &sandbox_id,
                            &exec_id,
                            &BackgroundExecStdinRequest {
                                data: data.to_vec(),
                                close: false,
                            },
                        )
                        .await
                        .map(|_| ()),
                    OadControl::Resize { rows, cols } => client
                        .resize_exec(
                            &sandbox_id,
                            &exec_id,
                            &BackgroundExecResizeRequest { rows, cols },
                        )
                        .await
                        .map(|_| ()),
                };
                if let Err(err) = result {
                    tracing::debug!(
                        sandbox_id,
                        exec_id,
                        error = %err,
                        "failed to send terminal control to oad exec"
                    );
                }
            }
        });
    }

    async fn pump_events(&self) -> Result<()> {
        let mut stream = self
            .client
            .exec_events(&self.sandbox_id, &self.exec_id, 1)
            .await?
            .bytes_stream();
        let mut buffer = String::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("failed to read oad exec event stream")?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(frame) = take_sse_frame(&mut buffer) {
                if let Some(event) = parse_sse_event(&frame)? {
                    match event.event {
                        BackgroundExecEventKind::Stdout { data }
                        | BackgroundExecEventKind::Stderr { data } => {
                            self.push_output(Bytes::from(data));
                        }
                        BackgroundExecEventKind::Exited { exit_code } => {
                            let _ = self.exit_tx.send(Some(exit_code));
                            return Ok(());
                        }
                        BackgroundExecEventKind::Failed { message } => {
                            self.push_output(Bytes::from(format!(
                                "\r\nomniagent: oad exec failed: {message}\r\n"
                            )));
                            let _ = self.exit_tx.send(Some(-1));
                            return Ok(());
                        }
                    }
                }
            }
        }
        // The stream ended without an Exited/Failed event. The daemon only
        // closes the stream this way when the session disappears (e.g. the
        // sandbox was torn down) or the connection dropped — a normal process
        // exit always emits an explicit Exited event handled above. Reporting
        // success here would make the supervisor treat a vanished or crashed
        // agent as a clean exit, so surface it as a failure instead.
        self.push_output(Bytes::from_static(
            b"\r\nomniagent: oad exec event stream ended unexpectedly\r\n",
        ));
        let _ = self.exit_tx.send(Some(-1));
        Ok(())
    }

    fn push_output(&self, chunk: Bytes) {
        if let Ok(mut buf) = self.backlog.lock() {
            buf.extend(chunk.iter().copied());
            if buf.len() > BACKLOG_CAP {
                let overflow = buf.len() - BACKLOG_CAP;
                buf.drain(..overflow);
            }
        }
        // Broadcast even when the backlog lock is unavailable (e.g. poisoned by
        // a panic), so live terminal clients keep receiving output — including
        // the error/exit banners pushed elsewhere — regardless of backlog health.
        let _ = self.output_tx.send(chunk);
    }

    #[must_use]
    pub fn attach(&self) -> (Vec<u8>, broadcast::Receiver<Bytes>) {
        self.backlog.lock().map_or_else(
            |_| (Vec::new(), self.output_tx.subscribe()),
            |backlog| {
                (
                    backlog.iter().copied().collect(),
                    self.output_tx.subscribe(),
                )
            },
        )
    }

    pub fn send_input(&self, data: Bytes) {
        let _ = self.control_tx.send(OadControl::Input(data));
    }

    pub fn resize(&self, rows: u16, cols: u16) {
        let _ = self.control_tx.send(OadControl::Resize { rows, cols });
    }

    pub async fn wait_exit(&self) -> i32 {
        let mut rx = self.exit_rx.clone();
        loop {
            let current = *rx.borrow();
            if let Some(code) = current {
                return code;
            }
            if rx.changed().await.is_err() {
                return 0;
            }
        }
    }

    pub async fn shutdown(&self) {
        let mut cleaned = self.cleanup.lock().await;
        if *cleaned {
            return;
        }
        *cleaned = true;
        drop(cleaned);

        if self.exit_rx.borrow().is_none() {
            let _ = self.client.kill_exec(&self.sandbox_id, &self.exec_id).await;
        }
        if !self.keep_sandbox {
            let _ = self.client.delete(&self.sandbox_id).await;
        }
    }
}
