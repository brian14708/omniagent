//! Records a session's terminal output as an [asciicast v2] file.
//!
//! The supervisor already fans PTY output out over a broadcast channel (see
//! [`crate::agent`]); the recorder attaches its own subscriber so it sees every
//! chunk independently of the channel-forwarding bridge. Each chunk is written
//! as an `[elapsed, "o", data]` event under a v2 header, producing a `.cast`
//! file that `asciinema play` (and the web player) can replay. The file is
//! uploaded at session close.
//!
//! [asciicast v2]: https://docs.asciinema.org/manual/asciicast/v2/

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use serde_json::json;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::client::decode_streaming;
use crate::worker::WorkerHandle;

/// A live terminal recording writing to a `.cast` file on disk.
pub struct CastRecorder {
    path: PathBuf,
    task: JoinHandle<()>,
}

impl CastRecorder {
    /// Starts recording the worker's terminal output to `path` (creating parent
    /// directories). A background task drains the PTY broadcast until the agent
    /// exits, mirroring the output bridge's drain-then-stop ordering so the tail
    /// of the session is captured. Returns an error only if the file cannot be
    /// created; recording itself is best-effort thereafter.
    ///
    /// # Errors
    ///
    /// Returns an error if the parent directory or file cannot be created, or
    /// the header cannot be written.
    pub fn spawn(path: PathBuf, worker: &WorkerHandle, rows: u16, cols: u16) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let file = File::create(&path)
            .with_context(|| format!("failed to create recording {}", path.display()))?;
        let mut writer = BufWriter::new(file);
        write_header(&mut writer, rows, cols)
            .with_context(|| format!("failed to write recording header {}", path.display()))?;

        let attachment = worker.attach();
        let worker = worker.clone();
        let task = tokio::spawn(async move {
            record_loop(writer, worker, attachment.backlog, attachment.output).await;
        });

        Ok(Self { path, task })
    }

    /// Awaits the recording task so the `.cast` file is fully flushed, then
    /// returns its path. Call before uploading the artifact.
    pub async fn finalize(self) -> PathBuf {
        let _ = self.task.await;
        self.path
    }
}

/// Writes the asciicast v2 header line.
fn write_header(writer: &mut BufWriter<File>, rows: u16, cols: u16) -> Result<()> {
    let timestamp = time::OffsetDateTime::now_utc().unix_timestamp();
    let header = json!({
        "version": 2,
        "width": cols,
        "height": rows,
        "timestamp": timestamp,
    });
    writeln!(writer, "{header}")?;
    writer.flush()?;
    Ok(())
}

/// Drains output into the writer until the agent exits, then flushes.
///
/// Like the channel output bridge, output is drained in preference to noticing
/// the exit (`biased`), and any chunks still buffered in the broadcast are
/// flushed after the process exits so the recording is not truncated.
async fn record_loop(
    mut writer: BufWriter<File>,
    worker: WorkerHandle,
    backlog: Vec<u8>,
    mut output: broadcast::Receiver<Bytes>,
) {
    let start = Instant::now();
    let mut carry: Vec<u8> = Vec::new();

    if !backlog.is_empty() {
        write_event(&mut writer, start, &mut carry, &backlog);
    }

    let exit = worker.wait_exit();
    tokio::pin!(exit);
    loop {
        tokio::select! {
            biased;
            chunk = output.recv() => match chunk {
                Ok(chunk) => write_event(&mut writer, start, &mut carry, &chunk),
                Err(broadcast::error::RecvError::Lagged(n)) => on_lagged(&mut writer, start, &mut carry, n),
                Err(broadcast::error::RecvError::Closed) => break,
            },
            _ = &mut exit => break,
        }
    }

    // Flush whatever is still buffered in the broadcast after exit.
    loop {
        match output.try_recv() {
            Ok(chunk) => write_event(&mut writer, start, &mut carry, &chunk),
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                on_lagged(&mut writer, start, &mut carry, n);
            }
            Err(_) => break,
        }
    }

    if let Err(err) = writer.flush() {
        tracing::warn!(error = %err, "failed to flush terminal recording");
    }
}

/// Handles a broadcast lag: the recorder fell behind and `skipped` chunks were
/// dropped. The byte stream is now discontinuous, so clear any partial-codepoint
/// `carry` (it would mis-frame the next surviving chunk) and write a visible gap
/// marker so the recording isn't silently corrupted.
fn on_lagged(writer: &mut BufWriter<File>, start: Instant, carry: &mut Vec<u8>, skipped: u64) {
    carry.clear();
    tracing::warn!(skipped, "terminal recording lagged; dropped output");
    let elapsed = start.elapsed().as_secs_f64();
    let event = json!([
        elapsed,
        "o",
        "\r\n[omniagent: recording lagged, output dropped]\r\n"
    ]);
    if let Err(err) = writeln!(writer, "{event}") {
        tracing::warn!(error = %err, "failed to write terminal recording gap marker");
    }
}

/// Appends one `[elapsed, "o", data]` asciicast event.
fn write_event(writer: &mut BufWriter<File>, start: Instant, carry: &mut Vec<u8>, bytes: &[u8]) {
    let data = decode_streaming(carry, bytes);
    if data.is_empty() {
        return;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let event = json!([elapsed, "o", data]);
    if let Err(err) = writeln!(writer, "{event}") {
        tracing::warn!(error = %err, "failed to write terminal recording event");
    }
}
