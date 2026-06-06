//! Spawns the supervised agent inside a PTY and exposes its terminal as a
//! broadcast byte stream with an input channel and resize control.
//!
//! `portable-pty` (wezterm) gives us a cross-platform master/slave PTY. The
//! reader and writer it hands back are blocking, so each is driven on its own
//! OS thread; output is fanned out over a `broadcast` channel and also kept in
//! a small backlog so a newly attached web/TUI client can prime its terminal.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use portable_pty::{ChildKiller, CommandBuilder, PtySize, native_pty_system};
use tokio::sync::{broadcast, mpsc, watch};

/// Bytes of recent terminal output retained for replay on attach (~256 KiB).
const BACKLOG_CAP: usize = 256 * 1024;
/// Capacity of the output broadcast channel, in chunks.
const OUTPUT_CHANNEL_CAP: usize = 2048;

/// An instruction sent toward the PTY master.
#[derive(Debug)]
enum AgentInput {
    Data(Bytes),
    Resize { rows: u16, cols: u16 },
}

/// Handle to a spawned agent process and its PTY.
#[derive(Clone)]
pub struct AgentHandle {
    input_tx: mpsc::UnboundedSender<AgentInput>,
    output_tx: broadcast::Sender<Bytes>,
    backlog: Arc<Mutex<VecDeque<u8>>>,
    exit_rx: watch::Receiver<Option<i32>>,
    /// Terminates the child process explicitly when the supervisor exits; shared
    /// across handle clones.
    killer: Arc<Mutex<AgentKiller>>,
}

impl AgentHandle {
    /// Spawns `argv` in a fresh PTY, layering `extra_env` over the inherited
    /// process environment (used to point the agent's LLM base URLs at the
    /// recording proxy).
    pub fn spawn(
        argv: &[String],
        extra_env: &[(String, String)],
        cwd: Option<&Path>,
        rows: u16,
        cols: u16,
    ) -> Result<Self> {
        let program = argv
            .first()
            .context("agent command must not be empty")?
            .clone();

        let pair = native_pty_system()
            .openpty(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .context("failed to allocate PTY")?;

        let mut builder = CommandBuilder::new(&program);
        for arg in &argv[1..] {
            builder.arg(arg);
        }
        // `CommandBuilder` seeds itself from the current process environment, so
        // applying just the overrides lets the injected base-URL variables win.
        for (key, value) in extra_env {
            builder.env(key, value);
        }
        if let Some(dir) = cwd {
            builder.cwd(dir);
        }

        let mut child = pair
            .slave
            .spawn_command(builder)
            .with_context(|| format!("failed to spawn agent {program:?}"))?;
        let pid = child.process_id();
        // Drop the slave so the master observes EOF once the child exits.
        drop(pair.slave);

        let reader = pair
            .master
            .try_clone_reader()
            .context("failed to clone PTY reader")?;
        let writer = pair
            .master
            .take_writer()
            .context("failed to take PTY writer")?;

        let (output_tx, _) = broadcast::channel::<Bytes>(OUTPUT_CHANNEL_CAP);
        let (input_tx, input_rx) = mpsc::unbounded_channel::<AgentInput>();
        let (exit_tx, exit_rx) = watch::channel::<Option<i32>>(None);
        let backlog = Arc::new(Mutex::new(VecDeque::new()));
        // Take a killer before the child is moved into the wait thread, so the
        // supervisor can terminate the agent on its own exit paths.
        let killer = Arc::new(Mutex::new(AgentKiller::new(pid, child.clone_killer())));

        spawn_reader_thread(reader, output_tx.clone(), Arc::clone(&backlog));
        spawn_control_thread(pair.master, writer, input_rx);
        spawn_wait_thread(
            move || child.wait().ok().map(|status| exit_status_code(&status)),
            exit_tx,
        );

        Ok(Self {
            input_tx,
            output_tx,
            backlog,
            exit_rx,
            killer,
        })
    }

    /// Atomically snapshots the retained backlog and subscribes to live output.
    ///
    /// The reader thread appends each chunk to the backlog and broadcasts it
    /// while holding the backlog lock, and this method takes the snapshot and
    /// subscribes under that same lock. A given chunk is therefore visible in
    /// exactly one of the two — already in the returned backlog, or yet to
    /// arrive on the receiver — never both (duplicated output) and never
    /// neither (lost output) across the attach.
    #[must_use]
    pub fn attach(&self) -> (Vec<u8>, broadcast::Receiver<Bytes>) {
        // The Ok arm clones the backlog and subscribes while still holding the
        // guard, which is what makes the pair atomic against the reader thread.
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

    /// Writes bytes to the agent's stdin (e.g. browser/TUI keystrokes).
    pub fn send_input(&self, data: Bytes) {
        let _ = self.input_tx.send(AgentInput::Data(data));
    }

    /// Resizes the PTY window.
    pub fn resize(&self, rows: u16, cols: u16) {
        let _ = self.input_tx.send(AgentInput::Resize { rows, cols });
    }

    /// Resolves once the agent exits, yielding its exit code.
    pub async fn wait_exit(&self) -> i32 {
        let mut rx = self.exit_rx.clone();
        loop {
            // Copy out of the borrow before awaiting so the watch lock is not
            // held across the suspension point.
            let current = *rx.borrow();
            if let Some(code) = current {
                return code;
            }
            if rx.changed().await.is_err() {
                return 0;
            }
        }
    }

    /// Terminates the agent process. Idempotent and safe to call after the agent
    /// has already exited; used so the PTY child — which runs in its own session
    /// and never receives the supervisor's Ctrl-C — does not outlive the
    /// supervisor on interrupt or web-server shutdown.
    pub fn shutdown(&self) {
        // If the wait thread has already reaped the child, there is nothing to
        // terminate and re-signalling this numeric pid could race with pid reuse.
        if self.exit_rx.borrow().is_some() {
            return;
        }
        if let Ok(mut killer) = self.killer.lock() {
            killer.kill();
        }
    }
}

/// Best-effort process terminator for the supervised PTY child.
///
/// `portable-pty`'s cloned Unix killer sends only SIGHUP, which a child can
/// ignore. The PTY child is created as a session leader, so on Unix we can
/// signal its whole process group and escalate to SIGKILL after a short grace
/// period. Non-Unix platforms fall back to the portable killer.
#[derive(Debug)]
struct AgentKiller {
    pid: Option<u32>,
    fallback: Box<dyn ChildKiller + Send + Sync>,
    killed: bool,
}

impl AgentKiller {
    fn new(pid: Option<u32>, fallback: Box<dyn ChildKiller + Send + Sync>) -> Self {
        Self {
            pid,
            fallback,
            killed: false,
        }
    }

    fn kill(&mut self) {
        if self.killed {
            return;
        }
        self.killed = true;
        terminate_process(self.pid, self.fallback.as_mut());
    }
}

#[cfg(unix)]
fn terminate_process(pid: Option<u32>, fallback: &mut (dyn ChildKiller + Send + Sync)) {
    let Some(pid) = pid
        .and_then(|pid| i32::try_from(pid).ok())
        .and_then(rustix::process::Pid::from_raw)
    else {
        let _ = fallback.kill();
        return;
    };

    // Children spawned by portable-pty call setsid(), so the process group id is
    // the child's pid. Signal the group to include shells and grandchildren.
    signal_process_group(pid, rustix::process::Signal::HUP);
    signal_process_group(pid, rustix::process::Signal::TERM);
    for _ in 0..10 {
        if !process_group_exists(pid) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    signal_process_group(pid, rustix::process::Signal::KILL);
    wait_for_process_group_exit(pid, 10);
}

#[cfg(unix)]
fn signal_process_group(pid: rustix::process::Pid, signal: rustix::process::Signal) {
    // ESRCH just means the process group already exited.
    match rustix::process::kill_process_group(pid, signal) {
        Ok(()) | Err(rustix::io::Errno::SRCH) => {}
        Err(err) => {
            tracing::debug!(pid = %pid, signal = ?signal, error = %err, "failed to signal agent process group");
        }
    }
}

#[cfg(unix)]
fn process_group_exists(pid: rustix::process::Pid) -> bool {
    !matches!(
        rustix::process::test_kill_process_group(pid),
        Err(rustix::io::Errno::SRCH)
    )
}

#[cfg(unix)]
fn wait_for_process_group_exit(pid: rustix::process::Pid, attempts: usize) {
    for _ in 0..attempts {
        if !process_group_exists(pid) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

#[cfg(not(unix))]
fn terminate_process(_pid: Option<u32>, fallback: &mut (dyn ChildKiller + Send + Sync)) {
    let _ = fallback.kill();
}

fn exit_status_code(status: &portable_pty::ExitStatus) -> i32 {
    i32::try_from(status.exit_code()).unwrap_or(-1)
}

fn spawn_reader_thread(
    mut reader: Box<dyn Read + Send>,
    output_tx: broadcast::Sender<Bytes>,
    backlog: Arc<Mutex<VecDeque<u8>>>,
) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let chunk = Bytes::copy_from_slice(&buf[..n]);
                    // Append to the backlog and broadcast under one lock so an
                    // attaching client (see `AgentHandle::attach`) observes each
                    // chunk in exactly one place — never lost in the gap between
                    // snapshot and subscribe, never delivered twice.
                    if let Ok(mut buf) = backlog.lock() {
                        buf.extend(chunk.iter().copied());
                        if buf.len() > BACKLOG_CAP {
                            let overflow = buf.len() - BACKLOG_CAP;
                            buf.drain(..overflow);
                        }
                        // Ignore the "no subscribers" error; backlog still has it.
                        let _ = output_tx.send(chunk);
                    }
                }
            }
        }
    });
}

fn spawn_control_thread(
    master: Box<dyn portable_pty::MasterPty + Send>,
    mut writer: Box<dyn Write + Send>,
    mut input_rx: mpsc::UnboundedReceiver<AgentInput>,
) {
    std::thread::spawn(move || {
        while let Some(input) = input_rx.blocking_recv() {
            match input {
                AgentInput::Data(data) => {
                    if writer.write_all(&data).is_err() {
                        break;
                    }
                    let _ = writer.flush();
                }
                AgentInput::Resize { rows, cols } => {
                    let _ = master.resize(PtySize {
                        rows,
                        cols,
                        pixel_width: 0,
                        pixel_height: 0,
                    });
                }
            }
        }
    });
}

fn spawn_wait_thread<F>(wait: F, exit_tx: watch::Sender<Option<i32>>)
where
    F: FnOnce() -> Option<i32> + Send + 'static,
{
    std::thread::spawn(move || {
        let code = wait().unwrap_or(-1);
        let _ = exit_tx.send(Some(code));
    });
}
