use std::fs::OpenOptions as StdOpenOptions;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};

use oad_core::{OadPaths, PAUSE_CONTAINER, SandboxId, sync_dir, sync_file, temp_path};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tracing::debug;

/// gVisor runtime program name, resolved from `PATH` at invocation time.
const RUNSC: &str = "runsc";
pub const RUNSC_CHECKPOINT_IMAGE: &str = "checkpoint.img";
const RUNSC_ERROR_LOG_TAIL_BYTES: u64 = 64 * 1024;
const SAVE_RESTORE_NETSTACK_ARG: &str = "-save-restore-netstack=true";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreSpecValidation {
    Enforce,
    Warning,
}

impl RestoreSpecValidation {
    const fn as_runsc_arg(self) -> &'static str {
        match self {
            Self::Enforce => "enforce",
            Self::Warning => "warning",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Runsc {
    state_dir: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub struct RestoreConfig<'a> {
    pub bundle_dir: &'a Path,
    pub image_dir: &'a Path,
    pub pidfile: &'a Path,
    pub overlay_dir: &'a Path,
    pub spec_validation: RestoreSpecValidation,
    pub log_path: &'a Path,
}

/// Captured result of running a command inside a container via `runsc exec`.
#[derive(Debug, Clone)]
pub struct ExecOutput {
    /// Exit code of the executed command (-1 if terminated by a signal).
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Running `runsc exec` process with attached standard streams.
pub struct ExecProcess {
    pub child: Child,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
    pub stderr: ChildStderr,
}

impl Runsc {
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: state_dir.into(),
        }
    }

    /// Creates (but does not start) a container via `runsc create`.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if `runsc` cannot be spawned or exits non-zero.
    pub async fn create(
        &self,
        bundle_dir: &Path,
        pidfile: &Path,
        container: &str,
        overlay_dir: &Path,
        log_path: &Path,
    ) -> Result<(), RuntimeError> {
        let args = self.create_args(bundle_dir, pidfile, container, overlay_dir);
        self.run_logged(args, log_path).await
    }

    /// Starts a previously created container via `runsc start`.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if `runsc` cannot be spawned or exits non-zero.
    pub async fn start(
        &self,
        container: &str,
        overlay_dir: &Path,
        log_path: &Path,
    ) -> Result<(), RuntimeError> {
        let args = self.start_args(container, overlay_dir);
        self.run_logged(args, log_path).await
    }

    /// Checkpoints a container's whole sandbox into `image_dir` via
    /// `runsc checkpoint`. Checkpoint the sandbox root (`pause`) container to
    /// capture the state of every container in the sandbox.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if `runsc` cannot be spawned or exits non-zero.
    pub async fn checkpoint(
        &self,
        container: &str,
        image_dir: &Path,
        overlay_dir: &Path,
        log_path: &Path,
        leave_running: bool,
    ) -> Result<(), RuntimeError> {
        let args = self.checkpoint_args(container, image_dir, overlay_dir, leave_running);
        self.run_logged(args, log_path).await
    }

    /// Restores a container from a checkpoint image via `runsc restore`, leaving
    /// it running detached.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if `runsc` cannot be spawned or exits non-zero.
    pub async fn restore(
        &self,
        container: &str,
        config: RestoreConfig<'_>,
    ) -> Result<(), RuntimeError> {
        let args = self.restore_args(
            container,
            config.bundle_dir,
            config.image_dir,
            config.pidfile,
            config.overlay_dir,
            config.spec_validation,
        );
        self.run_logged(args, config.log_path).await
    }

    /// Deletes a container via `runsc delete --force`.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if `runsc` cannot be spawned or exits non-zero.
    pub async fn delete(&self, container: &str) -> Result<(), RuntimeError> {
        let runsc_log = temp_runsc_log()?;
        let args = self.delete_args(container, Some(runsc_log.path()));
        self.run(args).await.map(|_| ())
    }

    /// Returns the raw JSON state of a container via `runsc state`.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if `runsc` cannot be spawned or exits non-zero.
    pub async fn state(&self, container: &str) -> Result<String, RuntimeError> {
        let runsc_log = temp_runsc_log()?;
        let args = self.state_args(container, Some(runsc_log.path()));
        let output = self.run(args).await?;
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn base_args(&self, log_path: Option<&Path>) -> Vec<String> {
        let mut args = vec!["-log-format".to_string(), "json".to_string()];
        if let Some(log_path) = log_path {
            args.extend(["-log".to_string(), log_path.display().to_string()]);
        }
        args.extend(["-root".to_string(), self.state_dir.display().to_string()]);
        args
    }

    /// Builds the `--overlay2` global flag backing the read-only EROFS rootfs
    /// with a writable overlay in `overlay_dir`.
    ///
    /// The rootfs is an anonymous (non-gofer) EROFS mount, for which gVisor
    /// requires the root overlay medium to be a host directory: the default
    /// `self` medium would store the upper layer inside the rootfs itself, which
    /// is impossible on a read-only EROFS image, and `runsc` panics with
    /// "anonymous overlay medium = \"self\" does not have dir= prefix". Pointing
    /// the medium at a per-container host directory gives each container an
    /// isolated, writable upper layer. The flag must be passed to both `create`
    /// and `start`, since starting a (sub)container spawns a gofer process that
    /// re-reads the overlay configuration.
    fn overlay_arg(overlay_dir: &Path) -> String {
        format!("--overlay2=root:dir={}", overlay_dir.display())
    }

    #[must_use]
    pub fn create_args(
        &self,
        bundle_dir: &Path,
        pidfile: &Path,
        container: &str,
        overlay_dir: &Path,
    ) -> Vec<String> {
        let mut args = self.base_args(None);
        args.push(Self::overlay_arg(overlay_dir));
        args.extend([
            "create".to_string(),
            "-bundle".to_string(),
            bundle_dir.display().to_string(),
            "-pid-file".to_string(),
            pidfile.display().to_string(),
            container.to_string(),
        ]);
        args
    }

    #[must_use]
    pub fn start_args(&self, container: &str, overlay_dir: &Path) -> Vec<String> {
        let mut args = self.base_args(None);
        args.extend([
            "-net-disconnect-ok=true".to_string(),
            SAVE_RESTORE_NETSTACK_ARG.to_string(),
        ]);
        args.push(Self::overlay_arg(overlay_dir));
        args.extend(["start".to_string(), container.to_string()]);
        args
    }

    #[must_use]
    pub fn checkpoint_args(
        &self,
        container: &str,
        image_dir: &Path,
        overlay_dir: &Path,
        leave_running: bool,
    ) -> Vec<String> {
        let mut args = self.base_args(None);
        args.push(Self::overlay_arg(overlay_dir));
        args.push("checkpoint".to_string());
        if leave_running {
            args.push("-leave-running".to_string());
        }
        args.extend([
            "-image-path".to_string(),
            image_dir.display().to_string(),
            container.to_string(),
        ]);
        args
    }

    #[must_use]
    pub fn restore_args(
        &self,
        container: &str,
        bundle_dir: &Path,
        image_dir: &Path,
        pidfile: &Path,
        overlay_dir: &Path,
        spec_validation: RestoreSpecValidation,
    ) -> Vec<String> {
        let mut args = self.base_args(None);
        args.push(SAVE_RESTORE_NETSTACK_ARG.to_string());
        args.push(Self::overlay_arg(overlay_dir));
        args.extend([
            "-restore-spec-validation".to_string(),
            spec_validation.as_runsc_arg().to_string(),
        ]);
        args.extend([
            "restore".to_string(),
            "-bundle".to_string(),
            bundle_dir.display().to_string(),
            "-image-path".to_string(),
            image_dir.display().to_string(),
            "-pid-file".to_string(),
            pidfile.display().to_string(),
            "-detach".to_string(),
            container.to_string(),
        ]);
        args
    }

    #[must_use]
    pub fn delete_args(&self, container: &str, log_path: Option<&Path>) -> Vec<String> {
        let mut args = self.base_args(log_path);
        args.extend([
            "delete".to_string(),
            "-force".to_string(),
            container.to_string(),
        ]);
        args
    }

    #[must_use]
    pub fn state_args(&self, container: &str, log_path: Option<&Path>) -> Vec<String> {
        let mut args = self.base_args(log_path);
        args.extend(["state".to_string(), container.to_string()]);
        args
    }

    /// Executes a new process inside an already-running container.
    ///
    /// A `--` separator precedes the container id and command so that argv
    /// entries beginning with `-` are not parsed as `runsc exec` flags.
    #[must_use]
    pub fn exec_args(
        &self,
        container: &str,
        command: &[String],
        env: &[String],
        cwd: Option<&str>,
        log_path: Option<&Path>,
    ) -> Vec<String> {
        let mut args = self.base_args(log_path);
        args.push("exec".to_string());
        if let Some(cwd) = cwd {
            args.push("-cwd".to_string());
            args.push(cwd.to_string());
        }
        for var in env {
            args.push("-env".to_string());
            args.push(var.clone());
        }
        args.push("--".to_string());
        args.push(container.to_string());
        args.extend(command.iter().cloned());
        args
    }

    /// Runs `argv` inside `container` and captures its output.
    ///
    /// Unlike [`create`](Self::create)/[`start`](Self::start), a non-zero exit
    /// of the executed command is a normal result returned in
    /// [`ExecOutput::exit_code`], not a [`RuntimeError`]. Only failure to spawn
    /// `runsc` itself is an error.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::Spawn`] if `runsc` cannot be spawned.
    pub async fn exec(
        &self,
        container: &str,
        command: &[String],
        env: &[String],
        cwd: Option<&str>,
    ) -> Result<ExecOutput, RuntimeError> {
        let runsc_log = temp_runsc_log()?;
        let args = self.exec_args(container, command, env, cwd, Some(runsc_log.path()));
        debug!(runsc = RUNSC, args = ?args, "running runsc exec");
        let output = Command::new(RUNSC)
            .args(&args)
            .output()
            .await
            .map_err(RuntimeError::Spawn)?;
        Ok(ExecOutput {
            // A signal-terminated process has no exit code; report -1.
            exit_code: output.status.code().unwrap_or(-1),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }

    /// Starts `argv` inside `container` and returns attached process streams.
    ///
    /// The returned child is the host-side `runsc exec` process. Its standard
    /// streams are connected to the executed process inside the container.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::Spawn`] if `runsc` cannot be spawned.
    pub async fn spawn_exec(
        &self,
        container: &str,
        command: &[String],
        env: &[String],
        cwd: Option<&str>,
        log_path: Option<&Path>,
    ) -> Result<ExecProcess, RuntimeError> {
        if let Some(parent) = log_path.and_then(Path::parent) {
            fs::create_dir_all(parent).await.map_err(RuntimeError::Io)?;
        }
        let args = self.exec_args(container, command, env, cwd, log_path);
        debug!(runsc = RUNSC, args = ?args, "spawning runsc exec");
        let mut child = Command::new(RUNSC)
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(RuntimeError::Spawn)?;

        let Some(stdin) = child.stdin.take() else {
            let _ = child.start_kill();
            return Err(RuntimeError::Io(std::io::Error::other(
                "spawned runsc exec without stdin pipe",
            )));
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = child.start_kill();
            return Err(RuntimeError::Io(std::io::Error::other(
                "spawned runsc exec without stdout pipe",
            )));
        };
        let Some(stderr) = child.stderr.take() else {
            let _ = child.start_kill();
            return Err(RuntimeError::Io(std::io::Error::other(
                "spawned runsc exec without stderr pipe",
            )));
        };

        Ok(ExecProcess {
            child,
            stdin,
            stdout,
            stderr,
        })
    }

    async fn run_logged(&self, args: Vec<String>, log_path: &Path) -> Result<(), RuntimeError> {
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent).await.map_err(RuntimeError::Io)?;
        }
        let stdout = StdOpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .map_err(RuntimeError::Io)?;
        let stderr = stdout.try_clone().map_err(RuntimeError::Io)?;

        debug!(runsc = RUNSC, args = ?args, log = %log_path.display(), "running runsc");
        let status = Command::new(RUNSC)
            .args(&args)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .status()
            .await
            .map_err(RuntimeError::Spawn)?;

        if status.success() {
            Ok(())
        } else {
            Err(RuntimeError::Exit {
                status,
                args,
                stdout: String::new(),
                stderr: runsc_log_error(log_path).await,
            })
        }
    }

    async fn run(&self, args: Vec<String>) -> Result<std::process::Output, RuntimeError> {
        debug!(runsc = RUNSC, args = ?args, "running runsc");
        let output = Command::new(RUNSC)
            .args(&args)
            .output()
            .await
            .map_err(RuntimeError::Spawn)?;

        if output.status.success() {
            Ok(output)
        } else {
            Err(RuntimeError::Exit {
                status: output.status,
                args,
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            })
        }
    }
}

/// Builds a [`Runsc`] for a sandbox, rooted at its per-sandbox state directory.
fn runsc_for_sandbox(paths: &OadPaths, sandbox_id: &SandboxId) -> Runsc {
    Runsc::new(paths.runsc_state_dir(sandbox_id))
}

/// Creates and starts every container in a sandbox, in order.
///
/// # Errors
///
/// Returns [`RuntimeError`] if creating the state directories fails, or if any
/// container's `runsc create`/`runsc start` invocation fails.
pub async fn start_container_sequence(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    containers: &[String],
) -> Result<(), RuntimeError> {
    fs::create_dir_all(paths.runsc_state_dir(sandbox_id))
        .await
        .map_err(RuntimeError::Io)?;
    fs::create_dir_all(paths.pidfiles_dir(sandbox_id))
        .await
        .map_err(RuntimeError::Io)?;
    fs::create_dir_all(paths.logs_dir(sandbox_id))
        .await
        .map_err(RuntimeError::Io)?;

    let runsc = runsc_for_sandbox(paths, sandbox_id);
    for container in containers {
        let bundle = paths.bundle_dir(sandbox_id, container);
        let pidfile = paths.pidfile(sandbox_id, container);
        let overlay = paths.rootfs_overlay_dir(sandbox_id, container);
        let log = paths.container_log(sandbox_id, container);
        runsc
            .create(&bundle, &pidfile, container, &overlay, &log)
            .await?;
        runsc.start(container, &overlay, &log).await?;
    }
    Ok(())
}

/// Deletes every container that is still visible to `runsc state`, in reverse
/// order.
///
/// Containers that are already gone are skipped. Delete failures for visible
/// containers are returned to the caller.
///
/// # Errors
///
/// Reserved for future setup failures before deletion begins.
pub async fn delete_visible_container_sequence(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    containers: &[String],
) -> Result<Vec<(String, String)>, RuntimeError> {
    let runsc = runsc_for_sandbox(paths, sandbox_id);
    Ok(delete_visible_containers(&runsc, sandbox_id, containers, "reconciliation cleanup").await)
}

/// Deletes every container still visible to `runsc state`, in reverse order,
/// collecting failures rather than stopping on the first one.
///
/// Containers that are already gone are silently skipped. The `context` string
/// is included in debug log messages to identify the call site.
async fn delete_visible_containers(
    runsc: &Runsc,
    sandbox_id: &SandboxId,
    containers: &[String],
    context: &str,
) -> Vec<(String, String)> {
    let mut failures = Vec::new();
    for container in containers.iter().rev() {
        if let Err(err) = runsc.state(container).await {
            debug!(
                sandbox_id = %sandbox_id,
                container,
                %err,
                context,
                "container not visible during cleanup"
            );
            continue;
        }
        if let Err(err) = runsc.delete(container).await {
            failures.push((container.clone(), err.to_string()));
        }
    }
    failures
}

/// Checkpoints an entire sandbox into `image_dir`, then tears its containers
/// down (freeing memory) while leaving the bundles on disk for a later restore.
///
/// Each container gets its own checkpoint image under `image_dir/<container>`.
/// `runsc checkpoint` saves the named container's current process set; a single
/// checkpoint of the root `pause` container is not enough to preserve processes
/// created in workload containers via `runsc exec`. The root container is
/// checkpointed first with `--leave-running`, so restore can put the sandbox
/// into restore mode before subcontainers are restored. Workload containers are
/// checkpointed afterward, with only the final checkpoint stopping the sandbox.
/// `runsc state` is queried before each delete (mirroring containerd) to avoid
/// a delete that races the freshly stopped sandbox.
///
/// # Errors
///
/// Returns [`RuntimeError`] if the temporary image directory cannot be created
/// or published, or if the `runsc checkpoint`/`delete` invocations fail.
pub async fn checkpoint_sandbox(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    containers: &[String],
    image_dir: &Path,
) -> Result<(), RuntimeError> {
    let tmp_image_dir = temp_path(image_dir);
    let checkpointed = async {
        remove_dir_if_exists(&tmp_image_dir).await?;
        fs::create_dir_all(&tmp_image_dir)
            .await
            .map_err(RuntimeError::Io)?;

        checkpoint_suspend_images(paths, sandbox_id, containers, &tmp_image_dir).await?;

        let runsc = runsc_for_sandbox(paths, sandbox_id);
        let failures =
            delete_visible_containers(&runsc, sandbox_id, containers, "post-checkpoint teardown")
                .await;
        if !failures.is_empty() {
            return Err(RuntimeError::TeardownFailures(failures));
        }

        publish_checkpoint_dir(&tmp_image_dir, image_dir).await?;
        Ok(())
    }
    .await;

    if checkpointed.is_err() {
        let _ = fs::remove_dir_all(&tmp_image_dir).await;
    }
    checkpointed
}

/// Snapshots a running sandbox into `image_dir` without stopping it.
///
/// Like [`checkpoint_sandbox`], each workload container gets its own image, but
/// `--leave-running` keeps the live sandbox executing afterward, so the
/// snapshot is a point-in-time fork source rather than a suspend. The
/// containers are *not* torn down.
///
/// # Errors
///
/// Returns [`RuntimeError`] if the temporary image directory cannot be created
/// or published, or if the `runsc checkpoint` invocation fails.
pub async fn snapshot_sandbox(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    containers: &[String],
    image_dir: &Path,
) -> Result<(), RuntimeError> {
    let tmp_image_dir = temp_path(image_dir);
    let snapshotted = async {
        remove_dir_if_exists(&tmp_image_dir).await?;
        fs::create_dir_all(&tmp_image_dir)
            .await
            .map_err(RuntimeError::Io)?;

        checkpoint_container_images(
            paths,
            sandbox_id,
            checkpoint_targets(containers),
            &tmp_image_dir,
        )
        .await?;

        publish_checkpoint_dir(&tmp_image_dir, image_dir).await?;
        Ok(())
    }
    .await;

    if snapshotted.is_err() {
        let _ = fs::remove_dir_all(&tmp_image_dir).await;
    }
    snapshotted
}

async fn checkpoint_container_images(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    containers: Vec<(String, bool)>,
    image_dir: &Path,
) -> Result<(), RuntimeError> {
    let runsc = runsc_for_sandbox(paths, sandbox_id);
    for (container, leave_running) in containers {
        let container_image_dir = container_checkpoint_dir(image_dir, &container);
        fs::create_dir_all(&container_image_dir)
            .await
            .map_err(RuntimeError::Io)?;
        let overlay = paths.rootfs_overlay_dir(sandbox_id, &container);
        let log = paths.container_log(sandbox_id, &container);
        runsc
            .checkpoint(
                &container,
                &container_image_dir,
                &overlay,
                &log,
                leave_running,
            )
            .await?;
    }
    Ok(())
}

async fn checkpoint_suspend_images(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    containers: &[String],
    image_dir: &Path,
) -> Result<(), RuntimeError> {
    checkpoint_container_images(
        paths,
        sandbox_id,
        checkpoint_suspend_targets(containers),
        image_dir,
    )
    .await
}

fn checkpoint_targets(containers: &[String]) -> Vec<(String, bool)> {
    checkpoint_target_names(containers)
        .into_iter()
        .map(|container| (container, true))
        .collect()
}

fn checkpoint_suspend_targets(containers: &[String]) -> Vec<(String, bool)> {
    let names = checkpoint_target_names(containers);
    let last_index = names.len().saturating_sub(1);
    names
        .into_iter()
        .enumerate()
        .map(|(index, container)| (container, index != last_index))
        .collect()
}

fn checkpoint_target_names(containers: &[String]) -> Vec<String> {
    let workloads = containers
        .iter()
        .filter(|container| container.as_str() != PAUSE_CONTAINER)
        .cloned()
        .collect::<Vec<_>>();
    if containers
        .iter()
        .any(|container| container.as_str() == PAUSE_CONTAINER)
    {
        let mut targets = vec![PAUSE_CONTAINER.to_string()];
        targets.extend(workloads);
        targets
    } else {
        workloads
    }
}

fn container_checkpoint_dir(image_dir: &Path, container: &str) -> PathBuf {
    image_dir.join(container)
}

async fn checkpoint_image_dir_for_container(
    image_dir: &Path,
    container: &str,
) -> Result<PathBuf, RuntimeError> {
    let container_image_dir = container_checkpoint_dir(image_dir, container);
    if checkpoint_dir_has_image(&container_image_dir).await {
        return Ok(container_image_dir);
    }
    if checkpoint_dir_has_image(image_dir).await {
        return Ok(image_dir.to_path_buf());
    }
    Err(RuntimeError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "no checkpoint image for container {container} under {}",
            image_dir.display()
        ),
    )))
}

/// Copies an existing checkpoint image from `src_image_dir` into `dst_image_dir`,
/// publishing it atomically.
///
/// Used to snapshot a suspended sandbox: it already has a checkpoint image on
/// disk (from `suspend`), so the snapshot reuses that image directly rather than
/// resuming the sandbox just to re-checkpoint it.
///
/// # Errors
///
/// Returns [`RuntimeError::Io`] if the source has no checkpoint image, or if the
/// copy or atomic publish fails.
pub async fn copy_checkpoint_image(
    src_image_dir: &Path,
    dst_image_dir: &Path,
    containers: &[String],
) -> Result<(), RuntimeError> {
    if !checkpoint_image_complete_for_containers(src_image_dir, containers).await {
        return Err(RuntimeError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("no checkpoint image at {}", src_image_dir.display()),
        )));
    }

    let tmp_image_dir = temp_path(dst_image_dir);
    let copied = async {
        remove_dir_if_exists(&tmp_image_dir).await?;
        fs::create_dir_all(&tmp_image_dir)
            .await
            .map_err(RuntimeError::Io)?;
        copy_dir_recursive(src_image_dir, &tmp_image_dir).await?;
        publish_checkpoint_dir(&tmp_image_dir, dst_image_dir).await?;
        Ok(())
    }
    .await;

    if copied.is_err() {
        let _ = fs::remove_dir_all(&tmp_image_dir).await;
    }
    copied
}

/// Recursively copies the contents of `src` into the existing directory `dst`.
async fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), RuntimeError> {
    let mut stack = vec![(src.to_path_buf(), dst.to_path_buf())];
    while let Some((from, to)) = stack.pop() {
        let mut entries = fs::read_dir(&from).await.map_err(RuntimeError::Io)?;
        while let Some(entry) = entries.next_entry().await.map_err(RuntimeError::Io)? {
            let file_type = entry.file_type().await.map_err(RuntimeError::Io)?;
            let target = to.join(entry.file_name());
            if file_type.is_dir() {
                fs::create_dir_all(&target)
                    .await
                    .map_err(RuntimeError::Io)?;
                stack.push((entry.path(), target));
            } else {
                fs::copy(entry.path(), &target)
                    .await
                    .map_err(RuntimeError::Io)?;
            }
        }
    }
    Ok(())
}

async fn remove_dir_if_exists(path: &Path) -> Result<(), RuntimeError> {
    match fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(RuntimeError::Io(err)),
    }
}

async fn path_exists(path: &Path) -> Result<bool, RuntimeError> {
    fs::try_exists(path).await.map_err(RuntimeError::Io)
}

async fn publish_checkpoint_dir(
    tmp_image_dir: &Path,
    image_dir: &Path,
) -> Result<(), RuntimeError> {
    sync_checkpoint_dir(tmp_image_dir).await?;
    if let Some(parent) = image_dir.parent() {
        fs::create_dir_all(parent).await.map_err(RuntimeError::Io)?;
    }
    let backup = backup_checkpoint_dir(image_dir);
    // Remove any stale backup unconditionally; remove_dir_if_exists is a no-op
    // when the path doesn't exist, so the extra path_exists guard is unnecessary.
    remove_dir_if_exists(&backup).await?;

    let moved_existing_to_backup = if path_exists(image_dir).await? {
        fs::rename(image_dir, &backup)
            .await
            .map_err(RuntimeError::Io)?;
        if let Some(parent) = image_dir.parent() {
            sync_dir(parent).await.map_err(RuntimeError::Io)?;
        }
        true
    } else {
        false
    };

    if let Err(err) = fs::rename(tmp_image_dir, image_dir).await {
        if moved_existing_to_backup {
            let _ = fs::rename(&backup, image_dir).await;
            if let Some(parent) = image_dir.parent() {
                let _ = sync_dir(parent).await;
            }
        }
        return Err(RuntimeError::Io(err));
    }

    if let Some(parent) = image_dir.parent() {
        sync_dir(parent).await.map_err(RuntimeError::Io)?;
    }
    // Remove the backup; no need to sync the parent again — the rename above
    // already made the published image durable.
    remove_dir_if_exists(&backup).await?;
    Ok(())
}

async fn sync_checkpoint_dir(image_dir: &Path) -> Result<(), RuntimeError> {
    let top_level_image = image_dir.join(RUNSC_CHECKPOINT_IMAGE);
    if fs::try_exists(&top_level_image)
        .await
        .map_err(RuntimeError::Io)?
    {
        sync_file(&top_level_image)
            .await
            .map_err(RuntimeError::Io)?;
    } else {
        let mut entries = fs::read_dir(image_dir).await.map_err(RuntimeError::Io)?;
        while let Some(entry) = entries.next_entry().await.map_err(RuntimeError::Io)? {
            if !entry.file_type().await.map_err(RuntimeError::Io)?.is_dir() {
                continue;
            }
            let container_dir = entry.path();
            sync_file(&container_dir.join(RUNSC_CHECKPOINT_IMAGE))
                .await
                .map_err(RuntimeError::Io)?;
            sync_dir(&container_dir).await.map_err(RuntimeError::Io)?;
        }
    }
    sync_dir(image_dir).await.map_err(RuntimeError::Io)
}

fn backup_checkpoint_dir(image_dir: &Path) -> PathBuf {
    let dir_name = image_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("checkpoint");
    image_dir.with_file_name(format!(".{dir_name}.backup"))
}

/// Restores an entire sandbox from a checkpoint image, recreating each
/// container (root `pause` first) from its on-disk bundle.
///
/// New checkpoints store one image per container under
/// `image_dir/<container>`; legacy checkpoints used one top-level image and are
/// still accepted.
///
/// # Errors
///
/// Returns [`RuntimeError`] if creating the state directories fails, or if any
/// container's `runsc restore` invocation fails.
pub async fn restore_sandbox(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    containers: &[String],
    image_dir: &Path,
    spec_validation: RestoreSpecValidation,
) -> Result<(), RuntimeError> {
    fs::create_dir_all(paths.runsc_state_dir(sandbox_id))
        .await
        .map_err(RuntimeError::Io)?;
    fs::create_dir_all(paths.pidfiles_dir(sandbox_id))
        .await
        .map_err(RuntimeError::Io)?;
    fs::create_dir_all(paths.logs_dir(sandbox_id))
        .await
        .map_err(RuntimeError::Io)?;

    let runsc = runsc_for_sandbox(paths, sandbox_id);
    let legacy_checkpoint = checkpoint_dir_has_image(image_dir).await;
    for container in containers {
        let bundle = paths.bundle_dir(sandbox_id, container);
        let pidfile = paths.pidfile(sandbox_id, container);
        let overlay = paths.rootfs_overlay_dir(sandbox_id, container);
        let log = paths.container_log(sandbox_id, container);
        if legacy_checkpoint
            || checkpoint_dir_has_image(&container_checkpoint_dir(image_dir, container)).await
        {
            let container_image_dir =
                checkpoint_image_dir_for_container(image_dir, container).await?;
            runsc
                .restore(
                    container,
                    RestoreConfig {
                        bundle_dir: &bundle,
                        image_dir: &container_image_dir,
                        pidfile: &pidfile,
                        overlay_dir: &overlay,
                        spec_validation,
                        log_path: &log,
                    },
                )
                .await?;
        } else if container == PAUSE_CONTAINER {
            runsc
                .create(&bundle, &pidfile, container, &overlay, &log)
                .await?;
            runsc.start(container, &overlay, &log).await?;
        } else {
            return Err(RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "no checkpoint image for container {container} under {}",
                    image_dir.display()
                ),
            )));
        }
    }
    Ok(())
}

/// Returns true when the expected checkpoint images exist, recovering a backup
/// left by an interrupted publish when possible.
pub async fn checkpoint_image_complete_for_containers(
    image_dir: &Path,
    containers: &[String],
) -> bool {
    if checkpoint_dir_has_image(image_dir).await
        || checkpoint_tree_complete_for_containers(image_dir, containers).await
    {
        let _ = remove_dir_if_exists(&backup_checkpoint_dir(image_dir)).await;
        return true;
    }
    recover_backup_checkpoint_dir_for_containers(image_dir, containers).await
}

async fn checkpoint_dir_has_image(image_dir: &Path) -> bool {
    fs::try_exists(image_dir.join(RUNSC_CHECKPOINT_IMAGE))
        .await
        .unwrap_or(false)
}

async fn checkpoint_tree_complete_for_containers(image_dir: &Path, containers: &[String]) -> bool {
    let targets = checkpoint_target_names(containers);
    if targets.is_empty() {
        return false;
    }

    for container in targets {
        if !checkpoint_dir_has_image(&container_checkpoint_dir(image_dir, &container)).await {
            return false;
        }
    }
    true
}

async fn recover_backup_checkpoint_dir_for_containers(
    image_dir: &Path,
    containers: &[String],
) -> bool {
    let backup = backup_checkpoint_dir(image_dir);
    if !checkpoint_dir_has_image(&backup).await
        && !checkpoint_tree_complete_for_containers(&backup, containers).await
    {
        return false;
    }

    let _ = remove_dir_if_exists(image_dir).await;
    match fs::rename(&backup, image_dir).await {
        Ok(()) => {
            if let Some(parent) = image_dir.parent() {
                let _ = sync_dir(parent).await;
            }
            true
        }
        Err(_) => {
            checkpoint_dir_has_image(image_dir).await
                || checkpoint_tree_complete_for_containers(image_dir, containers).await
        }
    }
}

/// Executes a command inside a running container of a sandbox.
///
/// A non-zero exit of the executed command is reported in the returned
/// [`ExecOutput`], not as an error; only failure to spawn `runsc` is an error.
///
/// # Errors
///
/// Returns [`RuntimeError::Spawn`] if `runsc` cannot be spawned.
pub async fn exec_in_container(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    container: &str,
    argv: &[String],
    env: &[String],
    cwd: Option<&str>,
) -> Result<ExecOutput, RuntimeError> {
    let runsc = runsc_for_sandbox(paths, sandbox_id);
    runsc.exec(container, argv, env, cwd).await
}

/// Starts a command inside a running container and returns attached process
/// streams for background control.
///
/// # Errors
///
/// Returns [`RuntimeError::Spawn`] if `runsc` cannot be spawned.
pub async fn spawn_exec_in_container(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    container: &str,
    argv: &[String],
    env: &[String],
    cwd: Option<&str>,
    log_path: Option<&Path>,
) -> Result<ExecProcess, RuntimeError> {
    let runsc = runsc_for_sandbox(paths, sandbox_id);
    runsc.spawn_exec(container, argv, env, cwd, log_path).await
}

/// Reports whether `container` in the sandbox is currently running according to
/// `runsc state`.
///
/// Any failure to query the runtime (missing container, stale state directory,
/// `runsc` error) is treated as not-running, so a restarted daemon can reconcile
/// a sandbox it can no longer see down to `Stopped` rather than leaving it
/// `Unknown`.
pub async fn container_running(paths: &OadPaths, sandbox_id: &SandboxId, container: &str) -> bool {
    container_running_result(paths, sandbox_id, container)
        .await
        .unwrap_or(false)
}

/// Reports whether `container` is running, preserving runtime query failures
/// for callers that must distinguish "stopped" from "could not determine".
///
/// # Errors
///
/// Returns [`RuntimeError`] if `runsc state` cannot be queried.
pub async fn container_running_result(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    container: &str,
) -> Result<bool, RuntimeError> {
    let runsc = runsc_for_sandbox(paths, sandbox_id);
    let state = runsc.state(container).await?;
    Ok(parse_runsc_status(&state).as_deref() == Some("running"))
}

/// Extracts the OCI `status` field from `runsc state` JSON output.
fn parse_runsc_status(state_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(state_json)
        .ok()?
        .get("status")?
        .as_str()
        .map(str::to_string)
}

async fn read_tail(path: &Path, max_bytes: u64) -> std::io::Result<String> {
    let mut file = fs::File::open(path).await?;
    let len = file.metadata().await?.len();
    let start = len.saturating_sub(max_bytes);
    file.seek(SeekFrom::Start(start)).await?;

    let capacity = usize::try_from(len - start).unwrap_or(usize::MAX);
    let mut buffer = Vec::with_capacity(capacity);
    file.read_to_end(&mut buffer).await?;
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

async fn runsc_log_error(log_path: &Path) -> String {
    match read_tail(log_path, RUNSC_ERROR_LOG_TAIL_BYTES).await {
        Ok(log) if log.trim().is_empty() => format!("see {} (log was empty)", log_path.display()),
        Ok(log) => format!("see {}; log tail:\n{}", log_path.display(), log),
        Err(err) => format!("see {} (failed to read log: {err})", log_path.display()),
    }
}

fn temp_runsc_log() -> Result<NamedTempFile, RuntimeError> {
    tempfile::Builder::new()
        .prefix("oad-runsc-")
        .suffix(".jsonl")
        .tempfile()
        .map_err(RuntimeError::Io)
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("failed to spawn runsc: {0}")]
    Spawn(std::io::Error),
    #[error("runsc exited with {status}; args={args:?}; stdout={stdout:?}; stderr={stderr:?}")]
    Exit {
        status: ExitStatus,
        args: Vec<String>,
        stdout: String,
        stderr: String,
    },
    #[error("i/o error: {0}")]
    Io(std::io::Error),
    #[error("failed to delete containers after checkpoint: {0:?}")]
    TeardownFailures(Vec<(String, String)>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_args_match_runsc_contract() {
        let runsc = Runsc::new("/tmp/state");
        assert_eq!(
            runsc.create_args(
                Path::new("/tmp/bundle"),
                Path::new("/tmp/pid"),
                "web",
                Path::new("/tmp/bundle/rootfs-overlay"),
            ),
            vec![
                "-log-format",
                "json",
                "-root",
                "/tmp/state",
                "--overlay2=root:dir=/tmp/bundle/rootfs-overlay",
                "create",
                "-bundle",
                "/tmp/bundle",
                "-pid-file",
                "/tmp/pid",
                "web",
            ]
        );
    }

    #[test]
    fn start_args_carry_overlay_and_checkpoint_netstack_flags() {
        let runsc = Runsc::new("/tmp/state");
        assert_eq!(
            runsc.start_args("web", Path::new("/tmp/bundle/rootfs-overlay")),
            vec![
                "-log-format",
                "json",
                "-root",
                "/tmp/state",
                "-net-disconnect-ok=true",
                "-save-restore-netstack=true",
                "--overlay2=root:dir=/tmp/bundle/rootfs-overlay",
                "start",
                "web",
            ]
        );
    }

    #[test]
    fn checkpoint_args_target_image_path_and_overlay() {
        let runsc = Runsc::new("/tmp/state");
        assert_eq!(
            runsc.checkpoint_args(
                "pause",
                Path::new("/tmp/ckpt"),
                Path::new("/tmp/bundle/rootfs-overlay"),
                false,
            ),
            vec![
                "-log-format",
                "json",
                "-root",
                "/tmp/state",
                "--overlay2=root:dir=/tmp/bundle/rootfs-overlay",
                "checkpoint",
                "-image-path",
                "/tmp/ckpt",
                "pause",
            ]
        );
    }

    #[test]
    fn checkpoint_args_leave_running_adds_flag_before_image_path() {
        let runsc = Runsc::new("/tmp/state");
        assert_eq!(
            runsc.checkpoint_args(
                "pause",
                Path::new("/tmp/ckpt"),
                Path::new("/tmp/bundle/rootfs-overlay"),
                true,
            ),
            vec![
                "-log-format",
                "json",
                "-root",
                "/tmp/state",
                "--overlay2=root:dir=/tmp/bundle/rootfs-overlay",
                "checkpoint",
                "-leave-running",
                "-image-path",
                "/tmp/ckpt",
                "pause",
            ]
        );
    }

    #[test]
    fn restore_args_detach_with_bundle_image_and_pidfile() {
        let runsc = Runsc::new("/tmp/state");
        assert_eq!(
            runsc.restore_args(
                "web",
                Path::new("/tmp/bundle"),
                Path::new("/tmp/ckpt"),
                Path::new("/tmp/pid"),
                Path::new("/tmp/bundle/rootfs-overlay"),
                RestoreSpecValidation::Enforce,
            ),
            vec![
                "-log-format",
                "json",
                "-root",
                "/tmp/state",
                "-save-restore-netstack=true",
                "--overlay2=root:dir=/tmp/bundle/rootfs-overlay",
                "-restore-spec-validation",
                "enforce",
                "restore",
                "-bundle",
                "/tmp/bundle",
                "-image-path",
                "/tmp/ckpt",
                "-pid-file",
                "/tmp/pid",
                "-detach",
                "web",
            ]
        );
    }

    #[test]
    fn restore_args_can_warn_on_spec_validation() {
        let runsc = Runsc::new("/tmp/state");
        assert!(
            runsc
                .restore_args(
                    "web",
                    Path::new("/tmp/bundle"),
                    Path::new("/tmp/ckpt"),
                    Path::new("/tmp/pid"),
                    Path::new("/tmp/bundle/rootfs-overlay"),
                    RestoreSpecValidation::Warning,
                )
                .windows(2)
                .any(|args| args == ["-restore-spec-validation", "warning"])
        );
    }

    #[test]
    fn checkpoint_targets_leave_all_containers_running_for_snapshots() {
        assert_eq!(
            checkpoint_targets(&["pause".to_string(), "main".to_string()]),
            vec![("pause".to_string(), true), ("main".to_string(), true)]
        );
    }

    #[test]
    fn checkpoint_suspend_targets_stop_only_last_container() {
        assert_eq!(
            checkpoint_suspend_targets(&["pause".to_string(), "main".to_string()]),
            vec![("pause".to_string(), true), ("main".to_string(), false)]
        );
    }

    #[test]
    fn checkpoint_targets_include_pause_when_it_is_only_container() {
        assert_eq!(
            checkpoint_targets(&["pause".to_string()]),
            vec![("pause".to_string(), true)]
        );
    }

    #[tokio::test]
    async fn checkpoint_image_complete_requires_image() {
        let temp = tempfile::tempdir().unwrap();
        let image_dir = temp.path();
        let containers = vec!["pause".to_string(), "main".to_string()];

        assert!(!checkpoint_image_complete_for_containers(image_dir, &containers).await);

        fs::write(image_dir.join(RUNSC_CHECKPOINT_IMAGE), b"checkpoint")
            .await
            .unwrap();
        assert!(checkpoint_image_complete_for_containers(image_dir, &containers).await);
    }

    #[tokio::test]
    async fn checkpoint_image_complete_requires_all_per_container_images() {
        let temp = tempfile::tempdir().unwrap();
        let image_dir = temp.path();
        let containers = vec!["pause".to_string(), "main".to_string()];

        fs::create_dir_all(image_dir.join("main")).await.unwrap();
        fs::write(
            image_dir.join("main").join(RUNSC_CHECKPOINT_IMAGE),
            b"checkpoint",
        )
        .await
        .unwrap();

        assert!(!checkpoint_image_complete_for_containers(image_dir, &containers).await);

        fs::create_dir_all(image_dir.join("pause")).await.unwrap();
        fs::write(
            image_dir.join("pause").join(RUNSC_CHECKPOINT_IMAGE),
            b"checkpoint",
        )
        .await
        .unwrap();

        assert!(checkpoint_image_complete_for_containers(image_dir, &containers).await);
    }

    #[tokio::test]
    async fn checkpoint_image_dir_prefers_container_specific_image() {
        let temp = tempfile::tempdir().unwrap();
        let image_dir = temp.path();
        let main_image_dir = image_dir.join("main");
        fs::create_dir_all(&main_image_dir).await.unwrap();
        fs::write(main_image_dir.join(RUNSC_CHECKPOINT_IMAGE), b"main")
            .await
            .unwrap();

        assert_eq!(
            checkpoint_image_dir_for_container(image_dir, "main")
                .await
                .unwrap(),
            main_image_dir
        );
    }

    #[tokio::test]
    async fn checkpoint_image_dir_falls_back_to_legacy_image() {
        let temp = tempfile::tempdir().unwrap();
        let image_dir = temp.path();
        fs::write(image_dir.join(RUNSC_CHECKPOINT_IMAGE), b"legacy")
            .await
            .unwrap();

        assert_eq!(
            checkpoint_image_dir_for_container(image_dir, "main")
                .await
                .unwrap(),
            image_dir
        );
    }

    #[tokio::test]
    async fn checkpoint_image_complete_recovers_backup_after_interrupted_publish() {
        let temp = tempfile::tempdir().unwrap();
        let image_dir = temp.path().join("checkpoint");
        let containers = vec!["pause".to_string(), "main".to_string()];
        let backup = backup_checkpoint_dir(&image_dir);
        fs::create_dir_all(&backup).await.unwrap();
        fs::write(backup.join(RUNSC_CHECKPOINT_IMAGE), b"old")
            .await
            .unwrap();

        assert!(checkpoint_image_complete_for_containers(&image_dir, &containers).await);
        assert_eq!(
            fs::read(image_dir.join(RUNSC_CHECKPOINT_IMAGE))
                .await
                .unwrap(),
            b"old"
        );
        assert!(!backup.exists());
    }

    #[tokio::test]
    async fn publish_checkpoint_dir_replaces_final_from_temp() {
        let temp = tempfile::tempdir().unwrap();
        let image_dir = temp.path().join("checkpoint");
        fs::create_dir_all(&image_dir).await.unwrap();
        fs::write(image_dir.join(RUNSC_CHECKPOINT_IMAGE), b"old")
            .await
            .unwrap();

        let tmp_image_dir = temp_path(&image_dir);
        fs::create_dir_all(&tmp_image_dir).await.unwrap();
        fs::write(tmp_image_dir.join(RUNSC_CHECKPOINT_IMAGE), b"new")
            .await
            .unwrap();

        publish_checkpoint_dir(&tmp_image_dir, &image_dir)
            .await
            .unwrap();

        assert_eq!(
            fs::read(image_dir.join(RUNSC_CHECKPOINT_IMAGE))
                .await
                .unwrap(),
            b"new"
        );
        assert!(!tmp_image_dir.exists());
        assert!(!backup_checkpoint_dir(&image_dir).exists());
    }

    #[test]
    fn delete_args_force_container() {
        let runsc = Runsc::new("/tmp/state");
        assert_eq!(
            runsc.delete_args("web", Some(Path::new("/tmp/runsc-delete.log"))),
            vec![
                "-log-format",
                "json",
                "-log",
                "/tmp/runsc-delete.log",
                "-root",
                "/tmp/state",
                "delete",
                "-force",
                "web",
            ]
        );
    }

    #[test]
    fn exec_args_place_flags_before_separator_and_argv() {
        let runsc = Runsc::new("/tmp/state");
        let args = runsc.exec_args(
            "web",
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "echo hi".to_string(),
            ],
            &["FOO=bar".to_string()],
            Some("/srv"),
            Some(Path::new("/tmp/runsc-exec.log")),
        );
        assert_eq!(
            args,
            vec![
                "-log-format",
                "json",
                "-log",
                "/tmp/runsc-exec.log",
                "-root",
                "/tmp/state",
                "exec",
                "-cwd",
                "/srv",
                "-env",
                "FOO=bar",
                "--",
                "web",
                "/bin/sh",
                "-c",
                "echo hi",
            ]
        );
    }

    #[test]
    fn exec_args_omit_optional_flags() {
        let runsc = Runsc::new("/tmp/state");
        let args = runsc.exec_args(
            "web",
            &["ls".to_string()],
            &[],
            None,
            Some(Path::new("/tmp/runsc-exec.log")),
        );
        assert!(!args.iter().any(|arg| arg == "--alsologtostderr"));
        assert_eq!(args.iter().filter(|a| *a == "-cwd").count(), 0);
        assert_eq!(args.iter().filter(|a| *a == "-env").count(), 0);
        assert_eq!(&args[args.len() - 3..], &["--", "web", "ls"]);
        assert!(
            args.windows(2)
                .any(|pair| pair == ["-log", "/tmp/runsc-exec.log"])
        );
    }

    #[test]
    fn parse_runsc_status_reads_status_field() {
        assert_eq!(
            parse_runsc_status(r#"{"id":"web","status":"running","pid":42}"#).as_deref(),
            Some("running")
        );
        assert_eq!(parse_runsc_status("not json"), None);
        assert_eq!(parse_runsc_status(r#"{"id":"web"}"#), None);
    }
}
