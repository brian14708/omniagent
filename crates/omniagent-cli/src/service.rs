//! Manage omniagent as a user-level background service: a systemd `--user` unit
//! on Linux and a launchd `LaunchAgent` on macOS. The daemon runs in the
//! foreground and shuts down cleanly on SIGTERM (see [`crate::daemon`]), so the
//! service is a plain `Type=simple` / `KeepAlive` agent that invokes
//! `<bin> daemon`.
//!
//! Credentials are loaded by the daemon from `config.json`, so no token is ever
//! written into the unit file.

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::path::Path;
use std::path::PathBuf;

use anyhow::Result;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use anyhow::bail;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use anyhow::{Context, bail};

#[cfg(target_os = "linux")]
const SERVICE_NAME: &str = "omniagent.service";
#[cfg(target_os = "macos")]
const SERVICE_LABEL: &str = "dev.omniagent.daemon";

/// Path of the service unit this platform would manage, for display in the
/// `uninstall` preview.
#[must_use]
#[cfg(target_os = "linux")]
pub fn unit_path() -> PathBuf {
    systemd_user_dir().join(SERVICE_NAME)
}

/// Path of the service plist this platform would manage, for display in the
/// `uninstall` preview.
#[must_use]
#[cfg(target_os = "macos")]
pub fn unit_path() -> Option<PathBuf> {
    launch_agents_dir().map(|d| d.join(format!("{SERVICE_LABEL}.plist")))
}

// --- Shared helpers (only compiled where a service backend exists) ------------

/// Absolute path of the running binary, resolved through symlinks so the unit
/// points at a stable location even after the launching shell's `PATH` changes.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn current_bin() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("cannot determine the omniagent binary path")?;
    Ok(std::fs::canonicalize(&exe).unwrap_or(exe))
}

/// Runs a command and fails with its stderr if it exits non-zero.
#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn run(cmd: &str, args: &[&str]) -> Result<()> {
    let output = tokio::process::Command::new(cmd)
        .args(args)
        .output()
        .await
        .with_context(|| format!("failed to run `{cmd}` (is it installed?)"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("`{cmd} {}` failed: {}", args.join(" "), stderr.trim());
    }
    Ok(())
}

/// Runs a command best-effort, ignoring any failure (used for teardown steps
/// that are fine to no-op when nothing is loaded).
#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn run_ok(cmd: &str, args: &[&str]) {
    let _ = tokio::process::Command::new(cmd).args(args).output().await;
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn write_unit(path: &Path, body: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn remove_file_if_present(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

// --- Linux (systemd --user) ---------------------------------------------------

#[cfg(target_os = "linux")]
fn systemd_user_dir() -> PathBuf {
    crate::config::omniagent_config_dir()
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
        .join("systemd")
        .join("user")
}

#[cfg(target_os = "linux")]
pub async fn install(full_access: bool) -> Result<()> {
    let bin = current_bin()?;
    let exec = if full_access {
        format!("{} daemon --full-access", bin.display())
    } else {
        format!("{} daemon", bin.display())
    };
    let unit = format!(
        "[Unit]\n\
         Description=OmniAgent daemon\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={exec}\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    );
    let path = systemd_user_dir().join(SERVICE_NAME);
    write_unit(&path, &unit)?;
    println!("omniagent: wrote {}", path.display());

    run("systemctl", &["--user", "daemon-reload"]).await?;
    run("systemctl", &["--user", "enable", "--now", SERVICE_NAME]).await?;
    println!("omniagent: service installed and started (systemctl --user status omniagent)");
    Ok(())
}

#[cfg(target_os = "linux")]
pub async fn uninstall() -> Result<()> {
    run_ok("systemctl", &["--user", "disable", "--now", SERVICE_NAME]).await;
    let path = systemd_user_dir().join(SERVICE_NAME);
    let existed = path.exists();
    remove_file_if_present(&path)?;
    run_ok("systemctl", &["--user", "daemon-reload"]).await;
    if existed {
        println!("omniagent: removed service {}", path.display());
    }
    Ok(())
}

// --- macOS (launchd LaunchAgent) ----------------------------------------------

#[cfg(target_os = "macos")]
fn launch_agents_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(|home| PathBuf::from(home).join("Library").join("LaunchAgents"))
}

#[cfg(target_os = "macos")]
pub async fn install(full_access: bool) -> Result<()> {
    let bin = current_bin()?;
    let data_dir = crate::session::omniagent_data_dir();
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("failed to create {}", data_dir.display()))?;
    let out_log = data_dir.join("daemon.log");
    let err_log = data_dir.join("daemon.err.log");

    let mut program_args = format!(
        "    <string>{}</string>\n    <string>daemon</string>\n",
        bin.display()
    );
    if full_access {
        program_args.push_str("    <string>--full-access</string>\n");
    }

    let plist = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
         \x20 <key>Label</key>\n\
         \x20 <string>{SERVICE_LABEL}</string>\n\
         \x20 <key>ProgramArguments</key>\n\
         \x20 <array>\n{program_args}  </array>\n\
         \x20 <key>RunAtLoad</key>\n\
         \x20 <true/>\n\
         \x20 <key>KeepAlive</key>\n\
         \x20 <true/>\n\
         \x20 <key>StandardOutPath</key>\n\
         \x20 <string>{out}</string>\n\
         \x20 <key>StandardErrorPath</key>\n\
         \x20 <string>{err}</string>\n\
         </dict>\n\
         </plist>\n",
        out = out_log.display(),
        err = err_log.display(),
    );

    let dir = launch_agents_dir().context("cannot determine $HOME for LaunchAgents directory")?;
    let path = dir.join(format!("{SERVICE_LABEL}.plist"));
    write_unit(&path, &plist)?;
    println!("omniagent: wrote {}", path.display());

    let uid = rustix::process::getuid().as_raw();
    let domain = format!("gui/{uid}");
    let path_str = path.to_string_lossy().into_owned();

    // Clear any stale registration, then load. `bootstrap` is the modern entry
    // point; fall back to `load -w` on older macOS that lacks it.
    run_ok("launchctl", &["bootout", &domain, &path_str]).await;
    if run("launchctl", &["bootstrap", &domain, &path_str])
        .await
        .is_err()
    {
        run("launchctl", &["load", "-w", &path_str]).await?;
    }
    println!("omniagent: service installed and started (launchctl print {domain}/{SERVICE_LABEL})");
    Ok(())
}

#[cfg(target_os = "macos")]
pub async fn uninstall() -> Result<()> {
    let dir = launch_agents_dir().context("cannot determine $HOME for LaunchAgents directory")?;
    let path = dir.join(format!("{SERVICE_LABEL}.plist"));
    let uid = rustix::process::getuid().as_raw();
    let domain = format!("gui/{uid}");
    let target = format!("{domain}/{SERVICE_LABEL}");
    let path_str = path.to_string_lossy().into_owned();

    run_ok("launchctl", &["bootout", &target]).await;
    run_ok("launchctl", &["unload", "-w", &path_str]).await;
    let existed = path.exists();
    remove_file_if_present(&path)?;
    if existed {
        println!("omniagent: removed service {}", path.display());
    }
    Ok(())
}

// --- Unsupported platforms ----------------------------------------------------

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub async fn install(_full_access: bool) -> Result<()> {
    bail!("service management is only supported on Linux (systemd) and macOS (launchd)");
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub async fn uninstall() -> Result<()> {
    bail!("service management is only supported on Linux (systemd) and macOS (launchd)");
}
