//! `omniagent` — a daemon that supervises coding agents (codex, claude, gemini,
//! …) while intercepting and recording their LLM API traffic, driven over a
//! local control socket and a resilient control-plane connection.
//!
//! Each agent runs in a PTY with its `*_BASE_URL` env vars pointed at a local
//! recording proxy. The CLI is daemon-centric: run `omniagent setup` once, then
//! `omniagent daemon`; use `stop`/`sessions` to drive it and `workspaces` to
//! bound where it may spawn agents.

mod agent;
mod agent_log;
mod atif;
mod cast;
mod client;
mod codex;
mod config;
mod control;
mod daemon;
mod executor;
mod files;
mod git;
mod protocol;
mod proxy;
mod record;
mod review;
mod service;
mod session;
mod sse;
mod upload;
mod worker;
mod workspace;

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use time::OffsetDateTime;
use tokio::net::TcpListener;

use crate::client::{ClientConfig, WorkspacePolicy};
use crate::config::{ConfigStore, omniagent_config_dir};
use crate::control::{ControlRequest, ControlResponse};
use crate::proxy::ProxyState;
use crate::record::TraceStore;
use crate::review::ReviewStore;
use crate::session::omniagent_data_dir;

/// Version string stamped by `build.rs`: the crate version plus the build's git
/// short SHA and commit date, so `--version` identifies the exact build.
const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("OMNIAGENT_BUILD_SHA"),
    " ",
    env!("OMNIAGENT_BUILD_DATE"),
    ")"
);

/// Supervise and record coding agents.
#[derive(Debug, Parser)]
#[command(name = "omniagent", version = VERSION, about, long_about = None)]
struct Cli {
    /// Address to bind servers to.
    #[arg(
        long,
        global = true,
        env = "OMNIAGENT_BIND",
        default_value = "127.0.0.1"
    )]
    bind: IpAddr,

    /// File to append recorded LLM spans to as JSONL. Defaults to a fresh
    /// per-session file under `$XDG_DATA_HOME/omniagent/traces/` (or
    /// `~/.local/share/omniagent/traces/`).
    #[arg(long, global = true, env = "OMNIAGENT_TRACE_FILE")]
    trace_file: Option<PathBuf>,

    /// Disable persisting recorded spans to disk (it is enabled by default).
    #[arg(
        long,
        global = true,
        env = "OMNIAGENT_NO_TRACE_FILE",
        default_value_t = false
    )]
    no_trace_file: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Interactive first-run setup: save credentials and allow workspaces.
    Setup(SetupArgs),
    /// Store `OmniAgent` control-plane credentials the daemon connects with.
    Login(LoginArgs),
    /// Run the persistent daemon: host many sessions over one resilient
    /// connection and accept control commands on a local socket.
    Daemon(DaemonArgs),
    /// Stop a session hosted by the running daemon.
    Stop(StopArgs),
    /// Manage locally remembered central sessions.
    Sessions(SessionsArgs),
    /// Manage the allowed workspaces the daemon may spawn agents under.
    Workspaces(WorkspacesArgs),
    /// Install or remove omniagent as a background service (systemd/launchd).
    Service(ServiceArgs),
    /// Remove the omniagent binary and (by default) its config and data.
    Uninstall(UninstallArgs),
}

#[derive(Debug, Args)]
struct DaemonArgs {
    /// `OmniAgent` control-plane URL. Defaults to credentials saved by `omniagent login`.
    #[arg(long, env = "OMNIAGENT_SERVER_URL")]
    server_url: Option<String>,

    /// Long-lived CLI API token. Defaults to credentials saved by `omniagent login`.
    #[arg(long, env = "OMNIAGENT_API_TOKEN", hide = true, hide_env_values = true)]
    token: Option<String>,

    /// Allow spawning agents in any directory, bypassing the allowed-workspaces
    /// allowlist. Use only when the control plane is fully trusted.
    #[arg(long)]
    full_access: bool,
}

#[derive(Debug, Args)]
struct StopArgs {
    /// Server session id to stop.
    session_id: String,
}

#[derive(Debug, Args)]
struct LoginArgs {
    /// `OmniAgent` control-plane URL, e.g. <http://127.0.0.1:4000>.
    #[arg(long, env = "OMNIAGENT_SERVER_URL")]
    server_url: String,

    /// Long-lived CLI API token.
    #[arg(long, env = "OMNIAGENT_API_TOKEN", hide = true, hide_env_values = true)]
    token: String,
}

#[derive(Debug, Args)]
struct SetupArgs {
    /// Control-plane URL (skips the prompt).
    #[arg(long, env = "OMNIAGENT_SERVER_URL")]
    server_url: Option<String>,

    /// Long-lived CLI API token (skips the prompt).
    #[arg(long, env = "OMNIAGENT_API_TOKEN", hide = true, hide_env_values = true)]
    token: Option<String>,

    /// Allow a workspace directory the daemon may spawn agents under (repeatable;
    /// skips the prompt).
    #[arg(long = "workspace")]
    workspaces: Vec<PathBuf>,
}

#[derive(Debug, Args)]
struct SessionsArgs {
    #[command(subcommand)]
    command: SessionsCommand,
}

#[derive(Debug, Subcommand)]
enum SessionsCommand {
    /// List sessions remembered by this client.
    List,
}

#[derive(Debug, Args)]
struct WorkspacesArgs {
    #[command(subcommand)]
    command: WorkspacesCommand,
}

#[derive(Debug, Subcommand)]
enum WorkspacesCommand {
    /// Allow the daemon to spawn agents under this directory (and subdirectories).
    Add { path: PathBuf },
    /// Remove a directory from the allowed-workspaces allowlist.
    Remove { path: PathBuf },
    /// List the allowed workspaces.
    List,
}

#[derive(Debug, Args)]
struct ServiceArgs {
    #[command(subcommand)]
    command: ServiceCommand,
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    /// Install and start omniagent as a user background service.
    Install {
        /// Run the daemon with --full-access (bypass the workspace allowlist).
        #[arg(long)]
        full_access: bool,
    },
    /// Stop and remove the omniagent background service.
    Uninstall,
}

#[derive(Debug, Args)]
struct UninstallArgs {
    /// Keep the config (including the saved API token) and trace/session data;
    /// remove only the binary.
    #[arg(long)]
    keep_data: bool,

    /// Do not prompt for confirmation.
    #[arg(long, short = 'y')]
    yes: bool,
}

#[tokio::main]
async fn main() {
    init_tracing();
    if let Err(err) = run(Cli::parse()).await {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter =
        EnvFilter::try_from_env("OMNIAGENT_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

async fn run(cli: Cli) -> Result<()> {
    let trace_path = resolve_trace_path(cli.trace_file, cli.no_trace_file);
    match cli.command {
        Command::Setup(args) => setup(args).await,
        Command::Login(args) => login(args),
        Command::Daemon(args) => run_daemon_cmd(cli.bind, args, trace_path).await,
        Command::Stop(args) => stop_via_daemon(args).await,
        Command::Sessions(args) => sessions(&args).await,
        Command::Workspaces(args) => workspaces(&args).await,
        Command::Service(args) => service(&args).await,
        Command::Uninstall(args) => uninstall(args).await,
    }
}

fn login(args: LoginArgs) -> Result<()> {
    ConfigStore::default().set_credentials(args.server_url.clone(), args.token)?;
    println!("omniagent: saved credentials for {}", args.server_url);
    Ok(())
}

/// `omniagent setup`: interactive first-run wizard. Saves control-plane
/// credentials and allows one or more workspaces. Prompts on a TTY; otherwise
/// requires `--server-url`/`--token` flags.
async fn setup(args: SetupArgs) -> Result<()> {
    use std::io::{IsTerminal, Write};

    let store = ConfigStore::default();
    let interactive = std::io::stdin().is_terminal();
    let saved = store.credentials()?;

    let server_url = match args.server_url {
        Some(url) => url,
        None if interactive => {
            let default = saved.as_ref().map_or_else(
                || "http://127.0.0.1:4000".to_string(),
                |(url, _)| url.clone(),
            );
            print!("Control-plane URL [{default}]: ");
            std::io::stdout().flush()?;
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            let trimmed = input.trim();
            if trimmed.is_empty() {
                default
            } else {
                trimmed.to_string()
            }
        }
        None => bail!(
            "missing OmniAgent control-plane URL; pass --server-url (no terminal for prompts)"
        ),
    };

    let token = match args.token {
        Some(token) => token,
        None if interactive => {
            let hint = if saved.is_some() {
                " [keep current]"
            } else {
                ""
            };
            println!("Note: the API token is read visibly (it is stored 0600 in the config).");
            print!("API token{hint}: ");
            std::io::stdout().flush()?;
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            let trimmed = input.trim();
            if trimmed.is_empty() {
                saved
                    .as_ref()
                    .map(|(_, token)| token.clone())
                    .context("an API token is required")?
            } else {
                trimmed.to_string()
            }
        }
        None => bail!("missing OmniAgent API token; pass --token (no terminal for prompts)"),
    };

    store.set_credentials(server_url.clone(), token)?;
    println!("omniagent: saved credentials for {server_url}");

    let mut workspaces = args.workspaces;
    if workspaces.is_empty() && interactive {
        workspaces = prompt_workspaces()?;
    }

    for ws in &workspaces {
        match store.add_workspace(ws) {
            Ok(canonical) => println!("omniagent: allowed workspace {}", canonical.display()),
            Err(err) => eprintln!("omniagent: skipped {}: {err:#}", ws.display()),
        }
    }

    if store.list_workspaces()?.is_empty() {
        println!(
            "omniagent: no workspaces allowed yet — add one with `omniagent workspaces add <dir>`"
        );
    }
    println!("omniagent: setup complete — start the daemon with `omniagent daemon`");
    if interactive {
        offer_service_install().await;
    }
    Ok(())
}

/// Interactively collects workspace directories for `setup`, defaulting the
/// first entry to the current directory and finishing on a blank line or EOF.
fn prompt_workspaces() -> Result<Vec<PathBuf>> {
    use std::io::Write;

    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    println!("Allow workspaces the daemon may run agents under (blank line to finish):");
    let mut workspaces = Vec::new();
    let mut first = true;
    loop {
        if first {
            print!("Workspace directory [{}]: ", cwd.display());
        } else {
            print!("Workspace directory (blank to finish): ");
        }
        std::io::stdout().flush()?;
        let mut input = String::new();
        if std::io::stdin().read_line(&mut input)? == 0 {
            break;
        }
        let trimmed = input.trim();
        let path = if trimmed.is_empty() {
            if first {
                cwd.clone()
            } else {
                break;
            }
        } else {
            PathBuf::from(trimmed)
        };
        workspaces.push(path);
        first = false;
    }
    Ok(workspaces)
}

async fn sessions(args: &SessionsArgs) -> Result<()> {
    match args.command {
        SessionsCommand::List => match control::send_request(&ControlRequest::List).await {
            Ok(ControlResponse::Sessions(sessions)) => {
                for session in sessions {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        session.session_id,
                        session.name.unwrap_or_default(),
                        session.cwd,
                        session.proxy_url,
                        session.argv.join(" ")
                    );
                }
            }
            _ => println!("omniagent: no running daemon (start one with `omniagent daemon`)"),
        },
    }
    Ok(())
}

/// `omniagent workspaces`: manage the daemon's allowed-workspaces allowlist.
async fn workspaces(args: &WorkspacesArgs) -> Result<()> {
    let store = ConfigStore::default();
    match &args.command {
        WorkspacesCommand::Add { path } => {
            let canonical = store.add_workspace(path)?;
            println!("omniagent: allowed workspace {}", canonical.display());
            notify_daemon_workspaces().await;
        }
        WorkspacesCommand::Remove { path } => {
            if store.remove_workspace(path)? {
                println!("omniagent: removed workspace {}", path.display());
                notify_daemon_workspaces().await;
            } else {
                println!("omniagent: no matching workspace {}", path.display());
            }
        }
        WorkspacesCommand::List => {
            for workspace in store.list_workspaces()? {
                println!("{workspace}");
            }
        }
    }
    Ok(())
}

/// `omniagent service`: install or remove omniagent as a user-level background
/// service (systemd `--user` on Linux, launchd `LaunchAgent` on macOS).
async fn service(args: &ServiceArgs) -> Result<()> {
    match args.command {
        ServiceCommand::Install { full_access } => service::install(full_access).await,
        ServiceCommand::Uninstall => service::uninstall().await,
    }
}

/// Prompts at the end of `setup` to install the background service. Best-effort:
/// a declined prompt or a failed install never fails `setup`.
async fn offer_service_install() {
    let yes = confirm("Install omniagent as a background service so it starts automatically?")
        .await
        .unwrap_or(false);
    if !yes {
        return;
    }
    if let Err(err) = service::install(false).await {
        eprintln!("omniagent: could not install service: {err:#}");
        eprintln!("omniagent: you can retry later with `omniagent service install`");
    }
}

/// Best-effort: tell a running daemon to re-advertise its workspace allowlist so
/// the console's pickers update without a restart. A missing daemon is not an
/// error — the new allowlist applies the next time the daemon registers.
async fn notify_daemon_workspaces() {
    match control::send_request(&ControlRequest::RefreshWorkspaces).await {
        Ok(ControlResponse::WorkspacesRefreshed) => {
            println!("omniagent: notified running daemon");
        }
        Ok(ControlResponse::Error { message }) => {
            eprintln!("omniagent: daemon could not refresh workspaces: {message}");
        }
        // No running daemon (socket unreachable) or an unexpected reply: nothing
        // to do — the allowlist takes effect when the daemon next registers.
        Ok(_) | Err(_) => {}
    }
}

/// `omniagent uninstall`: remove the binary and, unless `--keep-data`, the config
/// (which stores the long-lived API token) and the trace/session data dir.
async fn uninstall(args: UninstallArgs) -> Result<()> {
    let binary = std::env::current_exe().context("cannot determine the omniagent binary path")?;
    let config_dir = omniagent_config_dir();
    let data_dir = omniagent_data_dir();
    let socket = control::socket_path();

    println!("omniagent uninstall will remove:");
    println!("  binary: {}", binary.display());
    #[cfg(target_os = "linux")]
    println!("  service unit: {}", service::unit_path().display());
    #[cfg(target_os = "macos")]
    if let Some(unit) = service::unit_path() {
        println!("  service unit: {}", unit.display());
    }
    if args.keep_data {
        println!(
            "  keeping config and data (--keep-data); your API token stays in {}",
            config_dir.join("config.json").display()
        );
    } else {
        println!(
            "  config (includes your saved API token): {}",
            config_dir.display()
        );
        println!("  data (traces, sessions): {}", data_dir.display());
    }

    if !args.yes && !confirm("Proceed?").await? {
        println!("omniagent: uninstall cancelled");
        return Ok(());
    }

    // Remove the background service first: it supervises (and would restart) the
    // daemon, so tearing it down also stops the daemon it manages.
    if let Err(err) = service::uninstall().await {
        eprintln!("omniagent: could not remove service: {err:#}");
    }

    // Refuse while a daemon is still live (e.g. one started by hand) so we don't
    // pull files out from under it. A `Pong` reply is the reliable signal; a
    // lingering socket file is not.
    if matches!(
        control::send_request(&ControlRequest::Ping).await,
        Ok(ControlResponse::Pong)
    ) {
        bail!(
            "an omniagent daemon is still running; stop it (and its sessions) before uninstalling"
        );
    }

    if !args.keep_data {
        remove_dir_if_present(&config_dir)?;
        remove_dir_if_present(&data_dir)?;
        remove_file_if_present(&socket)?;
    }

    // Remove the binary last: on Unix the running process keeps its inode until exit.
    match std::fs::remove_file(&binary) {
        Ok(()) => println!("omniagent: removed {}", binary.display()),
        Err(err) => eprintln!(
            "omniagent: could not remove {} ({err}); remove it manually with:\n    rm {}",
            binary.display(),
            binary.display()
        ),
    }

    println!("omniagent: uninstall complete");
    Ok(())
}

/// Removes a directory tree, treating an already-absent path as success.
fn remove_dir_if_present(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {
            println!("omniagent: removed {}", path.display());
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

/// Removes a file, treating an already-absent path as success.
fn remove_file_if_present(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

/// Prompts on stdout and reads a yes/no answer from stdin, defaulting to no.
async fn confirm(prompt: &str) -> Result<bool> {
    let prompt = prompt.to_string();
    tokio::task::spawn_blocking(move || {
        use std::io::Write;
        print!("{prompt} [y/N] ");
        std::io::stdout().flush().ok();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        Ok::<bool, std::io::Error>(matches!(
            input.trim().to_ascii_lowercase().as_str(),
            "y" | "yes"
        ))
    })
    .await
    .context("failed to read confirmation")?
    .context("failed to read from stdin")
}

/// Default trace archive for this session, under the XDG data dir:
/// `$XDG_DATA_HOME/omniagent/traces/<session>.jsonl`, falling back to
/// `$HOME/.local/share/omniagent/traces/<session>.jsonl` (and the cwd if
/// neither is set). Each run gets a fresh file so sessions never share an
/// archive.
///
/// Per the XDG Base Directory spec a relative `$XDG_DATA_HOME` is ignored.
fn default_trace_path() -> PathBuf {
    omniagent_data_dir()
        .join("traces")
        .join(session_file_name())
}

/// Filesystem-safe, sortable name for this session's trace file:
/// `YYYYMMDDThhmmssZ-<8 hex>.jsonl`. The id suffix disambiguates sessions that
/// start within the same second.
fn session_file_name() -> String {
    let now = OffsetDateTime::now_utc();
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z-{}.jsonl",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        &id[..8],
    )
}

/// Resolves the effective trace-archive path: `None` disables persistence,
/// otherwise the explicit `--trace-file` or the XDG default.
fn resolve_trace_path(trace_file: Option<PathBuf>, no_trace_file: bool) -> Option<PathBuf> {
    if no_trace_file {
        None
    } else {
        Some(trace_file.unwrap_or_else(default_trace_path))
    }
}

/// Binds the recording proxy and starts serving it, returning its bound address.
async fn start_proxy(
    bind: IpAddr,
    port: u16,
    traces: Arc<TraceStore>,
    reviews: Arc<ReviewStore>,
    model_override: Option<String>,
) -> Result<SocketAddr> {
    let listener = TcpListener::bind(SocketAddr::new(bind, port))
        .await
        .with_context(|| format!("failed to bind proxy on {bind}:{port}"))?;
    let addr = listener.local_addr()?;
    let app = proxy::router(ProxyState::new(traces, reviews, model_override));
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!(error = %err, "proxy server stopped");
        }
    });
    Ok(addr)
}

/// Resolves the proxy base URL an agent should use. The proxy binds on `bind`,
/// but the agent connects over loopback, so advertise a connectable host.
fn proxy_base_url(bind: IpAddr, addr: SocketAddr) -> String {
    let host = if bind.is_unspecified() {
        IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    } else {
        bind
    };
    proxy_base_url_for_host(&host.to_string(), addr.port())
}

/// Assembles `http://host:port`, bracketing bare IPv6 literals so the URL
/// authority parses correctly (e.g. `::1` must become `[::1]`).
fn proxy_base_url_for_host(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("http://[{host}]:{port}")
    } else {
        format!("http://{host}:{port}")
    }
}

/// Resolve the control-plane URL and token from flags or the saved config.
fn resolve_credentials(
    server_url: Option<String>,
    token: Option<String>,
) -> Result<(String, String)> {
    let saved = ConfigStore::default().credentials()?;
    let server_url = server_url
        .or_else(|| saved.as_ref().map(|(url, _)| url.clone()))
        .context(
            "missing OmniAgent control-plane URL; pass --server-url or run `omniagent login`",
        )?;
    let token = token
        .or_else(|| saved.as_ref().map(|(_, token)| token.clone()))
        .context("missing OmniAgent API token; pass --token or run `omniagent login`")?;
    Ok((server_url, token))
}

/// `omniagent daemon`: run the persistent supervisor and control socket.
async fn run_daemon_cmd(bind: IpAddr, args: DaemonArgs, trace_path: Option<PathBuf>) -> Result<()> {
    let (server_url, token) = resolve_credentials(args.server_url, args.token)?;
    // Full-access can be set by the flag or persisted in the config.
    let full_access = args.full_access || ConfigStore::default().load()?.full_access;
    let policy = if full_access {
        WorkspacePolicy::FullAccess
    } else {
        WorkspacePolicy::Restricted
    };
    daemon::run_daemon(
        bind,
        ClientConfig { server_url, token },
        trace_path.is_some(),
        policy,
    )
    .await
}

/// `omniagent stop`: ask the daemon to stop a session.
async fn stop_via_daemon(args: StopArgs) -> Result<()> {
    match control::send_request(&ControlRequest::Stop {
        session_id: args.session_id.clone(),
    })
    .await?
    {
        ControlResponse::Stopped { found: true } => {
            println!("omniagent: stopped {}", args.session_id);
            Ok(())
        }
        ControlResponse::Stopped { found: false } => {
            bail!("no live session {}", args.session_id)
        }
        ControlResponse::Error { message } => bail!(message),
        _ => bail!("unexpected response from daemon"),
    }
}
