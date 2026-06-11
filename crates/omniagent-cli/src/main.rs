//! `omniagent` — a daemon that supervises coding agents (codex, claude, gemini,
//! …) while intercepting and recording their LLM API traffic, driven over a
//! local control socket and a resilient control-plane connection.
//!
//! Each agent runs in a PTY with its `*_BASE_URL` env vars pointed at a local
//! recording proxy. The CLI is daemon-centric: run `omniagent daemon`, then
//! `run`/`stop`/`sessions` to drive it and `workspaces` to bound where it may
//! spawn agents.

mod agent;
mod agent_log;
mod agents;
mod atif;
mod cast;
mod client;
mod codex;
mod config;
mod control;
mod daemon;
mod files;
mod protocol;
mod proxy;
mod record;
mod review;
mod session;
mod sse;
mod upload;
mod worker;

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use time::OffsetDateTime;
use tokio::net::TcpListener;

use crate::client::{ClientConfig, WorkspacePolicy};
use crate::config::ConfigStore;
use crate::control::{ControlRequest, ControlResponse};
use crate::proxy::ProxyState;
use crate::record::TraceStore;
use crate::review::ReviewStore;
use crate::session::omniagent_data_dir;

/// Supervise and record coding agents.
#[derive(Debug, Parser)]
#[command(name = "omniagent", version, about, long_about = None)]
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
        Command::Login(args) => login(args),
        Command::Daemon(args) => run_daemon_cmd(cli.bind, args, trace_path).await,
        Command::Stop(args) => stop_via_daemon(args).await,
        Command::Sessions(args) => sessions(&args).await,
        Command::Workspaces(args) => workspaces(&args),
    }
}

fn login(args: LoginArgs) -> Result<()> {
    ConfigStore::default().set_credentials(args.server_url.clone(), args.token)?;
    println!("omniagent: saved credentials for {}", args.server_url);
    Ok(())
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
fn workspaces(args: &WorkspacesArgs) -> Result<()> {
    let store = ConfigStore::default();
    match &args.command {
        WorkspacesCommand::Add { path } => {
            let canonical = store.add_workspace(path)?;
            println!("omniagent: allowed workspace {}", canonical.display());
        }
        WorkspacesCommand::Remove { path } => {
            if store.remove_workspace(path)? {
                println!("omniagent: removed workspace {}", path.display());
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
) -> Result<SocketAddr> {
    let listener = TcpListener::bind(SocketAddr::new(bind, port))
        .await
        .with_context(|| format!("failed to bind proxy on {bind}:{port}"))?;
    let addr = listener.local_addr()?;
    let app = proxy::router(ProxyState::new(traces, reviews));
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
