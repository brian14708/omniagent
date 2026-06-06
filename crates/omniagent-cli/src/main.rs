//! `omniagent` — supervises a coding agent (codex, claude, gemini, …) while
//! intercepting and recording its LLM API traffic.
//!
//! It spawns the agent in a PTY with its `*_BASE_URL` env vars pointed at a
//! local recording proxy, then exposes the agent through one of several modes:
//!
//! * `serve`      — web control plane: ghostty-web terminal + live LLM trace view.
//! * `proxy`      — run only the recording proxy (no agent) and print the env
//!   vars to export into an externally launched agent.

mod agent;
mod compare;
mod files;
mod oad_agent;
mod proxy;
mod record;
mod review;
mod sse;
mod web;
mod worker;

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use oad_api::{
    CreateSandboxRequest, DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS, ExecRequest,
    StartBackgroundExecRequest,
};
use oad_cli::client::OadClient;
use oad_core::{ContainerSpec, EnvVar};
use time::OffsetDateTime;
use tokio::net::TcpListener;

use crate::compare::CompareStore;
use crate::oad_agent::OadAgentHandle;
use crate::proxy::ProxyState;
use crate::record::TraceStore;
use crate::review::ReviewStore;
use crate::worker::WorkerHandle;

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
    /// Spawn an agent and serve a web control plane (terminal + trace view).
    Serve(Box<ServeArgs>),
    /// Run only the recording proxy (no agent) and print env vars to export.
    Proxy(ProxyArgs),
}

#[derive(Debug, Args)]
struct ServeArgs {
    /// Web UI port.
    #[arg(long, default_value_t = 7878)]
    port: u16,

    /// Disable the human review gate (it is enabled by default).
    #[arg(long, env = "OMNIAGENT_NO_REVIEW", default_value_t = false)]
    no_review: bool,

    /// Seconds to wait for a human response-review decision before
    /// auto-approving the response (0 waits forever).
    #[arg(long, env = "OMNIAGENT_REVIEW_TIMEOUT_SECS", default_value_t = 30)]
    review_timeout_secs: u64,

    /// Default models offered when replaying a request for side-by-side
    /// comparison (comma-separated; editable per run in the web UI).
    #[arg(long, env = "OMNIAGENT_COMPARE_MODELS", value_delimiter = ',')]
    compare_models: Vec<String>,

    /// Proxy port (0 picks a free port).
    #[arg(long, default_value_t = 0)]
    proxy_port: u16,

    /// Working directory for the agent. In `oad` mode, this is the in-container
    /// working directory.
    #[arg(long)]
    cwd: Option<String>,

    #[command(subcommand)]
    target: ServeTarget,
}

#[derive(Debug, Subcommand)]
enum ServeTarget {
    /// Create an oad sandbox and run the agent inside it.
    Oad(OadServeArgs),
    /// Agent command and arguments, e.g. `-- claude` or `-- codex`.
    #[command(external_subcommand)]
    Agent(Vec<String>),
}

#[derive(Debug, Args)]
struct OadServeArgs {
    /// Container image that contains the coding-agent binary.
    #[arg(long)]
    image: String,

    /// Shell script to run inside the sandbox before launching the agent.
    #[arg(long)]
    init_script: Option<PathBuf>,

    /// Explicit sandbox id. The daemon generates one when omitted.
    #[arg(long)]
    sandbox_id: Option<String>,

    /// Keep the sandbox after `OmniAgent` exits.
    #[arg(long, default_value_t = false)]
    keep_sandbox: bool,

    /// Base URL of the oad daemon.
    #[arg(long, env = "OAD_URL", default_value = "http://127.0.0.1:8080")]
    oad_url: String,

    /// Bearer token for oad.
    #[arg(long, env = "OAD_BEARER_TOKEN", hide = true, hide_env_values = true)]
    oad_token: Option<String>,

    /// Agent command and arguments, e.g. `-- claude` or `-- codex`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    argv: Vec<String>,
}

#[derive(Debug, Args)]
struct ProxyArgs {
    /// Proxy port.
    #[arg(long, default_value_t = 7879)]
    port: u16,
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
        Command::Serve(args) => serve(cli.bind, *args, trace_path).await,
        Command::Proxy(args) => run_proxy_only(cli.bind, args, trace_path).await,
    }
}

/// Default trace archive for this session, under the XDG data dir:
/// `$XDG_DATA_HOME/omniagent/traces/<session>.jsonl`, falling back to
/// `$HOME/.local/share/omniagent/traces/<session>.jsonl` (and the cwd if
/// neither is set). Each run gets a fresh file so sessions never share an
/// archive.
///
/// Per the XDG Base Directory spec a relative `$XDG_DATA_HOME` is ignored.
fn default_trace_path() -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("omniagent")
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

/// Builds the span store, attaching a JSONL sink when persistence is enabled.
fn build_trace_store(trace_path: Option<&Path>) -> Result<Arc<TraceStore>> {
    let store = match trace_path {
        Some(path) => TraceStore::with_sink(path)
            .with_context(|| format!("failed to open trace file {}", path.display()))?,
        None => TraceStore::new(),
    };
    Ok(Arc::new(store))
}

/// Binds the recording proxy and starts serving it, returning its bound address.
async fn start_proxy(
    bind: IpAddr,
    port: u16,
    traces: Arc<TraceStore>,
    reviews: Arc<ReviewStore>,
    compare: Arc<CompareStore>,
) -> Result<SocketAddr> {
    let listener = TcpListener::bind(SocketAddr::new(bind, port))
        .await
        .with_context(|| format!("failed to bind proxy on {bind}:{port}"))?;
    let addr = listener.local_addr()?;
    let app = proxy::router(ProxyState::new(traces, reviews, compare));
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

fn require_argv(argv: Vec<String>) -> Result<Vec<String>> {
    if argv.is_empty() {
        bail!("provide an agent command, e.g. `omniagent serve -- claude`");
    }
    Ok(argv)
}

fn resolve_agent_cwd(cwd: Option<String>) -> Result<PathBuf> {
    cwd.map(PathBuf::from).map_or_else(
        || std::env::current_dir().context("failed to resolve current working directory"),
        Ok,
    )
}

async fn start_oad_agent(
    proxy_port: u16,
    traces: Arc<TraceStore>,
    reviews: Arc<ReviewStore>,
    compare: Arc<CompareStore>,
    cwd: Option<String>,
    args: OadServeArgs,
) -> Result<(OadAgentHandle, String, String, Vec<String>)> {
    let command = require_argv(args.argv)?;
    let token = args
        .oad_token
        .filter(|token| !token.is_empty())
        .context("missing oad bearer token; pass --oad-token or set OAD_BEARER_TOKEN")?;
    let client = OadClient::new(&args.oad_url, token)?;
    let sandbox = client
        .create(&CreateSandboxRequest {
            id: args.sandbox_id.clone(),
            containers: vec![ContainerSpec {
                name: "agent".to_string(),
                image: args.image,
                command: vec!["sleep".to_string()],
                args: vec!["infinity".to_string()],
                env: Vec::new(),
            }],
            from_snapshot: None,
            network: None,
        })
        .await
        .context("failed to create oad sandbox")?
        .sandbox;
    let sandbox_id = sandbox.id.to_string();
    let mut keep_sandbox = args.keep_sandbox;

    let result = async {
        if let Some(script) = args.init_script.as_deref() {
            run_init_script(&client, &sandbox_id, script, cwd.as_deref()).await?;
        }

        let network = client
            .network(&sandbox_id)
            .await
            .context("failed to query oad sandbox network info")?;
        let proxy_bind = network
            .host_gateway_ip
            .parse::<IpAddr>()
            .with_context(|| format!("invalid oad host gateway IP {}", network.host_gateway_ip))?;
        let proxy_addr = start_proxy(proxy_bind, proxy_port, traces, reviews, compare).await?;
        let proxy_url = proxy_base_url_for_host(&network.host_gateway_ip, proxy_addr.port());
        let env = proxy::agent_env(&proxy_url)
            .into_iter()
            .map(|(name, value)| EnvVar { name, value })
            .collect::<Vec<_>>();

        let exec = client
            .start_exec(
                &sandbox_id,
                &StartBackgroundExecRequest {
                    container: Some("agent".to_string()),
                    command: command.clone(),
                    env,
                    cwd,
                    pty: true,
                    rows: Some(DEFAULT_PTY_ROWS),
                    cols: Some(DEFAULT_PTY_COLS),
                },
            )
            .await
            .context("failed to start agent in oad sandbox")?
            .exec;

        let agent = OadAgentHandle::new(
            client.clone(),
            sandbox_id.clone(),
            exec.id,
            args.keep_sandbox,
        );
        keep_sandbox = true;
        Ok((agent, proxy_url, sandbox_id.clone(), command.clone()))
    }
    .await;

    if result.is_err() && !keep_sandbox {
        let _ = client.delete(&sandbox_id).await;
    }
    result
}

async fn run_init_script(
    client: &OadClient,
    sandbox_id: &str,
    script: &Path,
    cwd: Option<&str>,
) -> Result<()> {
    let body = tokio::fs::read_to_string(script)
        .await
        .with_context(|| format!("failed to read init script {}", script.display()))?;
    let output = client
        .exec(
            sandbox_id,
            &ExecRequest {
                container: Some("agent".to_string()),
                command: vec!["/bin/sh".to_string(), "-lc".to_string(), body],
                env: Vec::new(),
                cwd: cwd.map(str::to_string),
            },
        )
        .await
        .context("failed to run init script in oad sandbox")?;
    if output.exit_code != 0 {
        bail!(
            "oad init script exited with {}; stdout: {}; stderr: {}",
            output.exit_code,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

async fn serve(bind: IpAddr, args: ServeArgs, trace_path: Option<PathBuf>) -> Result<()> {
    let traces = build_trace_store(trace_path.as_deref())?;
    let review_timeout =
        (args.review_timeout_secs > 0).then(|| Duration::from_secs(args.review_timeout_secs));
    let reviews = Arc::new(ReviewStore::new(!args.no_review, review_timeout));
    let compare = Arc::new(CompareStore::new(args.compare_models));

    // Bind the web listener before spawning the agent. If binding fails (for
    // example, the port is already in use), returning here must not leave a PTY
    // child running without a supervisor.
    let listener = TcpListener::bind(SocketAddr::new(bind, args.port))
        .await
        .with_context(|| format!("failed to bind web UI on {bind}:{}", args.port))?;
    let web_addr = listener.local_addr()?;

    // The agent is spawned lazily once the startup screen posts `/api/launch`,
    // so review on/off and the chosen mode can be decided in the UI. The proxy
    // and stores are wired up front so the proxy URL is known for that launch.
    let agent_slot: Arc<tokio::sync::RwLock<Option<WorkerHandle>>> =
        Arc::new(tokio::sync::RwLock::new(None));

    let (launch, workspace) = match args.target {
        ServeTarget::Agent(agent_argv) => {
            let command = require_argv(agent_argv)?;
            let cwd = resolve_agent_cwd(args.cwd)?;
            let proxy_addr = start_proxy(
                bind,
                args.proxy_port,
                traces.clone(),
                reviews.clone(),
                compare.clone(),
            )
            .await?;
            let proxy_url = proxy_base_url(bind, proxy_addr);
            println!("omniagent: recording proxy on {proxy_url}");
            println!("omniagent: agent = {}", command.join(" "));
            let launch = web::LaunchConfig {
                argv: command,
                cwd: cwd.clone(),
                proxy_url,
                rows: DEFAULT_PTY_ROWS,
                cols: DEFAULT_PTY_COLS,
                mode: std::sync::Mutex::new(None),
            };
            (launch, cwd)
        }
        ServeTarget::Oad(oad_args) => {
            // Container-backed agents still launch immediately (no startup
            // screen): the sandbox + remote exec are created here.
            let cwd = resolve_agent_cwd(args.cwd.clone())?;
            let (agent, proxy_url, sandbox_id, command) = start_oad_agent(
                args.proxy_port,
                traces.clone(),
                reviews.clone(),
                compare.clone(),
                args.cwd,
                oad_args,
            )
            .await?;
            println!("omniagent: recording proxy on {proxy_url}");
            println!("omniagent: oad sandbox = {sandbox_id}");
            println!("omniagent: agent = {}", command.join(" "));
            *agent_slot.write().await = Some(WorkerHandle::new(agent));
            let launch = web::LaunchConfig {
                argv: command,
                cwd: cwd.clone(),
                proxy_url,
                rows: DEFAULT_PTY_ROWS,
                cols: DEFAULT_PTY_COLS,
                mode: std::sync::Mutex::new(Some("agent".to_string())),
            };
            (launch, cwd)
        }
    };

    println!("omniagent: web UI on http://{web_addr}");
    if let Some(path) = &trace_path {
        println!("omniagent: persisting traces to {}", path.display());
    }
    if !compare.default_models().is_empty() {
        println!(
            "omniagent: comparison models = {}",
            compare.default_models().join(", ")
        );
    }

    let state = web::WebState {
        agent: agent_slot,
        launch: Arc::new(launch),
        workspace: Arc::new(workspace),
        traces,
        reviews,
        compare,
    };
    let app = web::router(state.clone());

    // Serve until the server stops or we are interrupted. The agent's lifecycle
    // is independent (it may not exist yet, or may outlive a reconnecting UI).
    let server = axum::serve(listener, app);
    let outcome = tokio::select! {
        result = server => result.context("web server failed"),
        _ = tokio::signal::ctrl_c() => {
            println!("\nomniagent: interrupted");
            Ok(())
        }
    };
    // Stop the agent so its PTY subprocess never outlives this supervisor.
    let running = state.agent.read().await.clone();
    if let Some(agent) = running {
        agent.shutdown().await;
    }
    outcome
}

async fn run_proxy_only(bind: IpAddr, args: ProxyArgs, trace_path: Option<PathBuf>) -> Result<()> {
    let traces = build_trace_store(trace_path.as_deref())?;
    let reviews = Arc::new(ReviewStore::new(false, None));
    let compare = Arc::new(CompareStore::new(Vec::new()));
    let proxy_addr = start_proxy(bind, args.port, traces.clone(), reviews, compare).await?;
    let proxy_url = proxy_base_url(bind, proxy_addr);

    println!("omniagent: recording proxy on {proxy_url}");
    if let Some(path) = &trace_path {
        println!("omniagent: persisting traces to {}", path.display());
    }
    println!("# export these into the agent you launch separately:");
    for (key, value) in proxy::agent_env(&proxy_url) {
        println!("export {key}={value}");
    }
    println!("# note: Gemini base-URL override is ignored under cached OAuth (use API-key auth)");

    // Run until interrupted.
    tokio::signal::ctrl_c().await.ok();
    println!("\nomniagent: shutting down");
    Ok(())
}
