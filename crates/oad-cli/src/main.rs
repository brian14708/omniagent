//! `oadctl` — command-line client for the oad sandbox daemon.

mod client;

use std::io::Write;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use oad_api::{CreateSandboxRequest, CreateSnapshotRequest, ExecRequest};
use oad_core::{ContainerSpec, EnvVar, SandboxRecord};

use crate::client::OadClient;

/// Command-line client for the oad sandbox daemon.
#[derive(Debug, Parser)]
#[command(name = "oadctl", version, about, long_about = None)]
struct Cli {
    /// Base URL of the oad daemon.
    #[arg(
        long,
        global = true,
        env = "OAD_URL",
        default_value = "http://127.0.0.1:8080"
    )]
    url: String,

    /// Bearer token for authentication.
    #[arg(
        long,
        global = true,
        env = "OAD_BEARER_TOKEN",
        hide = true,
        hide_env_values = true
    )]
    token: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Check daemon health (no authentication required).
    Health,
    /// Create and start a sandbox.
    Create(CreateArgs),
    /// List all sandboxes.
    #[command(alias = "ls")]
    List,
    /// Show a single sandbox by id.
    Get {
        /// Sandbox id.
        id: String,
    },
    /// Stop and delete a sandbox by id.
    #[command(alias = "rm")]
    Delete {
        /// Sandbox id.
        id: String,
    },
    /// Read recent log lines from a container.
    Logs(LogsArgs),
    /// Run a command inside a running container.
    Exec(ExecArgs),
    /// Suspend a running sandbox (checkpoint and free its memory).
    Suspend {
        /// Sandbox id.
        id: String,
    },
    /// Resume a suspended sandbox from its checkpoint.
    Resume {
        /// Sandbox id.
        id: String,
    },
    /// Manage forkable snapshots.
    #[command(subcommand)]
    Snapshot(SnapshotCommand),
}

#[derive(Debug, Subcommand)]
enum SnapshotCommand {
    /// Snapshot a running sandbox (keeps it running).
    Create {
        /// Source sandbox id.
        id: String,
        /// Snapshot name (used as the fork source). Generated if omitted.
        #[arg(long)]
        name: Option<String>,
    },
    /// List all snapshots.
    #[command(alias = "ls")]
    List,
    /// Delete a snapshot by name.
    #[command(alias = "rm")]
    Delete {
        /// Snapshot name.
        name: String,
    },
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// Optional sandbox id (the daemon generates a UUID when omitted).
    #[arg(long)]
    id: Option<String>,

    /// Fork from this snapshot instead of booting fresh containers.
    #[arg(long)]
    from_snapshot: Option<String>,

    /// Read a full `CreateSandboxRequest` as JSON from this file ("-" for stdin).
    #[arg(long, conflicts_with_all = ["image", "env"])]
    file: Option<String>,

    /// Container image to run (single-container shorthand).
    #[arg(long)]
    image: Option<String>,

    /// Container name for the shorthand form.
    #[arg(long, default_value = "main")]
    name: String,

    /// Environment variable as KEY=VALUE (repeatable).
    #[arg(long = "env", value_name = "KEY=VALUE")]
    env: Vec<String>,

    /// Command and arguments to run, e.g. `-- /bin/sh -c "..."`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    argv: Vec<String>,
}

#[derive(Debug, Args)]
struct LogsArgs {
    /// Sandbox id.
    id: String,

    /// Container to read from (defaults to the first non-pause container).
    #[arg(long)]
    container: Option<String>,

    /// Maximum number of trailing lines to return (server caps at 5000).
    #[arg(long)]
    tail: Option<usize>,
}

#[derive(Debug, Args)]
struct ExecArgs {
    /// Sandbox id.
    id: String,

    /// Container to exec in (defaults to the first non-pause container).
    #[arg(long)]
    container: Option<String>,

    /// Environment variable as KEY=VALUE (repeatable).
    #[arg(long = "env", value_name = "KEY=VALUE")]
    env: Vec<String>,

    /// Working directory for the command.
    #[arg(long)]
    cwd: Option<String>,

    /// Command and arguments to run, e.g. `-- /bin/sh -c "..."`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    argv: Vec<String>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run(Cli::parse()).await {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    // `/healthz` needs no auth; everything else requires a bearer token.
    let token = match cli.command {
        Command::Health => cli.token.unwrap_or_default(),
        _ => require_token(cli.token)?,
    };
    let client = OadClient::new(&cli.url, token)?;

    match cli.command {
        Command::Health => {
            let value = client.health().await?;
            let status = value.get("status").and_then(|s| s.as_str()).unwrap_or("?");
            println!("daemon at {} is healthy (status: {status})", cli.url);
        }
        Command::Create(args) => {
            let request = build_create_request(args)?;
            let resp = client.create(&request).await?;
            print_record(&resp.sandbox);
        }
        Command::List => {
            let resp = client.list().await?;
            print_record_table(&resp.sandboxes);
        }
        Command::Get { id } => {
            let resp = client.get(&id).await?;
            print_record(&resp.sandbox);
        }
        Command::Delete { id } => {
            let resp = client.delete(&id).await?;
            println!(
                "deleted sandbox {} (status: {:?})",
                resp.sandbox.id, resp.sandbox.status
            );
        }
        Command::Logs(args) => {
            let resp = client
                .logs(&args.id, args.container.as_deref(), args.tail)
                .await?;
            for line in &resp.lines {
                println!("{line}");
            }
        }
        Command::Exec(args) => {
            if args.argv.is_empty() {
                bail!("provide a command to run, e.g. `oadctl exec <id> -- /bin/sh -c '...'`");
            }
            let env = args
                .env
                .iter()
                .map(|entry| parse_env(entry))
                .collect::<Result<Vec<_>>>()?;
            let request = ExecRequest {
                container: args.container,
                command: args.argv,
                env,
                cwd: args.cwd,
            };
            let resp = client.exec(&args.id, &request).await?;
            // Mirror the executed process: its stdout/stderr go to ours and its
            // exit code becomes ours, so `oadctl exec` stays composable. The
            // captured streams are raw process bytes and are emitted verbatim.
            std::io::stdout().write_all(&resp.stdout)?;
            let _ = std::io::stdout().flush();
            std::io::stderr().write_all(&resp.stderr)?;
            let _ = std::io::stderr().flush();
            std::process::exit(resp.exit_code);
        }
        Command::Suspend { id } => {
            let resp = client.suspend(&id).await?;
            print_record(&resp.sandbox);
        }
        Command::Resume { id } => {
            let resp = client.resume(&id).await?;
            print_record(&resp.sandbox);
        }
        Command::Snapshot(cmd) => run_snapshot(&client, cmd).await?,
    }

    Ok(())
}

async fn run_snapshot(client: &OadClient, cmd: SnapshotCommand) -> Result<()> {
    match cmd {
        SnapshotCommand::Create { id, name } => {
            let request = CreateSnapshotRequest { name };
            let resp = client.snapshot(&id, &request).await?;
            let s = &resp.snapshot;
            println!(
                "created snapshot {} from sandbox {id} (containers: {})",
                s.name,
                s.containers.join(", ")
            );
        }
        SnapshotCommand::List => {
            let resp = client.list_snapshots().await?;
            if resp.snapshots.is_empty() {
                println!("no snapshots");
            } else {
                for s in &resp.snapshots {
                    println!(
                        "{}  [{}]  {}",
                        s.name,
                        s.containers.join(", "),
                        s.created_at
                    );
                }
            }
        }
        SnapshotCommand::Delete { name } => {
            client.delete_snapshot(&name).await?;
            println!("deleted snapshot {name}");
        }
    }
    Ok(())
}

fn require_token(token: Option<String>) -> Result<String> {
    token
        .filter(|t| !t.is_empty())
        .context("missing bearer token; pass --token or set OAD_BEARER_TOKEN")
}

/// Reads the contents of `source` into a string. Pass `"-"` to read from stdin.
fn read_file_or_stdin(source: &str) -> Result<String> {
    if source == "-" {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("failed to read request JSON from stdin")?;
        Ok(buf)
    } else {
        std::fs::read_to_string(source).with_context(|| format!("failed to read {source}"))
    }
}

/// Builds the create request either from a JSON file/stdin, a snapshot to fork
/// from, or the single-container shorthand flags.
fn build_create_request(args: CreateArgs) -> Result<CreateSandboxRequest> {
    if let Some(file) = args.file {
        let body = read_file_or_stdin(&file)?;
        let mut request: CreateSandboxRequest =
            serde_json::from_str(&body).context("failed to parse CreateSandboxRequest JSON")?;
        // An explicit --id overrides whatever the file specifies.
        if args.id.is_some() {
            request.id = args.id;
        }
        return Ok(request);
    }

    // Forking from a snapshot: containers come from the snapshot.
    if let Some(snapshot) = args.from_snapshot {
        return Ok(CreateSandboxRequest {
            id: args.id,
            containers: Vec::new(),
            from_snapshot: Some(snapshot),
        });
    }

    let Some(image) = args.image else {
        bail!(
            "provide --image (with optional `-- CMD ARGS`), --from-snapshot, or --file to create a sandbox"
        );
    };

    let env = args
        .env
        .iter()
        .map(|entry| parse_env(entry))
        .collect::<Result<Vec<_>>>()?;

    Ok(CreateSandboxRequest {
        id: args.id,
        containers: vec![ContainerSpec {
            name: args.name,
            image,
            command: args.argv,
            args: Vec::new(),
            env,
        }],
        from_snapshot: None,
    })
}

fn parse_env(entry: &str) -> Result<EnvVar> {
    let (name, value) = entry
        .split_once('=')
        .with_context(|| format!("invalid --env {entry:?}; expected KEY=VALUE"))?;
    if name.is_empty() {
        bail!("invalid --env {entry:?}; key must not be empty");
    }
    Ok(EnvVar {
        name: name.to_string(),
        value: value.to_string(),
    })
}

fn print_record(record: &SandboxRecord) {
    println!("id:         {}", record.id);
    println!("status:     {:?}", record.status);
    println!("containers: {}", record.containers.join(", "));
    println!("created:    {}", record.created_at);
    println!("updated:    {}", record.updated_at);
    if let Some(err) = &record.last_error {
        println!("last_error: {err}");
    }
}

fn print_record_table(records: &[SandboxRecord]) {
    if records.is_empty() {
        println!("no sandboxes");
        return;
    }
    let id_width = records
        .iter()
        .map(|r| r.id.as_str().len())
        .max()
        .unwrap_or(2)
        .max(2);
    println!("{:<id_width$}  {:<8}  CONTAINERS", "ID", "STATUS");
    for record in records {
        println!(
            "{:<id_width$}  {:<8}  {}",
            record.id.as_str(),
            format!("{:?}", record.status),
            record.containers.join(", "),
        );
    }
}
