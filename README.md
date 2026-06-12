# OmniAgent

OmniAgent is split into:

- a Rust CLI/client (`omniagent`) that runs the local/OAD agent, PTY bridge, workspace file access, and LLM recording proxy;
- an OmniAgent control plane (`apps/omniagent` + `apps/omniagent_web`) that stores sessions/traces/reviews/comparisons in Postgres and serves the LiveView UI.

## Nix process-compose dev stack

The flake exposes a process-compose based development stack that starts local Postgres and the OmniAgent control plane together:

```sh
nix run .#dev
```

This uses `.devenv/postgres` for local Postgres data, creates an `omniagent` superuser with password `omniagent`, runs `mix ecto.setup`, then starts `mix phx.server`. The same tools are available in the dev shell:

```sh
nix develop
process-compose --help
```

## OmniAgent control plane

The control plane is implemented as a Phoenix umbrella app generated with `mix phx.new --umbrella` and lives under `apps/`.

For local development, copy `.env.example` to `.env`, update it if needed, and load it before running Mix commands:

```sh
set -a
source .env
set +a
```

Create/migrate/seed the database:

```sh
mix ecto.setup
```

The seed creates an admin user and API token from:

- `OMNIAGENT_ADMIN_EMAIL` (default `admin@omniagent.local`)
- `OMNIAGENT_DEV_TOKEN` (default `dev-token`)

Start the control plane:

```sh
mix phx.server
```

Open the LiveView dashboard at `http://127.0.0.1:4000`.

### Multi-node deployment

The control plane scales horizontally. Durable state (Postgres), artifact blobs
(S3/RustFS), and UI fan-out (`Phoenix.PubSub`) are already node-agnostic; the
in-memory command registries (`Omniagent.ClientCommands`, `Omniagent.Daemons`) are
made cluster-wide with OTP `:pg`, so a LiveView on one node can drive a CLI/daemon
whose WebSocket landed on another.

Nodes discover each other through **Postgres `LISTEN/NOTIFY`** (`libcluster` +
`libcluster_postgres`) — no extra infra beyond the database. The cluster strategy
reuses the same DB connection env as the Repo (`DATABASE_URL`, or the discrete
`PG*`/`POSTGRES_*` vars) and is enabled in every environment except `:test`. Set
`CLUSTER_ENABLED=false` to force single-node (empty topology).

Discovery is only how nodes _find_ each other; the transport is still distributed
Erlang, so every node must boot with a stable node name and a shared cookie, and
must be able to reach the others over EPMD (4369) + the dist port. In a release:

```sh
export RELEASE_DISTRIBUTION=name
export RELEASE_NODE=omniagent@<this-node-host-or-ip>
export RELEASE_COOKIE=<same-secret-on-every-node>
```

In locked-down networks, pin the dist port range with
`ERL_AFLAGS="-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9100"`.

Run two nodes locally (both pointed at the same Postgres; they auto-discover):

```sh
iex --sname a --cookie omni -S mix phx.server
PORT=4001 iex --sname b --cookie omni -S mix phx.server
# In either shell, `Node.list()` should list the other node.
```

If a node crashes, surviving nodes reconcile its orphaned sessions to `offline`
via a periodic sweep: a session still marked `online` with no live channel
anywhere and no client heartbeat within the staleness window is marked offline
(the crashed node can no longer run its own offline transition).

## Rust client

Install the latest nightly:

```sh
curl -fsSL https://raw.githubusercontent.com/brian14708/omniagent/main/install.sh | sh
```

Build/check from source:

```sh
cargo check -p omniagent-cli
cargo test -p omniagent-cli
```

Store server credentials:

```sh
omniagent login --server-url "$OMNIAGENT_SERVER_URL" --token "$OMNIAGENT_API_TOKEN"
```

Allow a workspace the daemon may spawn agents under:

```sh
omniagent workspaces add /path/to/your/project
```

Run the daemon (foreground; connects out to the control plane). Sessions are
started from the OmniAgent web console:

```sh
omniagent daemon
```

List locally remembered sessions, or stop one:

```sh
omniagent sessions list
omniagent stop <session-id>
```

The CLI keeps provider API keys local, starts a local recording proxy, streams terminal/trace/review/compare events to the OmniAgent control plane over an outbound WebSocket, and serves file/diff requests from the local workspace only.

### Upgrading

Re-run the installer; it pulls the latest nightly, overwrites the binary in
place, and reports the version change:

```sh
curl -fsSL https://raw.githubusercontent.com/brian14708/omniagent/main/install.sh | sh
```

Verify with `omniagent --version` (the build's git SHA and date are stamped in).

### Uninstalling

Remove the binary along with the config (which holds your saved API token) and
trace/session data:

```sh
omniagent uninstall            # prompts before deleting
omniagent uninstall --yes      # no prompt
omniagent uninstall --keep-data  # remove only the binary, keep config + data
```

Stop the daemon first — `uninstall` refuses while one is running. If automatic
removal of the binary fails (e.g. it lives in a root-owned directory), remove
these paths manually:

```sh
rm ~/.local/bin/omniagent          # or wherever INSTALL_DIR put it
rm -rf ~/.config/omniagent         # config + API token
rm -rf ~/.local/share/omniagent    # traces + sessions
```
