defmodule Omniagent.Daemons do
  @moduledoc """
  Tracks connected omniagent daemons so the web UI can ask one to spawn a new
  agent session.

  A daemon opens a `daemon:<id>` control channel and registers here with its
  metadata (hostname, cwd, available agent commands). `spawn_agent/2` relays a
  `spawn_agent` command to a chosen daemon, which starts the session locally; the
  new session then registers over its own `client:<id>` channel and shows up in
  the sessions list through the normal flow.

  ## Cluster behaviour

  Daemon channel pids are joined to a `:pg` group keyed by `{:daemon, id}` in the
  shared `Omniagent.PG` scope, so `spawn_agent/2` can reach a daemon connected to
  any node (Erlang `send/2` is location-transparent). Daemon *metadata* lives in
  each node's local GenServer state, so `list/0` fans out across the cluster with
  `:erpc` and merges every node's view. `:pg` drops a crashed node's daemons
  automatically, so `list/0` self-heals; a node-down notification just triggers
  the UI refresh the departed node can no longer broadcast.
  """

  use GenServer

  alias Omniagent.Events

  # Shared process-group scope started in Omniagent.Application.
  @pg Omniagent.PG

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def register(daemon_id, pid, metadata),
    do: GenServer.call(__MODULE__, {:register, daemon_id, pid, metadata})

  def unregister(daemon_id, pid), do: GenServer.cast(__MODULE__, {:unregister, daemon_id, pid})

  @doc "List connected daemons across the whole cluster as `%{id, metadata}` maps."
  def list do
    [node() | Node.list()]
    |> :erpc.multicall(GenServer, :call, [__MODULE__, :local_list], 5_000)
    |> Enum.flat_map(fn
      {:ok, daemons} -> daemons
      _ -> []
    end)
  end

  @doc """
  Ask `daemon_id` to spawn an agent, wherever in the cluster it is connected.
  `params` is a map with `argv` (list) and optional `cwd`/`name`. Returns `:ok` or
  `{:error, :offline}`.
  """
  def spawn_agent(daemon_id, params) do
    case :pg.get_members(@pg, {:daemon, daemon_id}) do
      [] ->
        {:error, :offline}

      [pid | _] ->
        send(pid, {:daemon_command, "spawn_agent", params})
        :ok
    end
  end

  @doc """
  Ask `daemon_id` to create a new project workspace. `params` is a map with
  `name`; the daemon creates it under its local data dir, git-inits it, adds it
  to its allowed-workspaces allowlist, and re-advertises its metadata (so the
  new workspace appears in the pickers). Returns `:ok` or `{:error, :offline}`.
  """
  def create_workspace(daemon_id, params) do
    case :pg.get_members(@pg, {:daemon, daemon_id}) do
      [] ->
        {:error, :offline}

      [pid | _] ->
        send(pid, {:daemon_command, "create_workspace", params})
        :ok
    end
  end

  @impl true
  def init(_state) do
    :net_kernel.monitor_nodes(true)
    {:ok, %{daemons: %{}}}
  end

  @impl true
  def handle_call({:register, daemon_id, pid, metadata}, _from, state) do
    Process.monitor(pid)
    :pg.join(@pg, {:daemon, daemon_id}, pid)
    state = put_in(state.daemons[daemon_id], %{pid: pid, metadata: metadata})
    Events.broadcast_daemons({:daemons_updated})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:local_list, _from, state) do
    daemons =
      Enum.map(state.daemons, fn {id, %{metadata: metadata}} ->
        %{id: id, metadata: metadata}
      end)

    {:reply, daemons, state}
  end

  @impl true
  def handle_cast({:unregister, daemon_id, pid}, state) do
    :pg.leave(@pg, {:daemon, daemon_id}, pid)
    state = drop_if_pid(state, daemon_id, pid)
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    dead = for {id, %{pid: ^pid}} <- state.daemons, do: id
    state = Enum.reduce(dead, state, fn id, acc -> drop(acc, id) end)
    {:noreply, state}
  end

  # A peer node went down: its daemons have already dropped out of `:pg` and the
  # cluster-wide `list/0`, but the departed node can no longer broadcast the
  # refresh, so trigger it here for connected UIs.
  @impl true
  def handle_info({:nodedown, _node}, state) do
    Events.broadcast_daemons({:daemons_updated})
    {:noreply, state}
  end

  def handle_info({:nodeup, _node}, state), do: {:noreply, state}

  defp drop_if_pid(state, daemon_id, pid) do
    case Map.get(state.daemons, daemon_id) do
      %{pid: ^pid} -> drop(state, daemon_id)
      _ -> state
    end
  end

  defp drop(state, daemon_id) do
    state = %{state | daemons: Map.delete(state.daemons, daemon_id)}
    Events.broadcast_daemons({:daemons_updated})
    state
  end
end
