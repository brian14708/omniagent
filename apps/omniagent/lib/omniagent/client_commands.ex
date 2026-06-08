defmodule Omniagent.ClientCommands do
  @moduledoc """
  Tracks connected CLI channel processes so LiveView/server code can command
  them, and owns the offline transition with a reconnect grace window.

  ## Cluster behaviour

  The registry is cluster-wide: each channel pid is joined to a `:pg` group keyed
  by `{:client, session_id}` in the shared `Omniagent.PG` scope. Because Erlang
  `send/2` is location-transparent, `send_command/3` can resolve and message a
  channel that is connected to any node in the cluster — the LiveView serving a
  user need not run on the same node as the CLI's WebSocket.

  Lifecycle (monitoring + the grace reaper below) stays **node-local**: the node
  that owns a channel pid is the one that monitors it and flips the session
  offline. When a channel terminates (network blip, reload, crash) the session is
  *not* marked offline immediately. Instead a reap timer is armed; if the client
  reconnects and re-registers within the grace window the timer is cancelled and
  the session stays continuously online. Only if the window elapses with no
  reconnect does the session flip offline. This keeps a momentary outage from
  flickering the session status (and avoids spurious `disconnected_at` writes).

  If a whole node *crashes*, its monitors and reapers die with it, so the
  offline transition cannot run there. Surviving nodes reconcile on a periodic
  sweep: any session still `online`, with no `:pg` member anywhere, and whose last
  heartbeat (`updated_at`) is older than the staleness cutoff, is marked offline.
  The cutoff (well above the ~15s client heartbeat) is what keeps the sweep from
  collapsing the grace window of a session that merely blipped and is about to
  reconnect — those have a fresh `updated_at`, so only genuinely-gone sessions are
  reaped. `:pg` membership is the source of truth and `Sessions.mark_offline/2` is
  idempotent, so several nodes sweeping at once is harmless and no leader election
  is needed.
  """

  use GenServer

  require Logger

  alias Omniagent.Sessions

  # Shared process-group scope started in Omniagent.Application.
  @pg Omniagent.PG

  # How long to wait after a disconnect before marking a session offline.
  @default_grace_ms 10_000

  # Cluster reconciliation: how often surviving nodes sweep for orphaned sessions,
  # and how stale a session's last heartbeat must be before the sweep reaps it.
  # The staleness cutoff is well above the client's ~15s heartbeat so an actively
  # reconnecting session is never reaped out from under its owning node's reaper.
  @default_sweep_ms 30_000
  @default_stale_ms 90_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def register(session_id, pid), do: GenServer.call(__MODULE__, {:register, session_id, pid})
  def unregister(session_id, pid), do: GenServer.cast(__MODULE__, {:unregister, session_id, pid})

  @doc """
  Sends `event`/`payload` to the channel owning `session_id`, wherever in the
  cluster it is connected. Returns `:ok` or `{:error, :offline}`.

  This is a plain cluster-wide `:pg` lookup, not a `GenServer.call`, so it does not
  serialize through (or depend on) the local registry process.
  """
  def send_command(session_id, event, payload \\ %{}) do
    case :pg.get_members(@pg, {:client, session_id}) do
      [] ->
        {:error, :offline}

      [pid | _] ->
        send(pid, {:client_command, event, payload})
        :ok
    end
  end

  @impl true
  def init(opts) do
    grace_ms = Keyword.get(opts, :grace_ms, @default_grace_ms)
    sweep_ms = Keyword.get(opts, :sweep_ms, @default_sweep_ms)
    stale_ms = Keyword.get(opts, :stale_ms, @default_stale_ms)

    # Periodically reconcile sessions stranded `online` by a crashed node. Off in
    # single-node test runs.
    if Keyword.get(opts, :reconcile, Application.get_env(:omniagent, :session_reconcile, true)) do
      Process.send_after(self(), :sweep, sweep_ms)
    end

    {:ok,
     %{sessions: %{}, reapers: %{}, grace_ms: grace_ms, sweep_ms: sweep_ms, stale_ms: stale_ms}}
  end

  @impl true
  def handle_call({:register, session_id, pid}, _from, state) do
    Process.monitor(pid)
    :pg.join(@pg, {:client, session_id}, pid)
    # A reconnect within the grace window: cancel the pending offline reap.
    state = cancel_reaper(state, session_id)
    {:reply, :ok, put_in(state.sessions[session_id], pid)}
  end

  @impl true
  def handle_cast({:unregister, session_id, pid}, state) do
    :pg.leave(@pg, {:client, session_id}, pid)

    state =
      if Map.get(state.sessions, session_id) == pid do
        %{state | sessions: Map.delete(state.sessions, session_id)}
        |> schedule_reaper(session_id)
      else
        state
      end

    {:noreply, state}
  end

  @impl true
  def handle_info({:reap, session_id}, state) do
    mark_offline_safe(session_id)
    {:noreply, %{state | reapers: Map.delete(state.reapers, session_id)}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # A channel process died without a clean unregister: drop it and arm the
    # same grace reap so the session still transitions offline. (`:pg` already
    # drops the dead pid from its group automatically.)
    dead = for {session_id, ^pid} <- state.sessions, do: session_id

    state =
      Enum.reduce(dead, state, fn session_id, acc ->
        %{acc | sessions: Map.delete(acc.sessions, session_id)}
        |> schedule_reaper(session_id)
      end)

    {:noreply, state}
  end

  # Periodic cluster reconciliation. Runs on every node (idempotent writes), so a
  # crashed node's orphaned sessions are cleaned up by whoever is still alive, and
  # the sweep retries indefinitely — closing any race with `:pg`'s own pruning of
  # the departed node's members.
  def handle_info(:sweep, state) do
    reconcile_orphaned_sessions(state.stale_ms)
    Process.send_after(self(), :sweep, state.sweep_ms)
    {:noreply, state}
  end

  # Mark offline every session that is still `online`, has not heartbeat within the
  # staleness window, and has no `:pg` member anywhere — i.e. whose owning channel
  # has gone without a clean offline write (typically a crashed node).
  defp reconcile_orphaned_sessions(stale_ms) do
    cutoff =
      DateTime.utc_now()
      |> DateTime.add(-stale_ms, :millisecond)
      |> DateTime.truncate(:microsecond)

    for session_id <- Sessions.list_stale_online_session_ids(cutoff),
        :pg.get_members(@pg, {:client, session_id}) == [] do
      mark_offline_safe(session_id)
    end
  end

  # Harden against a transient datastore failure: a failed offline write must not
  # crash the registry (which would drop every live session's pid).
  defp mark_offline_safe(session_id) do
    Sessions.mark_offline(session_id, "offline")
  rescue
    error -> Logger.warning("failed to mark session #{session_id} offline: #{inspect(error)}")
  end

  # Arm a single offline reap for `session_id`, replacing any existing timer.
  defp schedule_reaper(state, session_id) do
    state = cancel_reaper(state, session_id)
    ref = Process.send_after(self(), {:reap, session_id}, state.grace_ms)
    put_in(state.reapers[session_id], ref)
  end

  defp cancel_reaper(state, session_id) do
    case Map.pop(state.reapers, session_id) do
      {nil, _reapers} ->
        state

      {ref, reapers} ->
        Process.cancel_timer(ref)
        %{state | reapers: reapers}
    end
  end
end
