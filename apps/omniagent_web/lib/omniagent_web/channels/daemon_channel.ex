defmodule OmniagentWeb.DaemonChannel do
  @moduledoc """
  Control channel for a connected omniagent daemon (`daemon:<id>`).

  The daemon registers itself with its metadata; the server can then push a
  `spawn_agent` command to start a new agent session on that daemon.
  """

  use Phoenix.Channel
  require Logger

  alias Omniagent.Daemons

  @impl true
  def join("daemon:" <> daemon_id, _payload, socket) do
    {:ok, assign(socket, :daemon_id, daemon_id)}
  end

  @impl true
  def handle_in("daemon_register", metadata, socket) do
    daemon_id = socket.assigns.daemon_id
    Daemons.register(daemon_id, self(), metadata)
    {:reply, {:ok, %{daemon_id: daemon_id}}, socket}
  end

  @impl true
  def handle_info({:daemon_command, event, payload}, socket) do
    push(socket, event, payload)
    {:noreply, socket}
  end

  @impl true
  def terminate(reason, socket) do
    if daemon_id = socket.assigns[:daemon_id] do
      Daemons.unregister(daemon_id, self())
      Logger.debug("daemon channel terminated for #{daemon_id}: #{inspect(reason)}")
    end

    :ok
  end
end
