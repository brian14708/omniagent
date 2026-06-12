defmodule OmniagentWeb.ClientChannel do
  use Phoenix.Channel
  require Logger

  alias Omniagent.{ClientCommands, Events, Reviews, Sessions, Traces}

  @impl true
  def join("client:" <> client_id, _payload, socket) do
    socket = assign(socket, :client_id, client_id)
    {:ok, %{client_id: client_id}, socket}
  end

  @impl true
  def handle_in("session_register", payload, socket) do
    payload = Map.put(payload, "client_id", socket.assigns.client_id)

    case Sessions.register_or_resume_session(socket.assigns.current_user, payload) do
      {:ok, session} ->
        ClientCommands.register(session.id, self())
        {:reply, {:ok, session_payload(session)}, assign(socket, :session_id, session.id)}

      {:error, reason} ->
        {:reply, {:error, %{reason: inspect(reason)}}, socket}
    end
  end

  def handle_in("session_resume", payload, socket) do
    handle_in("session_register", payload, socket)
  end

  def handle_in("pty_output_batch", %{"events" => events}, socket) when is_list(events) do
    with {:ok, session_id} <- session_id(socket) do
      {max_seq, combined} = Events.record_pty_outputs(session_id, events)
      acked = Sessions.update_last_sequence(session_id, max_seq)
      if combined != "", do: Events.broadcast(session_id, {:pty_output, %{"data" => combined}})
      {:reply, {:ok, %{last_client_sequence: acked}}, socket}
    else
      _ -> {:reply, {:error, %{reason: "session not registered"}}, socket}
    end
  end

  def handle_in("pty_output_batch", _payload, socket) do
    {:reply, {:error, %{reason: "pty_output_batch requires an events list"}}, socket}
  end

  def handle_in("pty_exit", payload, socket) do
    terminal_event(socket, payload, "pty_exit", :pty_exit)
  end

  def handle_in("session_close", payload, socket) do
    terminal_event(socket, payload, "session_close", :session_close)
  end

  def handle_in("trace_span", payload, socket) do
    with {:ok, session_id} <- session_id(socket),
         {:ok, _span} <- Traces.record_span(session_id, payload) do
      {:noreply, socket}
    else
      error -> {:reply, {:error, %{reason: inspect(error)}}, socket}
    end
  end

  def handle_in("review_item", payload, socket) do
    with {:ok, session_id} <- session_id(socket),
         {:ok, _item} <- Reviews.upsert_review_item(session_id, payload) do
      {:noreply, socket}
    else
      error -> {:reply, {:error, %{reason: inspect(error)}}, socket}
    end
  end

  # Structured codex app-server conversation events. Durable (sequenced, replayed
  # on reconnect like pty_output): persisted for backlog reconstruction and
  # broadcast live to the console. Acked lazily via the heartbeat high-water mark,
  # same as trace_span/review_item. Only broadcast once the row is persisted, so
  # the live view and the replayed backlog can't disagree on a persistence error.
  def handle_in(event, payload, socket)
      when event in ["codex_item", "codex_turn", "codex_token_usage", "codex_error"] do
    seq = payload["sequence"] || 0

    with {:ok, session_id} <- session_id(socket),
         {:ok, _event} <- Events.record_session_event(session_id, "client", event, seq, payload) do
      Events.broadcast(session_id, {codex_tag(event), payload})
      {:noreply, socket}
    else
      {:error, :missing_session} ->
        {:reply, {:error, %{reason: "session not registered"}}, socket}

      error ->
        {:reply, {:error, %{reason: inspect(error)}}, socket}
    end
  end

  # The high-volume codex streaming deltas are ephemeral: broadcast live only, not
  # persisted (the durable codex_item completed event carries the final text).
  def handle_in("codex_delta", payload, socket) do
    with {:ok, session_id} <- session_id(socket) do
      Events.broadcast(session_id, {:codex_delta, payload})
      {:noreply, socket}
    else
      _ -> {:reply, {:error, %{reason: "session not registered"}}, socket}
    end
  end

  def handle_in(event, payload, socket)
      when event in ["diff_response", "fs_change"] do
    with {:ok, session_id} <- session_id(socket) do
      Events.broadcast(session_id, {String.to_existing_atom(event), payload})
      {:noreply, socket}
    else
      _ -> {:reply, {:error, %{reason: "session not registered"}}, socket}
    end
  end

  def handle_in("heartbeat", payload, socket) do
    reply = %{server_time: DateTime.utc_now() |> DateTime.to_iso8601()}

    reply =
      case session_id(socket) do
        {:ok, session_id} ->
          seq = payload["sequence"] || 0
          acked = Sessions.update_last_sequence(session_id, seq)
          Events.broadcast(session_id, {:client_heartbeat, payload})
          Map.put(reply, :last_client_sequence, acked)

        _ ->
          reply
      end

    {:reply, {:ok, reply}, socket}
  end

  # Defensive catch-all: an inbound event with no matching clause above (e.g. a
  # version-skewed daemon still emitting an event this server no longer handles)
  # would otherwise crash the channel with FunctionClauseError and drop the
  # socket — taking every session multiplexed on it down. Log and ignore.
  def handle_in(event, _payload, socket) do
    Logger.warning("client channel: ignoring unhandled inbound event #{inspect(event)}")
    {:noreply, socket}
  end

  @impl true
  def handle_info({:client_command, event, payload}, socket) do
    push(socket, event, payload)
    {:noreply, socket}
  end

  @impl true
  def terminate(reason, socket) do
    if session_id = socket.assigns[:session_id] do
      # Hand the offline transition to ClientCommands, which marks the session
      # offline only after a short grace window. A transient network blip that
      # reconnects within the window cancels the reap, so the session never
      # flickers offline -> online for a momentary outage.
      ClientCommands.unregister(session_id, self())
      Logger.debug("client channel terminated for session #{session_id}: #{inspect(reason)}")
    end

    :ok
  end

  defp session_id(socket) do
    case socket.assigns[:session_id] do
      nil -> {:error, :missing_session}
      session_id -> {:ok, session_id}
    end
  end

  # Maps a codex channel event name to its PubSub broadcast tag (literal atoms so
  # they always exist for the LiveView's handle_info clauses).
  defp codex_tag("codex_item"), do: :codex_item
  defp codex_tag("codex_turn"), do: :codex_turn
  defp codex_tag("codex_token_usage"), do: :codex_token_usage
  defp codex_tag("codex_error"), do: :codex_error

  # Records a terminal lifecycle event, marks the session offline, and
  # broadcasts it. Shared by the `pty_exit` and `session_close` handlers.
  defp terminal_event(socket, payload, event_type, broadcast_tag) do
    with {:ok, session_id} <- session_id(socket) do
      seq = payload["sequence"] || 0
      Events.record_session_event(session_id, "client", event_type, seq, payload)
      Sessions.mark_offline(session_id, "exited")
      Events.broadcast(session_id, {broadcast_tag, payload})
      {:noreply, socket}
    else
      _ -> {:reply, {:error, %{reason: "session not registered"}}, socket}
    end
  end

  defp session_payload(session) do
    %{
      id: session.id,
      name: session.name,
      status: session.status,
      cwd: session.cwd,
      argv: session.argv,
      last_client_sequence: session.last_client_sequence
    }
  end
end
