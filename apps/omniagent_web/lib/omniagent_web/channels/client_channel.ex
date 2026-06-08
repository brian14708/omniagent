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

  def handle_in(event, payload, socket)
      when event in ["file_response", "diff_response", "dir_response"] do
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
