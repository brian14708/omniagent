defmodule OmniagentWeb.ConsoleLive do
  @moduledoc """
  The agent-native console: a three-pane layout — sessions sidebar (left), the
  agent terminal with its LLM trace stream (middle), and a tabbed
  Files / Reviews / Artifacts inspector (right). Handles both `/` (no selection)
  and `/sessions/:id` (a session selected) as one LiveView.
  """
  use OmniagentWeb, :live_view

  alias Omniagent.{
    Accounts,
    Artifacts,
    ClientCommands,
    Daemons,
    Events,
    Payload,
    Reviews,
    Sessions,
    Traces
  }

  @impl true
  def mount(_params, _session, socket) do
    user = Accounts.default_user()

    if connected?(socket) and user do
      Events.subscribe_user(user.id)
      Events.subscribe_daemons()
    end

    {:ok,
     socket
     |> assign(:current_user, user)
     |> assign(:sessions, list_sessions(user))
     |> assign(:daemons, Daemons.list())
     |> assign(:session, nil)
     |> assign(:subscribed_id, nil)
     |> assign(:reviews, [])
     |> assign(:artifacts, [])
     |> assign(:playing_artifact, nil)
     |> assign(:auto_approve, true)
     |> assign(:right_tab, "files")
     |> assign(:file_result, nil)
     |> assign(:dir_tree, %{})
     |> assign(:dir_expanded, MapSet.new())
     |> assign(:dir_loading, false)
     |> assign(:dir_error, nil)
     |> assign(:show_new_agent, false)
     |> assign(:sidebar_collapsed, false)
     |> assign(:right_collapsed, false)
     |> assign(:left_w, 288)
     |> assign(:right_w, 384)
     |> assign(:term_pct, 62)
     |> assign(:pending_focus, false)
     |> assign(:page_title, "Console")}
  end

  @impl true
  def handle_params(%{"id" => id}, _uri, socket) do
    user = socket.assigns.current_user

    case user && Sessions.get_user_session(user.id, id) do
      nil ->
        {:noreply, socket |> put_flash(:error, "Session not found.") |> push_patch(to: ~p"/")}

      session ->
        {:noreply, select_session(socket, session)}
    end
  end

  def handle_params(_params, _uri, socket) do
    {:noreply, deselect_session(socket)}
  end

  # ── Terminal I/O (relayed to the connected client) ──────────────────────────

  @impl true
  def handle_event("pty_input", %{"data" => data}, socket) do
    send_command(socket, "pty_input", %{"data" => data})
    {:noreply, socket}
  end

  def handle_event("resize", %{"rows" => rows, "cols" => cols}, socket) do
    send_command(socket, "pty_resize", %{"rows" => rows, "cols" => cols})
    {:noreply, socket}
  end

  # ── Codex app-server conversation (relayed to the connected client) ─────────

  def handle_event("codex_send", %{"text" => text}, socket) do
    case String.trim(text || "") do
      "" -> {:noreply, socket}
      trimmed -> {:noreply, codex_command(socket, "codex_input", %{"text" => trimmed})}
    end
  end

  def handle_event("codex_interrupt", _params, socket) do
    {:noreply, codex_command(socket, "codex_interrupt", %{})}
  end

  # ── Right pane ──────────────────────────────────────────────────────────────

  def handle_event("toggle_sidebar", _params, socket) do
    {:noreply,
     socket |> assign(:sidebar_collapsed, not socket.assigns.sidebar_collapsed) |> save_prefs()}
  end

  def handle_event("toggle_right_panel", _params, socket) do
    {:noreply,
     socket |> assign(:right_collapsed, not socket.assigns.right_collapsed) |> save_prefs()}
  end

  # Drag-resize of a panel divider. The Resize hook clamps client-side too; we
  # re-clamp here as the source of truth before persisting.
  def handle_event("resize_panel", %{"prop" => prop, "value" => value}, socket)
      when is_number(value) do
    socket =
      case prop do
        "left_w" -> assign(socket, :left_w, clamp(value, 180, 520))
        "right_w" -> assign(socket, :right_w, clamp(value, 240, 600))
        "term_pct" -> assign(socket, :term_pct, clamp(value, 20, 85))
        _ -> socket
      end

    {:noreply, save_prefs(socket)}
  end

  def handle_event("select_tab", %{"tab" => tab}, socket) do
    {:noreply, socket |> assign(:right_tab, tab) |> ensure_tab_loaded(tab) |> save_prefs()}
  end

  # Hydrates cosmetic UI prefs from the browser (sent once by the Prefs hook on
  # mount). Unknown/garbage values are ignored; does not re-persist.
  def handle_event("prefs_restore", prefs, socket) when is_map(prefs) do
    socket =
      socket
      |> assign_bool_pref(:sidebar_collapsed, prefs["sidebar_collapsed"])
      |> assign_bool_pref(:right_collapsed, prefs["right_collapsed"])
      |> assign_num_pref(:left_w, prefs["left_w"], 180, 520)
      |> assign_num_pref(:right_w, prefs["right_w"], 240, 600)
      |> assign_num_pref(:term_pct, prefs["term_pct"], 20, 85)
      |> restore_tab(prefs["right_tab"])

    {:noreply, socket}
  end

  def handle_event("play_recording", %{"id" => id}, socket) do
    artifact = Enum.find(socket.assigns.artifacts, &(&1.id == id))
    {:noreply, assign(socket, :playing_artifact, artifact)}
  end

  def handle_event("close_recording", _params, socket) do
    {:noreply, assign(socket, :playing_artifact, nil)}
  end

  # Lazily fetch a span's heavy detail (stream events + headers) the first time
  # it's opened — these are omitted from the trace list to keep session switches
  # cheap. Replies to the Traces hook's `pushEvent` callback.
  def handle_event("load_span", %{"id" => id}, socket) do
    detail =
      case socket.assigns.session && Traces.get_span(socket.assigns.session.id, id) do
        nil -> %{error: "not_found"}
        span -> Traces.span_detail(span)
      end

    {:reply, detail, socket}
  end

  # Expand/collapse a directory in the file-explorer tree. Children are fetched
  # lazily the first time a directory is expanded.
  def handle_event("toggle_dir", %{"path" => path}, socket) do
    expanded = socket.assigns.dir_expanded

    socket =
      if MapSet.member?(expanded, path) do
        assign(socket, :dir_expanded, MapSet.delete(expanded, path))
      else
        socket = assign(socket, :dir_expanded, MapSet.put(expanded, path))
        if Map.has_key?(socket.assigns.dir_tree, path), do: socket, else: list_dir(socket, path)
      end

    {:noreply, socket}
  end

  def handle_event("open_file", %{"path" => path}, socket) do
    {:noreply, open_file(socket, path)}
  end

  # Manual path entry, kept as a fallback alongside the clickable browser.
  def handle_event("request_file", %{"path" => path}, socket) do
    {:noreply, open_file(socket, path)}
  end

  def handle_event("close_file", _params, socket) do
    {:noreply, assign(socket, :file_result, nil)}
  end

  def handle_event("review_decision", %{"id" => id, "action" => action}, socket) do
    if session = socket.assigns.session do
      decide(session.id, id, action)
    end

    {:noreply, socket}
  end

  def handle_event("toggle_auto_approve", _params, socket) do
    auto_approve = not socket.assigns.auto_approve

    if auto_approve and not is_nil(socket.assigns.session) do
      for item <- socket.assigns.reviews, is_nil(item.decision) do
        decide(socket.assigns.session.id, item.external_id, "approve")
      end
    end

    {:noreply, assign(socket, :auto_approve, auto_approve)}
  end

  def handle_event("delete_session", _params, socket) do
    session = socket.assigns.session

    case Sessions.delete_session(session.user_id, session.id) do
      {:ok, _} ->
        {:noreply, socket |> put_flash(:info, "Session deleted.") |> push_patch(to: ~p"/")}

      {:error, :session_online} ->
        {:noreply, put_flash(socket, :error, "Cannot delete an online session.")}

      {:error, _} ->
        {:noreply, put_flash(socket, :error, "Could not delete session.")}
    end
  end

  # ── New-agent modal (create via daemon) ─────────────────────────────────────

  def handle_event("open_new_agent", _params, socket) do
    {:noreply, assign(socket, :daemons, Daemons.list()) |> assign(:show_new_agent, true)}
  end

  def handle_event("close_new_agent", _params, socket) do
    {:noreply, assign(socket, :show_new_agent, false)}
  end

  def handle_event("create_agent", params, socket) do
    %{"daemon_id" => daemon_id, "agent" => selection} = params
    # The dropdown folds the codex backend choice into the agent selection:
    # `codex-app-server` means agent codex driven over the native app-server.
    {agent, app_server?} = agent_selection(selection)
    # Extra args are appended to the agent's resolved launch command. Split
    # respecting shell-style quoting so `--foo "a b"` yields two argv entries.
    extra = split_command(params["custom_command"])

    cond do
      daemon_id == "" ->
        {:noreply, put_flash(socket, :error, "Pick a daemon to run the agent on.")}

      agent == nil ->
        {:noreply, put_flash(socket, :error, "Pick an agent to run.")}

      true ->
        payload =
          %{"agent" => agent, "custom_command" => extra}
          |> put_present("cwd", params["cwd"])
          |> put_present("name", params["name"])
          |> put_present("model", params["model"])
          |> put_app_server(app_server?)

        case Daemons.spawn_agent(daemon_id, payload) do
          :ok ->
            {:noreply,
             socket
             |> assign(:show_new_agent, false)
             |> assign(:pending_focus, true)
             |> put_flash(:info, "Starting #{Enum.join([agent | extra], " ")}…")}

          {:error, :offline} ->
            {:noreply, put_flash(socket, :error, "That daemon is no longer connected.")}
        end
    end
  end

  # ── Inbound PubSub ──────────────────────────────────────────────────────────

  @impl true
  def handle_info({:pty_output, %{"data" => data}}, socket) when is_binary(data) do
    {:noreply, push_event(socket, "pty_output", %{data: data})}
  end

  def handle_info({:pty_exit, payload}, socket) do
    {:noreply, push_event(socket, "pty_exit", %{code: exit_code(payload)})}
  end

  def handle_info({:trace_span, span}, socket) do
    {:noreply, push_event(socket, "trace_span", Traces.span_summary(span))}
  end

  # Codex app-server conversation events → the Codex hook, which owns the
  # conversation DOM (mirrors how the Terminal/Traces hooks consume push_event).
  def handle_info({:codex_item, payload}, socket) do
    {:noreply, push_event(socket, "codex_item", payload)}
  end

  def handle_info({:codex_delta, payload}, socket) do
    {:noreply, push_event(socket, "codex_delta", payload)}
  end

  def handle_info({:codex_turn, payload}, socket) do
    {:noreply, push_event(socket, "codex_turn", payload)}
  end

  def handle_info({:codex_token_usage, payload}, socket) do
    {:noreply, push_event(socket, "codex_token_usage", payload)}
  end

  def handle_info({:codex_error, payload}, socket) do
    {:noreply, push_event(socket, "codex_error", payload)}
  end

  def handle_info({:review_item, item}, socket) do
    if socket.assigns.auto_approve and not is_nil(socket.assigns.session) and
         is_nil(Map.get(item, :decision)) do
      decide(socket.assigns.session.id, item.external_id, "approve")
    end

    {:noreply, refresh_reviews(socket)}
  end

  def handle_info({:review_decision, _item, _decision}, socket) do
    {:noreply, refresh_reviews(socket)}
  end

  def handle_info({:artifact_added, _artifact}, socket) do
    {:noreply, refresh_artifacts(socket)}
  end

  def handle_info({:file_response, payload}, socket) do
    {:noreply, assign(socket, :file_result, payload)}
  end

  def handle_info({:dir_response, payload}, socket) do
    socket =
      if payload["ok"] do
        tree = Map.put(socket.assigns.dir_tree, payload["path"] || "", payload["entries"] || [])
        assign(socket, dir_tree: tree, dir_loading: false, dir_error: nil)
      else
        assign(socket,
          dir_loading: false,
          dir_error: payload["error"] || "could not list directory"
        )
      end

    {:noreply, socket}
  end

  def handle_info({:session_updated, session}, socket) do
    existing = socket.assigns.sessions
    new? = not Enum.any?(existing, &(&1.id == session.id))
    # Any update bumps `updated_at` to now, so the touched session sorts to the
    # front of the `updated_at desc` list — splice it in place of its old copy
    # rather than re-querying every session for the user on each broadcast.
    socket =
      assign(socket, :sessions, [session | Enum.reject(existing, &(&1.id == session.id))])

    cond do
      # A new session appeared after the user spawned an agent — jump to it.
      socket.assigns.pending_focus and new? ->
        {:noreply,
         socket |> assign(:pending_focus, false) |> push_patch(to: ~p"/sessions/#{session.id}")}

      socket.assigns.session && socket.assigns.session.id == session.id ->
        socket = assign(socket, :session, session)

        socket =
          if Sessions.codex_native?(session),
            do: push_event(socket, "codex_status", %{status: session.status}),
            else: socket

        {:noreply, socket}

      true ->
        {:noreply, socket}
    end
  end

  def handle_info({:session_deleted, id}, socket) do
    {:noreply, assign(socket, :sessions, Enum.reject(socket.assigns.sessions, &(&1.id == id)))}
  end

  def handle_info({:daemons_updated}, socket) do
    {:noreply, assign(socket, :daemons, Daemons.list())}
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  # ── Selection helpers ───────────────────────────────────────────────────────

  defp select_session(socket, session) do
    socket = unsubscribe_current(socket)
    if connected?(socket), do: Events.subscribe(session.id)

    reviews = Reviews.list_reviews(session.id)

    if connected?(socket) do
      for item <- reviews, is_nil(item.decision) do
        decide(session.id, item.external_id, "approve")
      end
    end

    socket
    |> assign(:session, session)
    |> assign(:subscribed_id, session.id)
    |> assign(:reviews, reviews)
    |> assign(:artifacts, Artifacts.list_artifacts(session.id))
    |> assign(:playing_artifact, nil)
    |> assign(:right_tab, "files")
    |> assign(:file_result, nil)
    |> assign(:dir_tree, %{})
    |> assign(:dir_expanded, MapSet.new())
    |> assign(:dir_loading, false)
    |> assign(:dir_error, nil)
    |> assign(:page_title, session.name || "Session")
    |> push_session_backlog(session)
    |> push_event("trace_init", %{spans: Traces.list_spans(session.id)})
    |> list_dir("")
  end

  # Primes the middle-pane renderer for a freshly selected session: the codex
  # conversation hook gets its persisted item/turn backlog; a PTY session gets
  # its terminal scrollback. (Ephemeral codex deltas aren't replayed — the
  # durable completed items carry the final text.)
  defp push_session_backlog(socket, session) do
    if Sessions.codex_native?(session) do
      socket
      |> push_event("codex_init", %{events: codex_backlog(session.id)})
      |> push_event("codex_status", %{status: session.status})
    else
      push_event(socket, "pty_backlog", %{data: terminal_backlog(session.id)})
    end
  end

  defp codex_backlog(session_id) do
    session_id
    |> Events.list_codex_events()
    |> Enum.map(fn %{event_type: type, payload: payload} -> %{type: type, payload: payload} end)
  end

  defp deselect_session(socket) do
    socket
    |> unsubscribe_current()
    |> assign(:session, nil)
    |> assign(:subscribed_id, nil)
    |> assign(:reviews, [])
    |> assign(:artifacts, [])
    |> assign(:playing_artifact, nil)
    |> assign(:file_result, nil)
    |> assign(:dir_tree, %{})
    |> assign(:dir_expanded, MapSet.new())
    |> assign(:dir_loading, false)
    |> assign(:dir_error, nil)
    |> assign(:page_title, "Console")
  end

  defp unsubscribe_current(socket) do
    if socket.assigns[:subscribed_id], do: Events.unsubscribe(socket.assigns.subscribed_id)
    assign(socket, :subscribed_id, nil)
  end

  # Browse into a directory (empty path = workspace root).
  defp list_dir(%{assigns: %{session: nil}} = socket, _path), do: socket

  defp list_dir(socket, path) do
    case send_command(socket, "dir_request", %{"path" => path}) do
      :ok -> assign(socket, dir_loading: true, dir_error: nil)
      _ -> assign(socket, dir_loading: false, dir_error: offline_msg())
    end
  end

  # Read a single file into the viewer.
  defp open_file(socket, path) do
    result =
      case send_command(socket, "file_request", %{"path" => path}) do
        :ok -> %{"path" => path, "loading" => true}
        _ -> %{"path" => path, "ok" => false, "error" => offline_msg()}
      end

    assign(socket, :file_result, result)
  end

  defp send_command(socket, event, payload) do
    case socket.assigns.session do
      nil -> {:error, :no_session}
      session -> ClientCommands.send_command(session.id, event, payload)
    end
  end

  # Relays a codex conversation command, flashing if the agent is offline.
  defp codex_command(socket, event, payload) do
    case send_command(socket, event, payload) do
      :ok -> socket
      _ -> put_flash(socket, :error, "agent offline — reconnect to send to codex")
    end
  end

  defp offline_msg, do: "agent offline — reconnect to browse files"

  # Lazily fetch the data a right-pane tab needs the first time it's shown.
  defp ensure_tab_loaded(socket, "files") do
    if socket.assigns.dir_tree == %{} and is_nil(socket.assigns.file_result),
      do: list_dir(socket, ""),
      else: socket
  end

  defp ensure_tab_loaded(socket, _tab), do: socket

  # ── UI preferences (persisted client-side by the Prefs hook) ─────────────────

  defp save_prefs(socket) do
    push_event(socket, "prefs_save", %{
      sidebar_collapsed: socket.assigns.sidebar_collapsed,
      right_collapsed: socket.assigns.right_collapsed,
      right_tab: socket.assigns.right_tab,
      left_w: socket.assigns.left_w,
      right_w: socket.assigns.right_w,
      term_pct: socket.assigns.term_pct
    })
  end

  defp assign_bool_pref(socket, key, value) when is_boolean(value), do: assign(socket, key, value)
  defp assign_bool_pref(socket, _key, _value), do: socket

  defp assign_num_pref(socket, key, value, min, max) when is_number(value),
    do: assign(socket, key, clamp(value, min, max))

  defp assign_num_pref(socket, _key, _value, _min, _max), do: socket

  defp clamp(value, min, max), do: value |> max(min) |> min(max)

  defp restore_tab(socket, tab) when tab in ["files", "reviews", "artifacts"] do
    socket |> assign(:right_tab, tab) |> ensure_tab_loaded(tab)
  end

  defp restore_tab(socket, _tab), do: socket

  # Path of an entry given its parent directory (empty parent = root).
  @doc false
  def join_path("", name), do: name
  def join_path(dir, name), do: dir <> "/" <> name

  # Flattens the loaded subtree into indented render rows, honouring which
  # directories are expanded. Only loaded + expanded directories contribute
  # children, so the tree fills in lazily as `dir_response`s arrive.
  @doc false
  def file_rows(tree, expanded, dir \\ "", depth \\ 0) do
    tree
    |> Map.get(dir, [])
    |> Enum.flat_map(fn entry ->
      path = join_path(dir, entry["name"])
      is_dir = !!entry["dir"]
      open? = is_dir and MapSet.member?(expanded, path)
      row = %{name: entry["name"], path: path, dir: is_dir, depth: depth, expanded: open?}
      [row | if(open?, do: file_rows(tree, expanded, path, depth + 1), else: [])]
    end)
  end

  defp refresh_reviews(socket) do
    if session = socket.assigns.session,
      do: assign(socket, :reviews, Reviews.list_reviews(session.id)),
      else: socket
  end

  defp refresh_artifacts(socket) do
    if session = socket.assigns.session,
      do: assign(socket, :artifacts, Artifacts.list_artifacts(session.id)),
      else: socket
  end

  defp list_sessions(nil), do: []
  defp list_sessions(user), do: Sessions.list_sessions(user.id)

  defp decide(session_id, review_id, action) do
    decision = %{"action" => action}
    Reviews.decide_review(session_id, review_id, decision)

    ClientCommands.send_command(session_id, "review_decision", %{
      "id" => review_id,
      "decision" => decision
    })
  end

  defp terminal_backlog(session_id) do
    session_id
    |> Events.list_pty_chunks()
    |> Enum.map_join(fn
      %{event_type: "pty_output", payload: %{"data" => data}} when is_binary(data) ->
        data

      %{event_type: "pty_exit", payload: payload} ->
        "\r\n[agent exited #{exit_code(payload)}]\r\n"

      _ ->
        ""
    end)
  end

  defp exit_code(payload), do: Payload.fetch(payload, :exit_code)

  defp put_present(map, _key, value) when value in [nil, ""], do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)

  # Maps the agent dropdown value to a canonical agent name and whether the
  # codex app-server (native) backend was requested. Unknown selections yield
  # `{nil, false}` so the handler can flash an error.
  defp agent_selection("claude"), do: {"claude", false}
  defp agent_selection("codex"), do: {"codex", false}
  defp agent_selection("codex-app-server"), do: {"codex", true}
  defp agent_selection("gemini"), do: {"gemini", false}
  defp agent_selection(_), do: {nil, false}

  # Only set the app_server flag when on; the daemon ignores it for non-codex
  # agents.
  defp put_app_server(map, true), do: Map.put(map, "app_server", true)
  defp put_app_server(map, _value), do: map

  # Shell-aware argv split (honours quotes); falls back to whitespace on any
  # malformed input (e.g. an unbalanced quote) rather than raising.
  defp split_command(nil), do: []

  defp split_command(command) do
    OptionParser.split(command)
  rescue
    _ -> String.split(command, ~r/\s+/, trim: true)
  end

  # ── Reviews pane ────────────────────────────────────────────────────────────

  @doc false
  def review_action(%{"action" => action}), do: action
  def review_action(_), do: nil

  def review_decision_label(decision) do
    case review_action(decision) do
      "approve" -> "Approved"
      "retry" -> "Retried"
      "reject" -> "Rejected"
      nil -> "Decided"
      other -> String.capitalize(other)
    end
  end

  def review_decision_class(decision) do
    case review_action(decision) do
      "approve" -> "ok"
      "retry" -> "primary"
      "reject" -> "danger"
      _ -> ""
    end
  end

  # ── Artifacts pane ──────────────────────────────────────────────────────────

  @doc false
  def artifact_label("trajectory"), do: "ATIF trajectory"
  def artifact_label("recording"), do: "Terminal recording"
  def artifact_label("raw_requests"), do: "Raw LLM requests"
  def artifact_label("session_log"), do: "Native session log"
  def artifact_label(kind), do: kind

  # First visible character of a session's name (or id), for the collapsed
  # sidebar's avatar rail.
  @doc false
  def session_initial(session) do
    (session.name || session.id || "?")
    |> String.trim()
    |> String.first()
    |> Kernel.||("?")
    |> String.upcase()
  end

  @doc false
  def format_bytes(nil), do: "—"
  def format_bytes(bytes) when bytes < 1024, do: "#{bytes} B"
  def format_bytes(bytes) when bytes < 1_048_576, do: "#{Float.round(bytes / 1024, 1)} KB"
  def format_bytes(bytes), do: "#{Float.round(bytes / 1_048_576, 1)} MB"
end
