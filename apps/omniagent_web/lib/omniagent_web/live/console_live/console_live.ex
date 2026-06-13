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
    OadInstances,
    OadWorkspaces,
    Payload,
    Reviews,
    Sessions,
    Traces
  }

  alias Omniagent.Oad

  # Codex app-server conversation events relayed verbatim to the Codex hook.
  @codex_events ~w(codex_item codex_delta codex_turn codex_token_usage codex_error)a

  # Cosmetic UI prefs persisted client-side by the Prefs hook. These specs drive
  # both save_prefs (push current values) and prefs_restore (clamp/validate
  # incoming values); `right_tab` is handled separately as it needs validation
  # against the known tabs plus a lazy data load.
  @bool_prefs ~w(sidebar_collapsed right_collapsed sessions_collapsed)a
  @num_prefs %{left_w: {180, 520}, right_w: {240, 600}, term_pct: {20, 85}}

  @impl true
  def mount(_params, _session, socket) do
    user = Accounts.default_user()

    if connected?(socket) and user do
      Events.subscribe_user(user.id)
      Events.subscribe_daemons()
      # oad instances register/expire out of band (HTTP heartbeat), so poll for
      # the live set rather than relying on a push.
      Process.send_after(self(), :refresh_oad, 30_000)
    end

    {:ok,
     socket
     |> assign(%{
       current_user: user,
       sessions: list_sessions(user),
       daemons: Daemons.list(),
       oad_instances: OadInstances.list_live(),
       oad_workspaces: OadWorkspaces.list(),
       term_size: nil,
       page_title: "Console"
     })
     # Selected-session state (also reset by select_session/deselect_session).
     |> assign(%{
       session: nil,
       subscribed_id: nil,
       reviews: [],
       artifacts: [],
       playing_artifact: nil,
       auto_approve: true,
       right_tab: "files",
       pending_focus: false
     })
     |> reset_changes()
     # Modal state: `:modal` names the open dialog (nil = none); the rest is the
     # data each dialog reads. Build progress lives here too, updated while open.
     |> assign(%{
       modal: nil,
       new_agent_daemon: nil,
       new_agent_workspace: nil,
       new_agent_mode: "in_place",
       new_workspace_daemon: nil,
       oad_agent_workspace: nil,
       oad_build_name: nil,
       oad_build_status: nil,
       oad_build_log: []
     })
     # Cosmetic UI prefs (later hydrated from the browser by the Prefs hook).
     |> assign(%{
       sessions_collapsed: true,
       sidebar_collapsed: false,
       right_collapsed: false,
       left_w: 312,
       right_w: 384,
       term_pct: 62
     })}
  end

  @impl true
  def handle_params(%{"id" => id}, _uri, socket) do
    user = socket.assigns.current_user

    case user && Sessions.get_user_session(user.id, id) do
      nil ->
        {:noreply,
         socket |> put_flash(:error, "Session not found.") |> push_patch(to: ~p"/console")}

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
    # Remember the browser terminal size so we can (a) open new oad PTYs at the
    # right width and (b) re-assert it when a session (re)connects.
    {:noreply, assign(socket, :term_size, %{rows: rows, cols: cols})}
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
      |> restore_bool_prefs(prefs)
      |> restore_num_prefs(prefs)
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

  def handle_event("open_diff", %{"path" => path}, socket) do
    {:noreply, open_diff(socket, path)}
  end

  def handle_event("close_diff", _params, socket) do
    {:noreply, assign(socket, :diff_result, nil)}
  end

  def handle_event("refresh_changes", _params, socket) do
    {:noreply, list_changes(socket)}
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
        {:noreply, socket |> put_flash(:info, "Session deleted.") |> push_patch(to: ~p"/console")}

      {:error, :session_online} ->
        {:noreply, put_flash(socket, :error, "Cannot delete an online session.")}

      {:error, _} ->
        {:noreply, put_flash(socket, :error, "Could not delete session.")}
    end
  end

  # ── New-agent modal (launched from a workspace) ─────────────────────────────

  # Collapse/expand the "Sessions" panel (inactive + unmatched sessions). Default
  # collapsed; persisted.
  def handle_event("toggle_sessions", _params, socket) do
    {:noreply,
     socket |> assign(:sessions_collapsed, not socket.assigns.sessions_collapsed) |> save_prefs()}
  end

  # Open the agent modal pre-scoped to a specific daemon + workspace.
  def handle_event("launch_workspace", %{"daemon_id" => daemon_id, "path" => path}, socket) do
    {:noreply,
     socket
     |> assign(:daemons, Daemons.list())
     |> assign(:modal, :new_agent)
     |> assign(:new_agent_daemon, daemon_id)
     |> assign(:new_agent_workspace, path)
     |> assign(:new_agent_mode, "in_place")}
  end

  # Closes whichever simple show/hide modal is open. The diff and recording
  # modals are data-driven (close_diff / close_recording clear their data).
  def handle_event("close_modal", _params, socket) do
    {:noreply, assign(socket, :modal, nil)}
  end

  # Keeps the modal's worktree sub-pickers in sync as the user changes the
  # worktree mode. Daemon + workspace are fixed (hidden inputs) once launched.
  def handle_event("new_agent_change", params, socket) do
    {:noreply,
     assign(socket, :new_agent_mode, params["worktree_mode"] || socket.assigns.new_agent_mode)}
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
          |> put_present("workspace", params["workspace"])
          |> put_present("cwd", params["cwd"])
          |> put_present("name", params["name"])
          |> put_present("model", params["model"])
          |> put_app_server(app_server?)
          |> apply_worktree(params["worktree_mode"], params)

        case Daemons.spawn_agent(daemon_id, payload) do
          :ok ->
            {:noreply,
             socket
             |> assign(:modal, nil)
             |> assign(:pending_focus, true)
             |> put_flash(:info, "Starting #{Enum.join([agent | extra], " ")}…")}

          {:error, :offline} ->
            {:noreply, put_flash(socket, :error, "That daemon is no longer connected.")}
        end
    end
  end

  # ── New-workspace modal (create a git repo via daemon) ──────────────────────

  def handle_event("open_new_workspace", params, socket) do
    daemons = Daemons.list()
    daemon_id = params["daemon_id"] || first_daemon_id(daemons)

    {:noreply,
     socket
     |> assign(:daemons, daemons)
     |> assign(:modal, :new_workspace)
     |> assign(:new_workspace_daemon, daemon_id)}
  end

  # ── oad workspaces ──────────────────────────────────────────────────────────

  def handle_event("open_new_oad_workspace", _params, socket) do
    {:noreply, assign(socket, :modal, :new_oad_workspace)}
  end

  def handle_event("create_oad_workspace", params, socket) do
    name = String.trim(params["name"] || "")
    image = String.trim(params["image"] || "")

    cond do
      socket.assigns.oad_instances == [] ->
        {:noreply, put_flash(socket, :error, "No oad instance is registered.")}

      name == "" or image == "" ->
        {:noreply, put_flash(socket, :error, "Workspace name and image are required.")}

      not Regex.match?(~r/^[A-Za-z0-9._-]+$/, name) ->
        {:noreply,
         put_flash(socket, :error, "Name may contain only letters, digits, '.', '_', or '-'.")}

      true ->
        {:noreply,
         start_build(
           socket,
           %{
             name: name,
             image: image,
             repo: blank_to_nil(params["repo"]),
             git_ref: blank_to_nil(params["git_ref"])
           },
           "building #{name}…",
           "Building oad workspace #{name}…"
         )}
    end
  end

  def handle_event("rebuild_oad_workspace", %{"name" => name}, socket) do
    case OadWorkspaces.get_by_name(name) do
      nil ->
        {:noreply, put_flash(socket, :error, "Workspace not found.")}

      ws ->
        {:noreply,
         start_build(
           socket,
           %{
             name: ws.name,
             image: ws.image,
             repo: ws.repo,
             git_ref: ws.git_ref,
             workspace_folder: ws.workspace_folder,
             oad_base_url: ws.oad_base_url
           },
           "rebuilding #{name}…",
           "Rebuilding #{name}…"
         )}
    end
  end

  # Opens the oad agent-picker modal scoped to a workspace.
  def handle_event("open_oad_agent", %{"workspace" => name}, socket) do
    {:noreply,
     socket
     |> assign(:modal, :oad_agent)
     |> assign(:oad_agent_workspace, name)}
  end

  def handle_event("start_oad_session", params, socket) do
    name = params["workspace"]
    agent = params["agent"] || "claude"

    opts =
      %{user: socket.assigns.current_user, agent: agent}
      |> put_present(:name, params["name"])
      |> put_present(:model, params["model"])
      |> put_present(:custom_command, params["custom_command"])
      |> put_term_size(socket.assigns.term_size)

    case Oad.Sessions.start_session(name, opts) do
      {:ok, _session} ->
        {:noreply,
         socket
         |> assign(:modal, nil)
         |> assign(:pending_focus, true)
         |> put_flash(:info, "Starting #{agent} on #{name}…")}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Could not start session: #{inspect(reason)}")}
    end
  end

  def handle_event("create_workspace", params, socket) do
    %{"daemon_id" => daemon_id, "name" => name} = params
    name = String.trim(name || "")

    cond do
      daemon_id in [nil, ""] ->
        {:noreply, put_flash(socket, :error, "Pick a daemon to create the workspace on.")}

      name == "" ->
        {:noreply, put_flash(socket, :error, "Enter a workspace name.")}

      true ->
        # The daemon creates the workspace asynchronously and re-advertises its
        # metadata; the resulting {:daemons_updated} refreshes the pickers.
        case Daemons.create_workspace(daemon_id, %{"name" => name}) do
          :ok ->
            {:noreply,
             socket
             |> assign(:modal, nil)
             |> put_flash(:info, "Creating workspace #{name}…")}

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
  def handle_info({type, payload}, socket) when type in @codex_events do
    {:noreply, push_event(socket, Atom.to_string(type), payload)}
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

  # Every diff response carries the full changed-files list (cheap git status),
  # so it doubles as the changes-list refresh. A non-empty path also carries that
  # file's unified diff for the viewer.
  def handle_info({:diff_response, payload}, socket) do
    socket =
      if payload["ok"] do
        diff = payload["diff"] || %{}

        socket
        |> assign(changes: diff["files"] || [], changes_loading: false, changes_error: nil)
        |> assign_open_diff(payload["path"], diff["diff"])
      else
        error = payload["error"] || "could not read changes"

        socket
        |> assign(changes_loading: false, changes_error: error)
        |> assign_diff_error(payload["path"], error)
      end

    {:noreply, socket}
  end

  # The daemon's filesystem watcher pushes the changed-files list whenever the
  # workspace changes — assign it directly, no round-trip.
  def handle_info({:fs_change, payload}, socket) do
    files = payload["files"] || []
    changed? = files != socket.assigns.changes
    socket = assign(socket, changes: files, changes_loading: false, changes_error: nil)

    # Keep an open diff fresh if its file is among the changes. Refresh in
    # place — re-request without flipping the viewer back to its loading state,
    # so a stream of edits doesn't make the open diff flicker. Only when the
    # changed-files set actually moved, so an unchanged re-push (e.g. a watcher
    # re-prime on reconnect) doesn't trigger a redundant round-trip.
    if changed? do
      case socket.assigns.diff_result do
        %{"path" => path} when is_binary(path) and path != "" ->
          if Enum.any?(files, &(&1["path"] == path)) do
            send_command(socket, "diff_request", %{"path" => path})
          end

        _ ->
          :ok
      end
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

        # Re-assert the terminal size once the session is online: an oad client
        # often registers after the browser's first resize (which was dropped as
        # offline), leaving the PTY at its default width until corrected here.
        reassert_term_size(socket, session)

        {:noreply, socket}

      true ->
        {:noreply, socket}
    end
  end

  def handle_info({:session_deleted, id}, socket) do
    {:noreply, assign(socket, :sessions, Enum.reject(socket.assigns.sessions, &(&1.id == id)))}
  end

  # A streamed build log line — append only. Kept cheap (no DB query) because
  # build steps stream their output line-by-line; status changes arrive
  # separately via {:oad_build_done, ...}.
  def handle_info({:oad_build, _name, message}, socket) do
    log = [message | socket.assigns.oad_build_log] |> Enum.take(200)
    {:noreply, assign(socket, :oad_build_log, log)}
  end

  # Terminal build status — stops the modal spinner and refreshes the workspace
  # list once (so the row shows ready/error and its actions).
  def handle_info({:oad_build_done, _name, status}, socket) do
    {:noreply,
     socket
     |> assign(:oad_build_status, status)
     |> assign(:oad_workspaces, OadWorkspaces.list())}
  end

  def handle_info(:refresh_oad, socket) do
    Process.send_after(self(), :refresh_oad, 30_000)

    {:noreply,
     socket
     |> assign(:oad_instances, OadInstances.list_live())
     |> assign(:oad_workspaces, OadWorkspaces.list())}
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
    |> reset_changes()
    |> assign(:page_title, session.name || "Session")
    |> push_session_backlog(session)
    |> push_event("trace_init", %{spans: Traces.list_spans(session.id)})
    |> list_changes()
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
    |> reset_changes()
    |> assign(:page_title, "Console")
  end

  defp unsubscribe_current(socket) do
    if socket.assigns[:subscribed_id], do: Events.unsubscribe(socket.assigns.subscribed_id)
    assign(socket, :subscribed_id, nil)
  end

  # Clears the Changes panel + diff viewer to their empty state.
  defp reset_changes(socket) do
    assign(socket, changes: [], changes_loading: false, changes_error: nil, diff_result: nil)
  end

  # Fetch the working-tree changes (git status vs HEAD). Empty path = the whole
  # changed-files list only; the daemon folds that list into every diff response.
  defp list_changes(%{assigns: %{session: nil}} = socket), do: socket

  defp list_changes(socket) do
    case send_command(socket, "diff_request", %{"path" => ""}) do
      :ok -> assign(socket, changes_loading: true, changes_error: nil)
      _ -> assign(socket, changes_loading: false, changes_error: offline_msg())
    end
  end

  # Fetch one file's unified diff into the diff viewer.
  defp open_diff(socket, path) do
    result =
      case send_command(socket, "diff_request", %{"path" => path}) do
        :ok -> %{"path" => path, "loading" => true}
        _ -> %{"path" => path, "ok" => false, "error" => offline_msg()}
      end

    assign(socket, :diff_result, result)
  end

  # Updates the diff viewer only when a specific file was requested (path != "").
  # The list-only refresh (path "") leaves any open diff untouched.
  defp assign_open_diff(socket, path, diff_text) when is_binary(path) and path != "" do
    result =
      if diff_text in [nil, ""] do
        %{"path" => path, "ok" => true, "diff" => "", "empty" => true}
      else
        %{"path" => path, "ok" => true, "diff" => diff_text}
      end

    assign(socket, :diff_result, result)
  end

  defp assign_open_diff(socket, _path, _diff_text), do: socket

  defp assign_diff_error(socket, path, error) when is_binary(path) and path != "" do
    assign(socket, :diff_result, %{"path" => path, "ok" => false, "error" => error})
  end

  defp assign_diff_error(socket, _path, _error), do: socket

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

  # Pushes the known browser terminal size to an online session's client, so a
  # freshly-registered (oad) client adopts the right PTY width. Side-effecting;
  # returns the socket unchanged.
  defp reassert_term_size(socket, session) do
    case socket.assigns.term_size do
      %{rows: rows, cols: cols} ->
        if active?(session) do
          ClientCommands.send_command(session.id, "pty_resize", %{"rows" => rows, "cols" => cols})
        end

      _ ->
        :ok
    end

    socket
  end

  defp offline_msg, do: "agent offline — reconnect to read changes"

  # Lazily fetch the data a right-pane tab needs the first time it's shown.
  defp ensure_tab_loaded(socket, "files") do
    if socket.assigns.changes == [], do: list_changes(socket), else: socket
  end

  defp ensure_tab_loaded(socket, _tab), do: socket

  # ── UI preferences (persisted client-side by the Prefs hook) ─────────────────

  defp save_prefs(socket) do
    keys = [:right_tab | @bool_prefs ++ Map.keys(@num_prefs)]
    push_event(socket, "prefs_save", Map.new(keys, &{&1, socket.assigns[&1]}))
  end

  defp restore_bool_prefs(socket, prefs) do
    Enum.reduce(@bool_prefs, socket, fn key, acc ->
      assign_bool_pref(acc, key, prefs[Atom.to_string(key)])
    end)
  end

  defp restore_num_prefs(socket, prefs) do
    Enum.reduce(@num_prefs, socket, fn {key, {min, max}}, acc ->
      assign_num_pref(acc, key, prefs[Atom.to_string(key)], min, max)
    end)
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

  # Maps a `git status --porcelain` code to a one-letter badge + colour.
  @doc false
  def status_badge(status) do
    cond do
      status == "??" -> %{label: "U", color: "var(--prov-green)"}
      String.contains?(status, "D") -> %{label: "D", color: "var(--prov-red)"}
      String.contains?(status, "A") -> %{label: "A", color: "var(--prov-green)"}
      String.contains?(status, "R") -> %{label: "R", color: "var(--accent)"}
      String.contains?(status, "M") -> %{label: "M", color: "var(--prov-orange)"}
      true -> %{label: status, color: "var(--faint)"}
    end
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

  defp blank_to_nil(value) do
    case value && String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  # Subscribes to a build's progress topic, kicks off the async build, and opens
  # the progress modal seeded with `log_line`. Shared by create + rebuild.
  defp start_build(socket, attrs, log_line, flash) do
    name = attrs.name
    if connected?(socket), do: Phoenix.PubSub.subscribe(Omniagent.PubSub, "oad_build:#{name}")
    Oad.Builder.build_async(attrs)

    socket
    |> open_build_modal(name, [log_line])
    |> put_flash(:info, flash)
  end

  # Opens the build-progress modal for `name`, seeding its log and resetting it
  # to the building state.
  defp open_build_modal(socket, name, log) do
    socket
    |> assign(:modal, :oad_build)
    |> assign(:oad_build_name, name)
    |> assign(:oad_build_status, "building")
    |> assign(:oad_build_log, log)
  end

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

  # Adds the browser terminal size to oad session opts so the agent PTY opens at
  # the right width (avoids the agent laying out wider than the visible view).
  defp put_term_size(opts, %{rows: rows, cols: cols}) when is_integer(rows) and is_integer(cols),
    do: Map.merge(opts, %{rows: rows, cols: cols})

  defp put_term_size(opts, _), do: opts

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

  # Folds the chosen worktree mode into the spawn payload: "create" requests an
  # isolated worktree (optionally for a named branch off a base); "existing"
  # reuses a linked worktree; anything else spawns in place.
  defp apply_worktree(map, "create", params) do
    map
    |> Map.put("create_worktree", true)
    |> put_present("branch", params["branch"])
    |> put_present("base_branch", params["base_branch"])
  end

  defp apply_worktree(map, "existing", params),
    do: put_present(map, "worktree", params["worktree"])

  defp apply_worktree(map, _mode, _params), do: map

  # ── New-agent workspace pickers (data from daemon-advertised metadata) ───────

  @doc false
  def daemon_by_id(_daemons, nil), do: nil
  def daemon_by_id(daemons, id), do: Enum.find(daemons, &(&1.id == id))

  defp first_daemon_id([daemon | _]), do: daemon.id
  defp first_daemon_id(_), do: nil

  # "hostname:pid" label for a daemon (just the hostname when no pid advertised).
  @doc false
  def daemon_label(daemon) do
    host = daemon.metadata["hostname"] || "daemon"

    case daemon.metadata["pid"] do
      nil -> host
      pid -> "#{host}:#{pid}"
    end
  end

  @doc false
  def daemon_workspaces(nil), do: []
  def daemon_workspaces(daemon), do: daemon.metadata["workspaces"] || []

  @doc false
  def workspace_by_path(daemon, path) do
    daemon |> daemon_workspaces() |> Enum.find(&(&1["path"] == path))
  end

  # Git details for a workspace map (`nil` for plain dirs / older daemons).
  @doc false
  def workspace_git(nil), do: nil
  def workspace_git(ws), do: ws["git"]

  # The sidebar shows a workspace by its trailing segment (the project name) — the
  # meaningful suffix — with the full path in a tooltip.
  @doc false
  def workspace_label(path) when is_binary(path) do
    case Path.basename(path) do
      "" -> path
      base -> base
    end
  end

  def workspace_label(path), do: path

  # Inactive = ended sessions (exited or offline), treated alike. They collect in
  # the collapsible "Sessions" panel rather than under their workspace.
  @inactive_statuses ~w(exited offline)

  @doc false
  def active?(session), do: session.status not in @inactive_statuses

  # Active sessions launched against a given workspace root (matched on the origin
  # repo path recorded in session metadata at spawn). These nest under the
  # workspace in the tree.
  @doc false
  def workspace_sessions(sessions, path) do
    Enum.filter(sessions, &(active?(&1) and &1.metadata["workspace"] == path))
  end

  # Active sessions running on a given oad workspace (matched on the workspace
  # name recorded in session metadata at start). These nest under the oad
  # workspace in the sidebar, mirroring daemon workspace sessions.
  @doc false
  def oad_workspace_sessions(sessions, name) do
    Enum.filter(sessions, &(active?(&1) and &1.metadata["oad_workspace"] == name))
  end

  # The "Sessions" panel: everything not nested live under a visible workspace —
  # all inactive (exited/offline) sessions plus any active session whose workspace
  # isn't advertised by a connected daemon or oad workspace. Kept reachable but
  # tucked away.
  @doc false
  def panel_sessions(sessions, daemons, oad_workspaces) do
    paths = for d <- daemons, ws <- daemon_workspaces(d), into: MapSet.new(), do: ws["path"]
    oad_names = for w <- oad_workspaces, into: MapSet.new(), do: w.name

    Enum.reject(sessions, fn session ->
      active?(session) and
        (MapSet.member?(paths, session.metadata["workspace"]) or
           MapSet.member?(oad_names, session.metadata["oad_workspace"]))
    end)
  end

  # Footer identity. Auth is admin-session based, so fall back to "admin" when no
  # user row is resolved.
  @doc false
  def user_label(nil), do: "admin"
  def user_label(user), do: user.email || "admin"

  @doc false
  def user_initial(user) do
    (user |> user_label() |> String.first() || "a") |> String.upcase()
  end

  # A single session entry in the sidebar. `compact` renders the subordinate form
  # nested under a workspace (no argv line); the full form is used standalone.
  attr :session, :map, required: true
  attr :active_id, :string, default: nil
  attr :compact, :boolean, default: false

  def session_row(assigns) do
    ~H"""
    <.link
      patch={~p"/sessions/#{@session.id}"}
      class={[
        "block rounded-md transition-colors",
        (@compact && "px-2 py-1") || "px-3 py-2",
        @active_id == @session.id && "bg-[var(--accent-soft)]",
        @active_id != @session.id && "hover:bg-[var(--panel-2)]"
      ]}
    >
      <div class="flex items-center gap-1.5">
        <.icon name="hero-command-line-micro" class="size-3.5 flex-none text-[var(--faint)]" />
        <span class={[
          "truncate text-[var(--text)]",
          (@compact && "text-[12px]") || "text-sm"
        ]}>
          {@session.name || @session.id}
        </span>
      </div>
      <%= unless @compact do %>
        <div class="mt-1 truncate pl-4 font-mono text-[11px] text-[var(--faint)]">
          {Enum.join(@session.argv || [], " ")}
        </div>
      <% end %>
    </.link>
    """
  end

  # A centered overlay dialog. `on_close` is the click-away/escape target;
  # `panel_class`/`overlay_class` tune size + backdrop per modal.
  attr :on_close, :string, default: "close_modal"
  attr :panel_class, :string, default: "w-full max-w-md p-5"
  attr :overlay_class, :string, default: "bg-black/60"
  slot :inner_block, required: true

  def modal(assigns) do
    ~H"""
    <div class={["fixed inset-0 z-50 flex items-center justify-center p-4", @overlay_class]}>
      <div class={["oa-panel", @panel_class]} phx-click-away={@on_close}>
        {render_slot(@inner_block)}
      </div>
    </div>
    """
  end

  # The Name / Model / Extra-args trio shared by the new-agent and oad-agent
  # forms. The agent dropdown differs between them, so it stays inline.
  attr :extra_placeholder, :string, default: "appended to the agent command"

  def agent_meta_fields(assigns) do
    ~H"""
    <label class="block">
      <span class="oa-label mb-1">Name (optional)</span>
      <input name="name" class="oa-input w-full" placeholder="my session" autocomplete="off" />
    </label>
    <label class="block">
      <span class="oa-label mb-1">Model (optional)</span>
      <input
        name="model"
        class="oa-input w-full"
        placeholder="defaults to the agent's model"
        autocomplete="off"
      />
    </label>
    <label class="block">
      <span class="oa-label mb-1">Extra args (optional)</span>
      <input
        name="custom_command"
        class="oa-input w-full"
        placeholder={@extra_placeholder}
        autocomplete="off"
      />
    </label>
    """
  end

  attr :changes, :list, required: true
  attr :changes_error, :any, default: nil
  attr :changes_loading, :boolean, default: false

  def files_tab(assigns) do
    ~H"""
    <%!-- Header: Changes label, count, refresh --%>
    <div class="mb-3 flex items-center gap-1">
      <span class="font-mono text-[11px] text-[var(--dim)]">Changes</span>
      <span class="ml-auto font-mono text-[10px] text-[var(--faint)]">
        <%= if @changes != [] do %>
          {length(@changes)} changed
        <% end %>
      </span>
      <button
        type="button"
        phx-click="refresh_changes"
        title="Refresh changes"
        class="flex-none px-1.5 text-[var(--faint)] hover:text-[var(--text)]"
      >
        ⟳
      </button>
    </div>

    <%!-- Changed-files list (git status vs HEAD); a click opens the diff modal --%>
    <%= cond do %>
      <% @changes == [] and @changes_error -> %>
        <p class="font-mono text-xs text-[var(--prov-red)]">{@changes_error}</p>
      <% @changes == [] and @changes_loading -> %>
        <p class="font-mono text-xs text-[var(--faint)]">loading…</p>
      <% @changes == [] -> %>
        <p class="font-mono text-xs text-[var(--faint)]">no changes</p>
      <% true -> %>
        <%= if @changes_error do %>
          <p class="mb-2 font-mono text-xs text-[var(--prov-red)]">{@changes_error}</p>
        <% end %>
        <div class="overflow-hidden">
          <%= for file <- @changes do %>
            <% badge = status_badge(file["status"]) %>
            <button
              type="button"
              phx-click="open_diff"
              phx-value-path={file["path"]}
              class="flex w-full items-center gap-2 py-1 pl-2 pr-2 text-left font-mono text-[11px] hover:bg-[var(--panel-2)]"
            >
              <span
                class="w-4 flex-none text-center font-semibold"
                style={"color: #{badge.color}"}
              >
                {badge.label}
              </span>
              <span class="truncate text-[var(--dim)]">{file["path"]}</span>
            </button>
          <% end %>
        </div>
    <% end %>
    """
  end

  attr :reviews, :list, required: true
  attr :auto_approve, :boolean, required: true

  def reviews_tab(assigns) do
    ~H"""
    <div class="mb-3 flex items-center justify-between">
      <span class="oa-label">Review gate</span>
      <button
        type="button"
        phx-click="toggle_auto_approve"
        aria-pressed={@auto_approve}
        class={"oa-btn text-xs #{if @auto_approve, do: "ok", else: ""}"}
      >
        <span class={"oa-dot #{if @auto_approve, do: "on", else: ""}"}></span>
        Auto {if @auto_approve, do: "on", else: "off"}
      </button>
    </div>
    <div class="space-y-2">
      <%= for item <- @reviews do %>
        <div class="oa-panel p-3">
          <div class="font-mono text-[11px] text-[var(--dim)]">
            {item.external_id} · {item.phase} · {item.path}
          </div>
          <%= if item.decision do %>
            <div class="mt-2 flex items-center gap-2 font-mono text-[11px]">
              <span class={"oa-status #{review_decision_class(item.decision)}"}>
                {review_decision_label(item.decision)}
              </span>
              <%= if item.decided_at do %>
                <span class="text-[var(--faint)]">
                  {Calendar.strftime(item.decided_at, "%Y-%m-%d %H:%M")}
                </span>
              <% end %>
            </div>
          <% else %>
            <div class="mt-2 flex flex-wrap gap-2">
              <button
                phx-click="review_decision"
                phx-value-id={item.external_id}
                phx-value-action="approve"
                class="oa-btn ok text-xs"
              >
                Approve
              </button>
              <button
                phx-click="review_decision"
                phx-value-id={item.external_id}
                phx-value-action="retry"
                class="oa-btn primary text-xs"
              >
                Retry
              </button>
              <button
                phx-click="review_decision"
                phx-value-id={item.external_id}
                phx-value-action="reject"
                class="oa-btn danger text-xs"
              >
                Reject
              </button>
            </div>
          <% end %>
        </div>
      <% end %>
      <%= if @reviews == [] do %>
        <p class="font-mono text-xs text-[var(--faint)]">no pending reviews</p>
      <% end %>
    </div>
    """
  end

  attr :artifacts, :list, required: true
  attr :session, :map, required: true

  def artifacts_tab(assigns) do
    ~H"""
    <div class="mb-2 oa-label">Captured artifacts</div>
    <%= if @artifacts == [] do %>
      <p class="font-mono text-xs text-[var(--faint)]">
        no artifacts yet — recording, traces, and logs upload when the session ends
      </p>
    <% else %>
      <ul class="space-y-2">
        <%= for artifact <- @artifacts do %>
          <li class="oa-panel flex items-center justify-between gap-3 p-3">
            <div class="min-w-0">
              <div class="truncate text-sm text-[var(--text)]">
                {artifact_label(artifact.kind)}
              </div>
              <div class="mt-0.5 font-mono text-[11px] text-[var(--faint)]">
                {format_bytes(artifact.size)} · {Calendar.strftime(
                  artifact.inserted_at,
                  "%Y-%m-%d %H:%M"
                )}
              </div>
            </div>
            <div class="flex flex-none items-center gap-2">
              <%= if artifact.kind == "recording" do %>
                <button
                  type="button"
                  phx-click="play_recording"
                  phx-value-id={artifact.id}
                  class="oa-btn primary text-xs"
                >
                  Play
                </button>
              <% end %>
              <a
                href={~p"/sessions/#{@session.id}/artifacts/#{artifact.id}/download"}
                class="oa-btn text-xs"
                download
              >
                Download
              </a>
            </div>
          </li>
        <% end %>
      </ul>
    <% end %>
    """
  end

  attr :sessions, :list, required: true
  attr :session, :any, required: true
  attr :daemons, :list, required: true
  attr :oad_instances, :list, required: true
  attr :oad_workspaces, :list, required: true
  attr :sidebar_collapsed, :boolean, required: true
  attr :sessions_collapsed, :boolean, required: true
  attr :current_user, :any, required: true

  def sidebar(assigns) do
    ~H"""
    <aside class={[
      "flex flex-none flex-col border-r border-[var(--line)] bg-[var(--panel)]",
      @sidebar_collapsed && "w-12",
      !@sidebar_collapsed && "w-[var(--left-w)]"
    ]}>
      <%= if @sidebar_collapsed do %>
        <button
          type="button"
          phx-click="toggle_sidebar"
          title="Expand sidebar"
          class="flex h-[49px] w-full flex-none items-center justify-center border-b border-[var(--line)] text-[var(--faint)] hover:text-[var(--text)]"
        >
          »
        </button>
        <div class="flex flex-1 flex-col items-center gap-2 overflow-y-auto py-3">
          <%= for session <- Enum.filter(@sessions, &active?/1) do %>
            <.link
              patch={~p"/sessions/#{session.id}"}
              title={session.name || session.id}
              class={[
                "relative flex h-9 w-9 flex-none items-center justify-center rounded-full border text-xs font-bold uppercase transition-colors",
                @session && @session.id == session.id &&
                  "border-[var(--accent)] bg-[var(--accent-soft)] text-[var(--accent)]",
                !(@session && @session.id == session.id) &&
                  "border-[var(--line)] text-[var(--dim)] hover:bg-[var(--panel-2)]"
              ]}
            >
              {session_initial(session)}
            </.link>
          <% end %>
          <button
            type="button"
            phx-click="toggle_sidebar"
            title="Expand to launch an agent"
            class="flex h-9 w-9 flex-none items-center justify-center rounded-full border border-[var(--line)] text-[var(--faint)] hover:border-[var(--accent)] hover:text-[var(--accent)]"
          >
            +
          </button>
        </div>
      <% else %>
        <div class="flex items-center justify-between border-b border-[var(--line)] px-3 py-2.5">
          <.link href={~p"/"} class="flex min-w-0 items-center gap-3">
            <img src={~p"/logo.svg"} alt="" class="oa-logo" />
            <Layouts.wordmark />
          </.link>
          <button
            type="button"
            phx-click="toggle_sidebar"
            title="Collapse sidebar"
            class="flex-none rounded-md px-1.5 py-1 text-sm text-[var(--faint)] hover:bg-[var(--panel-2)] hover:text-[var(--text)]"
          >
            «
          </button>
        </div>

        <div class="flex-1 space-y-6 overflow-y-auto px-2 py-3">
          <%= if @daemons == [] do %>
            <div class="px-2 py-10 text-center">
              <p class="text-sm text-[var(--dim)]">No daemon connected</p>
              <p class="mt-1 font-mono text-[12px] text-[var(--faint)]">
                run <code>omniagent daemon</code>
              </p>
            </div>
          <% else %>
            <%= for daemon <- @daemons do %>
              <% workspaces = daemon_workspaces(daemon) %>
              <section class="space-y-1">
                <div class="group/daemon flex items-center gap-2 px-1 pt-1">
                  <span class="flex-none text-[11px] font-semibold uppercase tracking-wide text-[var(--faint)]">
                    {daemon_label(daemon)}
                  </span>
                  <span class="h-px flex-1 bg-[var(--line)]"></span>
                  <button
                    type="button"
                    phx-click="open_new_workspace"
                    phx-value-daemon_id={daemon.id}
                    title="Create a new workspace on this daemon"
                    class="flex-none rounded px-1 text-[11px] uppercase tracking-wide text-[var(--faint)] opacity-0 hover:text-[var(--accent)] focus:opacity-100 group-hover/daemon:opacity-100"
                  >
                    + ws
                  </button>
                </div>

                <%= if workspaces == [] do %>
                  <button
                    type="button"
                    phx-click="open_new_workspace"
                    phx-value-daemon_id={daemon.id}
                    class="block px-2 py-1 text-left font-mono text-[12px] text-[var(--faint)] hover:text-[var(--accent)]"
                  >
                    no workspaces — <span class="text-[var(--accent)]">create one</span>
                  </button>
                <% else %>
                  <ul class="space-y-0.5">
                    <%= for ws <- workspaces do %>
                      <% ws_sessions = workspace_sessions(@sessions, ws["path"]) %>
                      <li>
                        <button
                          type="button"
                          phx-click="launch_workspace"
                          phx-value-daemon_id={daemon.id}
                          phx-value-path={ws["path"]}
                          title={"Run an agent on #{ws["path"]}"}
                          class="group/ws flex w-full items-center gap-2 rounded-md py-1.5 pl-2 pr-2 text-left hover:bg-[var(--panel-2)]"
                        >
                          <.icon
                            name="hero-folder-micro"
                            class="size-3.5 flex-none text-[var(--faint)]"
                          />
                          <span class="min-w-0 flex-1 truncate text-sm text-[var(--dim)] group-hover/ws:text-[var(--text)]">
                            {workspace_label(ws["path"])}
                          </span>
                          <span class="flex-none text-[11px] uppercase tracking-wide text-[var(--faint)] opacity-0 group-hover/ws:text-[var(--accent)] group-hover/ws:opacity-100">
                            + agent
                          </span>
                        </button>
                        <%= if ws_sessions != [] do %>
                          <div class="mt-px space-y-px pl-5">
                            <%= for session <- ws_sessions do %>
                              <.session_row
                                session={session}
                                active_id={@session && @session.id}
                                compact
                              />
                            <% end %>
                          </div>
                        <% end %>
                      </li>
                    <% end %>
                  </ul>
                <% end %>
              </section>
            <% end %>
          <% end %>

          <section class="space-y-1">
            <div class="flex items-center gap-2 px-1 pt-1">
              <span class="flex-none text-[11px] font-semibold uppercase tracking-wide text-[var(--faint)]">
                oad
              </span>
              <span class="h-px flex-1 bg-[var(--line)]"></span>
              <%= if @oad_instances != [] do %>
                <button
                  type="button"
                  phx-click="open_new_oad_workspace"
                  title="Build a new oad workspace"
                  class="flex-none rounded px-1 text-[11px] uppercase tracking-wide text-[var(--faint)] hover:text-[var(--accent)]"
                >
                  + ws
                </button>
              <% end %>
            </div>

            <%= if @oad_instances == [] do %>
              <p class="px-2 py-1 font-mono text-[12px] text-[var(--faint)]">
                no oad instance registered
              </p>
            <% else %>
              <%= if @oad_workspaces == [] do %>
                <button
                  type="button"
                  phx-click="open_new_oad_workspace"
                  class="block px-2 py-1 text-left font-mono text-[12px] text-[var(--faint)] hover:text-[var(--accent)]"
                >
                  no workspaces — <span class="text-[var(--accent)]">build one</span>
                </button>
              <% else %>
                <ul class="space-y-0.5">
                  <%= for ws <- @oad_workspaces do %>
                    <% oad_sessions = oad_workspace_sessions(@sessions, ws.name) %>
                    <li class="group/oadws">
                      <div class="flex items-center gap-2 rounded-md py-1.5 pl-2 pr-2 hover:bg-[var(--panel-2)]">
                        <.icon
                          name="hero-folder-micro"
                          class="size-3.5 flex-none text-[var(--faint)]"
                        />
                        <span class="min-w-0 flex-1 truncate text-sm text-[var(--dim)]">
                          {ws.name}
                          <span class="text-[11px] text-[var(--faint)]">· {ws.status}</span>
                        </span>
                        <%= if ws.status == "ready" do %>
                          <button
                            type="button"
                            phx-click="open_oad_agent"
                            phx-value-workspace={ws.name}
                            title={"Run an agent on #{ws.name}"}
                            class="flex-none text-[11px] uppercase tracking-wide text-[var(--faint)] opacity-0 hover:text-[var(--accent)] group-hover/oadws:opacity-100"
                          >
                            + agent
                          </button>
                        <% end %>
                        <button
                          type="button"
                          phx-click="rebuild_oad_workspace"
                          phx-value-name={ws.name}
                          title="Rebuild this workspace"
                          class="flex-none text-[11px] uppercase tracking-wide text-[var(--faint)] opacity-0 hover:text-[var(--accent)] group-hover/oadws:opacity-100"
                        >
                          rebuild
                        </button>
                      </div>
                      <%= if oad_sessions != [] do %>
                        <div class="mt-px space-y-px pl-5">
                          <%= for session <- oad_sessions do %>
                            <.session_row
                              session={session}
                              active_id={@session && @session.id}
                              compact
                            />
                          <% end %>
                        </div>
                      <% end %>
                    </li>
                  <% end %>
                </ul>
              <% end %>
            <% end %>
          </section>

          <% panel = panel_sessions(@sessions, @daemons, @oad_workspaces) %>
          <%= if panel != [] do %>
            <section class="space-y-1">
              <button
                type="button"
                phx-click="toggle_sessions"
                class="group/sessions flex w-full items-center gap-1 pr-1 py-0.5 text-left"
              >
                <span class={[
                  "inline-block w-2 flex-none text-center text-[9px] text-[var(--faint)] transition-transform",
                  !@sessions_collapsed && "rotate-90"
                ]}>
                  ›
                </span>
                <span class="flex-1 text-[12px] font-semibold uppercase tracking-wide text-[var(--faint)] group-hover/sessions:text-[var(--dim)]">
                  Inactive
                </span>
                <span class="flex-none font-mono text-[11px] text-[var(--faint)]">
                  {length(panel)}
                </span>
              </button>
              <%= unless @sessions_collapsed do %>
                <div class="space-y-px">
                  <%= for session <- panel do %>
                    <.session_row session={session} active_id={@session && @session.id} compact />
                  <% end %>
                </div>
              <% end %>
            </section>
          <% end %>
        </div>

        <div class="flex items-center gap-2 border-t border-[var(--line)] px-3 py-2.5">
          <span class="flex h-7 w-7 flex-none items-center justify-center rounded-full border border-[var(--line)] bg-[var(--panel-2)] text-[11px] font-bold uppercase text-[var(--dim)]">
            {user_initial(@current_user)}
          </span>
          <span
            class="min-w-0 flex-1 truncate text-[12px] text-[var(--dim)]"
            title={user_label(@current_user)}
          >
            {user_label(@current_user)}
          </span>
          <.link
            href={~p"/logout"}
            method="delete"
            title="Sign out"
            class="flex-none rounded-md px-1.5 py-1 text-[12px] text-[var(--faint)] hover:bg-[var(--panel-2)] hover:text-[var(--text)]"
          >
            Sign out
          </.link>
        </div>
      <% end %>
    </aside>
    """
  end

  # Middle pane for a selected session: header, terminal/codex/ended view, and
  # the trace stream. The empty (no-selection) state stays in the template.
  attr :session, :map, required: true

  def session_pane(assigns) do
    ~H"""
    <header class="flex flex-none items-center justify-between gap-3 border-b border-[var(--line)] px-4 py-3">
      <div class="flex min-w-0 items-center gap-3">
        <h1 class="oa-display truncate text-lg font-bold">{@session.name || @session.id}</h1>
        <span class={"oa-status #{status_class(@session.status)}"}>{@session.status}</span>
        <code class="hidden truncate font-mono text-[11px] text-[var(--faint)] md:block">
          {@session.cwd}
        </code>
        <%= if branch = @session.metadata["branch"] do %>
          <span class="oa-label hidden shrink-0 md:inline-flex" title="git branch">
            ⎇ {branch}
          </span>
        <% end %>
      </div>
      <%= if @session.status != "online" do %>
        <button
          type="button"
          phx-click="delete_session"
          data-confirm="Delete this session and all its recorded data?"
          class="oa-btn danger text-xs"
        >
          Delete
        </button>
      <% end %>
    </header>

    <div
      class="flex min-h-0 flex-col border-b border-[var(--line)] p-2"
      style="flex: 0 0 var(--term-basis)"
    >
      <%= cond do %>
        <% Sessions.codex_native?(@session) -> %>
          <%!-- Native codex conversation: the Codex hook owns the whole pane
                (transcript + composer + interrupt), driven by push_event. --%>
          <div
            id={"codex-#{@session.id}"}
            phx-hook="Codex"
            phx-update="ignore"
            data-status={@session.status}
            class="oa-codex flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border border-[var(--line)] bg-[var(--panel)]"
          >
          </div>
        <% @session.status == "online" -> %>
          <div
            id={"terminal-#{@session.id}"}
            phx-hook="Terminal"
            phx-update="ignore"
            class="oa-terminal min-h-0 flex-1 overflow-hidden rounded-md border border-[var(--line)] bg-black p-2"
          >
          </div>
        <% true -> %>
          <div class="flex min-h-0 flex-1 flex-col items-center justify-center gap-2 rounded-md border border-[var(--line)] bg-black text-center">
            <p class="oa-display text-sm font-bold text-[var(--dim)]">Session ended</p>
            <p class="font-mono text-[11px] text-[var(--faint)]">
              open the <span class="text-[var(--accent)]">Artifacts</span>
              tab to replay or download the recording
            </p>
          </div>
      <% end %>
    </div>

    <div
      id="gutter-term"
      phx-hook="Resize"
      data-axis="y"
      data-var="--term-basis"
      data-prop="term_pct"
      data-unit="%"
      data-min="20"
      data-max="85"
      class="oa-gutter oa-gutter-y"
      title="Drag to resize"
    >
    </div>

    <div class="flex min-h-0 flex-1 flex-col p-2">
      <div class="oa-label mb-1 px-1">Traces</div>
      <div
        id={"traces-#{@session.id}"}
        phx-hook="Traces"
        phx-update="ignore"
        class="oa-traces min-h-0 flex-1 overflow-y-auto"
      >
        <div class="oa-trace-empty" data-trace-empty>
          no LLM calls yet — requests appear as the agent talks to the model
        </div>
      </div>
    </div>
    """
  end

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
