defmodule Omniagent.Oad.Sessions do
  @moduledoc """
  Starts and reaps agent sessions on oad workspaces.

  Forks a fresh, isolated sandbox from the workspace's immutable snapshot,
  pre-creates the session row, and starts `omniagent serve-session` inside the
  fork via a background exec. The in-sandbox process opens its own `client:`
  channel back to the control plane and runs the full session pipeline; a reaper
  task watches the exec and deletes the fork when it exits.

  Auth note: the in-sandbox process authenticates to the control plane with a
  token passed via `OMNIAGENT_SESSION_TOKEN`. For now this is the configured dev
  token (`:omniagent, :oad_session_token`); a per-session **scoped** token
  (mint/expire/revoke + scope enforcement) is deferred to a dedicated auth PR —
  this module is the plumbing seam where that minted token will be substituted.
  """

  require Logger

  alias Omniagent.{OadInstances, OadWorkspaces, Sessions}
  alias Omniagent.Oad.Client
  alias Omniagent.OadInstances.OadInstance
  alias Omniagent.OadWorkspaces.OadWorkspace

  @poll_interval 10_000
  @default_omniagent_path "/opt/omniagent/bin/omniagent"

  @doc """
  Starts a session on the named workspace. `opts`:

    * `:user` — owning user (defaults to the console operator)
    * `:agent` — `claude` | `codex` | `gemini` (default `claude`)
    * `:name` — optional session name
  """
  def start_session(workspace_name, opts \\ %{}) do
    opts = Map.new(opts)
    user = opts[:user] || Omniagent.Accounts.default_user()
    agent = opts[:agent] || "claude"

    with {:ok, workspace} <- ready_workspace(workspace_name),
         {:ok, instance} <- place_session(workspace),
         {:ok, server_url} <- server_url(instance),
         {:ok, session} <- pre_create_session(user, workspace, agent, opts[:name]),
         {:ok, fork_id} <- fork(instance, workspace) do
      case launch(instance, fork_id, workspace, session, agent, server_url, opts) do
        {:ok, exec_id} ->
          Sessions.merge_metadata(session, %{
            "runner" => "oad",
            "oad_base_url" => instance.base_url,
            "oad_workspace" => workspace.name,
            "fork_sandbox_id" => fork_id,
            "exec_id" => exec_id
          })

          spawn_reaper(instance, fork_id, exec_id, session.id)
          {:ok, session}

        {:error, reason} ->
          # Don't leak the fork we just created if launching the agent failed.
          Client.delete_sandbox(instance, fork_id)
          {:error, reason}
      end
    end
  end

  @doc "Stops a session: kills its exec, deletes the fork. Best-effort."
  def stop_session(%{metadata: metadata}) when is_map(metadata) do
    with base_url when is_binary(base_url) <- metadata["oad_base_url"],
         fork_id when is_binary(fork_id) <- metadata["fork_sandbox_id"],
         %OadInstance{} = instance <- find_instance(base_url) do
      if exec_id = metadata["exec_id"], do: Client.kill_exec(instance, fork_id, exec_id)
      Client.delete_sandbox(instance, fork_id)
      :ok
    else
      _ -> {:error, :not_an_oad_session}
    end
  end

  def stop_session(_), do: {:error, :not_an_oad_session}

  defp ready_workspace(name) do
    case OadWorkspaces.get_by_name(name) do
      %OadWorkspace{status: "ready", snapshot: snapshot} = ws when is_binary(snapshot) ->
        {:ok, ws}

      %OadWorkspace{} ->
        {:error, :workspace_not_ready}

      nil ->
        {:error, :workspace_not_found}
    end
  end

  defp live_instance(base_url) do
    case find_instance(base_url) do
      %OadInstance{} = instance -> {:ok, instance}
      nil -> {:error, {:oad_instance_offline, base_url}}
    end
  end

  # Picks the oad instance to run a session on. A CAS-published workspace (one
  # with a `descriptor_key`) is portable: prefer the endpoint that built it (warm
  # cache, known CAS-capable) but fall back to any live instance, which
  # materializes the snapshot from object storage on demand. A legacy,
  # node-local workspace must run on the endpoint that holds its snapshot.
  # Phase 3 replaces "first live" with a capacity- and cache-aware scheduler.
  defp place_session(%OadWorkspace{descriptor_key: key, oad_base_url: base_url})
       when is_binary(key) and key != "" do
    live = OadInstances.list_live()

    case Enum.find(live, &(&1.base_url == base_url)) || List.first(live) do
      %OadInstance{} = instance -> {:ok, instance}
      nil -> {:error, :no_live_oad_instance}
    end
  end

  defp place_session(%OadWorkspace{oad_base_url: base_url}) when is_binary(base_url) do
    live_instance(base_url)
  end

  defp place_session(%OadWorkspace{}), do: {:error, :no_oad_endpoint}

  defp find_instance(base_url) do
    Enum.find(OadInstances.list_live(), &(&1.base_url == base_url))
  end

  defp pre_create_session(user, workspace, agent, name) do
    Sessions.create_pending_session(user, %{
      name: name || workspace.name,
      cwd: workspace.workspace_folder,
      metadata: %{"runner" => "oad", "oad_workspace" => workspace.name, "agent" => agent}
    })
  end

  defp fork(instance, workspace) do
    body =
      %{"from_snapshot" => workspace.snapshot}
      |> maybe_put_resources(workspace.resources)

    case Client.create(instance, body) do
      {:ok, %{"sandbox" => %{"id" => id}}} -> {:ok, id}
      {:ok, other} -> {:error, {:unexpected_fork_response, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  # Translates the workspace's CPU/memory specs into the OCI-shaped `resources`
  # override the daemon applies to every container restored from the snapshot.
  # Omits the key entirely when nothing is set, so the fork stays unconstrained.
  defp maybe_put_resources(body, resources) when is_map(resources) do
    case oci_resources(resources) do
      map when map == %{} -> body
      oci -> Map.put(body, "resources", oci)
    end
  end

  defp maybe_put_resources(body, _), do: body

  defp oci_resources(resources) do
    %{}
    |> put_cpu(Map.get(resources, "cpu") || Map.get(resources, :cpu))
    |> put_memory(Map.get(resources, "memory") || Map.get(resources, :memory))
  end

  # CPU is entered as a (possibly fractional) core count and mapped onto CFS
  # bandwidth: quota = cores * period, with the conventional 100ms period.
  @cpu_period 100_000
  defp put_cpu(map, value) do
    case parse_cpu(value) do
      nil -> map
      quota -> Map.put(map, "cpu", %{"quota" => quota, "period" => @cpu_period})
    end
  end

  defp put_memory(map, value) do
    case parse_memory(value) do
      nil -> map
      bytes -> Map.put(map, "memory", %{"limit" => bytes})
    end
  end

  defp parse_cpu(value) when is_number(value) and value > 0,
    do: round(value * @cpu_period)

  defp parse_cpu(value) when is_binary(value) do
    case Float.parse(String.trim(value)) do
      {cores, _} when cores > 0 -> round(cores * @cpu_period)
      _ -> nil
    end
  end

  defp parse_cpu(_), do: nil

  # Accepts a plain byte count, a number (interpreted as MB), or a suffixed size
  # (Ki/Mi/Gi binary or K/M/G decimal). Returns bytes, or nil when unparseable.
  defp parse_memory(value) when is_integer(value) and value > 0, do: value * 1_048_576

  defp parse_memory(value) when is_binary(value) do
    trimmed = value |> String.trim() |> String.downcase()

    case Regex.run(~r/^(\d+(?:\.\d+)?)\s*([a-z]*)$/, trimmed) do
      [_, num, suffix] ->
        case Float.parse(num) do
          {n, _} when n > 0 -> round(n * memory_multiplier(suffix))
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp parse_memory(_), do: nil

  defp memory_multiplier("gi"), do: 1024 * 1024 * 1024
  defp memory_multiplier("mi"), do: 1024 * 1024
  defp memory_multiplier("ki"), do: 1024
  defp memory_multiplier("g"), do: 1_000_000_000
  defp memory_multiplier("m"), do: 1_000_000
  defp memory_multiplier("k"), do: 1_000
  defp memory_multiplier("b"), do: 1
  # A bare number is treated as megabytes (the form the modal hints at).
  defp memory_multiplier(""), do: 1_048_576
  defp memory_multiplier(_), do: 1_048_576

  defp launch(instance, fork_id, workspace, session, agent, server_url, opts) do
    omniagent_path = omniagent_path(instance)

    # custom_command is a trailing var-arg on serve-session, so it must come last.
    command =
      [
        omniagent_path,
        "serve-session",
        "--server-url",
        server_url,
        "--session-id",
        session.id,
        "--agent",
        agent,
        "--cwd",
        workspace.workspace_folder
      ] ++
        model_args(opts) ++
        size_args(opts) ++
        setup_args(workspace) ++
        extra_args(opts)

    body = %{
      "command" => command,
      "cwd" => workspace.workspace_folder,
      "pty" => false,
      "env" => session_env(session_token(), server_url, workspace)
    }

    case Client.start_exec(instance, fork_id, body) do
      {:ok, %{"exec" => %{"id" => exec_id}}} -> {:ok, exec_id}
      {:ok, other} -> {:error, {:unexpected_exec_response, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp model_args(opts) do
    case blank_to_nil(opts[:model]) do
      nil -> []
      model -> ["--model", model]
    end
  end

  # The browser terminal size, so the agent PTY opens at the right width instead
  # of serve-session's default (avoids the agent laying out wider than the view).
  defp size_args(opts) do
    case {opts[:rows], opts[:cols]} do
      {rows, cols} when is_integer(rows) and is_integer(cols) and rows > 0 and cols > 0 ->
        ["--rows", Integer.to_string(rows), "--cols", Integer.to_string(cols)]

      _ ->
        []
    end
  end

  defp extra_args(opts) do
    case blank_to_nil(opts[:custom_command]) do
      nil -> []
      command -> split_command(command)
    end
  end

  defp blank_to_nil(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp blank_to_nil(_), do: nil

  # Shell-aware split (honours quotes); falls back to whitespace on malformed
  # input rather than raising.
  defp split_command(command) do
    OptionParser.split(command)
  rescue
    _ -> String.split(command, ~r/\s+/, trim: true)
  end

  defp setup_args(%OadWorkspace{start_script: script}) when is_binary(script) and script != "",
    do: ["--setup-script", script]

  defp setup_args(_), do: []

  # Environment for the in-sandbox agent: the session token, the control-plane
  # URL it dials back on, and the provider credentials (Anthropic + OpenAI) it
  # needs to reach an LLM. We forward only these specific vars from the control
  # plane — NOT the whole environment, which would clobber the sandbox's PATH
  # and leak unrelated secrets. `:oad_session_env` config can add/override more.
  @forwarded_provider_env ~w(
    ANTHROPIC_BASE_URL
    ANTHROPIC_AUTH_TOKEN
    ANTHROPIC_API_KEY
    OPENAI_BASE_URL
    OPENAI_API_KEY
  )

  defp session_env(token, server_url, workspace) do
    provider =
      Map.new(@forwarded_provider_env, fn key -> {key, System.get_env(key)} end)
      |> Map.reject(fn {_key, value} -> is_nil(value) end)

    provider
    |> Map.merge(%{
      "OMNIAGENT_SESSION_TOKEN" => token,
      "OMNIAGENT_CONTROL_PLANE_URL" => server_url
    })
    |> Map.merge(stringify_env(Application.get_env(:omniagent, :oad_session_env, %{})))
    |> Map.merge(stringify_env(workspace_env(workspace)))
    |> Enum.map(fn {key, value} -> %{"name" => key, "value" => value} end)
  end

  defp workspace_env(%OadWorkspace{env: env}) when is_map(env), do: env
  defp workspace_env(_), do: %{}

  defp stringify_env(map) do
    Map.new(map, fn {key, value} -> {to_string(key), to_string(value)} end)
  end

  defp spawn_reaper(instance, fork_id, exec_id, session_id) do
    Task.Supervisor.start_child(Omniagent.TaskSupervisor, fn ->
      watch_until_done(instance, fork_id, exec_id)
      # Capture why serve-session ended before the fork (and its exec log) is
      # torn down — a session that dies on startup is otherwise undiagnosable.
      log_exec_outcome(instance, fork_id, exec_id, session_id)
      Logger.info("oad session #{session_id}: exec finished, deleting fork #{fork_id}")
      Client.delete_sandbox(instance, fork_id)
      # Auth PR: revoke the per-session scoped token here.
    end)
  end

  defp watch_until_done(instance, fork_id, exec_id) do
    case Client.get_exec(instance, fork_id, exec_id) do
      {:ok, %{"exec" => %{"status" => status}}} when status in ["exited", "failed"] ->
        :ok

      {:error, {:http, 404, _}} ->
        :ok

      _ ->
        Process.sleep(@poll_interval)
        watch_until_done(instance, fork_id, exec_id)
    end
  end

  # Logs serve-session's terminal status; on an abnormal exit, also replays its
  # captured stdout/stderr so a startup failure (e.g. can't reach the control
  # plane, agent failed to spawn) is visible in the control-plane log.
  defp log_exec_outcome(instance, fork_id, exec_id, session_id) do
    case Client.get_exec(instance, fork_id, exec_id) do
      {:ok, %{"exec" => %{"status" => "exited", "exit_code" => 0}}} ->
        Logger.info("oad session #{session_id}: serve-session exited cleanly")

      {:ok, %{"exec" => exec}} ->
        output = capture_exec_output(instance, fork_id, exec_id)

        Logger.warning(
          "oad session #{session_id}: serve-session ended abnormally " <>
            "(status=#{exec["status"]} exit_code=#{inspect(exec["exit_code"])} " <>
            "last_error=#{inspect(exec["last_error"])})" <>
            if(output == "", do: "", else: "\n--- serve-session output ---\n" <> output)
        )

      other ->
        Logger.warning(
          "oad session #{session_id}: could not read serve-session status: #{inspect(other)}"
        )
    end
  end

  defp capture_exec_output(instance, fork_id, exec_id) do
    reducer = fn event, acc ->
      case event["type"] do
        type when type in ["stdout", "stderr"] -> [decode_base64(event["data"]) | acc]
        _ -> acc
      end
    end

    case Client.stream_exec_events(instance, fork_id, exec_id, [], reducer) do
      {:ok, {_terminal, chunks}} ->
        chunks |> Enum.reverse() |> Enum.join() |> String.trim() |> String.slice(0, 4000)

      _ ->
        ""
    end
  end

  defp decode_base64(nil), do: ""

  defp decode_base64(value) do
    case Base.decode64(value) do
      {:ok, bytes} -> bytes
      :error -> value
    end
  end

  defp omniagent_path(%OadInstance{capabilities: caps}) when is_map(caps),
    do: caps["omniagent_path"] || @default_omniagent_path

  defp omniagent_path(_), do: @default_omniagent_path

  # The control-plane URL the in-sandbox agent dials back on. Prefer what the
  # oad daemon reported at registration (it already knows it via
  # OAD_CONTROL_PLANE_URL), so the control plane needs no separate config; fall
  # back to the locally configured :public_url.
  defp server_url(%OadInstance{capabilities: caps}) when is_map(caps) do
    case caps["control_plane_url"] do
      url when is_binary(url) and url != "" -> {:ok, url}
      _ -> server_url_from_config()
    end
  end

  defp server_url(_), do: server_url_from_config()

  defp server_url_from_config do
    case Application.get_env(:omniagent, :public_url) do
      url when is_binary(url) and url != "" -> {:ok, url}
      _ -> {:error, :missing_public_url}
    end
  end

  # Dev token plumbing — replaced by a minted per-session scoped token in the
  # auth PR.
  defp session_token do
    Application.get_env(:omniagent, :oad_session_token) || System.get_env("OMNIAGENT_DEV_TOKEN") ||
      "dev-token"
  end
end
