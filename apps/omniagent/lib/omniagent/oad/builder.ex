defmodule Omniagent.Oad.Builder do
  @moduledoc """
  Builds and refreshes oad workspaces.

  A build runs inside a short-lived *builder* sandbox on a registered oad
  instance: install/upgrade the agent CLIs, clone the repo, parse its
  devcontainer for lifecycle hooks, run the create hooks, then snapshot the
  result into an immutable, versioned workspace snapshot and tear the builder
  down. The `omniagent` binary itself is supplied to every sandbox by oad's
  static mount, so it is not installed here.

  `build/1` runs synchronously and returns the updated workspace record;
  `build_async/1` runs it under a task and streams progress over PubSub
  (`"oad_build:<name>"`) for the console.
  """

  require Logger

  alias Omniagent.{OadInstances, OadWorkspaces}
  alias Omniagent.Oad.{Client, Devcontainer}
  alias Omniagent.OadInstances.OadInstance

  @pubsub Omniagent.PubSub

  # Default agent CLIs installed into the base. Overridable via params. Pulled
  # from the internal npm registry (the public registry is slow/unreachable on
  # this network); `--loglevel http` makes npm stream each dependency fetch so
  # the install shows live progress.
  @default_agent_install "npm install -g --loglevel http @anthropic-ai/claude-code @openai/codex @google/gemini-cli"
  @builder_keepalive ["sleep", "infinity"]

  # The daemon boots a fresh sandbox asynchronously (returns 202, pulls the
  # image in the background), so after create we poll until it is running. A
  # cold image pull can be very long; cap polling generously to avoid spinning
  # forever against a wedged daemon.
  @poll_interval_ms 3_000
  @boot_timeout_ms 60 * 60 * 1_000
  @heartbeat_every_polls 5

  @doc "Runs a build/update in a supervised fire-and-forget task (progress via PubSub)."
  def build_async(params) do
    Task.Supervisor.start_child(Omniagent.TaskSupervisor, fn -> safe_build(params) end)
  end

  # Converts an unhandled crash into a surfaced failure: build/1 only handles
  # {:error, _}, so without this an exception would leave the workspace stuck in
  # "building" and the console spinner running forever.
  defp safe_build(params) do
    build(params)
  rescue
    error ->
      name = param(params, :name)

      Logger.error(
        "oad build #{inspect(name)} crashed: " <> Exception.format(:error, error, __STACKTRACE__)
      )

      if name do
        if ws = OadWorkspaces.get_by_name(name) do
          OadWorkspaces.mark_error(ws, "build crashed: #{Exception.message(error)}")
        end

        emit(name, "build crashed: #{Exception.message(error)}")
        emit_done(name, "error")
      end

      {:error, {:crashed, error}}
  end

  @doc """
  Builds (or rebuilds) a workspace. `params`:

    * `:name` — workspace name (required)
    * `:image` — devcontainer/base image (required)
    * `:repo` / `:git_ref` — repository to clone and ref (optional)
    * `:oad_base_url` — target oad endpoint (optional; first live instance otherwise)
    * `:workspace_folder` — in-container path (default `/workspace`)
    * `:agent_install` — shell command installing the agent CLIs (optional)
  """
  def build(params) do
    name = require_param(params, :name)
    image = require_param(params, :image)
    workspace_folder = param(params, :workspace_folder) || "/workspace"

    with {:ok, instance} <- resolve_instance(params),
         {:ok, workspace} <-
           OadWorkspaces.upsert(%{
             name: name,
             oad_base_url: instance.base_url,
             image: image,
             workspace_folder: workspace_folder,
             repo: param(params, :repo),
             git_ref: param(params, :git_ref),
             status: "building"
           }) do
      revision = (workspace.revision || 0) + 1

      case run_build(instance, params, name, image, workspace_folder, revision) do
        {:ok, result} ->
          marked = OadWorkspaces.mark_ready(workspace, Map.put(result, :revision, revision))
          # Emit *after* persisting the terminal status so the console, on
          # receiving these, reads the committed "ready" state.
          emit(name, "workspace #{name} ready")
          emit_done(name, "ready")
          marked

        {:error, reason} ->
          message = format_error(reason)
          OadWorkspaces.mark_error(workspace, message)
          emit(name, "build failed: #{message}")
          emit_done(name, "error")
          {:error, reason}
      end
    end
  end

  defp run_build(instance, params, name, image, workspace_folder, revision) do
    emit(name, "creating builder sandbox from #{image}")

    create_body = %{
      "containers" => [
        %{"name" => "main", "image" => image, "command" => @builder_keepalive}
      ]
    }

    with {:ok, %{"sandbox" => %{"id" => sandbox_id}}} <- Client.create(instance, create_body) do
      # create returns 202 immediately; the sandbox boots (and pulls its image)
      # in the background, so wait for it to be running before running steps.
      result =
        with :ok <- wait_for_running(instance, sandbox_id, name) do
          build_steps(instance, sandbox_id, params, name, workspace_folder, revision)
        end

      # Always tear the builder down, success or failure.
      _ = Client.delete_sandbox(instance, sandbox_id)
      result
    else
      {:ok, other} -> {:error, {:unexpected_create_response, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  # Polls the builder sandbox until it is running (the daemon finished pulling
  # the image and started the container) or fails. Emits heartbeats so the
  # console shows progress through a long pull.
  defp wait_for_running(instance, sandbox_id, name) do
    emit(name, "waiting for builder sandbox to start (pulling image)…")
    deadline = System.monotonic_time(:millisecond) + @boot_timeout_ms
    poll_sandbox(instance, sandbox_id, name, deadline, 0)
  end

  defp poll_sandbox(instance, sandbox_id, name, deadline, polls) do
    case Client.get_sandbox(instance, sandbox_id) do
      {:ok, %{"sandbox" => %{"status" => "running"}}} ->
        emit(name, "builder sandbox running")
        :ok

      {:ok, %{"sandbox" => %{"status" => "error"} = sandbox}} ->
        {:error, {:boot_failed, sandbox["last_error"] || "unknown error"}}

      {:ok, %{"sandbox" => %{"status" => "pending"}}} ->
        continue_polling(instance, sandbox_id, name, deadline, polls)

      # Any other terminal status (stopped/stopping/suspended/unknown) means the
      # sandbox never reached running — e.g. a daemon restart mid-boot.
      {:ok, %{"sandbox" => %{"status" => status}}} ->
        {:error, {:boot_failed, "builder sandbox is #{status}"}}

      {:ok, other} ->
        {:error, {:unexpected_create_response, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp continue_polling(instance, sandbox_id, name, deadline, polls) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline do
      {:error, {:boot_timeout, @boot_timeout_ms}}
    else
      polls = polls + 1

      if rem(polls, @heartbeat_every_polls) == 0 do
        elapsed_s = div(@boot_timeout_ms - (deadline - now), 1_000)
        emit(name, "still pulling image… (#{elapsed_s}s)")
      end

      Process.sleep(@poll_interval_ms)
      poll_sandbox(instance, sandbox_id, name, deadline, polls)
    end
  end

  defp build_steps(instance, sandbox_id, params, name, workspace_folder, revision) do
    agent_install = param(params, :agent_install) || @default_agent_install
    repo = param(params, :repo)
    git_ref = param(params, :git_ref)

    with :ok <- step(instance, sandbox_id, name, "install agents", agent_install),
         agent_versions = capture_agent_versions(instance, sandbox_id),
         :ok <- maybe_clone(instance, sandbox_id, name, repo, git_ref, workspace_folder),
         devcontainer = read_devcontainer(instance, sandbox_id, workspace_folder),
         :ok <- run_create_hooks(instance, sandbox_id, name, workspace_folder, devcontainer),
         {:ok, snapshot} <- snapshot(instance, sandbox_id, name, revision) do
      emit(name, "snapshot #{snapshot} ready")

      {:ok,
       %{
         snapshot: snapshot,
         agent_versions: agent_versions,
         start_script: devcontainer && Devcontainer.start_script(devcontainer),
         workspace_folder: workspace_folder
       }}
    end
  end

  # No repo: still create the workspace folder so it exists in the snapshot —
  # sessions exec with it as cwd, and runsc fails (exit 128) if it's missing.
  defp maybe_clone(instance, sandbox_id, name, nil, _ref, folder) do
    step(instance, sandbox_id, name, "create workspace folder", "mkdir -p #{shell_quote(folder)}")
  end

  defp maybe_clone(instance, sandbox_id, name, repo, ref, folder) do
    ref_args = if ref && ref != "", do: "--branch #{shell_quote(ref)} ", else: ""

    script =
      "rm -rf #{shell_quote(folder)} && git clone #{ref_args}#{shell_quote(repo)} #{shell_quote(folder)}"

    step(instance, sandbox_id, name, "clone repo", script)
  end

  defp run_create_hooks(_i, _s, _name, _folder, nil), do: :ok

  defp run_create_hooks(instance, sandbox_id, name, folder, devcontainer) do
    case Devcontainer.create_script(devcontainer) do
      nil -> :ok
      script -> step(instance, sandbox_id, name, "devcontainer create hooks", script, folder)
    end
  end

  defp snapshot(instance, sandbox_id, name, revision) do
    snapshot_name = "#{name}-v#{revision}"

    case Client.snapshot(instance, sandbox_id, %{"name" => snapshot_name}) do
      {:ok, _} -> {:ok, snapshot_name}
      {:error, reason} -> {:error, reason}
    end
  end

  # Reads the repo's devcontainer.json (if any) and parses it; nil on absence.
  defp read_devcontainer(instance, sandbox_id, folder) do
    script =
      "cat #{folder}/.devcontainer/devcontainer.json 2>/dev/null || " <>
        "cat #{folder}/.devcontainer.json 2>/dev/null || true"

    with {:ok, %{"stdout" => stdout, "exit_code" => 0}} <-
           Client.exec(instance, sandbox_id, %{"command" => ["sh", "-lc", script]}),
         {:ok, text} <- Base.decode64(stdout || ""),
         true <- String.trim(text) != "",
         {:ok, parsed} <- Devcontainer.parse(text) do
      parsed
    else
      _ -> nil
    end
  end

  defp capture_agent_versions(instance, sandbox_id) do
    for agent <- ~w(claude codex gemini), into: %{} do
      script = "#{agent} --version 2>/dev/null | head -1 || true"

      version =
        case Client.exec(instance, sandbox_id, %{"command" => ["sh", "-lc", script]}) do
          {:ok, %{"stdout" => stdout}} -> stdout |> decode64() |> String.trim()
          _ -> ""
        end

      {agent, version}
    end
  end

  # Runs one build step in the workspace folder, streaming its output live to
  # the console (over PubSub) as it runs; a non-zero exit aborts the build. Uses
  # a background exec + SSE event stream rather than a one-off exec so a long
  # step (npm install, devcontainer hooks) shows progress instead of one line.
  defp step(instance, sandbox_id, name, label, script, cwd \\ nil) do
    emit(name, label)
    body = %{"command" => ["sh", "-lc", script]}
    body = if cwd, do: Map.put(body, "cwd", cwd), else: body

    case Client.start_exec(instance, sandbox_id, body) do
      {:ok, %{"exec" => %{"id" => exec_id}}} ->
        stream_step(instance, sandbox_id, name, label, exec_id)

      {:ok, other} ->
        {:error, {:unexpected_create_response, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp stream_step(instance, sandbox_id, name, label, exec_id) do
    # Emit each output line as it streams; retain stderr (newest-first) so a
    # failing step can report a useful tail.
    reducer = fn event, stderr ->
      case event["type"] do
        "stdout" ->
          emit_output(name, decode64(event["data"]))
          stderr

        "stderr" ->
          text = decode64(event["data"])
          emit_output(name, text)
          [text | stderr]

        _ ->
          stderr
      end
    end

    case Client.stream_exec_events(instance, sandbox_id, exec_id, [], reducer) do
      {:ok, {%{"type" => "exited", "exit_code" => 0}, _stderr}} ->
        :ok

      {:ok, {%{"type" => "exited", "exit_code" => code}, stderr}} ->
        emit(name, "#{label} exited #{code}")
        {:error, {:step_failed, label, code, stderr_tail(stderr)}}

      {:ok, {%{"type" => "failed", "message" => message}, _stderr}} ->
        emit(name, "#{label} failed: #{message}")
        {:error, {:step_failed, label, -1, message}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp stderr_tail(stderr_chunks), do: stderr_chunks |> Enum.reverse() |> Enum.join()

  defp resolve_instance(params) do
    case param(params, :oad_base_url) do
      nil ->
        case OadInstances.list_live() do
          [%OadInstance{} = instance | _] -> {:ok, instance}
          [] -> {:error, :no_live_oad_instance}
        end

      base_url ->
        case Enum.find(OadInstances.list_live(), &(&1.base_url == base_url)) do
          %OadInstance{} = instance -> {:ok, instance}
          nil -> {:error, {:oad_instance_offline, base_url}}
        end
    end
  end

  defp decode64(nil), do: ""

  defp decode64(value) do
    case Base.decode64(value) do
      {:ok, bytes} -> bytes
      :error -> value
    end
  end

  # Conservative single-quote shell quoting for interpolated values.
  defp shell_quote(value) do
    "'" <> String.replace(value, "'", "'\\''") <> "'"
  end

  defp param(params, key), do: Map.get(params, key) || Map.get(params, Atom.to_string(key))

  defp require_param(params, key) do
    case param(params, key) do
      nil -> raise ArgumentError, "missing required param #{inspect(key)}"
      value -> value
    end
  end

  defp emit(name, message) do
    Logger.info("oad build #{name}: #{message}")
    Phoenix.PubSub.broadcast(@pubsub, "oad_build:#{name}", {:oad_build, name, message})
  end

  # Streams a step's command output as individual log lines (indented under the
  # step label). Logged at :debug to avoid flooding the app log with every line
  # while still surfacing in the console's build modal.
  defp emit_output(name, text) do
    text
    |> String.split(~r/\r?\n/)
    |> Enum.each(fn line ->
      case String.trim_trailing(line) do
        "" ->
          :ok

        trimmed ->
          Logger.debug("oad build #{name}: #{trimmed}")

          Phoenix.PubSub.broadcast(
            @pubsub,
            "oad_build:#{name}",
            {:oad_build, name, "  " <> trimmed}
          )
      end
    end)
  end

  # Signals the terminal build status to the console, which updates the modal
  # and refreshes the workspace list once (separate from the high-volume log
  # stream so the console doesn't re-query per output line).
  defp emit_done(name, status) do
    Phoenix.PubSub.broadcast(@pubsub, "oad_build:#{name}", {:oad_build_done, name, status})
  end

  defp format_error({:step_failed, label, code, stderr}),
    do: "#{label} failed (exit #{code}): #{String.slice(to_string(stderr), 0, 500)}"

  defp format_error({:boot_failed, reason}),
    do: "builder sandbox failed to start: #{String.slice(to_string(reason), 0, 500)}"

  defp format_error({:boot_timeout, ms}),
    do: "builder sandbox did not start within #{div(ms, 60_000)} min"

  defp format_error({:unexpected_create_response, other}),
    do: "unexpected oad response: #{inspect(other)}"

  defp format_error({:http, status, message}), do: "oad #{status}: #{message}"
  defp format_error({:transport, :timeout}), do: "oad timed out (no response)"
  defp format_error({:transport, :connect_timeout}), do: "oad unreachable (connect timed out)"
  defp format_error({:transport, reason}), do: "oad unreachable: #{inspect(reason)}"
  defp format_error(other), do: inspect(other)
end
