defmodule Omniagent.Oad.Client do
  @moduledoc """
  HTTP client for a registered oad instance's `/v1` API.

  Thin wrapper over `:hackney` (already a dependency) that authenticates with the
  instance's `api_token` and (de)serializes JSON. The control plane uses this to
  build workspaces (create builder sandbox, exec, snapshot) and run sessions
  (fork from a snapshot, start a background exec) directly against the oad
  instance that registered. SSE event streaming (`/execs/:id/events`) is added
  with the build/reap pipeline.

  Each call returns `{:ok, decoded_json}` for 2xx, `{:error, {:http, status,
  message}}` for a daemon rejection, or `{:error, {:transport, reason}}` when the
  request cannot be made.
  """

  alias Omniagent.OadInstances.OadInstance

  require Logger

  @timeout 30_000
  # The snapshot (checkpoint commit) is the remaining synchronous, potentially
  # slow call — sandbox creation is now asynchronous (the daemon returns 202 and
  # boots in the background; see `Omniagent.Oad.Builder`). Give snapshot a
  # generous timeout, overridable via config.
  @snapshot_timeout 600_000
  # Max gap between SSE chunks while streaming a background exec's output. The
  # daemon keep-alives every 15s, so 60s tolerates idle steps without hanging.
  @stream_idle_timeout 60_000

  def create(instance, body), do: request(instance, :post, "/v1/sandboxes", body)
  def get_sandbox(instance, id), do: request(instance, :get, "/v1/sandboxes/#{id}", nil)
  def delete_sandbox(instance, id), do: request(instance, :delete, "/v1/sandboxes/#{id}", nil)
  def suspend(instance, id), do: request(instance, :post, "/v1/sandboxes/#{id}/suspend", nil)
  def resume(instance, id), do: request(instance, :post, "/v1/sandboxes/#{id}/resume", nil)
  def network(instance, id), do: request(instance, :get, "/v1/sandboxes/#{id}/network", nil)

  def exec(instance, id, body), do: request(instance, :post, "/v1/sandboxes/#{id}/exec", body)

  def start_exec(instance, id, body),
    do: request(instance, :post, "/v1/sandboxes/#{id}/execs", body)

  def get_exec(instance, id, exec_id),
    do: request(instance, :get, "/v1/sandboxes/#{id}/execs/#{exec_id}", nil)

  def kill_exec(instance, id, exec_id),
    do: request(instance, :delete, "/v1/sandboxes/#{id}/execs/#{exec_id}", nil)

  def write_stdin(instance, id, exec_id, body),
    do: request(instance, :post, "/v1/sandboxes/#{id}/execs/#{exec_id}/stdin", body)

  def snapshot(instance, id, body),
    do:
      request(instance, :post, "/v1/sandboxes/#{id}/snapshot", body, timeout: snapshot_timeout())

  def list_snapshots(instance), do: request(instance, :get, "/v1/snapshots", nil)

  def delete_snapshot(instance, name),
    do: request(instance, :delete, "/v1/snapshots/#{name}", nil)

  @doc """
  Streams a background exec's SSE event feed (`/execs/:id/events`), folding each
  decoded event into `acc` via `reducer.(event, acc)` until a terminal
  (`"exited"`/`"failed"`) event arrives or the stream closes.

  Each `event` is the decoded JSON map (`"type"`, `"data"` base64 for
  stdout/stderr, `"exit_code"`, `"message"`). Returns `{:ok, {terminal_event,
  acc}}`, or `{:error, reason}` on transport/HTTP failure or an unexpected
  close. Replays from the first event so output emitted before subscribing is
  not lost.
  """
  def stream_exec_events(instance, sandbox_id, exec_id, acc, reducer)

  def stream_exec_events(
        %OadInstance{base_url: base, api_token: token},
        sandbox_id,
        exec_id,
        acc,
        reducer
      ) do
    url =
      String.trim_trailing(base, "/") <>
        "/v1/sandboxes/#{sandbox_id}/execs/#{exec_id}/events?from=1"

    headers = [
      {"authorization", "Bearer " <> token},
      {"accept", "text/event-stream"}
    ]

    # hackney 4.x buffers the whole body for a normal request, so an SSE stream
    # must use async mode: it returns {:ok, ref} and delivers the response as
    # {:hackney_response, ref, ...} messages to this process. recv_timeout bounds
    # the gap between chunks (the daemon keep-alives every 15s).
    opts = [recv_timeout: @stream_idle_timeout, connect_timeout: @timeout, async: true]

    case :hackney.request(:get, url, headers, "", opts) do
      {:ok, ref} ->
        receive_sse(ref, %{status: nil, buffer: "", acc: acc}, reducer)

      {:error, reason} ->
        {:error, {:transport, reason}}
    end
  end

  defp receive_sse(ref, state, reducer) do
    receive do
      {:hackney_response, ^ref, {:status, status, _reason}} ->
        receive_sse(ref, %{state | status: status}, reducer)

      {:hackney_response, ^ref, {:headers, _headers}} ->
        receive_sse(ref, state, reducer)

      {:hackney_response, ^ref, chunk} when is_binary(chunk) ->
        handle_sse_chunk(ref, state, reducer, chunk)

      {:hackney_response, ^ref, :done} ->
        if state.status in 200..299 do
          {:error, :stream_closed}
        else
          decode(state.status || 0, state.buffer)
        end

      {:hackney_response, ^ref, {:error, reason}} ->
        {:error, {:transport, reason}}

      # Ignore anything else for this stream (e.g. redirects) and keep waiting.
      {:hackney_response, ^ref, _other} ->
        receive_sse(ref, state, reducer)
    after
      @stream_idle_timeout ->
        stop_async(ref)
        {:error, {:transport, :timeout}}
    end
  end

  defp handle_sse_chunk(ref, %{status: status} = state, reducer, chunk)
       when status in 200..299 do
    {frames, rest} = split_sse_frames(state.buffer <> chunk)

    case apply_sse_frames(frames, state.acc, reducer) do
      {:terminal, event, acc} ->
        stop_async(ref)
        {:ok, {event, acc}}

      {:continue, acc} ->
        receive_sse(ref, %{state | buffer: rest, acc: acc}, reducer)
    end
  end

  # Non-2xx: accumulate the body so :done can surface it as an HTTP error.
  defp handle_sse_chunk(ref, state, reducer, chunk) do
    receive_sse(ref, %{state | buffer: state.buffer <> chunk}, reducer)
  end

  # Aborts the async response and drains any messages still queued for it so they
  # don't linger in the caller's mailbox (each step opens a fresh stream).
  defp stop_async(ref) do
    _ = :hackney.stop_async(ref)
    flush_async(ref)
  end

  defp flush_async(ref) do
    receive do
      {:hackney_response, ^ref, _} -> flush_async(ref)
    after
      0 -> :ok
    end
  end

  # Splits an SSE buffer on the blank-line frame delimiter, returning the
  # complete frames and any trailing partial frame to carry into the next chunk.
  defp split_sse_frames(buffer) do
    parts = String.split(buffer, "\n\n")
    {complete, rest} = Enum.split(parts, -1)
    {complete, List.first(rest) || ""}
  end

  defp apply_sse_frames(frames, acc, reducer) do
    Enum.reduce_while(frames, {:continue, acc}, fn frame, {:continue, acc} ->
      case decode_sse_event(frame) do
        nil ->
          {:cont, {:continue, acc}}

        event ->
          acc = reducer.(event, acc)

          if event["type"] in ["exited", "failed"] do
            {:halt, {:terminal, event, acc}}
          else
            {:cont, {:continue, acc}}
          end
      end
    end)
  end

  # Extracts the `data:` payload of an SSE frame and decodes it as JSON. Returns
  # nil for keep-alive comments and frames without a JSON data line.
  defp decode_sse_event(frame) do
    data =
      frame
      |> String.split("\n")
      |> Enum.flat_map(fn line ->
        case String.trim_trailing(line, "\r") do
          "data:" <> rest -> [String.trim_leading(rest, " ")]
          _ -> []
        end
      end)
      |> Enum.join("\n")

    case data do
      "" ->
        nil

      json ->
        case Jason.decode(json) do
          {:ok, event} -> event
          {:error, _} -> nil
        end
    end
  end

  defp snapshot_timeout do
    Application.get_env(:omniagent, :oad_snapshot_timeout, @snapshot_timeout)
  end

  defp request(instance, method, path, body, opts \\ [])

  defp request(%OadInstance{base_url: base, api_token: token}, method, path, body, opts) do
    url = String.trim_trailing(base, "/") <> path
    timeout = Keyword.get(opts, :timeout, @timeout)

    headers = [
      {"authorization", "Bearer " <> token},
      {"content-type", "application/json"}
    ]

    payload = if is_nil(body), do: "", else: Jason.encode!(body)
    req_opts = [recv_timeout: timeout, connect_timeout: @timeout]

    Logger.debug("oad #{method} #{url} (timeout #{timeout}ms)")
    started = System.monotonic_time(:millisecond)

    # hackney 4.x returns the full response body directly as the 4th element
    # (a binary); `with_body`/`:hackney.body/1` are gone.
    case :hackney.request(method, url, headers, payload, req_opts) do
      {:ok, status, _resp_headers, resp_body} ->
        elapsed = System.monotonic_time(:millisecond) - started
        Logger.debug("oad #{method} #{url} -> #{status} (#{elapsed}ms)")
        decode(status, resp_body)

      {:ok, status, _resp_headers} ->
        elapsed = System.monotonic_time(:millisecond) - started
        Logger.debug("oad #{method} #{url} -> #{status} (#{elapsed}ms, no body)")
        decode(status, "")

      {:error, reason} ->
        elapsed = System.monotonic_time(:millisecond) - started

        Logger.warning(
          "oad #{method} #{url} transport error after #{elapsed}ms: #{inspect(reason)}"
        )

        {:error, {:transport, reason}}
    end
  end

  defp decode(status, "") when status in 200..299, do: {:ok, %{}}

  defp decode(status, body) when status in 200..299 do
    case Jason.decode(body) do
      {:ok, json} -> {:ok, json}
      {:error, _} -> {:ok, %{"raw" => body}}
    end
  end

  defp decode(status, body) do
    message =
      case Jason.decode(body) do
        {:ok, %{"error" => %{"message" => message}}} -> message
        {:ok, other} -> inspect(other)
        _ -> body
      end

    {:error, {:http, status, message}}
  end
end
