defmodule OmniagentWeb.ArtifactController do
  @moduledoc """
  Receives session artifacts (ATIF trajectory, terminal recording) over HTTP and
  stores them in S3-compatible object storage via `Omniagent.Artifacts`.

  The body is the raw artifact bytes (`application/octet-stream`); the artifact
  kind comes from the `X-Artifact-Kind` header. Only the session's owner may
  upload to it.
  """

  use OmniagentWeb, :controller

  alias Omniagent.{Accounts, Artifacts, Sessions}

  # Bounds a single artifact upload (asciicast recordings can be large).
  @max_body_bytes 64 * 1024 * 1024

  def create(conn, %{"session_id" => session_id}) do
    with {:ok, _session} <- authorize_session(conn, session_id),
         {:ok, kind} <- fetch_kind(conn),
         {:ok, body, conn} <- read_full_body(conn),
         {:ok, artifact} <- Artifacts.store_artifact(session_id, kind, body) do
      conn
      |> put_status(:created)
      |> json(%{id: artifact.id, key: artifact.key, size: artifact.size})
    else
      {:error, :not_found} -> error(conn, :not_found, "session not found")
      {:error, :missing_kind} -> error(conn, :bad_request, "missing X-Artifact-Kind header")
      {:error, :too_large} -> error(conn, :request_entity_too_large, "artifact too large")
      {:error, reason} -> error(conn, :unprocessable_entity, inspect(reason))
    end
  end

  @doc """
  Streams a stored artifact back to the browser as a file download.

  Reached via the browser pipeline (not the bearer-token API), so it authorizes
  against the console's `default_user` — mirroring how `OmniagentWeb.ConsoleLive`
  scopes sessions — rather than `conn.assigns.current_user`.
  """
  def download(conn, %{"session_id" => session_id, "id" => id}) do
    with {:ok, _session} <- authorize_browser_session(session_id),
         %Artifacts.Artifact{} = artifact <- Artifacts.get_artifact(session_id, id),
         {:ok, body} <- Artifacts.download_artifact(artifact) do
      conn
      |> put_resp_content_type(artifact.content_type || "application/octet-stream")
      |> put_resp_header(
        "content-disposition",
        ~s(attachment; filename="#{Path.basename(artifact.key)}")
      )
      |> send_resp(200, body)
    else
      nil -> conn |> put_status(:not_found) |> text("artifact not found")
      {:error, :not_found} -> conn |> put_status(:not_found) |> text("session not found")
      {:error, reason} -> conn |> put_status(:unprocessable_entity) |> text(inspect(reason))
    end
  end

  defp authorize_session(conn, session_id) do
    case Sessions.get_user_session(conn.assigns.current_user.id, session_id) do
      nil -> {:error, :not_found}
      session -> {:ok, session}
    end
  end

  defp authorize_browser_session(session_id) do
    with %{id: user_id} <- Accounts.default_user(),
         session when not is_nil(session) <- Sessions.get_user_session(user_id, session_id) do
      {:ok, session}
    else
      _ -> {:error, :not_found}
    end
  end

  defp fetch_kind(conn) do
    case get_req_header(conn, "x-artifact-kind") do
      [kind | _] when kind != "" -> {:ok, kind}
      _ -> {:error, :missing_kind}
    end
  end

  # Reads the entire request body as an iolist, rejecting once the accumulated
  # size exceeds @max_body_bytes. The cap is checked on every chunk — partial
  # and final alike — so a body whose last read completes over the limit is not
  # silently accepted.
  defp read_full_body(conn, acc \\ []) do
    case read_body(conn, length: @max_body_bytes) do
      {:ok, chunk, conn} ->
        acc = [acc, chunk]

        if IO.iodata_length(acc) > @max_body_bytes,
          do: {:error, :too_large},
          else: {:ok, IO.iodata_to_binary(acc), conn}

      {:more, chunk, conn} ->
        acc = [acc, chunk]

        if IO.iodata_length(acc) > @max_body_bytes,
          do: {:error, :too_large},
          else: read_full_body(conn, acc)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp error(conn, status, message) do
    conn
    |> put_status(status)
    |> json(%{error: message})
  end
end
