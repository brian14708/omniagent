defmodule Omniagent.Artifacts do
  @moduledoc """
  Session artifacts (ATIF trajectories, terminal recordings) persisted to
  S3-compatible object storage (RustFS) with a row in the `artifacts` table.
  """

  import Ecto.Query

  alias Omniagent.Artifacts.Artifact
  alias Omniagent.Events
  alias Omniagent.Repo

  def list_artifacts(session_id) do
    Artifact
    |> where([artifact], artifact.agent_session_id == ^session_id)
    |> order_by([artifact], asc: artifact.inserted_at)
    |> Repo.all()
  end

  def get_artifact(session_id, id) do
    Artifact
    |> where([artifact], artifact.agent_session_id == ^session_id and artifact.id == ^id)
    |> Repo.one()
  end

  @doc """
  Fetches an artifact's stored object bytes from object storage.

  Returns `{:ok, binary}`, or `{:error, {:storage, reason}}` if the object cannot
  be read. Artifacts are bounded by the upload cap, so reading the whole object
  into memory mirrors how `store_artifact/3` writes it.
  """
  def download_artifact(%Artifact{bucket: bucket, key: key}) do
    case bucket |> ExAws.S3.get_object(key) |> ExAws.request() do
      {:ok, %{body: body}} -> {:ok, body}
      {:error, reason} -> {:error, {:storage, reason}}
    end
  end

  @doc """
  Uploads `binary` to object storage and records an artifact row.

  The object key is `sessions/<session_id>/<kind>-<uuid>.<ext>`. Returns
  `{:ok, %Artifact{}}`, or `{:error, reason}` if the upload or the insert fails
  (the row is only written after the object lands in storage).
  """
  def store_artifact(session_id, kind, binary) when is_binary(binary) do
    {content_type, ext} = kind_meta(kind)
    bucket = bucket()
    key = "sessions/#{session_id}/#{kind}-#{Ecto.UUID.generate()}.#{ext}"

    with :ok <- put_object(bucket, key, binary, content_type),
         {:ok, artifact} <-
           insert_artifact(session_id, kind, bucket, key, content_type, binary) do
      Events.broadcast(session_id, {:artifact_added, artifact})
      {:ok, artifact}
    end
  end

  defp put_object(bucket, key, binary, content_type) do
    opts = if content_type, do: [content_type: content_type], else: []

    case bucket |> ExAws.S3.put_object(key, binary, opts) |> ExAws.request() do
      {:ok, _response} -> :ok
      {:error, reason} -> {:error, {:storage, reason}}
    end
  end

  defp insert_artifact(session_id, kind, bucket, key, content_type, binary) do
    %Artifact{}
    |> Artifact.changeset(%{
      agent_session_id: session_id,
      kind: kind,
      bucket: bucket,
      key: key,
      content_type: content_type,
      size: byte_size(binary),
      checksum: checksum(binary)
    })
    |> Repo.insert()
  end

  # The content type and object-key extension for each artifact kind. This is the
  # single authority on per-kind metadata.
  defp kind_meta("trajectory"), do: {"application/json", "json"}
  defp kind_meta("recording"), do: {"application/x-asciicast", "cast"}
  defp kind_meta("raw_requests"), do: {"application/x-ndjson", "bin"}
  defp kind_meta("session_log"), do: {"application/x-ndjson", "jsonl"}
  defp kind_meta(_kind), do: {"application/octet-stream", "bin"}

  defp checksum(binary), do: :crypto.hash(:sha256, binary) |> Base.encode16(case: :lower)

  defp bucket, do: Application.get_env(:omniagent, :artifacts_bucket, "omniagent-artifacts")
end
