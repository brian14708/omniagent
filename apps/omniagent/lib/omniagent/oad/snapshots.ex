defmodule Omniagent.Oad.Snapshots do
  @moduledoc """
  Registry of snapshots published to the content-addressed store (CAS), and the
  reference counts of the chunks they hold.

  When a workspace build captures a snapshot, the oad daemon chunks its
  checkpoint images into the CAS and returns a descriptor key plus the chunk
  hashes (see `Omniagent.Oad.Builder`). `register/1` records the snapshot and
  increments each referenced chunk's refcount in one transaction, so a chunk
  shared across snapshot revisions is counted once per referencing snapshot.
  `missing_chunks/1` backs the daemon's batched existence check
  (`POST /api/oad/cas/check`); a later GC phase collects chunks whose refcount
  reaches zero.
  """

  import Ecto.Query

  alias Ecto.Multi
  alias Omniagent.Oad.Snapshots.{Chunk, Snapshot}
  alias Omniagent.Repo

  @doc """
  Returns the subset of `hashes` not yet present in the chunk index, preserving
  input order, so the daemon uploads only the chunks the store is missing.
  """
  def missing_chunks(hashes) when is_list(hashes) do
    present =
      from(c in Chunk, where: c.hash in ^hashes, select: c.hash)
      |> Repo.all()
      |> MapSet.new()

    Enum.reject(hashes, &MapSet.member?(present, &1))
  end

  @doc "Fetches a registered snapshot by its name, or `nil`."
  def get_by_name(snapshot_name) do
    Repo.get_by(Snapshot, snapshot_name: snapshot_name)
  end

  @doc """
  Registers a published snapshot and reference-counts its chunks in one
  transaction.

  Inserts the snapshot row and, only when it is newly created, increments each
  referenced chunk's refcount (inserting unseen chunks at refcount 1). Because
  the increment is gated on first registration, retrying an already-registered
  snapshot is a no-op for refcounts and never double-counts.

  Accepts string- or atom-keyed attrs with `snapshot_name`, `descriptor_key`,
  optional `workspace_name`/`total_bytes`, and `chunk_hashes`.
  """
  def register(attrs) do
    attrs = normalize(attrs)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    hashes = Enum.uniq(attrs.chunk_hashes)

    Multi.new()
    |> Multi.run(:existing, fn repo, _ ->
      {:ok, repo.get_by(Snapshot, snapshot_name: attrs.snapshot_name)}
    end)
    |> Multi.run(:snapshot, fn repo, %{existing: existing} ->
      case existing do
        nil ->
          %Snapshot{}
          |> Snapshot.changeset(%{
            snapshot_name: attrs.snapshot_name,
            workspace_name: attrs.workspace_name,
            descriptor_key: attrs.descriptor_key,
            total_bytes: attrs.total_bytes,
            chunk_count: length(hashes)
          })
          |> repo.insert()

        snapshot ->
          {:ok, snapshot}
      end
    end)
    |> Multi.run(:chunks, fn repo, %{existing: existing} ->
      if is_nil(existing) do
        {:ok, increment_chunks(repo, hashes, now)}
      else
        {:ok, 0}
      end
    end)
    |> Repo.transaction()
    |> case do
      {:ok, %{snapshot: snapshot}} -> {:ok, snapshot}
      {:error, _step, reason, _changes} -> {:error, reason}
    end
  end

  # Bulk-upserts chunk refcounts: inserts unseen chunks at refcount 1 and
  # atomically increments the refcount of chunks already present.
  defp increment_chunks(_repo, [], _now), do: 0

  defp increment_chunks(repo, hashes, now) do
    entries =
      Enum.map(hashes, fn hash ->
        %{hash: hash, refcount: 1, first_seen_at: now, inserted_at: now, updated_at: now}
      end)

    {count, _} =
      repo.insert_all(Chunk, entries,
        on_conflict: [inc: [refcount: 1]],
        conflict_target: :hash
      )

    count
  end

  defp normalize(attrs) do
    %{
      snapshot_name: fetch(attrs, :snapshot_name),
      workspace_name: fetch(attrs, :workspace_name),
      descriptor_key: fetch(attrs, :descriptor_key),
      total_bytes: fetch(attrs, :total_bytes) || 0,
      chunk_hashes: fetch(attrs, :chunk_hashes) || []
    }
  end

  defp fetch(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))
end
