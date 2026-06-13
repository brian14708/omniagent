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
      from(c in Chunk, where: c.hash in ^hashes and c.refcount > 0, select: c.hash)
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
            chunk_count: length(hashes),
            chunk_hashes: hashes
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

  @grace_seconds 86_400

  @doc """
  Unregisters a snapshot: decrements the refcount of each chunk it referenced and
  deletes the snapshot row, in one transaction. Chunks that reach refcount 0
  become collectable (after the grace window). A no-op for an unknown snapshot.
  """
  def unregister_snapshot(snapshot_name) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    Repo.transaction(fn ->
      case Repo.get_by(Snapshot, snapshot_name: snapshot_name) do
        nil ->
          :ok

        %Snapshot{chunk_hashes: hashes} = snapshot ->
          if hashes != [] do
            from(c in Chunk, where: c.hash in ^hashes)
            |> Repo.update_all(inc: [refcount: -1], set: [updated_at: now])
          end

          Repo.delete!(snapshot)
          :ok
      end
    end)
    |> case do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Hashes of chunks eligible for garbage collection: refcount has reached 0 and
  stayed there past the grace window (so a quick rebuild re-referencing them does
  not race collection).
  """
  def collectable_chunks(grace_seconds \\ @grace_seconds) do
    cutoff = DateTime.utc_now() |> DateTime.add(-grace_seconds, :second)

    from(c in Chunk, where: c.refcount <= 0 and c.updated_at < ^cutoff, select: c.hash)
    |> Repo.all()
  end

  @doc """
  Garbage-collects unreferenced chunks: deletes the collectable objects from the
  store via `delete_fn`, then removes their index rows (re-checking refcount, so a
  chunk re-referenced in the meantime is kept). `delete_fn` receives the list of
  hashes and returns `:ok` or `{:error, reason}`; it defaults to deleting from the
  configured CAS bucket. Returns `{:ok, deleted_count}`.
  """
  def gc(grace_seconds \\ @grace_seconds, delete_fn \\ &delete_from_store/1) do
    case collectable_chunks(grace_seconds) do
      [] ->
        {:ok, 0}

      hashes ->
        case delete_fn.(hashes) do
          :ok ->
            {count, _} =
              from(c in Chunk, where: c.hash in ^hashes and c.refcount <= 0)
              |> Repo.delete_all()

            {:ok, count}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  # Deletes chunk objects from the configured CAS bucket (S3-compatible RustFS).
  defp delete_from_store(hashes) do
    bucket = Application.get_env(:omniagent, :cas_bucket)
    prefix = Application.get_env(:omniagent, :cas_prefix, "")

    if is_binary(bucket) and bucket != "" do
      keys = Enum.map(hashes, &chunk_object_key(prefix, &1))

      case ExAws.S3.delete_all_objects(bucket, keys) |> ExAws.request() do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    else
      # No CAS bucket configured: nothing to delete from object storage; let the
      # index rows be removed so accounting stays consistent in dev/test.
      :ok
    end
  end

  defp chunk_object_key(prefix, hash) do
    fanout = String.slice(hash, 0, 2)
    base = "chunks/blake3/#{fanout}/#{hash}"
    prefix = String.trim(prefix, "/")
    if prefix == "", do: base, else: "#{prefix}/#{base}"
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
