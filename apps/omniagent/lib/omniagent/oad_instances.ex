defmodule Omniagent.OadInstances do
  @moduledoc """
  Registry of oad sandbox daemons that have registered with this control plane.

  oad daemons heartbeat to `POST /api/oad/register`; each beat upserts a row and
  refreshes `last_seen_at`. The control plane then calls a live instance's `/v1`
  API directly (see `Omniagent.Oad.Client`). Liveness is `last_seen_at` within
  `stale_after/0`; stale rows are filtered from `list_live/0` and can be pruned.

  DB-backed (rather than a `:pg` registry like `Omniagent.Daemons`) because
  registration arrives over HTTP and can land on any node — the database is the
  natural cluster-wide store, and any node can call an instance over HTTP.
  """

  import Ecto.Query

  alias Omniagent.OadInstances.OadInstance
  alias Omniagent.Repo

  # An instance is considered offline after this many seconds without a beat
  # (oad beats every ~15s, so this tolerates a few missed beats).
  @stale_after_seconds 60

  def stale_after, do: @stale_after_seconds

  @doc """
  Upserts an instance from a registration heartbeat. Refreshes `last_seen_at`
  and replaces any prior instance advertising the same `base_url` (so a restarted
  oad — which reports a fresh `instance_id` — cleanly supersedes its old row).
  """
  def register(attrs) do
    attrs = normalize(attrs)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    attrs = Map.put(attrs, :last_seen_at, now)

    Repo.transaction(fn ->
      if base_url = attrs[:base_url] do
        from(i in OadInstance,
          where: i.base_url == ^base_url and i.instance_id != ^attrs[:instance_id]
        )
        |> Repo.delete_all()
      end

      %OadInstance{}
      |> OadInstance.changeset(attrs)
      |> Repo.insert(
        on_conflict:
          {:replace,
           [
             :name,
             :base_url,
             :api_token,
             :capabilities,
             :version,
             :last_seen_at,
             :alloc_cpu_millis,
             :alloc_memory_bytes,
             :alloc_disk_bytes,
             :labels,
             :status,
             :warm_snapshots,
             :updated_at
           ]},
        conflict_target: :instance_id
      )
      |> case do
        {:ok, instance} -> instance
        {:error, changeset} -> Repo.rollback(changeset)
      end
    end)
  end

  @doc "Removes an instance by its reported `instance_id`. Returns the count."
  def deregister(instance_id) do
    {count, _} =
      from(i in OadInstance, where: i.instance_id == ^instance_id) |> Repo.delete_all()

    count
  end

  @doc "Lists instances seen within the staleness window."
  def list_live do
    from(i in OadInstance, where: i.last_seen_at > ^cutoff(), order_by: [asc: i.name])
    |> Repo.all()
  end

  @doc "Fetches a live instance by its database id or reported `instance_id`."
  def get_live(id) when is_binary(id) do
    from(i in OadInstance,
      where: (i.id == ^id or i.instance_id == ^id) and i.last_seen_at > ^cutoff()
    )
    |> Repo.one()
  rescue
    # `i.id == ^id` raises if `id` is not a valid UUID; fall back to instance_id.
    Ecto.Query.CastError ->
      from(i in OadInstance, where: i.instance_id == ^id and i.last_seen_at > ^cutoff())
      |> Repo.one()
  end

  @doc "Deletes instances that have not beaten within the staleness window."
  def prune_stale do
    {count, _} = from(i in OadInstance, where: i.last_seen_at < ^cutoff()) |> Repo.delete_all()
    count
  end

  defp cutoff, do: DateTime.utc_now() |> DateTime.add(-@stale_after_seconds, :second)

  # Accepts string- or atom-keyed attrs (controller passes string keys).
  defp normalize(attrs) do
    %{
      instance_id: fetch(attrs, :instance_id),
      name: fetch(attrs, :name),
      base_url: fetch(attrs, :base_url),
      api_token: fetch(attrs, :api_token),
      capabilities: fetch(attrs, :capabilities) || %{},
      version: fetch(attrs, :version),
      alloc_cpu_millis: fetch(attrs, :alloc_cpu_millis) || 0,
      alloc_memory_bytes: fetch(attrs, :alloc_memory_bytes) || 0,
      alloc_disk_bytes: fetch(attrs, :alloc_disk_bytes) || 0,
      labels: fetch(attrs, :labels) || %{},
      status: fetch(attrs, :status) || "active",
      warm_snapshots: fetch(attrs, :warm_snapshots) || []
    }
  end

  defp fetch(attrs, key) do
    Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))
  end
end
