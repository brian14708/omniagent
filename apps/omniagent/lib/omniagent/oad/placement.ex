defmodule Omniagent.Oad.Placement do
  @moduledoc """
  Transactional capacity placement for oad work (sessions and builds).

  `acquire/1` ranks live instances with `Omniagent.Oad.Scheduler`, then commits
  capacity on the best candidate in a single transaction: a conditional
  `UPDATE ... WHERE alloc - committed >= req` makes the check-and-decrement
  atomic, so two schedulers racing for the same node cannot both win — the loser
  sees zero rows updated and falls through to the next candidate. Committed
  capacity is held by an `Omniagent.Oad.Placement.Lease` row and returned by
  `release/1` (called by the session reaper) or reclaimed by `reap_expired/1`
  (a backstop for dead nodes / lost reapers).

  This is the cluster-wide serialization point: the control plane runs on
  multiple BEAM nodes, so placement must arbitrate through the database, not
  in-memory state (the same reasoning `Omniagent.OadInstances` documents).
  """

  import Ecto.Query

  alias Omniagent.Oad.Placement.Lease
  alias Omniagent.Oad.Scheduler
  alias Omniagent.OadInstances
  alias Omniagent.OadInstances.OadInstance
  alias Omniagent.Repo

  @lease_seconds 3600
  @max_attempts 5

  @doc """
  Acquires capacity for `request` on the best live instance, returning
  `{:ok, instance, lease}`. Returns `{:error, :no_capacity}` when no admissible
  instance has room, or `{:error, :contended}` if it loses too many races.
  """
  def acquire(request) do
    candidates = OadInstances.list_live()

    candidates
    |> Scheduler.rank(request)
    |> acquire_first(request, @max_attempts)
  end

  defp acquire_first([], _request, _attempts), do: {:error, :no_capacity}
  defp acquire_first(_ranked, _request, 0), do: {:error, :contended}

  defp acquire_first([instance | rest], request, attempts) do
    case try_commit(instance, request) do
      {:ok, lease} -> {:ok, instance, lease}
      :lost -> acquire_first(rest, request, attempts - 1)
      {:error, reason} -> {:error, reason}
    end
  end

  defp try_commit(instance, request) do
    cpu = Map.get(request, :cpu_millis, 0)
    mem = Map.get(request, :memory_bytes, 0)
    disk = Map.get(request, :disk_bytes, 0)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    expires = DateTime.add(now, @lease_seconds, :second)

    Repo.transaction(fn ->
      {count, _} =
        from(i in OadInstance,
          where:
            i.id == ^instance.id and
              (i.alloc_cpu_millis == 0 or
                 i.alloc_cpu_millis - i.committed_cpu_millis >= ^cpu) and
              (i.alloc_memory_bytes == 0 or
                 i.alloc_memory_bytes - i.committed_memory_bytes >= ^mem) and
              (i.alloc_disk_bytes == 0 or
                 i.alloc_disk_bytes - i.committed_disk_bytes >= ^disk)
        )
        |> Repo.update_all(
          inc: [
            committed_cpu_millis: cpu,
            committed_memory_bytes: mem,
            committed_disk_bytes: disk
          ]
        )

      if count == 1 do
        %Lease{}
        |> Lease.changeset(%{
          instance_db_id: instance.id,
          kind: Map.get(request, :kind, "session"),
          workspace: Map.get(request, :workspace),
          session_id: Map.get(request, :session_id),
          req_cpu_millis: cpu,
          req_memory_bytes: mem,
          req_disk_bytes: disk,
          state: "assigned",
          lease_expires_at: expires
        })
        |> Repo.insert()
        |> case do
          {:ok, lease} -> lease
          {:error, changeset} -> Repo.rollback(changeset)
        end
      else
        Repo.rollback(:lost)
      end
    end)
    |> case do
      {:ok, lease} -> {:ok, lease}
      {:error, :lost} -> :lost
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Releases a lease, crediting its committed capacity back to the instance.
  Idempotent: releasing an already-released or missing lease is a no-op.
  """
  def release(lease_id) when is_binary(lease_id) do
    Repo.transaction(fn ->
      case Repo.get(Lease, lease_id) do
        %Lease{state: "assigned"} = lease ->
          from(i in OadInstance, where: i.id == ^lease.instance_db_id)
          |> Repo.update_all(
            inc: [
              committed_cpu_millis: -lease.req_cpu_millis,
              committed_memory_bytes: -lease.req_memory_bytes,
              committed_disk_bytes: -lease.req_disk_bytes
            ]
          )

          lease |> Lease.changeset(%{state: "released"}) |> Repo.update!()
          :ok

        _ ->
          :ok
      end
    end)
    |> case do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def release(_), do: :ok

  @doc """
  Reclaims capacity from `assigned` leases whose deadline has passed — a backstop
  for nodes that died or reapers that were lost. Returns the count reclaimed.
  """
  def reap_expired(now \\ nil) do
    now = now || DateTime.utc_now()

    expired =
      from(l in Lease, where: l.state == "assigned" and l.lease_expires_at < ^now, select: l.id)
      |> Repo.all()

    Enum.each(expired, &release/1)
    length(expired)
  end

  @doc """
  Builds a placement request from a workspace's `resources` map (CPU in cores,
  memory as a size string/number), merging in `extra` keys such as
  `:snapshot_name`, `:kind`, `:workspace`, and `:session_id`.
  """
  def request_from_resources(resources, extra \\ %{}) when is_map(resources) do
    cpu = Map.get(resources, "cpu") || Map.get(resources, :cpu)
    mem = Map.get(resources, "memory") || Map.get(resources, :memory)

    %{
      cpu_millis: parse_cpu_millis(cpu),
      memory_bytes: parse_bytes(mem),
      disk_bytes: 0
    }
    |> Map.merge(Map.new(extra))
  end

  # CPU entered as a (possibly fractional) core count -> millicores.
  defp parse_cpu_millis(value) when is_number(value) and value > 0, do: round(value * 1000)

  defp parse_cpu_millis(value) when is_binary(value) do
    case Float.parse(String.trim(value)) do
      {cores, _} when cores > 0 -> round(cores * 1000)
      _ -> 0
    end
  end

  defp parse_cpu_millis(_), do: 0

  # Memory as bytes: bare integer = MB; suffixed Ki/Mi/Gi (binary) or K/M/G/B.
  defp parse_bytes(value) when is_integer(value) and value > 0, do: value * 1_048_576

  defp parse_bytes(value) when is_binary(value) do
    case Regex.run(~r/^(\d+(?:\.\d+)?)\s*([a-zA-Z]*)$/, String.trim(value)) do
      [_, num, suffix] ->
        case Float.parse(num) do
          {n, _} when n > 0 -> round(n * memory_multiplier(String.downcase(suffix)))
          _ -> 0
        end

      _ ->
        0
    end
  end

  defp parse_bytes(_), do: 0

  defp memory_multiplier("gi"), do: 1024 * 1024 * 1024
  defp memory_multiplier("mi"), do: 1024 * 1024
  defp memory_multiplier("ki"), do: 1024
  defp memory_multiplier("g"), do: 1_000_000_000
  defp memory_multiplier("m"), do: 1_000_000
  defp memory_multiplier("k"), do: 1_000
  defp memory_multiplier("b"), do: 1
  defp memory_multiplier(_), do: 1_048_576
end
