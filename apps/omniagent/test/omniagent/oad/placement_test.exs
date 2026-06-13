defmodule Omniagent.Oad.PlacementTest do
  use Omniagent.DataCase, async: true

  alias Omniagent.Oad.Placement
  alias Omniagent.Oad.Placement.Lease
  alias Omniagent.OadInstances.OadInstance
  alias Omniagent.Repo

  defp insert_instance(attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    base = %{
      instance_id: "i-#{System.unique_integer([:positive])}",
      base_url: "http://#{System.unique_integer([:positive])}:8080",
      api_token: "t",
      last_seen_at: now,
      status: "active"
    }

    %OadInstance{}
    |> OadInstance.changeset(Map.merge(base, Map.new(attrs)))
    |> Repo.insert!()
  end

  test "acquire commits capacity; release returns it" do
    inst = insert_instance(alloc_cpu_millis: 4000, alloc_memory_bytes: 8_000_000_000)

    {:ok, chosen, lease} = Placement.acquire(%{cpu_millis: 1000, memory_bytes: 2_000_000_000})
    assert chosen.id == inst.id

    reloaded = Repo.get(OadInstance, inst.id)
    assert reloaded.committed_cpu_millis == 1000
    assert reloaded.committed_memory_bytes == 2_000_000_000

    assert :ok = Placement.release(lease.id)
    back = Repo.get(OadInstance, inst.id)
    assert back.committed_cpu_millis == 0
    assert back.committed_memory_bytes == 0
  end

  test "returns :no_capacity when no live node has room" do
    insert_instance(alloc_cpu_millis: 1000, committed_cpu_millis: 1000)
    assert {:error, :no_capacity} = Placement.acquire(%{cpu_millis: 2000})
  end

  test "selects a node with capacity when the first is full" do
    _full = insert_instance(name: "full", alloc_cpu_millis: 1000, committed_cpu_millis: 1000)
    free = insert_instance(name: "free", alloc_cpu_millis: 4000)

    {:ok, chosen, _lease} = Placement.acquire(%{cpu_millis: 2000})
    assert chosen.id == free.id
  end

  test "release is idempotent and tolerates unknown leases" do
    insert_instance(alloc_cpu_millis: 4000)
    {:ok, _inst, lease} = Placement.acquire(%{cpu_millis: 1000})

    assert :ok = Placement.release(lease.id)
    assert :ok = Placement.release(lease.id)
    assert :ok = Placement.release("00000000-0000-0000-0000-000000000000")
  end

  test "reap_expired reclaims capacity from stale leases" do
    inst = insert_instance(alloc_cpu_millis: 4000)
    {:ok, _i, lease} = Placement.acquire(%{cpu_millis: 1000})
    assert Repo.get(OadInstance, inst.id).committed_cpu_millis == 1000

    from(l in Lease, where: l.id == ^lease.id)
    |> Repo.update_all(set: [lease_expires_at: DateTime.add(DateTime.utc_now(), -10, :second)])

    assert Placement.reap_expired() >= 1
    assert Repo.get(OadInstance, inst.id).committed_cpu_millis == 0
  end

  test "request_from_resources parses cores and memory sizes" do
    req =
      Placement.request_from_resources(%{"cpu" => "2", "memory" => "4Gi"}, snapshot_name: "ws-v1")

    assert req.cpu_millis == 2000
    assert req.memory_bytes == 4 * 1024 * 1024 * 1024
    assert req.snapshot_name == "ws-v1"
  end
end
