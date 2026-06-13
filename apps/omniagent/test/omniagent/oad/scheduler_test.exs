defmodule Omniagent.Oad.SchedulerTest do
  use ExUnit.Case, async: true

  alias Omniagent.Oad.Scheduler

  # A bare map standing in for an OadInstance for the pure scheduler.
  defp inst(attrs) do
    Map.merge(
      %{
        id: attrs[:name],
        name: "n",
        status: "active",
        alloc_cpu_millis: 0,
        alloc_memory_bytes: 0,
        alloc_disk_bytes: 0,
        committed_cpu_millis: 0,
        committed_memory_bytes: 0,
        committed_disk_bytes: 0,
        labels: %{},
        warm_snapshots: []
      },
      Map.new(attrs)
    )
  end

  test "rejects draining nodes" do
    refute Scheduler.admissible?(inst(status: "draining"), %{})
    assert Scheduler.admissible?(inst(status: "active"), %{})
  end

  test "unknown (0) capacity is treated as unconstrained" do
    assert Scheduler.admissible?(inst(name: "a"), %{cpu_millis: 8000, memory_bytes: 1_000_000_000})
  end

  test "rejects a node without free capacity, admits one with room" do
    full = inst(alloc_cpu_millis: 2000, committed_cpu_millis: 2000)
    free = inst(alloc_cpu_millis: 4000, committed_cpu_millis: 1000)
    refute Scheduler.admissible?(full, %{cpu_millis: 1000})
    assert Scheduler.admissible?(free, %{cpu_millis: 1000})
  end

  test "enforces required label selectors" do
    gpu = inst(labels: %{"gpu" => "true"})
    refute Scheduler.admissible?(inst(labels: %{}), %{labels: %{"gpu" => "true"}})
    assert Scheduler.admissible?(gpu, %{labels: %{"gpu" => "true"}})
  end

  test "ranks cache-affinity above spread" do
    warm =
      inst(
        name: "warm",
        warm_snapshots: ["ws-v3"],
        alloc_cpu_millis: 4000,
        committed_cpu_millis: 3500
      )

    empty =
      inst(name: "empty", warm_snapshots: [], alloc_cpu_millis: 4000, committed_cpu_millis: 0)

    ranked = Scheduler.rank([empty, warm], %{snapshot_name: "ws-v3", cpu_millis: 100})
    assert Enum.map(ranked, & &1.name) == ["warm", "empty"]
  end

  test "without affinity, prefers the least-utilized node (spread)" do
    busy = inst(name: "busy", alloc_cpu_millis: 4000, committed_cpu_millis: 3000)
    idle = inst(name: "idle", alloc_cpu_millis: 4000, committed_cpu_millis: 200)

    ranked = Scheduler.rank([busy, idle], %{cpu_millis: 100})
    assert Enum.map(ranked, & &1.name) == ["idle", "busy"]
  end

  test "rank drops inadmissible candidates" do
    ok = inst(name: "ok", alloc_cpu_millis: 4000)
    full = inst(name: "full", alloc_cpu_millis: 1000, committed_cpu_millis: 1000)

    assert Scheduler.rank([full, ok], %{cpu_millis: 500}) |> Enum.map(& &1.name) == ["ok"]
  end
end
