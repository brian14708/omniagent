defmodule Omniagent.Oad.Scheduler do
  @moduledoc """
  Pure placement scoring for oad instances.

  `rank/2` filters a list of candidate instances to those that can host a
  request, then orders them best-first. It is a pure function of its inputs (no
  database, no process), so it is exhaustively unit-testable; the transactional
  capacity commit lives in `Omniagent.Oad.Placement`.

  A `request` is a map with optional keys `:cpu_millis`, `:memory_bytes`,
  `:disk_bytes` (defaulting to 0 = unconstrained), `:snapshot_name` (for
  cache-affinity), and `:labels` (a required-label selector).

  Filtering rejects non-`active` nodes, nodes lacking required labels, and nodes
  without free capacity. A reported allocatable of `0` means the daemon has not
  declared that resource, and the scheduler treats it as unconstrained.

  Scoring prefers, in order: **cache-affinity** (the snapshot is already warm on
  the node, avoiding an object-store pull) then **spread** (the least-utilized
  node, to bound blast-radius). Phase 4 replaces the exact warm-snapshot list
  with an approximate chunk-level signal.
  """

  @affinity_weight 1_000.0

  @doc "Filters `candidates` to those that can host `request`, best-ranked first."
  def rank(candidates, request) do
    candidates
    |> Enum.filter(&admissible?(&1, request))
    |> Enum.sort_by(&score(&1, request), :desc)
  end

  @doc "Whether `instance` can host `request` (status, capacity, labels)."
  def admissible?(instance, request) do
    instance.status == "active" and fits?(instance, request) and labels_match?(instance, request)
  end

  @doc "Placement score for `instance` (higher is better)."
  def score(instance, request) do
    affinity = if affinity?(instance, request), do: @affinity_weight, else: 0.0
    affinity + (1.0 - utilization(instance))
  end

  defp affinity?(instance, request) do
    case Map.get(request, :snapshot_name) do
      name when is_binary(name) -> name in (instance.warm_snapshots || [])
      _ -> false
    end
  end

  defp fits?(instance, request) do
    resource_fits?(
      instance.alloc_cpu_millis,
      instance.committed_cpu_millis,
      Map.get(request, :cpu_millis, 0)
    ) and
      resource_fits?(
        instance.alloc_memory_bytes,
        instance.committed_memory_bytes,
        Map.get(request, :memory_bytes, 0)
      ) and
      resource_fits?(
        instance.alloc_disk_bytes,
        instance.committed_disk_bytes,
        Map.get(request, :disk_bytes, 0)
      )
  end

  # A reported allocatable of 0 means "unknown" — treat as unconstrained.
  defp resource_fits?(0, _committed, _req), do: true
  defp resource_fits?(alloc, committed, req), do: alloc - committed >= req

  defp labels_match?(instance, request) do
    case Map.get(request, :labels) do
      labels when is_map(labels) and map_size(labels) > 0 ->
        node_labels = instance.labels || %{}
        Enum.all?(labels, fn {k, v} -> Map.get(node_labels, to_string(k)) == v end)

      _ ->
        true
    end
  end

  # Worst (max) per-resource committed fraction; resources with unknown capacity
  # (alloc 0) do not contribute.
  defp utilization(instance) do
    [
      frac(instance.committed_cpu_millis, instance.alloc_cpu_millis),
      frac(instance.committed_memory_bytes, instance.alloc_memory_bytes),
      frac(instance.committed_disk_bytes, instance.alloc_disk_bytes)
    ]
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> 0.0
      fracs -> Enum.max(fracs)
    end
  end

  defp frac(_committed, 0), do: nil
  defp frac(committed, alloc), do: committed / alloc
end
