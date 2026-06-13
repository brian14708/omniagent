defmodule Omniagent.Oad.SnapshotsTest do
  use Omniagent.DataCase, async: true

  alias Omniagent.Oad.Snapshots
  alias Omniagent.Oad.Snapshots.Chunk

  describe "missing_chunks/1" do
    test "returns all hashes when none are present, preserving order" do
      assert Snapshots.missing_chunks(["a", "b", "c"]) == ["a", "b", "c"]
    end

    test "returns only the hashes not yet registered" do
      {:ok, _} =
        Snapshots.register(%{
          snapshot_name: "ws-v1",
          descriptor_key: "d1",
          chunk_hashes: ["a", "b"]
        })

      assert Snapshots.missing_chunks(["a", "b", "c"]) == ["c"]
    end
  end

  describe "register/1" do
    test "records the snapshot and reference-counts its distinct chunks" do
      {:ok, snap} =
        Snapshots.register(%{
          snapshot_name: "ws-v1",
          workspace_name: "ws",
          descriptor_key: "descriptors/ws-v1.json",
          total_bytes: 100,
          # "b" is duplicated within the snapshot; it counts once.
          chunk_hashes: ["a", "b", "b"]
        })

      assert snap.snapshot_name == "ws-v1"
      assert snap.descriptor_key == "descriptors/ws-v1.json"
      assert snap.total_bytes == 100
      assert snap.chunk_count == 2
      assert refcount("a") == 1
      assert refcount("b") == 1
    end

    test "is idempotent: re-registering the same snapshot does not double-count" do
      attrs = %{snapshot_name: "ws-v1", descriptor_key: "d1", chunk_hashes: ["a", "b"]}
      {:ok, _} = Snapshots.register(attrs)
      {:ok, _} = Snapshots.register(attrs)

      assert refcount("a") == 1
      assert refcount("b") == 1
    end

    test "increments a shared chunk once per referencing snapshot" do
      {:ok, _} =
        Snapshots.register(%{
          snapshot_name: "ws-v1",
          descriptor_key: "d1",
          chunk_hashes: ["a", "b"]
        })

      {:ok, _} =
        Snapshots.register(%{
          snapshot_name: "ws-v2",
          descriptor_key: "d2",
          chunk_hashes: ["b", "c"]
        })

      assert refcount("a") == 1
      assert refcount("b") == 2
      assert refcount("c") == 1
    end
  end

  defp refcount(hash), do: Repo.get(Chunk, hash).refcount
end
