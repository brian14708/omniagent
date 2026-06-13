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

  describe "garbage collection" do
    test "unregister decrements refcounts; shared chunks survive" do
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

      assert :ok = Snapshots.unregister_snapshot("ws-v1")
      assert refcount("a") == 0
      assert refcount("b") == 1
      assert refcount("c") == 1
      assert Snapshots.get_by_name("ws-v1") == nil
    end

    test "collectable_chunks respects refcount and the grace window" do
      {:ok, _} =
        Snapshots.register(%{snapshot_name: "ws-v1", descriptor_key: "d1", chunk_hashes: ["a"]})

      :ok = Snapshots.unregister_snapshot("ws-v1")

      # Within the grace window the chunk is not yet collectable...
      assert Snapshots.collectable_chunks(86_400) == []
      # ...but with zero grace it is.
      assert Snapshots.collectable_chunks(0) == ["a"]
    end

    test "gc deletes collectable chunks via delete_fn, then removes their rows" do
      {:ok, _} =
        Snapshots.register(%{
          snapshot_name: "ws-v1",
          descriptor_key: "d1",
          chunk_hashes: ["a", "b"]
        })

      :ok = Snapshots.unregister_snapshot("ws-v1")

      parent = self()
      delete_fn = fn hashes -> send(parent, {:deleted, Enum.sort(hashes)}) && :ok end

      assert {:ok, 2} = Snapshots.gc(0, delete_fn)
      assert_received {:deleted, ["a", "b"]}
      assert Repo.get(Chunk, "a") == nil
      assert Repo.get(Chunk, "b") == nil
    end

    test "gc keeps a still-referenced chunk" do
      {:ok, _} =
        Snapshots.register(%{snapshot_name: "ws-v1", descriptor_key: "d1", chunk_hashes: ["a"]})

      assert {:ok, 0} = Snapshots.gc(0, fn _ -> :ok end)
      assert refcount("a") == 1
    end

    test "missing_chunks reports a refcount-0 chunk as missing so it is re-uploaded" do
      {:ok, _} =
        Snapshots.register(%{snapshot_name: "ws-v1", descriptor_key: "d1", chunk_hashes: ["a"]})

      :ok = Snapshots.unregister_snapshot("ws-v1")
      assert Snapshots.missing_chunks(["a"]) == ["a"]
    end
  end

  defp refcount(hash), do: Repo.get(Chunk, hash).refcount
end
