defmodule Omniagent.Repo.Migrations.OadSnapshotChunkHashes do
  use Ecto.Migration

  def change do
    alter table(:oad_snapshots) do
      # The distinct chunk hashes this snapshot references, so unregistering it
      # can decrement each chunk's refcount. (Chunks at refcount 0 past the GC
      # grace window become collectable.)
      add :chunk_hashes, {:array, :string}, null: false, default: []
    end
  end
end
