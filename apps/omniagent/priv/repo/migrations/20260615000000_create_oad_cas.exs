defmodule Omniagent.Repo.Migrations.CreateOadCas do
  use Ecto.Migration

  def change do
    # Snapshots published to the content-addressed store (CAS). One row per
    # captured snapshot revision; `descriptor_key` locates the portable
    # SnapshotDescriptor in object storage.
    create table(:oad_snapshots, primary_key: false) do
      add :id, :binary_id, primary_key: true

      add :snapshot_name, :string, null: false
      add :workspace_name, :string
      add :descriptor_key, :string, null: false
      # Total uncompressed size of the snapshot's checkpoint images.
      add :total_bytes, :bigint, null: false, default: 0
      # Number of distinct chunks the snapshot references.
      add :chunk_count, :integer, null: false, default: 0

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:oad_snapshots, [:snapshot_name])
    create index(:oad_snapshots, [:workspace_name])

    # Reference-counted chunk index. The daemon's batched existence check
    # (`POST /api/oad/cas/check`) reads it; snapshot registration increments
    # refcounts; a later GC phase collects chunks whose refcount reaches zero.
    create table(:oad_chunks, primary_key: false) do
      add :hash, :string, primary_key: true
      add :refcount, :integer, null: false, default: 0
      add :first_seen_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create index(:oad_chunks, [:refcount])
  end
end
