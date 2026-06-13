defmodule Omniagent.Repo.Migrations.OadInstanceScheduling do
  use Ecto.Migration

  def change do
    alter table(:oad_instances) do
      # Allocatable capacity reported by the daemon's heartbeat. Zero means
      # "unknown" and the scheduler treats that resource as unconstrained.
      add :alloc_cpu_millis, :integer, null: false, default: 0
      add :alloc_memory_bytes, :bigint, null: false, default: 0
      add :alloc_disk_bytes, :bigint, null: false, default: 0

      # Committed capacity — owned by the scheduler, maintained transactionally by
      # placement acquire/release. NOT refreshed by heartbeats.
      add :committed_cpu_millis, :integer, null: false, default: 0
      add :committed_memory_bytes, :bigint, null: false, default: 0
      add :committed_disk_bytes, :bigint, null: false, default: 0

      add :labels, :map, null: false, default: %{}
      # active | draining
      add :status, :string, null: false, default: "active"
      # Names of snapshots fully materialized on this node (cache-affinity input).
      add :warm_snapshots, {:array, :string}, null: false, default: []
    end

    # Capacity leases: each holds the committed capacity for one placed unit of
    # work until released (by the reaper) or reclaimed (by the expiry sweeper).
    create table(:oad_placements, primary_key: false) do
      add :id, :binary_id, primary_key: true

      add :instance_db_id,
          references(:oad_instances, type: :binary_id, on_delete: :delete_all),
          null: false

      # session | build
      add :kind, :string, null: false
      add :workspace, :string
      add :session_id, :string

      add :req_cpu_millis, :integer, null: false, default: 0
      add :req_memory_bytes, :bigint, null: false, default: 0
      add :req_disk_bytes, :bigint, null: false, default: 0

      # assigned | released
      add :state, :string, null: false, default: "assigned"
      add :lease_expires_at, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    create index(:oad_placements, [:instance_db_id])
    create index(:oad_placements, [:state])
    create index(:oad_placements, [:lease_expires_at])
  end
end
