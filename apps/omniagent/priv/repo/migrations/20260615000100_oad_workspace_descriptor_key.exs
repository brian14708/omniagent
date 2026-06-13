defmodule Omniagent.Repo.Migrations.OadWorkspaceDescriptorKey do
  use Ecto.Migration

  def change do
    alter table(:oad_workspaces) do
      # Object key of the portable SnapshotDescriptor, set when the snapshot was
      # published to the content-addressed store. Its presence means sessions can
      # run on any CAS-enabled node, not only the build endpoint.
      add :descriptor_key, :string

      # Demote from NOT NULL: with portable snapshots, oad_base_url is an advisory
      # "built on" record / operator pin, no longer the routing key.
      modify :oad_base_url, :string, null: true, from: {:string, null: false}
    end
  end
end
