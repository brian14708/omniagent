defmodule Omniagent.Repo.Migrations.CreateOadInstances do
  use Ecto.Migration

  def change do
    create table(:oad_instances, primary_key: false) do
      add :id, :binary_id, primary_key: true

      # The id the oad daemon reports for itself (stable for its process
      # lifetime; a restart reports a fresh one).
      add :instance_id, :string, null: false
      add :name, :string
      # Base URL the control plane calls to reach this oad's /v1 API.
      add :base_url, :string, null: false
      # Bearer token for this oad's /v1 API (server-side only; never sent to the
      # browser).
      add :api_token, :string, null: false
      add :capabilities, :map, null: false, default: %{}
      add :version, :string
      add :last_seen_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:oad_instances, [:instance_id])
    create index(:oad_instances, [:base_url])
    create index(:oad_instances, [:last_seen_at])
  end
end
