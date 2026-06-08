defmodule Omniagent.Repo.Migrations.CreateArtifacts do
  use Ecto.Migration

  def change do
    create table(:artifacts, primary_key: false) do
      add :id, :binary_id, primary_key: true

      add :agent_session_id,
          references(:agent_sessions, type: :binary_id, on_delete: :delete_all),
          null: false

      add :kind, :string, null: false
      add :bucket, :string, null: false
      add :key, :string, null: false
      add :content_type, :string
      add :size, :bigint
      add :checksum, :string
      add :metadata, :map, null: false, default: %{}
      timestamps(type: :utc_datetime_usec)
    end

    create index(:artifacts, [:agent_session_id])
    create unique_index(:artifacts, [:bucket, :key])
  end
end
