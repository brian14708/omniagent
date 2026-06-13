defmodule Omniagent.Repo.Migrations.CreateOadWorkspaces do
  use Ecto.Migration

  def change do
    create table(:oad_workspaces, primary_key: false) do
      add :id, :binary_id, primary_key: true

      add :name, :string, null: false
      # The oad endpoint (advertise_url) whose snapshot store holds this
      # workspace. Tied to the endpoint, not a specific instance_id, since oad
      # reports a fresh instance_id on restart while its snapshots persist.
      add :oad_base_url, :string, null: false

      # Immutable snapshot backing the workspace and its monotonic revision.
      add :snapshot, :string
      add :revision, :integer, null: false, default: 0

      add :image, :string, null: false
      add :workspace_folder, :string, null: false, default: "/workspace"
      add :repo, :string
      add :git_ref, :string
      # Devcontainer postStart/postAttach script run per session.
      add :start_script, :text
      # Installed agent CLI versions baked into the base (name -> version).
      add :agent_versions, :map, null: false, default: %{}

      # building | ready | error
      add :status, :string, null: false, default: "building"
      add :last_error, :text
      add :built_at, :utc_datetime_usec

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:oad_workspaces, [:name])
    create index(:oad_workspaces, [:oad_base_url])
  end
end
