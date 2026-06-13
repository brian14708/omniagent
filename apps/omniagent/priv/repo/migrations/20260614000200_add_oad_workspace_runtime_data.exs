defmodule Omniagent.Repo.Migrations.AddOadWorkspaceRuntimeData do
  use Ecto.Migration

  def change do
    alter table(:oad_workspaces) do
      # The (possibly hand-edited) agent install script, persisted so a rebuild
      # replays the same selection/command.
      add :agent_install, :text
      # LLM env vars (name -> value) injected at build time and merged into the
      # per-session agent environment.
      add :env, :map, null: false, default: %{}
      # CPU/memory specs (e.g. %{"cpu" => "2", "memory" => "4Gi"}) applied as
      # cgroup limits when a session forks from the snapshot.
      add :resources, :map, null: false, default: %{}
    end
  end
end
