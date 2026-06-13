defmodule Omniagent.OadWorkspaces do
  @moduledoc """
  Context for oad workspaces (immutable snapshots built on an oad instance).

  CRUD plus small lifecycle helpers used by the build/update pipeline
  (`Omniagent.Oad.Builder`): create a `building` record, then mark it `ready`
  with the new snapshot/revision or `error` with a message.
  """

  import Ecto.Query, except: [update: 2, update: 3]

  alias Omniagent.OadWorkspaces.OadWorkspace
  alias Omniagent.Repo

  def list do
    from(w in OadWorkspace, order_by: [asc: w.name]) |> Repo.all()
  end

  def get(id), do: Repo.get(OadWorkspace, id)
  def get_by_name(name), do: Repo.get_by(OadWorkspace, name: name)

  @doc "Creates (or returns the existing) workspace record in `building` status."
  def upsert(attrs) do
    name = attrs[:name] || attrs["name"]

    case name && get_by_name(name) do
      %OadWorkspace{} = existing ->
        existing |> OadWorkspace.changeset(attrs) |> Repo.update()

      _ ->
        %OadWorkspace{} |> OadWorkspace.changeset(attrs) |> Repo.insert()
    end
  end

  def update(%OadWorkspace{} = workspace, attrs) do
    workspace |> OadWorkspace.changeset(attrs) |> Repo.update()
  end

  @doc "Marks a workspace ready with the freshly built snapshot/revision."
  def mark_ready(%OadWorkspace{} = workspace, attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    update(
      workspace,
      Map.merge(attrs, %{status: "ready", last_error: nil, built_at: now})
    )
  end

  def mark_error(%OadWorkspace{} = workspace, message) do
    update(workspace, %{status: "error", last_error: message})
  end

  def delete(%OadWorkspace{} = workspace), do: Repo.delete(workspace)
end
