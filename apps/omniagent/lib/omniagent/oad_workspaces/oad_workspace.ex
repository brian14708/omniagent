defmodule Omniagent.OadWorkspaces.OadWorkspace do
  @moduledoc """
  A custom workspace built on an oad instance.

  A workspace is an immutable oad snapshot (`snapshot`, bumped each rebuild via
  `revision`) of a devcontainer image with the repo cloned and lifecycle hooks
  run. Sessions fork a fresh sandbox from `snapshot`; rebuilding produces a new
  revision and swaps it in. Tied to an oad endpoint by `oad_base_url`.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @statuses ~w(building ready error)

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "oad_workspaces" do
    field :name, :string
    field :oad_base_url, :string
    field :snapshot, :string
    field :revision, :integer, default: 0
    field :image, :string
    field :workspace_folder, :string, default: "/workspace"
    field :repo, :string
    field :git_ref, :string
    field :start_script, :string
    field :agent_versions, :map, default: %{}
    field :status, :string, default: "building"
    field :last_error, :string
    field :built_at, :utc_datetime_usec

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(workspace, attrs) do
    workspace
    |> cast(attrs, [
      :name,
      :oad_base_url,
      :snapshot,
      :revision,
      :image,
      :workspace_folder,
      :repo,
      :git_ref,
      :start_script,
      :agent_versions,
      :status,
      :last_error,
      :built_at
    ])
    |> validate_required([:name, :oad_base_url, :image, :workspace_folder])
    |> validate_inclusion(:status, @statuses)
    # The name is used as the snapshot path segment ("<name>-v<rev>"), which oad
    # validates as an id segment — keep it to safe characters.
    |> validate_format(:name, ~r/^[A-Za-z0-9._-]+$/,
      message: "must contain only letters, digits, '.', '_', or '-'"
    )
    |> unique_constraint(:name)
  end
end
