defmodule Omniagent.Oad.Snapshots.Snapshot do
  @moduledoc """
  A snapshot published to the content-addressed store, recorded so the control
  plane can locate its portable descriptor and (with `Chunk`) reference-count the
  chunks it holds.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "oad_snapshots" do
    field :snapshot_name, :string
    field :workspace_name, :string
    field :descriptor_key, :string
    field :total_bytes, :integer, default: 0
    field :chunk_count, :integer, default: 0

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  def changeset(snapshot, attrs) do
    snapshot
    |> cast(attrs, [
      :snapshot_name,
      :workspace_name,
      :descriptor_key,
      :total_bytes,
      :chunk_count
    ])
    |> validate_required([:snapshot_name, :descriptor_key])
    |> unique_constraint(:snapshot_name)
  end
end
