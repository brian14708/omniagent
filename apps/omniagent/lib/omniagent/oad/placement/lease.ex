defmodule Omniagent.Oad.Placement.Lease do
  @moduledoc """
  A capacity lease: holds the committed CPU/memory/disk for one placed unit of
  work (a session or build) on an oad instance until released or reclaimed.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "oad_placements" do
    field :instance_db_id, :binary_id
    field :kind, :string
    field :workspace, :string
    field :session_id, :string
    field :req_cpu_millis, :integer, default: 0
    field :req_memory_bytes, :integer, default: 0
    field :req_disk_bytes, :integer, default: 0
    field :state, :string, default: "assigned"
    field :lease_expires_at, :utc_datetime_usec

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  def changeset(lease, attrs) do
    lease
    |> cast(attrs, [
      :instance_db_id,
      :kind,
      :workspace,
      :session_id,
      :req_cpu_millis,
      :req_memory_bytes,
      :req_disk_bytes,
      :state,
      :lease_expires_at
    ])
    |> validate_required([:instance_db_id, :kind])
  end
end
