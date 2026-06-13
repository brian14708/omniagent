defmodule Omniagent.OadInstances.OadInstance do
  @moduledoc """
  A registered oad sandbox daemon the control plane can call directly.

  Populated by oad's heartbeat (`POST /api/oad/register`). `api_token` is the
  bearer the control plane uses for that instance's `/v1` API and is never
  exposed to the browser; `base_url` is the address oad advertised for callbacks.
  Liveness is derived from `last_seen_at`.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "oad_instances" do
    field :instance_id, :string
    field :name, :string
    field :base_url, :string
    field :api_token, :string
    field :capabilities, :map, default: %{}
    field :version, :string
    field :last_seen_at, :utc_datetime_usec

    # Allocatable capacity reported by the daemon (0 = unknown/unconstrained).
    field :alloc_cpu_millis, :integer, default: 0
    field :alloc_memory_bytes, :integer, default: 0
    field :alloc_disk_bytes, :integer, default: 0
    # Committed capacity — scheduler-owned, never set from a heartbeat.
    field :committed_cpu_millis, :integer, default: 0
    field :committed_memory_bytes, :integer, default: 0
    field :committed_disk_bytes, :integer, default: 0
    field :labels, :map, default: %{}
    field :status, :string, default: "active"
    field :warm_snapshots, {:array, :string}, default: []

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(instance, attrs) do
    instance
    |> cast(attrs, [
      :instance_id,
      :name,
      :base_url,
      :api_token,
      :capabilities,
      :version,
      :last_seen_at,
      :alloc_cpu_millis,
      :alloc_memory_bytes,
      :alloc_disk_bytes,
      :labels,
      :status,
      :warm_snapshots
    ])
    |> validate_required([:instance_id, :base_url, :api_token, :last_seen_at])
    |> unique_constraint(:instance_id)
  end
end
