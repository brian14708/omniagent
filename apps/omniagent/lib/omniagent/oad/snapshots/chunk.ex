defmodule Omniagent.Oad.Snapshots.Chunk do
  @moduledoc """
  Reference-counted entry in the content-addressed chunk index, keyed by the
  chunk's hex `blake3` hash. `refcount` is the number of registered snapshots
  referencing the chunk; it is maintained by `Omniagent.Oad.Snapshots` and drops
  to zero when a chunk becomes collectable.
  """

  use Ecto.Schema

  @primary_key {:hash, :string, autogenerate: false}
  schema "oad_chunks" do
    field :refcount, :integer, default: 0
    field :first_seen_at, :utc_datetime_usec

    timestamps(type: :utc_datetime_usec)
  end
end
