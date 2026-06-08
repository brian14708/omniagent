defmodule Omniagent.Sessions.AgentSession do
  use Ecto.Schema
  import Ecto.Changeset

  alias Omniagent.Accounts.User

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "agent_sessions" do
    field :name, :string
    field :status, :string, default: "offline"
    field :cwd, :string
    field :argv, {:array, :string}, default: []
    field :client_id, :string
    field :last_client_sequence, :integer, default: 0
    field :connected_at, :utc_datetime_usec
    field :disconnected_at, :utc_datetime_usec
    field :metadata, :map, default: %{}
    belongs_to :user, User

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(session, attrs) do
    session
    |> cast(attrs, [
      :name,
      :status,
      :cwd,
      :argv,
      :client_id,
      :last_client_sequence,
      :connected_at,
      :disconnected_at,
      :metadata,
      :user_id
    ])
    |> validate_required([:user_id])
    |> validate_inclusion(:status, ["online", "offline", "exited", "error"])
  end
end
