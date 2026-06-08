defmodule Omniagent.Events.SessionEvent do
  use Ecto.Schema
  import Ecto.Changeset

  alias Omniagent.Sessions.AgentSession

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "session_events" do
    field :source, :string
    field :event_type, :string
    field :sequence, :integer
    field :payload, :map, default: %{}
    field :occurred_at, :utc_datetime_usec
    belongs_to :agent_session, AgentSession

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(event, attrs) do
    event
    |> cast(attrs, [:source, :event_type, :sequence, :payload, :occurred_at, :agent_session_id])
    |> validate_required([:source, :event_type, :sequence, :agent_session_id])
  end
end
