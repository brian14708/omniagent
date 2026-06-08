defmodule Omniagent.Reviews.ReviewItem do
  use Ecto.Schema
  import Ecto.Changeset

  alias Omniagent.Sessions.AgentSession

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "review_items" do
    field :external_id, :string
    field :sequence, :integer
    field :phase, :string
    field :attempt, :integer
    field :provider, :string
    field :model, :string
    field :method, :string
    field :path, :string
    field :streaming, :boolean, default: false
    field :request, :map, default: %{}
    field :response, :map, default: %{}
    field :usage, :map, default: %{}
    field :status, :integer
    field :latency_ms, :integer
    field :started_at, :utc_datetime_usec
    field :error, :string
    field :decision, :map
    field :decided_at, :utc_datetime_usec
    belongs_to :agent_session, AgentSession

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(item, attrs) do
    item
    |> cast(attrs, [
      :external_id,
      :sequence,
      :phase,
      :attempt,
      :provider,
      :model,
      :method,
      :path,
      :streaming,
      :request,
      :response,
      :usage,
      :status,
      :latency_ms,
      :started_at,
      :error,
      :decision,
      :decided_at,
      :agent_session_id
    ])
    |> validate_required([:external_id, :phase, :attempt, :provider, :path, :agent_session_id])
    |> unique_constraint(:external_id, name: :review_items_agent_session_id_external_id_index)
  end
end
