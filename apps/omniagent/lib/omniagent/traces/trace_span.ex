defmodule Omniagent.Traces.TraceSpan do
  use Ecto.Schema
  import Ecto.Changeset

  alias Omniagent.Sessions.AgentSession

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "trace_spans" do
    field :external_id, :string
    field :sequence, :integer
    field :provider, :string
    field :model, :string
    field :method, :string
    field :request_base_url, :string
    field :upstream_base_url, :string
    field :path, :string
    field :streaming, :boolean, default: false
    field :request_headers, :map, default: %{}
    field :request, :map, default: %{}
    field :response_headers, :map, default: %{}
    field :response, :map, default: %{}
    field :stream_events, {:array, :map}, default: []
    field :usage, :map, default: %{}
    field :labels, {:array, :map}, default: []
    field :status, :integer
    field :started_at, :utc_datetime_usec
    field :latency_ms, :integer
    field :error, :string
    belongs_to :agent_session, AgentSession

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(span, attrs) do
    span
    |> cast(attrs, [
      :external_id,
      :sequence,
      :provider,
      :model,
      :method,
      :request_base_url,
      :upstream_base_url,
      :path,
      :streaming,
      :request_headers,
      :request,
      :response_headers,
      :response,
      :stream_events,
      :usage,
      :labels,
      :status,
      :started_at,
      :latency_ms,
      :error,
      :agent_session_id
    ])
    |> validate_required([:external_id, :sequence, :provider, :path, :agent_session_id])
    |> unique_constraint(:external_id, name: :trace_spans_agent_session_id_external_id_index)
    |> unique_constraint(:sequence, name: :trace_spans_agent_session_id_sequence_index)
  end
end
