defmodule Omniagent.Traces do
  @moduledoc "Persisted LLM trace spans."

  import Ecto.Query

  alias Omniagent.Events
  alias Omniagent.Payload
  alias Omniagent.Repo
  alias Omniagent.Traces.TraceSpan

  def list_spans(session_id) do
    TraceSpan
    |> where([span], span.agent_session_id == ^session_id)
    |> order_by([span], asc: span.sequence)
    |> Repo.all()
  end

  def record_span(session_id, payload) when is_map(payload) do
    attrs = normalize_span(session_id, payload)

    result =
      %TraceSpan{}
      |> TraceSpan.changeset(attrs)
      |> Repo.insert(
        on_conflict: {:replace_all_except, [:id, :inserted_at]},
        conflict_target: [:agent_session_id, :external_id],
        returning: true
      )

    with {:ok, span} <- result do
      Events.record_session_event(session_id, "client", "trace_span", span.sequence, payload)
      Events.broadcast(session_id, {:trace_span, span})
      {:ok, span}
    end
  end

  defp normalize_span(session_id, payload) do
    %{
      agent_session_id: session_id,
      external_id: Payload.fetch(payload, :id),
      sequence: Payload.fetch(payload, :sequence),
      provider: provider_to_string(Payload.fetch(payload, :provider)),
      model: Payload.fetch(payload, :model),
      method: Payload.fetch(payload, :method),
      request_base_url: Payload.fetch(payload, :request_base_url),
      upstream_base_url: Payload.fetch(payload, :upstream_base_url),
      path: Payload.fetch(payload, :path),
      streaming: Payload.fetch(payload, :streaming, false),
      request_headers: Payload.fetch(payload, :request_headers, %{}),
      request: Payload.map_value(Payload.fetch(payload, :request)),
      response_headers: Payload.fetch(payload, :response_headers, %{}),
      response: Payload.map_value(Payload.fetch(payload, :response)),
      stream_events: Payload.fetch(payload, :stream_events, []),
      usage: Payload.fetch(payload, :usage, %{}),
      labels: Payload.fetch(payload, :labels, []),
      status: Payload.fetch(payload, :status),
      started_at: Payload.parse_datetime(Payload.fetch(payload, :started_at)),
      latency_ms: Payload.fetch(payload, :latency_ms),
      error: Payload.fetch(payload, :error)
    }
  end

  defp provider_to_string(nil), do: nil
  defp provider_to_string(value), do: to_string(value)
end
