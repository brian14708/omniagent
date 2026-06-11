defmodule Omniagent.Traces do
  @moduledoc "Persisted LLM trace spans."

  import Ecto.Query

  alias Omniagent.Events
  alias Omniagent.Payload
  alias Omniagent.Repo
  alias Omniagent.Traces.TraceSpan

  # The fields shipped with the trace list / each incremental span. Excludes the
  # heavy, never-listed payloads (`stream_events` and the header maps), which are
  # fetched per-span on demand via `get_span/2` + `span_detail/1` when a span is
  # opened. `request`/`response` stay in the summary because the client's trace
  # search matches against them.
  @summary_fields [
    :id,
    :external_id,
    :sequence,
    :provider,
    :model,
    :method,
    :path,
    :status,
    :latency_ms,
    :streaming,
    :request,
    :response,
    :usage,
    :labels,
    :error
  ]

  @detail_fields [:stream_events, :request_headers, :response_headers]

  @doc """
  Summary maps for every span in a session, ordered for replay. Selects only the
  summary columns so a long trace doesn't pull its full request/response/stream
  history into the LiveView process or down the socket.
  """
  def list_spans(session_id) do
    TraceSpan
    |> where([span], span.agent_session_id == ^session_id)
    |> order_by([span], asc: span.sequence)
    |> select([span], map(span, @summary_fields))
    |> Repo.all()
  end

  @doc "A single span (full row), scoped to its session, for lazy detail loads."
  def get_span(session_id, span_id) do
    TraceSpan
    |> where([span], span.agent_session_id == ^session_id and span.id == ^span_id)
    |> Repo.one()
  end

  @doc "Light span payload for the trace list (drops the heavy detail fields)."
  def span_summary(span), do: Map.take(span, @summary_fields)

  @doc "Heavy span fields fetched lazily when a span is opened."
  def span_detail(span), do: Map.take(span, @detail_fields)

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
