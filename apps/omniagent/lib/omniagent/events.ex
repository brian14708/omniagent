defmodule Omniagent.Events do
  @moduledoc "Session event persistence and PubSub helpers."

  import Ecto.Query

  alias Omniagent.Events.SessionEvent
  alias Omniagent.Repo

  @pubsub Omniagent.PubSub

  def topic(session_id), do: "session:" <> session_id

  def subscribe(session_id), do: Phoenix.PubSub.subscribe(@pubsub, topic(session_id))

  def unsubscribe(session_id), do: Phoenix.PubSub.unsubscribe(@pubsub, topic(session_id))

  def broadcast(session_id, message) do
    Phoenix.PubSub.broadcast(@pubsub, topic(session_id), message)
  end

  def user_topic(user_id), do: "user_sessions:" <> user_id

  def subscribe_user(user_id), do: Phoenix.PubSub.subscribe(@pubsub, user_topic(user_id))

  def broadcast_user(user_id, message) do
    Phoenix.PubSub.broadcast(@pubsub, user_topic(user_id), message)
  end

  @daemons_topic "daemons"

  def subscribe_daemons, do: Phoenix.PubSub.subscribe(@pubsub, @daemons_topic)

  def broadcast_daemons(message) do
    Phoenix.PubSub.broadcast(@pubsub, @daemons_topic, message)
  end

  def record_session_event(session_id, source, event_type, sequence, payload \\ %{}) do
    %SessionEvent{}
    |> SessionEvent.changeset(%{
      agent_session_id: session_id,
      source: source,
      event_type: event_type,
      sequence: sequence || 0,
      payload: payload || %{},
      occurred_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
    })
    |> Repo.insert(
      on_conflict: :nothing,
      conflict_target: [:agent_session_id, :source, :event_type, :sequence]
    )
  end

  @doc """
  Persists a batch of `pty_output` events in a single `insert_all` (idempotent on
  the `(session, source, type, sequence)` unique index). Returns
  `{max_sequence, concatenated_data}` where `max_sequence` covers every event in
  the batch (so the ack advances even on a replay) but the data is concatenated
  only from the rows actually inserted — a replayed/duplicate batch conflicts on
  every row and yields `""`, so the channel broadcasts nothing rather than
  re-emitting already-seen terminal output.
  """
  def record_pty_outputs(session_id, events) when is_list(events) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    {rows, max_seq} =
      Enum.reduce(events, {[], 0}, fn event, {rows, max_seq} ->
        seq = event["sequence"] || 0

        row = %{
          id: Ecto.UUID.generate(),
          agent_session_id: session_id,
          source: "client",
          event_type: "pty_output",
          sequence: seq,
          payload: event,
          occurred_at: now,
          inserted_at: now,
          updated_at: now
        }

        {[row | rows], max(max_seq, seq)}
      end)

    {_count, inserted} =
      Repo.insert_all(SessionEvent, Enum.reverse(rows),
        on_conflict: :nothing,
        conflict_target: [:agent_session_id, :source, :event_type, :sequence],
        returning: [:sequence, :payload]
      )

    combined =
      (inserted || [])
      |> Enum.sort_by(& &1.sequence)
      |> Enum.map(& &1.payload["data"])
      |> Enum.filter(&is_binary/1)
      |> IO.iodata_to_binary()

    {max_seq, combined}
  end

  @pty_backlog_limit 5000

  @doc """
  Persisted PTY stream for a session, ordered for replay. Filters to
  `pty_output`/`pty_exit` because `record_session_event/5` also stores
  `trace_span` events in the same table. Bounded to the most recent
  `#{@pty_backlog_limit}` chunks so a long session doesn't load its whole
  history into memory.
  """
  def list_pty_chunks(session_id) do
    recent =
      SessionEvent
      |> where(
        [event],
        event.agent_session_id == ^session_id and event.event_type in ["pty_output", "pty_exit"]
      )
      |> order_by([event], desc: event.sequence, desc: event.inserted_at)
      |> limit(^@pty_backlog_limit)
      |> Repo.all()

    Enum.reverse(recent)
  end

  @codex_backlog_limit 2000

  @doc """
  Persisted structured codex events for a session, ordered for replay when the
  console selects the session. Returns the durable `codex_item`/`codex_turn`/
  `codex_token_usage`/`codex_error` rows (the ephemeral `codex_delta` stream is
  not persisted — completed items carry the final text). Bounded to the most
  recent `#{@codex_backlog_limit}` events.
  """
  def list_codex_events(session_id) do
    types = ["codex_item", "codex_turn", "codex_token_usage", "codex_error"]

    recent =
      SessionEvent
      |> where(
        [event],
        event.agent_session_id == ^session_id and event.event_type in ^types
      )
      |> order_by([event], desc: event.sequence, desc: event.inserted_at)
      |> limit(^@codex_backlog_limit)
      |> Repo.all()

    Enum.reverse(recent)
  end

  def list_session_events(session_id, opts \\ []) do
    limit = Keyword.get(opts, :limit, 200)

    SessionEvent
    |> where([event], event.agent_session_id == ^session_id)
    |> order_by([event], asc: event.sequence, asc: event.inserted_at)
    |> limit(^limit)
    |> Repo.all()
  end
end
