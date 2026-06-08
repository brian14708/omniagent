defmodule Omniagent.Reviews do
  @moduledoc "Review queue persistence and decision delivery."

  import Ecto.Query

  alias Omniagent.Events
  alias Omniagent.Payload
  alias Omniagent.Repo
  alias Omniagent.Reviews.ReviewItem

  def list_reviews(session_id) do
    ReviewItem
    |> where([item], item.agent_session_id == ^session_id)
    |> order_by([item], asc: item.sequence, asc: item.inserted_at)
    |> Repo.all()
  end

  def upsert_review_item(session_id, payload) do
    attrs = normalize_review(session_id, payload)

    result =
      %ReviewItem{}
      |> ReviewItem.changeset(attrs)
      |> Repo.insert(
        on_conflict: {:replace_all_except, [:id, :inserted_at]},
        conflict_target: [:agent_session_id, :external_id],
        returning: true
      )

    with {:ok, item} <- result do
      Events.record_session_event(
        session_id,
        "client",
        "review_item",
        item.sequence || 0,
        payload
      )

      Events.broadcast(session_id, {:review_item, item})
      {:ok, item}
    end
  end

  def decide_review(session_id, review_id, decision) do
    query =
      from item in ReviewItem,
        where: item.agent_session_id == ^session_id and item.external_id == ^review_id

    case Repo.one(query) do
      nil ->
        {:error, :not_found}

      item ->
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

        item
        |> ReviewItem.changeset(%{decision: decision, decided_at: now})
        |> Repo.update()
        |> tap(fn
          {:ok, updated} -> Events.broadcast(session_id, {:review_decision, updated, decision})
          _ -> :ok
        end)
    end
  end

  defp normalize_review(session_id, payload) do
    %{
      agent_session_id: session_id,
      external_id: Payload.fetch(payload, :id),
      sequence: Payload.fetch(payload, :sequence),
      phase: Payload.fetch(payload, :phase),
      attempt: Payload.fetch(payload, :attempt, 1),
      provider: to_string(Payload.fetch(payload, :provider)),
      model: Payload.fetch(payload, :model),
      method: Payload.fetch(payload, :method),
      path: Payload.fetch(payload, :path),
      streaming: Payload.fetch(payload, :streaming, false),
      request: Payload.map_value(Payload.fetch(payload, :request)),
      response: Payload.map_value(Payload.fetch(payload, :response)),
      usage: Payload.fetch(payload, :usage, %{}),
      status: Payload.fetch(payload, :status),
      latency_ms: Payload.fetch(payload, :latency_ms),
      started_at: Payload.parse_datetime(Payload.fetch(payload, :started_at)),
      error: Payload.fetch(payload, :error)
    }
  end
end
