defmodule Omniagent.Sessions do
  @moduledoc "Agent session registry and lifecycle state."

  import Ecto.Query

  alias Omniagent.Events
  alias Omniagent.Payload
  alias Omniagent.Repo
  alias Omniagent.Sessions.AgentSession

  def list_sessions(user_id) do
    AgentSession
    |> where([session], session.user_id == ^user_id)
    |> order_by([session], desc: session.updated_at)
    |> Repo.all()
  end

  def get_session!(id), do: Repo.get!(AgentSession, id)

  @doc """
  IDs of sessions marked `online` whose last activity (`updated_at`, bumped by
  every client heartbeat) predates `cutoff`. Used by the cluster reconciler to
  find sessions whose owning node may have crashed: a live client heartbeats every
  ~15s, so a stale `updated_at` plus no live channel means the session is gone.
  """
  def list_stale_online_session_ids(cutoff) do
    AgentSession
    |> where([session], session.status == "online" and session.updated_at < ^cutoff)
    |> select([session], session.id)
    |> Repo.all()
  end

  def get_user_session(user_id, session_id) do
    AgentSession
    |> where([session], session.user_id == ^user_id and session.id == ^session_id)
    |> Repo.one()
  end

  @doc """
  Deletes a session the user owns, as long as it is not currently online.
  Associated rows (events, traces, reviews, comparisons, connections) cascade
  via `on_delete: :delete_all`.
  """
  def delete_session(user_id, session_id) do
    case get_user_session(user_id, session_id) do
      nil ->
        {:error, :not_found}

      %AgentSession{status: "online"} ->
        {:error, :session_online}

      session ->
        with {:ok, deleted} <- Repo.delete(session) do
          Events.broadcast_user(deleted.user_id, {:session_deleted, deleted.id})
          {:ok, deleted}
        end
    end
  end

  def register_or_resume_session(user, attrs) do
    now = now()
    session_id = blank_to_nil(Payload.fetch(attrs, :session_id))

    attrs = %{
      user_id: user.id,
      name: Payload.fetch(attrs, :name),
      cwd: Payload.fetch(attrs, :cwd),
      argv: Payload.fetch(attrs, :argv),
      client_id: Payload.fetch(attrs, :client_id),
      metadata: Payload.fetch(attrs, :metadata),
      status: "online",
      connected_at: now,
      disconnected_at: nil
    }

    result =
      if session_id do
        case get_user_session(user.id, session_id) do
          nil -> {:error, :not_found}
          session -> update_session(session, attrs)
        end
      else
        create_session(attrs)
      end

    with {:ok, session} <- result do
      Events.broadcast(session.id, {:session_updated, session})
      Events.broadcast_user(session.user_id, {:session_updated, session})
      {:ok, session}
    end
  end

  def mark_offline(session_id, status \\ "offline") do
    case Repo.get(AgentSession, session_id) do
      nil ->
        {:error, :not_found}

      session ->
        update_session(session, %{
          status: status,
          disconnected_at: now()
        })
        |> tap(fn
          {:ok, updated} ->
            Events.broadcast(updated.id, {:session_updated, updated})
            Events.broadcast_user(updated.user_id, {:session_updated, updated})

          _ ->
            :ok
        end)
    end
  end

  @doc """
  Advances a session's `last_client_sequence` high-water mark.

  Guarded with `GREATEST/2` so replayed or out-of-order events (which happen on
  reconnect, when the client re-sends its buffered outbox) can never regress the
  mark. Returns the post-update value so the channel can echo it back as a
  cumulative ack for the client's outbox trimming.
  """
  def update_last_sequence(session_id, sequence) when is_integer(sequence) do
    query =
      from(session in AgentSession,
        where: session.id == ^session_id,
        update: [
          set: [
            last_client_sequence:
              fragment("GREATEST(?, ?)", session.last_client_sequence, ^sequence),
            updated_at: ^now()
          ]
        ],
        select: session.last_client_sequence
      )

    case Repo.update_all(query, []) do
      {1, [acked]} -> acked
      _ -> sequence
    end
  end

  defp create_session(attrs) do
    %AgentSession{}
    |> AgentSession.changeset(prune(attrs))
    |> Repo.insert()
  end

  defp update_session(session, attrs) do
    session
    |> AgentSession.changeset(prune(attrs))
    |> Repo.update()
  end

  # Drop nil payload fields so a partial resume payload doesn't clobber stored
  # values (and so absent argv/metadata fall back to the schema defaults on
  # create). disconnected_at is exempt: resume sets it to nil to clear it and
  # mark the session reconnected.
  defp prune(attrs) do
    Map.reject(attrs, fn {key, value} -> is_nil(value) and key != :disconnected_at end)
  end

  defp blank_to_nil(value) when value in [nil, ""], do: nil
  defp blank_to_nil(value), do: value

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
