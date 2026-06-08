defmodule Omniagent.SessionsTest do
  use Omniagent.DataCase, async: true

  alias Omniagent.Sessions

  setup do
    {:ok, user} =
      Omniagent.Accounts.create_user(%{email: "seq-#{System.unique_integer([:positive])}@test"})

    {:ok, session} =
      Sessions.register_or_resume_session(user, %{
        "cwd" => "/tmp",
        "argv" => ["claude"],
        "client_id" => "client-#{System.unique_integer([:positive])}"
      })

    %{user: user, session: session}
  end

  describe "update_last_sequence/2" do
    test "advances the high-water mark and returns the new value", %{session: session} do
      assert Sessions.update_last_sequence(session.id, 5) == 5
      assert Sessions.update_last_sequence(session.id, 10) == 10
    end

    test "is monotonic: a replayed/lower sequence never regresses the mark", %{session: session} do
      assert Sessions.update_last_sequence(session.id, 10) == 10
      # Reconnect replays sequence 3..10; the mark must stay at 10.
      assert Sessions.update_last_sequence(session.id, 3) == 10
      assert Sessions.update_last_sequence(session.id, 7) == 10
      assert get_session!(session.id).last_client_sequence == 10
    end

    test "persists the high-water mark to the row", %{session: session} do
      Sessions.update_last_sequence(session.id, 42)
      assert get_session!(session.id).last_client_sequence == 42
    end
  end

  defp get_session!(id), do: Omniagent.Repo.get!(Omniagent.Sessions.AgentSession, id)
end
