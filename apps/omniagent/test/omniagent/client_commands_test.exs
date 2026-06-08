defmodule Omniagent.ClientCommandsTest do
  # Not async: the grace reaper marks the session offline from a separate
  # GenServer process, which needs the shared SQL sandbox connection.
  use Omniagent.DataCase

  alias Omniagent.{ClientCommands, Sessions}

  setup do
    {:ok, user} =
      Omniagent.Accounts.create_user(%{email: "grace-#{System.unique_integer([:positive])}@test"})

    {:ok, session} =
      Sessions.register_or_resume_session(user, %{
        "cwd" => "/tmp",
        "argv" => ["claude"],
        "client_id" => "client-#{System.unique_integer([:positive])}"
      })

    # A short grace window keeps the test fast.
    {:ok, commands} = GenServer.start_link(ClientCommands, grace_ms: 60)
    %{session: session, commands: commands}
  end

  defp dummy_pid do
    spawn(fn -> Process.sleep(:infinity) end)
  end

  defp status(session_id) do
    Repo.get!(Omniagent.Sessions.AgentSession, session_id).status
  end

  test "marks the session offline only after the grace window elapses", %{
    session: session,
    commands: commands
  } do
    pid = dummy_pid()
    :ok = GenServer.call(commands, {:register, session.id, pid})
    GenServer.cast(commands, {:unregister, session.id, pid})

    # Within the grace window the session stays online.
    Process.sleep(20)
    assert status(session.id) == "online"

    # After the window it is reaped offline.
    Process.sleep(80)
    assert status(session.id) == "offline"
  end

  test "a reconnect within the grace window cancels the reap", %{
    session: session,
    commands: commands
  } do
    pid = dummy_pid()
    :ok = GenServer.call(commands, {:register, session.id, pid})
    GenServer.cast(commands, {:unregister, session.id, pid})

    # Reconnect (re-register) before the window elapses.
    Process.sleep(20)
    reconnect = dummy_pid()
    :ok = GenServer.call(commands, {:register, session.id, reconnect})

    # The pending reap is cancelled, so the session never flips offline.
    Process.sleep(80)
    assert status(session.id) == "online"
  end

  test "send_command delivers to the registered channel pid", %{
    session: session,
    commands: commands
  } do
    # Register the test process as the session's channel; send_command resolves
    # the pid through :pg (cluster-wide) and messages it directly.
    :ok = GenServer.call(commands, {:register, session.id, self()})

    assert :ok = ClientCommands.send_command(session.id, "pty_input", %{"data" => "x"})
    assert_receive {:client_command, "pty_input", %{"data" => "x"}}
  end

  test "send_command reports offline for an unregistered session" do
    assert {:error, :offline} = ClientCommands.send_command("missing", "pty_input", %{})
  end

  describe "cluster reconciliation sweep" do
    import Ecto.Query
    alias Omniagent.Sessions.AgentSession

    defp backdate(session_id) do
      from(s in AgentSession, where: s.id == ^session_id)
      |> Repo.update_all(set: [updated_at: ~U[2020-01-01 00:00:00.000000Z]])
    end

    defp sweep(commands) do
      send(commands, :sweep)
      # :sys.get_state queues behind :sweep in the mailbox, so it returns only
      # after the sweep (and its mark_offline writes) have been processed.
      :sys.get_state(commands)
    end

    setup do
      # No auto-schedule; we drive :sweep by hand. stale_ms high so a freshly
      # created session is never considered stale by wall-clock drift.
      {:ok, reaper} =
        GenServer.start_link(ClientCommands,
          reconcile: false,
          sweep_ms: 60_000,
          stale_ms: 60_000
        )

      %{reaper: reaper}
    end

    test "reaps a stale online session with no live channel", %{
      session: session,
      reaper: reaper
    } do
      backdate(session.id)
      sweep(reaper)
      assert status(session.id) == "offline"
    end

    test "leaves a fresh online session alone (grace window not collapsed)", %{
      session: session,
      reaper: reaper
    } do
      # updated_at is ~now from setup, so it is within the staleness window.
      sweep(reaper)
      assert status(session.id) == "online"
    end

    test "leaves a stale session that still has a live channel", %{
      session: session,
      reaper: reaper
    } do
      backdate(session.id)
      :ok = GenServer.call(reaper, {:register, session.id, self()})
      sweep(reaper)
      assert status(session.id) == "online"
    end
  end
end
