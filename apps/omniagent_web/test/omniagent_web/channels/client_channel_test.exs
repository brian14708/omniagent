defmodule OmniagentWeb.ClientChannelTest do
  # Not async: the channel process and the globally-supervised ClientCommands
  # both touch the datastore, so they need the shared SQL sandbox connection.
  use ExUnit.Case

  import Phoenix.ChannelTest

  alias Omniagent.{Accounts, Repo, Sessions}
  alias OmniagentWeb.{ClientChannel, ClientSocket}

  @endpoint OmniagentWeb.Endpoint

  setup tags do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo, shared: not tags[:async])
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    {:ok, user} =
      Accounts.create_user(%{email: "chan-#{System.unique_integer([:positive])}@test"})

    raw_token = "tok-#{System.unique_integer([:positive])}"
    {:ok, _} = Accounts.create_api_token(user, raw_token)

    {:ok, socket} = connect(ClientSocket, %{"token" => raw_token})
    %{socket: socket}
  end

  defp join_and_register(socket, topic) do
    {:ok, _reply, socket} = subscribe_and_join(socket, ClientChannel, topic, %{})
    ref = push(socket, "session_register", %{"cwd" => "/tmp", "argv" => ["claude"]})
    assert_reply(ref, :ok, %{id: session_id})
    {socket, session_id}
  end

  test "heartbeat reply carries the acknowledged last_client_sequence", %{socket: socket} do
    {socket, _session_id} = join_and_register(socket, "client:hb-1")

    ref = push(socket, "heartbeat", %{"sequence" => 7})
    assert_reply(ref, :ok, reply)
    assert reply.last_client_sequence == 7
    assert Map.has_key?(reply, :server_time)
  end

  test "a lower heartbeat sequence does not regress the ack", %{socket: socket} do
    {socket, _session_id} = join_and_register(socket, "client:hb-2")

    ref = push(socket, "heartbeat", %{"sequence" => 10})
    assert_reply(ref, :ok, %{last_client_sequence: 10})

    # A replayed/older heartbeat after a reconnect must not move the mark back.
    ref = push(socket, "heartbeat", %{"sequence" => 4})
    assert_reply(ref, :ok, %{last_client_sequence: 10})
  end

  test "session_close marks the session exited and records an event", %{socket: socket} do
    {socket, session_id} = join_and_register(socket, "client:close-1")
    assert Sessions.get_session!(session_id).status == "online"

    Omniagent.Events.subscribe(session_id)
    push(socket, "session_close", %{"exit_code" => 0, "agent" => %{"supported" => true}})

    assert_receive {:session_close, %{"exit_code" => 0}}
    assert Sessions.get_session!(session_id).status == "exited"
  end

  test "pty_output_batch persists each chunk, acks, and broadcasts combined data", %{
    socket: socket
  } do
    {socket, session_id} = join_and_register(socket, "client:pty-1")
    Omniagent.Events.subscribe(session_id)

    events = [%{"data" => "foo", "sequence" => 1}, %{"data" => "bar", "sequence" => 2}]
    ref = push(socket, "pty_output_batch", %{"events" => events})

    # ack carries the max sequence in the batch
    assert_reply(ref, :ok, %{last_client_sequence: 2})
    # one coalesced broadcast for the whole batch
    assert_receive {:pty_output, %{"data" => "foobar"}}
    # both chunks are persisted as individual sequenced rows
    chunks = Omniagent.Events.list_pty_chunks(session_id)
    assert Enum.map(chunks, & &1.sequence) == [1, 2]

    # resending the same batch is idempotent (unique index), still acks
    ref = push(socket, "pty_output_batch", %{"events" => events})
    assert_reply(ref, :ok, %{last_client_sequence: 2})
    assert length(Omniagent.Events.list_pty_chunks(session_id)) == 2
    # ...and broadcasts nothing on replay (no duplicate terminal output)
    refute_receive {:pty_output, _}
  end
end
