defmodule OmniagentWeb.DaemonChannelTest do
  use ExUnit.Case

  import Phoenix.ChannelTest

  alias Omniagent.{Accounts, Daemons, Repo}
  alias OmniagentWeb.{ClientSocket, DaemonChannel}

  @endpoint OmniagentWeb.Endpoint

  setup tags do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Repo, shared: not tags[:async])
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    {:ok, user} =
      Accounts.create_user(%{email: "daemon-#{System.unique_integer([:positive])}@test"})

    raw_token = "tok-#{System.unique_integer([:positive])}"
    {:ok, _} = Accounts.create_api_token(user, raw_token)

    {:ok, socket} = connect(ClientSocket, %{"token" => raw_token})
    %{socket: socket, daemon_id: "d-#{System.unique_integer([:positive])}"}
  end

  test "registers, then relays a spawn_agent command to the daemon", %{
    socket: socket,
    daemon_id: daemon_id
  } do
    {:ok, _reply, channel} = subscribe_and_join(socket, DaemonChannel, "daemon:#{daemon_id}", %{})

    ref = push(channel, "daemon_register", %{"hostname" => "testhost", "agents" => ["claude"]})
    assert_reply(ref, :ok, %{daemon_id: ^daemon_id})

    # The daemon is now in the registry with its metadata.
    assert Enum.any?(Daemons.list(), fn d ->
             d.id == daemon_id and d.metadata["hostname"] == "testhost"
           end)

    # A spawn request is pushed down the channel to this daemon process.
    assert :ok = Daemons.spawn_agent(daemon_id, %{"argv" => ["claude"], "cwd" => "/tmp"})
    assert_push("spawn_agent", %{"argv" => ["claude"], "cwd" => "/tmp"})
  end

  test "spawn_agent on an unknown daemon reports offline" do
    assert {:error, :offline} = Daemons.spawn_agent("nope", %{"argv" => ["claude"]})
  end
end
