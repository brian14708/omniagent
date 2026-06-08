defmodule OmniagentWeb.ClientSocket do
  use Phoenix.Socket

  alias Omniagent.Accounts

  channel "client:*", OmniagentWeb.ClientChannel
  channel "daemon:*", OmniagentWeb.DaemonChannel

  @impl true
  def connect(%{"token" => token}, socket, _connect_info) do
    case Accounts.verify_api_token(token) do
      {:ok, user, api_token} ->
        socket =
          socket
          |> assign(:current_user, user)
          |> assign(:api_token_id, api_token.id)

        {:ok, socket}

      _ ->
        :error
    end
  end

  def connect(_params, _socket, _connect_info), do: :error

  @impl true
  def id(socket), do: "client_socket:#{socket.assigns.current_user.id}"
end
