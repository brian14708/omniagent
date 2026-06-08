defmodule OmniagentWeb.Plugs.ApiAuth do
  @moduledoc """
  Authenticates HTTP API requests with a bearer API token.

  Mirrors the socket handshake in `OmniagentWeb.ClientSocket`: the
  `Authorization: Bearer <token>` header is verified against the `api_tokens`
  table and, on success, the owning user is assigned to the connection. On
  failure the request is halted with `401`.
  """

  import Plug.Conn

  alias Omniagent.Accounts

  def init(opts), do: opts

  def call(conn, _opts) do
    with [header] <- get_req_header(conn, "authorization"),
         "Bearer " <> token <- header,
         {:ok, user, _api_token} <- Accounts.verify_api_token(token) do
      assign(conn, :current_user, user)
    else
      _ ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(401, Jason.encode!(%{error: "unauthorized"}))
        |> halt()
    end
  end
end
