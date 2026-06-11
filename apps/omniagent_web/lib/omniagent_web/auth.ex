defmodule OmniagentWeb.Auth do
  @moduledoc """
  Session-based gate for the browser UI.

  Uses static placeholder credentials (`admin` / `admin`) for now — swap
  `valid_credentials?/2` for real account auth later. `call/2` is a module plug
  for the browser pipeline; `on_mount/4` mirrors it over the LiveView socket so
  reconnects can't bypass the dead-render check.
  """
  use OmniagentWeb, :verified_routes

  import Plug.Conn
  import Phoenix.Controller

  @username "admin"
  @password "admin"

  @doc """
  Validates the static admin credentials.

  Compared with `Plug.Crypto.secure_compare/2` so the check is constant-time
  even for these placeholder values.
  """
  def valid_credentials?(username, password) do
    Plug.Crypto.secure_compare(to_string(username), @username) and
      Plug.Crypto.secure_compare(to_string(password), @password)
  end

  # ── Browser pipeline plug ──

  def init(opts), do: opts

  def call(conn, _opts) do
    if get_session(conn, :admin) do
      conn
    else
      conn
      |> put_flash(:error, "Please sign in to continue.")
      |> redirect(to: ~p"/login")
      |> halt()
    end
  end

  # ── LiveView mount guard ──

  def on_mount(:require_admin, _params, session, socket) do
    if session["admin"] do
      {:cont, socket}
    else
      socket =
        socket
        |> Phoenix.LiveView.put_flash(:error, "Please sign in to continue.")
        |> Phoenix.LiveView.redirect(to: ~p"/login")

      {:halt, socket}
    end
  end
end
