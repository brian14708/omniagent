defmodule OmniagentWeb.SessionController do
  @moduledoc """
  Login / logout for the browser UI.

  Lives in a plain controller (not a LiveView) because only a regular request
  can write the signed session cookie. Credentials are checked against the
  static placeholder in `OmniagentWeb.Auth`.
  """
  use OmniagentWeb, :controller

  alias OmniagentWeb.Auth

  def new(conn, _params) do
    render(conn, :new, page_title: "Sign in")
  end

  def create(conn, %{"username" => username, "password" => password}) do
    if Auth.valid_credentials?(username, password) do
      conn
      |> renew_session()
      |> put_session(:admin, true)
      |> put_flash(:info, "Welcome back.")
      |> redirect(to: ~p"/console")
    else
      conn
      |> put_flash(:error, "Invalid username or password.")
      |> redirect(to: ~p"/login")
    end
  end

  def create(conn, _params) do
    conn
    |> put_flash(:error, "Invalid username or password.")
    |> redirect(to: ~p"/login")
  end

  def delete(conn, _params) do
    conn
    |> renew_session()
    |> put_flash(:info, "Signed out.")
    |> redirect(to: ~p"/")
  end

  # Drop any existing session data and rotate the session id to avoid fixation.
  defp renew_session(conn) do
    conn
    |> configure_session(renew: true)
    |> clear_session()
  end
end
