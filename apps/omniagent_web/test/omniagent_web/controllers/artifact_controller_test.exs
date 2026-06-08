defmodule OmniagentWeb.ArtifactControllerTest do
  use OmniagentWeb.ConnCase, async: true

  alias Omniagent.{Accounts, Sessions}

  defp user_with_token do
    {:ok, user} =
      Accounts.create_user(%{email: "art-#{System.unique_integer([:positive])}@test"})

    raw_token = "tok-#{System.unique_integer([:positive])}"
    {:ok, _} = Accounts.create_api_token(user, raw_token)
    {user, raw_token}
  end

  defp log_in_admin(conn) do
    conn
    |> init_test_session(%{})
    |> put_session(:admin, true)
  end

  test "rejects requests without a bearer token", %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/octet-stream")
      |> post(~p"/api/sessions/00000000-0000-0000-0000-000000000000/artifacts", "data")

    assert json_response(conn, 401)["error"] == "unauthorized"
  end

  test "returns 404 for a session the caller does not own", %{conn: conn} do
    {_user, token} = user_with_token()

    conn =
      conn
      |> put_req_header("authorization", "Bearer " <> token)
      |> put_req_header("x-artifact-kind", "recording")
      |> put_req_header("content-type", "application/octet-stream")
      |> post(~p"/api/sessions/00000000-0000-0000-0000-000000000000/artifacts", "data")

    assert json_response(conn, 404)["error"] == "session not found"
  end

  test "requires the X-Artifact-Kind header", %{conn: conn} do
    {user, token} = user_with_token()

    {:ok, session} =
      Sessions.register_or_resume_session(user, %{"cwd" => "/tmp", "argv" => ["claude"]})

    conn =
      conn
      |> put_req_header("authorization", "Bearer " <> token)
      |> put_req_header("content-type", "application/octet-stream")
      |> post(~p"/api/sessions/#{session.id}/artifacts", "data")

    assert json_response(conn, 400)["error"] =~ "X-Artifact-Kind"
  end

  test "download returns 404 for an unknown session", %{conn: conn} do
    conn =
      conn
      |> log_in_admin()
      |> get(
        ~p"/sessions/00000000-0000-0000-0000-000000000000/artifacts/00000000-0000-0000-0000-000000000000/download"
      )

    assert response(conn, 404) =~ "session not found"
  end
end
