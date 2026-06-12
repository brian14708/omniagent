defmodule OmniagentWeb.ConsoleLiveTest do
  use OmniagentWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Omniagent.{Accounts, Daemons, Sessions}

  setup %{conn: conn} do
    {:ok, conn: Plug.Test.init_test_session(conn, %{"admin" => true})}
  end

  test "shows the empty state when no daemon is connected", %{conn: conn} do
    {:ok, _view, html} = live(conn, ~p"/console")
    assert html =~ "No daemon connected"
  end

  test "renders a connected daemon's workspaces in the sidebar", %{conn: conn} do
    daemon_id = "d-#{System.unique_integer([:positive])}"

    :ok =
      Daemons.register(daemon_id, self(), %{
        "hostname" => "testhost",
        "pid" => 4242,
        "workspaces" => [%{"path" => "/home/me/my-project", "kind" => "git_repo"}]
      })

    {:ok, _view, html} = live(conn, ~p"/console")

    assert html =~ "testhost"
    # The workspace shows by its trailing segment (suffix), not the full path.
    assert html =~ "my-project"
  end

  test "inactive sessions live in the collapsible Sessions panel", %{conn: conn} do
    # mount resolves the console user via Accounts.default_user/0 (admin email).
    {:ok, user} = Accounts.create_user(%{email: "admin@omniagent.local"})

    {:ok, session} =
      Sessions.register_or_resume_session(user, %{
        "cwd" => "/tmp",
        "argv" => ["claude"],
        "client_id" => "c-#{System.unique_integer([:positive])}",
        "name" => "ghost-session"
      })

    {:ok, _} = Sessions.mark_offline(session.id, "exited")

    {:ok, view, html} = live(conn, ~p"/console")
    # The panel is collapsed by default, so the inactive session is hidden.
    refute html =~ "ghost-session"

    shown = view |> element("button[phx-click=\"toggle_sessions\"]") |> render_click()
    assert shown =~ "ghost-session"
  end
end
