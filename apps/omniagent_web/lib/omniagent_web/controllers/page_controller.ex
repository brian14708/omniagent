defmodule OmniagentWeb.PageController do
  use OmniagentWeb, :controller

  def home(conn, _params) do
    render(conn, :home, page_title: "Home", logged_in: get_session(conn, :admin) == true)
  end
end
