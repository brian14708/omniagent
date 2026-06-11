defmodule OmniagentWeb.Router do
  use OmniagentWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {OmniagentWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
    plug OmniagentWeb.Plugs.ApiAuth
  end

  pipeline :require_auth do
    plug OmniagentWeb.Auth
  end

  scope "/", OmniagentWeb do
    pipe_through :browser

    get "/", PageController, :home
    get "/login", SessionController, :new
    post "/login", SessionController, :create
    delete "/logout", SessionController, :delete
  end

  scope "/", OmniagentWeb do
    pipe_through [:browser, :require_auth]

    live_session :authenticated, on_mount: {OmniagentWeb.Auth, :require_admin} do
      live "/console", ConsoleLive, :index
      live "/sessions/:id", ConsoleLive, :show
    end

    get "/sessions/:session_id/artifacts/:id/download", ArtifactController, :download
  end

  scope "/api", OmniagentWeb do
    pipe_through :api

    post "/sessions/:session_id/artifacts", ArtifactController, :create
  end

  # Enable LiveDashboard in development
  if Application.compile_env(:omniagent_web, :dev_routes) do
    # If you want to use the LiveDashboard in production, you should put
    # it behind authentication and allow only admins to access it.
    # If your application does not have an admins-only section yet,
    # you can use Plug.BasicAuth to set up some basic authentication
    # as long as you are also using SSL (which you should anyway).
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard",
        metrics: OmniagentWeb.Telemetry,
        ecto_repos: [Omniagent.Repo]
    end
  end
end
