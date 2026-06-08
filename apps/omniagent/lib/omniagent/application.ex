defmodule Omniagent.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    topologies = Application.get_env(:libcluster, :topologies, [])

    children = [
      # Form the Erlang cluster first (Postgres-based discovery) so PubSub and the
      # :pg-backed registries below are cluster-wide as soon as they start.
      {Cluster.Supervisor, [topologies, [name: Omniagent.ClusterSupervisor]]},
      # Shared process-group scope backing the cluster-visible ClientCommands and
      # Daemons registries (group keys: {:client, id} / {:daemon, id}).
      %{id: :pg, start: {:pg, :start_link, [Omniagent.PG]}},
      Omniagent.Repo,
      {Phoenix.PubSub, name: Omniagent.PubSub},
      Omniagent.ClientCommands,
      Omniagent.Daemons
      # Start a worker by calling: Omniagent.Worker.start_link(arg)
      # {Omniagent.Worker, arg}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Omniagent.Supervisor)
  end
end
