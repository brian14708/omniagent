defmodule Omniagent.Repo do
  use Ecto.Repo,
    otp_app: :omniagent,
    adapter: Ecto.Adapters.Postgres
end
