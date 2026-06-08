import Config

# Configure your database
#
# The MIX_TEST_PARTITION environment variable can be used
# to provide built-in test partitioning in CI environment.
# Run `mix help test` for more information.
config :omniagent, Omniagent.Repo,
  url: System.get_env("TEST_DATABASE_URL"),
  username: System.get_env("PGUSER") || System.get_env("POSTGRES_USER") || "postgres",
  password: System.get_env("PGPASSWORD") || System.get_env("POSTGRES_PASSWORD") || "postgres",
  hostname: System.get_env("PGHOST") || System.get_env("POSTGRES_HOST") || "localhost",
  port: String.to_integer(System.get_env("PGPORT") || System.get_env("POSTGRES_PORT") || "5432"),
  database:
    System.get_env("PGTESTDATABASE") || System.get_env("POSTGRES_TEST_DB") ||
      "omniagent_test#{System.get_env("MIX_TEST_PARTITION")}",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: System.schedulers_online() * 2

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :omniagent_web, OmniagentWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "V/fxJFWJ5XKXRxyL28kAntd9UmefXf3m+ThaivAhLI/3PFMcbRZVOvvuLr1Psd6u",
  server: false

# Print only warnings and errors during test
config :logger, level: :warning

# No cluster reconciliation sweep in single-node test runs.
config :omniagent, session_reconcile: false

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime

# Enable helpful, but potentially expensive runtime checks
config :phoenix_live_view,
  enable_expensive_runtime_checks: true

# Sort query params output of verified routes for robust url comparisons
config :phoenix,
  sort_verified_routes_query_params: true
