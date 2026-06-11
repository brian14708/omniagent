import Config

# config/runtime.exs is executed for all environments, including
# during releases. It is executed after compilation and before the
# system starts, so it is typically used to load production configuration
# and secrets from environment variables or elsewhere. Do not define
# any compile-time configuration in here, as it won't be applied.
# The block below contains prod specific runtime configuration.

http_port = String.to_integer(System.get_env("PORT", "4000"))

config :omniagent_web, OmniagentWeb.Endpoint, http: [port: http_port]

# Cluster the control-plane nodes using Postgres LISTEN/NOTIFY for discovery
# (libcluster_postgres). Erlang distribution is still the transport; Postgres is
# only how nodes find each other, so no extra infra beyond the DB we already run.
#
# The strategy opens its own Postgrex connection, so it needs DB credentials. We
# reuse the same connection env the Repo uses: DATABASE_URL when set, otherwise
# the discrete PG*/POSTGRES_* vars (matching config/dev.exs and config/test.exs).
#
# Disabled in :test (single node) and whenever clustering is explicitly turned off
# via CLUSTER_ENABLED=false, in which case the topology is empty and
# Cluster.Supervisor becomes a no-op.
cluster_db_config =
  case System.get_env("DATABASE_URL") do
    url when is_binary(url) and url != "" ->
      uri = URI.parse(url)

      {user, pass} =
        case String.split(uri.userinfo || "postgres:postgres", ":", parts: 2) do
          [u, p] -> {u, p}
          [u] -> {u, ""}
          _ -> {"postgres", "postgres"}
        end

      [
        hostname: uri.host || "localhost",
        port: uri.port || 5432,
        username: user,
        password: pass,
        database: String.trim_leading(uri.path || "/postgres", "/")
      ]

    _ ->
      [
        hostname: System.get_env("PGHOST") || System.get_env("POSTGRES_HOST") || "localhost",
        port:
          String.to_integer(System.get_env("PGPORT") || System.get_env("POSTGRES_PORT") || "5432"),
        username: System.get_env("PGUSER") || System.get_env("POSTGRES_USER") || "postgres",
        password:
          System.get_env("PGPASSWORD") || System.get_env("POSTGRES_PASSWORD") || "postgres",
        database: System.get_env("PGDATABASE") || System.get_env("POSTGRES_DB") || "omniagent_dev"
      ]
  end

topologies =
  if config_env() == :test or System.get_env("CLUSTER_ENABLED") == "false" do
    []
  else
    [
      omniagent: [
        strategy: LibclusterPostgres.Strategy,
        config: Keyword.merge(cluster_db_config, channel_name: "omniagent_cluster")
      ]
    ]
  end

config :libcluster, topologies: topologies

# Start the endpoint when run inside an OTP release (e.g. `PHX_SERVER=true bin/omniagent start`).
# Without this an assembled release boots the supervision tree but never serves HTTP.
if System.get_env("PHX_SERVER") do
  config :omniagent_web, OmniagentWeb.Endpoint, server: true
end

# S3-compatible object storage (RustFS) for session artifacts. Configured for
# every environment so the artifact upload endpoint works in dev too; override
# via the RUSTFS_* environment variables. RustFS speaks the S3 API (AWS SigV4),
# so ExAws talks to it directly once pointed at the endpoint.
rustfs_endpoint = System.get_env("RUSTFS_ENDPOINT_URL", "http://localhost:9000")
rustfs_uri = URI.parse(rustfs_endpoint)

config :ex_aws, :s3,
  scheme: "#{rustfs_uri.scheme || "http"}://",
  host: rustfs_uri.host || "localhost",
  port: rustfs_uri.port || 9000,
  region: System.get_env("RUSTFS_REGION", "us-east-1")

config :ex_aws,
  access_key_id: System.get_env("RUSTFS_ACCESS_KEY_ID", "rustfsadmin"),
  secret_access_key: System.get_env("RUSTFS_SECRET_ACCESS_KEY", "rustfsadmin")

if bucket = System.get_env("RUSTFS_BUCKET") do
  config :omniagent, :artifacts_bucket, bucket
end

if config_env() == :prod do
  phoenix_host = System.get_env("PHX_HOST") || "example.com"
  phoenix_scheme = System.get_env("PHX_SCHEME") || "https"

  phoenix_port =
    String.to_integer(
      System.get_env("PHX_PORT") || if(phoenix_scheme == "https", do: "443", else: "80")
    )

  database_url =
    System.get_env("DATABASE_URL") ||
      raise """
      environment variable DATABASE_URL is missing.
      For example: ecto://USER:PASS@HOST/DATABASE
      """

  maybe_ipv6 = if System.get_env("ECTO_IPV6") in ~w(true 1), do: [:inet6], else: []

  config :omniagent, Omniagent.Repo,
    # ssl: true,
    url: database_url,
    pool_size: String.to_integer(System.get_env("POOL_SIZE") || "10"),
    # For machines with several cores, consider starting multiple pools of `pool_size`
    # pool_count: 4,
    socket_options: maybe_ipv6

  # The secret key base is used to sign/encrypt cookies and other secrets.
  # A default value is used in config/dev.exs and config/test.exs but you
  # want to use a different value for prod and you most likely don't want
  # to check this value into version control, so we use an environment
  # variable instead.
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  config :omniagent_web, OmniagentWeb.Endpoint,
    url: [host: phoenix_host, port: phoenix_port, scheme: phoenix_scheme],
    http: [
      # Enable IPv6 and bind on all interfaces.
      # Set it to  {0, 0, 0, 0, 0, 0, 0, 1} for local network only access.
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: http_port
    ],
    secret_key_base: secret_key_base

  # ## Using releases
  #
  # If you are doing OTP releases, you need to instruct Phoenix
  # to start each relevant endpoint:
  #
  #     config :omniagent_web, OmniagentWeb.Endpoint, server: true
  #
  # Then you can assemble a release by calling `mix release`.
  # See `mix help release` for more information.

  # ## SSL Support
  #
  # To get SSL working, you will need to add the `https` key
  # to your endpoint configuration:
  #
  #     config :omniagent_web, OmniagentWeb.Endpoint,
  #       https: [
  #         ...,
  #         port: 443,
  #         cipher_suite: :strong,
  #         keyfile: System.get_env("SOME_APP_SSL_KEY_PATH"),
  #         certfile: System.get_env("SOME_APP_SSL_CERT_PATH")
  #       ]
  #
  # The `cipher_suite` is set to `:strong` to support only the
  # latest and more secure SSL ciphers. This means old browsers
  # and clients may not be supported. You can set it to
  # `:compatible` for wider support.
  #
  # `:keyfile` and `:certfile` expect an absolute path to the key
  # and cert in disk or a relative path inside priv, for example
  # "priv/ssl/server.key". For all supported SSL configuration
  # options, see https://hexdocs.pm/plug/Plug.SSL.html#configure/1
  #
  # We also recommend setting `force_ssl` in your config/prod.exs,
  # ensuring no data is ever sent via http, always redirecting to https:
  #
  #     config :omniagent_web, OmniagentWeb.Endpoint,
  #       force_ssl: [hsts: true]
  #
  # Check `Plug.SSL` for all available options in `force_ssl`.
end
