# This file is responsible for configuring your umbrella
# and **all applications** and their dependencies with the
# help of the Config module.
#
# Note that all applications in your umbrella share the
# same configuration and dependencies, which is why they
# all use the same configuration file. If you want different
# configurations or dependencies per app, it is best to
# move said applications out of the umbrella.
import Config

# Configure Mix tasks and generators
config :omniagent,
  ecto_repos: [Omniagent.Repo]

config :omniagent_web,
  ecto_repos: [Omniagent.Repo],
  generators: [context_app: :omniagent]

# Configures the endpoint
config :omniagent_web, OmniagentWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: OmniagentWeb.ErrorHTML, json: OmniagentWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: Omniagent.PubSub,
  live_view: [signing_salt: "Ul3ssVJI"]

# Configure esbuild (the version is required)
config :esbuild,
  version: "0.25.4",
  omniagent_web: [
    args:
      ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets/js --external:/fonts/* --external:/images/* --alias:@=. --loader:.woff2=file),
    cd: Path.expand("../apps/omniagent_web/assets", __DIR__),
    env: %{"NODE_PATH" => [Path.expand("../deps", __DIR__), Mix.Project.build_path()]}
  ]

# Configure tailwind (the version is required)
config :tailwind,
  version: "4.1.12",
  omniagent_web: [
    args: ~w(
      --input=assets/css/app.css
      --output=priv/static/assets/css/app.css
    ),
    cd: Path.expand("../apps/omniagent_web", __DIR__)
  ]

# Configure Elixir's Logger
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

# S3-compatible object storage (RustFS) for session artifacts. The endpoint,
# bucket and credentials are set per-environment in config/runtime.exs.
config :ex_aws, json_codec: Jason
config :omniagent, :artifacts_bucket, "omniagent-artifacts"

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
