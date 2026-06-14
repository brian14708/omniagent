# `nix run .#dev` — the local development stack (Postgres, RustFS, the Elixir
# control plane, and the oad sandbox daemon) wired together with process-compose.
{ ... }:
{
  perSystem =
    {
      self',
      pkgs,
      beamPackages,
      rustfs,
      ...
    }:
    {
      process-compose.dev =
        let
          postgresDataDir = "data/postgres";
          rustfsDataDir = "data/rustfs";
          oadDataDir = "data/oad";
          # Dev RustFS root credentials, shared by the control plane (via the
          # RUSTFS_* env below) and oad's CAS client (via OAD_S3_*).
          rustfsAccessKey = "rustfsadmin";
          rustfsSecretKey = "rustfsadmin";
          # Shared secret oad presents when self-registering with the control
          # plane (POST /api/oad/register).
          oadRegisterToken = "dev-oad-register-token";
        in
        {
          settings = {
            environment = {
              PGUSER = "omniagent";
              PGPASSWORD = "omniagent";
              PGHOST = "127.0.0.1";
              PGPORT = "5432";
              PGDATABASE = "omniagent";
              # RustFS root credentials; the control plane's ExAws client
              # reads the matching RUSTFS_ACCESS_KEY_ID / RUSTFS_SECRET_ACCESS_KEY
              # defaults (also "rustfsadmin") from config/runtime.exs.
              RUSTFS_ACCESS_KEY = rustfsAccessKey;
              RUSTFS_SECRET_KEY = rustfsSecretKey;
              # Shared registration secret the control plane checks on
              # POST /api/oad/register; the oad process presents the matching
              # OAD_CONTROL_PLANE_TOKEN.
              OMNIAGENT_OAD_REGISTER_TOKEN = oadRegisterToken;
            };
            processes = {
              postgres = {
                command = pkgs.writeShellApplication {
                  name = "omniagent-postgres";
                  runtimeInputs = [
                    pkgs.coreutils
                    pkgs.postgresql_18
                  ];
                  text = ''
                    set -euo pipefail
                    export PGDATA=${postgresDataDir}

                    mkdir -p "$(dirname "$PGDATA")"
                    if [ ! -s "$PGDATA/PG_VERSION" ]; then
                      initdb --username=postgres --auth=trust --no-locale --encoding=UTF8 "$PGDATA"
                      {
                        printf '%s\n' "listen_addresses = '127.0.0.1'"
                        printf '%s\n' "port = 5432"
                        printf '%s\n' "unix_socket_directories = '/tmp'"
                      } >> "$PGDATA/postgresql.conf"

                      pg_ctl -D "$PGDATA" -w start
                      createuser --username=postgres -s "$PGUSER" || true
                      psql -v ON_ERROR_STOP=1 --username=postgres --dbname=postgres \
                        -c "ALTER USER $PGUSER PASSWORD '$PGPASSWORD';"
                      pg_ctl -D "$PGDATA" -m fast -w stop
                    fi

                    exec postgres -D "$PGDATA"
                  '';
                };
                readiness_probe = {
                  exec.command = "${pkgs.postgresql}/bin/pg_isready";
                  initial_delay_seconds = 2;
                  period_seconds = 2;
                  timeout_seconds = 1;
                  success_threshold = 1;
                  failure_threshold = 30;
                };
              };

              rustfs = {
                command = pkgs.writeShellApplication {
                  name = "omniagent-rustfs";
                  runtimeInputs = [
                    pkgs.coreutils
                    rustfs
                  ];
                  text = ''
                    set -euo pipefail
                    export RUSTFS_VOLUMES=${rustfsDataDir}
                    export RUSTFS_ADDRESS=":9000"
                    # The web console binds a second port we don't need in dev.
                    export RUSTFS_CONSOLE_ENABLE=false

                    mkdir -p "$RUSTFS_VOLUMES"
                    exec rustfs "$RUSTFS_VOLUMES"
                  '';
                };
                readiness_probe = {
                  # curl (without -f) exits 0 once the S3 endpoint answers at
                  # all, even with a 403, so this just waits for the listener.
                  exec.command = "${pkgs.curl}/bin/curl -s -o /dev/null http://127.0.0.1:9000";
                  initial_delay_seconds = 2;
                  period_seconds = 2;
                  timeout_seconds = 2;
                  success_threshold = 1;
                  failure_threshold = 30;
                };
              };

              # One-shot: create the artifacts and CAS buckets RustFS does not
              # make on its own, then exit. The control plane and oad wait for
              # this to finish.
              "rustfs-setup" = {
                command = pkgs.writeShellApplication {
                  name = "omniagent-rustfs-setup";
                  runtimeInputs = [ pkgs.minio-client ];
                  text = ''
                    set -euo pipefail
                    export MC_CONFIG_DIR=data/mc
                    mc alias set rustfs-dev http://127.0.0.1:9000 "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY"
                    mc mb --ignore-existing rustfs-dev/omniagent-artifacts
                    mc mb --ignore-existing rustfs-dev/omniagent-cas
                  '';
                };
                depends_on.rustfs.condition = "process_healthy";
              };

              "control-plane" = {
                command = pkgs.writeShellApplication {
                  name = "omniagent-control-plane";
                  runtimeInputs = [
                    pkgs.coreutils
                    pkgs.curl
                    beamPackages.elixir_1_20
                    beamPackages.erlang
                    pkgs.git
                    pkgs.nodejs_latest
                    pkgs.postgresql_18
                  ];
                  text = ''
                    set -euo pipefail

                    mix deps.get
                    npm ci --prefix apps/omniagent_web/assets
                    mix ecto.setup
                    exec mix phx.server
                  '';
                };
                depends_on = {
                  postgres.condition = "process_healthy";
                  "rustfs-setup".condition = "process_completed_successfully";
                };
                readiness_probe = {
                  http_get = {
                    host = "127.0.0.1";
                    port = 4000;
                    path = "/";
                  };
                  initial_delay_seconds = 5;
                  period_seconds = 5;
                  timeout_seconds = 2;
                  success_threshold = 1;
                  failure_threshold = 30;
                };
              };
            }
            // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
              # oad hosts gVisor sandboxes and is Linux-only: it manages
              # network namespaces, nftables rules and bind mounts, all of
              # which require root. is_elevated makes process-compose launch
              # it via sudo (it prompts once on `up`).
              oad = {
                is_elevated = true;
                command = pkgs.writeShellApplication {
                  name = "omniagent-oad";
                  runtimeInputs = [
                    pkgs.coreutils
                    pkgs.envoy-bin
                    pkgs.erofs-utils
                    pkgs.gvisor
                    pkgs.iproute2
                    pkgs.iptables
                    pkgs.nftables
                    self'.packages.default
                  ];
                  # All config is exported in-script rather than via the
                  # process `environment` so it survives the sudo elevation.
                  text = ''
                    set -euo pipefail

                    # /v1 bearer token the control plane presents to oad; the
                    # daemon refuses to start without one. Matches the dev seed
                    # token (OMNIAGENT_DEV_TOKEN default of "dev-token").
                    export OAD_BEARER_TOKEN=dev-token
                    export OAD_BASE_DIR=${oadDataDir}
                    export OAD_INSTANCE_NAME=dev

                    # Self-register with the local control plane so it can
                    # schedule sandboxes here. OAD_CONTROL_PLANE_TOKEN must
                    # match the control plane's OMNIAGENT_OAD_REGISTER_TOKEN.
                    export OAD_CONTROL_PLANE_URL=http://127.0.0.1:4000
                    export OAD_CONTROL_PLANE_TOKEN=${oadRegisterToken}

                    # Distributed CAS backed by the dev RustFS instance, using
                    # the omniagent-cas bucket created by rustfs-setup.
                    export OAD_S3_ENDPOINT_URL=http://127.0.0.1:9000
                    export OAD_S3_REGION=us-east-1
                    export OAD_S3_BUCKET=omniagent-cas
                    export OAD_S3_ACCESS_KEY_ID=${rustfsAccessKey}
                    export OAD_S3_SECRET_ACCESS_KEY=${rustfsSecretKey}

                    mkdir -p ${oadDataDir}
                    exec oad
                  '';
                };
                depends_on = {
                  "rustfs-setup".condition = "process_completed_successfully";
                  "control-plane".condition = "process_healthy";
                };
                readiness_probe = {
                  http_get = {
                    host = "127.0.0.1";
                    port = 8080;
                    path = "/healthz";
                  };
                  initial_delay_seconds = 3;
                  period_seconds = 5;
                  timeout_seconds = 2;
                  success_threshold = 1;
                  failure_threshold = 30;
                };
              };
            };
          };
        };
    };
}
