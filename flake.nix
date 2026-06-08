{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
  };

  outputs =
    inputs@{
      flake-parts,
      crane,
      nixpkgs,
      rust-overlay,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.process-compose-flake.flakeModule
        inputs.treefmt-nix.flakeModule
      ];
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      perSystem =
        {
          self',
          system,
          pkgs,
          lib,
          ...
        }:
        let
          rustToolchain = p: p.rust-bin.stable.latest.default;
          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
          cargoSrc = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              (craneLib.filterCargoSources path type)
              || pkgs.lib.hasInfix "/crates/oad/migrations/" (toString path)
              || pkgs.lib.hasInfix "/crates/oad/proto/" (toString path);
          };
          pname = "oad";
          beamPackages = pkgs.beam29Packages;

          # RustFS (S3-compatible object storage for session artifacts) is not in
          # nixpkgs, so fetch the upstream release binary per platform. The Linux
          # builds are static musl, so they need no autopatchelf.
          rustfsVersion = "1.0.0-beta.7";
          rustfs =
            let
              asset =
                {
                  x86_64-linux = {
                    suffix = "linux-x86_64-musl";
                    sha256 = "f13bc89efa7881a199bf286390a89c7320ee2e52d90c341affd41bc5e7ba2677";
                  };
                  aarch64-linux = {
                    suffix = "linux-aarch64-musl";
                    sha256 = "989552479bfb2cc8e1ffa3af9c95da072d082a2e7124f54a1af1b1dcb68264ce";
                  };
                  x86_64-darwin = {
                    suffix = "macos-x86_64";
                    sha256 = "b45cd47c3b3c903ab31b2b6d936be474d5ee64527a40271e7817dd6c961d5387";
                  };
                  aarch64-darwin = {
                    suffix = "macos-aarch64";
                    sha256 = "8ff8db2068db450b22c4dbaa86b7abcf35110307f8e036aa83db387f540a2b53";
                  };
                }
                .${system};
            in
            pkgs.stdenvNoCC.mkDerivation {
              pname = "rustfs";
              version = rustfsVersion;
              src = pkgs.fetchurl {
                url = "https://github.com/rustfs/rustfs/releases/download/${rustfsVersion}/rustfs-${asset.suffix}-v${rustfsVersion}.zip";
                inherit (asset) sha256;
              };
              nativeBuildInputs = [ pkgs.unzip ];
              sourceRoot = ".";
              installPhase = ''
                runHook preInstall
                install -Dm755 "$(find . -type f -name rustfs | head -n1)" "$out/bin/rustfs"
                runHook postInstall
              '';
              meta.mainProgram = "rustfs";
            };
        in
        {
          _module.args.pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
            overlays = [ rust-overlay.overlays.default ];
          };
          devShells.default = craneLib.devShell {
            # gVisor (provides `runsc`) is Linux-only; skip it on darwin so the
            # shell still evaluates there.
            packages = [
              beamPackages.elixir_1_20
              beamPackages.erlang
              pkgs.nodejs_latest
              pkgs.postgresql_18
              pkgs.process-compose
              pkgs.protobuf
              pkgs.pnpm
              rustfs
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
              pkgs.envoy-bin
              pkgs.erofs-utils
              pkgs.gvisor
              pkgs.iproute2
              pkgs.nftables
            ];
          };

          process-compose.dev =
            let
              postgresDataDir = "data/postgres";
              rustfsDataDir = "data/rustfs";
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
                  RUSTFS_ACCESS_KEY = "rustfsadmin";
                  RUSTFS_SECRET_KEY = "rustfsadmin";
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

                  # One-shot: create the artifacts bucket RustFS does not make on
                  # its own, then exit. The control plane waits for this to finish.
                  "rustfs-setup" = {
                    command = pkgs.writeShellApplication {
                      name = "omniagent-rustfs-setup";
                      runtimeInputs = [ pkgs.minio-client ];
                      text = ''
                        set -euo pipefail
                        export MC_CONFIG_DIR=data/mc
                        mc alias set rustfs-dev http://127.0.0.1:9000 "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY"
                        mc mb --ignore-existing rustfs-dev/omniagent-artifacts
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
                };
              };
            };

          treefmt = {
            projectRootFile = "flake.nix";
            programs = {
              actionlint.enable = true;
              nixfmt.enable = true;
              prettier.enable = true;
              rustfmt = {
                enable = true;
                package = rustToolchain pkgs;
              };
            };
          };

          packages.default = craneLib.buildPackage {
            inherit pname;
            version = "0.1.0";
            src = cargoSrc;
            cargoVendorDir = craneLib.vendorCargoDeps {
              src = cargoSrc;
              cargoLock = ./Cargo.lock;
            };
            cargoExtraArgs = "-p oad";
            nativeBuildInputs = [
              pkgs.protobuf
            ];
          };

          checks = pkgs.lib.mapAttrs' (n: pkgs.lib.nameValuePair "package-${n}") self'.packages;
        };
      flake = { };
    };
}
