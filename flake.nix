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
        in
        {
          _module.args.pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          devShells.default = craneLib.devShell {
            # gVisor (provides `runsc`) is Linux-only; skip it on darwin so the
            # shell still evaluates there.
            packages = [
              pkgs.protobuf
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
              pkgs.envoy-bin
              pkgs.erofs-utils
              pkgs.gvisor
              pkgs.iproute2
              pkgs.nftables
            ];
          };

          treefmt = {
            projectRootFile = "flake.nix";
            programs = {
              actionlint.enable = true;
              nixfmt.enable = true;
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

          checks = {
          }
          // (pkgs.lib.mapAttrs' (n: pkgs.lib.nameValuePair "package-${n}") self'.packages);
        };
      flake = {
      };
    };
}
