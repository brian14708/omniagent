# Shared per-system values — the pinned `pkgs`, the Rust toolchain, crane, and
# the RustFS package — exposed to the other flake modules via `_module.args`.
{ inputs, ... }:
{
  perSystem =
    {
      system,
      pkgs,
      ...
    }:
    let
      rustToolchain =
        p:
        p.rust-bin.stable.latest.default.override {
          targets = pkgs.lib.optionals pkgs.stdenv.isLinux [
            "x86_64-unknown-linux-musl"
          ];
        };
      craneLib = (inputs.crane.mkLib pkgs).overrideToolchain rustToolchain;
      cargoSrc = pkgs.lib.cleanSourceWith {
        src = ../.;
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
      _module.args = {
        pkgs = import inputs.nixpkgs {
          inherit system;
          config.allowUnfree = true;
          overlays = [ inputs.rust-overlay.overlays.default ];
        };
        inherit
          rustToolchain
          craneLib
          cargoSrc
          pname
          beamPackages
          rustfs
          ;
      };
    };
}
