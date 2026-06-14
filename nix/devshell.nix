# Development shell: the Rust toolchain (via crane) plus the runtime tooling for
# the Elixir control plane and the dev process-compose stack.
{ ... }:
{
  perSystem =
    {
      pkgs,
      craneLib,
      beamPackages,
      rustfs,
      ...
    }:
    {
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
    };
}
