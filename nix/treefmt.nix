# treefmt formatter configuration (`nix fmt` / the `formatting` check).
{ ... }:
{
  perSystem =
    {
      pkgs,
      rustToolchain,
      ...
    }:
    {
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
    };
}
