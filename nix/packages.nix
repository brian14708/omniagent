# The `oad` daemon package (built with crane) and a check that builds it.
{ ... }:
{
  perSystem =
    {
      self',
      pkgs,
      craneLib,
      cargoSrc,
      pname,
      ...
    }:
    {
      packages.default = craneLib.buildPackage {
        inherit pname;
        version = "0.1.0";
        src = cargoSrc;
        cargoVendorDir = craneLib.vendorCargoDeps {
          src = cargoSrc;
          cargoLock = ../Cargo.lock;
        };
        cargoExtraArgs = "-p oad";
        nativeBuildInputs = [
          pkgs.protobuf
        ];
      };

      checks = pkgs.lib.mapAttrs' (n: pkgs.lib.nameValuePair "package-${n}") self'.packages;
    };
}
