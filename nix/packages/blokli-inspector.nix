# blokli-inspector.nix - Blokli inspector binary package definitions
#
# Defines all variants of the blokli-inspector CLI binary for different
# platforms and profiles. blokli-inspector is a consumer of blokli-client
# used for inspecting running Blokli instances.

{
  lib,
  builders,
  sources,
  blokliInspectorCrateInfo,
  rev,
  nixLib,
}:

let
  # Common build arguments for blokli-inspector variants
  mkBlokliInspectorBuildArgs =
    { src, depsSrc }:
    {
      inherit src depsSrc rev;
      cargoToml = ./../../inspector/Cargo.toml;
    };

  localArgs = mkBlokliInspectorBuildArgs {
    src = sources.main;
    depsSrc = sources.deps;
  };

  mkBlokliInspectorPlatformPackages =
    platform:
    let
      name = "blokli-inspector-${platform}";
    in
    {
      "${name}" = builders.${platform}.callPackage nixLib.mkRustPackage localArgs;
    }
    // lib.optionalAttrs (lib.hasSuffix "-linux" platform) {
      "${name}-dev" = builders.${platform}.callPackage nixLib.mkRustPackage (
        localArgs // { CARGO_PROFILE = "dev"; }
      );
    };

  blokliInspectorPlatformPackages = builtins.foldl' (a: b: a // b) { } (
    map mkBlokliInspectorPlatformPackages [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ]
  );
in
{
  blokli-inspector = builders.local.callPackage nixLib.mkRustPackage localArgs;

  blokli-inspector-clippy = builders.local.callPackage nixLib.mkRustPackage (
    localArgs
    // {
      runClippy = true;
    }
  );
}
// blokliInspectorPlatformPackages
