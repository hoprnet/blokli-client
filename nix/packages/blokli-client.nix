# blokli-client.nix - Blokli client library package definitions
#
# Defines all variants of the blokli-client library for different platforms and profiles.
# Blokli client is a GraphQL client library for connecting to the Blokli API.

{
  lib,
  pkgs,
  builders,
  sources,
  blokliClientCrateInfo,
  rev,
  nixLib,
}:

let
  # Common build arguments for blokli-client variants
  mkblokliClientBuildArgs =
    { src, depsSrc }:
    {
      inherit src depsSrc rev;
      cargoToml = ./../../client/Cargo.toml;
    };

  localArgs = mkblokliClientBuildArgs {
    src = sources.main;
    depsSrc = sources.deps;
  };

  mkBlokliClientPlatformPackages =
    platform:
    let
      name = "lib-blokli-client-${platform}";
    in
    {
      "${name}" = builders.${platform}.callPackage nixLib.mkRustLibrary localArgs;
    }
    // lib.optionalAttrs (lib.hasSuffix "-linux" platform) {
      "${name}-dev" = builders.${platform}.callPackage nixLib.mkRustLibrary (
        localArgs // { CARGO_PROFILE = "dev"; }
      );
    };

  blokliClientPlatformPackages = builtins.foldl' (a: b: a // b) { } (
    map mkBlokliClientPlatformPackages [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ]
  );

  blokliClientClippy = builders.local.callPackage nixLib.mkRustPackage (
    (mkblokliClientBuildArgs {
      src = sources.main;
      depsSrc = sources.deps;
    })
    // {
      runClippy = true;
      prependPackageName = false;
      cargoExtraArgs = "--workspace";
    }
  );
in
{
  lib-blokli-client = builders.local.callPackage nixLib.mkRustLibrary localArgs;

  blokli-client-nextest = builders.local.callPackage nixLib.mkRustPackage (
    (mkblokliClientBuildArgs {
      src = sources.test;
      depsSrc = sources.deps;
    })
    // {
      runNextest = true;
      testCargoProfile = "ci-test";
      prependPackageName = false;
      cargoExtraArgs = "--workspace --exclude blokli-integration-tests";
    }
  );

  blokli-client-clippy = blokliClientClippy;

  blokli-client-coverage = builders.localCoverage.callPackage nixLib.mkRustPackage (
    (mkblokliClientBuildArgs {
      src = sources.test;
      depsSrc = sources.deps;
    })
    // {
      runCoverage = true;
      cargoLlvmCovCommand = "nextest";
      testCargoProfile = "ci-test";
      cargoExtraArgs = "--exclude blokli-integration-tests";
      extraNativeBuildInputs = [ pkgs.cargo-nextest ];
    }
  );

  # Backward-compatible alias for callers of the original package.
  clippy = blokliClientClippy;
}
// blokliClientPlatformPackages
