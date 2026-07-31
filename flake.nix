# flake.nix - HOPR blokli Nix flake configuration
#
# This is the main entry point for the Nix flake. It uses the HOPR nix-lib
# for reusable Rust build functions and formatting configuration.
#
# Structure:
# - nix/packages/: Package definitions (blokli-client)
# - nix/checks.nix: CI/CD quality checks
# - nix-lib (external): Rust builders, Docker images, treefmt, and utilities

{
  description = "HOPR blokli-client - Rust client library for the Blokli API";

  # External dependencies - kept in main flake for Nix flake requirements
  #
  # INPUTS REFERENCE:
  #
  # Core Nix ecosystem dependencies:
  # - flake-parts: Modular flake framework for better organization
  # - nixpkgs: The main Nix package repository (using release 25.05 for stability)
  # - nix-lib: HOPR Nix library with reusable Rust build functions
  #
  # Development tools and quality assurance:
  # - pre-commit: Git hooks for code quality enforcement
  # - treefmt-nix: Universal code formatter integration for Nix
  # - flake-root: Utilities for finding flake root directory
  #
  # Input optimization strategy:
  # All inputs follow nixpkgs where possible to reduce closure size and improve caching.
  # This is achieved through the "follows" directive below.
  inputs = {
    # Core Nix ecosystem dependencies
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/release-26.05";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixos-unstable";

    # HOPR Nix Library (provides flake-utils and reusable build functions)
    nix-lib.url = "github:hoprnet/nix-lib/v1.3.0";

    # Rust build system
    crane.url = "github:ipetkov/crane";
    rust-overlay.url = "github:oxalica/rust-overlay";

    # Development tools and quality assurance
    pre-commit.url = "github:cachix/git-hooks.nix";
    flake-root.url = "github:srid/flake-root";
    foundry.url = "github:shazow/foundry.nix";
    solc.url = "github:hellwolf/solc.nix";

    # Input dependency optimization
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
    pre-commit.inputs.nixpkgs.follows = "nixpkgs";
    nix-lib.inputs.nixpkgs.follows = "nixpkgs";
    nix-lib.inputs.crane.follows = "crane";
    nix-lib.inputs.rust-overlay.follows = "rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
    foundry.inputs.nixpkgs.follows = "nixpkgs";
    solc.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-unstable,
      flake-parts,
      nix-lib,
      crane,
      rust-overlay,
      pre-commit,
      foundry,
      solc,
      ...
    }@inputs:
    flake-parts.lib.mkFlake { inherit inputs; } {
      # Import flake modules for additional functionality
      imports = [
        inputs.nix-lib.flakeModules.default
        inputs.flake-root.flakeModule
      ];

      # Per-system configuration
      # Each system gets its own set of packages, shells, etc.
      perSystem =
        {
          config,
          lib,
          system,
          ...
        }:
        let
          # Git revision for version tracking
          rev = toString (self.shortRev or (self.dirtyShortRev or "dirty"));

          # Filesystem utilities for source filtering
          fs = lib.fileset;

          # Nixpkgs with rust-overlay, foundry overlay, and solc overlay
          overlays = [
            rust-overlay.overlays.default
            foundry.overlay
            solc.overlay
          ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };
          pkgsUnstable = import nixpkgs-unstable {
            inherit system overlays;
          };

          # Platform information
          buildPlatform = pkgs.stdenv.buildPlatform;

          # Import nix-lib for this system
          nixLib = nix-lib.lib.${system};

          # Crane library for Rust builds (for crate info extraction)
          craneLib = (crane.mkLib pkgs).overrideToolchain (p: p.rust-bin.stable.latest.default);

          # blokli-client crate information
          blokliClientCrateInfoOriginal = craneLib.crateNameFromCargoToml {
            cargoToml = ./client/Cargo.toml;
          };
          blokliClientCrateInfo = {
            pname = "blokli-client";
            # Normalize version to major.minor.patch for consistent caching
            version = pkgs.lib.strings.concatStringsSep "." (
              pkgs.lib.lists.take 3 (builtins.splitVersion blokliClientCrateInfoOriginal.version)
            );
          };

          # blokli-inspector crate information
          blokliInspectorCrateInfoOriginal = craneLib.crateNameFromCargoToml {
            cargoToml = ./inspector/Cargo.toml;
          };
          blokliInspectorCrateInfo = {
            pname = "blokli-inspector";
            # Normalize version to major.minor.patch for consistent caching
            version = pkgs.lib.strings.concatStringsSep "." (
              pkgs.lib.lists.take 3 (builtins.splitVersion blokliInspectorCrateInfoOriginal.version)
            );
          };

          # Create source trees for different build contexts using nix-lib
          sources = {
            main = nixLib.mkSrc {
              inherit fs;
              root = ./.;
              extraExtensions = [
                "csv"
                "graphql"
              ];
            };
            test = nixLib.mkTestSrc {
              inherit fs;
              root = ./.;
              extraExtensions = [
                "csv"
                "graphql"
                "snap"
              ];
            };
            deps = nixLib.mkDepsSrc {
              inherit fs;
              root = ./.;
            };
          };

          # Create all Rust builders for cross-compilation using nix-lib
          builders = nixLib.mkRustBuilders {
            rustToolchainFile = ./rust-toolchain.toml;
          };

          blokliClientPackages = import ./nix/packages/blokli-client.nix {
            inherit
              lib
              pkgs
              builders
              sources
              blokliClientCrateInfo
              rev
              nixLib
              ;
          };

          blokliInspectorPackages = import ./nix/packages/blokli-inspector.nix {
            inherit
              lib
              builders
              sources
              blokliInspectorCrateInfo
              rev
              nixLib
              ;
          };

          # Combine all packages
          packages =
            blokliClientPackages
            // blokliInspectorPackages
            // {
              # Additional standalone packages

              # Pre-commit hooks check
              pre-commit-check = pkgs.callPackage ./nix/packages/pre-commit-check.nix {
                inherit
                  pre-commit
                  system
                  config
                  ;
              };
            };

          utilityApps = {
            update-github-labels = nixLib.mkUpdateGithubLabelsApp;
            audit = nixLib.mkAuditApp { };
            check = nixLib.mkCheckApp { inherit system; };
            test = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "test" ''
                  nix develop --command ${pkgs.just}/bin/just test
                ''
              );
            };
            test-integration = {
              type = "app";
              program = toString (
                pkgs.writeShellApplication {
                  name = "test-integration";
                  runtimeInputs = [ pkgs.cargo-nextest ];
                  text = builtins.readFile ./nix/scripts/test-integration.sh;
                }
                + "/bin/test-integration"
              );
            };
            nextest = {
              type = "app";
              program = toString (
                pkgs.writeShellScript "nextest" ''
                  export PATH="${pkgs.cargo-nextest}/bin:$PATH"
                  nix develop --command ${pkgs.just}/bin/just nextest
                ''
              );
            };
          };

          # Rust toolchains
          stableToolchain =
            (pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override
              {
                targets = [
                  (
                    if buildPlatform.config == "arm64-apple-darwin" then
                      "aarch64-apple-darwin"
                    else
                      buildPlatform.config
                  )
                ];
              };

          nightlyToolchain = (pkgs.pkgsBuildHost.rust-bin.nightly.latest.default).override {
            targets = [
              (
                if buildPlatform.config == "arm64-apple-darwin" then
                  "aarch64-apple-darwin"
                else
                  buildPlatform.config
              )
            ];
            extensions = [
              "rust-src"
              "rust-analyzer"
              "clippy"
              "rustfmt"
            ];
          };

          # Development shells using nix-lib
          shellArgs = {
            treefmtWrapper = config.treefmt.build.wrapper;
            treefmtPrograms = pkgs.lib.attrValues config.treefmt.build.programs;
            shellHook = ''
              echo "Running pre-commit checks..."
              _github_token="''${GITHUB_TOKEN:-''${GH_TOKEN:-$(gh auth token 2>/dev/null || true)}}"
              if [ -n "$_github_token" ]; then
                export GITHUB_TOKEN="$_github_token"
              fi
              unset _github_token
              ${packages.pre-commit-check.shellHook}
            '';
            extraPackages = with pkgs; [
              gh
              nodejs
              foundry-bin
              pkgs.solc
              cargo-insta
              cargo-machete
              cargo-release
              cargo-shear
              yq
              uv
            ];
          };
          shells = {
            default = nixLib.mkDevShell (
              {
                rustToolchain = stableToolchain;
                shellName = "Development";
              }
              // shellArgs
            );

            experiment = nixLib.mkDevShell (
              {
                rustToolchain = nightlyToolchain;
                shellName = "Experimental Nightly";
              }
              // shellArgs
            );

            ci = nixLib.mkDevShell {
              rustToolchainFile = ./rust-toolchain.toml;
              shellName = "blokli CI";
              treefmtWrapper = config.treefmt.build.wrapper;
              treefmtPrograms = pkgs.lib.attrValues config.treefmt.build.programs;
              extraPackages = with pkgs; [
                cargo-machete
                cargo-release
                cargo-shear
                zizmor
              ];
            };
            coverage = nixLib.mkDevShell {
              rustToolchainFile = ./rust-toolchain.toml;
              shellName = "Coverage";
              withLlvmTools = true;
            };
          };

          # Import checks
          checks = import ./nix/checks.nix {
            inherit packages;
          };
        in
        {
          # Configure treefmt using nix-lib options
          nix-lib.treefmt = {
            globalExcludes = [
              # GraphQL schema source for Cynic codegen
              "client/target-api-schema.graphql"

              # locally installed npm packages
              ".npm/"
            ];
            extraFormatters = {
              programs.nixfmt.package = pkgs.nixfmt;
              programs.prettier.package = pkgs.prettier;
              settings.formatter.shfmt.includes = [
                "*.sh"
                "tests/**/*.sh"
              ];
              settings.formatter.yamlfmt.includes = [
                ".github/labeler.yml"
                ".github/workflows/*.yaml"
              ];
              # GraphQL formatter — uses prettier (nix-packaged) instead of bunx
              settings.formatter.format-graphql = {
                command = pkgs.writeShellApplication {
                  name = "format-graphql";
                  runtimeInputs = with pkgs; [
                    prettier
                  ];
                  text = ''
                    prettier --parser graphql --write "$@"
                  '';
                };
                includes = [ "client/*.graphql" ];
              };
              settings.formatter.graphql-schema-linter = {
                command = pkgs.writeShellApplication {
                  name = "graphql-schema-linter";
                  runtimeInputs = [ pkgs.nodejs ];
                  text = ''
                    npx --yes graphql-schema-linter "$@"
                  '';
                };
                includes = [ "client/*.graphql" ];
              };
              # Markdown formatter
              settings.formatter.deno = {
                command = pkgs.writeShellApplication {
                  name = "deno-fmt";
                  runtimeInputs = [ pkgs.deno ];
                  text = ''
                    deno fmt --config deno.json "$@"
                  '';
                };
                includes = [
                  "**/*.md"
                  "*.md"
                ];
              };
              # GitHub Actions workflow linter
              settings.formatter.actionlint = {
                command = pkgs.writeShellApplication {
                  name = "actionlint";
                  runtimeInputs = [ pkgs.actionlint ];
                  text = ''
                    actionlint "$@"
                  '';
                };
                includes = [ ".github/workflows/*.yaml" ];
              };
            };
          };

          # Export checks for CI
          inherit checks;

          # Export applications using nix-lib
          apps = utilityApps;

          # Export packages
          packages = packages // {
            # Set default package
            default = packages.lib-blokli-client;
          };

          # Export development shells
          devShells = shells;

          # Formatter is automatically exported by nix-lib.flakeModules.default
        };

      # Supported systems for building
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
    };
}
