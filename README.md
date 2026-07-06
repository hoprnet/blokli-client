# Blokli Client

[![codecov](https://codecov.io/gh/hoprnet/blokli-client/branch/main/graph/badge.svg)](https://codecov.io/gh/hoprnet/blokli-client)

This repository contains the Rust client library for Blokli's GraphQL API and transaction endpoints.

## Components

- `client/`: `blokli-client` library, Cynic GraphQL types, query helpers, subscriptions, transactions, and test utilities
- `inspector/`: CLI helper for inspecting a running Blokli instance through the client
- `tests/integration/`: client integration tests against a running Blokli-compatible environment

## Development

This project uses [just](https://github.com/casey/just) as a command runner and
[Nix Flakes](https://nix.dev/manual/nix/2.30/command-ref/new-cli/nix3-flake.html#description) for the development environment.

Enter the Nix development environment:

```bash
nix develop
```

Build the workspace:

```bash
just build
```

Run unit tests:

```bash
just test
```

Format, lint, and check compilation:

```bash
just quick
```

Run the inspector:

```bash
just run-inspector --help
```

## Client DNS Override

`blokli-client` uses system DNS by default. For deployments where Blokli is reachable through a stable, VPN-exempt IP but DNS can be
unreliable, callers can configure a DNS override in `BlokliClientConfig`.

The Blokli URL should stay hostname-based. The override pins DNS resolution for that hostname, so TLS SNI and certificate validation still
use the original hostname. If `BlokliDnsOverride::port` is set, Blokli uses that port for requests; otherwise it uses the original URL port
or the scheme default.

```rust
use std::net::IpAddr;

use blokli_client::{BlokliClient, BlokliClientConfig, BlokliDnsOverride};

fn build_client() -> Result<BlokliClient, Box<dyn std::error::Error>> {
    Ok(BlokliClient::new(
        "https://blokli.example.org".parse()?,
        BlokliClientConfig {
            dns_override: Some(BlokliDnsOverride {
                ip: IpAddr::from([203, 0, 113, 10]),
                port: None,
            }),
            ..Default::default()
        },
    ))
}
```

## GraphQL Schema

The client code generation schema lives at `client/target-api-schema.graphql`.

When the upstream Blokli API schema changes, update this file and regenerate/check the client by running:

```bash
just quick
```

## Testing

Run all non-integration tests:

```bash
just test
```

Run a specific package:

```bash
just test-package blokli-client
just test-package blokli-inspector
```

Run integration tests:

```bash
just integration-test
```

Integration tests expect their external Blokli-compatible environment to be configured by the test fixtures and environment variables under
`tests/integration/`.

## Repository Layout

- `client/src/api/`: schema-facing GraphQL modules
- `client/src/client/`: public client implementation
- `client/src/errors.rs`: client error types
- `client/tests/`: client unit and mock-server tests
- `inspector/src/`: inspector CLI implementation
- `tests/integration/`: integration test fixtures and test cases

## Useful Commands

```bash
just                 # list available commands
just fmt             # format
just clippy          # lint
just check           # cargo check
just nextest         # run unit tests with nextest
nix build .          # build default Nix package
nix build .#checks.$(nix eval --impure --raw --expr builtins.currentSystem).blokli-client-clippy
```
