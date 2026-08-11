# Blokli Client

[![codecov](https://codecov.io/gh/hoprnet/blokli-client/branch/main/graph/badge.svg)](https://codecov.io/gh/hoprnet/blokli-client)

This repository contains the Rust client library for Blokli's GraphQL API and transaction endpoints.

## Using the Client

The main entry point is `BlokliClient`. Query, subscription, and transaction methods are provided by traits, so import the trait for the API
family you want to use:

```rust
use blokli_client::{BlokliClient, BlokliClientConfig, BlokliQueryClient};

async fn query_version() -> Result<String, Box<dyn std::error::Error>> {
    let client = BlokliClient::new(
        "https://blokli.example.org".parse()?,
        BlokliClientConfig::default(),
    );

    Ok(client.query_version().await?)
}
```

Common public items are re-exported at the crate root:

- `BlokliQueryClient`: one-shot GraphQL queries
- `BlokliSubscriptionClient`: SSE-backed subscriptions
- `BlokliTransactionClient`: signed transaction submission and tracking
- selector types such as `AccountSelector`, `ChannelSelector`, `SafeSelector`, and `TicketSelector`
- response models under `blokli_client::types`

Full API documentation, including advanced configuration such as timeouts, subscriptions, transactions, and DNS overrides, is available on
[docs.rs](https://docs.rs/blokli-client).

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

## Test Utilities

The `testing` feature exposes an in-memory test client:

```toml
blokli-client = { version = "...", features = ["testing"] }
```

`BlokliTestClient` implements the same query, subscription, and transaction traits as `BlokliClient`, backed by a `BlokliTestState`. Tests
can provide a `BlokliTestStateMutator` to model the effects of submitted signed transactions without running a Blokli service.

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
BLOKLI_TEST_REMOTE_IMAGE='europe-west3-docker.pkg.dev/hoprassociation/docker-images/bloklid-anvil:latest' \
  nix run .#test-integration
```

The command builds a cacheable Nextest archive, pulls the self-contained `bloklid-anvil` image, and runs one isolated container per test
binary. The image embeds Anvil, deployed HOPR contracts, and Blokli. Replace `<digest>` with the immutable digest to test; explicitly
setting `BLOKLI_TEST_REMOTE_IMAGE` overrides the pinned default image.

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
nix build .#checks.$(nix eval --impure --raw --expr builtins.currentSystem).clippy
```
