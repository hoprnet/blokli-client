# Agent Guidelines for HOPR Blokli Client

`blokli-client` is the Rust client library for talking to Blokli's GraphQL API and transaction endpoints.

## Project Overview

- `client/` — `blokli-client` library, generated Cynic GraphQL types, query helpers, subscriptions, transaction helpers, and test support
- `inspector/` — CLI/TUI-style helper for inspecting Blokli data through the client
- `tests/integration/` — client integration tests against a running Blokli-compatible test environment
- `tests/smoke/` — smoke-test assets for validating client behavior against deployed or containerized services

**Tech stack:** Rust, Tokio, Cynic, reqwest, eventsource-client, HOPR foundation types, Nix Flakes, and `just`.

## Quick Reference

**After making code changes**, run `just quick` from inside `nix develop` when available. It should format, lint, and check compilation.

For narrower validation, use package-specific commands such as:

```bash
just test-package blokli-client
cargo test -p blokli-client
cargo test -p blokli-inspector
```

Run `just` with no arguments to see the currently available commands.

## Documentation Map

- `README.md` — Project overview, quickstart, and client usage examples
- `client/target-api-schema.graphql` — GraphQL schema used by Cynic code generation
- `client/src/api/` — Generated or schema-facing API types
- `client/src/client/` — Public client implementation for queries, subscriptions, transactions, and test helpers
- `client/src/errors.rs` — Client error types and conversions

## Build Commands

All `just` commands must be run within the `nix develop` shell unless the command explicitly says otherwise.

Use workspace dependencies from the root `Cargo.toml`. Keep package manifests focused on package-specific features and metadata.

## Code Style

### Naming & Formatting

- snake_case for functions and variables, PascalCase for types and enums
- 4 spaces indentation
- `//!` for module docs, `///` for item docs
- Explicit type annotations on all public function signatures

### Imports

**All imports at module top level** — never inside functions or impl blocks.

Group imports in this order:

1. `std::`
2. external crates alphabetically
3. local crates/modules alphabetically

```rust
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use url::Url;

use crate::client::queries::BlokliQueryClient;
use crate::errors::BlokliClientError;
```

- **No wildcard imports** (`use module::*`)
- **No inline fully-qualified paths** — add imports instead
- Keep generated/schema-facing code consistent with the surrounding generated API modules

### Error Handling

- Use `thiserror::Error` for custom error types
- Use `anyhow::Result` only for application-level code, examples, tests, and binaries where appropriate
- Return `Result<T, E>` for fallible library operations
- No `unwrap()` or `expect()` in production library code
- Preserve useful context when converting transport, GraphQL, serialization, URL, or subscription errors

### Async Code

- Use `async/await` and Tokio-compatible primitives
- Do not block inside async contexts
- Keep retry, timeout, DNS override, and subscription behavior explicit and testable
- Prefer streaming abstractions for subscriptions and event streams instead of buffering unbounded data

## HOPR Foundation Types

Prefer HOPR foundation types over creating local replacements for addresses, balances, channels, accounts, and crypto primitives:

| Crate        | Key Types                                                                                          |
| ------------ | -------------------------------------------------------------------------------------------------- |
| `hopr-types` | `Address`, `Balance<C>`, `HoprBalance`, `XDaiBalance`, `U256`, `Hash`, and related HOPR primitives |

Use the available prelude modules such as `hopr_types::primitive::prelude` and `hopr_types::crypto::prelude` when they fit the module.
Implement `From`/`TryFrom` conversions at the client boundary when translating GraphQL data into public Rust types.

## Client Design

### GraphQL API

- Treat `client/target-api-schema.graphql` as the schema contract for generated Cynic query types
- Keep query definitions close to the relevant API modules under `client/src/api/v1/graphql/`
- Keep public client methods ergonomic and strongly typed; avoid leaking raw GraphQL response shapes unless that is already the local
  pattern
- When adding API coverage, include both the GraphQL selection and the public client method that callers should use

### Subscriptions

- Subscription code lives in `client/src/client/subscriptions.rs`
- Use SSE/event streams consistently with the existing `eventsource-client` integration
- Handle reconnects, stream termination, and parse errors deliberately
- Test happy paths and malformed/error events with mocks where practical

### Transactions

- Transaction helpers live in `client/src/client/transactions.rs`
- Keep transaction submission mode and response handling explicit
- Do not assume permanence from in-memory or async submission state; callers should rely on on-chain confirmation or API results for durable
  status

### Inspector

- `inspector/` is a consumer of the client library
- Keep inspector-specific formatting, table rendering, and CLI concerns out of the library
- Use the inspector when a behavior is useful for humans but not part of the reusable client API

## Testing

- Unit tests: `#[cfg(test)]` in the same file when close to the implementation
- Async tests: use `#[tokio::test]`
- Mock external HTTP/SSE dependencies for client behavior where possible
- Test error paths as well as happy paths, especially transport errors, GraphQL errors, malformed responses, and subscription termination

### Assertion Strategy

- Use `insta` snapshots for complex objects, nested response shapes, rendered tables, or lists:

  ```rust
  let result = client_response_to_summary(response)?;
  insta::assert_yaml_snapshot!(result);
  ```

- Prefer a single snapshot over many `assert_eq` calls when asserting a structured value
- Use simple `assert`/`assert_eq` for single scalar values or clear error variants:

  ```rust
  assert_eq!(summary.channel_count, 2);
  assert!(result.is_err());
  ```

### Integration Tests

Integration tests live in `tests/integration/` and exercise the client against a real or containerized Blokli-compatible service.

**When to use:** end-to-end client queries, subscriptions, transaction flows, response compatibility, and load behavior.

**When not to use:** pure parsing, conversion logic, table rendering, or behavior that can be covered with local mocks.

Use package-specific integration commands from `just` when available, or run:

```bash
cargo test -p blokli-integration-tests
```

## What to Avoid

- Wildcard imports, imports inside functions/impl blocks, inline fully-qualified paths
- Creating custom types when HOPR foundation types already cover the domain
- Hardcoding GraphQL error strings or response shapes when typed schema/query support exists
- Missing type annotations on public functions
- `unwrap()`/`expect()` in production library code
- Blocking operations in async contexts
- Unbounded buffering of subscription streams or large responses
- Mixing inspector presentation concerns into the reusable client API

## Security & Performance

- Validate external inputs such as URLs, DNS override values, and user-provided request parameters
- Do not log secrets, private keys, bearer tokens, or full sensitive payloads
- Preserve TLS hostname validation when using DNS overrides
- Use request timeouts, bounded retries, and backoff where appropriate
- Keep pagination and streaming behavior explicit to avoid accidental large memory usage
- Keep Zstandard and other compression behavior compatible with the underlying HTTP client configuration

## Development Workflow

1. Make focused changes
2. Run `just quick`
3. Run `just test` or targeted tests such as `cargo test -p blokli-client`
4. Update snapshots intentionally when expected output changes
5. Commit

## Additional Resources

- [Cynic](https://cynic-rs.dev/)
- [reqwest](https://docs.rs/reqwest/)
- [eventsource-client](https://docs.rs/eventsource-client/)
- [Tokio](https://tokio.rs/)
