# Blokli Client

`blokli-client` is the Rust client library for [Blokli](https://hoprnet.org/)'s GraphQL API and transaction endpoints.

## Usage

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

## Features

- `serde`: derive `Serialize`/`Deserialize` on the public response models
- `testing`: expose `BlokliTestClient`, an in-memory client implementing the same query, subscription, and transaction traits, backed by a
  `BlokliTestState`. Tests can provide a `BlokliTestStateMutator` to model the effects of submitted signed transactions without running a
  Blokli service.

```toml
blokli-client = { version = "...", features = ["testing"] }
```

## Related crates

- [`blokli-inspector`](https://crates.io/crates/blokli-inspector): CLI tool for inspecting a running Blokli instance through this client

## License

GPL-3.0-only
