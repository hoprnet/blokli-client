//! Rust client for the Blokli GraphQL API and transaction endpoints.
//!
//! `blokli-client` provides a typed, async interface to a Blokli service. It wraps Blokli's GraphQL API with
//! ergonomic Rust traits for:
//!
//! - querying accounts, balances, channels, safes, ticket statistics, chain metadata, and transaction state;
//! - subscribing to server-sent event streams for accounts, channels, health, graph changes, safe deployments, ticket
//!   redemptions, and tracked transactions;
//! - submitting signed transactions and optionally waiting for tracking or confirmation.
//!
//! The main entry point is [`BlokliClient`]. Most operations are trait methods, so bring the trait for the operation
//! family into scope:
//!
//! - [`BlokliQueryClient`] for one-shot GraphQL queries;
//! - [`BlokliSubscriptionClient`] for streaming subscriptions;
//! - [`BlokliTransactionClient`] for signed transaction submission and tracking.
//!
//! # API concepts
//!
//! A [`BlokliClient`] is configured with the Blokli service base URL, not the GraphQL endpoint itself. For example,
//! `https://blokli.example.org` becomes `https://blokli.example.org/graphql` for GraphQL requests and SSE
//! subscriptions.
//!
//! Queries and subscriptions use typed selectors instead of generic filter maps. Address-like values such as
//! [`ChainAddress`], [`ChannelId`], [`PacketKey`], and [`TxReceipt`] are byte arrays at the public boundary and are
//! encoded as hex strings for GraphQL. Blokli-specific identifiers such as [`KeyId`] and [`TxId`] are kept distinct:
//! a [`TxId`] is a Blokli tracking id, while a [`TxReceipt`] is the on-chain transaction hash returned by submission
//! endpoints.
//!
//! Subscriptions are GraphQL operations delivered over server-sent events. They yield streams of `Result<T, E>` so
//! callers can decide how to handle item-level errors. Transaction helpers operate on already-signed raw transaction
//! bytes; this crate does not sign transactions.
//!
//! # Quick start
//!
//! Create a client with a Blokli base URL. The client derives the GraphQL endpoint by appending `/graphql` to that
//! base URL.
//!
//! ```no_run
//! use blokli_client::{BlokliClient, BlokliClientConfig, BlokliQueryClient};
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = BlokliClient::new("https://blokli.example.org".parse()?, BlokliClientConfig::default());
//!
//!     let version = client.query_version().await?;
//!     let chain = client.query_chain_info().await?;
//!
//!     println!("Blokli {version} indexes chain {}", chain.chain_id);
//!     Ok(())
//! }
//! ```
//!
//! # Querying
//!
//! Selectors are strongly typed. For example, channel queries use [`ChannelSelector`] plus an optional
//! [`ChannelFilter`] and status.
//!
//! ```no_run
//! use blokli_client::{BlokliClient, BlokliClientConfig, BlokliQueryClient, ChannelFilter, ChannelSelector};
//!
//! async fn example(source: u32) -> Result<(), Box<dyn std::error::Error>> {
//!     let client = BlokliClient::new("https://blokli.example.org".parse()?, BlokliClientConfig::default());
//!     let selector = ChannelSelector {
//!         filter: Some(ChannelFilter::SourceKeyId(source)),
//!         ..Default::default()
//!     };
//!
//!     let channels = client.query_channels(selector).await?;
//!     println!("{} channels found", channels.channels.len());
//!     Ok(())
//! }
//! ```
//!
//! # Subscriptions
//!
//! Subscription methods return [`futures::Stream`] values. Streams use Blokli's SSE endpoint and yield `Result` items
//! so callers can decide how to handle transient transport, parsing, or GraphQL errors.
//!
//! ```no_run
//! use blokli_client::{AccountSelector, BlokliClient, BlokliClientConfig, BlokliSubscriptionClient};
//! use futures::TryStreamExt;
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = BlokliClient::new("https://blokli.example.org".parse()?, BlokliClientConfig::default());
//!     let mut accounts = Box::pin(client.subscribe_accounts(AccountSelector::Any)?);
//!
//!     while let Some(account) = accounts.try_next().await? {
//!         println!("account key id: {}", account.keyid);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Transactions
//!
//! Transaction methods expect already-signed raw transactions. `submit_transaction` returns immediately after
//! submission, `submit_and_track_transaction` returns a Blokli tracking id, and `submit_and_confirm_transaction`
//! waits for the requested number of confirmations.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use blokli_client::{BlokliClient, BlokliClientConfig, BlokliTransactionClient};
//!
//! async fn example(signed_transaction: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
//!     let client = BlokliClient::new("https://blokli.example.org".parse()?, BlokliClientConfig::default());
//!
//!     let tx_id = client.submit_and_track_transaction(signed_transaction).await?;
//!     let transaction = client.track_transaction(tx_id, Duration::from_secs(120)).await?;
//!
//!     println!("transaction status: {:?}", transaction.status);
//!     Ok(())
//! }
//! ```
//!
//! # Public API layout
//!
//! Common selectors, traits, and address aliases are re-exported at the crate root. The versioned API remains available
//! under [`api::v1`], and GraphQL response models are grouped under [`types`].
//!
//! # Errors
//!
//! Fallible operations return [`BlokliClientError`]. Use [`BlokliClientError::kind`] when matching on stable client
//! error categories such as invalid input, GraphQL errors, timeouts, or transaction tracking failures.
//!
//! # DNS override
//!
//! By default, [`BlokliClient`] uses the system DNS resolver through `reqwest`. Callers that need to keep Blokli
//! communication working while DNS is unreliable can configure [`BlokliClientConfig::dns_override`] to pin the Blokli
//! URL hostname to a fixed IP address.
//!
//! The request hostname is not rewritten. For example, a client configured with `https://blokli.example.org` and a DNS
//! override still sends requests for `blokli.example.org`, preserving TLS SNI and certificate validation while
//! bypassing system DNS for that hostname. When [`BlokliDnsOverride::port`] is set, it becomes the request port;
//! otherwise the original URL port or scheme default is used.
//!
//! ```no_run
//! use std::net::IpAddr;
//!
//! use blokli_client::{BlokliClient, BlokliClientConfig, BlokliDnsOverride};
//!
//! let client = BlokliClient::new(
//!     "https://blokli.example.org".parse()?,
//!     BlokliClientConfig {
//!         dns_override: Some(BlokliDnsOverride {
//!             ip: IpAddr::from([203, 0, 113, 10]),
//!             port: None,
//!         }),
//!         ..Default::default()
//!     },
//! );
//! # let _ = client;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Leave `dns_override` as `None` to use normal system DNS resolution.
/// Current Blokli client API.
pub mod api;
mod client;
/// Errors returned by the Blokli client.
pub mod errors;

/// Version of the `blokli-client` crate.
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

pub use api::{
    AccountSelector, BlokliQueryClient, BlokliSubscriptionClient, BlokliTransactionClient, ChainAddress, ChannelFilter,
    ChannelId, ChannelSelector, KeyId, ModulePredictionInput, PacketKey, RedeemedStatsSelector, SafeSelector,
    ServiceSelector, ServiceTypeId, TicketSelector, TxId, TxReceipt, types,
};
pub use client::{BlokliClient, BlokliClientConfig, BlokliDnsOverride, ReqwestTransport};
#[cfg(feature = "testing")]
pub use client::{
    BlokliTestClient, BlokliTestState, BlokliTestStateMutator, BlokliTestStateSnapshot, GraphQlQueries, NopStateMutator,
};
pub use errors::{BlokliClientError, ErrorKind, TrackingErrorKind};

#[cfg(feature = "testing")]
pub mod internal {
    pub use super::api::internal::*;
}

#[doc(hidden)]
pub mod exports {
    pub use url::Url;
    #[cfg(feature = "testing")]
    pub use {
        cynic::{Operation, StreamingOperation},
        indexmap::{IndexMap, map::Entry},
    };
}
