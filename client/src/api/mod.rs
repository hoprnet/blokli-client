//! Versioned Blokli API traits, selectors, aliases, and response models.
//!
//! Most applications can import the common items directly from the crate root:
//!
//! ```no_run
//! use blokli_client::{BlokliClient, BlokliClientConfig, BlokliQueryClient};
//! ```
//!
//! This module keeps the underlying versioned API available for callers that want to be explicit about the Blokli
//! schema version they are targeting. The current public API is v1.
//!
//! # What lives here
//!
//! - [`BlokliQueryClient`] for one-shot GraphQL queries.
//! - [`BlokliSubscriptionClient`] for SSE-backed GraphQL subscriptions.
//! - [`BlokliTransactionClient`] for signed transaction submission and tracking.
//! - Selectors such as [`AccountSelector`], [`ChannelSelector`], [`SafeSelector`], [`ServiceSelector`], and
//!   [`TicketSelector`].
//! - GraphQL response models under [`types`].
//!
//! Address-like values are byte-array aliases such as [`ChainAddress`], [`ChannelId`], [`PacketKey`], and
//! [`TxReceipt`]. GraphQL fields that are naturally decimal or hex strings remain represented by the generated
//! response model types under [`types`].

pub mod v1;

pub(crate) use v1::{Result, internal};

pub const VERSION: &str = "v1";
pub use v1::{
    AccountSelector, BlokliQueryClient, BlokliSubscriptionClient, BlokliTransactionClient, ChainAddress, ChannelFilter,
    ChannelId, ChannelSelector, KeyId, ModulePredictionInput, PacketKey, RedeemedStatsSelector, SafeSelector,
    ServiceSelector, ServiceTypeId, TicketSelector, TxId, TxReceipt, types,
};
