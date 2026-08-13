//! Blokli API v1 client contract.
//!
//! This module contains the public trait surface implemented by [`BlokliClient`](crate::BlokliClient), plus the
//! selectors and response models used by those traits.
//!
//! # Trait families
//!
//! Use these traits to make the corresponding methods available on [`BlokliClient`](crate::BlokliClient):
//!
//! - [`BlokliQueryClient`] for request/response GraphQL queries.
//! - [`BlokliSubscriptionClient`] for SSE-backed GraphQL subscriptions.
//! - [`BlokliTransactionClient`] for signed transaction submission and tracking.
//!
//! # Selectors
//!
//! Query and subscription methods avoid unstructured filter maps. Instead, pass one of the selector types:
//!
//! - [`AccountSelector`] selects accounts by key id, chain address, packet key, or all accounts.
//! - [`ChannelSelector`] combines an optional [`ChannelFilter`], channel status, and safe address.
//! - [`SafeSelector`] selects safes by safe address, owner, chain key alias, or registered node.
//! - [`RedeemedStatsSelector`] selects ticket redemption aggregates.
//! - [`ServiceSelector`] selects service registry entries by service type, node, or both.
//! - [`TicketSelector`] filters ticket redemption subscription events.
//!
//! # Response models
//!
//! The [`types`] module re-exports the schema-facing GraphQL response structs and enums that are returned by public
//! methods. These models intentionally mirror the Blokli API shape while conversions at the client boundary normalize
//! common success/error unions into `Result` values.
//!
//! # Example
//!
//! ```no_run
//! use blokli_client::{BlokliClient, BlokliClientConfig, BlokliQueryClient, ChannelFilter, ChannelSelector};
//!
//! async fn example(destination: u32) -> Result<(), Box<dyn std::error::Error>> {
//!     let client = BlokliClient::new("https://blokli.example.org".parse()?, BlokliClientConfig::default());
//!     let selector = ChannelSelector {
//!         filter: Some(ChannelFilter::DestinationKeyId(destination)),
//!         ..Default::default()
//!     };
//!     let stats = client.query_channel_stats(selector).await?;
//!
//!     println!("{} channels matched", stats.count);
//!     Ok(())
//! }
//! ```

use std::{fmt::Formatter, time::Duration};

pub(crate) mod graphql;
pub mod types {
    pub use super::graphql::{
        ChannelStatus, DateTime, Hex32, ReadinessState, Token, TokenValueString, Uint64,
        accounts::Account,
        balances::{HoprBalance, NativeBalance, RedeemedStats, SafeHoprAllowance},
        channels::{Channel, ChannelStats, ChannelsList, SafesBalance},
        graph::OpenedChannelsGraphEntry,
        info::{ChainInfo, Compatibility, ContractAddressMap, TicketParameters},
        safe::{ModuleAddress, Safe},
        services::{
            ServiceEntry, ServiceRegistryConfig, ServiceTypeInfo, ServiceTypeUpdate, ServiceTypeUpdateKind,
            ServiceUpdate, ServiceUpdateKind,
        },
        tickets::{RedeemTicketDetails, RedemptionResult},
        txs::{SafeExecution, Transaction, TransactionStatus},
    };
}

pub(crate) mod internal {
    pub use super::graphql::{
        accounts::{
            AccountVariables, QueryAccountCount, QueryAccounts, QueryTxCount, SubscribeAccounts, TxCountVariables,
        },
        balances::{
            BalanceVariables, QueryHoprBalance, QueryNativeBalance, QueryRedeemedStats, QuerySafeAllowance,
            RedeemedStatsFilter, RedeemedStatsVariables,
        },
        channels::{
            ChannelStatsVariables, ChannelsVariables, QueryChannelCount, QueryChannelStats, QueryChannels,
            QuerySafesBalance, SafesBalanceVariables, SubscribeChannels,
        },
        graph::SubscribeGraph,
        info::{QueryChainInfo, QueryCompatibility, QueryHealth, QueryVersion, SubscribeHealth, SubscribeTicketParams},
        safe::{
            ModuleAddressVariables, QueryModuleAddress, QuerySafeBy, SafeByVariables, SafeSelectorInput,
            SubscribeSafeDeployment,
        },
        services::{
            QueryServiceCount, QueryServiceRegistryConfig, QueryServiceTypes, QueryServices, ServicePageVariables,
            ServiceTypeVariables, ServiceVariables, SubscribeServiceRegistryConfig, SubscribeServiceTypes,
            SubscribeServices,
        },
        tickets::{SubscribeTicketRedeemed, TicketRedeemedVariables},
        txs::{
            ConfirmTransactionVariables, MutateConfirmTransaction, MutateSendTransaction, MutateTrackTransaction,
            QueryTransaction, SendTransactionVariables, SubscribeTransaction, TransactionsVariables,
        },
    };
}

/// EVM-style 20-byte chain address used by Blokli account, safe, and node filters.
pub type ChainAddress = [u8; 20];
/// HOPR packet key used to identify accounts.
pub type PacketKey = [u8; 32];
/// Concrete 32-byte payment channel identifier.
pub type ChannelId = [u8; 32];
/// Service type identifier used by the on-chain service registry.
///
/// The registry stores the identifier as a raw `bytes32`. By convention it holds right-padded
/// printable ASCII, so `gvpn:exit` is
/// `0x6776706e3a657869740000000000000000000000000000000000000000000000`, but the contract does not
/// enforce that, and any non-zero 32-byte value can appear on chain. Blokli renders the identifier
/// as its ASCII name when it follows the convention and as `0x`-prefixed hex otherwise, so the
/// string fields of [`ServiceEntry`](types::ServiceEntry) and
/// [`ServiceTypeInfo`](types::ServiceTypeInfo) may hold either form.
pub type ServiceTypeId = [u8; 32];
/// Transaction receipt or hash returned by transaction submission endpoints.
pub type TxReceipt = [u8; 32];
/// Numeric Blokli key id.
pub type KeyId = u32;
/// Blokli transaction tracking identifier.
///
/// This id is returned by [`BlokliTransactionClient::submit_and_track_transaction`] and can be passed to
/// [`BlokliQueryClient::query_transaction_status`], [`BlokliSubscriptionClient::subscribe_track_transaction`], or
/// [`BlokliTransactionClient::track_transaction`].
pub type TxId = String;

/// Selects [`Account`](types::Account) records by key id, chain address, packet key, or all accounts.
///
/// `AccountSelector::Any` is accepted by [`BlokliQueryClient::count_accounts`] and
/// [`BlokliSubscriptionClient::subscribe_accounts`]. [`BlokliQueryClient::query_accounts`] requires a narrower
/// selector to avoid accidentally fetching an unbounded account list.
#[derive(Clone)]
pub enum AccountSelector {
    /// Select an account by its key id.
    KeyId(KeyId),
    /// Select an account by its on-chain address.
    Address(ChainAddress),
    /// Select an account by its packet key.
    PacketKey(PacketKey),
    /// Matches any account.
    Any,
}

impl std::fmt::Debug for AccountSelector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyId(key_id) => write!(f, "KeyId({})", key_id),
            Self::Address(address) => write!(f, "Address({})", hex::encode(address)),
            Self::PacketKey(packet_key) => write!(f, "PacketKey({})", hex::encode(packet_key)),
            AccountSelector::Any => write!(f, "Any"),
        }
    }
}

/// Selects [`Channel`](types::Channel) records by optional channel filter, status, and safe contract address.
///
/// Use [`ChannelSelector::default`] to address all channels when a method supports unfiltered access. Query methods
/// that could otherwise return large result sets may require at least one filter.
#[derive(Debug, Clone, Default)]
pub struct ChannelSelector {
    /// Filter for the selected channels.
    pub filter: Option<ChannelFilter>,
    /// Optional status filter for the selected channels.
    pub status: Option<types::ChannelStatus>,
    /// Optional safe contract address; restricts to channels where the source belongs to this safe.
    pub safe_address: Option<ChainAddress>,
}

impl ChannelSelector {
    /// Returns `true` if the selector matches any channel.
    pub fn matches_all(&self) -> bool {
        self.filter.is_none() && self.status.is_none() && self.safe_address.is_none()
    }
}

/// Filters [`Channel`](types::Channel) records by channel id, source key id, destination key id, or both endpoint key
/// ids.
#[derive(Clone)]
pub enum ChannelFilter {
    /// Select a channel by its channel id.
    ChannelId(ChannelId),
    /// Select channels by its destination key id.
    DestinationKeyId(KeyId),
    /// Select channels by its source key id.
    SourceKeyId(KeyId),
    /// Select channels by both source and destination key id.
    SourceAndDestinationKeyIds(KeyId, KeyId),
}

impl std::fmt::Debug for ChannelFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChannelId(channel_id) => write!(f, "ChannelId({})", hex::encode(channel_id)),
            Self::DestinationKeyId(key_id) => write!(f, "DestinationKeyId({})", key_id),
            Self::SourceKeyId(key_id) => write!(f, "SourceKeyId({})", key_id),
            Self::SourceAndDestinationKeyIds(source_key_id, destination_key_id) => write!(
                f,
                "SourceAndDestinationKeyIds({}, {})",
                source_key_id, destination_key_id
            ),
        }
    }
}

/// Selects deployed [`Safe`](types::Safe) records by safe address, owner, chain key alias, or registered node.
#[derive(Clone)]
pub enum SafeSelector {
    /// Select a safe by its address.
    SafeAddress(ChainAddress),
    /// Select a safe by a current owner address.
    Owner(ChainAddress),
    /// Select a safe by the owner's chain key legacy alias.
    ChainKey(ChainAddress),
    /// Select a safe by any of the registered nodes.
    RegisteredNode(ChainAddress),
}

impl std::fmt::Debug for SafeSelector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SafeAddress(address) => write!(f, "SafeAddress({})", hex::encode(address)),
            Self::Owner(address) => write!(f, "Owner({})", hex::encode(address)),
            Self::ChainKey(address) => write!(f, "ChainKey({})", hex::encode(address)),
            Self::RegisteredNode(address) => write!(f, "RegisteredNode({})", hex::encode(address)),
        }
    }
}

/// Selects [`ServiceEntry`](types::ServiceEntry) records by service type, node address, or both.
///
/// `ServiceSelector::Any` is accepted by [`BlokliQueryClient::count_services`] and
/// [`BlokliSubscriptionClient::subscribe_services`]. [`BlokliQueryClient::query_services`] requires
/// a narrower selector: the registry is permissionless and anyone can grow it, so a bare
/// enumeration is not offered.
#[derive(Clone, Copy)]
pub enum ServiceSelector {
    /// Select every entry of one service type.
    ServiceType(ServiceTypeId),
    /// Select every entry offered by one node.
    Node(ChainAddress),
    /// Select the single entry for one service type and one node.
    ServiceTypeAndNode {
        /// Service type identifier.
        service_type: ServiceTypeId,
        /// Node chain address.
        node: ChainAddress,
    },
    /// Matches any registry entry.
    Any,
}

impl std::fmt::Debug for ServiceSelector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServiceType(service_type) => write!(f, "ServiceType({})", hex::encode(service_type)),
            Self::Node(node) => write!(f, "Node({})", hex::encode(node)),
            Self::ServiceTypeAndNode { service_type, node } => write!(
                f,
                "ServiceTypeAndNode(service_type={}, node={})",
                hex::encode(service_type),
                hex::encode(node)
            ),
            Self::Any => write!(f, "Any"),
        }
    }
}

/// Allows querying redeemed ticket aggregates by safe address, node address, or both.
#[derive(Clone, Copy)]
pub enum RedeemedStatsSelector {
    /// Aggregate all rows for the given safe address.
    SafeAddress(ChainAddress),
    /// Aggregate all rows for the given node address.
    NodeAddress(ChainAddress),
    /// Return the single row matching the given safe/node pair.
    SafeAndNodeAddress {
        /// Safe contract address.
        safe_address: ChainAddress,
        /// Node address.
        node_address: ChainAddress,
    },
}

impl std::fmt::Debug for RedeemedStatsSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SafeAddress(safe) => write!(f, "SafeAddress({})", hex::encode(safe)),
            Self::NodeAddress(node) => write!(f, "NodeAddress({})", hex::encode(node)),
            Self::SafeAndNodeAddress {
                safe_address,
                node_address,
            } => write!(
                f,
                "SafeAndNodeAddress(safe={}, node={})",
                hex::encode(safe_address),
                hex::encode(node_address)
            ),
        }
    }
}

/// Filters which ticket redemption events are delivered by a [`BlokliSubscriptionClient::subscribe_ticket_redeemed`]
/// subscription.
///
/// Pass one of the variants to receive only events matching that criterion, or [`TicketSelector::Any`] to receive all
/// events.
///
/// # Examples
///
/// ```ignore
/// use blokli_client::api::v1::{TicketSelector, ChannelId, ChainAddress};
///
/// // Subscribe to all redemptions in a specific channel
/// let by_channel = TicketSelector::ChannelId(channel_id);
///
/// // Subscribe to all redemptions where a specific node is the issuer
/// let by_issuer = TicketSelector::IssuerAddress(issuer_address);
///
/// // Subscribe to every redemption event regardless of channel or party
/// let any = TicketSelector::Any;
/// ```
#[derive(Clone)]
pub enum TicketSelector {
    /// Filter by channel id.
    ChannelId(ChannelId),
    /// Filter by issuer (source node) address.
    IssuerAddress(ChainAddress),
    /// Filter by recipient (destination node) address.
    RecipientAddress(ChainAddress),
    /// Matches any ticket redemption event.
    Any,
}

impl std::fmt::Debug for TicketSelector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChannelId(channel_id) => write!(f, "ChannelId({})", hex::encode(channel_id)),
            Self::IssuerAddress(address) => write!(f, "IssuerAddress({})", hex::encode(address)),
            Self::RecipientAddress(address) => write!(f, "RecipientAddress({})", hex::encode(address)),
            Self::Any => write!(f, "Any"),
        }
    }
}

/// Input for [`BlokliQueryClient::query_module_address_prediction`].
#[derive(Clone, PartialEq, Eq)]
pub struct ModulePredictionInput {
    /// Safe deployment nonce.
    pub nonce: u64,
    /// Owner of the deployed Safe.
    pub owner: ChainAddress,
    /// Predicted Safe address.
    pub safe_address: ChainAddress,
}

impl std::fmt::Debug for ModulePredictionInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModulePredictionInput")
            .field("nonce", &self.nonce)
            .field("owner", &hex::encode(self.owner))
            .field("safe_address", &hex::encode(self.safe_address))
            .finish()
    }
}

pub(crate) type Result<T> = std::result::Result<T, crate::errors::BlokliClientError>;

/// One-shot GraphQL queries against a Blokli instance.
///
/// These methods return the current indexed state known to Blokli at request time. They do not subscribe for later
/// changes and they do not retry application-level GraphQL errors. Transport, decoding, invalid input, and Blokli
/// GraphQL union errors are returned as [`BlokliClientError`](crate::errors::BlokliClientError).
#[async_trait::async_trait]
pub trait BlokliQueryClient {
    /// Counts accounts matching the given [`AccountSelector`].
    ///
    /// [`AccountSelector::Any`] is accepted here and counts every indexed account.
    async fn count_accounts(&self, selector: AccountSelector) -> Result<u32>;
    /// Returns accounts matching the given [`AccountSelector`].
    ///
    /// Unlike [`count_accounts`](BlokliQueryClient::count_accounts), this method rejects [`AccountSelector::Any`] to
    /// avoid accidentally fetching an unbounded account list.
    async fn query_accounts(&self, selector: AccountSelector) -> Result<Vec<types::Account>>;
    /// Returns the native-chain balance for an account or safe address.
    ///
    /// The address is encoded as hexadecimal for the GraphQL request. Invalid addresses or upstream query failures are
    /// surfaced as client errors.
    async fn query_native_balance(&self, address: &ChainAddress) -> Result<types::NativeBalance>;
    /// Returns the HOPR token balance for an account or safe address.
    async fn query_token_balance(&self, address: &ChainAddress, token: types::Token) -> Result<types::HoprBalance>;
    /// Returns the number of indexed transactions sent from the given address.
    async fn query_transaction_count(&self, address: &ChainAddress) -> Result<u64>;
    /// Returns the HOPR token allowance configured for a safe address.
    async fn query_safe_allowance(&self, address: &ChainAddress) -> Result<types::SafeHoprAllowance>;
    /// Returns redeemed and rejected ticket aggregates filtered by safe, node, or both.
    ///
    /// Use [`RedeemedStatsSelector::SafeAndNodeAddress`] when a single safe/node pair is required.
    async fn query_redeemed_stats(&self, selector: RedeemedStatsSelector) -> Result<types::RedeemedStats>;
    /// Returns deployed safes matching the given [`SafeSelector`].
    async fn query_safe(&self, selector: SafeSelector) -> Result<Vec<types::Safe>>;
    /// Returns the predicted module address for the given safe deployment data.
    async fn query_module_address_prediction(&self, input: ModulePredictionInput) -> Result<ChainAddress>;
    /// Counts channels matching the given [`ChannelSelector`].
    ///
    /// Prefer [`query_channel_stats`](BlokliQueryClient::query_channel_stats), which also returns the aggregate
    /// channel balance.
    #[deprecated(
        since = "0.22.0",
        note = "Use query_channel_stats instead, which returns both count and total wxHOPR balance."
    )]
    async fn count_channels(&self, selector: ChannelSelector) -> Result<u32>;
    /// Returns channel count and total wxHOPR balance matching the given [`ChannelSelector`].
    ///
    /// An unfiltered selector returns stats across all indexed channels.
    async fn query_channel_stats(&self, selector: ChannelSelector) -> Result<types::ChannelStats>;
    /// Returns channels matching the given [`ChannelSelector`].
    ///
    /// At least one filter or safe address must be set. For unfiltered aggregate data, use
    /// [`query_channel_stats`](BlokliQueryClient::query_channel_stats).
    async fn query_channels(&self, selector: ChannelSelector) -> Result<types::ChannelsList>;
    /// Returns the total wxHOPR balance held across indexed safe contracts.
    ///
    /// When `owner_address` is provided, restricts to safes whose registered accounts have that chain key.
    async fn query_safes_balance(&self, owner_address: Option<ChainAddress>) -> Result<types::SafesBalance>;
    /// Counts service registry entries matching the given [`ServiceSelector`].
    ///
    /// [`ServiceSelector::Any`] is accepted here and counts every indexed entry.
    async fn count_services(&self, selector: ServiceSelector) -> Result<u32>;
    /// Returns service registry entries matching the given [`ServiceSelector`].
    ///
    /// Unlike [`count_services`](BlokliQueryClient::count_services), this method rejects
    /// [`ServiceSelector::Any`]: the registry is permissionless and anyone can grow it, so a bare
    /// enumeration is not offered.
    async fn query_services(&self, selector: ServiceSelector) -> Result<Vec<types::ServiceEntry>>;
    /// Returns only entries whose node is currently bound in the NodeSafeRegistry selected by the
    /// service registry itself.
    async fn query_live_services(&self, selector: ServiceSelector) -> Result<Vec<types::ServiceEntry>>;
    /// Returns service type configuration, optionally restricted to a single type.
    ///
    /// Passing `None` returns every registered type. Unlike the entry set, the set of types is
    /// gated by the registry-wide type registration fee, so enumerating it is bounded.
    async fn query_service_types(&self, service_type: Option<ServiceTypeId>) -> Result<Vec<types::ServiceTypeInfo>>;
    /// Returns the current registry-wide type registration fee and node-safe registry pointer.
    ///
    /// This is the one-shot alternative to
    /// [`BlokliSubscriptionClient::subscribe_service_registry_config`].
    async fn query_service_registry_config(&self) -> Result<types::ServiceRegistryConfig>;
    /// Returns the latest known status for a tracked transaction id.
    ///
    /// The `tx_id` is the Blokli tracking id returned by
    /// [`BlokliTransactionClient::submit_and_track_transaction`], not necessarily the on-chain transaction hash.
    async fn query_transaction_status(&self, tx_id: TxId) -> Result<types::Transaction>;
    /// Returns chain, contract, fee, ticket, and timing parameters reported by Blokli.
    async fn query_chain_info(&self) -> Result<types::ChainInfo>;
    /// Returns the Blokli server version string.
    async fn query_version(&self) -> Result<String>;
    /// Returns the current health state as reported by the legacy health query.
    async fn query_health(&self) -> Result<String>;
    /// Queries server compatibility information.
    ///
    /// Legacy endpoint. `supported_client_versions` is always `"*"` on current servers,
    /// meaning any client version is accepted.
    async fn query_compatibility(&self) -> Result<types::Compatibility>;
}

/// SSE-backed GraphQL subscriptions to Blokli updates.
///
/// Subscription methods return streams of `Result<T, BlokliClientError>`. The client uses the configured reconnect,
/// read-timeout, TCP keepalive, and restart-delay options from [`BlokliClientConfig`](crate::BlokliClientConfig).
/// Transport issues may be retried internally; malformed GraphQL payloads and terminal stream errors are yielded as
/// stream items so callers can decide whether to continue, log, or abort.
pub trait BlokliSubscriptionClient {
    /// Streams channel updates matching the given [`ChannelSelector`].
    ///
    /// An unfiltered selector subscribes to all channel updates. Each yielded item is a single updated channel.
    fn subscribe_channels(
        &self,
        selector: ChannelSelector,
    ) -> Result<impl futures::Stream<Item = Result<types::Channel>> + Send>;
    /// Streams account updates matching the given [`AccountSelector`].
    ///
    /// [`AccountSelector::Any`] subscribes to all account updates.
    fn subscribe_accounts(
        &self,
        selector: AccountSelector,
    ) -> Result<impl futures::Stream<Item = Result<types::Account>> + Send>;
    /// Streams updates for the open-channel graph.
    ///
    /// The initial stream emits one entry per currently open channel. Later updates
    /// include all channel state transitions, including `CLOSED` entries. Consumers
    /// should merge entries by `channel.concrete_channel_id` and use closed entries
    /// as removal signals for an open-channel graph.
    fn subscribe_graph(&self) -> Result<impl futures::Stream<Item = Result<types::OpenedChannelsGraphEntry>> + Send>;
    /// Streams updates of ticket price and winning-probability parameters.
    fn subscribe_ticket_params(&self) -> Result<impl futures::Stream<Item = Result<types::TicketParameters>> + Send>;
    /// Streams readiness updates for the Blokli instance.
    fn subscribe_health(&self) -> Result<impl futures::Stream<Item = Result<types::ReadinessState>> + Send>;
    /// Streams on-chain safe deployments indexed by Blokli.
    fn subscribe_safe_deployments(&self) -> Result<impl futures::Stream<Item = Result<types::Safe>> + Send>;
    /// Streams changes to service registry entries matching the given [`ServiceSelector`].
    ///
    /// Each item reports one registration, update, or deregistration. Deregistration carries no
    /// entry, because the entry no longer exists; the service type and node on the
    /// [`ServiceUpdate`](types::ServiceUpdate) identify what was removed.
    ///
    /// [`ServiceSelector::Any`] subscribes to every registry change.
    fn subscribe_services(
        &self,
        selector: ServiceSelector,
    ) -> Result<impl futures::Stream<Item = Result<types::ServiceUpdate>> + Send>;
    /// Streams changes to service type and registry-wide configuration.
    ///
    /// Passing `None` subscribes to every type. The two registry-wide kinds,
    /// [`RegistrationFeeChanged`](types::ServiceTypeUpdateKind::RegistrationFeeChanged) and
    /// [`RegistryPointerChanged`](types::ServiceTypeUpdateKind::RegistryPointerChanged), carry no
    /// service type and report their new state on
    /// [`registry_config`](types::ServiceTypeUpdate::registry_config).
    fn subscribe_service_types(
        &self,
        service_type: Option<ServiceTypeId>,
    ) -> Result<impl futures::Stream<Item = Result<types::ServiceTypeUpdate>> + Send>;
    /// Streams the complete registry-wide configuration.
    ///
    /// The first item is the current type registration fee and node-safe registry pointer. Later
    /// items contain the complete configuration after either value changes, so callers do not need
    /// a separate query before subscribing.
    fn subscribe_service_registry_config(
        &self,
    ) -> Result<impl futures::Stream<Item = Result<types::ServiceRegistryConfig>> + Send + 'static>;
    /// Streams status updates for a tracked transaction id.
    ///
    /// The `tx_id` is the Blokli tracking id returned by
    /// [`BlokliTransactionClient::submit_and_track_transaction`].
    fn subscribe_track_transaction(
        &self,
        tx_id: TxId,
    ) -> Result<impl futures::Stream<Item = Result<types::Transaction>> + Send>;
    /// Subscribes to on-chain ticket redemption events matching the given [`TicketSelector`].
    ///
    /// Returns an infinite stream of `Result<`[`types::RedeemTicketDetails`]`>`. Each item represents
    /// one redemption event that passed the selector filter. The stream terminates when the
    /// underlying SSE connection closes; errors (network, parse) are yielded as `Err` items.
    ///
    /// Use [`TicketSelector::Any`] to receive every redemption, or narrow by channel, issuer, or
    /// recipient address.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use futures::StreamExt;
    /// use blokli_client::api::v1::{BlokliSubscriptionClient, TicketSelector};
    ///
    /// let mut stream = client
    ///     .subscribe_ticket_redeemed(TicketSelector::Any)
    ///     .expect("failed to subscribe");
    ///
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(event) => println!("redeemed ticket {} in epoch {}", event.index, event.epoch),
    ///         Err(e) => eprintln!("stream error: {e}"),
    ///     }
    /// }
    /// ```
    fn subscribe_ticket_redeemed(
        &self,
        selector: TicketSelector,
    ) -> Result<impl futures::Stream<Item = Result<types::RedeemTicketDetails>> + Send>;
}

/// Signed transaction submission and tracking through Blokli.
///
/// These methods do not sign transactions. Callers provide raw signed transaction bytes. Submission success means
/// Blokli accepted or relayed the transaction according to the chosen mode; callers that need durable chain state
/// should rely on confirmations or independent chain observation.
#[async_trait::async_trait]
pub trait BlokliTransactionClient {
    /// Submits a signed transaction and returns the on-chain transaction hash reported by Blokli.
    ///
    /// This method does not wait for confirmation.
    async fn submit_transaction(&self, signed_tx: &[u8]) -> Result<TxReceipt>;
    /// Submits a signed transaction and returns a Blokli tracking id.
    ///
    /// Pass the returned id to [`BlokliQueryClient::query_transaction_status`],
    /// [`BlokliSubscriptionClient::subscribe_track_transaction`], or
    /// [`track_transaction`](BlokliTransactionClient::track_transaction).
    async fn submit_and_track_transaction(&self, signed_tx: &[u8]) -> Result<TxId>;
    /// Submits a signed transaction and waits for the requested number of confirmations.
    ///
    /// Blokli caps very large confirmation counts internally. A timeout or RPC error is returned as
    /// [`BlokliClientError`](crate::errors::BlokliClientError).
    async fn submit_and_confirm_transaction(&self, signed_tx: &[u8], num_confirmations: usize) -> Result<TxReceipt>;
    /// Tracks the transaction given the `tx_id` previously returned
    /// by [`submit_and_track_transaction`](BlokliTransactionClient::submit_and_track_transaction) until it is confirmed
    /// or [fails](crate::errors::TrackingErrorKind).
    async fn track_transaction(&self, tx_id: TxId, client_timeout: Duration) -> Result<types::Transaction>;
}
