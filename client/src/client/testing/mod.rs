//! In-memory Blokli client implementation for downstream tests.
//!
//! This module is available with the `testing` feature. It provides [`BlokliTestClient`], a state-backed client that
//! implements the same query, subscription, and transaction traits as [`BlokliClient`](crate::BlokliClient). Use it
//! when library consumers need deterministic tests without running a Blokli service.
//!
//! The client starts from a [`BlokliTestState`]. Submitted transactions are passed to a
//! [`BlokliTestStateMutator`], which may update that state. The client then broadcasts account, channel, safe,
//! service-registry, and ticket-parameter changes to active subscriptions.
//!
//! This is a test double, not a byte-for-byte Blokli server emulator. It enforces basic consistency checks and keeps
//! subscription behavior close to the public traits, but callers remain responsible for modeling the state transitions
//! they care about in their mutator.

use std::{
    ops::Div,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_broadcast::TrySendError;
use futures::{Stream, StreamExt};
use futures_time::{stream::StreamExt as TimeStreamExt, time::Duration as Duration2};
use hopr_types::{crypto::types::Hash, primitive::prelude::HoprBalance as PrimitiveHoprBalance};
use indexmap::IndexMap;

use crate::{
    api::{types::*, v1::graphql::services::service_type_name, *},
    errors::{BlokliClientError, ErrorKind, InternalTxError, TrackingErrorKind},
};

fn serialize_as_empty_map<K, V, S>(_: &IndexMap<K, V>, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    K: serde::Serialize,
    V: serde::Serialize,
    S: serde::Serializer,
{
    serde::Serialize::serialize(&IndexMap::<K, V>::new(), serializer)
}

fn default_service_registry_config() -> ServiceRegistryConfig {
    ServiceRegistryConfig {
        type_registration_fee: "0 wxHOPR".into(),
        node_safe_registry: "0x0000000000000000000000000000000000000000".into(),
    }
}

/// In-memory state served by [`BlokliTestClient`].
///
/// Fields are public so tests can build fixtures directly. Maps are keyed by the same identifiers used by the public
/// client responses, typically hex-encoded addresses or ids. [`BlokliTestState::default`] provides a small coherent
/// baseline suitable for tests that only need to override a few fields.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BlokliTestState {
    /// Contains KeyID -> Account
    pub accounts: IndexMap<u32, Account>,
    /// Contains native balances for addresses.
    pub native_balances: IndexMap<String, NativeBalance>,
    /// Contains token balances for addresses.
    pub token_balances: IndexMap<String, HoprBalance>,
    /// Contains safe allowances for addresses.
    pub safe_allowances: IndexMap<String, SafeHoprAllowance>,
    /// Contains deployed Safes for addresses.
    pub deployed_safes: IndexMap<String, Safe>,
    /// Ticket redemption statistics per Safe address
    pub safe_redeem_stats: IndexMap<String, RedeemedStats>,
    /// Contains transaction counts for addresses.
    pub tx_counts: IndexMap<String, u64>,
    /// Contains ChannelId -> Channel.
    pub channels: IndexMap<String, Channel>,
    /// Contains service registry entries, keyed by [`BlokliTestState::service_entry_key`].
    pub services: IndexMap<String, ServiceEntry>,
    /// Contains service type configuration, keyed by [`ServiceTypeInfo::service_type`].
    pub service_types: IndexMap<String, ServiceTypeInfo>,
    /// Contains the registry-wide type registration fee and node-safe registry pointer.
    #[serde(default = "default_service_registry_config")]
    pub service_registry_config: ServiceRegistryConfig,
    /// Contains chain info.
    pub chain_info: ChainInfo,
    /// Version of the Blokli server.
    pub version: String,
    /// Health of the Blokli server.
    pub health: String,
    /// Active transactions.
    ///
    /// This field is transient and not serialized.
    // Always serialize as empty, because the data are non-deterministic and do not make sense to compare.
    #[serde(serialize_with = "serialize_as_empty_map")]
    pub active_txs: IndexMap<TxId, Transaction>,
}

impl PartialEq for BlokliTestState {
    fn eq(&self, other: &Self) -> bool {
        // Skip active_txs because they are non-deterministic.
        self.accounts == other.accounts
            && self.deployed_safes == other.deployed_safes
            && self.native_balances == other.native_balances
            && self.token_balances == other.token_balances
            && self.safe_allowances == other.safe_allowances
            && self.safe_redeem_stats == other.safe_redeem_stats
            && self.tx_counts == other.tx_counts
            && self.channels == other.channels
            && self.services == other.services
            && self.service_types == other.service_types
            && self.service_registry_config == other.service_registry_config
            && self.chain_info == other.chain_info
            && self.version == other.version
            && self.health == other.health
    }
}

impl Default for BlokliTestState {
    fn default() -> Self {
        Self {
            accounts: Default::default(),
            native_balances: Default::default(),
            token_balances: Default::default(),
            safe_allowances: Default::default(),
            deployed_safes: Default::default(),
            safe_redeem_stats: Default::default(),
            tx_counts: Default::default(),
            channels: Default::default(),
            services: Default::default(),
            service_types: Default::default(),
            service_registry_config: default_service_registry_config(),
            chain_info: ChainInfo {
                channel_closure_grace_period: Uint64("300".into()),
                channel_dst: Some("0000000000000000000000000000000000000000000000000000000000000000".into()),
                block_number: 1,
                chain_id: 100,
                gas_price: Some("1000000000".into()),
                ledger_dst: Some("0000000000000000000000000000000000000000000000000000000000000000".into()),
                max_fee_per_gas: Some("3000000000".into()),
                max_priority_fee_per_gas: Some("100000000".into()),
                min_ticket_winning_probability: 1.0,
                key_binding_fee: TokenValueString("0.01 wxHOPR".into()),
                safe_registry_dst: Some("0000000000000000000000000000000000000000000000000000000000000000".into()),
                ticket_price: TokenValueString("1 wxHOPR".into()),
                network: "jura".into(),
                contract_addresses: ContractAddressMap(
                    r#"
                {
                    "announcements": "0xf1c143B1bA20C7606d56aA2FA94502D25744b982",
                    "channels": "0x77C9414043d27fdC98A6A2d73fc77b9b383092a7",
                    "module_implementation": "0x32863c4974fBb6253E338a0cb70C382DCeD2eFCb",
                    "node_safe_registry": "0x4F7C7dE3BA2B29ED8B2448dF2213cA43f94E45c0",
                    "node_stake_factory": "0x791d190b2c95397F4BcE7bD8032FD67dCEA7a5F2",
                    "node_safe_migration": "0x0000000000000000000000000000000000000000",
                    "service_registry": "0x9A676e781A523b5d0C0e43731313A708CB607508",
                    "token": "0xD4fdec44DB9D44B8f2b6d529620f9C0C7066A2c1",
                    "ticket_price_oracle": "0x442df1d946303fB088C9377eefdaeA84146DA0A6",
                    "winning_probability_oracle": "0xC15675d4CCa538D91a91a8D3EcFBB8499C3B0471",
                    "xhopr_token": "0xA51c1fc2f0D1a1b8494Ed1FE312d7C3a78Ed91C0"
                }"#
                    .into(),
                ),
                expected_block_time: Uint64("5".into()),
                finality: Uint64("3".into()),
            },

            version: "1".to_string(),
            health: "OK".to_string(),
            active_txs: Default::default(),
        }
    }
}

impl BlokliTestState {
    fn safe_matches_owner(safe: &Safe, owner_hex: &str) -> bool {
        safe.chain_key == owner_hex || safe.owners.iter().any(|owner| owner == owner_hex)
    }

    /// Builds the [`services`](BlokliTestState::services) key for a service type and node address.
    ///
    /// The key is `"<service type>/<node address>"`, where the service type is written the way Blokli renders it -
    /// the ASCII name, or `0x`-prefixed hex - and the node address is hex, with or without a `0x` prefix.
    pub fn service_entry_key(service_type: &str, node: &ChainAddress) -> String {
        format!("{service_type}/{}", hex::encode(node))
    }

    /// Convenience method to return a reference to the [`ServiceEntry`] of a node for a service type.
    pub fn get_service_entry(&self, service_type: &ServiceTypeId, node: &ChainAddress) -> Option<&ServiceEntry> {
        self.services
            .values()
            .find(|entry| service_type_matches(&entry.service_type, service_type) && hex_matches(&entry.node, node))
    }

    /// Convenience method to return a mutable reference to the [`ServiceEntry`] of a node for a service type.
    pub fn get_service_entry_mut(
        &mut self,
        service_type: &ServiceTypeId,
        node: &ChainAddress,
    ) -> Option<&mut ServiceEntry> {
        self.services
            .values_mut()
            .find(|entry| service_type_matches(&entry.service_type, service_type) && hex_matches(&entry.node, node))
    }

    /// Convenience method to return a reference to the [`ServiceTypeInfo`] of a service type.
    pub fn get_service_type(&self, service_type: &ServiceTypeId) -> Option<&ServiceTypeInfo> {
        self.service_types
            .values()
            .find(|info| service_type_matches(&info.service_type, service_type))
    }

    /// Convenience method to return a mutable reference to the [`ServiceTypeInfo`] of a service type.
    pub fn get_service_type_mut(&mut self, service_type: &ServiceTypeId) -> Option<&mut ServiceTypeInfo> {
        self.service_types
            .values_mut()
            .find(|info| service_type_matches(&info.service_type, service_type))
    }

    /// Convenience method to return a reference to an [`Account`] with a given [`ChainAddress`].
    pub fn get_account(&self, chain_key: &ChainAddress) -> Option<&Account> {
        self.accounts
            .values()
            .find(|acc| acc.chain_key == hex::encode(chain_key))
    }

    /// Convenience method to return a mutable reference to an [`Account`] with a given [`ChainAddress`].
    pub fn get_account_mut(&mut self, chain_key: &ChainAddress) -> Option<&mut Account> {
        self.accounts
            .values_mut()
            .find(|acc| acc.chain_key == hex::encode(chain_key))
    }

    /// Convenience method to return a reference to a [`Channel`] with a given [` ChannelId `].
    pub fn get_channel_by_id(&self, channel_id: &ChannelId) -> Option<&Channel> {
        self.channels.get(&hex::encode(channel_id))
    }

    /// Convenience method to return a mutable reference to a [`Channel`] with a given [` ChannelId `].
    pub fn get_channel_by_id_mut(&mut self, channel_id: &ChannelId) -> Option<&mut Channel> {
        self.channels.get_mut(&hex::encode(channel_id))
    }

    /// Convenience method to return a reference to Safe balance corresponding to the given [`ChainAddress`] of the
    /// [`Account`].
    pub fn get_account_safe_token_balance(&self, chain_key: &ChainAddress) -> Option<&HoprBalance> {
        let account = self.get_account(chain_key)?;
        self.token_balances.get(account.safe_address.as_ref()?)
    }

    /// Convenience method to return a mutable reference to Safe balance corresponding to the given [`ChainAddress`] of
    /// the [`Account`].
    pub fn get_account_safe_token_balance_mut(&mut self, chain_key: &ChainAddress) -> Option<&mut HoprBalance> {
        let account = self.get_account(chain_key).and_then(|a| a.safe_address.clone())?;
        self.token_balances.get_mut(&account)
    }

    /// Convenience method to return a reference to Safe allowance corresponding to the given [`ChainAddress`] of the
    /// [`Account`].
    pub fn get_account_safe_allowance(&self, chain_key: &ChainAddress) -> Option<&SafeHoprAllowance> {
        let account = self.get_account(chain_key)?;
        self.safe_allowances.get(account.safe_address.as_ref()?)
    }

    /// Convenience method to return a mutable reference to Safe allowance corresponding to the given [`ChainAddress`]
    /// of the [`Account`].
    pub fn get_account_safe_allowance_mut(&mut self, chain_key: &ChainAddress) -> Option<&mut SafeHoprAllowance> {
        let account = self.get_account(chain_key).and_then(|a| a.safe_address.clone())?;
        self.safe_allowances.get_mut(&account)
    }

    /// Gets [`RedeemedStats`] for the given Safe address.
    pub fn get_safe_redeem_stats(&self, chain_address: &ChainAddress) -> Option<&RedeemedStats> {
        self.safe_redeem_stats.get(&hex::encode(chain_address))
    }

    /// Gets [`RedeemedStats`] for the given Safe address by mutable reference.
    pub fn get_safe_redeem_stats_mut(&mut self, chain_address: &ChainAddress) -> Option<&mut RedeemedStats> {
        self.safe_redeem_stats.get_mut(&hex::encode(chain_address))
    }

    /// Convenience method to return references to [`Safe`]s with the given owner's [`ChainAddress`].
    pub fn get_safe_by_owner(&self, owner: &ChainAddress) -> Vec<&Safe> {
        let owner_hex = hex::encode(owner);
        self.deployed_safes
            .values()
            .filter(|safe| Self::safe_matches_owner(safe, &owner_hex))
            .collect()
    }

    /// Convenience method to return mutable references to [`Safe`]s with the given owner's [`ChainAddress`].
    pub fn get_safe_by_owner_mut(&mut self, owner: &ChainAddress) -> Vec<&mut Safe> {
        let owner_hex = hex::encode(owner);
        self.deployed_safes
            .values_mut()
            .filter(|safe| Self::safe_matches_owner(safe, &owner_hex))
            .collect()
    }
}

/// Applies signed-transaction effects to a [`BlokliTestState`].
///
/// Implement this trait when a test needs transaction submission methods to modify the in-memory state. The mutator is
/// called synchronously while the test client holds the state write lock. If the mutator returns an error, the state is
/// reverted to its previous value and the simulated transaction reports a failure according to the submission mode.
pub trait BlokliTestStateMutator {
    /// Updates the state given the signed transaction.
    ///
    /// [`BlokliTestClient`] makes several consistency checks on the updates.
    /// For example, all mutations that remove anything from the state are not allowed.
    ///
    /// For arbitrary state updates via the client, see [`BlokliTestClient::hidden_state_update`].
    fn update_state(&self, signed_tx: &[u8], state: &mut BlokliTestState) -> Result<()>;
}

/// No-op state mutator.
///
/// Useful for tests that only query a fixed [`BlokliTestState`] or that mutate state manually with
/// [`BlokliTestClient::hidden_state_update`].
#[derive(Clone, Debug, Default)]
pub struct NopStateMutator;

impl BlokliTestStateMutator for NopStateMutator {
    fn update_state(&self, _: &[u8], _: &mut BlokliTestState) -> Result<()> {
        Ok(())
    }
}

impl<F: Fn(&[u8], &mut BlokliTestState) -> Result<()>> BlokliTestStateMutator for F {
    fn update_state(&self, signed_tx: &[u8], state: &mut BlokliTestState) -> Result<()> {
        self(signed_tx, state)
    }
}

type AccountEvents = (
    async_broadcast::Sender<Account>,
    async_broadcast::InactiveReceiver<Account>,
);

type GraphEvents = (
    async_broadcast::Sender<(Account, Channel, Account)>,
    async_broadcast::InactiveReceiver<(Account, Channel, Account)>,
);

type TicketParamEvents = (
    async_broadcast::Sender<TicketParameters>,
    async_broadcast::InactiveReceiver<TicketParameters>,
);

type SafeDeployEvents = (async_broadcast::Sender<Safe>, async_broadcast::InactiveReceiver<Safe>);

type ServiceEvents = (
    async_broadcast::Sender<ServiceUpdate>,
    async_broadcast::InactiveReceiver<ServiceUpdate>,
);

type ServiceTypeEvents = (
    async_broadcast::Sender<ServiceTypeUpdate>,
    async_broadcast::InactiveReceiver<ServiceTypeUpdate>,
);

type ServiceRegistryConfigEvents = (
    async_broadcast::Sender<ServiceRegistryConfig>,
    async_broadcast::InactiveReceiver<ServiceRegistryConfig>,
);

/// Snapshot of the [`BlokliTestState`] inside a [`BlokliTestClient`].
///
/// Snapshots are cheap handles containing a cloned state view. Call [`refresh`](BlokliTestStateSnapshot::refresh) to
/// replace the stored view with the latest shared state.
#[derive(Clone)]
pub struct BlokliTestStateSnapshot {
    state: Arc<parking_lot::RwLock<BlokliTestState>>,
    snapshot: BlokliTestState,
}

impl BlokliTestStateSnapshot {
    /// Refreshes the snapshot by fetching it from the [`BlokliTestClient`].
    pub fn refresh(mut self) -> Self {
        {
            let state = self.state.read();
            self.snapshot = state.clone();
        }
        self
    }
}

impl AsRef<BlokliTestState> for BlokliTestStateSnapshot {
    fn as_ref(&self) -> &BlokliTestState {
        &self.snapshot
    }
}

impl std::ops::Deref for BlokliTestStateSnapshot {
    type Target = BlokliTestState;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

/// In-memory Blokli client for tests.
///
/// `BlokliTestClient` implements [`BlokliQueryClient`], [`BlokliSubscriptionClient`], and
/// [`BlokliTransactionClient`] against a shared [`BlokliTestState`]. Clones share the same state and broadcast
/// channels, which makes it possible to submit simulated transactions from one handle and observe subscription updates
/// from another.
///
/// Transactions submitted through the client call the configured [`BlokliTestStateMutator`]. Mutations that remove
/// accounts, channels, balances, allowances, or active transactions are rejected to avoid producing inconsistent test
/// state. For direct fixture edits that should not emit subscription events, use
/// [`hidden_state_update`](BlokliTestClient::hidden_state_update).
///
/// This type is exported only with the `testing` feature.
#[derive(Clone)]
pub struct BlokliTestClient<M> {
    state: Arc<parking_lot::RwLock<BlokliTestState>>,
    mutator: M,
    accounts_channel: AccountEvents,
    channels_channel: GraphEvents,
    ticket_channel: TicketParamEvents,
    safe_deployed_channel: SafeDeployEvents,
    services_channel: ServiceEvents,
    service_types_channel: ServiceTypeEvents,
    service_registry_config_channel: ServiceRegistryConfigEvents,
    tx_simulation_delay: Duration,
    use_internal_txs: bool,
}

fn channel_matches(channel: &Channel, selector: &ChannelSelector, accounts: &IndexMap<u32, Account>) -> bool {
    let filter = match selector.filter {
        Some(ChannelFilter::ChannelId(id)) => channel.concrete_channel_id == hex::encode(id),
        Some(ChannelFilter::DestinationKeyId(dst_id)) => channel.destination as u32 == dst_id,
        Some(ChannelFilter::SourceKeyId(src_id)) => channel.source as u32 == src_id,
        Some(ChannelFilter::SourceAndDestinationKeyIds(src_id, dst_id)) => {
            channel.source as u32 == src_id && channel.destination as u32 == dst_id
        }
        None => true,
    };
    let safe_ok = selector.safe_address.is_none_or(|safe| {
        accounts
            .get(&(channel.source as u32))
            .and_then(|acc| acc.safe_address.as_ref())
            .is_some_and(|acc_safe| *acc_safe == hex::encode(safe))
    });
    filter && safe_ok && selector.status.is_none_or(|status| channel.status == status)
}

fn account_matches(account: &Account, selector: &AccountSelector) -> bool {
    match selector {
        AccountSelector::Address(address) => account.chain_key == hex::encode(address),
        AccountSelector::KeyId(id) => account.keyid as u32 == *id,
        AccountSelector::PacketKey(packet_key) => account.packet_key == hex::encode(packet_key),
        AccountSelector::Any => true,
    }
}

/// Compares a hexadecimal string from a fixture with raw bytes, tolerating a `0x` prefix and either case.
fn hex_matches(value: &str, expected: &[u8]) -> bool {
    value
        .trim_start_matches("0x")
        .eq_ignore_ascii_case(&hex::encode(expected))
}

/// Matches a service type as Blokli renders it against a raw [`ServiceTypeId`].
///
/// Both renderings are accepted, so a fixture can spell a type either as its ASCII name or as hex.
fn service_type_matches(rendered: &str, wanted: &ServiceTypeId) -> bool {
    hex_matches(rendered, wanted) || service_type_name(wanted).is_some_and(|name| rendered == name)
}

fn service_matches(service_type: &str, node: &str, selector: &ServiceSelector) -> bool {
    match selector {
        ServiceSelector::ServiceType(wanted) => service_type_matches(service_type, wanted),
        ServiceSelector::Node(wanted) => hex_matches(node, wanted),
        ServiceSelector::ServiceTypeAndNode {
            service_type: wanted_type,
            node: wanted_node,
        } => service_type_matches(service_type, wanted_type) && hex_matches(node, wanted_node),
        ServiceSelector::Any => true,
    }
}

fn broadcast_or_log<T: Clone>(sender: &async_broadcast::Sender<T>, value: T, description: &str) {
    match sender.try_broadcast(value) {
        Err(TrySendError::Full(_)) => {
            tracing::error!("failed to broadcast {description} - channel is full");
        }
        Err(TrySendError::Closed(_)) => {
            tracing::error!("failed to broadcast {description} - channel is closed");
        }
        _ => {}
    }
}

impl<M: BlokliTestStateMutator> BlokliTestClient<M> {
    /// Constructs a new client that owns the given [`initial_state`](BlokliTestState).
    ///
    /// After construction, the only way to mutate the state is when the client calls the given
    /// [`mutator`](BlokliTestStateMutator) based on a [submitted](BlokliTransactionClient) transaction.
    pub fn new(initial_state: BlokliTestState, mutator: M) -> Self {
        let (mut accounts_tx, accounts_rx) = async_broadcast::broadcast(1024);
        accounts_tx.set_await_active(false);
        accounts_tx.set_overflow(false);

        let (mut channels_tx, channels_rx) = async_broadcast::broadcast(1024);
        channels_tx.set_await_active(false);
        channels_tx.set_overflow(false);

        let (mut tickets_tx, tickets_rx) = async_broadcast::broadcast(1024);
        tickets_tx.set_await_active(false);
        tickets_tx.set_overflow(false);

        let (mut safes_tx, safes_rx) = async_broadcast::broadcast(1024);
        safes_tx.set_await_active(false);
        safes_tx.set_overflow(false);

        let (mut services_tx, services_rx) = async_broadcast::broadcast(1024);
        services_tx.set_await_active(false);
        services_tx.set_overflow(false);

        let (mut service_types_tx, service_types_rx) = async_broadcast::broadcast(1024);
        service_types_tx.set_await_active(false);
        service_types_tx.set_overflow(false);

        let (mut service_registry_config_tx, service_registry_config_rx) = async_broadcast::broadcast(1024);
        service_registry_config_tx.set_await_active(false);
        service_registry_config_tx.set_overflow(false);

        Self {
            state: Arc::new(parking_lot::RwLock::new(initial_state)),
            mutator,
            accounts_channel: (accounts_tx, accounts_rx.deactivate()),
            channels_channel: (channels_tx, channels_rx.deactivate()),
            ticket_channel: (tickets_tx, tickets_rx.deactivate()),
            safe_deployed_channel: (safes_tx, safes_rx.deactivate()),
            services_channel: (services_tx, services_rx.deactivate()),
            service_types_channel: (service_types_tx, service_types_rx.deactivate()),
            service_registry_config_channel: (service_registry_config_tx, service_registry_config_rx.deactivate()),
            tx_simulation_delay: Duration::from_secs(1),
            use_internal_txs: false,
        }
    }

    /// Replaces the transaction mutator.
    ///
    /// The returned client keeps the same shared state and subscription channels.
    #[must_use]
    pub fn with_mutator(mut self, mutator: M) -> Self {
        self.mutator = mutator;
        self
    }

    /// Enables or disables internal safe transaction simulation.
    ///
    /// When enabled, a mutator error wrapped in [`InternalTxError`](crate::errors::InternalTxError) produces a
    /// confirmed outer transaction with failed safe execution details. The default is disabled.
    #[must_use]
    pub fn with_use_internal_txs(mut self, use_internal_txs: bool) -> Self {
        self.use_internal_txs = use_internal_txs;
        self
    }

    /// Sets the delay before a simulated transaction is confirmed or emitted by tracking streams.
    ///
    /// The default is 1 second.
    #[must_use]
    pub fn with_tx_simulation_delay(mut self, tx_simulation_delay: Duration) -> Self {
        self.tx_simulation_delay = tx_simulation_delay;
        self
    }

    /// Returns the current snapshot of the internal state.
    ///
    /// The snapshot can be repeatedly [refreshed](BlokliTestStateSnapshot::refresh) to get the latest state.
    pub fn snapshot(&self) -> BlokliTestStateSnapshot {
        let state = self.state.read();
        BlokliTestStateSnapshot {
            state: self.state.clone(),
            snapshot: state.clone(),
        }
    }

    /// Performs an arbitrary state update without broadcasting subscription events.
    ///
    /// This is useful for arranging fixtures between assertions. Use transaction submission or
    /// [`update_price_and_win_prob`](BlokliTestClient::update_price_and_win_prob) when tests need subscribers to
    /// observe the change.
    pub fn hidden_state_update(&self, update: impl FnOnce(&mut BlokliTestState)) {
        let mut state = self.state.write();
        update(&mut state);
    }

    /// Updates the ticket price and/or minimum ticket-winning probability.
    ///
    /// These changes update the shared state and broadcast a [`TicketParameters`] event to active subscribers when at
    /// least one value changes.
    pub fn update_price_and_win_prob(&self, new_price: Option<TokenValueString>, new_win_prob: Option<f64>) {
        let mut updated = false;
        let (new_price_param, new_win_prob_param) = {
            let mut state = self.state.write();

            let mut new_price_param = state.chain_info.ticket_price.clone();
            if let Some(new_price) = new_price {
                state.chain_info.ticket_price = new_price.clone();

                new_price_param = new_price;
                updated = true;
            }

            let mut new_win_prob_param = state.chain_info.min_ticket_winning_probability;
            if let Some(new_win_prob) = new_win_prob {
                state.chain_info.min_ticket_winning_probability = new_win_prob;

                new_win_prob_param = new_win_prob;
                updated = true;
            }
            (new_price_param, new_win_prob_param)
        };

        if updated
            && let Err(error) = self.ticket_channel.0.try_broadcast(TicketParameters {
                min_ticket_winning_probability: new_win_prob_param,
                ticket_price: new_price_param,
            })
        {
            tracing::error!(%error, "failed to broadcast ticket parameters update");
        }
    }

    fn do_query_channels(&self, selector: ChannelSelector) -> Result<Vec<Channel>> {
        let state = self.state.read();
        Ok(state
            .channels
            .values()
            .filter(|c| channel_matches(c, &selector, &state.accounts))
            .cloned()
            .collect())
    }

    fn do_query_accounts(&self, selector: AccountSelector) -> Result<Vec<Account>> {
        Ok(self
            .state
            .read()
            .accounts
            .values()
            .filter(|a| account_matches(a, &selector))
            .cloned()
            .collect())
    }

    fn do_query_services(&self, selector: ServiceSelector) -> Result<Vec<ServiceEntry>> {
        Ok(self
            .state
            .read()
            .services
            .values()
            .filter(|entry| service_matches(&entry.service_type, &entry.node, &selector))
            .cloned()
            .collect())
    }
}

#[async_trait::async_trait]
impl<M: BlokliTestStateMutator + Send + Sync> BlokliQueryClient for BlokliTestClient<M> {
    async fn query_curvy_pending_notes(
        &self,
        _from_block: Option<u64>,
        _after: Option<CurvyEventCursor>,
        _first: u32,
    ) -> Result<CurvyPendingNotes> {
        Ok(CurvyPendingNotes { notes: Vec::new() })
    }

    async fn query_curvy_committed_notes(
        &self,
        _from_block: Option<u64>,
        _after: Option<CurvyEventCursor>,
        _first: u32,
    ) -> Result<CurvyCommittedNotes> {
        Ok(CurvyCommittedNotes { notes: Vec::new() })
    }

    async fn query_curvy_committed_nullifiers(
        &self,
        _from_block: Option<u64>,
        _after: Option<CurvyEventCursor>,
        _first: u32,
    ) -> Result<CurvyCommittedNullifiers> {
        Ok(CurvyCommittedNullifiers { nullifiers: Vec::new() })
    }

    async fn query_curvy_sync_checkpoint(&self, _block_hash: Option<String>) -> Result<CurvySyncCheckpoint> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_sync_notes(
        &self,
        _checkpoint: String,
        _from_index: Option<u64>,
        _first: u32,
    ) -> Result<CurvySyncNotePage> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_sync_nullifiers(
        &self,
        _checkpoint: String,
        _from_index: Option<u64>,
        _first: u32,
    ) -> Result<CurvySyncNullifierPage> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_shard_roots(
        &self,
        _checkpoint: String,
        _from_index: Option<u64>,
        _first: u32,
    ) -> Result<CurvyShardRootPage> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_aggregator_state(&self) -> Result<CurvyAggregatorState> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_note_status(&self, _note_id: String) -> Result<CurvyNoteStatus> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_valid_notes_root(&self, _root: String) -> Result<bool> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_nullifier_spent(&self, _nullifier: String) -> Result<bool> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_vault_fees(&self) -> Result<CurvyVaultFees> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_aggregator_fees(&self) -> Result<CurvyAggregatorFees> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_vault_token_count(&self) -> Result<CurvyVaultTokenCount> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_vault_token(&self, _token_id: String) -> Result<CurvyVaultToken> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_entry_portal_address(&self, _owner_hash: String, _recovery: String) -> Result<String> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_exit_portal_address(
        &self,
        _exit_address: String,
        _exit_chain_id: String,
        _recovery: String,
    ) -> Result<String> {
        Err(ErrorKind::NoData.into())
    }

    async fn query_curvy_portal_registered(&self, _portal_address: String) -> Result<bool> {
        Err(ErrorKind::NoData.into())
    }

    async fn count_accounts(&self, selector: AccountSelector) -> Result<u32> {
        Ok(match selector {
            AccountSelector::Any => self.state.read().accounts.len() as u32,
            selector => self.query_accounts(selector).await?.len() as u32,
        })
    }

    async fn query_accounts(&self, selector: AccountSelector) -> Result<Vec<Account>> {
        self.do_query_accounts(selector)
    }

    async fn query_native_balance(&self, address: &ChainAddress) -> Result<NativeBalance> {
        let address = hex::encode(address);
        self.state
            .read()
            .native_balances
            .get(&address)
            .cloned()
            .ok_or_else(|| ErrorKind::NoData.into())
    }

    async fn query_token_balance(&self, address: &ChainAddress, _token: Token) -> Result<HoprBalance> {
        let address = hex::encode(address);
        self.state
            .read()
            .token_balances
            .get(&address)
            .cloned()
            .ok_or_else(|| ErrorKind::NoData.into())
    }

    async fn query_transaction_count(&self, address: &ChainAddress) -> Result<u64> {
        let address = hex::encode(address);
        let state = self.state.upgradable_read();
        if let Some(value) = state.tx_counts.get(&address) {
            return Ok(*value);
        }

        let mut state = parking_lot::RwLockUpgradableReadGuard::upgrade(state);
        Ok(*state.tx_counts.entry(address).or_default())
    }

    async fn query_safe_allowance(&self, address: &ChainAddress) -> Result<SafeHoprAllowance> {
        let address = hex::encode(address);
        self.state
            .read()
            .safe_allowances
            .get(&address)
            .cloned()
            .ok_or_else(|| ErrorKind::NoData.into())
    }

    async fn query_redeemed_stats(&self, selector: RedeemedStatsSelector) -> Result<RedeemedStats> {
        let state = self.state.upgradable_read();

        let maybe_safe = match selector {
            RedeemedStatsSelector::SafeAddress(addr) => Some(addr),
            RedeemedStatsSelector::SafeAndNodeAddress { safe_address, .. } => Some(safe_address),
            RedeemedStatsSelector::NodeAddress(_) => None,
        };

        if let Some(safe_address) = maybe_safe {
            let safe_address_hex = hex::encode(safe_address);
            if !state.deployed_safes.contains_key(&safe_address_hex) {
                return Err(ErrorKind::NoData.into());
            }

            if let Some(v) = state.safe_redeem_stats.get(&safe_address_hex) {
                Ok(v.clone())
            } else {
                let mut state = parking_lot::RwLockUpgradableReadGuard::upgrade(state);
                let stats = RedeemedStats {
                    __typename: "RedeemedStats".to_string(),
                    redeemed_amount: TokenValueString("0 wxHOPR".into()),
                    redemption_count: Uint64("0".into()),
                    rejected_amount: TokenValueString("0 wxHOPR".into()),
                    rejection_count: Uint64("0".into()),
                };
                state.safe_redeem_stats.insert(safe_address_hex, stats.clone());
                Ok(stats)
            }
        } else {
            Err(ErrorKind::NoData.into())
        }
    }

    async fn query_safe(&self, selector: SafeSelector) -> Result<Vec<Safe>> {
        let state = self.state.read();
        match selector {
            SafeSelector::SafeAddress(addr) => Ok(state
                .deployed_safes
                .get(&hex::encode(addr))
                .cloned()
                .into_iter()
                .collect()),
            SafeSelector::Owner(owner_address) | SafeSelector::ChainKey(owner_address) => Ok(state
                .deployed_safes
                .values()
                .filter(|s| BlokliTestState::safe_matches_owner(s, &hex::encode(owner_address)))
                .cloned()
                .collect()),
            SafeSelector::RegisteredNode(node_address) => Ok(state
                .deployed_safes
                .values()
                .filter(|s| s.registered_nodes.contains(&hex::encode(node_address)))
                .cloned()
                .collect()),
        }
    }

    async fn query_module_address_prediction(&self, input: ModulePredictionInput) -> Result<ChainAddress> {
        let hash = Hash::create(&[
            input.nonce.to_be_bytes().as_ref(),
            input.owner.as_ref(),
            input.safe_address.as_ref(),
        ]);

        hash.as_ref()[0..20].try_into().map_err(|_| ErrorKind::NoData.into())
    }

    async fn count_channels(&self, selector: ChannelSelector) -> Result<u32> {
        Ok(if selector.matches_all() {
            self.state.read().channels.len() as u32
        } else {
            self.query_channels(selector).await?.channels.len() as u32
        })
    }

    async fn query_channel_stats(&self, selector: ChannelSelector) -> Result<ChannelStats> {
        let channels = self.do_query_channels(selector)?;
        let count = i32::try_from(channels.len()).map_err(|_| ErrorKind::ParseError)?;
        let mut total = PrimitiveHoprBalance::zero();
        for ch in &channels {
            let bal: PrimitiveHoprBalance = ch.balance.0.parse().map_err(|_| ErrorKind::ParseError)?;
            total += bal;
        }
        Ok(ChannelStats {
            count,
            balance: TokenValueString(total.to_string()),
        })
    }

    async fn query_channels(&self, selector: ChannelSelector) -> Result<ChannelsList> {
        let channels = self.do_query_channels(selector)?;
        Ok(ChannelsList {
            __typename: "ChannelsList".to_string(),
            channels,
        })
    }

    async fn query_safes_balance(&self, owner_address: Option<ChainAddress>) -> Result<SafesBalance> {
        let state = self.state.read();
        let matching_safes: Vec<&Safe> = if let Some(owner) = owner_address {
            let owner_hex = hex::encode(owner);
            state
                .deployed_safes
                .values()
                .filter(|s| BlokliTestState::safe_matches_owner(s, &owner_hex))
                .collect()
        } else {
            state.deployed_safes.values().collect()
        };

        let count = i32::try_from(matching_safes.len()).map_err(|_| ErrorKind::ParseError)?;
        let mut total = PrimitiveHoprBalance::zero();
        for safe in &matching_safes {
            if let Some(hopr_balance) = state.token_balances.get(&safe.address) {
                let bal: PrimitiveHoprBalance = hopr_balance.balance.0.parse().map_err(|_| ErrorKind::ParseError)?;
                total += bal;
            }
        }

        Ok(SafesBalance {
            count,
            balance: TokenValueString(total.to_string()),
        })
    }

    async fn count_services(&self, selector: ServiceSelector) -> Result<u32> {
        Ok(match selector {
            ServiceSelector::Any => self.state.read().services.len() as u32,
            selector => self.do_query_services(selector)?.len() as u32,
        })
    }

    async fn query_services(&self, selector: ServiceSelector) -> Result<Vec<ServiceEntry>> {
        self.do_query_services(selector)
    }

    async fn query_live_services(&self, selector: ServiceSelector) -> Result<Vec<ServiceEntry>> {
        let entries = self.query_services(selector).await?;
        let state = self.state.read();
        Ok(entries
            .into_iter()
            .filter(|entry| {
                state
                    .deployed_safes
                    .values()
                    .any(|safe| safe.registered_nodes.iter().any(|node| node == &entry.node))
            })
            .collect())
    }

    async fn query_service_types(&self, service_type: Option<ServiceTypeId>) -> Result<Vec<ServiceTypeInfo>> {
        Ok(self
            .state
            .read()
            .service_types
            .values()
            .filter(|info| service_type.is_none_or(|wanted| service_type_matches(&info.service_type, &wanted)))
            .cloned()
            .collect())
    }

    async fn query_service_registry_config(&self) -> Result<ServiceRegistryConfig> {
        Ok(self.state.read().service_registry_config.clone())
    }

    async fn query_transaction_status(&self, tx_id: TxId) -> Result<Transaction> {
        self.state
            .read()
            .active_txs
            .get(&tx_id)
            .cloned()
            .ok_or_else(|| ErrorKind::NoData.into())
    }

    async fn query_chain_info(&self) -> Result<ChainInfo> {
        Ok(self.state.read().chain_info.clone())
    }

    async fn query_version(&self) -> Result<String> {
        Ok(self.state.read().version.clone())
    }

    async fn query_health(&self) -> Result<String> {
        Ok(self.state.read().health.clone())
    }

    async fn query_compatibility(&self) -> Result<Compatibility> {
        Ok(Compatibility {
            api_version: env!("CARGO_PKG_VERSION").to_string(),
            supported_client_versions: "*".to_string(),
            features: vec![],
        })
    }
}

impl<M: BlokliTestStateMutator + Send + Sync> BlokliSubscriptionClient for BlokliTestClient<M> {
    fn subscribe_channels(&self, selector: ChannelSelector) -> Result<impl Stream<Item = Result<Channel>> + Send> {
        Ok(if selector.matches_all() {
            let channels = self.state.read().channels.clone();
            futures::stream::iter(channels.into_values())
                .map(Ok)
                .chain(
                    self.channels_channel
                        .1
                        .activate_cloned()
                        .map(|(_, channel, _)| Ok(channel)),
                )
                .boxed()
        } else {
            let accounts = self.state.read().accounts.clone();
            futures::stream::iter(self.do_query_channels(selector.clone())?)
                .map(Ok)
                .chain(
                    self.channels_channel
                        .1
                        .activate_cloned()
                        .filter(move |(_, c, _)| futures::future::ready(channel_matches(c, &selector, &accounts)))
                        .map(|(_, channel, _)| Ok(channel)),
                )
                .boxed()
        })
    }

    fn subscribe_accounts(&self, selector: AccountSelector) -> Result<impl Stream<Item = Result<Account>> + Send> {
        Ok(match selector {
            AccountSelector::Any => {
                let accounts = self.state.read().accounts.clone();
                futures::stream::iter(accounts.into_values())
                    .map(Ok)
                    .chain(self.accounts_channel.1.activate_cloned().map(Ok))
                    .boxed()
            }
            selector => futures::stream::iter(self.do_query_accounts(selector.clone())?)
                .map(Ok)
                .chain(
                    self.accounts_channel
                        .1
                        .activate_cloned()
                        .filter(move |a| futures::future::ready(account_matches(a, &selector)))
                        .map(Ok),
                )
                .boxed(),
        })
    }

    fn subscribe_graph(&self) -> Result<impl Stream<Item = Result<OpenedChannelsGraphEntry>> + Send> {
        let (accounts, channels) = {
            let state = self.state.read();
            (state.accounts.clone(), state.channels.clone())
        };

        Ok(futures::stream::iter(channels.into_values().map(move |channel| {
            let source = accounts
                .get(&(channel.source as u32))
                .cloned()
                .ok_or_else(|| BlokliClientError::from(ErrorKind::NoData))?;
            let destination = accounts
                .get(&(channel.destination as u32))
                .cloned()
                .ok_or_else(|| BlokliClientError::from(ErrorKind::NoData))?;

            Ok::<_, BlokliClientError>(OpenedChannelsGraphEntry {
                channel,
                destination,
                source,
            })
        }))
        .chain(
            self.channels_channel
                .1
                .activate_cloned()
                .map(|(source, channel, destination)| {
                    Ok(OpenedChannelsGraphEntry {
                        channel,
                        destination,
                        source,
                    })
                }),
        ))
    }

    fn subscribe_ticket_params(&self) -> Result<impl Stream<Item = Result<TicketParameters>> + Send> {
        let info = self.state.read().chain_info.clone();
        Ok(futures::stream::once(futures::future::ready(TicketParameters {
            min_ticket_winning_probability: info.min_ticket_winning_probability,
            ticket_price: info.ticket_price,
        }))
        .chain(self.ticket_channel.1.activate_cloned())
        .map(Ok))
    }

    fn subscribe_health(&self) -> Result<impl Stream<Item = Result<ReadinessState>> + Send> {
        Ok(futures::stream::once(futures::future::ready(Ok(ReadinessState::Ready))).boxed())
    }

    fn subscribe_safe_deployments(&self) -> Result<impl Stream<Item = Result<Safe>> + Send> {
        let safes = self.state.read().deployed_safes.clone();
        Ok(futures::stream::iter(safes.into_values())
            .chain(self.safe_deployed_channel.1.activate_cloned())
            .map(Ok))
    }

    /// Streams current matching entries followed by changes produced by simulated transactions.
    fn subscribe_services(
        &self,
        selector: ServiceSelector,
    ) -> Result<impl Stream<Item = Result<ServiceUpdate>> + Send> {
        let (initial, updates) = {
            let state = self.state.read();
            let initial = state
                .services
                .values()
                .filter(|entry| service_matches(&entry.service_type, &entry.node, &selector))
                .cloned()
                .map(|entry| ServiceUpdate {
                    kind: ServiceUpdateKind::Registered,
                    service_type: entry.service_type.clone(),
                    node: entry.node.clone(),
                    entry: Some(entry),
                })
                .collect::<Vec<_>>();
            (initial, self.services_channel.1.activate_cloned())
        };
        Ok(futures::stream::iter(initial)
            .chain(updates)
            .filter(move |update| {
                futures::future::ready(service_matches(&update.service_type, &update.node, &selector))
            })
            .map(Ok))
    }

    /// Streams service type configuration changes produced by simulated transactions.
    ///
    /// The two registry-wide kinds, [`ServiceTypeUpdateKind::RegistrationFeeChanged`] and
    /// [`ServiceTypeUpdateKind::RegistryPointerChanged`], are never emitted: [`BlokliTestState`] models per-type
    /// configuration only.
    fn subscribe_service_types(
        &self,
        service_type: Option<ServiceTypeId>,
    ) -> Result<impl Stream<Item = Result<ServiceTypeUpdate>> + Send> {
        let (initial, updates) = {
            let state = self.state.read();
            let initial = state
                .service_types
                .values()
                .filter(|config| service_type.is_none_or(|wanted| service_type_matches(&config.service_type, &wanted)))
                .cloned()
                .map(|config| ServiceTypeUpdate {
                    kind: ServiceTypeUpdateKind::Registered,
                    service_type: Some(config.service_type.clone()),
                    config: Some(config),
                    registry_config: None,
                })
                .collect::<Vec<_>>();
            (initial, self.service_types_channel.1.activate_cloned())
        };
        Ok(futures::stream::iter(initial)
            .chain(updates)
            .filter(move |update| {
                futures::future::ready(service_type.is_none_or(|wanted| {
                    update
                        .service_type
                        .as_deref()
                        .is_some_and(|rendered| service_type_matches(rendered, &wanted))
                }))
            })
            .map(Ok))
    }

    fn subscribe_service_registry_config(
        &self,
    ) -> Result<impl Stream<Item = Result<ServiceRegistryConfig>> + Send + 'static> {
        // Activate the receiver while holding the state read lock. Simulated transactions hold the
        // write lock through mutation and publication, so no update can fall between the snapshot
        // and live portions of this test stream.
        let (initial, updates) = {
            let state = self.state.read();
            (
                state.service_registry_config.clone(),
                self.service_registry_config_channel.1.activate_cloned(),
            )
        };
        Ok(futures::stream::once(futures::future::ready(initial))
            .chain(updates)
            .map(Ok))
    }

    fn subscribe_track_transaction(
        &self,
        tx_id: TxId,
    ) -> Result<impl futures::Stream<Item = Result<types::Transaction>> + Send> {
        let tx = self
            .state
            .write()
            .active_txs
            .shift_remove(&tx_id)
            .ok_or_else(|| BlokliClientError::from(ErrorKind::NoData))?;

        Ok(futures::stream::once(futures::future::ok(tx)).delay(Duration2::from(self.tx_simulation_delay)))
    }

    fn subscribe_ticket_redeemed(
        &self,
        _selector: TicketSelector,
    ) -> Result<impl futures::Stream<Item = Result<RedeemTicketDetails>> + Send> {
        Ok(futures::stream::empty())
    }

    fn subscribe_curvy_pending_notes(
        &self,
        _from_block: Option<u64>,
    ) -> Result<impl futures::Stream<Item = Result<CurvyPendingNote>> + Send> {
        Ok(futures::stream::empty())
    }

    fn subscribe_curvy_committed_notes(
        &self,
        _from_block: Option<u64>,
    ) -> Result<impl futures::Stream<Item = Result<CurvyCommittedNote>> + Send> {
        Ok(futures::stream::empty())
    }

    fn subscribe_curvy_committed_nullifiers(
        &self,
        _from_block: Option<u64>,
    ) -> Result<impl futures::Stream<Item = Result<CurvyCommittedNullifier>> + Send> {
        Ok(futures::stream::empty())
    }
}

/// Broadcast senders that [`simulate_tx_execution`] notifies once the mutator has been applied.
struct SubscriptionSenders<'a> {
    accounts: &'a async_broadcast::Sender<Account>,
    channels: &'a async_broadcast::Sender<(Account, Channel, Account)>,
    tickets: &'a async_broadcast::Sender<TicketParameters>,
    safes: &'a async_broadcast::Sender<Safe>,
    services: &'a async_broadcast::Sender<ServiceUpdate>,
    service_types: &'a async_broadcast::Sender<ServiceTypeUpdate>,
    service_registry_config: &'a async_broadcast::Sender<ServiceRegistryConfig>,
}

impl<M: BlokliTestStateMutator> BlokliTestClient<M> {
    fn subscription_senders(&self) -> SubscriptionSenders<'_> {
        SubscriptionSenders {
            accounts: &self.accounts_channel.0,
            channels: &self.channels_channel.0,
            tickets: &self.ticket_channel.0,
            safes: &self.safe_deployed_channel.0,
            services: &self.services_channel.0,
            service_types: &self.service_types_channel.0,
            service_registry_config: &self.service_registry_config_channel.0,
        }
    }
}

/// Broadcasts the registry entry changes between two states.
///
/// Removals are broadcast as [`ServiceUpdateKind::Deregistered`] rather than rejected, unlike the removals guarded
/// against in [`simulate_tx_execution`]: deregistration is a first-class registry operation.
fn broadcast_service_changes(
    old_state: &BlokliTestState,
    state: &BlokliTestState,
    services_channel: &async_broadcast::Sender<ServiceUpdate>,
) {
    for (key, old_entry) in &old_state.services {
        if !state.services.contains_key(key) {
            broadcast_or_log(
                services_channel,
                ServiceUpdate {
                    kind: ServiceUpdateKind::Deregistered,
                    service_type: old_entry.service_type.clone(),
                    node: old_entry.node.clone(),
                    entry: None,
                },
                "service deregistration",
            );
        }
    }

    for (key, new_entry) in &state.services {
        let kind = match old_state.services.get(key) {
            None => ServiceUpdateKind::Registered,
            Some(old_entry) if old_entry != new_entry => ServiceUpdateKind::Updated,
            Some(_) => continue,
        };

        broadcast_or_log(
            services_channel,
            ServiceUpdate {
                kind,
                service_type: new_entry.service_type.clone(),
                node: new_entry.node.clone(),
                entry: Some(new_entry.clone()),
            },
            "service entry change",
        );
    }
}

/// Broadcasts the per-type configuration changes between two states.
///
/// One event is emitted per changed field, matching the on-chain events. The two registry-wide kinds are never
/// emitted, because [`BlokliTestState`] models per-type configuration only.
fn broadcast_service_type_changes(
    old_state: &BlokliTestState,
    state: &BlokliTestState,
    service_types_channel: &async_broadcast::Sender<ServiceTypeUpdate>,
) {
    for (key, new_type) in &state.service_types {
        let kinds: Vec<ServiceTypeUpdateKind> = match old_state.service_types.get(key) {
            None => vec![ServiceTypeUpdateKind::Registered],
            Some(old_type) => [
                (old_type.owner != new_type.owner).then_some(ServiceTypeUpdateKind::OwnerChanged),
                (old_type.requirement != new_type.requirement).then_some(ServiceTypeUpdateKind::RequirementChanged),
                (old_type.registration_burn != new_type.registration_burn)
                    .then_some(ServiceTypeUpdateKind::RegistrationBurnChanged),
                (old_type.update_burn != new_type.update_burn).then_some(ServiceTypeUpdateKind::UpdateBurnChanged),
            ]
            .into_iter()
            .flatten()
            .collect(),
        };

        for kind in kinds {
            broadcast_or_log(
                service_types_channel,
                ServiceTypeUpdate {
                    kind,
                    service_type: Some(new_type.service_type.clone()),
                    config: Some(new_type.clone()),
                    registry_config: None,
                },
                "service type change",
            );
        }
    }
}

fn simulate_tx_execution(
    signed_tx: &[u8],
    state: &mut BlokliTestState,
    mutator: &dyn BlokliTestStateMutator,
    senders: SubscriptionSenders<'_>,
) -> Result<()> {
    let old_state = state.clone();
    if let Err(error) = mutator.update_state(signed_tx, state) {
        *state = old_state;
        return Err(error);
    }

    if old_state.accounts.len() > state.accounts.len() {
        *state = old_state;
        return Err(ErrorKind::MockClientError(anyhow::anyhow!("mutation cannot remove accounts")).into());
    }

    if old_state.channels.len() > state.channels.len() {
        *state = old_state;
        return Err(ErrorKind::MockClientError(anyhow::anyhow!("mutation cannot remove channels")).into());
    }

    if old_state.native_balances.len() > state.native_balances.len() {
        *state = old_state;
        return Err(ErrorKind::MockClientError(anyhow::anyhow!("mutation cannot remove native balances")).into());
    }

    if old_state.token_balances.len() > state.token_balances.len() {
        *state = old_state;
        return Err(ErrorKind::MockClientError(anyhow::anyhow!("mutation cannot remove token balances")).into());
    }

    if old_state.safe_allowances.len() > state.safe_allowances.len() {
        *state = old_state;
        return Err(ErrorKind::MockClientError(anyhow::anyhow!("mutation cannot remove safe allowances")).into());
    }

    if old_state.active_txs.len() > state.active_txs.len() {
        *state = old_state;
        return Err(ErrorKind::MockClientError(anyhow::anyhow!("mutation cannot remove active txs")).into());
    }

    // Service registry entries are deliberately absent from these guards, because deregistration removes an entry.
    // Service types are guarded, because abandoning a type clears its owner instead of removing the type.
    if old_state.service_types.len() > state.service_types.len() {
        *state = old_state;
        return Err(ErrorKind::MockClientError(anyhow::anyhow!("mutation cannot remove service types")).into());
    }

    // Compare accounts and broadcast changes
    state
        .accounts
        .iter()
        .filter(|&(new_id, new_account)| {
            old_state.accounts.get(new_id).is_none_or(|old_account| {
                // Change is notified only if safe address or multi addresses changed
                old_account.safe_address != new_account.safe_address
                    || old_account.multi_addresses != new_account.multi_addresses
            })
        })
        .for_each(
            |(_, changed_account)| match senders.accounts.try_broadcast(changed_account.clone()) {
                Err(TrySendError::Full(_)) => {
                    tracing::error!("failed to broadcast account change - channel is full");
                }
                Err(TrySendError::Closed(_)) => {
                    tracing::error!("failed to broadcast account change - channel is closed");
                }
                _ => {}
            },
        );

    // Compare channels and broadcast changes
    state
        .channels
        .iter()
        .filter(|&(new_id, new_channel)| {
            old_state
                .channels
                .get(new_id)
                .is_none_or(|old_channel| old_channel != new_channel)
        })
        .filter_map(|(_, changed_channel)| {
            let source = state.accounts.get(&(changed_channel.source as u32)).cloned();
            let destination = state.accounts.get(&(changed_channel.destination as u32)).cloned();
            source
                .zip(destination)
                .map(|(source, destination)| (source, changed_channel.clone(), destination))
        })
        .for_each(|(source, changed_channel, destination)| {
            match senders.channels.try_broadcast((source, changed_channel, destination)) {
                Err(TrySendError::Full(_)) => {
                    tracing::error!("failed to broadcast channel change - channel is full");
                }
                Err(TrySendError::Closed(_)) => {
                    tracing::error!("failed to broadcast channel change - channel is closed");
                }
                _ => {}
            }
        });

    if state.chain_info.min_ticket_winning_probability != old_state.chain_info.min_ticket_winning_probability
        || state.chain_info.ticket_price != old_state.chain_info.ticket_price
    {
        match senders.tickets.try_broadcast(TicketParameters {
            min_ticket_winning_probability: state.chain_info.min_ticket_winning_probability,
            ticket_price: state.chain_info.ticket_price.clone(),
        }) {
            Err(TrySendError::Full(_)) => {
                tracing::error!("failed to broadcast ticket params change - channel is full");
            }
            Err(TrySendError::Closed(_)) => {
                tracing::error!("failed to broadcast ticket params change - channel is closed");
            }
            _ => {}
        }
    }

    // Compare safes and broadcast changes
    state
        .deployed_safes
        .iter()
        .filter(|&(new_id, new_safe)| {
            old_state
                .deployed_safes
                .get(new_id)
                .is_none_or(|old_safe| old_safe != new_safe)
        })
        .for_each(
            |(_, changed_safe)| match senders.safes.try_broadcast(changed_safe.clone()) {
                Err(TrySendError::Full(_)) => {
                    tracing::error!("failed to broadcast safe change - channel is full");
                }
                Err(TrySendError::Closed(_)) => {
                    tracing::error!("failed to broadcast safe change - channel is closed");
                }
                _ => {}
            },
        );

    broadcast_service_changes(&old_state, state, senders.services);
    broadcast_service_type_changes(&old_state, state, senders.service_types);
    if old_state.service_registry_config != state.service_registry_config {
        broadcast_or_log(
            senders.service_registry_config,
            state.service_registry_config.clone(),
            "service registry configuration change",
        );
    }

    Ok(())
}

#[async_trait::async_trait]
impl<M: BlokliTestStateMutator + Send + Sync> BlokliTransactionClient for BlokliTestClient<M> {
    async fn submit_transaction(&self, signed_tx: &[u8]) -> Result<TxReceipt> {
        let mut tx_receipt = [0u8; 32];
        rand::fill(&mut tx_receipt);

        let mut state = self.state.write();
        if let Err(error) = simulate_tx_execution(signed_tx, &mut state, &self.mutator, self.subscription_senders()) {
            tracing::error!(%error, signed_tx_data = hex::encode(signed_tx), "failed to execute transaction, state reverted");
        } else {
            tracing::debug!("transaction execution succeeded");
        }

        Ok(tx_receipt)
    }

    async fn submit_and_track_transaction(&self, signed_tx: &[u8]) -> Result<TxId> {
        let tx_id = hex::encode(rand::random_iter::<u8>().take(16).collect::<Vec<_>>());
        let tx_hash = hex::encode(rand::random_iter::<u8>().take(32).collect::<Vec<_>>());

        let mut state = self.state.write();

        let mut internal_tx_failure_reason: Option<String> = None;

        let status = simulate_tx_execution(
            signed_tx,
            &mut state,
            &self.mutator,
            self.subscription_senders(),
        )
        .map(|_| {
            tracing::debug!("transaction execution succeeded");
            TransactionStatus::Confirmed
        })
        .inspect_err(|e| if let ErrorKind::MockClientError(int_err) = e.kind() {
            internal_tx_failure_reason = int_err.downcast_ref::<InternalTxError>().map(|err| err.0.to_string());
        })
        .unwrap_or_else(|error| {
            tracing::error!(%error, signed_tx_data = hex::encode(signed_tx), "failed to execute transaction, state reverted");
            // Make the outer transaction confirmed if there was an internal transaction failure
            if self.use_internal_txs && internal_tx_failure_reason.is_some() {
                TransactionStatus::Confirmed
            } else {
                TransactionStatus::Reverted
            }
        });

        state.active_txs.insert(
            tx_id.clone(),
            Transaction {
                id: tx_id.clone().into(),
                status,
                submitted_at: DateTime(chrono::DateTime::<chrono::Utc>::from(SystemTime::now()).to_rfc3339()),
                transaction_hash: Hex32(tx_hash.clone()),
                safe_execution: (self.use_internal_txs && status == TransactionStatus::Confirmed).then(|| {
                    SafeExecution {
                        success: internal_tx_failure_reason.is_none(),
                        safe_tx_hash: Some(Hex32(tx_hash)),
                        revert_reason: internal_tx_failure_reason,
                    }
                }),
            },
        );

        Ok(tx_id)
    }

    async fn submit_and_confirm_transaction(&self, signed_tx: &[u8], num_confirmations: usize) -> Result<TxReceipt> {
        futures_time::task::sleep((self.tx_simulation_delay * num_confirmations as u32).into()).await;

        let mut tx_receipt = [0u8; 32];
        rand::fill(&mut tx_receipt);

        let mut state = self.state.write();
        simulate_tx_execution(
            signed_tx,
            &mut state,
            &self.mutator,
            self.subscription_senders(),
        )
        .inspect_err(|error| {
            tracing::error!(%error, signed_tx_data = hex::encode(signed_tx), "failed to execute transaction, state reverted");
        })?;

        tracing::debug!("transaction execution succeeded");
        Ok(tx_receipt)
    }

    async fn track_transaction(&self, tx_id: TxId, client_timeout: Duration) -> Result<Transaction> {
        futures_time::task::sleep(self.tx_simulation_delay.min(client_timeout.div(2)).into()).await;
        let tx = self
            .state
            .write()
            .active_txs
            .shift_remove(&tx_id)
            .ok_or_else(|| BlokliClientError::from(ErrorKind::NoData))?;

        match tx.status {
            TransactionStatus::Confirmed => Ok(tx),
            TransactionStatus::Timeout => Err(ErrorKind::TrackingError(TrackingErrorKind::Timeout).into()),
            TransactionStatus::SubmissionFailed => {
                Err(ErrorKind::TrackingError(TrackingErrorKind::SubmissionFailed).into())
            }
            TransactionStatus::ValidationFailed => {
                Err(ErrorKind::TrackingError(TrackingErrorKind::ValidationFailed).into())
            }
            TransactionStatus::Reverted => Err(ErrorKind::TrackingError(TrackingErrorKind::Reverted).into()),
            _ => Err(ErrorKind::MockClientError(anyhow::anyhow!("unexpected transaction status")).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures::StreamExt;

    use super::{
        BlokliQueryClient, BlokliSubscriptionClient, BlokliTestClient, BlokliTestState, BlokliTransactionClient,
        ChainAddress, NopStateMutator, Result, ServiceEntry, ServiceRegistryConfig, ServiceSelector, ServiceTypeInfo,
        ServiceTypeUpdateKind, ServiceUpdateKind, Uint64,
    };

    /// `bytes32("gvpn:exit")`, the canonical id of the GnosisVPN exit-node service.
    const GVPN_EXIT: [u8; 32] = [
        0x67, 0x76, 0x70, 0x6e, 0x3a, 0x65, 0x78, 0x69, 0x74, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ];
    const NODE: ChainAddress = [0x11; 20];
    const OTHER_NODE: ChainAddress = [0x22; 20];

    fn entry(service_type: &str, node: &ChainAddress) -> ServiceEntry {
        ServiceEntry {
            service_type: service_type.to_string(),
            node: hex::encode(node),
            safe: hex::encode([0x33; 20]),
            metadata: "0xdeadbeef".to_string(),
            registered_at: Uint64("1700000000".into()),
            updated_at: Uint64("1700000000".into()),
        }
    }

    fn service_type_info(service_type: &str, owner: Option<&str>) -> ServiceTypeInfo {
        ServiceTypeInfo {
            service_type: service_type.to_string(),
            owner: owner.map(str::to_string),
            requirement: None,
            registration_burn: "1 wxHOPR".to_string(),
            update_burn: "0 wxHOPR".to_string(),
        }
    }

    fn state_with_entries() -> BlokliTestState {
        let mut state = BlokliTestState::default();
        state.services.insert(
            BlokliTestState::service_entry_key("gvpn:exit", &NODE),
            entry("gvpn:exit", &NODE),
        );
        state.services.insert(
            BlokliTestState::service_entry_key("gvpn:relay", &OTHER_NODE),
            entry("gvpn:relay", &OTHER_NODE),
        );
        state
    }

    /// `hopr_types::chain::ContractAddresses` puts `#[serde(default)]` on none of its fields, so a key missing from
    /// this blob is a runtime failure on the first transaction of any consumer test, not a compile error.
    #[test]
    fn default_contract_addresses_include_the_service_registry() -> anyhow::Result<()> {
        let blob = BlokliTestState::default().chain_info.contract_addresses.0;
        let addresses: BTreeMap<String, String> = serde_json::from_str(&blob)?;

        // Consumers deserialize this blob into `hopr_types::chain::ContractAddresses`, which
        // under `use-bindings` is the bindings struct and carries no `#[serde(default)]` on any
        // field. A missing key is therefore a runtime failure on the first transaction of every
        // test that builds a dynamic client without calling a `with_*_chain_info` builder - and
        // it cannot fail to compile, because this is a string literal. This crate deliberately
        // does not depend on `hopr-types/chain`, so the field list is spelled out here instead
        // of being derived; keep it in step with that struct.
        let required = [
            "announcements",
            "channels",
            "module_implementation",
            "node_safe_migration",
            "node_safe_registry",
            "node_stake_factory",
            "service_registry",
            "ticket_price_oracle",
            "token",
            "winning_probability_oracle",
            "xhopr_token",
        ];

        let missing: Vec<&str> = required
            .into_iter()
            .filter(|key| !addresses.contains_key(*key))
            .collect();

        assert_eq!(Vec::<&str>::new(), missing);
        insta::assert_yaml_snapshot!(addresses);

        Ok(())
    }

    #[tokio::test]
    async fn query_services_accepts_the_any_selector() -> anyhow::Result<()> {
        let client = BlokliTestClient::new(state_with_entries(), NopStateMutator);

        assert_eq!(client.query_services(ServiceSelector::Any).await?.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn count_services_accepts_the_any_selector() -> anyhow::Result<()> {
        let client = BlokliTestClient::new(state_with_entries(), NopStateMutator);

        assert_eq!(client.count_services(ServiceSelector::Any).await?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn query_services_matches_a_service_type_written_as_its_ascii_name() -> anyhow::Result<()> {
        let client = BlokliTestClient::new(state_with_entries(), NopStateMutator);

        let entries = client.query_services(ServiceSelector::ServiceType(GVPN_EXIT)).await?;

        insta::assert_yaml_snapshot!(entries);
        Ok(())
    }

    #[tokio::test]
    async fn query_services_matches_a_service_type_written_as_hex() -> anyhow::Result<()> {
        let mut state = BlokliTestState::default();
        let hex_id = format!("0x{}", hex::encode(GVPN_EXIT));
        state.services.insert(
            BlokliTestState::service_entry_key(&hex_id, &NODE),
            entry(&hex_id, &NODE),
        );
        let client = BlokliTestClient::new(state, NopStateMutator);

        let entries = client.query_services(ServiceSelector::ServiceType(GVPN_EXIT)).await?;

        assert_eq!(entries.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn query_services_narrows_to_one_node() -> anyhow::Result<()> {
        let client = BlokliTestClient::new(state_with_entries(), NopStateMutator);

        let entries = client.query_services(ServiceSelector::Node(OTHER_NODE)).await?;

        insta::assert_yaml_snapshot!(entries);
        Ok(())
    }

    #[tokio::test]
    async fn query_service_types_returns_every_type_when_unfiltered() -> anyhow::Result<()> {
        let mut state = BlokliTestState::default();
        state
            .service_types
            .insert("gvpn:exit".to_string(), service_type_info("gvpn:exit", Some("0x4444")));
        state
            .service_types
            .insert("gvpn:relay".to_string(), service_type_info("gvpn:relay", None));
        let client = BlokliTestClient::new(state, NopStateMutator);

        let types = client.query_service_types(None).await?;

        insta::assert_yaml_snapshot!(types);
        Ok(())
    }

    #[tokio::test]
    async fn query_service_registry_config_returns_current_configuration() -> anyhow::Result<()> {
        let mut state = BlokliTestState::default();
        state.service_registry_config = ServiceRegistryConfig {
            type_registration_fee: "1000 wxHOPR".into(),
            node_safe_registry: "0x4444444444444444444444444444444444444444".into(),
        };
        let client = BlokliTestClient::new(state, NopStateMutator);

        let config = client.query_service_registry_config().await?;

        insta::assert_yaml_snapshot!(config);
        Ok(())
    }

    #[tokio::test]
    async fn subscribe_services_reports_registration_update_and_deregistration() -> anyhow::Result<()> {
        let client = BlokliTestClient::new(
            BlokliTestState::default(),
            |signed_tx: &[u8], state: &mut BlokliTestState| {
                let key = BlokliTestState::service_entry_key("gvpn:exit", &NODE);
                match signed_tx {
                    [0] => {
                        state.services.insert(key, entry("gvpn:exit", &NODE));
                    }
                    [1] => {
                        let mut updated = entry("gvpn:exit", &NODE);
                        updated.metadata = "0xc0ffee".to_string();
                        updated.updated_at = Uint64("1700000100".into());
                        state.services.insert(key, updated);
                    }
                    _ => {
                        state.services.shift_remove(&key);
                    }
                }
                Result::Ok(())
            },
        );

        let mut stream = client.subscribe_services(ServiceSelector::ServiceType(GVPN_EXIT))?;
        for step in [0u8, 1, 2] {
            client.submit_transaction(&[step]).await?;
        }

        let updates = stream.by_ref().take(3).collect::<Vec<_>>().await;
        let updates = updates.into_iter().collect::<Result<Vec<_>>>()?;

        insta::assert_yaml_snapshot!(updates);
        assert_eq!(
            updates.iter().map(|update| update.kind).collect::<Vec<_>>(),
            vec![
                ServiceUpdateKind::Registered,
                ServiceUpdateKind::Updated,
                ServiceUpdateKind::Deregistered
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn subscribe_service_registry_config_reports_snapshot_then_update() -> anyhow::Result<()> {
        let mut initial = BlokliTestState::default();
        initial.service_registry_config = ServiceRegistryConfig {
            type_registration_fee: "1 wxHOPR".into(),
            node_safe_registry: "0x1111111111111111111111111111111111111111".into(),
        };
        let client = BlokliTestClient::new(initial, |_: &[u8], state: &mut BlokliTestState| {
            state.service_registry_config = ServiceRegistryConfig {
                type_registration_fee: "2 wxHOPR".into(),
                node_safe_registry: "0x2222222222222222222222222222222222222222".into(),
            };
            Result::Ok(())
        });

        let mut stream = client.subscribe_service_registry_config()?;
        let initial = stream
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing snapshot"))??;
        client.submit_transaction(&[0]).await?;
        let updated = stream.next().await.ok_or_else(|| anyhow::anyhow!("missing update"))??;

        insta::assert_yaml_snapshot!(vec![initial, updated]);
        Ok(())
    }

    #[tokio::test]
    async fn subscribe_services_filters_out_other_nodes() -> anyhow::Result<()> {
        let client = BlokliTestClient::new(BlokliTestState::default(), |_: &[u8], state: &mut BlokliTestState| {
            state.services.insert(
                BlokliTestState::service_entry_key("gvpn:exit", &OTHER_NODE),
                entry("gvpn:exit", &OTHER_NODE),
            );
            state.services.insert(
                BlokliTestState::service_entry_key("gvpn:exit", &NODE),
                entry("gvpn:exit", &NODE),
            );
            Result::Ok(())
        });

        let mut stream = client.subscribe_services(ServiceSelector::Node(NODE))?;
        client.submit_transaction(&[0]).await?;

        let update = stream
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("service subscription ended early"))??;

        assert_eq!(update.node, hex::encode(NODE));
        Ok(())
    }

    #[tokio::test]
    async fn subscribe_service_types_reports_one_event_per_changed_field() -> anyhow::Result<()> {
        let mut initial = BlokliTestState::default();
        initial
            .service_types
            .insert("gvpn:exit".to_string(), service_type_info("gvpn:exit", Some("0x4444")));

        let client = BlokliTestClient::new(initial, |_: &[u8], state: &mut BlokliTestState| {
            let info = state
                .service_types
                .get_mut("gvpn:exit")
                .ok_or_else(|| anyhow::anyhow!("missing service type"))
                .map_err(|e| crate::errors::ErrorKind::MockClientError(e))?;
            info.owner = None;
            info.update_burn = "5 wei wxHOPR".to_string();
            Result::Ok(())
        });

        let mut stream = client.subscribe_service_types(Some(GVPN_EXIT))?;
        client.submit_transaction(&[0]).await?;

        let updates = stream.by_ref().take(2).collect::<Vec<_>>().await;
        let updates = updates.into_iter().collect::<Result<Vec<_>>>()?;

        assert_eq!(
            updates.iter().map(|update| update.kind).collect::<Vec<_>>(),
            vec![
                ServiceTypeUpdateKind::OwnerChanged,
                ServiceTypeUpdateKind::UpdateBurnChanged
            ]
        );
        insta::assert_yaml_snapshot!(updates);
        Ok(())
    }
}
