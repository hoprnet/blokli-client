use cynic::SubscriptionBuilder;
use futures::{Stream, TryStreamExt};

use super::{BlokliClient, GraphQlQueries};
use crate::api::{
    AccountSelector, BlokliSubscriptionClient, ChannelSelector, Result, ServiceSelector, ServiceTypeId, TicketSelector,
    TxId,
    internal::{
        AccountVariables, ChannelsVariables, ServiceTypeVariables, ServiceVariables, SubscribeAccounts,
        SubscribeChannels, SubscribeGraph, SubscribeHealth, SubscribeSafeDeployment, SubscribeServiceRegistryConfig,
        SubscribeServiceTypes, SubscribeServices, SubscribeTicketParams, SubscribeTicketRedeemed,
        TicketRedeemedVariables,
    },
    types::{
        Account, Channel, OpenedChannelsGraphEntry, ReadinessState, RedeemTicketDetails, Safe, ServiceRegistryConfig,
        ServiceTypeUpdate, ServiceUpdate, TicketParameters, Transaction,
    },
};
#[cfg(feature = "curvy")]
use crate::api::{
    internal::{
        CurvyEventSubscriptionVariables, SubscribeCurvyCommittedNote, SubscribeCurvyCommittedNullifier,
        SubscribeCurvyPendingNote,
    },
    types::{CurvyCommittedNote, CurvyCommittedNullifier, CurvyPendingNote, Uint64},
};

impl GraphQlQueries {
    /// `SubscribeChannels` subscription GraphQL query.
    pub fn subscribe_channels(
        selector: ChannelSelector,
    ) -> cynic::StreamingOperation<SubscribeChannels, ChannelsVariables> {
        SubscribeChannels::build(ChannelsVariables::from(selector))
    }

    /// `SubscribeAccounts` subscription GraphQL query.
    pub fn subscribe_accounts(
        selector: AccountSelector,
    ) -> cynic::StreamingOperation<SubscribeAccounts, AccountVariables> {
        SubscribeAccounts::build(AccountVariables::from(selector))
    }

    /// `SubscribeGraph` subscription GraphQL query.
    pub fn subscribe_graph() -> cynic::StreamingOperation<SubscribeGraph, ()> {
        SubscribeGraph::build(())
    }

    /// `SubscribeTicketParams` subscription GraphQL query.
    pub fn subscribe_ticket_params() -> cynic::StreamingOperation<SubscribeTicketParams, ()> {
        SubscribeTicketParams::build(())
    }

    /// `SubscribeHealth` subscription GraphQL query.
    pub fn subscribe_health() -> cynic::StreamingOperation<SubscribeHealth, ()> {
        SubscribeHealth::build(())
    }

    /// `SubscribeSafeDeployment` subscription GraphQL query.
    pub fn subscribe_safe_deployments() -> cynic::StreamingOperation<SubscribeSafeDeployment, ()> {
        SubscribeSafeDeployment::build(())
    }

    /// `SubscribeServices` subscription GraphQL query.
    pub fn subscribe_services(
        selector: ServiceSelector,
    ) -> cynic::StreamingOperation<SubscribeServices, ServiceVariables> {
        SubscribeServices::build(ServiceVariables::from(selector))
    }

    /// `SubscribeServiceTypes` subscription GraphQL query.
    pub fn subscribe_service_types(
        service_type: Option<ServiceTypeId>,
    ) -> cynic::StreamingOperation<SubscribeServiceTypes, ServiceTypeVariables> {
        SubscribeServiceTypes::build(ServiceTypeVariables::from(service_type))
    }

    /// `SubscribeServiceRegistryConfig` subscription GraphQL query.
    pub fn subscribe_service_registry_config() -> cynic::StreamingOperation<SubscribeServiceRegistryConfig, ()> {
        SubscribeServiceRegistryConfig::build(())
    }

    /// `SubscribeTicketRedeemed` subscription GraphQL query.
    pub fn subscribe_ticket_redeemed(
        selector: TicketSelector,
    ) -> cynic::StreamingOperation<SubscribeTicketRedeemed, TicketRedeemedVariables> {
        SubscribeTicketRedeemed::build(TicketRedeemedVariables::from(selector))
    }

    #[cfg(feature = "curvy")]
    /// Pending Curvy note subscription used for local ownership detection.
    pub fn subscribe_curvy_pending_notes(
        from_block: Option<u64>,
    ) -> cynic::StreamingOperation<SubscribeCurvyPendingNote, CurvyEventSubscriptionVariables> {
        SubscribeCurvyPendingNote::build(CurvyEventSubscriptionVariables {
            from_block: from_block.map(|block| Uint64(block.to_string())),
        })
    }

    #[cfg(feature = "curvy")]
    /// Committed Curvy note subscription used for owned-note correlation.
    pub fn subscribe_curvy_committed_notes(
        from_block: Option<u64>,
    ) -> cynic::StreamingOperation<SubscribeCurvyCommittedNote, CurvyEventSubscriptionVariables> {
        SubscribeCurvyCommittedNote::build(CurvyEventSubscriptionVariables {
            from_block: from_block.map(|block| Uint64(block.to_string())),
        })
    }

    #[cfg(feature = "curvy")]
    /// Committed Curvy nullifier subscription.
    pub fn subscribe_curvy_committed_nullifiers(
        from_block: Option<u64>,
    ) -> cynic::StreamingOperation<SubscribeCurvyCommittedNullifier, CurvyEventSubscriptionVariables> {
        SubscribeCurvyCommittedNullifier::build(CurvyEventSubscriptionVariables {
            from_block: from_block.map(|block| Uint64(block.to_string())),
        })
    }
}

impl BlokliSubscriptionClient for BlokliClient {
    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    fn subscribe_channels(&self, selector: ChannelSelector) -> Result<impl Stream<Item = Result<Channel>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_channels(selector))?
            .try_filter_map(|item| futures::future::ok(Some(item.channel_updated))))
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    fn subscribe_accounts(&self, selector: AccountSelector) -> Result<impl Stream<Item = Result<Account>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_accounts(selector))?
            .try_filter_map(|item| futures::future::ok(Some(item.account_updated))))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_graph(&self) -> Result<impl Stream<Item = Result<OpenedChannelsGraphEntry>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_graph())?
            .try_filter_map(|item| futures::future::ok(Some(item.opened_channel_graph_updated))))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_ticket_params(&self) -> Result<impl Stream<Item = Result<TicketParameters>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_ticket_params())?
            .try_filter_map(|item| futures::future::ok(Some(item.ticket_parameters_updated))))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_health(&self) -> Result<impl Stream<Item = Result<ReadinessState>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_health())?
            .try_filter_map(|item| futures::future::ok(Some(item.health))))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_safe_deployments(&self) -> Result<impl Stream<Item = Result<Safe>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_safe_deployments())?
            .try_filter_map(|item| futures::future::ok(Some(item.safe_deployed))))
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    fn subscribe_services(
        &self,
        selector: ServiceSelector,
    ) -> Result<impl Stream<Item = Result<ServiceUpdate>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_services(selector))?
            .try_filter_map(|item| futures::future::ok(Some(item.service_updated))))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_service_types(
        &self,
        service_type: Option<ServiceTypeId>,
    ) -> Result<impl Stream<Item = Result<ServiceTypeUpdate>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_service_types(service_type))?
            .try_filter_map(|item| futures::future::ok(Some(item.service_type_updated))))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_service_registry_config(
        &self,
    ) -> Result<impl Stream<Item = Result<ServiceRegistryConfig>> + Send + 'static> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_service_registry_config())?
            .try_filter_map(|item| futures::future::ok(Some(item.service_registry_config_updated))))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_track_transaction(&self, tx_id: TxId) -> Result<impl Stream<Item = Result<Transaction>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_track_transaction(tx_id))?
            .try_filter_map(|item| futures::future::ok(Some(item.transaction_updated))))
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    fn subscribe_ticket_redeemed(
        &self,
        selector: TicketSelector,
    ) -> Result<impl futures::Stream<Item = Result<RedeemTicketDetails>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_ticket_redeemed(selector))?
            .try_filter_map(|item| futures::future::ok(Some(item.ticket_redeemed))))
    }

    #[cfg(feature = "curvy")]
    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_curvy_pending_notes(
        &self,
        from_block: Option<u64>,
    ) -> Result<impl futures::Stream<Item = Result<CurvyPendingNote>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_curvy_pending_notes(from_block))?
            .map_ok(|item| item.curvy_pending_note))
    }

    #[cfg(feature = "curvy")]
    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_curvy_committed_notes(
        &self,
        from_block: Option<u64>,
    ) -> Result<impl futures::Stream<Item = Result<CurvyCommittedNote>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_curvy_committed_notes(from_block))?
            .map_ok(|item| item.curvy_committed_note))
    }

    #[cfg(feature = "curvy")]
    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_curvy_committed_nullifiers(
        &self,
        from_block: Option<u64>,
    ) -> Result<impl futures::Stream<Item = Result<CurvyCommittedNullifier>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_curvy_committed_nullifiers(from_block))?
            .map_ok(|item| item.curvy_committed_nullifier))
    }
}

#[cfg(all(test, feature = "curvy"))]
mod tests {
    use serde_json::json;

    use super::GraphQlQueries;
    use crate::api::types::CurvyEventPosition;

    #[test]
    fn curvy_pending_note_subscription_serializes_from_block() {
        let operation = GraphQlQueries::subscribe_curvy_pending_notes(Some(9));

        let serialized = serde_json::to_value(operation).expect("subscription operation should serialize");

        assert_eq!(
            serialized["variables"],
            json!({
                "fromBlock": "9",
            })
        );
        assert!(
            serialized["query"]
                .as_str()
                .is_some_and(|query| query.contains("curvyPendingNote"))
        );
    }

    #[test]
    fn event_position_type_is_publicly_constructible() {
        let _position: Option<CurvyEventPosition> = None;
    }
}
