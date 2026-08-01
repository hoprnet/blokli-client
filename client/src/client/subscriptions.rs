use cynic::SubscriptionBuilder;
use futures::{Stream, TryStreamExt};

use super::{BlokliClient, GraphQlQueries};
use crate::api::{
    AccountSelector, BlokliSubscriptionClient, ChannelSelector, Result, TicketSelector, TxId,
    internal::{
        AccountVariables, ChannelsVariables, CurvyEventCursor, CurvyNoteEventFilter, CurvyNoteEventVariables,
        SubscribeAccounts, SubscribeChannels, SubscribeCurvyNoteEvents, SubscribeGraph, SubscribeHealth,
        SubscribeSafeDeployment, SubscribeTicketParams, SubscribeTicketRedeemed, TicketRedeemedVariables,
    },
    types::{
        Account, Channel, DepositEvent, DepositEventCursor, DepositEventFilter, OpenedChannelsGraphEntry,
        ReadinessState, RedeemTicketDetails, Safe, TicketParameters, Transaction,
    },
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

    /// `SubscribeTicketRedeemed` subscription GraphQL query.
    pub fn subscribe_ticket_redeemed(
        selector: TicketSelector,
    ) -> cynic::StreamingOperation<SubscribeTicketRedeemed, TicketRedeemedVariables> {
        SubscribeTicketRedeemed::build(TicketRedeemedVariables::from(selector))
    }

    /// Deposit lifecycle subscription backed by the raw Curvy note event operation.
    pub fn subscribe_deposit_events(
        after: Option<DepositEventCursor>,
        filter: DepositEventFilter,
    ) -> cynic::StreamingOperation<SubscribeCurvyNoteEvents, CurvyNoteEventVariables> {
        SubscribeCurvyNoteEvents::build(CurvyNoteEventVariables {
            after: after.map(CurvyEventCursor::from),
            filter: Some(CurvyNoteEventFilter::from(filter)),
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

    #[tracing::instrument(
        level = "debug",
        skip(self, filter),
        fields(
            ?after,
            event_kind_count = filter.event_kind_count(),
            deposit_note_id_count = filter.deposit_note_id_count(),
        )
    )]
    fn subscribe_deposit_events(
        &self,
        after: Option<DepositEventCursor>,
        filter: DepositEventFilter,
    ) -> Result<impl futures::Stream<Item = Result<DepositEvent>> + Send> {
        Ok(self
            .build_subscription_stream(GraphQlQueries::subscribe_deposit_events(after, filter))?
            .try_filter_map(|item| futures::future::ok(DepositEvent::from_graphql(item.curvy_note_events))))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::GraphQlQueries;
    use crate::api::types::{DepositEventCursor, DepositEventFilter};

    #[test]
    fn deposit_subscription_serializes_cursor_and_filter_variables() {
        let operation = GraphQlQueries::subscribe_deposit_events(
            Some(DepositEventCursor("9:0:0:0".to_string())),
            DepositEventFilter::completions(vec!["42".to_string(), "43".to_string()]),
        );

        let serialized = serde_json::to_value(operation).expect("subscription operation should serialize");

        assert_eq!(
            serialized["variables"],
            json!({
                "after": "9:0:0:0",
                "filter": {
                    "kinds": ["COMMITTED"],
                    "noteIds": ["42", "43"],
                },
            })
        );
        assert!(
            serialized["query"]
                .as_str()
                .is_some_and(|query| query.contains("curvyNoteEvents"))
        );
    }
}
