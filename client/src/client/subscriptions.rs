use cynic::SubscriptionBuilder;
use futures::{Stream, TryStreamExt};

use super::{BlokliClient, GraphQlQueries};
#[cfg(feature = "curvy")]
use crate::api::{
    internal::{
        CurvyEventSubscriptionVariables, SubscribeCurvyCommittedNote, SubscribeCurvyCommittedNullifier,
        SubscribeCurvyPendingNote,
    },
    types::{CurvyCommittedNote, CurvyCommittedNullifier, CurvyPendingNote, Uint64},
};
use crate::{
    api::{
        AccountSelector, BlokliSubscriptionClient, ChannelSelector, Result, ServiceSelector, ServiceTypeId,
        TicketSelector, TxId,
        internal::{
            AccountVariables, ChannelsVariables, ServiceTypeVariables, ServiceVariables, SubscribeAccounts,
            SubscribeChannels, SubscribeGraph, SubscribeHealth, SubscribeSafeDeployment,
            SubscribeServiceRegistryConfig, SubscribeServiceTypes, SubscribeServices, SubscribeTicketParams,
            SubscribeTicketRedeemed, TicketRedeemedVariables,
        },
        types::{
            Account, Channel, OpenedChannelsGraphEntry, ReadinessState, RedeemTicketDetails, Safe,
            ServiceRegistryConfig, ServiceTypeUpdate, ServiceUpdate, TicketParameters, Transaction,
        },
    },
    health::SubscriptionHealthHandle,
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
    fn subscribe_channels(&self, selector: ChannelSelector) -> Result<impl Stream<Item = Result<Channel>> + Send> {
        Ok(self.subscribe_channels_with_health(selector)?.0)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    fn subscribe_channels_with_health(
        &self,
        selector: ChannelSelector,
    ) -> Result<(impl Stream<Item = Result<Channel>> + Send, SubscriptionHealthHandle)> {
        let (stream, health) =
            self.build_subscription_stream(GraphQlQueries::subscribe_channels(selector), "channels")?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.channel_updated))),
            health,
        ))
    }

    fn subscribe_accounts(&self, selector: AccountSelector) -> Result<impl Stream<Item = Result<Account>> + Send> {
        Ok(self.subscribe_accounts_with_health(selector)?.0)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    fn subscribe_accounts_with_health(
        &self,
        selector: AccountSelector,
    ) -> Result<(impl Stream<Item = Result<Account>> + Send, SubscriptionHealthHandle)> {
        let (stream, health) =
            self.build_subscription_stream(GraphQlQueries::subscribe_accounts(selector), "accounts")?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.account_updated))),
            health,
        ))
    }

    fn subscribe_graph(&self) -> Result<impl Stream<Item = Result<OpenedChannelsGraphEntry>> + Send> {
        Ok(self.subscribe_graph_with_health()?.0)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_graph_with_health(
        &self,
    ) -> Result<(
        impl Stream<Item = Result<OpenedChannelsGraphEntry>> + Send,
        SubscriptionHealthHandle,
    )> {
        let (stream, health) = self.build_subscription_stream(GraphQlQueries::subscribe_graph(), "graph")?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.opened_channel_graph_updated))),
            health,
        ))
    }

    fn subscribe_ticket_params(&self) -> Result<impl Stream<Item = Result<TicketParameters>> + Send> {
        Ok(self.subscribe_ticket_params_with_health()?.0)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_ticket_params_with_health(
        &self,
    ) -> Result<(
        impl Stream<Item = Result<TicketParameters>> + Send,
        SubscriptionHealthHandle,
    )> {
        let (stream, health) =
            self.build_subscription_stream(GraphQlQueries::subscribe_ticket_params(), "ticket_params")?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.ticket_parameters_updated))),
            health,
        ))
    }

    fn subscribe_health(&self) -> Result<impl Stream<Item = Result<ReadinessState>> + Send> {
        Ok(self.subscribe_health_with_health()?.0)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_health_with_health(
        &self,
    ) -> Result<(
        impl Stream<Item = Result<ReadinessState>> + Send,
        SubscriptionHealthHandle,
    )> {
        let (stream, health) = self.build_subscription_stream(GraphQlQueries::subscribe_health(), "health")?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.health))),
            health,
        ))
    }

    fn subscribe_safe_deployments(&self) -> Result<impl Stream<Item = Result<Safe>> + Send> {
        Ok(self.subscribe_safe_deployments_with_health()?.0)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_safe_deployments_with_health(
        &self,
    ) -> Result<(impl Stream<Item = Result<Safe>> + Send, SubscriptionHealthHandle)> {
        let (stream, health) =
            self.build_subscription_stream(GraphQlQueries::subscribe_safe_deployments(), "safe_deployments")?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.safe_deployed))),
            health,
        ))
    }

    fn subscribe_services(
        &self,
        selector: ServiceSelector,
    ) -> Result<impl Stream<Item = Result<ServiceUpdate>> + Send> {
        Ok(self.subscribe_services_with_health(selector)?.0)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    fn subscribe_services_with_health(
        &self,
        selector: ServiceSelector,
    ) -> Result<(
        impl Stream<Item = Result<ServiceUpdate>> + Send,
        SubscriptionHealthHandle,
    )> {
        let (stream, health) =
            self.build_subscription_stream(GraphQlQueries::subscribe_services(selector), "services")?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.service_updated))),
            health,
        ))
    }

    fn subscribe_service_types(
        &self,
        service_type: Option<ServiceTypeId>,
    ) -> Result<impl Stream<Item = Result<ServiceTypeUpdate>> + Send> {
        Ok(self.subscribe_service_types_with_health(service_type)?.0)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_service_types_with_health(
        &self,
        service_type: Option<ServiceTypeId>,
    ) -> Result<(
        impl Stream<Item = Result<ServiceTypeUpdate>> + Send,
        SubscriptionHealthHandle,
    )> {
        let (stream, health) =
            self.build_subscription_stream(GraphQlQueries::subscribe_service_types(service_type), "service_types")?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.service_type_updated))),
            health,
        ))
    }

    fn subscribe_service_registry_config(
        &self,
    ) -> Result<impl Stream<Item = Result<ServiceRegistryConfig>> + Send + 'static> {
        Ok(self.subscribe_service_registry_config_with_health()?.0)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_service_registry_config_with_health(
        &self,
    ) -> Result<(
        impl Stream<Item = Result<ServiceRegistryConfig>> + Send + 'static,
        SubscriptionHealthHandle,
    )> {
        let (stream, health) = self.build_subscription_stream(
            GraphQlQueries::subscribe_service_registry_config(),
            "service_registry_config",
        )?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.service_registry_config_updated))),
            health,
        ))
    }

    fn subscribe_track_transaction(&self, tx_id: TxId) -> Result<impl Stream<Item = Result<Transaction>> + Send> {
        Ok(self.subscribe_track_transaction_with_health(tx_id)?.0)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_track_transaction_with_health(
        &self,
        tx_id: TxId,
    ) -> Result<(impl Stream<Item = Result<Transaction>> + Send, SubscriptionHealthHandle)> {
        let (stream, health) =
            self.build_subscription_stream(GraphQlQueries::subscribe_track_transaction(tx_id), "track_transaction")?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.transaction_updated))),
            health,
        ))
    }

    fn subscribe_ticket_redeemed(
        &self,
        selector: TicketSelector,
    ) -> Result<impl futures::Stream<Item = Result<RedeemTicketDetails>> + Send> {
        Ok(self.subscribe_ticket_redeemed_with_health(selector)?.0)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    fn subscribe_ticket_redeemed_with_health(
        &self,
        selector: TicketSelector,
    ) -> Result<(
        impl futures::Stream<Item = Result<RedeemTicketDetails>> + Send,
        SubscriptionHealthHandle,
    )> {
        let (stream, health) =
            self.build_subscription_stream(GraphQlQueries::subscribe_ticket_redeemed(selector), "ticket_redeemed")?;
        Ok((
            stream.try_filter_map(|item| futures::future::ok(Some(item.ticket_redeemed))),
            health,
        ))
    }

    #[cfg(feature = "curvy")]
    fn subscribe_curvy_pending_notes(
        &self,
        from_block: Option<u64>,
    ) -> Result<impl futures::Stream<Item = Result<CurvyPendingNote>> + Send> {
        Ok(self.subscribe_curvy_pending_notes_with_health(from_block)?.0)
    }

    #[cfg(feature = "curvy")]
    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_curvy_pending_notes_with_health(
        &self,
        from_block: Option<u64>,
    ) -> Result<(
        impl futures::Stream<Item = Result<CurvyPendingNote>> + Send,
        SubscriptionHealthHandle,
    )> {
        let (stream, health) = self.build_subscription_stream(
            GraphQlQueries::subscribe_curvy_pending_notes(from_block),
            "curvy_pending_notes",
        )?;
        Ok((stream.map_ok(|item| item.curvy_pending_note), health))
    }

    #[cfg(feature = "curvy")]
    fn subscribe_curvy_committed_notes(
        &self,
        from_block: Option<u64>,
    ) -> Result<impl futures::Stream<Item = Result<CurvyCommittedNote>> + Send> {
        Ok(self.subscribe_curvy_committed_notes_with_health(from_block)?.0)
    }

    #[cfg(feature = "curvy")]
    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_curvy_committed_notes_with_health(
        &self,
        from_block: Option<u64>,
    ) -> Result<(
        impl futures::Stream<Item = Result<CurvyCommittedNote>> + Send,
        SubscriptionHealthHandle,
    )> {
        let (stream, health) = self.build_subscription_stream(
            GraphQlQueries::subscribe_curvy_committed_notes(from_block),
            "curvy_committed_notes",
        )?;
        Ok((stream.map_ok(|item| item.curvy_committed_note), health))
    }

    #[cfg(feature = "curvy")]
    fn subscribe_curvy_committed_nullifiers(
        &self,
        from_block: Option<u64>,
    ) -> Result<impl futures::Stream<Item = Result<CurvyCommittedNullifier>> + Send> {
        Ok(self.subscribe_curvy_committed_nullifiers_with_health(from_block)?.0)
    }

    #[cfg(feature = "curvy")]
    #[tracing::instrument(level = "debug", skip(self))]
    fn subscribe_curvy_committed_nullifiers_with_health(
        &self,
        from_block: Option<u64>,
    ) -> Result<(
        impl futures::Stream<Item = Result<CurvyCommittedNullifier>> + Send,
        SubscriptionHealthHandle,
    )> {
        let (stream, health) = self.build_subscription_stream(
            GraphQlQueries::subscribe_curvy_committed_nullifiers(from_block),
            "curvy_committed_nullifiers",
        )?;
        Ok((stream.map_ok(|item| item.curvy_committed_nullifier), health))
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

#[cfg(test)]
mod stream_tests {
    use std::{cell::RefCell, io, sync::Once, time::Duration};

    use futures::StreamExt;
    use tokio::{
        net::TcpListener,
        time::{Instant, sleep, timeout},
    };

    use super::super::sse_test_server::{Connection, SseTestServer, Step, event, keepalive};
    use crate::{
        BlokliClient, BlokliClientConfig, ErrorKind, SubscriptionEnd, SubscriptionHealthHandle, SubscriptionProblem,
        SubscriptionStatus,
        api::{BlokliSubscriptionClient, types::ReadinessState},
    };

    const READY: &str = r#"{"data":{"health":"READY"}}"#;
    /// Cut off mid-object, as a truncated frame would be.
    const TRUNCATED: &str = r#"{"data":{"heal"#;

    fn client(server: &SseTestServer, cfg: BlokliClientConfig) -> BlokliClient {
        BlokliClient::new(server.url(), cfg)
    }

    fn fast_restarts() -> BlokliClientConfig {
        BlokliClientConfig {
            stream_reconnect_timeout: Duration::from_millis(400),
            subscription_stream_restart_delay: Some(Duration::from_millis(50)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn keepalive_comments_keep_a_quiet_subscription_open() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![
            keepalive(),
            Step::Wait(Duration::from_millis(100)),
            keepalive(),
            Step::Wait(Duration::from_millis(100)),
            event(READY),
            Step::Hang,
        ])])
        .await?;
        let client = client(&server, fast_restarts());
        let stream = client.subscribe_health()?;
        futures::pin_mut!(stream);

        let item = timeout(Duration::from_secs(5), stream.next()).await?;

        assert!(matches!(item, Some(Ok(ReadinessState::Ready))), "got {item:?}");
        assert_eq!(server.connections(), 1, "keepalives alone must not cause a reconnect");
        Ok(())
    }

    #[tokio::test]
    async fn a_malformed_event_does_not_end_the_subscription() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![event(TRUNCATED), Step::Hang]),
            Connection::ok(vec![event(READY), Step::Hang]),
        ])
        .await?;
        let client = client(&server, fast_restarts());
        let stream = client.subscribe_health()?;
        futures::pin_mut!(stream);

        let item = timeout(Duration::from_secs(5), stream.next()).await?;

        assert!(
            matches!(item, Some(Ok(ReadinessState::Ready))),
            "expected the update sent after the malformed one, got {item:?}"
        );
        assert_eq!(
            server.connections(),
            2,
            "the malformed event should cause one reconnect"
        );
        Ok(())
    }

    #[tokio::test]
    async fn repeated_malformed_events_back_off_instead_of_reconnecting_in_a_tight_loop() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![event(TRUNCATED), Step::Hang])]).await?;
        let client = client(&server, fast_restarts());
        let stream = client.subscribe_health()?;
        futures::pin_mut!(stream);

        // Nothing valid ever arrives, so this only ends by the timeout.
        let item = timeout(Duration::from_millis(1200), stream.next()).await;

        assert!(item.is_err(), "the subscription must keep retrying, got {item:?}");
        // Waits of 50, 100, 200, 400, 400 ms fit about six connections into 1.2 s; a fixed 50 ms would give ~20.
        let connections = server.connections();
        assert!(
            (2..=7).contains(&connections),
            "unexpected number of connections: {connections}"
        );
        Ok(())
    }

    /// Records every status the handle reports until the future is dropped.
    async fn record_statuses(handle: &SubscriptionHealthHandle, seen: &mut Vec<SubscriptionStatus>) {
        loop {
            let status = handle.get().status;
            if seen.last() != Some(&status) {
                seen.push(status);
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn a_quiet_subscription_with_only_keepalives_is_healthy() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![
            keepalive(),
            Step::Wait(Duration::from_millis(100)),
            keepalive(),
            Step::Wait(Duration::from_millis(100)),
            keepalive(),
            Step::Hang,
        ])])
        .await?;
        let client = client(&server, fast_restarts());
        let (stream, health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        let item = timeout(Duration::from_millis(400), stream.next()).await;

        assert!(item.is_err(), "no update was sent, got {item:?}");
        let health = health.get();
        assert_eq!(health.status, SubscriptionStatus::Healthy);
        assert!(health.last_transport_activity.is_some());
        assert_eq!(health.last_update, None, "keepalives are not updates");
        assert_eq!(health.failures, 0);
        assert_eq!(server.connections(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn an_outage_degrades_the_subscription_and_a_reconnect_recovers_it() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![event(READY)]),
            Connection::error(503),
            Connection::error(503),
            Connection::ok(vec![event(READY), Step::Hang]),
        ])
        .await?;
        let client = client(&server, fast_restarts());
        let (stream, health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        let first = timeout(Duration::from_secs(5), stream.next()).await?;
        assert!(matches!(first, Some(Ok(ReadinessState::Ready))), "got {first:?}");
        assert_eq!(health.get().status, SubscriptionStatus::Healthy);
        assert!(health.get().last_update.is_some());

        let mut seen = Vec::new();
        let replayed = tokio::select! {
            item = timeout(Duration::from_secs(5), stream.next()) => item?,
            _ = record_statuses(&health, &mut seen) => unreachable!("recording never ends"),
        };

        assert!(matches!(replayed, Some(Ok(ReadinessState::Ready))), "got {replayed:?}");
        assert!(seen.contains(&SubscriptionStatus::Degraded), "statuses seen: {seen:?}");
        let health = health.get();
        let SubscriptionStatus::Recovered { outage } = health.status else {
            panic!("expected Recovered, got {:?}", health.status);
        };
        assert!(outage > Duration::ZERO);
        assert_eq!(health.failures, 0);
        assert_eq!(health.last_problem, Some(SubscriptionProblem::HttpStatus(503)));
        assert_eq!(server.connections(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn dropping_the_stream_ends_the_subscription_as_cancelled() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![event(READY), Step::Hang])]).await?;
        let client = client(&server, fast_restarts());
        let (stream, health) = client.subscribe_health_with_health()?;
        let mut stream = Box::pin(stream);

        let item = timeout(Duration::from_secs(5), stream.next()).await?;
        assert!(matches!(item, Some(Ok(ReadinessState::Ready))), "got {item:?}");
        drop(stream);

        assert_eq!(
            health.get().status,
            SubscriptionStatus::Ended(SubscriptionEnd::Cancelled)
        );
        Ok(())
    }

    const GRAPHQL_ERROR: &str = r#"{"errors":[{"message":"unknown field"}]}"#;

    #[tokio::test]
    async fn a_graphql_error_is_yielded_once_and_retried_slowly() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![event(GRAPHQL_ERROR), Step::Hang]),
            Connection::ok(vec![event(GRAPHQL_ERROR), Step::Hang]),
            Connection::ok(vec![event(READY), Step::Hang]),
        ])
        .await?;
        let cfg = fast_restarts();
        let slow_retry = cfg.stream_reconnect_timeout;
        let client = client(&server, cfg);
        let (stream, health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);
        let started = Instant::now();

        let first = timeout(Duration::from_secs(5), stream.next()).await?;
        assert!(
            matches!(&first, Some(Err(error)) if matches!(error.kind(), ErrorKind::GraphQLError(_))),
            "got {first:?}"
        );
        assert_eq!(health.get().status, SubscriptionStatus::Rejected);
        assert_eq!(health.get().last_problem, Some(SubscriptionProblem::GraphQl));

        // The second rejection is not yielded again; the next item is the update after it.
        let next = timeout(Duration::from_secs(5), stream.next()).await?;
        assert!(matches!(next, Some(Ok(ReadinessState::Ready))), "got {next:?}");
        assert!(started.elapsed() >= slow_retry * 2, "retried faster than the slow pace");
        assert!(matches!(health.get().status, SubscriptionStatus::Recovered { .. }));
        assert_eq!(server.connections(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn an_http_rejection_is_yielded_once_and_retried_slowly() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::error(401),
            Connection::error(401),
            Connection::ok(vec![event(READY), Step::Hang]),
        ])
        .await?;
        let cfg = fast_restarts();
        let slow_retry = cfg.stream_reconnect_timeout;
        let client = client(&server, cfg);
        let (stream, health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);
        let started = Instant::now();

        let first = timeout(Duration::from_secs(5), stream.next()).await?;
        assert!(
            matches!(&first, Some(Err(error)) if matches!(error.kind(), ErrorKind::Subscription(_))),
            "got {first:?}"
        );
        assert_eq!(health.get().status, SubscriptionStatus::Rejected);
        assert_eq!(health.get().last_problem, Some(SubscriptionProblem::HttpStatus(401)));

        let next = timeout(Duration::from_secs(5), stream.next()).await?;
        assert!(matches!(next, Some(Ok(ReadinessState::Ready))), "got {next:?}");
        assert!(started.elapsed() >= slow_retry * 2, "retried faster than the slow pace");
        assert!(matches!(health.get().status, SubscriptionStatus::Recovered { .. }));
        assert_eq!(server.connections(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn a_lasting_rejection_is_retried_at_the_slow_pace_without_ending() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::error(403)]).await?;
        let client = client(&server, fast_restarts());
        let (stream, health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        let mut items = Vec::new();
        let _ = timeout(Duration::from_millis(1300), async {
            while let Some(item) = stream.next().await {
                items.push(item);
            }
        })
        .await;

        assert_eq!(items.len(), 1, "the rejection is yielded once, got {items:?}");
        assert!(items[0].is_err());
        assert_eq!(health.get().status, SubscriptionStatus::Rejected);
        // One try at the start, then one every 400 ms.
        let connections = server.connections();
        assert!(
            (2..=4).contains(&connections),
            "unexpected number of connections: {connections}"
        );
        assert!(health.get().failures >= 2);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_subscriptions_report_their_own_health() -> anyhow::Result<()> {
        let working = SseTestServer::start(vec![Connection::ok(vec![event(READY), Step::Hang])]).await?;
        let failing = SseTestServer::start(vec![Connection::error(503)]).await?;
        let working_client = client(&working, fast_restarts());
        let failing_client = client(&failing, fast_restarts());
        let (working_stream, working_health) = working_client.subscribe_health_with_health()?;
        let (failing_stream, failing_health) = failing_client.subscribe_health_with_health()?;
        futures::pin_mut!(working_stream, failing_stream);

        // Poll both at the same time, as a caller merging several subscriptions would.
        let polled = timeout(
            Duration::from_millis(600),
            futures::future::join(working_stream.next(), failing_stream.next()),
        )
        .await;
        assert!(polled.is_err(), "the failing subscription must keep retrying");

        assert_ne!(working_health.subscription(), failing_health.subscription());
        assert!(working_health.get().is_healthy(), "{:?}", working_health.get());
        let failing = failing_health.get();
        assert_eq!(failing.status, SubscriptionStatus::Degraded);
        assert!(failing.failures >= 1);
        assert_eq!(failing.last_problem, Some(SubscriptionProblem::HttpStatus(503)));
        Ok(())
    }

    fn with_stall_threshold(threshold: Option<Duration>) -> BlokliClientConfig {
        BlokliClientConfig {
            subscription_stall_threshold: threshold,
            ..fast_restarts()
        }
    }

    #[tokio::test]
    async fn a_silent_connection_is_reported_as_stalled_without_an_error() -> anyhow::Result<()> {
        // Answers with the response head, then sends nothing: no events, no keepalives, no error.
        let server = SseTestServer::start(vec![Connection::ok(vec![Step::Hang])]).await?;
        let client = client(&server, with_stall_threshold(Some(Duration::from_millis(300))));
        let (stream, health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        let item = timeout(Duration::from_millis(700), stream.next()).await;

        assert!(item.is_err(), "nothing was sent, got {item:?}");
        let health = health.get();
        assert_eq!(health.status, SubscriptionStatus::Stalled);
        assert_eq!(health.failures, 0, "no error arrived");
        assert_eq!(health.last_problem, None);
        assert_eq!(server.connections(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn keepalives_keep_a_quiet_subscription_from_stalling() -> anyhow::Result<()> {
        let mut steps = Vec::new();
        for _ in 0..10 {
            steps.push(keepalive());
            steps.push(Step::Wait(Duration::from_millis(100)));
        }
        steps.push(Step::Hang);
        let server = SseTestServer::start(vec![Connection::ok(steps)]).await?;
        let client = client(&server, with_stall_threshold(Some(Duration::from_millis(300))));
        let (stream, health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        let item = timeout(Duration::from_millis(800), stream.next()).await;

        assert!(item.is_err(), "no update was sent, got {item:?}");
        assert_eq!(health.get().status, SubscriptionStatus::Healthy);
        assert_eq!(health.get().last_update, None);
        Ok(())
    }

    #[tokio::test]
    async fn a_stalled_subscription_recovers_after_reconnecting() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![Step::Hang]),
            Connection::ok(vec![event(READY), Step::Hang]),
        ])
        .await?;
        let threshold = Duration::from_millis(200);
        let client = client(
            &server,
            BlokliClientConfig {
                // Cuts the silent connection after the stall was reported, so the client reconnects.
                subscription_read_timeout: Some(Duration::from_millis(500)),
                ..with_stall_threshold(Some(threshold))
            },
        );
        let (stream, health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        let mut seen = Vec::new();
        let item = tokio::select! {
            item = timeout(Duration::from_secs(5), stream.next()) => item?,
            _ = record_statuses(&health, &mut seen) => unreachable!("recording never ends"),
        };

        assert!(matches!(item, Some(Ok(ReadinessState::Ready))), "got {item:?}");
        assert!(seen.contains(&SubscriptionStatus::Stalled), "statuses seen: {seen:?}");
        let health = health.get();
        let SubscriptionStatus::Recovered { outage } = health.status else {
            panic!("expected Recovered, got {:?}", health.status);
        };
        assert!(
            outage >= threshold,
            "outage {outage:?} is shorter than the stall threshold"
        );
        assert_eq!(health.last_problem, Some(SubscriptionProblem::Timeout));
        assert_eq!(server.connections(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn stall_detection_can_be_turned_off() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![Step::Hang])]).await?;
        let client = client(&server, with_stall_threshold(None));
        let (stream, health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        let item = timeout(Duration::from_millis(500), stream.next()).await;

        assert!(item.is_err(), "nothing was sent, got {item:?}");
        assert_eq!(health.get().status, SubscriptionStatus::Healthy);
        Ok(())
    }

    thread_local! {
        /// Log lines of the test running on this thread, while it captures them.
        static CAPTURED: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
    }

    /// Writes log output into the capture of the current thread, if there is one.
    struct ThreadCapture;

    impl io::Write for ThreadCapture {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            CAPTURED.with_borrow_mut(|captured| {
                if let Some(captured) = captured {
                    captured.extend_from_slice(buf);
                }
            });
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Log output of the client and of eventsource-client, collected for assertions.
    ///
    /// One process-wide subscriber writes into a per-thread buffer. A `#[tokio::test]` runs the client on its own
    /// thread, so each test sees only its own lines. Thread-local subscribers would be simpler, but tracing caches per
    /// call site whether anyone listens, and parallel tests adding and dropping them make that cache flaky.
    struct CapturedLogs;

    impl CapturedLogs {
        fn start() -> Self {
            static INSTALLED: Once = Once::new();
            INSTALLED.call_once(|| {
                // `try_init` also forwards the `log` records of eventsource-client. If something else installed a
                // subscriber first, nothing is captured and the assertions say so.
                let _ = tracing_subscriber::fmt()
                    .with_env_filter("blokli_client=trace,eventsource_client=trace")
                    .with_ansi(false)
                    .with_writer(|| ThreadCapture)
                    .try_init();
            });
            CAPTURED.with_borrow_mut(|captured| *captured = Some(Vec::new()));
            Self
        }

        fn lines(&self) -> Vec<String> {
            CAPTURED.with_borrow(|captured| {
                String::from_utf8_lossy(captured.as_deref().unwrap_or_default())
                    .lines()
                    .map(str::to_owned)
                    .collect()
            })
        }

        /// Number of this crate's lines at `level` that contain `message`.
        fn count(&self, level: &str, message: &str) -> usize {
            self.ours()
                .iter()
                .filter(|line| line.contains(&format!(" {level} ")) && line.contains(message))
                .count()
        }

        /// Lines written by this crate, leaving out eventsource-client's own.
        fn ours(&self) -> Vec<String> {
            self.lines()
                .into_iter()
                .filter(|line| line.contains("blokli_client"))
                .collect()
        }
    }

    impl Drop for CapturedLogs {
        fn drop(&mut self) {
            CAPTURED.with_borrow_mut(|captured| *captured = None);
        }
    }

    #[tokio::test]
    async fn an_outage_logs_one_warning_and_one_recovery_without_payloads() -> anyhow::Result<()> {
        let logs = CapturedLogs::start();
        let server = SseTestServer::start(vec![
            Connection::ok(vec![event(READY)]),
            Connection::error(503),
            Connection::error(503),
            Connection::ok(vec![event(READY), Step::Hang]),
        ])
        .await?;
        let client = client(&server, fast_restarts());
        let (stream, health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        for _ in 0..2 {
            let item = timeout(Duration::from_secs(5), stream.next()).await?;
            assert!(matches!(item, Some(Ok(ReadinessState::Ready))), "got {item:?}");
        }

        let subscription = health.subscription().to_string();
        assert_eq!(logs.count("WARN", "subscription interrupted"), 1, "{:#?}", logs.lines());
        assert_eq!(logs.count("WARN", ""), 1, "only the start of the outage is a warning");
        assert!(
            logs.count("DEBUG", "subscription still failing") >= 2,
            "{:#?}",
            logs.lines()
        );
        assert_eq!(logs.count("INFO", "subscription recovered"), 1, "{:#?}", logs.lines());
        let lines = logs.ours();
        assert!(
            lines
                .iter()
                .filter(|line| line.contains(" WARN ") || line.contains(" INFO "))
                .all(|line| line.contains(&subscription)),
            "every warning and recovery names the subscription: {lines:#?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("subscription recovered") && line.contains("outage=")),
            "the recovery states the outage length"
        );
        assert!(
            !lines
                .iter()
                .any(|line| line.contains("READY") || line.contains("SubscribeHealth")),
            "no payload or query in the logs: {lines:#?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_stall_is_logged_once() -> anyhow::Result<()> {
        let logs = CapturedLogs::start();
        let server = SseTestServer::start(vec![Connection::ok(vec![Step::Hang])]).await?;
        let client = client(&server, with_stall_threshold(Some(Duration::from_millis(200))));
        let (stream, _health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        let _ = timeout(Duration::from_millis(900), stream.next()).await;

        assert_eq!(logs.count("WARN", "subscription stalled"), 1, "{:#?}", logs.lines());
        assert_eq!(logs.count("WARN", ""), 1, "{:#?}", logs.lines());
        Ok(())
    }

    #[tokio::test]
    async fn a_lasting_rejection_is_logged_as_a_warning_once() -> anyhow::Result<()> {
        let logs = CapturedLogs::start();
        let server = SseTestServer::start(vec![Connection::error(403)]).await?;
        let client = client(&server, fast_restarts());
        let (stream, _health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        let _ = timeout(Duration::from_millis(1300), async {
            while stream.next().await.is_some() {}
        })
        .await;

        assert_eq!(
            logs.count("WARN", "Blokli rejected the subscription"),
            1,
            "{:#?}",
            logs.lines()
        );
        assert_eq!(logs.count("WARN", ""), 1, "{:#?}", logs.lines());
        assert!(
            logs.count("DEBUG", "Blokli still rejects the subscription") >= 1,
            "{:#?}",
            logs.lines()
        );
        Ok(())
    }

    #[tokio::test]
    async fn transport_errors_are_logged_without_the_url() -> anyhow::Result<()> {
        let logs = CapturedLogs::start();
        // A port nothing listens on, so every connection attempt fails.
        let closed_port = TcpListener::bind("127.0.0.1:0").await?.local_addr()?.port();
        let client = BlokliClient::new(
            // reqwest names the request URL in its errors; the path stands in for anything sensitive in it.
            format!("http://127.0.0.1:{closed_port}/secret-token/").parse()?,
            fast_restarts(),
        );
        let (stream, _health) = client.subscribe_health_with_health()?;
        futures::pin_mut!(stream);

        let _ = timeout(Duration::from_millis(500), stream.next()).await;

        let lines = logs.lines();
        assert!(
            lines.iter().any(|line| line.contains("request returned an error")),
            "eventsource-client should have logged the failed connections: {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains("secret-token")),
            "the URL leaked into the logs: {lines:#?}"
        );
        Ok(())
    }
}
