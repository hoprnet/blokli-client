//! Single SSE connection shared by all subscriptions of one client.
//!
//! Blokli serves one GraphQL operation per SSE request, so combining subscriptions means combining them into one
//! GraphQL document. Every registered subscription becomes an aliased root field of a single subscription operation;
//! the hub renders that document, drives the connection, and routes each incoming payload back to the subscription
//! that owns the alias.
//!
//! A subscription operation with several root fields is outside the GraphQL specification, which requires exactly one
//! root field per subscription. Blokli serves it anyway: async-graphql does not enforce that rule, merges the streams
//! of all root fields, and emits one response per event that is keyed by the response key, which is the alias used
//! here.
//!
//! Health stays per subscription even though the transport is shared. The hub feeds connection-level events, such as
//! a reconnect or a malformed frame, into every registered subscription's tracker, and data events only into the
//! subscription that owns the alias. A problem with the connection therefore shows up on all subscriptions that use
//! it, which is what a caller would see with one connection per subscription too.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError, Weak},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use cynic::{GraphQlError, GraphQlErrorPathSegment, GraphQlResponse, StreamingOperation};
use eventsource_client::{Client, ReconnectOptionsBuilder, SSE};
use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, watch};

use super::{
    BlokliClientConfig, DEFAULT_MALFORMED_EVENT_DELAY, MIN_RECONNECTION_DELAY, REDIRECT_LIMIT, ReqwestTransport,
    SCHEMA_VERSION, derive_graphql_url, describe, is_rejection, problem_of, rejected_retry_delay, response_to_data,
};
use crate::{
    errors::{BlokliClientError, ErrorKind},
    health::{HealthTracker, SubscriptionEnd, SubscriptionProblem},
};

/// Debounce applied before opening a connection so subscriptions registered in a burst share one document.
const SLOT_CHANGE_DEBOUNCE: Duration = Duration::from_millis(25);

/// Per-subscription queue depth. A slot that falls this far behind loses events instead of stalling the connection.
const SLOT_CHANNEL_CAPACITY: usize = 1024;

/// Number of deregistered aliases tolerated in a live document before the connection is rebuilt.
const MAX_STALE_ALIASES: usize = 32;

/// Operation name of the combined subscription document.
const COMBINED_OPERATION_NAME: &str = "BlokliSubscriptions";

/// Identifier of a registered subscription slot.
pub(crate) type SlotId = u64;

/// One subscription rendered into a single-field GraphQL document.
#[derive(Clone, Debug)]
pub(crate) struct SubscriptionSpec {
    document: String,
    variables: serde_json::Value,
}

impl SubscriptionSpec {
    /// Renders a cynic subscription operation into the document and variable map used to assemble a connection.
    pub(crate) fn new<Q, V>(operation: StreamingOperation<Q, V>) -> std::result::Result<Self, BlokliClientError>
    where
        V: cynic::serde::Serialize,
    {
        let mut operation = serde_json::to_value(&operation).map_err(ErrorKind::from)?;
        let operation = operation.as_object_mut().ok_or(ErrorKind::ParseError)?;
        let document = operation
            .remove("query")
            .and_then(|query| query.as_str().map(str::to_owned))
            .ok_or(ErrorKind::ParseError)?;
        let variables = operation
            .remove("variables")
            .filter(|variables| !variables.is_null())
            .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));

        Ok(Self { document, variables })
    }
}

/// One item delivered to a registered subscription.
#[derive(Debug)]
pub(crate) enum SlotEvent {
    /// Payload of the slot's root field, keyed by the field name as a single-field response would be.
    Data(serde_json::Value),
    /// Error that belongs to the slot.
    Error(BlokliClientError),
}

/// Shared state of one client's subscriptions.
#[derive(Clone)]
pub(crate) struct SubscriptionHub {
    shared: Arc<HubShared>,
}

impl fmt::Debug for SubscriptionHub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Do not wait for the state lock while formatting so that logging can never block a connection.
        let slots = self.shared.state.try_lock().map(|state| state.slots.len());

        f.debug_struct("SubscriptionHub").field("slots", &slots).finish()
    }
}

impl SubscriptionHub {
    /// Creates an idle hub. No connection is opened until a subscription is polled.
    pub(crate) fn new(base_url: url::Url, cfg: BlokliClientConfig) -> Self {
        let (generation, _) = watch::channel(0);

        Self {
            shared: Arc::new(HubShared {
                base_url,
                cfg,
                state: Mutex::new(HubState::default()),
                generation,
            }),
        }
    }

    /// Returns the stream that yields the payloads of one subscription.
    pub(crate) fn subscribe(
        &self,
        spec: SubscriptionSpec,
        health: HealthTracker,
        reqwest_client: reqwest::Client,
    ) -> SubscriptionSlot {
        SubscriptionSlot {
            hub: self.clone(),
            pending: Some(PendingSubscription {
                spec,
                health,
                reqwest_client,
            }),
            receiver: None,
            guard: None,
            finished: false,
        }
    }

    fn register(
        &self,
        pending: PendingSubscription,
    ) -> std::result::Result<(mpsc::Receiver<SlotEvent>, SlotGuard), BlokliClientError> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| ErrorKind::InvalidInput("subscriptions require a Tokio runtime"))?;

        let (id, receiver) = {
            let mut state = self.shared.lock_state();
            let id = state.next_id;
            state.next_id += 1;
            let (tx, rx) = mpsc::channel(SLOT_CHANNEL_CAPACITY);
            state.reqwest_client = Some(pending.reqwest_client);
            state.slots.insert(
                id,
                SlotEntry {
                    spec: pending.spec,
                    health: pending.health,
                    tx,
                },
            );
            (id, rx)
        };

        self.shared.publish_generation();
        self.shared.ensure_supervisor();

        Ok((receiver, SlotGuard { hub: self.clone(), id }))
    }

    fn deregister(&self, id: SlotId) {
        // Dropping the entry drops its tracker, which marks the subscription as cancelled.
        self.shared.lock_state().slots.remove(&id);
        self.shared.publish_generation();
    }
}

/// Stream of the payloads of one registered subscription.
///
/// The subscription is registered with the hub when the stream is first polled and released when the stream ends or
/// is dropped.
pub(crate) struct SubscriptionSlot {
    hub: SubscriptionHub,
    pending: Option<PendingSubscription>,
    receiver: Option<mpsc::Receiver<SlotEvent>>,
    guard: Option<SlotGuard>,
    finished: bool,
}

struct PendingSubscription {
    spec: SubscriptionSpec,
    health: HealthTracker,
    reqwest_client: reqwest::Client,
}

impl Stream for SubscriptionSlot {
    type Item = SlotEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.finished {
            return Poll::Ready(None);
        }

        if this.receiver.is_none() {
            let Some(pending) = this.pending.take() else {
                this.finish();
                return Poll::Ready(None);
            };

            match this.hub.register(pending) {
                Ok((receiver, guard)) => {
                    this.receiver = Some(receiver);
                    this.guard = Some(guard);
                }
                Err(error) => {
                    this.finish();
                    return Poll::Ready(Some(SlotEvent::Error(error)));
                }
            }
        }

        match this.receiver.as_mut().map(|receiver| receiver.poll_recv(cx)) {
            Some(Poll::Ready(Some(event))) => Poll::Ready(Some(event)),
            Some(Poll::Ready(None)) => {
                this.finish();
                Poll::Ready(None)
            }
            Some(Poll::Pending) => Poll::Pending,
            None => {
                this.finish();
                Poll::Ready(None)
            }
        }
    }
}

impl SubscriptionSlot {
    fn finish(&mut self) {
        self.finished = true;
        self.receiver = None;
        self.guard = None;
    }
}

/// Releases a subscription from the hub when its stream is dropped.
struct SlotGuard {
    hub: SubscriptionHub,
    id: SlotId,
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.hub.deregister(self.id);
    }
}

struct HubShared {
    base_url: url::Url,
    cfg: BlokliClientConfig,
    state: Mutex<HubState>,
    generation: watch::Sender<u64>,
}

#[derive(Default)]
struct HubState {
    next_id: SlotId,
    slots: BTreeMap<SlotId, SlotEntry>,
    generation: u64,
    supervisor: Option<Weak<()>>,
    reqwest_client: Option<reqwest::Client>,
}

struct SlotEntry {
    spec: SubscriptionSpec,
    health: HealthTracker,
    tx: mpsc::Sender<SlotEvent>,
}

/// What one connection needs to know about a registered subscription.
#[derive(Clone)]
struct SlotSnapshot {
    spec: SubscriptionSpec,
    tx: mpsc::Sender<SlotEvent>,
}

/// One assembled SSE request and the slots that own its aliases.
struct Connection {
    body: String,
    targets: BTreeMap<String, SlotTarget>,
}

#[derive(Clone)]
struct SlotTarget {
    id: SlotId,
    field: String,
    tx: mpsc::Sender<SlotEvent>,
}

/// How the supervisor continues after a connection stopped driving subscriptions.
enum Outcome {
    /// The registered slot set changed and needs a new document.
    Rebuild,
    /// The connection should be reopened after the given delay.
    Reconnect(Duration),
    /// The subscriptions of this client are over.
    Stop(SubscriptionEnd),
}

/// What one SSE event asked the connection to do.
enum Dispatch {
    /// The event was routed, or carried nothing that concerns a subscription.
    Handled,
    /// The payload could not be decoded; reconnect after the backoff.
    Malformed(Duration),
    /// Blokli refused the document; retry at the slow pace.
    Rejected,
}

impl HubShared {
    fn lock_state(&self) -> MutexGuard<'_, HubState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn publish_generation(&self) {
        let generation = {
            let mut state = self.lock_state();
            state.generation = state.generation.wrapping_add(1);
            state.generation
        };

        // A missing receiver only means no connection is running right now.
        let _ = self.generation.send(generation);
    }

    /// Starts the connection supervisor when subscriptions are registered and none is running.
    fn ensure_supervisor(self: &Arc<Self>) {
        let token = Arc::new(());

        {
            let mut state = self.lock_state();
            if state.slots.is_empty() || state.supervisor.as_ref().and_then(Weak::upgrade).is_some() {
                return;
            }
            state.supervisor = Some(Arc::downgrade(&token));
        }

        let shared = Arc::clone(self);
        let generation = self.generation.subscribe();
        tokio::spawn(async move {
            run_supervisor(&shared, generation).await;
            if shared.retire(&token) {
                shared.ensure_supervisor();
            }
        });
    }

    /// Clears the supervisor registration if `token` still belongs to the supervisor that is exiting.
    fn retire(&self, token: &Arc<()>) -> bool {
        let mut state = self.lock_state();
        let is_current = state
            .supervisor
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, token));

        if is_current {
            state.supervisor = None;
        }

        is_current
    }

    fn snapshot(&self) -> BTreeMap<SlotId, SlotSnapshot> {
        let state = self.lock_state();
        state
            .slots
            .iter()
            .map(|(id, entry)| {
                (
                    *id,
                    SlotSnapshot {
                        spec: entry.spec.clone(),
                        tx: entry.tx.clone(),
                    },
                )
            })
            .collect()
    }

    /// Ends the listed subscriptions and releases their streams.
    fn end_slots(&self, ids: &[SlotId], end: SubscriptionEnd) {
        let mut state = self.lock_state();
        for id in ids {
            if let Some(mut entry) = state.slots.remove(id) {
                entry.health.end(end);
            }
        }
    }

    /// Ends every registered subscription and releases its stream.
    fn end_all(&self, end: SubscriptionEnd) {
        let mut state = self.lock_state();
        for entry in state.slots.values_mut() {
            entry.health.end(end);
        }
        state.slots.clear();
    }

    fn with_health(&self, ids: &[SlotId], mut update: impl FnMut(&mut HealthTracker)) {
        let mut state = self.lock_state();
        for id in ids {
            if let Some(entry) = state.slots.get_mut(id) {
                update(&mut entry.health);
            }
        }
    }

    /// Marks the subscriptions the server refused as rejected and returns the ones for which this is a new refusal,
    /// so the error is yielded only once per episode.
    fn reject(
        &self,
        ids: &[SlotId],
        problem: SubscriptionProblem,
        error: &dyn std::fmt::Display,
        retry_in: Duration,
    ) -> Vec<SlotId> {
        let mut state = self.lock_state();
        let mut first = Vec::new();

        for id in ids {
            if let Some(entry) = state.slots.get_mut(id)
                && entry.health.reject(problem, error, retry_in)
            {
                first.push(*id);
            }
        }

        first
    }

    /// The earliest moment at which a subscription would count as stalled.
    fn stall_wakeup(&self) -> Option<Instant> {
        let threshold = self.cfg.subscription_stall_threshold?;
        let state = self.lock_state();

        state
            .slots
            .values()
            .filter_map(|entry| entry.health.stall_deadline(threshold))
            .min()
    }

    /// Reports the subscriptions whose stall deadline has passed.
    fn mark_stalled(&self) {
        let Some(threshold) = self.cfg.subscription_stall_threshold else {
            return;
        };
        let now = Instant::now();
        let mut state = self.lock_state();

        for entry in state.slots.values_mut() {
            if entry
                .health
                .stall_deadline(threshold)
                .is_some_and(|deadline| deadline <= now)
            {
                entry.health.stalled();
            }
        }
    }

    /// Waits until the registered slot set is stable, then assembles the document to connect with.
    async fn prepare(&self, generation: &mut watch::Receiver<u64>) -> Option<Connection> {
        loop {
            let slots = self.snapshot();
            if slots.is_empty() {
                return None;
            }

            match tokio::time::timeout(SLOT_CHANGE_DEBOUNCE, generation.changed()).await {
                Ok(Ok(())) => continue,
                Ok(Err(_)) => return None,
                Err(_) => {}
            }

            return match build_connection(&slots) {
                Ok(connection) => Some(connection),
                Err(error) => {
                    tracing::error!(%error, "failed to build the combined subscription request");
                    for (id, entry) in &slots {
                        send(*id, &entry.tx, SlotEvent::Error(error.clone()));
                    }
                    self.end_slots(&slots.keys().copied().collect::<Vec<_>>(), SubscriptionEnd::Failed);
                    None
                }
            };
        }
    }

    /// Drives one SSE connection until it ends or the registered slot set needs a different document.
    async fn drive(
        &self,
        generation: &mut watch::Receiver<u64>,
        connection: Connection,
        malformed_event_delay: &mut Option<Duration>,
    ) -> Outcome {
        let Connection { body, targets } = connection;
        let ids: Vec<SlotId> = targets.values().map(|target| target.id).collect();

        let mut stream = match self.open_stream(body) {
            Ok(stream) => stream,
            Err(error) => {
                tracing::error!(%error, "failed to open the SSE connection");
                for target in targets.values() {
                    send(target.id, &target.tx, SlotEvent::Error(error.clone()));
                }
                return Outcome::Stop(SubscriptionEnd::Failed);
            }
        };

        loop {
            let stall = self.stall_wakeup();
            let stalled = async {
                match stall {
                    Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
                    None => std::future::pending::<()>().await,
                }
            };

            tokio::select! {
                biased;

                changed = generation.changed() => {
                    if changed.is_err() || self.reconnect_needed(&targets) {
                        return Outcome::Rebuild;
                    }
                }

                _ = stalled => self.mark_stalled(),

                event = stream.next() => match event {
                    Some(Ok(SSE::Event(event))) => {
                        tracing::trace!(
                            event_type = %event.event_type,
                            bytes = event.data.len(),
                            "SSE event"
                        );
                        match self.dispatch(&targets, &event.data, malformed_event_delay) {
                            Dispatch::Handled => {}
                            Dispatch::Malformed(delay) => return Outcome::Reconnect(delay),
                            Dispatch::Rejected => return Outcome::Reconnect(rejected_retry_delay(&self.cfg)),
                        }
                    }
                    Some(Ok(SSE::Comment(_))) => {
                        tracing::trace!("SSE keepalive");
                        self.with_health(&ids, |health| health.transport_activity());
                    }
                    Some(Ok(SSE::Connected(details))) => {
                        tracing::debug!(status = details.response().status(), "SSE connection established");
                        self.with_health(&ids, |health| health.connected());
                    }
                    Some(Err(error)) => {
                        let problem = problem_of(&error);
                        if let SubscriptionProblem::HttpStatus(status) = problem
                            && is_rejection(status)
                        {
                            // Retrying a refusal at the normal reconnect pace only adds load; retry slowly
                            // instead, so the subscription still comes back without a restart once the server
                            // accepts it again.
                            let first = self.reject(
                                &ids,
                                problem,
                                &describe(&error),
                                rejected_retry_delay(&self.cfg),
                            );
                            if !first.is_empty() {
                                let error = BlokliClientError::from(ErrorKind::Subscription(Box::new(error)));
                                for target in targets.values() {
                                    if first.contains(&target.id) {
                                        send(target.id, &target.tx, SlotEvent::Error(error.clone()));
                                    }
                                }
                            }
                            return Outcome::Reconnect(rejected_retry_delay(&self.cfg));
                        }

                        self.with_health(&ids, |health| health.problem(problem, &describe(&error)));
                    }
                    None => {
                        return match self.cfg.subscription_stream_restart_delay {
                            Some(delay) => Outcome::Reconnect(delay.max(MIN_RECONNECTION_DELAY)),
                            None => Outcome::Stop(SubscriptionEnd::Completed),
                        };
                    }
                },
            }
        }
    }

    /// Decides whether the live document still covers the registered subscriptions.
    fn reconnect_needed(&self, targets: &BTreeMap<String, SlotTarget>) -> bool {
        let live: BTreeSet<SlotId> = self.lock_state().slots.keys().copied().collect();
        if live.is_empty() {
            return true;
        }

        let connected: BTreeSet<SlotId> = targets.values().map(|target| target.id).collect();
        if !live.is_subset(&connected) {
            return true;
        }

        // Deregistered aliases stay subscribed until the next rebuild, which keeps long-lived subscriptions from
        // replaying their initial state whenever a sibling subscription comes and goes.
        connected.difference(&live).count() > MAX_STALE_ALIASES
    }

    /// Routes one SSE payload to the subscriptions that own the aliases it contains.
    fn dispatch(
        &self,
        targets: &BTreeMap<String, SlotTarget>,
        data: &str,
        malformed_event_delay: &mut Option<Duration>,
    ) -> Dispatch {
        let ids: Vec<SlotId> = targets.values().map(|target| target.id).collect();

        let response = match serde_json::from_str::<GraphQlResponse<serde_json::Value>>(data) {
            Ok(response) => response,
            Err(error) => {
                let error = BlokliClientError::from(ErrorKind::from(error));
                tracing::debug!(bytes = data.len(), "reconnecting after a malformed SSE event");
                self.with_health(&ids, |health| {
                    health.problem(SubscriptionProblem::MalformedEvent, &error)
                });

                return Dispatch::Malformed(next_malformed_event_delay(&self.cfg, malformed_event_delay));
            }
        };

        let Routed {
            payloads,
            slot_errors,
            document_errors,
        } = route(response, targets);
        let by_id: BTreeMap<SlotId, &SlotTarget> = targets.values().map(|target| (target.id, target)).collect();

        let payloads_were_routed = !payloads.is_empty();
        if payloads_were_routed {
            *malformed_event_delay = None;
        }

        for (id, payload) in payloads {
            self.with_health(&[id], |health| health.data_update());
            if let Some(target) = by_id.get(&id) {
                send(target.id, &target.tx, SlotEvent::Data(payload));
            }
        }

        for (id, errors) in slot_errors {
            let error = errors_to_client_error(errors);
            let first = self.reject(
                &[id],
                SubscriptionProblem::GraphQl,
                &error,
                rejected_retry_delay(&self.cfg),
            );
            if !first.is_empty()
                && let Some(target) = by_id.get(&id)
            {
                send(target.id, &target.tx, SlotEvent::Error(error));
            }
        }

        if document_errors.is_empty() {
            return Dispatch::Handled;
        }

        if payloads_were_routed {
            // Data wins over errors, as it does for one-shot operations.
            tracing::error!(
                ?document_errors,
                "subscription payload received with document-level errors"
            );
            return Dispatch::Handled;
        }

        let error = errors_to_client_error(document_errors);
        let first = self.reject(
            &ids,
            SubscriptionProblem::GraphQl,
            &error,
            rejected_retry_delay(&self.cfg),
        );

        for id in first {
            if let Some(target) = by_id.get(&id) {
                send(target.id, &target.tx, SlotEvent::Error(error.clone()));
            }
        }

        // Asking again right away gets the same answer, so retry at the slow pace even when the rejection is not
        // new and was therefore not yielded again.
        Dispatch::Rejected
    }

    fn open_stream(
        &self,
        body: String,
    ) -> std::result::Result<eventsource_client::BoxStream<eventsource_client::Result<SSE>>, BlokliClientError> {
        let graphql_url = derive_graphql_url(&self.base_url)?;
        let reqwest_client = self
            .lock_state()
            .reqwest_client
            .clone()
            .ok_or(ErrorKind::InvalidInput("subscription client is not initialised"))?;

        let sse_err = |error| ErrorKind::Subscription(Box::new(error));
        let initial_reconnect_delay = self
            .cfg
            .stream_reconnect_timeout
            .min(Duration::from_secs(2))
            .max(MIN_RECONNECTION_DELAY);

        let client = eventsource_client::ClientBuilder::for_url(graphql_url.as_str())
            .map_err(sse_err)?
            .header("Accept", "text/event-stream")
            .map_err(sse_err)?
            .header("Content-Type", "application/json")
            .map_err(sse_err)?
            .header("X-Blokli-Schema-Version", &SCHEMA_VERSION.to_string())
            .map_err(sse_err)?
            .method("POST".into())
            .body(body)
            .redirect_limit(REDIRECT_LIMIT as u32)
            .reconnect(
                ReconnectOptionsBuilder::new(true)
                    .retry_initial(true)
                    .delay(initial_reconnect_delay)
                    .backoff_factor(2)
                    .delay_max(self.cfg.stream_reconnect_timeout)
                    .build(),
            )
            .build_with_transport(ReqwestTransport::new(reqwest_client));

        Ok(client.stream())
    }
}

async fn run_supervisor(shared: &Arc<HubShared>, mut generation: watch::Receiver<u64>) {
    let mut malformed_event_delay = None;

    loop {
        let Some(connection) = shared.prepare(&mut generation).await else {
            return;
        };

        match shared
            .drive(&mut generation, connection, &mut malformed_event_delay)
            .await
        {
            Outcome::Rebuild => continue,
            Outcome::Reconnect(delay) => tokio::time::sleep(delay).await,
            Outcome::Stop(end) => {
                shared.end_all(end);
                return;
            }
        }
    }
}

/// The wait before reconnecting after a malformed event: the restart delay at first, then doubling up to
/// [`stream_reconnect_timeout`](BlokliClientConfig::stream_reconnect_timeout) while the events stay malformed.
fn next_malformed_event_delay(cfg: &BlokliClientConfig, state: &mut Option<Duration>) -> Duration {
    let first = cfg
        .subscription_stream_restart_delay
        .unwrap_or(DEFAULT_MALFORMED_EVENT_DELAY)
        .max(MIN_RECONNECTION_DELAY);
    let delay = state.map_or(first, |last| {
        last.saturating_mul(2).min(cfg.stream_reconnect_timeout.max(first))
    });
    *state = Some(delay);

    delay
}

/// Assembles the combined subscription document and the alias map of one connection.
fn build_connection(slots: &BTreeMap<SlotId, SlotSnapshot>) -> std::result::Result<Connection, BlokliClientError> {
    let mut definitions = Vec::with_capacity(slots.len());
    let mut fields = Vec::with_capacity(slots.len());
    let mut variables = serde_json::Map::new();
    let mut targets = BTreeMap::new();

    for (id, entry) in slots {
        let alias = format!("s{id}");
        let (definitions_of_slot, field_of_slot) = split_document(&entry.spec.document)?;

        if !definitions_of_slot.is_empty() {
            definitions.push(rename_variables(definitions_of_slot, &alias));
        }

        let field_of_slot = rename_variables(field_of_slot, &alias);
        let field_name = root_field_name(&field_of_slot)?;
        fields.push(alias_root_field(&field_of_slot, &alias)?);

        for (name, value) in entry.spec.variables.as_object().into_iter().flatten() {
            variables.insert(format!("{alias}_{name}"), value.clone());
        }

        targets.insert(
            alias,
            SlotTarget {
                id: *id,
                field: field_name,
                tx: entry.tx.clone(),
            },
        );
    }

    let document = if definitions.is_empty() {
        format!("subscription {COMBINED_OPERATION_NAME} {{{}}}", fields.concat())
    } else {
        format!(
            "subscription {COMBINED_OPERATION_NAME}({}) {{{}}}",
            definitions.join(", "),
            fields.concat()
        )
    };
    let body = serde_json::json!({
        "query": document,
        "variables": serde_json::Value::Object(variables),
    })
    .to_string();

    Ok(Connection { body, targets })
}

/// Splits a single-field subscription document into its variable definitions and its root field.
fn split_document(document: &str) -> std::result::Result<(&str, &str), BlokliClientError> {
    let document = document.trim_start();
    let after_keyword = document
        .strip_prefix("subscription")
        .ok_or(ErrorKind::ParseError)?
        .trim_start();

    let start = after_keyword.find(['(', '{']).ok_or(ErrorKind::ParseError)?;
    let after_name = &after_keyword[start..];

    let (definitions, selection) = if after_name.starts_with('(') {
        let close = matching_paren(after_name).ok_or(ErrorKind::ParseError)?;
        (&after_name[1..close], &after_name[close + 1..])
    } else {
        ("", after_name)
    };

    let selection = selection.trim_start();
    let open = selection.find('{').ok_or(ErrorKind::ParseError)?;
    let close = selection.rfind('}').ok_or(ErrorKind::ParseError)?;
    if close <= open {
        return Err(ErrorKind::ParseError.into());
    }

    Ok((definitions, &selection[open + 1..close]))
}

/// Returns the byte offset of the parenthesis that closes the leading one in `text`.
fn matching_paren(text: &str) -> Option<usize> {
    let mut depth = 0usize;

    for (offset, character) in text.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(offset);
                }
            }
            _ => {}
        }
    }

    None
}

/// Prefixes every variable reference in `text` so that slots of the same subscription can coexist in one document.
fn rename_variables(text: &str, alias: &str) -> String {
    let mut renamed = String::with_capacity(text.len() + alias.len());
    let mut characters = text.chars().peekable();

    while let Some(character) = characters.next() {
        renamed.push(character);
        if character != '$' {
            continue;
        }

        let mut name = String::new();
        while let Some(next) = characters.peek() {
            if next.is_ascii_alphanumeric() || *next == '_' {
                name.push(*next);
                characters.next();
            } else {
                break;
            }
        }

        if !name.is_empty() {
            renamed.push_str(alias);
            renamed.push('_');
            renamed.push_str(&name);
        }
    }

    renamed
}

/// Returns the name of the root field of a rendered selection.
fn root_field_name(field: &str) -> std::result::Result<String, BlokliClientError> {
    let field = field.trim_start();
    let name = field
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .next()
        .filter(|name| !name.is_empty())
        .ok_or(ErrorKind::ParseError)?;

    Ok(name.to_owned())
}

/// Aliases the root field of a rendered selection so that its payload can be routed back to the slot.
fn alias_root_field(field: &str, alias: &str) -> std::result::Result<String, BlokliClientError> {
    let start = field
        .find(|character: char| !character.is_whitespace())
        .ok_or(ErrorKind::ParseError)?;

    Ok(format!("{}{alias}: {}", &field[..start], &field[start..]))
}

/// What one decoded response asks the hub to deliver.
struct Routed {
    payloads: Vec<(SlotId, serde_json::Value)>,
    slot_errors: Vec<(SlotId, Vec<GraphQlError>)>,
    document_errors: Vec<GraphQlError>,
}

/// Splits a decoded response into per-subscription payloads and errors.
fn route(response: GraphQlResponse<serde_json::Value>, targets: &BTreeMap<String, SlotTarget>) -> Routed {
    let GraphQlResponse { data, errors } = response;

    let mut payloads = Vec::new();
    let mut document_errors = Vec::new();
    let mut slot_errors: BTreeMap<SlotId, Vec<GraphQlError>> = BTreeMap::new();

    match data {
        Some(serde_json::Value::Object(payloads_by_alias)) => {
            for (alias, payload) in payloads_by_alias {
                let Some(target) = targets.get(&alias) else {
                    tracing::debug!(alias, "ignoring subscription payload for an unregistered alias");
                    continue;
                };
                if !payload.is_null() {
                    let mut field = serde_json::Map::with_capacity(1);
                    field.insert(target.field.clone(), payload);
                    payloads.push((target.id, serde_json::Value::Object(field)));
                }
            }
        }
        Some(payload) => document_errors.push(GraphQlError::new(
            format!("unexpected subscription payload: {payload}"),
            None,
            None,
            None,
        )),
        None => {}
    }

    for error in errors.into_iter().flatten() {
        match alias_of(&error).and_then(|alias| targets.get(&alias).map(|target| target.id)) {
            Some(id) => slot_errors.entry(id).or_default().push(error),
            None => document_errors.push(error),
        }
    }

    Routed {
        payloads,
        slot_errors: slot_errors.into_iter().collect(),
        document_errors,
    }
}

fn alias_of(error: &GraphQlError) -> Option<String> {
    match error.path.as_ref()?.first()? {
        GraphQlErrorPathSegment::Field(alias) => Some(alias.clone()),
        GraphQlErrorPathSegment::Index(_) => None,
    }
}

fn errors_to_client_error(errors: Vec<GraphQlError>) -> BlokliClientError {
    response_to_data::<serde_json::Value>(GraphQlResponse {
        data: None,
        errors: Some(errors),
    })
    .err()
    .unwrap_or_else(|| ErrorKind::NoData.into())
}

fn send(id: SlotId, tx: &mpsc::Sender<SlotEvent>, event: SlotEvent) {
    match tx.try_send(event) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!(slot = id, "subscription consumer is too slow, dropping event");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::debug!(slot = id, "subscription is no longer registered");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        time::{Duration, Instant},
    };

    use futures::StreamExt;
    use serde_json::json;
    use tokio::{sync::mpsc, time::sleep, time::timeout};

    use super::{
        SlotId, SlotSnapshot, SlotTarget, SubscriptionSpec, build_connection, rename_variables, route, split_document,
    };
    use crate::{
        api::{AccountSelector, BlokliSubscriptionClient, ChannelFilter, ChannelSelector, types::ReadinessState},
        client::{
            BlokliClient, BlokliClientConfig, GraphQlQueries,
            sse_test_server::{Connection, SseTestServer, Step, event},
        },
    };

    const WAIT: Duration = Duration::from_secs(5);
    const READY: &str = r#"{"data":{"s0":"READY"}}"#;

    fn config() -> BlokliClientConfig {
        BlokliClientConfig {
            subscription_stream_restart_delay: None,
            ..Default::default()
        }
    }

    fn channel_spec() -> SubscriptionSpec {
        SubscriptionSpec::new(GraphQlQueries::subscribe_channels(ChannelSelector {
            filter: Some(ChannelFilter::SourceKeyId(3)),
            ..Default::default()
        }))
        .expect("channel spec should render")
    }

    fn account_spec() -> SubscriptionSpec {
        SubscriptionSpec::new(GraphQlQueries::subscribe_accounts(AccountSelector::KeyId(7)))
            .expect("account spec should render")
    }

    fn snapshot_entry(spec: SubscriptionSpec) -> SlotSnapshot {
        let (tx, _rx) = mpsc::channel(1);
        SlotSnapshot { spec, tx }
    }

    fn targets(ids: &[SlotId]) -> BTreeMap<String, SlotTarget> {
        ids.iter()
            .map(|id| {
                let (tx, _rx) = mpsc::channel(1);
                (
                    format!("s{id}"),
                    SlotTarget {
                        id: *id,
                        field: "accountUpdated".to_owned(),
                        tx,
                    },
                )
            })
            .collect()
    }

    fn channel_event(alias: &str, channel_id: &str) -> String {
        json!({
            "data": {
                alias: {
                    "balance": "10 wxHOPR",
                    "closureTime": null,
                    "concreteChannelId": channel_id,
                    "destination": 2,
                    "epoch": 1,
                    "source": 3,
                    "status": "OPEN",
                    "ticketIndex": "0",
                }
            }
        })
        .to_string()
    }

    fn account_event(alias: &str, keyid: i32) -> String {
        json!({
            "data": {
                alias: {
                    "chainKey": "aabb",
                    "keyid": keyid,
                    "multiAddresses": [],
                    "packetKey": "ccdd",
                    "safeAddress": null,
                }
            }
        })
        .to_string()
    }

    async fn wait_for(condition: impl Fn() -> bool) {
        let deadline = Instant::now() + WAIT;

        while !condition() {
            assert!(Instant::now() < deadline, "timed out waiting for the expected state");
            sleep(Duration::from_millis(5)).await;
        }
    }

    #[test]
    fn combined_document_aliases_every_subscription() {
        let slots: BTreeMap<SlotId, SlotSnapshot> =
            [(0, snapshot_entry(account_spec())), (1, snapshot_entry(channel_spec()))].into();

        let document: serde_json::Value =
            serde_json::from_str(&build_connection(&slots).expect("build").body).expect("body should be JSON");

        insta::assert_json_snapshot!(document);
    }

    #[test]
    fn split_document_separates_variable_definitions_from_the_root_field() {
        let spec = SubscriptionSpec::new(GraphQlQueries::subscribe_health()).expect("health spec should render");

        let (definitions, field) = split_document(&spec.document).expect("document should split");

        assert!(definitions.is_empty());
        assert_eq!(field.trim(), "health");
    }

    #[test]
    fn split_document_rejects_documents_without_a_subscription() {
        assert!(split_document("query { version }").is_err());
        assert!(split_document("subscription Broken").is_err());
    }

    #[test]
    fn rename_variables_prefixes_every_reference() {
        assert_eq!(
            rename_variables("($keyid: Int, $packet_key: String)", "s2"),
            "($s2_keyid: Int, $s2_packet_key: String)"
        );
        assert_eq!(
            rename_variables("accountUpdated(keyid: $keyid)", "s0"),
            "accountUpdated(keyid: $s0_keyid)"
        );
        assert_eq!(rename_variables("health", "s0"), "health");
    }

    #[test]
    fn route_delivers_payloads_by_alias() {
        let targets = targets(&[0, 1]);
        let response = serde_json::from_value(json!({
            "data": { "s1": { "keyid": 9 } }
        }))
        .expect("response should decode");

        let routed = route(response, &targets);

        assert_eq!(routed.payloads.len(), 1);
        assert_eq!(routed.payloads[0].0, 1);
        assert_eq!(routed.payloads[0].1, json!({ "accountUpdated": { "keyid": 9 } }));
        assert!(routed.document_errors.is_empty());
    }

    #[test]
    fn route_delivers_field_errors_to_the_matching_slot_only() {
        let targets = targets(&[0, 1]);
        let response = serde_json::from_value(json!({
            "data": null,
            "errors": [{ "message": "slot failed", "path": ["s1"] }]
        }))
        .expect("response should decode");

        let routed = route(response, &targets);

        assert_eq!(routed.slot_errors.len(), 1);
        assert_eq!(routed.slot_errors[0].0, 1);
        assert!(routed.document_errors.is_empty());
    }

    #[test]
    fn route_delivers_document_errors_to_every_slot() {
        let targets = targets(&[0, 1]);
        let response = serde_json::from_value(json!({
            "errors": [{ "message": "unknown field" }]
        }))
        .expect("response should decode");

        let routed = route(response, &targets);

        assert_eq!(routed.document_errors.len(), 1);
        assert!(routed.payloads.is_empty());
        assert!(routed.slot_errors.is_empty());
    }

    #[test]
    fn route_ignores_payloads_for_unregistered_aliases() {
        let targets = targets(&[0]);
        let response = serde_json::from_value(json!({
            "data": { "s7": { "keyid": 9 } }
        }))
        .expect("response should decode");

        assert!(route(response, &targets).payloads.is_empty());
    }

    #[tokio::test]
    async fn subscriptions_share_one_sse_connection() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![
            event(&channel_event("s0", "0x01")),
            event(&account_event("s1", 7)),
            Step::Hang,
        ])])
        .await?;
        let client = BlokliClient::new(server.url(), config());

        let mut channels = client.subscribe_channels(ChannelSelector {
            filter: Some(ChannelFilter::SourceKeyId(3)),
            ..Default::default()
        })?;
        let mut accounts = client.subscribe_accounts(AccountSelector::KeyId(7))?;

        let (channel, account) = timeout(WAIT, async { tokio::join!(channels.next(), accounts.next()) }).await?;

        assert_eq!(
            channel
                .expect("channel event")
                .expect("channel should decode")
                .concrete_channel_id,
            "0x01"
        );
        assert_eq!(account.expect("account event").expect("account should decode").keyid, 7);
        assert_eq!(server.connections(), 1, "both subscriptions must use one connection");

        let requests = server.requests();
        assert!(
            requests[0].contains("s0: channelUpdated"),
            "unexpected request: {}",
            requests[0]
        );
        assert!(
            requests[0].contains("s1: accountUpdated"),
            "unexpected request: {}",
            requests[0]
        );
        assert!(
            requests[0].contains("s1_keyid"),
            "variables must be scoped per subscription"
        );

        Ok(())
    }

    #[tokio::test]
    async fn adding_a_subscription_rebuilds_the_document() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![event(READY), Step::Hang]),
            Connection::ok(vec![event(READY), event(&account_event("s1", 7)), Step::Hang]),
        ])
        .await?;
        let client = BlokliClient::new(server.url(), config());

        let mut health = client.subscribe_health()?;
        let first = timeout(WAIT, health.next()).await?;
        assert_eq!(
            first.expect("health event").expect("health should decode"),
            ReadinessState::Ready
        );
        assert_eq!(server.connections(), 1);

        let mut accounts = client.subscribe_accounts(AccountSelector::Any)?;
        let (replayed, account) = timeout(WAIT, async { tokio::join!(health.next(), accounts.next()) }).await?;
        assert!(replayed.is_some(), "the rebuilt connection replays the health state");
        assert_eq!(account.expect("account event").expect("account should decode").keyid, 7);

        let requests = server.requests();
        assert!(
            requests[1].contains("s0: health"),
            "unexpected request: {}",
            requests[1]
        );
        assert!(
            requests[1].contains("s1: accountUpdated"),
            "unexpected request: {}",
            requests[1]
        );

        Ok(())
    }

    #[tokio::test]
    async fn dropping_the_last_subscription_closes_the_connection() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![event(READY), Step::Hang])]).await?;
        let client = BlokliClient::new(server.url(), config());

        let mut health = client.subscribe_health()?;
        let first = timeout(WAIT, health.next()).await?;
        assert!(first.is_some());
        assert_eq!(server.open(), 1);

        drop(health);
        wait_for(|| server.open() == 0).await;

        Ok(())
    }

    #[tokio::test]
    async fn subscriptions_of_one_connection_report_their_own_health() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![
            event(READY),
            event(&account_event("s1", 7)),
            Step::Hang,
        ])])
        .await?;
        let client = BlokliClient::new(server.url(), config());

        let (mut health, health_handle) = client.subscribe_health_with_health()?;
        let (mut accounts, accounts_handle) = client.subscribe_accounts_with_health(AccountSelector::Any)?;

        let (health_event, account_event) =
            timeout(WAIT, async { tokio::join!(health.next(), accounts.next()) }).await?;
        assert_eq!(
            health_event.expect("health event").expect("health should decode"),
            ReadinessState::Ready
        );

        assert!(account_event.is_some());
        assert_ne!(health_handle.subscription(), accounts_handle.subscription());
        assert!(health_handle.get().is_healthy());
        assert_eq!(health_handle.get().connections, 1);
        assert_eq!(accounts_handle.get().connections, 1);

        Ok(())
    }

    #[test]
    fn subscriptions_recover_after_their_runtime_is_dropped() {
        let server_runtime = tokio::runtime::Runtime::new().expect("server runtime");
        let server = server_runtime
            .block_on(SseTestServer::start(vec![
                Connection::ok(vec![event(READY), Step::Hang]),
                Connection::ok(vec![event(r#"{"data":{"s1":"READY"}}"#), Step::Hang]),
            ]))
            .expect("test server should start");
        let client = BlokliClient::new(server.url(), config());

        let first_runtime = tokio::runtime::Runtime::new().expect("first runtime");
        first_runtime.block_on(async {
            let mut stream = client.subscribe_health().expect("subscription should be created");
            let event = tokio::time::timeout(WAIT, stream.next())
                .await
                .expect("first runtime should receive an event");
            assert!(event.is_some());
        });
        drop(first_runtime);

        let second_runtime = tokio::runtime::Runtime::new().expect("second runtime");
        second_runtime.block_on(async {
            let mut stream = client.subscribe_health().expect("subscription should be created");
            let event = tokio::time::timeout(WAIT, stream.next())
                .await
                .expect("a new runtime must open a new connection");
            assert_eq!(
                event.expect("event").expect("health should decode"),
                ReadinessState::Ready
            );
        });
        drop(second_runtime);

        assert_eq!(server.connections(), 2);
    }
}
