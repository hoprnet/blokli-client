//! SSE connections shared by the subscriptions of one client.
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
//! A subscription that Blokli rejects with errors of its own moves to a connection of its own and retries there at
//! the slow pace; one whose payloads do not decode moves too and reconnects there after the malformed-event backoff.
//! Retrying it on the shared connection would reconnect, and replay the state of, every other subscription each time.
//!
//! Health stays per subscription even though the transport is shared. The hub feeds connection-level events, such as
//! a reconnect, a malformed frame, or any decoded event as a sign of life, into the tracker of every subscription on
//! that connection, and data updates only into the subscription that owns the alias. A problem with the connection
//! therefore shows up on all subscriptions that use it, which is what a caller would see with one connection per
//! subscription too.

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    fmt, mem,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use cynic::{GraphQlError, GraphQlErrorPathSegment, GraphQlResponse, StreamingOperation, serde::de::DeserializeOwned};
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

/// Longest time a stream of slot changes can postpone opening a connection.
const MAX_SLOT_CHANGE_WAIT: Duration = Duration::from_millis(250);

/// Per-subscription queue depth. A slot that falls this far behind loses events instead of stalling the connection,
/// and is resynchronised once it has caught up. It has to hold the initial state of a subscription, such as one
/// event per open channel, or a consumer slower than the network loses that state on every resynchronisation.
/// The queue allocates as it fills, so an unused capacity costs no memory.
const SLOT_CHANNEL_CAPACITY: usize = 16_384;

/// Number of deregistered aliases tolerated in a live document before the connection is rebuilt.
const MAX_STALE_ALIASES: usize = 32;

/// Operation name of the combined subscription document.
const COMBINED_OPERATION_NAME: &str = "BlokliSubscriptions";

/// Identifier of a registered subscription slot.
pub(crate) type SlotId = u64;

/// Decodes the payload of one subscription into the type its stream yields.
type Decoder = fn(serde_json::Value) -> Result<Box<dyn Any + Send>, serde_json::Error>;

fn decode<Q: DeserializeOwned + Send + 'static>(
    payload: serde_json::Value,
) -> Result<Box<dyn Any + Send>, serde_json::Error> {
    serde_json::from_value::<Q>(payload).map(|value| Box::new(value) as Box<dyn Any + Send>)
}

/// One subscription rendered into a single-field GraphQL document.
#[derive(Clone, Debug)]
pub(crate) struct SubscriptionSpec {
    document: String,
    variables: serde_json::Value,
    decode: Decoder,
}

impl SubscriptionSpec {
    /// Renders a cynic subscription operation into the document and variable map used to assemble a connection.
    pub(crate) fn new<Q, V>(operation: StreamingOperation<Q, V>) -> Result<Self, BlokliClientError>
    where
        Q: DeserializeOwned + Send + 'static,
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

        Ok(Self {
            document,
            variables,
            decode: decode::<Q>,
        })
    }
}

/// One item delivered to a registered subscription.
#[derive(Debug)]
pub(crate) enum SlotEvent {
    /// Decoded payload, of the type the slot's [`SubscriptionSpec`] was created for.
    Data(Box<dyn Any + Send>),
    /// Error that belongs to the slot.
    Error(BlokliClientError),
}

/// Shared state of the subscriptions that use the same connections.
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
        Self::with_sharing(base_url, cfg, false)
    }

    /// Creates an idle hub for a single subscription, which therefore connects without waiting for others to join.
    pub(crate) fn dedicated(base_url: url::Url, cfg: BlokliClientConfig) -> Self {
        Self::with_sharing(base_url, cfg, true)
    }

    fn with_sharing(base_url: url::Url, cfg: BlokliClientConfig, dedicated: bool) -> Self {
        let (generation, _) = watch::channel(0);

        Self {
            shared: Arc::new(HubShared {
                base_url,
                cfg,
                dedicated,
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
            active: None,
            finished: false,
        }
    }

    fn register(&self, pending: PendingSubscription) -> Result<ActiveSlot, BlokliClientError> {
        if tokio::runtime::Handle::try_current().is_err() {
            let PendingSubscription { mut health, .. } = pending;
            health.end(SubscriptionEnd::Failed);
            return Err(ErrorKind::InvalidInput("subscriptions require a Tokio runtime").into());
        }

        let lagging = Arc::new(AtomicBool::new(false));
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
                    lagging: Arc::clone(&lagging),
                    isolated: None,
                    resync: false,
                },
            );
            (id, rx)
        };

        self.shared.publish_generation();
        self.shared.ensure_supervisors();

        Ok(ActiveSlot {
            receiver,
            lagging,
            guard: SlotGuard { hub: self.clone(), id },
        })
    }

    fn deregister(&self, id: SlotId) {
        // Dropping the entry drops its tracker, which marks the subscription as cancelled.
        self.shared.lock_state().slots.remove(&id);
        self.shared.publish_generation();
    }

    /// Asks for a new connection so that the server replays the state a lagging slot missed.
    fn request_resync(&self, id: SlotId) {
        if let Some(entry) = self.shared.lock_state().slots.get_mut(&id) {
            entry.resync = true;
        }
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
    active: Option<ActiveSlot>,
    finished: bool,
}

struct PendingSubscription {
    spec: SubscriptionSpec,
    health: HealthTracker,
    reqwest_client: reqwest::Client,
}

struct ActiveSlot {
    receiver: mpsc::Receiver<SlotEvent>,
    /// Set by the connection when it dropped an event because the queue was full.
    lagging: Arc<AtomicBool>,
    guard: SlotGuard,
}

impl Stream for SubscriptionSlot {
    type Item = SlotEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.finished {
            return Poll::Ready(None);
        }

        if this.active.is_none() {
            let Some(pending) = this.pending.take() else {
                this.finish();
                return Poll::Ready(None);
            };

            match this.hub.register(pending) {
                Ok(active) => this.active = Some(active),
                Err(error) => {
                    this.finish();
                    return Poll::Ready(Some(SlotEvent::Error(error)));
                }
            }
        }

        let Some(active) = this.active.as_mut() else {
            this.finish();
            return Poll::Ready(None);
        };

        match active.receiver.poll_recv(cx) {
            Poll::Ready(Some(event)) => {
                // Once a slot that lost events has drained its queue, reconnect so the server replays what it missed.
                // Waiting until then keeps the replay from being dropped as well.
                if active.receiver.is_empty() && active.lagging.swap(false, Ordering::AcqRel) {
                    let id = active.guard.id;
                    this.hub.request_resync(id);
                }
                Poll::Ready(Some(event))
            }
            Poll::Ready(None) => {
                this.finish();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl SubscriptionSlot {
    fn finish(&mut self) {
        self.finished = true;
        self.active = None;
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
    /// The hub serves a single subscription.
    dedicated: bool,
    state: Mutex<HubState>,
    generation: watch::Sender<u64>,
}

#[derive(Default)]
struct HubState {
    next_id: SlotId,
    slots: BTreeMap<SlotId, SlotEntry>,
    generation: u64,
    /// Liveness token of the supervisor of the shared connection.
    supervisor: Option<Weak<()>>,
    reqwest_client: Option<reqwest::Client>,
}

struct SlotEntry {
    spec: SubscriptionSpec,
    health: HealthTracker,
    tx: mpsc::Sender<SlotEvent>,
    lagging: Arc<AtomicBool>,
    /// Set once the slot moved onto a connection of its own.
    isolated: Option<Isolation>,
    /// The slot missed events and needs a new connection to receive the server's state again.
    resync: bool,
}

/// The connection of its own that a slot moved to.
struct Isolation {
    /// Liveness token of the supervisor of the connection.
    supervisor: Weak<()>,
    cause: IsolationCause,
}

/// Why a slot moved onto a connection of its own.
#[derive(Clone, Copy, Debug)]
enum IsolationCause {
    /// Blokli refused the slot; it retries at the slow pace.
    Rejected,
    /// The slot's payloads did not decode; it reconnects after the malformed-event backoff.
    Malformed,
}

impl SlotEntry {
    fn belongs_to(&self, id: SlotId, scope: Scope) -> bool {
        match scope {
            Scope::Shared => self.isolated.is_none(),
            Scope::Isolated(own) => id == own,
        }
    }
}

/// Which connection of the hub a supervisor drives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    /// The connection shared by every subscription that Blokli has not rejected.
    Shared,
    /// The connection of one subscription that Blokli rejected.
    Isolated(SlotId),
}

/// What one connection needs to know about a registered subscription.
#[derive(Clone)]
struct SlotSnapshot {
    spec: SubscriptionSpec,
    tx: mpsc::Sender<SlotEvent>,
    lagging: Arc<AtomicBool>,
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
    lagging: Arc<AtomicBool>,
    decode: Decoder,
}

/// How the supervisor continues after a connection stopped driving subscriptions.
enum Outcome {
    /// The registered slot set changed and needs a new document.
    Rebuild,
    /// The connection should be reopened after the given delay.
    Reconnect(Duration),
    /// The subscriptions of this connection are over.
    Stop(SubscriptionEnd),
}

/// What one SSE event asked the connection to do.
enum Dispatch {
    /// The event was routed, or carried nothing that concerns a subscription.
    Handled,
    /// The event could not be decoded; reconnect after the backoff.
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

    /// Starts a supervisor for every connection that has subscriptions but no running supervisor.
    fn ensure_supervisors(self: &Arc<Self>) {
        let mut starts = Vec::new();

        {
            let mut state = self.lock_state();
            let shared_running = state.supervisor.as_ref().and_then(Weak::upgrade).is_some();
            if !shared_running && state.slots.values().any(|entry| entry.isolated.is_none()) {
                let token = Arc::new(());
                state.supervisor = Some(Arc::downgrade(&token));
                starts.push((Scope::Shared, token));
            }

            for (id, entry) in state.slots.iter_mut() {
                if let Some(isolation) = entry.isolated.as_mut()
                    && isolation.supervisor.upgrade().is_none()
                {
                    let token = Arc::new(());
                    isolation.supervisor = Arc::downgrade(&token);
                    starts.push((Scope::Isolated(*id), token));
                }
            }
        }

        for (scope, token) in starts {
            let shared = Arc::clone(self);
            let generation = self.generation.subscribe();
            tokio::spawn(async move {
                run_supervisor(&shared, scope, generation).await;
                // Released before checking again, so subscriptions that registered while this supervisor was
                // exiting get a new one.
                drop(token);
                shared.ensure_supervisors();
            });
        }
    }

    /// Moves a subscription Blokli rejected, or whose payloads do not decode, onto a connection of its own, so its
    /// retries do not reconnect the others.
    ///
    /// Its alias stays in the shared document until the next rebuild, like the alias of a dropped subscription.
    fn isolate(self: &Arc<Self>, id: SlotId, cause: IsolationCause) {
        {
            let mut state = self.lock_state();
            let Some(entry) = state.slots.get_mut(&id) else {
                return;
            };
            if entry.isolated.is_some() {
                return;
            }
            entry.isolated = Some(Isolation {
                supervisor: Weak::new(),
                cause,
            });
        }

        // Wakes the shared connection, which closes once no subscription is left on it.
        self.publish_generation();
        self.ensure_supervisors();
    }

    fn scope_ids(&self, scope: Scope) -> BTreeSet<SlotId> {
        self.lock_state()
            .slots
            .iter()
            .filter(|(id, entry)| entry.belongs_to(**id, scope))
            .map(|(id, _)| *id)
            .collect()
    }

    /// Returns the subscriptions of `scope` and clears their resync requests, which the next connection fulfils.
    fn snapshot(&self, scope: Scope) -> BTreeMap<SlotId, SlotSnapshot> {
        let mut state = self.lock_state();
        state
            .slots
            .iter_mut()
            .filter(|(id, entry)| entry.belongs_to(**id, scope))
            .map(|(id, entry)| {
                if mem::take(&mut entry.resync) {
                    entry.health.caught_up();
                }
                (
                    *id,
                    SlotSnapshot {
                        spec: entry.spec.clone(),
                        tx: entry.tx.clone(),
                        lagging: Arc::clone(&entry.lagging),
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

    /// Ends the subscriptions of `scope` that a stopped connection carried and releases their streams.
    ///
    /// Subscriptions that registered meanwhile were never part of that connection; the next supervisor serves them.
    fn end_connected(&self, scope: Scope, connected: &BTreeSet<SlotId>, end: SubscriptionEnd) {
        let ids: Vec<SlotId> = self.scope_ids(scope).intersection(connected).copied().collect();
        self.end_slots(&ids, end);
    }

    fn with_health(&self, scope: Scope, ids: &[SlotId], mut update: impl FnMut(&mut HealthTracker)) {
        let mut state = self.lock_state();
        for id in ids {
            if let Some(entry) = state.slots.get_mut(id)
                && entry.belongs_to(*id, scope)
            {
                update(&mut entry.health);
            }
        }
    }

    /// Marks the subscriptions the server refused as rejected and returns the ones for which this is a new refusal,
    /// so the error is yielded only once per episode.
    fn reject(
        &self,
        scope: Scope,
        ids: &[SlotId],
        problem: SubscriptionProblem,
        error: &dyn fmt::Display,
        retry_in: Duration,
    ) -> Vec<SlotId> {
        let mut state = self.lock_state();
        let mut first = Vec::new();

        for id in ids {
            if let Some(entry) = state.slots.get_mut(id)
                && entry.belongs_to(*id, scope)
                && entry.health.reject(problem, error, retry_in)
            {
                first.push(*id);
            }
        }

        first
    }

    /// The earliest moment at which a subscription of `scope` would count as stalled.
    fn stall_wakeup(&self, scope: Scope) -> Option<Instant> {
        let threshold = self.cfg.subscription_stall_threshold?;
        let state = self.lock_state();

        state
            .slots
            .iter()
            .filter(|(id, entry)| entry.belongs_to(**id, scope))
            .filter_map(|(_, entry)| entry.health.stall_deadline(threshold))
            .min()
    }

    /// Reports the subscriptions of `scope` whose stall deadline has passed.
    fn mark_stalled(&self, scope: Scope) {
        let Some(threshold) = self.cfg.subscription_stall_threshold else {
            return;
        };
        let now = Instant::now();
        let mut state = self.lock_state();

        for (id, entry) in state.slots.iter_mut() {
            if entry.belongs_to(*id, scope)
                && entry
                    .health
                    .stall_deadline(threshold)
                    .is_some_and(|deadline| deadline <= now)
            {
                entry.health.stalled();
            }
        }
    }

    /// Queues one event for a subscription and reports whether it was queued.
    ///
    /// A consumer that falls behind loses events instead of stalling the connection. Its health shows the gap, and
    /// once it has drained its queue the connection is rebuilt, which makes the server replay the missed state.
    fn deliver(&self, scope: Scope, target: &SlotTarget, event: SlotEvent) -> bool {
        match target.tx.try_send(event) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !target.lagging.swap(true, Ordering::AcqRel) {
                    tracing::warn!(
                        slot = target.id,
                        "subscription consumer is too slow, dropping events until it catches up"
                    );
                    self.with_health(scope, &[target.id], HealthTracker::lagging);
                }
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!(slot = target.id, "subscription is no longer registered");
                false
            }
        }
    }

    /// Waits until the registered slot set is stable, then assembles the document to connect with.
    async fn prepare(&self, scope: Scope, generation: &mut watch::Receiver<u64>) -> Option<Connection> {
        let wait_until = tokio::time::Instant::now() + MAX_SLOT_CHANGE_WAIT;

        loop {
            let slots = self.snapshot(scope);
            if slots.is_empty() {
                return None;
            }

            // Only the shared connection of a client's hub gathers several subscriptions; any other one carries
            // a single slot and has nothing to wait for. Continuous changes do not postpone the connection forever.
            if scope == Scope::Shared && !self.dedicated && tokio::time::Instant::now() < wait_until {
                match tokio::time::timeout(SLOT_CHANGE_DEBOUNCE, generation.changed()).await {
                    Ok(Ok(())) => continue,
                    Ok(Err(_)) => return None,
                    Err(_) => {}
                }
            }

            return match build_connection(&slots) {
                Ok(connection) => Some(connection),
                Err(error) => {
                    tracing::error!(%error, "failed to build the combined subscription request");
                    for entry in slots.values() {
                        // The slot ends right after, so a full queue loses nothing that matters.
                        let _ = entry.tx.try_send(SlotEvent::Error(error.clone()));
                    }
                    self.end_slots(&slots.keys().copied().collect::<Vec<_>>(), SubscriptionEnd::Failed);
                    None
                }
            };
        }
    }

    /// Waits `delay` before the next connection attempt, but no longer than until a subscription that the last
    /// connection did not cover joins, or until no subscription is left.
    async fn wait_before_reconnect(
        &self,
        scope: Scope,
        generation: &mut watch::Receiver<u64>,
        delay: Duration,
        connected: &BTreeSet<SlotId>,
    ) {
        let deadline = tokio::time::Instant::now() + delay;

        loop {
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => return,
                changed = generation.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let live = self.scope_ids(scope);
                    if live.is_empty() || !live.is_subset(connected) {
                        return;
                    }
                }
            }
        }
    }

    /// Drives one SSE connection until it ends or the registered slot set needs a different document.
    async fn drive(
        self: &Arc<Self>,
        scope: Scope,
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
                    self.deliver(scope, target, SlotEvent::Error(error.clone()));
                }
                return Outcome::Stop(SubscriptionEnd::Failed);
            }
        };

        loop {
            let stall = self.stall_wakeup(scope);
            let stalled = async {
                match stall {
                    Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
                    None => std::future::pending::<()>().await,
                }
            };

            tokio::select! {
                biased;

                changed = generation.changed() => {
                    if changed.is_err() || self.reconnect_needed(scope, &targets) {
                        return Outcome::Rebuild;
                    }
                }

                _ = stalled => self.mark_stalled(scope),

                event = stream.next() => match event {
                    Some(Ok(SSE::Event(event))) => {
                        tracing::trace!(
                            event_type = %event.event_type,
                            bytes = event.data.len(),
                            "SSE event"
                        );
                        match self.dispatch(scope, &targets, &event.data, malformed_event_delay) {
                            Dispatch::Handled => {}
                            Dispatch::Malformed(delay) => return Outcome::Reconnect(delay),
                            Dispatch::Rejected => return Outcome::Reconnect(rejected_retry_delay(&self.cfg)),
                        }
                    }
                    Some(Ok(SSE::Comment(_))) => {
                        tracing::trace!("SSE keepalive");
                        self.with_health(scope, &ids, |health| health.transport_activity());
                    }
                    Some(Ok(SSE::Connected(details))) => {
                        tracing::debug!(status = details.response().status(), "SSE connection established");
                        self.with_health(scope, &ids, |health| health.connected());
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
                                scope,
                                &ids,
                                problem,
                                &describe(&error),
                                rejected_retry_delay(&self.cfg),
                            );
                            if !first.is_empty() {
                                let error = BlokliClientError::from(ErrorKind::Subscription(Box::new(error)));
                                for target in targets.values() {
                                    if first.contains(&target.id) {
                                        self.deliver(scope, target, SlotEvent::Error(error.clone()));
                                    }
                                }
                            }
                            return Outcome::Reconnect(rejected_retry_delay(&self.cfg));
                        }

                        self.with_health(scope, &ids, |health| health.problem(problem, &describe(&error)));
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
    fn reconnect_needed(&self, scope: Scope, targets: &BTreeMap<String, SlotTarget>) -> bool {
        let state = self.lock_state();
        let mut live = BTreeSet::new();
        let mut resync = false;
        for (id, entry) in &state.slots {
            if entry.belongs_to(*id, scope) {
                live.insert(*id);
                resync |= entry.resync;
            }
        }

        if live.is_empty() || resync {
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
        self: &Arc<Self>,
        scope: Scope,
        targets: &BTreeMap<String, SlotTarget>,
        data: &str,
        malformed_event_delay: &mut Option<Duration>,
    ) -> Dispatch {
        let ids: Vec<SlotId> = targets.values().map(|target| target.id).collect();

        let routed = match serde_json::from_str::<GraphQlResponse<serde_json::Value>>(data) {
            Ok(response) => route(response, targets).ok_or_else(|| BlokliClientError::from(ErrorKind::NoData)),
            Err(error) => Err(BlokliClientError::from(ErrorKind::from(error))),
        };
        let Routed {
            payloads,
            slot_errors,
            document_errors,
        } = match routed {
            Ok(routed) => routed,
            Err(error) => {
                tracing::debug!(bytes = data.len(), "reconnecting after a malformed SSE event");
                self.with_health(scope, &ids, |health| {
                    health.problem(SubscriptionProblem::MalformedEvent, &error)
                });

                return Dispatch::Malformed(next_malformed_event_delay(&self.cfg, malformed_event_delay));
            }
        };

        let by_id: BTreeMap<SlotId, &SlotTarget> = targets.values().map(|target| (target.id, target)).collect();
        // Aliases of subscriptions that left this connection stay in its document until the next rebuild; what
        // arrives for them is no longer theirs.
        let live = self.scope_ids(scope);

        let had_payloads = !payloads.is_empty();
        let mut decoded_any = false;
        let mut malformed = false;

        for (id, payload) in payloads {
            let Some(target) = by_id.get(&id).filter(|_| live.contains(&id)) else {
                continue;
            };
            match (target.decode)(payload) {
                Ok(value) => {
                    decoded_any = true;
                    if self.deliver(scope, target, SlotEvent::Data(value)) {
                        self.with_health(scope, &[id], HealthTracker::data_update);
                    }
                }
                Err(error) => {
                    // Valid JSON in the wrong shape is as broken as a truncated frame: reconnecting makes the server
                    // replay its state, so the lost update is not silently missed. Only this subscription reconnects,
                    // on a connection of its own, so its siblings neither replay their state nor reset its backoff.
                    let error = BlokliClientError::from(ErrorKind::from(error));
                    tracing::debug!(slot = id, %error, "subscription payload does not match its selection");
                    self.with_health(scope, &[id], |health| {
                        health.problem(SubscriptionProblem::MalformedEvent, &error)
                    });
                    match scope {
                        Scope::Shared => self.isolate(id, IsolationCause::Malformed),
                        Scope::Isolated(_) => malformed = true,
                    }
                }
            }
        }

        if decoded_any {
            // The server sends keepalives only while the whole connection is idle, so an event for one subscription
            // is the sign of life for all of them.
            self.with_health(scope, &ids, HealthTracker::transport_activity);
        }

        let mut rejected = false;
        for (id, errors) in slot_errors {
            let Some(target) = by_id.get(&id).filter(|_| live.contains(&id)) else {
                continue;
            };
            let error = errors_to_client_error(errors);
            let first = self.reject(
                scope,
                &[id],
                SubscriptionProblem::GraphQl,
                &error,
                rejected_retry_delay(&self.cfg),
            );
            if !first.is_empty() {
                self.deliver(scope, target, SlotEvent::Error(error));
            }

            match scope {
                Scope::Shared => self.isolate(id, IsolationCause::Rejected),
                Scope::Isolated(_) => rejected = true,
            }
        }

        if !document_errors.is_empty() {
            if had_payloads {
                // Data wins over errors, as it does for one-shot operations.
                tracing::error!(
                    ?document_errors,
                    "subscription payload received with document-level errors"
                );
            } else {
                let error = errors_to_client_error(document_errors);
                let first = self.reject(
                    scope,
                    &ids,
                    SubscriptionProblem::GraphQl,
                    &error,
                    rejected_retry_delay(&self.cfg),
                );

                for id in first {
                    if let Some(target) = by_id.get(&id) {
                        self.deliver(scope, target, SlotEvent::Error(error.clone()));
                    }
                }

                // Asking again right away gets the same answer, so retry at the slow pace even when the rejection is
                // not new and was therefore not yielded again.
                return Dispatch::Rejected;
            }
        }

        if malformed {
            return Dispatch::Malformed(next_malformed_event_delay(&self.cfg, malformed_event_delay));
        }
        if decoded_any {
            *malformed_event_delay = None;
        }

        if rejected {
            Dispatch::Rejected
        } else {
            Dispatch::Handled
        }
    }

    fn open_stream(
        &self,
        body: String,
    ) -> Result<eventsource_client::BoxStream<eventsource_client::Result<SSE>>, BlokliClientError> {
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

async fn run_supervisor(shared: &Arc<HubShared>, scope: Scope, mut generation: watch::Receiver<u64>) {
    let mut malformed_event_delay = None;

    if let Scope::Isolated(id) = scope {
        let cause = shared
            .lock_state()
            .slots
            .get(&id)
            .and_then(|entry| entry.isolated.as_ref().map(|isolation| isolation.cause));
        // The subscription moved here after a failure, so it waits as it would have before reconnecting.
        let delay = match cause {
            Some(IsolationCause::Rejected) => rejected_retry_delay(&shared.cfg),
            Some(IsolationCause::Malformed) => next_malformed_event_delay(&shared.cfg, &mut malformed_event_delay),
            None => return,
        };
        shared
            .wait_before_reconnect(scope, &mut generation, delay, &BTreeSet::from([id]))
            .await;
    }

    loop {
        let Some(connection) = shared.prepare(scope, &mut generation).await else {
            return;
        };
        let connected: BTreeSet<SlotId> = connection.targets.values().map(|target| target.id).collect();

        match shared
            .drive(scope, &mut generation, connection, &mut malformed_event_delay)
            .await
        {
            Outcome::Rebuild => continue,
            Outcome::Reconnect(delay) => {
                shared
                    .wait_before_reconnect(scope, &mut generation, delay, &connected)
                    .await
            }
            Outcome::Stop(end) => {
                shared.end_connected(scope, &connected, end);
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
fn build_connection(slots: &BTreeMap<SlotId, SlotSnapshot>) -> Result<Connection, BlokliClientError> {
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
                lagging: Arc::clone(&entry.lagging),
                decode: entry.spec.decode,
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
fn split_document(document: &str) -> Result<(&str, &str), BlokliClientError> {
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
    let mut offset = 0;

    while let Some(character) = text[offset..].chars().next() {
        if let Some(length) = string_literal_len(&text[offset..]) {
            offset += length;
            continue;
        }

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
        offset += character.len_utf8();
    }

    None
}

/// Prefixes every variable reference in `text` so that slots of the same subscription can coexist in one document.
///
/// String literals are copied unchanged, so a `$` inside a string value is not mistaken for a variable.
fn rename_variables(text: &str, alias: &str) -> String {
    let mut renamed = String::with_capacity(text.len() + alias.len());
    let mut rest = text;

    while let Some(character) = rest.chars().next() {
        if let Some(length) = string_literal_len(rest) {
            renamed.push_str(&rest[..length]);
            rest = &rest[length..];
            continue;
        }

        renamed.push(character);
        rest = &rest[character.len_utf8()..];
        if character != '$' {
            continue;
        }

        let name_length = rest
            .find(|next: char| !(next.is_ascii_alphanumeric() || next == '_'))
            .unwrap_or(rest.len());
        if name_length > 0 {
            renamed.push_str(alias);
            renamed.push('_');
            renamed.push_str(&rest[..name_length]);
            rest = &rest[name_length..];
        }
    }

    renamed
}

/// Returns the byte length of the GraphQL string literal that `text` starts with, if it starts with one.
///
/// An unterminated literal extends to the end of `text`.
fn string_literal_len(text: &str) -> Option<usize> {
    if let Some(body) = text.strip_prefix(r#"""""#) {
        // A block string ends at the first `"""` that is not escaped as `\"""`.
        let mut offset = 0;
        while let Some(found) = body[offset..].find(r#"""""#) {
            let end = offset + found;
            if !body[..end].ends_with('\\') {
                return Some(3 + end + 3);
            }
            offset = end + 3;
        }
        return Some(text.len());
    }

    let body = text.strip_prefix('"')?;
    let mut escaped = false;
    for (offset, character) in body.char_indices() {
        match character {
            _ if escaped => escaped = false,
            '\\' => escaped = true,
            '"' => return Some(1 + offset + 1),
            _ => {}
        }
    }

    Some(text.len())
}

/// Returns the name of the root field of a rendered selection.
fn root_field_name(field: &str) -> Result<String, BlokliClientError> {
    let field = field.trim_start();
    let name = field
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .next()
        .filter(|name| !name.is_empty())
        .ok_or(ErrorKind::ParseError)?;

    Ok(name.to_owned())
}

/// Aliases the root field of a rendered selection so that its payload can be routed back to the slot.
fn alias_root_field(field: &str, alias: &str) -> Result<String, BlokliClientError> {
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
///
/// Returns `None` for a frame that carries neither data nor errors, which is as broken as one that does not parse.
fn route(response: GraphQlResponse<serde_json::Value>, targets: &BTreeMap<String, SlotTarget>) -> Option<Routed> {
    let GraphQlResponse { data, errors } = response;

    let payloads_by_alias = match data {
        Some(serde_json::Value::Object(payloads_by_alias)) => payloads_by_alias,
        Some(_) => return None,
        None => serde_json::Map::new(),
    };
    let errors = errors.unwrap_or_default();
    if payloads_by_alias.is_empty() && errors.is_empty() {
        return None;
    }

    let mut document_errors = Vec::new();
    let mut slot_errors: BTreeMap<SlotId, Vec<GraphQlError>> = BTreeMap::new();
    for error in errors {
        match alias_of(&error).and_then(|alias| targets.get(&alias).map(|target| target.id)) {
            Some(id) => slot_errors.entry(id).or_default().push(error),
            None => document_errors.push(error),
        }
    }

    let mut payloads = Vec::new();
    for (alias, payload) in payloads_by_alias {
        let Some(target) = targets.get(&alias) else {
            tracing::debug!(alias, "ignoring subscription payload for an unregistered alias");
            continue;
        };
        if payload.is_null() {
            // A field that failed is null next to its errors. Without errors, the decoder decides whether null is a
            // valid payload.
            if slot_errors.contains_key(&target.id) {
                continue;
            }
        } else if let Some(errors) = slot_errors.remove(&target.id) {
            // Data wins over errors, as it does for one-shot operations.
            tracing::error!(
                slot = target.id,
                ?errors,
                "subscription payload received with field errors"
            );
        }
        let mut field = serde_json::Map::with_capacity(1);
        field.insert(target.field.clone(), payload);
        payloads.push((target.id, serde_json::Value::Object(field)));
    }

    Some(Routed {
        payloads,
        slot_errors: slot_errors.into_iter().collect(),
        document_errors,
    })
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

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, atomic::AtomicBool},
        time::{Duration, Instant},
    };

    use futures::{FutureExt, StreamExt, executor::block_on};
    use serde_json::json;
    use tokio::{
        sync::mpsc,
        time::{sleep, timeout},
    };

    use super::{
        MAX_SLOT_CHANGE_WAIT, SLOT_CHANNEL_CAPACITY, Scope, SlotEntry, SlotId, SlotSnapshot, SlotTarget,
        SubscriptionHub, SubscriptionSpec, build_connection, decode, matching_paren, rename_variables, route,
        split_document,
    };
    use crate::{
        SubscriptionEnd, SubscriptionProblem, SubscriptionStatus,
        api::{AccountSelector, BlokliSubscriptionClient, ChannelFilter, ChannelSelector},
        client::{
            BlokliClient, BlokliClientConfig, GraphQlQueries,
            sse_test_server::{Connection, SseTestServer, Step, event, keepalive},
        },
        health::HealthTracker,
    };

    const WAIT: Duration = Duration::from_secs(5);

    fn config() -> BlokliClientConfig {
        BlokliClientConfig {
            subscription_stream_restart_delay: None,
            ..Default::default()
        }
    }

    fn channel_selector() -> ChannelSelector {
        ChannelSelector {
            filter: Some(ChannelFilter::SourceKeyId(3)),
            ..Default::default()
        }
    }

    fn channel_spec() -> SubscriptionSpec {
        SubscriptionSpec::new(GraphQlQueries::subscribe_channels(channel_selector()))
            .expect("channel spec should render")
    }

    fn account_spec() -> SubscriptionSpec {
        SubscriptionSpec::new(GraphQlQueries::subscribe_accounts(AccountSelector::KeyId(7)))
            .expect("account spec should render")
    }

    fn snapshot_entry(spec: SubscriptionSpec) -> SlotSnapshot {
        let (tx, _rx) = mpsc::channel(1);
        SlotSnapshot {
            spec,
            tx,
            lagging: Arc::default(),
        }
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
                        lagging: Arc::new(AtomicBool::new(false)),
                        decode: decode::<serde_json::Value>,
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

        let routed = route(response, &targets).expect("frame is routable");

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

        let routed = route(response, &targets).expect("frame is routable");

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

        let routed = route(response, &targets).expect("frame is routable");

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

        assert!(
            route(response, &targets)
                .expect("frame is routable")
                .payloads
                .is_empty()
        );
    }

    #[test]
    fn route_treats_frames_without_data_or_errors_as_malformed() {
        let targets = targets(&[0]);

        // Frames with neither field, such as `{}` or `{"data":null}`, already fail to parse.
        for frame in [
            json!({ "data": {} }),
            json!({ "errors": [] }),
            json!({ "data": "bad" }),
            json!({ "data": [] }),
        ] {
            let response = serde_json::from_value(frame.clone()).expect("response should decode");
            assert!(route(response, &targets).is_none(), "{frame} must count as malformed");
        }
    }

    #[test]
    fn route_lets_data_win_over_field_errors_of_the_same_subscription() {
        let targets = targets(&[0, 1]);
        let response = serde_json::from_value(json!({
            "data": { "s0": { "keyid": 9 }, "s1": null },
            "errors": [
                { "message": "partial failure", "path": ["s0", "keyid"] },
                { "message": "slot failed", "path": ["s1"] }
            ]
        }))
        .expect("response should decode");

        let routed = route(response, &targets).expect("frame is routable");

        assert_eq!(routed.payloads, vec![(0, json!({ "accountUpdated": { "keyid": 9 } }))]);
        let failed: Vec<SlotId> = routed.slot_errors.iter().map(|(id, _)| *id).collect();
        assert_eq!(failed, vec![1]);
    }

    #[test]
    fn route_hands_a_null_payload_without_errors_to_the_decoder() {
        let targets = targets(&[0]);
        let response = serde_json::from_value(json!({ "data": { "s0": null } })).expect("response should decode");

        let routed = route(response, &targets).expect("frame is routable");

        assert_eq!(routed.payloads, vec![(0, json!({ "accountUpdated": null }))]);
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
    async fn continuous_registrations_do_not_postpone_the_connection_forever() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![Step::Hang])]).await?;
        let client = BlokliClient::new(server.url(), config());
        let started = Instant::now();
        let mut streams = Vec::new();

        // Every registration comes before the debounce of the previous one has passed.
        while started.elapsed() < MAX_SLOT_CHANGE_WAIT * 3 && server.connections() == 0 {
            let mut channels = client.subscribe_channels(channel_selector())?;
            assert!(channels.next().now_or_never().is_none());
            streams.push(channels);
            sleep(Duration::from_millis(5)).await;
        }

        assert!(
            server.connections() > 0,
            "no connection after {} registrations",
            streams.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn adding_a_subscription_rebuilds_the_document() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![event(&channel_event("s0", "0x01")), Step::Hang]),
            Connection::ok(vec![
                event(&channel_event("s0", "0x01")),
                event(&account_event("s1", 7)),
                Step::Hang,
            ]),
        ])
        .await?;
        let client = BlokliClient::new(server.url(), config());

        let mut channels = client.subscribe_channels(channel_selector())?;
        let first = timeout(WAIT, channels.next()).await?;
        assert_eq!(
            first
                .expect("channel event")
                .expect("channel should decode")
                .concrete_channel_id,
            "0x01"
        );
        assert_eq!(server.connections(), 1);

        let mut accounts = client.subscribe_accounts(AccountSelector::Any)?;
        let (replayed, account) = timeout(WAIT, async { tokio::join!(channels.next(), accounts.next()) }).await?;
        assert!(replayed.is_some(), "the rebuilt connection replays the channel state");
        assert_eq!(account.expect("account event").expect("account should decode").keyid, 7);

        let requests = server.requests();
        assert!(
            requests[1].contains("s0: channelUpdated"),
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

    #[test]
    fn rename_variables_leaves_string_literals_alone() {
        assert_eq!(
            rename_variables(r#"f(a: $a, b: "$b \" $c", c: """$d \""" $e""", d: $f)"#, "s1"),
            r#"f(a: $s1_a, b: "$b \" $c", c: """$d \""" $e""", d: $s1_f)"#
        );
    }

    #[test]
    fn matching_paren_skips_parentheses_in_string_literals() {
        let text = r#"($a: String = ")(", $b: Int) { f }"#;

        assert_eq!(matching_paren(text), text.find(") {"));
    }

    #[tokio::test]
    async fn dropping_the_last_subscription_closes_the_connection() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![
            event(&channel_event("s0", "0x01")),
            Step::Hang,
        ])])
        .await?;
        let client = BlokliClient::new(server.url(), config());

        let mut channels = client.subscribe_channels(channel_selector())?;
        let first = timeout(WAIT, channels.next()).await?;
        assert!(first.is_some());
        assert_eq!(server.open(), 1);

        drop(channels);
        wait_for(|| server.open() == 0).await;

        Ok(())
    }

    #[tokio::test]
    async fn subscriptions_of_one_connection_report_their_own_health() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![
            event(&channel_event("s0", "0x01")),
            event(&account_event("s1", 7)),
            Step::Hang,
        ])])
        .await?;
        let client = BlokliClient::new(server.url(), config());

        let (mut channels, channels_handle) = client.subscribe_channels_with_health(channel_selector())?;
        let (mut accounts, accounts_handle) = client.subscribe_accounts_with_health(AccountSelector::Any)?;

        let (channel, account) = timeout(WAIT, async { tokio::join!(channels.next(), accounts.next()) }).await?;

        assert!(channel.is_some_and(|channel| channel.is_ok()));
        assert!(account.is_some_and(|account| account.is_ok()));
        assert_ne!(channels_handle.subscription(), accounts_handle.subscription());
        assert!(channels_handle.get().is_healthy());
        assert_eq!(channels_handle.get().connections, 1);
        assert_eq!(accounts_handle.get().connections, 1);

        Ok(())
    }

    #[test]
    fn subscriptions_recover_after_their_runtime_is_dropped() {
        let server_runtime = tokio::runtime::Runtime::new().expect("server runtime");
        let server = server_runtime
            .block_on(SseTestServer::start(vec![
                Connection::ok(vec![event(&channel_event("s0", "0x01")), Step::Hang]),
                Connection::ok(vec![event(&channel_event("s1", "0x02")), Step::Hang]),
            ]))
            .expect("test server should start");
        let client = BlokliClient::new(server.url(), config());

        let first_runtime = tokio::runtime::Runtime::new().expect("first runtime");
        first_runtime.block_on(async {
            let mut stream = client
                .subscribe_channels(channel_selector())
                .expect("subscription should be created");
            let event = timeout(WAIT, stream.next())
                .await
                .expect("first runtime should receive an event");
            assert!(event.is_some());
        });
        drop(first_runtime);

        let second_runtime = tokio::runtime::Runtime::new().expect("second runtime");
        second_runtime.block_on(async {
            let mut stream = client
                .subscribe_channels(channel_selector())
                .expect("subscription should be created");
            let event = timeout(WAIT, stream.next())
                .await
                .expect("a new runtime must open a new connection");
            assert_eq!(
                event
                    .expect("event")
                    .expect("channel should decode")
                    .concrete_channel_id,
                "0x02"
            );
        });
        drop(second_runtime);

        assert_eq!(server.connections(), 2);
    }

    #[tokio::test]
    async fn health_and_transaction_tracking_use_connections_of_their_own() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![Step::Hang])]).await?;
        let client = BlokliClient::new(server.url(), config());

        let mut channels = client.subscribe_channels(channel_selector())?;
        let mut accounts = client.subscribe_accounts(AccountSelector::Any)?;
        let mut health = client.subscribe_health()?;
        let mut transaction = client.subscribe_track_transaction("tx-1".to_owned())?;

        // Nothing is ever sent; polling only registers the subscriptions.
        let _ = timeout(Duration::from_millis(200), async {
            tokio::join!(channels.next(), accounts.next(), health.next(), transaction.next())
        })
        .await;
        wait_for(|| server.requests().len() == 3).await;

        let requests = server.requests();
        let with = |needle: &str| requests.iter().filter(|request| request.contains(needle)).count();
        assert_eq!(with("channelUpdated"), 1);
        assert_eq!(with("accountUpdated"), 1);
        assert_eq!(with("health"), 1);
        assert_eq!(with("transactionUpdated"), 1);
        let shared = requests
            .iter()
            .find(|request| request.contains("channelUpdated"))
            .expect("shared request");
        assert!(shared.contains("accountUpdated"), "unexpected request: {shared}");
        assert!(!shared.contains("health"), "unexpected request: {shared}");
        assert!(!shared.contains("transactionUpdated"), "unexpected request: {shared}");

        Ok(())
    }

    #[tokio::test]
    async fn a_slow_consumer_is_reported_and_resynchronised_once_it_catches_up() -> anyhow::Result<()> {
        let mut flood = vec![event(&channel_event("s0", "0x00"))];
        flood.extend((0..SLOT_CHANNEL_CAPACITY + 100).map(|_| event(&channel_event("s0", "0x01"))));
        // Traffic on the connection says nothing about the consumer, which is still behind.
        flood.push(keepalive());
        flood.push(Step::Hang);
        let server = SseTestServer::start(vec![
            Connection::ok(flood),
            Connection::ok(vec![event(&channel_event("s0", "0xff")), Step::Hang]),
        ])
        .await?;
        let client = BlokliClient::new(server.url(), config());
        let (mut channels, health) = client.subscribe_channels_with_health(channel_selector())?;

        let first = timeout(WAIT, channels.next()).await?;
        assert!(first.is_some());
        wait_for(|| health.get().last_problem == Some(SubscriptionProblem::Lagged)).await;
        sleep(Duration::from_millis(200)).await;
        assert_eq!(health.get().status, SubscriptionStatus::Degraded);
        assert_eq!(server.connections(), 1, "the connection must not wait for the consumer");

        let replayed = timeout(WAIT, async {
            while let Some(channel) = channels.next().await {
                if channel?.concrete_channel_id == "0xff" {
                    return anyhow::Ok(true);
                }
            }
            Ok(false)
        })
        .await??;

        assert!(replayed, "the state must be replayed after the consumer caught up");
        assert_eq!(server.connections(), 2);
        assert!(matches!(health.get().status, SubscriptionStatus::Recovered { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn a_payload_of_the_wrong_shape_reconnects_instead_of_failing_the_stream() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![event(r#"{"data":{"s0":{"unexpected":true}}}"#), Step::Hang]),
            Connection::ok(vec![event(&channel_event("s0", "0x01")), Step::Hang]),
        ])
        .await?;
        let client = BlokliClient::new(
            server.url(),
            BlokliClientConfig {
                subscription_stream_restart_delay: Some(Duration::from_millis(50)),
                ..Default::default()
            },
        );
        let (mut channels, health) = client.subscribe_channels_with_health(channel_selector())?;

        let item = timeout(WAIT, channels.next()).await?;

        assert_eq!(
            item.expect("channel event")
                .expect("the broken payload must not be yielded")
                .concrete_channel_id,
            "0x01"
        );
        assert_eq!(server.connections(), 2);
        assert_eq!(health.get().last_problem, Some(SubscriptionProblem::MalformedEvent));

        Ok(())
    }

    #[tokio::test]
    async fn a_rejected_subscription_retries_on_a_connection_of_its_own() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![
                event(r#"{"data":null,"errors":[{"message":"bad account","path":["s1"]}]}"#),
                event(&channel_event("s0", "0x01")),
                Step::Hang,
            ]),
            Connection::ok(vec![event(&account_event("s1", 7)), Step::Hang]),
        ])
        .await?;
        let client = BlokliClient::new(
            server.url(),
            BlokliClientConfig {
                stream_reconnect_timeout: Duration::from_millis(200),
                subscription_stream_restart_delay: None,
                ..Default::default()
            },
        );
        let (mut channels, channels_handle) = client.subscribe_channels_with_health(channel_selector())?;
        let (mut accounts, accounts_handle) = client.subscribe_accounts_with_health(AccountSelector::Any)?;

        let (channel, rejection) = timeout(WAIT, async { tokio::join!(channels.next(), accounts.next()) }).await?;
        assert!(channel.is_some_and(|channel| channel.is_ok()));
        assert!(
            rejection.as_ref().is_some_and(|rejection| rejection.is_err()),
            "got {rejection:?}"
        );

        let account = timeout(WAIT, accounts.next()).await?;
        assert_eq!(account.expect("account event").expect("account should decode").keyid, 7);

        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[1].contains("s1: accountUpdated"),
            "unexpected request: {}",
            requests[1]
        );
        assert!(
            !requests[1].contains("channelUpdated"),
            "unexpected request: {}",
            requests[1]
        );
        assert_eq!(
            channels_handle.get().connections,
            1,
            "the other subscription must not reconnect"
        );
        assert!(channels_handle.get().is_healthy());
        assert!(matches!(
            accounts_handle.get().status,
            SubscriptionStatus::Recovered { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn a_subscription_added_while_waiting_to_retry_connects_at_once() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![event(r#"{"errors":[{"message":"unknown field"}]}"#), Step::Hang]),
            Connection::ok(vec![
                event(&channel_event("s0", "0x01")),
                event(&account_event("s1", 7)),
                Step::Hang,
            ]),
        ])
        .await?;
        // The retry after the rejection would only come after 60 s.
        let client = BlokliClient::new(
            server.url(),
            BlokliClientConfig {
                stream_reconnect_timeout: Duration::from_secs(60),
                subscription_stream_restart_delay: None,
                ..Default::default()
            },
        );

        let mut channels = client.subscribe_channels(channel_selector())?;
        let rejection = timeout(WAIT, channels.next()).await?;
        assert!(rejection.is_some_and(|rejection| rejection.is_err()));

        let mut accounts = client.subscribe_accounts(AccountSelector::Any)?;
        let (channel, account) = timeout(WAIT, async { tokio::join!(channels.next(), accounts.next()) }).await?;

        assert!(channel.is_some_and(|channel| channel.is_ok()));
        assert_eq!(account.expect("account event").expect("account should decode").keyid, 7);
        assert_eq!(server.connections(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn isolating_the_only_shared_subscription_closes_the_shared_connection() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![Connection::ok(vec![
            event(r#"{"data":null,"errors":[{"message":"bad account","path":["s0"]}]}"#),
            Step::Hang,
        ])])
        .await?;
        // The isolated retry would only come after 60 s.
        let client = BlokliClient::new(
            server.url(),
            BlokliClientConfig {
                stream_reconnect_timeout: Duration::from_secs(60),
                subscription_stream_restart_delay: None,
                ..Default::default()
            },
        );

        let mut accounts = client.subscribe_accounts(AccountSelector::Any)?;
        let rejection = timeout(WAIT, accounts.next()).await?;
        assert!(rejection.is_some_and(|rejection| rejection.is_err()));

        wait_for(|| server.open() == 0).await;

        Ok(())
    }

    #[tokio::test]
    async fn a_busy_sibling_keeps_a_quiet_subscription_on_the_same_connection_from_stalling() -> anyhow::Result<()> {
        let mut steps = Vec::new();
        for _ in 0..15 {
            steps.push(event(&channel_event("s0", "0x01")));
            steps.push(Step::Wait(Duration::from_millis(100)));
        }
        steps.push(Step::Hang);
        let server = SseTestServer::start(vec![Connection::ok(steps)]).await?;
        let client = BlokliClient::new(
            server.url(),
            BlokliClientConfig {
                subscription_stall_threshold: Some(Duration::from_millis(500)),
                subscription_stream_restart_delay: None,
                ..Default::default()
            },
        );
        let mut channels = client.subscribe_channels(channel_selector())?;
        let (mut accounts, accounts_handle) = client.subscribe_accounts_with_health(AccountSelector::Any)?;

        let received = timeout(WAIT, async {
            let busy = async {
                for _ in 0..15 {
                    channels.next().await.expect("channel event")?;
                }
                anyhow::Ok(())
            };
            tokio::select! {
                biased;
                done = busy => done,
                account = accounts.next() => anyhow::bail!("unexpected account item: {account:?}"),
            }
        })
        .await?;
        received?;

        assert_eq!(server.connections(), 1);
        assert_ne!(accounts_handle.get().status, SubscriptionStatus::Stalled);

        Ok(())
    }

    #[tokio::test]
    async fn a_payload_of_the_wrong_shape_moves_only_its_subscription_to_a_connection_of_its_own() -> anyhow::Result<()>
    {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![
                event(&channel_event("s0", "0x01")),
                event(r#"{"data":{"s1":{"unexpected":true}}}"#),
                event(&channel_event("s0", "0x02")),
                Step::Hang,
            ]),
            Connection::ok(vec![event(&account_event("s1", 7)), Step::Hang]),
        ])
        .await?;
        let client = BlokliClient::new(
            server.url(),
            BlokliClientConfig {
                subscription_stream_restart_delay: Some(Duration::from_millis(50)),
                ..Default::default()
            },
        );
        let (mut channels, channels_handle) = client.subscribe_channels_with_health(channel_selector())?;
        let (mut accounts, accounts_handle) = client.subscribe_accounts_with_health(AccountSelector::Any)?;

        let (channel_ids, account) = timeout(WAIT, async {
            let channels = async {
                let mut ids = Vec::new();
                for _ in 0..2 {
                    ids.push(channels.next().await.expect("channel event")?.concrete_channel_id);
                }
                anyhow::Ok(ids)
            };
            tokio::join!(channels, accounts.next())
        })
        .await?;

        assert_eq!(channel_ids?, ["0x01", "0x02"]);
        assert_eq!(account.expect("account event").expect("account should decode").keyid, 7);
        assert_eq!(channels_handle.get().connections, 1, "the sibling must not reconnect");
        assert!(channels_handle.get().is_healthy());
        assert_eq!(
            accounts_handle.get().last_problem,
            Some(SubscriptionProblem::MalformedEvent)
        );
        let requests = server.requests();
        assert!(
            !requests[1].contains("channelUpdated"),
            "unexpected request: {}",
            requests[1]
        );

        Ok(())
    }

    #[tokio::test]
    async fn a_frame_without_object_data_is_malformed_rather_than_rejected() -> anyhow::Result<()> {
        let server = SseTestServer::start(vec![
            Connection::ok(vec![event(r#"{"data":"bad"}"#), Step::Hang]),
            Connection::ok(vec![event(&channel_event("s0", "0x01")), Step::Hang]),
        ])
        .await?;
        let client = BlokliClient::new(
            server.url(),
            BlokliClientConfig {
                stream_reconnect_timeout: Duration::from_secs(60),
                subscription_stream_restart_delay: Some(Duration::from_millis(50)),
                ..Default::default()
            },
        );
        let (mut channels, health) = client.subscribe_channels_with_health(channel_selector())?;

        let item = timeout(WAIT, channels.next()).await?;

        assert_eq!(
            item.expect("channel event")
                .expect("the broken frame must not be yielded")
                .concrete_channel_id,
            "0x01"
        );
        assert_eq!(health.get().last_problem, Some(SubscriptionProblem::MalformedEvent));

        Ok(())
    }

    #[test]
    fn polling_outside_a_tokio_runtime_fails_the_subscription() {
        let client = BlokliClient::new("http://127.0.0.1:1".parse().expect("valid URL"), config());
        let (mut channels, health) = client
            .subscribe_channels_with_health(channel_selector())
            .expect("subscription should be created");

        let item = block_on(channels.next());

        assert!(item.is_some_and(|item| item.is_err()));
        assert_eq!(health.get().status, SubscriptionStatus::Ended(SubscriptionEnd::Failed));
    }

    #[test]
    fn ending_a_connection_spares_subscriptions_it_did_not_carry() {
        let hub = SubscriptionHub::new("http://127.0.0.1:1".parse().expect("valid URL"), config());
        let mut handles = Vec::new();
        {
            let mut state = hub.shared.lock_state();
            for id in 0..2 {
                let (health, handle) = HealthTracker::new("test");
                let (tx, _rx) = mpsc::channel(1);
                state.slots.insert(
                    id,
                    SlotEntry {
                        spec: channel_spec(),
                        health,
                        tx,
                        lagging: Arc::default(),
                        isolated: None,
                        resync: false,
                    },
                );
                handles.push(handle);
            }
        }

        hub.shared
            .end_connected(Scope::Shared, &BTreeSet::from([0]), SubscriptionEnd::Completed);

        assert_eq!(
            handles[0].get().status,
            SubscriptionStatus::Ended(SubscriptionEnd::Completed)
        );
        assert_eq!(handles[1].get().status, SubscriptionStatus::Connecting);
        assert!(hub.shared.lock_state().slots.contains_key(&1));
    }
}
