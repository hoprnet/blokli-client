//! Health of individual SSE subscriptions.
//!
//! Every subscription opened through a `subscribe_*_with_health` method of
//! [`BlokliSubscriptionClient`](crate::BlokliSubscriptionClient) comes with a [`SubscriptionHealthHandle`]. The
//! handle reports the state of that one subscription, separately from the data it delivers:
//!
//! - **Transport liveness**: whether the connection to Blokli works. Any traffic counts, including the keepalive
//!   comments Blokli sends while there is nothing new. A subscription that only receives keepalives is healthy.
//! - **Data freshness**: when the last decoded update arrived. Having no updates is normal for a quiet chain, so it
//!   never makes a subscription unhealthy; only the caller can tell whether its data is too old.
//!
//! The handle is a cheap shared snapshot, not a channel. Read it with [`SubscriptionHealthHandle::get`] when the
//! state is needed, for example on a timer. The state is updated while the subscription stream is being polled; a
//! stream that is not polled does not read from its connection either, and its health keeps the last known value.
//!
//! Retries are not limited: a degraded subscription keeps reconnecting until it recovers or its stream is dropped.

use std::{
    fmt::{self, Display, Formatter},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Source of [`SubscriptionId::id`] values, unique within the process.
static NEXT_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);

/// Identifies one subscription: what it subscribes to, and which instance it is.
///
/// The name says what the subscription is for, such as `"accounts"`. The id is unique within the process, so two
/// subscriptions of the same kind can be told apart in health reports and logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId {
    name: &'static str,
    id: u64,
}

impl SubscriptionId {
    pub(crate) fn new(name: &'static str) -> Self {
        Self {
            name,
            id: NEXT_SUBSCRIPTION_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// What the subscription is for, such as `"accounts"` or `"graph"`.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Process-wide unique number of this subscription.
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Display for SubscriptionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.name, self.id)
    }
}

/// Where a subscription is in its life.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionStatus {
    /// The first connection is not established yet.
    Connecting,
    /// Connected, and nothing has gone wrong so far.
    Healthy,
    /// The connection was lost or delivered unusable data. The subscription is reconnecting.
    Degraded,
    /// Nothing at all has come over the connection for at least the
    /// [stall threshold](crate::BlokliClientConfig::subscription_stall_threshold), not even a keepalive comment.
    ///
    /// The subscription keeps reconnecting; this status only says that the problem has lasted long.
    Stalled,
    /// The server refused the subscription: an HTTP 4xx answer (other than 408, 425 or 429) or GraphQL errors
    /// without data.
    ///
    /// The error was yielded on the stream once. The subscription keeps retrying at the slow pace of
    /// [`stream_reconnect_timeout`](crate::BlokliClientConfig::stream_reconnect_timeout), so it recovers without a
    /// restart once the server accepts it again.
    Rejected,
    /// Connected again after being [`Degraded`](Self::Degraded), [`Stalled`](Self::Stalled) or
    /// [`Rejected`](Self::Rejected).
    ///
    /// The server replays its current snapshot after every reconnect, so the stream yields items that were already
    /// seen before the outage. Apply them idempotently.
    Recovered {
        /// How long the subscription was not working.
        outage: Duration,
    },
    /// The subscription stream is over and will not reconnect.
    Ended(SubscriptionEnd),
    /// The client does not track health, such as the in-memory test client. Nothing is known to be wrong.
    Untracked,
}

/// Why a subscription ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionEnd {
    /// The server closed the stream and restarts are turned off.
    Completed,
    /// The caller dropped the stream.
    Cancelled,
    /// An error that cannot be retried was yielded on the stream, and the stream ended after it.
    Failed,
}

/// What went wrong most recently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionProblem {
    /// The connection could not be made or broke down.
    Transport,
    /// No data arrived within the read timeout.
    Timeout,
    /// The server answered with an error status.
    HttpStatus(u16),
    /// The server ended the response.
    Closed,
    /// The server sent data that is not a valid event or not a valid response.
    MalformedEvent,
    /// The server answered with GraphQL errors and no data.
    GraphQl,
}

/// Snapshot of one subscription's health.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionHealth {
    /// Current status.
    pub status: SubscriptionStatus,
    /// When the subscription entered the current status.
    pub status_since: Instant,
    /// When the connection was last seen working: a new connection, a keepalive comment, or an event.
    pub last_transport_activity: Option<Instant>,
    /// When the last update was decoded and yielded. This is about data freshness only, not about health.
    pub last_update: Option<Instant>,
    /// Number of problems since the subscription was last working. Zero while it works.
    pub failures: u32,
    /// The most recent problem, kept after recovery.
    pub last_problem: Option<SubscriptionProblem>,
}

impl SubscriptionHealth {
    fn new(status: SubscriptionStatus) -> Self {
        Self {
            status,
            status_since: Instant::now(),
            last_transport_activity: None,
            last_update: None,
            failures: 0,
            last_problem: None,
        }
    }

    /// Whether nothing is known to be wrong: [`Healthy`](SubscriptionStatus::Healthy),
    /// [`Recovered`](SubscriptionStatus::Recovered), or [`Untracked`](SubscriptionStatus::Untracked).
    pub fn is_healthy(&self) -> bool {
        matches!(
            self.status,
            SubscriptionStatus::Healthy | SubscriptionStatus::Recovered { .. } | SubscriptionStatus::Untracked
        )
    }
}

/// Read access to the health of one subscription.
///
/// Cloning the handle is cheap; all clones see the same state.
#[derive(Clone, Debug)]
pub struct SubscriptionHealthHandle {
    subscription: SubscriptionId,
    state: Arc<Mutex<SubscriptionHealth>>,
}

impl SubscriptionHealthHandle {
    /// A handle for a client that does not track health. It always reports
    /// [`Untracked`](SubscriptionStatus::Untracked).
    pub fn untracked(name: &'static str) -> Self {
        Self {
            subscription: SubscriptionId::new(name),
            state: Arc::new(Mutex::new(SubscriptionHealth::new(SubscriptionStatus::Untracked))),
        }
    }

    /// Which subscription this handle reports on.
    pub fn subscription(&self) -> SubscriptionId {
        self.subscription
    }

    /// The latest health of the subscription.
    pub fn get(&self) -> SubscriptionHealth {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

/// Write side of a subscription's health, owned by the subscription stream.
///
/// Dropping it before the subscription ended on its own marks the subscription as
/// [`Cancelled`](SubscriptionEnd::Cancelled), because that only happens when the caller drops the stream.
#[derive(Debug)]
pub(crate) struct HealthTracker {
    state: Arc<Mutex<SubscriptionHealth>>,
    /// When the subscription was created; stalls are measured from here until the transport first works.
    created: Instant,
    /// Start of the current outage. For a subscription that never connected, its creation.
    outage_since: Instant,
}

impl HealthTracker {
    pub(crate) fn new(name: &'static str) -> (Self, SubscriptionHealthHandle) {
        let health = SubscriptionHealth::new(SubscriptionStatus::Connecting);
        let outage_since = health.status_since;
        let subscription = SubscriptionId::new(name);
        let state = Arc::new(Mutex::new(health));
        let handle = SubscriptionHealthHandle {
            subscription,
            state: state.clone(),
        };
        (
            Self {
                state,
                created: outage_since,
                outage_since,
            },
            handle,
        )
    }

    /// The connection delivered a keepalive or an event.
    pub(crate) fn transport_activity(&mut self) {
        self.record_activity(true);
    }

    /// A connection was (re)established.
    ///
    /// This does not end a rejection: a server that refuses the query with GraphQL errors accepts the connection
    /// first.
    pub(crate) fn connected(&mut self) {
        self.record_activity(false);
    }

    fn record_activity(&mut self, ends_rejection: bool) {
        let outage_since = self.outage_since;
        self.update(|health, now| {
            health.last_transport_activity = Some(now);
            let next = match health.status {
                SubscriptionStatus::Connecting => SubscriptionStatus::Healthy,
                SubscriptionStatus::Degraded | SubscriptionStatus::Stalled => SubscriptionStatus::Recovered {
                    outage: now.saturating_duration_since(outage_since),
                },
                SubscriptionStatus::Rejected if ends_rejection => SubscriptionStatus::Recovered {
                    outage: now.saturating_duration_since(outage_since),
                },
                _ => return,
            };
            health.failures = 0;
            set_status(health, next, now);
        });
    }

    /// An update was decoded and is about to be yielded.
    pub(crate) fn data_update(&mut self) {
        self.transport_activity();
        self.update(|health, now| health.last_update = Some(now));
    }

    /// Something went wrong; the subscription keeps reconnecting.
    pub(crate) fn problem(&mut self, problem: SubscriptionProblem) {
        let mut outage_started = None;
        self.update(|health, now| {
            health.failures = health.failures.saturating_add(1);
            health.last_problem = Some(problem);
            match health.status {
                SubscriptionStatus::Healthy | SubscriptionStatus::Recovered { .. } => {
                    outage_started = Some(now);
                    set_status(health, SubscriptionStatus::Degraded, now);
                }
                // A subscription that never connected has been out since it was created.
                SubscriptionStatus::Connecting => set_status(health, SubscriptionStatus::Degraded, now),
                _ => {}
            }
        });
        if let Some(since) = outage_started {
            self.outage_since = since;
        }
    }

    /// The server refused the subscription. Returns whether this starts a new rejection, so the caller yields the
    /// error only once per rejection.
    pub(crate) fn reject(&mut self, problem: SubscriptionProblem) -> bool {
        let mut first = false;
        let mut outage_started = None;
        self.update(|health, now| {
            health.failures = health.failures.saturating_add(1);
            health.last_problem = Some(problem);
            match health.status {
                SubscriptionStatus::Rejected | SubscriptionStatus::Ended(_) => return,
                SubscriptionStatus::Healthy | SubscriptionStatus::Recovered { .. } => outage_started = Some(now),
                _ => {}
            }
            first = true;
            set_status(health, SubscriptionStatus::Rejected, now);
        });
        if let Some(since) = outage_started {
            self.outage_since = since;
        }
        first
    }

    /// When the subscription counts as stalled if nothing comes over the connection until then.
    ///
    /// `None` when it is already stalled or has ended, so there is nothing left to wait for.
    pub(crate) fn stall_deadline(&self, threshold: Duration) -> Option<Instant> {
        let health = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match health.status {
            SubscriptionStatus::Stalled | SubscriptionStatus::Rejected | SubscriptionStatus::Ended(_) => None,
            _ => Some(health.last_transport_activity.unwrap_or(self.created) + threshold),
        }
    }

    /// Nothing has come over the connection for the stall threshold.
    pub(crate) fn stalled(&mut self) {
        let created = self.created;
        let mut outage_started = None;
        self.update(|health, now| {
            match health.status {
                // A silent connection: the outage began with the last sign of life.
                SubscriptionStatus::Connecting | SubscriptionStatus::Healthy | SubscriptionStatus::Recovered { .. } => {
                    outage_started = Some(health.last_transport_activity.unwrap_or(created));
                }
                SubscriptionStatus::Degraded => {}
                _ => return,
            }
            set_status(health, SubscriptionStatus::Stalled, now);
        });
        if let Some(since) = outage_started {
            self.outage_since = since;
        }
    }

    /// The subscription is over.
    pub(crate) fn end(&mut self, end: SubscriptionEnd) {
        self.update(|health, now| {
            if !matches!(health.status, SubscriptionStatus::Ended(_)) {
                set_status(health, SubscriptionStatus::Ended(end), now);
            }
        });
    }

    fn update(&self, change: impl FnOnce(&mut SubscriptionHealth, Instant)) {
        let mut health = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        change(&mut health, Instant::now());
    }
}

impl Drop for HealthTracker {
    fn drop(&mut self) {
        self.end(SubscriptionEnd::Cancelled);
    }
}

fn set_status(health: &mut SubscriptionHealth, status: SubscriptionStatus, now: Instant) {
    health.status = status;
    health.status_since = now;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{HealthTracker, SubscriptionEnd, SubscriptionHealthHandle, SubscriptionProblem, SubscriptionStatus};

    #[test]
    fn a_new_subscription_is_connecting_until_the_transport_works() {
        let (mut tracker, handle) = HealthTracker::new("accounts");
        assert_eq!(handle.get().status, SubscriptionStatus::Connecting);

        tracker.transport_activity();

        let health = handle.get();
        assert_eq!(health.status, SubscriptionStatus::Healthy);
        assert!(health.last_transport_activity.is_some());
        assert_eq!(health.last_update, None, "keepalives are not data");
    }

    #[test]
    fn problems_degrade_and_activity_recovers_with_the_outage_length() {
        let (mut tracker, handle) = HealthTracker::new("graph");
        tracker.transport_activity();

        tracker.problem(SubscriptionProblem::Closed);
        tracker.problem(SubscriptionProblem::HttpStatus(503));
        let degraded = handle.get();
        assert_eq!(degraded.status, SubscriptionStatus::Degraded);
        assert_eq!(degraded.failures, 2);

        tracker.transport_activity();

        let recovered = handle.get();
        assert!(matches!(recovered.status, SubscriptionStatus::Recovered { .. }));
        assert_eq!(recovered.failures, 0);
        assert_eq!(recovered.last_problem, Some(SubscriptionProblem::HttpStatus(503)));
    }

    #[test]
    fn a_silent_connection_stalls_and_recovers_with_the_outage_since_its_last_activity() {
        let (mut tracker, handle) = HealthTracker::new("accounts");
        tracker.transport_activity();
        let last_activity = handle.get().last_transport_activity.expect("activity was recorded");

        let threshold = Duration::from_secs(90);
        assert_eq!(tracker.stall_deadline(threshold), Some(last_activity + threshold));
        tracker.stalled();

        let stalled = handle.get();
        assert_eq!(stalled.status, SubscriptionStatus::Stalled);
        assert!(!stalled.is_healthy());
        assert_eq!(tracker.stall_deadline(threshold), None, "a stall is reported once");

        tracker.problem(SubscriptionProblem::Timeout);
        assert_eq!(
            handle.get().status,
            SubscriptionStatus::Stalled,
            "errors do not undo the stall"
        );

        tracker.transport_activity();
        let recovered = handle.get();
        let SubscriptionStatus::Recovered { outage } = recovered.status else {
            panic!("expected Recovered, got {:?}", recovered.status);
        };
        assert_eq!(outage, recovered.status_since.saturating_duration_since(last_activity));
    }

    #[test]
    fn a_subscription_that_never_connected_stalls_from_its_creation() {
        let (tracker, handle) = HealthTracker::new("graph");
        let created = handle.get().status_since;
        let threshold = Duration::from_millis(10);

        assert_eq!(tracker.stall_deadline(threshold), Some(created + threshold));
    }

    #[test]
    fn a_rejection_is_reported_once_and_only_traffic_after_connecting_ends_it() {
        let (mut tracker, handle) = HealthTracker::new("services");
        tracker.connected();

        assert!(
            tracker.reject(SubscriptionProblem::GraphQl),
            "the first rejection is new"
        );
        assert!(
            !tracker.reject(SubscriptionProblem::GraphQl),
            "a repeated rejection is not"
        );
        assert_eq!(handle.get().status, SubscriptionStatus::Rejected);
        assert_eq!(handle.get().failures, 2);
        assert!(!handle.get().is_healthy());
        assert_eq!(tracker.stall_deadline(Duration::from_secs(90)), None);

        tracker.problem(SubscriptionProblem::Closed);
        tracker.stalled();
        tracker.connected();
        assert_eq!(
            handle.get().status,
            SubscriptionStatus::Rejected,
            "connecting alone does not end it"
        );

        tracker.transport_activity();
        assert!(matches!(handle.get().status, SubscriptionStatus::Recovered { .. }));
        assert!(
            tracker.reject(SubscriptionProblem::HttpStatus(401)),
            "a rejection after recovery is new again"
        );
    }

    #[test]
    fn dropping_the_tracker_cancels_but_does_not_override_an_earlier_end() {
        let (tracker, cancelled) = HealthTracker::new("health");
        drop(tracker);
        assert_eq!(
            cancelled.get().status,
            SubscriptionStatus::Ended(SubscriptionEnd::Cancelled)
        );

        let (mut tracker, failed) = HealthTracker::new("health");
        tracker.end(SubscriptionEnd::Failed);
        drop(tracker);
        assert_eq!(failed.get().status, SubscriptionStatus::Ended(SubscriptionEnd::Failed));
    }

    #[test]
    fn subscriptions_of_the_same_kind_get_different_ids() {
        let (_first_tracker, first) = HealthTracker::new("channels");
        let (_second_tracker, second) = HealthTracker::new("channels");
        assert_ne!(first.subscription(), second.subscription());
        assert_eq!(first.subscription().name(), "channels");
    }

    #[test]
    fn an_untracked_handle_reports_nothing_wrong() {
        let health = SubscriptionHealthHandle::untracked("services").get();
        assert_eq!(health.status, SubscriptionStatus::Untracked);
        assert!(health.is_healthy());
    }
}
