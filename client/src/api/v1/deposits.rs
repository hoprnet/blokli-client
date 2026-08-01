//! Translation from raw Curvy note events into the connector-facing deposit lifecycle vocabulary.

use super::graphql::curvy::{
    CurvyEventCursor, CurvyNoteEvent, CurvyNoteEventFilter, CurvyNoteEventKind, CurvyPendingNote,
};

/// Exclusive deposit event cursor encoded as `block:transaction:log:item`.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DepositEventCursor(pub String);

impl DepositEventCursor {
    /// Builds a cursor from its indexed position components.
    pub fn new(block: u64, transaction_index: u64, log_index: u64, item_index: u32) -> Self {
        Self(format!("{block}:{transaction_index}:{log_index}:{item_index}"))
    }

    /// Parses the cursor into `(block, transaction index, log index, item index)`.
    pub fn components(&self) -> Option<(u64, u64, u64, u32)> {
        let mut parts = self.0.split(':');
        let components = (
            parts.next()?.parse().ok()?,
            parts.next()?.parse().ok()?,
            parts.next()?.parse().ok()?,
            parts.next()?.parse().ok()?,
        );
        parts.next().is_none().then_some(components)
    }
}

impl From<DepositEventCursor> for CurvyEventCursor {
    fn from(cursor: DepositEventCursor) -> Self {
        Self(cursor.0)
    }
}

impl From<CurvyEventCursor> for DepositEventCursor {
    fn from(cursor: CurvyEventCursor) -> Self {
        Self(cursor.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DepositEventKind {
    DetectionCandidate,
    Completed,
}

/// Filter for deposit lifecycle events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DepositEventFilter {
    kinds: Vec<DepositEventKind>,
    deposit_note_ids: Option<Vec<String>>,
}

impl DepositEventFilter {
    /// Selects the complete deposit lifecycle for ownership detection and correlation by `hopr-chain-connector`.
    pub fn lifecycle() -> Self {
        Self {
            kinds: vec![DepositEventKind::DetectionCandidate, DepositEventKind::Completed],
            deposit_note_ids: None,
        }
    }

    /// Selects candidates whose ownership metadata must be inspected locally by `hopr-chain-connector`.
    pub fn detection_candidates() -> Self {
        Self {
            kinds: vec![DepositEventKind::DetectionCandidate],
            deposit_note_ids: None,
        }
    }

    /// Selects completion events for note IDs already correlated by `hopr-chain-connector`.
    pub fn completions(deposit_note_ids: Vec<String>) -> Self {
        Self {
            kinds: vec![DepositEventKind::Completed],
            deposit_note_ids: Some(deposit_note_ids),
        }
    }

    pub(crate) fn event_kind_count(&self) -> usize {
        self.kinds.len()
    }

    pub(crate) fn deposit_note_id_count(&self) -> usize {
        self.deposit_note_ids.as_ref().map_or(0, Vec::len)
    }
}

impl From<DepositEventFilter> for CurvyNoteEventFilter {
    fn from(filter: DepositEventFilter) -> Self {
        Self {
            kinds: Some(
                filter
                    .kinds
                    .into_iter()
                    .map(|kind| match kind {
                        DepositEventKind::DetectionCandidate => CurvyNoteEventKind::Pending,
                        DepositEventKind::Completed => CurvyNoteEventKind::Committed,
                    })
                    .collect(),
            ),
            note_ids: filter.deposit_note_ids,
        }
    }
}

/// Connector-facing deposit lifecycle event translated from Blokli's raw Curvy terminology.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub enum DepositEvent {
    /// Contains the metadata the connector needs to check whether a deposit belongs to this node.
    DetectionCandidate(DepositDetectionCandidate),
    /// Indicates that a note is committed and spendable; the connector must correlate it before notifying PIX.
    Completed(DepositCompletion),
}

impl DepositEvent {
    /// Returns the exclusive cursor used to resume after this event.
    pub fn cursor(&self) -> &DepositEventCursor {
        match self {
            Self::DetectionCandidate(candidate) => &candidate.cursor,
            Self::Completed(completion) => &completion.cursor,
        }
    }

    /// Returns the note identifier used for local deposit correlation.
    pub fn deposit_note_id(&self) -> &str {
        match self {
            Self::DetectionCandidate(candidate) => &candidate.deposit_note_id,
            Self::Completed(completion) => &completion.deposit_note_id,
        }
    }

    pub(crate) fn from_graphql(event: CurvyNoteEvent) -> Option<Self> {
        match event {
            CurvyNoteEvent::CurvyPendingNote(note) => Some(Self::DetectionCandidate(note.into())),
            CurvyNoteEvent::CurvyCommittedNote(note) => Some(Self::Completed(DepositCompletion {
                cursor: note.cursor.into(),
                deposit_note_id: note.note_id,
                batch_index: note.batch_index,
            })),
            CurvyNoteEvent::Unknown => {
                tracing::warn!("dropping unknown Curvy note event variant; client schema may be outdated");
                None
            }
        }
    }
}

/// Metadata used by `hopr-chain-connector` to determine whether a note belongs to one of the node's BJJ addresses.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DepositDetectionCandidate {
    pub cursor: DepositEventCursor,
    pub deposit_note_id: String,
    pub ephemeral_key: Vec<String>,
    pub view_tag: i32,
    pub token: String,
    pub amount: String,
    pub is_plaintext: bool,
}

impl From<CurvyPendingNote> for DepositDetectionCandidate {
    fn from(note: CurvyPendingNote) -> Self {
        Self {
            cursor: note.cursor.into(),
            deposit_note_id: note.note_id,
            ephemeral_key: note.ephemeral_key,
            view_tag: note.view_tag,
            token: note.token,
            amount: note.amount,
            is_plaintext: note.is_plaintext,
        }
    }
}

/// A committed note that `hopr-chain-connector` can correlate with its locally owned deposit notes.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DepositCompletion {
    pub cursor: DepositEventCursor,
    pub deposit_note_id: String,
    pub batch_index: String,
}

#[cfg(test)]
mod tests {
    use super::{
        super::graphql::curvy::CurvyCommittedNote, CurvyEventCursor, CurvyNoteEvent, CurvyNoteEventFilter,
        CurvyNoteEventKind, CurvyPendingNote, DepositCompletion, DepositDetectionCandidate, DepositEvent,
        DepositEventCursor, DepositEventFilter,
    };

    #[test]
    fn cursor_round_trip() {
        let cursor = DepositEventCursor::new(10, 2, 7, 3);
        assert_eq!(cursor.components(), Some((10, 2, 7, 3)));
    }

    #[test]
    fn cursor_rejects_malformed_components() {
        for value in ["", "10:2:7", "10:2:7:3:1", "ten:2:7:3", "10:2:7:-1"] {
            assert_eq!(DepositEventCursor(value.to_string()).components(), None);
        }
    }

    #[test]
    fn cursor_converts_to_and_from_graphql_cursor() {
        let graphql_cursor = CurvyEventCursor::from(DepositEventCursor("10:2:7:3".to_string()));
        assert_eq!(graphql_cursor, CurvyEventCursor("10:2:7:3".to_string()));
        assert_eq!(
            DepositEventCursor::from(graphql_cursor),
            DepositEventCursor("10:2:7:3".to_string())
        );
    }

    #[test]
    fn filter_maps_strategy_stages_to_raw_event_kinds() {
        let raw = CurvyNoteEventFilter::from(DepositEventFilter::completions(vec!["42".to_string()]));

        assert_eq!(
            raw,
            CurvyNoteEventFilter {
                kinds: Some(vec![CurvyNoteEventKind::Committed]),
                note_ids: Some(vec!["42".to_string()]),
            }
        );
    }

    #[test]
    fn lifecycle_filter_requests_both_raw_event_kinds() {
        let raw = CurvyNoteEventFilter::from(DepositEventFilter::lifecycle());

        assert_eq!(
            raw,
            CurvyNoteEventFilter {
                kinds: Some(vec![CurvyNoteEventKind::Pending, CurvyNoteEventKind::Committed]),
                note_ids: None,
            }
        );
    }

    #[test]
    fn detection_candidate_filter_requests_pending_events() {
        let raw = CurvyNoteEventFilter::from(DepositEventFilter::detection_candidates());

        assert_eq!(
            raw,
            CurvyNoteEventFilter {
                kinds: Some(vec![CurvyNoteEventKind::Pending]),
                note_ids: None,
            }
        );
    }

    #[test]
    fn filter_summary_does_not_expose_note_ids() {
        let filter = DepositEventFilter::completions(vec!["42".to_string(), "43".to_string()]);

        assert_eq!(filter.event_kind_count(), 1);
        assert_eq!(filter.deposit_note_id_count(), 2);
    }

    #[test]
    fn pending_note_maps_to_detection_candidate() {
        let event = DepositEvent::from_graphql(CurvyNoteEvent::CurvyPendingNote(CurvyPendingNote {
            cursor: CurvyEventCursor("10:2:7:3".to_string()),
            note_id: "42".to_string(),
            ephemeral_key: vec!["11".to_string(), "12".to_string()],
            view_tag: 7,
            token: "1".to_string(),
            amount: "1000".to_string(),
            is_plaintext: false,
        }))
        .expect("pending note should map to a deposit event");

        assert_eq!(event.cursor(), &DepositEventCursor("10:2:7:3".to_string()));
        assert_eq!(event.deposit_note_id(), "42");
        assert_eq!(
            event,
            DepositEvent::DetectionCandidate(DepositDetectionCandidate {
                cursor: DepositEventCursor("10:2:7:3".to_string()),
                deposit_note_id: "42".to_string(),
                ephemeral_key: vec!["11".to_string(), "12".to_string()],
                view_tag: 7,
                token: "1".to_string(),
                amount: "1000".to_string(),
                is_plaintext: false,
            })
        );
    }

    #[test]
    fn committed_note_maps_to_completion() {
        let event = DepositEvent::from_graphql(CurvyNoteEvent::CurvyCommittedNote(CurvyCommittedNote {
            cursor: CurvyEventCursor("11:0:1:0".to_string()),
            note_id: "42".to_string(),
            batch_index: "9".to_string(),
        }))
        .expect("committed note should map to a deposit event");

        assert_eq!(event.cursor(), &DepositEventCursor("11:0:1:0".to_string()));
        assert_eq!(event.deposit_note_id(), "42");
        assert_eq!(
            event,
            DepositEvent::Completed(DepositCompletion {
                cursor: DepositEventCursor("11:0:1:0".to_string()),
                deposit_note_id: "42".to_string(),
                batch_index: "9".to_string(),
            })
        );
    }

    #[test]
    fn unknown_graphql_event_is_ignored() {
        assert_eq!(DepositEvent::from_graphql(CurvyNoteEvent::Unknown), None);
    }
}
