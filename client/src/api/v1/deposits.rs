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
    kind: DepositEventKind,
    deposit_note_ids: Option<Vec<String>>,
}

impl DepositEventFilter {
    /// Selects candidates whose ownership metadata must be inspected locally.
    pub fn detection_candidates() -> Self {
        Self {
            kind: DepositEventKind::DetectionCandidate,
            deposit_note_ids: None,
        }
    }

    /// Selects completion events for note IDs already correlated with local deposits.
    pub fn completions(deposit_note_ids: Vec<String>) -> Self {
        Self {
            kind: DepositEventKind::Completed,
            deposit_note_ids: Some(deposit_note_ids),
        }
    }
}

impl From<DepositEventFilter> for CurvyNoteEventFilter {
    fn from(filter: DepositEventFilter) -> Self {
        Self {
            kinds: Some(vec![match filter.kind {
                DepositEventKind::DetectionCandidate => CurvyNoteEventKind::Pending,
                DepositEventKind::Completed => CurvyNoteEventKind::Committed,
            }]),
            note_ids: filter.deposit_note_ids,
        }
    }
}

/// Strategy-oriented deposit lifecycle event.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub enum DepositEvent {
    /// Contains the metadata needed to check whether a deposit belongs to this node.
    DetectionCandidate(DepositDetectionCandidate),
    /// Indicates that a locally correlated deposit note is committed and spendable.
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
            CurvyNoteEvent::Unknown => None,
        }
    }
}

/// Metadata used by the node to determine whether a detected note belongs to one of its BJJ deposit addresses.
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

/// A locally correlated deposit note that is committed and spendable.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DepositCompletion {
    pub cursor: DepositEventCursor,
    pub deposit_note_id: String,
    pub batch_index: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trip() {
        let cursor = DepositEventCursor::new(10, 2, 7, 3);
        assert_eq!(cursor.components(), Some((10, 2, 7, 3)));
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
}
