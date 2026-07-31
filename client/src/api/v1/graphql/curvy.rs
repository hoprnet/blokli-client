use super::schema;

/// Exclusive Curvy event cursor encoded as `block:transaction:log:item`.
#[derive(cynic::Scalar, Clone, Debug, Eq, PartialEq)]
pub struct CurvyEventCursor(pub String);

/// Raw Curvy note event kind accepted by subscription filters.
#[derive(cynic::Enum, Clone, Copy, Debug, Eq, PartialEq)]
pub enum CurvyNoteEventKind {
    #[cynic(rename = "PENDING")]
    Pending,
    #[cynic(rename = "COMMITTED")]
    Committed,
}

/// Filter over raw indexed Curvy note fields.
#[derive(cynic::InputObject, Clone, Debug, Default, Eq, PartialEq)]
pub struct CurvyNoteEventFilter {
    /// Restrict delivery by raw event kind.
    pub kinds: Option<Vec<CurvyNoteEventKind>>,
    /// Restrict delivery to known decimal `uint256` note identifiers.
    pub note_ids: Option<Vec<String>>,
}

/// Variables for the resumable Curvy note subscription.
#[derive(cynic::QueryVariables, Clone, Debug, Default)]
pub struct CurvyNoteEventVariables {
    pub after: Option<CurvyEventCursor>,
    pub filter: Option<CurvyNoteEventFilter>,
}

/// GraphQL operation for the unified Curvy note event stream.
#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "CurvyNoteEventVariables")]
pub struct SubscribeCurvyNoteEvents {
    #[arguments(after: $after, filter: $filter)]
    pub curvy_note_events: CurvyNoteEvent,
}

/// One raw Curvy note item.
#[derive(cynic::InlineFragments, Clone, Debug, Eq, PartialEq)]
#[cynic(graphql_type = "CurvyNoteEvent")]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub enum CurvyNoteEvent {
    CurvyPendingNote(CurvyPendingNote),
    CurvyCommittedNote(CurvyCommittedNote),
    #[cynic(fallback)]
    Unknown,
}

/// One item from a Curvy `PendingNotes` event.
#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyPendingNote {
    pub cursor: CurvyEventCursor,
    pub note_id: String,
    pub ephemeral_key: Vec<String>,
    pub view_tag: i32,
    pub token: String,
    pub amount: String,
    pub is_plaintext: bool,
}

/// One item from a Curvy `CommittedNotes` event.
#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyCommittedNote {
    pub cursor: CurvyEventCursor,
    pub note_id: String,
    pub batch_index: String,
}
