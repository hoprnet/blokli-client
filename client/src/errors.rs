use cynic::http::CynicReqwestError;

/// Error type for the Blokli client.
///
/// Most public methods return this type for transport failures, GraphQL errors, Blokli application errors, parsing
/// failures, invalid local inputs, and transaction tracking failures. Use [`BlokliClientError::kind`] to inspect the
/// stable [`ErrorKind`] category.
#[derive(Debug)]
pub struct BlokliClientError(Box<ErrorKind>);

impl BlokliClientError {
    /// Returns the reference to [`ErrorKind`].
    pub fn kind(&self) -> &ErrorKind {
        self.0.as_ref()
    }
}

impl<T: Into<ErrorKind>> From<T> for BlokliClientError {
    fn from(kind: T) -> Self {
        Self(Box::new(kind.into()))
    }
}

impl std::fmt::Display for BlokliClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for BlokliClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Error kinds for transaction tracking failure.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TrackingErrorKind {
    /// Transaction was reverted.
    Reverted,
    /// Transaction timed out in Blokli.
    Timeout,
    /// Transaction submission failed.
    SubmissionFailed,
    /// Transaction validation failed.
    ValidationFailed,
}

/// Error kinds for the Blokli client.
#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    /// Blokli returned neither data nor usable GraphQL errors.
    #[error("no data returned from blokli unexpectedly")]
    NoData,
    /// Blokli returned an application-level error through a GraphQL union result.
    #[error("remote blokli error: {kind} ({code}): {message}")]
    BlokliError {
        /// Error family assigned by the client conversion layer.
        kind: &'static str,
        /// Stable error code returned by Blokli.
        code: String,
        /// Human-readable error message returned by Blokli.
        message: String,
    },
    /// Local input was rejected before the request was sent.
    #[error("invalid query input: {0}")]
    InvalidInput(&'static str),
    /// Transaction tracking reached a terminal failure state.
    #[error("transaction tracking error: {0:?}")]
    TrackingError(TrackingErrorKind),
    /// Blokli returned data in a shape or encoding the client could not parse.
    #[error("data returned from blokli was unparseable")]
    ParseError,
    /// A client-side timeout elapsed.
    #[error("operation timed out at the client")]
    Timeout,
    /// SSE subscription setup or transport failed.
    #[error(transparent)]
    Subscription(#[from] Box<eventsource_client::Error>),
    /// A URL could not be parsed or derived.
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
    /// JSON serialization or deserialization failed.
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    /// HTTP transport failed.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    /// Cynic request/response handling failed.
    #[error(transparent)]
    Cynic(#[from] CynicReqwestError),
    /// GraphQL returned errors without usable data.
    #[error(transparent)]
    GraphQLError(#[from] cynic::GraphQlError),
    #[cfg(feature = "testing")]
    #[error(transparent)]
    MockClientError(#[from] anyhow::Error),
}

/// A special kind of error type that is used to wrap errors simulates internal Safe TX failures.
#[cfg(feature = "testing")]
#[derive(Debug)]
pub struct InternalTxError(pub anyhow::Error);

#[cfg(feature = "testing")]
impl std::fmt::Display for InternalTxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "internal TX error: {}", self.0)
    }
}

#[cfg(feature = "testing")]
impl std::error::Error for InternalTxError {}
