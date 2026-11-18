use std::sync::Arc;

use cynic::http::CynicReqwestError;

use crate::api::types::TransactionDiagnosis;

/// Error type for the Blokli client.
///
/// Most public methods return this type for transport failures, GraphQL errors, Blokli application errors, parsing
/// failures, invalid local inputs, and transaction tracking failures. Use [`BlokliClientError::kind`] to inspect the
/// stable [`ErrorKind`] category.
///
/// Cloning an error is cheap and shares the underlying [`ErrorKind`], which lets one connection-level failure be
/// reported to every subscription stream that depends on it.
#[derive(Clone, Debug)]
pub struct BlokliClientError {
    kind: Arc<ErrorKind>,
    unmined_diagnosis: Option<Arc<TransactionDiagnosis>>,
}

impl BlokliClientError {
    /// Returns the reference to [`ErrorKind`].
    pub fn kind(&self) -> &ErrorKind {
        self.kind.as_ref()
    }

    /// Returns Blokli's explanation of why a timed out transaction was not mined.
    ///
    /// Only a [`TrackingErrorKind::Timeout`] error returned by transaction tracking can carry one, and only when
    /// the Blokli server knows the `diagnosis` field (older servers do not) and the follow-up query succeeded. Every
    /// other error returns `None`.
    pub fn unmined_diagnosis(&self) -> Option<&TransactionDiagnosis> {
        self.unmined_diagnosis.as_deref()
    }

    pub(crate) fn with_unmined_diagnosis(mut self, diagnosis: TransactionDiagnosis) -> Self {
        self.unmined_diagnosis = Some(Arc::new(diagnosis));
        self
    }

    /// Returns the typed refusal code for RPC and capacity refusals.
    ///
    /// Only [`ErrorKind::BlokliError`] values produced by Blokli's RPC error and overload results carry one; every
    /// other kind returns `None`. Codes this client does not know yet are kept as [`RefusalCode::Unknown`].
    pub fn refusal_code(&self) -> Option<RefusalCode> {
        match self.kind() {
            ErrorKind::BlokliError { kind, code, .. } if matches!(*kind, "rpc error" | "overloaded") => {
                Some(RefusalCode::from(code.as_str()))
            }
            _ => None,
        }
    }

    /// Returns the typed `reason` of a HOPR action rejection or throttle, `None` for every other kind.
    pub fn hopr_reason(&self) -> Option<HoprRefusalReason> {
        self.kind().reason_kind()
    }
}

impl ErrorKind {
    /// Returns the typed `reason` of [`ErrorKind::HoprActionRejected`] and [`ErrorKind::HoprActionThrottled`].
    ///
    /// The `reason` string field stays available; reasons this client does not know are kept as
    /// [`HoprRefusalReason::Unknown`].
    pub fn reason_kind(&self) -> Option<HoprRefusalReason> {
        match self {
            ErrorKind::HoprActionRejected { reason, .. } | ErrorKind::HoprActionThrottled { reason, .. } => {
                Some(HoprRefusalReason::from(reason.as_str()))
            }
            _ => None,
        }
    }
}

/// Stable code Blokli attaches to an RPC or capacity refusal of a submitted transaction.
///
/// Codes added by newer Blokli versions deserialise to [`RefusalCode::Unknown`] with the original string, so
/// matching on this enum never fails on an unseen code.
#[derive(Clone, Debug, PartialEq, Eq, strum::EnumString, strum::IntoStaticStr)]
#[non_exhaustive]
pub enum RefusalCode {
    /// The node already knows this transaction.
    #[strum(serialize = "ALREADY_KNOWN")]
    AlreadyKnown,
    /// A transaction with this nonce exists and the replacement fee is too low.
    #[strum(serialize = "REPLACEMENT_UNDERPRICED")]
    ReplacementUnderpriced,
    /// The nonce was already used.
    #[strum(serialize = "NONCE_TOO_LOW")]
    NonceTooLow,
    /// The nonce is ahead of the account's next nonce.
    #[strum(serialize = "NONCE_TOO_HIGH")]
    NonceTooHigh,
    /// The fee is below what the node accepts.
    #[strum(serialize = "FEE_TOO_LOW")]
    FeeTooLow,
    /// The signer cannot pay for the transaction.
    #[strum(serialize = "INSUFFICIENT_FUNDS")]
    InsufficientFunds,
    /// Any other RPC failure.
    #[strum(serialize = "RPC_ERROR")]
    RpcError,
    /// Blokli has no capacity to track another transaction; retry later.
    #[strum(serialize = "SUBMISSION_CAPACITY_EXCEEDED")]
    SubmissionCapacityExceeded,
    /// A code this client version does not know, with the original string.
    #[strum(default)]
    Unknown(String),
}

impl RefusalCode {
    /// The code string as sent by Blokli.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown(code) => code,
            other => other.into(),
        }
    }
}

/// Stable `reason` Blokli attaches to a HOPR action rejection or throttle.
///
/// No reason is enumerated yet: every value is [`HoprRefusalReason::Unknown`] with the original string. Variants
/// will be added as Blokli publishes the list.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HoprRefusalReason {
    /// A reason this client version does not know, with the original string.
    Unknown(String),
}

impl HoprRefusalReason {
    /// The reason string as sent by Blokli.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown(reason) => reason,
        }
    }
}

impl From<&str> for HoprRefusalReason {
    fn from(reason: &str) -> Self {
        Self::Unknown(reason.to_owned())
    }
}

impl<T: Into<ErrorKind>> From<T> for BlokliClientError {
    fn from(kind: T) -> Self {
        Self {
            kind: Arc::new(kind.into()),
            unmined_diagnosis: None,
        }
    }
}

impl std::fmt::Display for BlokliClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)
    }
}

impl std::error::Error for BlokliClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.kind.as_ref())
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
    /// Blokli refused a known HOPR action before broadcasting it.
    ///
    /// Blokli decoded the transaction as a supported HOPR node-management operation and found
    /// a precondition that the indexed chain state contradicts, so nothing was sent. Retrying
    /// the same action is futile until the on-chain state changes — unlike
    /// [`ErrorKind::HoprActionThrottled`], waiting alone will not help.
    #[error("blokli rejected the {operation} action ({reason}): {message}")]
    HoprActionRejected {
        /// The HOPR operation Blokli decoded, such as `finalize_channel_closure`.
        operation: String,
        /// Stable code for the precondition that failed.
        reason: String,
        /// Human-readable explanation from Blokli.
        message: String,
    },
    /// Blokli is temporarily suppressing this signer's submissions of this operation.
    ///
    /// Raised after repeated deterministically invalid submissions. The transaction was not
    /// broadcast, but unlike [`ErrorKind::HoprActionRejected`] this clears on its own: use
    /// `retry_after` to back off rather than retrying immediately.
    #[error("blokli suppressed the {operation} action ({reason}); retry in {}s", retry_after.as_secs())]
    HoprActionThrottled {
        /// The HOPR operation Blokli decoded.
        operation: String,
        /// Stable code for the precondition that most recently failed.
        reason: String,
        /// How long to wait before submitting this operation again.
        retry_after: std::time::Duration,
        /// Human-readable explanation from Blokli.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn blokli_error(kind: &'static str, code: &str) -> BlokliClientError {
        ErrorKind::BlokliError {
            kind,
            code: code.to_owned(),
            message: "m".to_owned(),
        }
        .into()
    }

    #[test]
    fn each_known_code_maps_to_its_variant() {
        for (code, expected) in [
            ("ALREADY_KNOWN", RefusalCode::AlreadyKnown),
            ("REPLACEMENT_UNDERPRICED", RefusalCode::ReplacementUnderpriced),
            ("NONCE_TOO_LOW", RefusalCode::NonceTooLow),
            ("NONCE_TOO_HIGH", RefusalCode::NonceTooHigh),
            ("FEE_TOO_LOW", RefusalCode::FeeTooLow),
            ("INSUFFICIENT_FUNDS", RefusalCode::InsufficientFunds),
            ("RPC_ERROR", RefusalCode::RpcError),
            ("SUBMISSION_CAPACITY_EXCEEDED", RefusalCode::SubmissionCapacityExceeded),
        ] {
            assert_eq!(RefusalCode::from(code), expected);
            assert_eq!(expected.as_str(), code);
        }
    }

    #[test]
    fn known_variants_parse_back_from_as_str() {
        for variant in [
            RefusalCode::AlreadyKnown,
            RefusalCode::ReplacementUnderpriced,
            RefusalCode::NonceTooLow,
            RefusalCode::NonceTooHigh,
            RefusalCode::FeeTooLow,
            RefusalCode::InsufficientFunds,
            RefusalCode::RpcError,
            RefusalCode::SubmissionCapacityExceeded,
        ] {
            assert_eq!(variant.as_str().parse::<RefusalCode>(), Ok(variant));
        }
    }

    #[test]
    fn an_unseen_code_keeps_its_original_string() {
        let code = RefusalCode::from("FUTURE_CODE");
        assert_eq!(code, RefusalCode::Unknown("FUTURE_CODE".to_owned()));
        assert_eq!(code.as_str(), "FUTURE_CODE");
    }

    #[test]
    fn refusal_code_is_only_exposed_for_rpc_and_overload_errors() {
        assert_eq!(
            blokli_error("rpc error", "NONCE_TOO_LOW").refusal_code(),
            Some(RefusalCode::NonceTooLow)
        );
        assert_eq!(
            blokli_error("overloaded", "SUBMISSION_CAPACITY_EXCEEDED").refusal_code(),
            Some(RefusalCode::SubmissionCapacityExceeded)
        );
        assert_eq!(blokli_error("function not allowed", "FEE_TOO_LOW").refusal_code(), None);
        assert_eq!(BlokliClientError::from(ErrorKind::NoData).refusal_code(), None);
    }

    #[test]
    fn an_unknown_hopr_reason_stays_unknown() {
        let rejected: BlokliClientError = ErrorKind::HoprActionRejected {
            operation: "fund_channel".to_owned(),
            reason: "SOME_NEW_REASON".to_owned(),
            message: "m".to_owned(),
        }
        .into();
        assert_eq!(
            rejected.hopr_reason(),
            Some(HoprRefusalReason::Unknown("SOME_NEW_REASON".to_owned()))
        );

        let throttled = ErrorKind::HoprActionThrottled {
            operation: "fund_channel".to_owned(),
            reason: "OTHER".to_owned(),
            retry_after: std::time::Duration::from_secs(1),
            message: "m".to_owned(),
        };
        assert_eq!(
            throttled.reason_kind(),
            Some(HoprRefusalReason::Unknown("OTHER".to_owned()))
        );
        assert_eq!(ErrorKind::NoData.reason_kind(), None);
    }
}
