use super::{DateTime, Hex32, schema};
use crate::{
    api::{TxId, TxReceipt},
    errors::{BlokliClientError, ErrorKind},
};

#[derive(cynic::QueryVariables)]
pub struct TransactionsVariables {
    pub id: cynic::Id,
}

#[derive(cynic::QueryVariables)]
pub struct SendTransactionVariables {
    pub raw_transaction: String,
}

#[derive(cynic::QueryVariables)]
pub struct ConfirmTransactionVariables {
    pub raw_transaction: String,
    pub confirmations: i32,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "TransactionsVariables")]
pub struct QueryTransaction {
    #[arguments(id: $id)]
    pub transaction: Option<TransactionResult>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "TransactionsVariables")]
pub struct SubscribeTransaction {
    #[arguments(id: $id)]
    pub transaction_updated: Transaction,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "MutationRoot", variables = "ConfirmTransactionVariables")]
pub struct MutateConfirmTransaction {
    #[arguments(input: { rawTransaction: $raw_transaction }, confirmations: $confirmations)]
    pub send_transaction_sync: SendTransactionSyncResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "MutationRoot", variables = "SendTransactionVariables")]
pub struct MutateTrackTransaction {
    #[arguments(input: { rawTransaction: $raw_transaction })]
    pub send_transaction_async: SendTransactionAsyncResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "MutationRoot", variables = "SendTransactionVariables")]
pub struct MutateSendTransaction {
    #[arguments(input: { rawTransaction: $raw_transaction })]
    pub send_transaction: SendTransactionResult,
}

/// Safe module execution details associated with a transaction.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SafeExecution {
    /// Whether the safe module execution succeeded.
    pub success: bool,
    /// Safe transaction hash, when safe execution data is available.
    #[cynic(rename = "safeTxHash")]
    pub safe_tx_hash: Option<Hex32>,
    /// Revert reason reported by safe execution, when available.
    #[cynic(rename = "revertReason")]
    pub revert_reason: Option<String>,
}

/// Transaction tracked by Blokli.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Transaction {
    /// Blokli tracking id.
    pub id: cynic::Id,
    /// Current transaction status.
    pub status: TransactionStatus,
    /// Timestamp at which Blokli accepted or observed the submission.
    pub submitted_at: DateTime,
    /// On-chain transaction hash.
    pub transaction_hash: Hex32,
    /// Safe execution details for safe module transactions.
    #[cynic(rename = "safeExecution")]
    pub safe_execution: Option<SafeExecution>,
}

/// Error returned for an unknown or invalid Blokli transaction tracking id.
#[derive(cynic::QueryFragment, Debug)]
pub struct InvalidTransactionIdError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Stable Blokli error code.
    pub code: String,
    /// Human-readable error message from Blokli.
    pub message: String,
}

impl From<InvalidTransactionIdError> for BlokliClientError {
    fn from(value: InvalidTransactionIdError) -> Self {
        ErrorKind::BlokliError {
            kind: "invalid transaction id",
            code: value.code,
            message: value.message,
        }
        .into()
    }
}

#[derive(cynic::InlineFragments, Debug)]
pub enum TransactionResult {
    Transaction(Transaction),
    InvalidTransactionIdError(InvalidTransactionIdError),
    #[cynic(fallback)]
    Unknown,
}

impl From<TransactionResult> for Result<Transaction, BlokliClientError> {
    fn from(value: TransactionResult) -> Self {
        match value {
            TransactionResult::Transaction(t) => Ok(t),
            TransactionResult::InvalidTransactionIdError(e) => Err(e.into()),
            TransactionResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

/// Operation fetching the diagnosis of a timed out transaction.
///
/// Kept apart from [`QueryTransaction`] on purpose: Blokli versions without the `diagnosis` field reject any
/// document selecting it, so this operation is sent only to servers known to support it.
#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "TransactionsVariables")]
pub struct QueryUnminedDiagnosis {
    #[arguments(id: $id)]
    pub transaction: Option<TransactionDiagnosisResult>,
}

/// Result of [`QueryUnminedDiagnosis`].
#[derive(cynic::InlineFragments, Debug)]
#[cynic(graphql_type = "TransactionResult")]
pub enum TransactionDiagnosisResult {
    Transaction(TransactionWithDiagnosis),
    InvalidTransactionIdError(InvalidTransactionIdError),
    #[cynic(fallback)]
    Unknown,
}

/// The `diagnosis` selection of a transaction.
#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "Transaction")]
pub struct TransactionWithDiagnosis {
    pub diagnosis: Option<RawTransactionDiagnosis>,
}

/// Wire form of `TransactionDiagnosis`.
#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "TransactionDiagnosis")]
pub struct RawTransactionDiagnosis {
    pub reason: RawUnminedReason,
    pub signed_fees: Option<RawTransactionFees>,
    pub recommended_fees: Option<RawTransactionFees>,
    pub min_replacement_fees: Option<RawTransactionFees>,
    pub retry_after_seconds: i32,
}

/// Wire form of `TransactionFees`: decimal strings, empty when unavailable.
#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "TransactionFees")]
pub struct RawTransactionFees {
    pub max_fee_per_gas: String,
    pub max_priority_fee_per_gas: String,
}

/// Wire form of `UnminedReason`, read as a plain string so that values added by a newer Blokli still decode.
///
/// Implemented by hand because `cynic::Enum` derives cannot keep the string of an unseen value.
#[derive(Debug, Clone, PartialEq, Eq, cynic::serde::Deserialize, cynic::serde::Serialize)]
#[serde(crate = "cynic::serde", transparent)]
pub struct RawUnminedReason(pub String);

impl cynic::Enum for RawUnminedReason {
    type SchemaType = schema::UnminedReason;
}

/// Why a submitted transaction was not mined before it timed out.
///
/// Values added by newer Blokli versions decode to [`UnminedReason::Unknown`] with the original string.
#[derive(Clone, Debug, PartialEq, Eq, strum::EnumString, strum::IntoStaticStr)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub enum UnminedReason {
    /// The node holds the transaction, but it was signed below the current recommended fee.
    #[strum(serialize = "UNDERPRICED")]
    Underpriced,
    /// The node holds the transaction and it waits behind earlier pending transactions of the signer.
    #[strum(serialize = "QUEUED")]
    Queued,
    /// An earlier nonce of the signer is neither mined nor pending.
    #[strum(serialize = "NONCE_GAP")]
    NonceGap,
    /// The node no longer knows the transaction and its nonce is still unused.
    #[strum(serialize = "DROPPED")]
    Dropped,
    /// The node no longer knows the transaction and its nonce was consumed by another one.
    #[strum(serialize = "REPLACED")]
    Replaced,
    /// Blokli could not establish the reason (the `UNKNOWN` value on the wire).
    #[strum(serialize = "UNKNOWN")]
    Undetermined,
    /// A reason this client version does not know, with the original string.
    #[strum(default)]
    Unknown(String),
}

impl UnminedReason {
    /// The reason string as sent by Blokli.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown(reason) => reason,
            other => other.into(),
        }
    }
}

/// EIP-1559 fees in wei.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TransactionFees {
    /// `maxFeePerGas` in wei.
    pub max_fee_per_gas: u128,
    /// `maxPriorityFeePerGas` in wei.
    pub max_priority_fee_per_gas: u128,
}

impl TransactionFees {
    /// `None` when Blokli reported the fees as unavailable (empty strings) or in an unparseable form.
    fn from_raw(raw: Option<RawTransactionFees>) -> Option<Self> {
        let raw = raw?;
        Some(Self {
            max_fee_per_gas: raw.max_fee_per_gas.parse().ok()?,
            max_priority_fee_per_gas: raw.max_priority_fee_per_gas.parse().ok()?,
        })
    }
}

/// Explanation of a transaction that timed out unmined.
///
/// A fee object is `None` when Blokli could not provide it, for example when its fee estimate was unavailable.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TransactionDiagnosis {
    /// Why the transaction was not mined.
    pub reason: UnminedReason,
    /// Fees the transaction was signed with.
    pub signed_fees: Option<TransactionFees>,
    /// Fees a transaction should currently be signed with.
    pub recommended_fees: Option<TransactionFees>,
    /// Lowest fees at which a replacement would be accepted.
    pub min_replacement_fees: Option<TransactionFees>,
    /// Seconds to wait before checking or retrying.
    pub retry_after_seconds: u32,
}

impl TryFrom<RawTransactionDiagnosis> for TransactionDiagnosis {
    type Error = BlokliClientError;

    fn try_from(raw: RawTransactionDiagnosis) -> Result<Self, Self::Error> {
        Ok(Self {
            reason: UnminedReason::from(raw.reason.0.as_str()),
            signed_fees: TransactionFees::from_raw(raw.signed_fees),
            recommended_fees: TransactionFees::from_raw(raw.recommended_fees),
            min_replacement_fees: TransactionFees::from_raw(raw.min_replacement_fees),
            retry_after_seconds: u32::try_from(raw.retry_after_seconds).map_err(|_| ErrorKind::ParseError)?,
        })
    }
}

impl TransactionDiagnosisResult {
    /// Extracts the diagnosis, `None` when the transaction carries none.
    pub fn into_diagnosis(self) -> Result<Option<TransactionDiagnosis>, BlokliClientError> {
        match self {
            Self::Transaction(t) => t.diagnosis.map(TransactionDiagnosis::try_from).transpose(),
            Self::InvalidTransactionIdError(e) => Err(e.into()),
            Self::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

/// Transaction lifecycle state reported by Blokli.
#[derive(cynic::Enum, Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransactionStatus {
    /// Transaction reached the configured confirmation requirement.
    #[cynic(rename = "CONFIRMED")]
    Confirmed,
    /// Transaction is being processed by Blokli.
    #[cynic(rename = "PENDING")]
    Pending,
    /// Transaction executed and reverted.
    #[cynic(rename = "REVERTED")]
    Reverted,
    /// Blokli failed to submit the transaction.
    #[cynic(rename = "SUBMISSION_FAILED")]
    SubmissionFailed,
    /// Transaction was submitted to the chain.
    #[cynic(rename = "SUBMITTED")]
    Submitted,
    /// Transaction tracking timed out.
    #[cynic(rename = "TIMEOUT")]
    Timeout,
    /// Blokli rejected the transaction before submission.
    #[cynic(rename = "VALIDATION_FAILED")]
    ValidationFailed,
    /// A value unknown to this client version, sent by a newer Blokli.
    ///
    /// It decodes instead of failing the whole response. It cannot be sent to Blokli.
    #[cynic(fallback)]
    Unknown,
}

/// Timeout returned by a synchronous transaction operation.
#[derive(cynic::QueryFragment, Debug)]
pub struct TimeoutError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Stable Blokli error code.
    pub code: String,
    /// Human-readable error message from Blokli.
    pub message: String,
}

impl From<TimeoutError> for BlokliClientError {
    fn from(value: TimeoutError) -> Self {
        ErrorKind::BlokliError {
            kind: "timeout",
            code: value.code,
            message: value.message,
        }
        .into()
    }
}

/// Successful immediate transaction submission result.
#[derive(cynic::QueryFragment, Debug)]
pub struct SendTransactionSuccess {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// On-chain transaction hash.
    pub transaction_hash: Hex32,
}

/// RPC error returned by Blokli while submitting a transaction.
#[derive(cynic::QueryFragment, Debug)]
pub struct RpcError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Stable Blokli error code.
    pub code: String,
    /// Human-readable error message from Blokli.
    pub message: String,
}

impl From<RpcError> for BlokliClientError {
    fn from(value: RpcError) -> Self {
        ErrorKind::BlokliError {
            kind: "rpc error",
            code: value.code,
            message: value.message,
        }
        .into()
    }
}

/// Error returned when a transaction calls a function that Blokli policy rejects.
#[derive(cynic::QueryFragment, Debug)]
pub struct FunctionNotAllowedError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Stable Blokli error code.
    pub code: String,
    /// Contract address containing the rejected function.
    pub contract_address: String,
    /// Rejected function selector.
    pub function_selector: String,
    /// Human-readable error message from Blokli.
    pub message: String,
}

impl From<FunctionNotAllowedError> for BlokliClientError {
    fn from(value: FunctionNotAllowedError) -> Self {
        ErrorKind::BlokliError {
            kind: "function not allowed",
            code: value.code,
            message: value.message,
        }
        .into()
    }
}

/// Error returned when a transaction targets a contract that Blokli policy rejects.
#[derive(cynic::QueryFragment, Debug)]
pub struct ContractNotAllowedError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Stable Blokli error code.
    pub code: String,
    /// Rejected contract address.
    pub contract_address: String,
    /// Human-readable error message from Blokli.
    pub message: String,
}

impl From<ContractNotAllowedError> for BlokliClientError {
    fn from(value: ContractNotAllowedError) -> Self {
        ErrorKind::BlokliError {
            kind: "contract not allowed",
            code: value.code,
            message: value.message,
        }
        .into()
    }
}

/// Error returned when Blokli has no spare capacity to track another submitted transaction.
///
/// This is a transient condition: the submission was not broadcast, and the same
/// transaction can be retried once Blokli has drained its monitoring backlog.
#[derive(cynic::QueryFragment, Debug)]
pub struct OverloadedError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Stable Blokli error code.
    pub code: String,
    /// Human-readable error message from Blokli.
    pub message: String,
}

impl From<OverloadedError> for BlokliClientError {
    fn from(value: OverloadedError) -> Self {
        ErrorKind::BlokliError {
            kind: "overloaded",
            code: value.code,
            message: value.message,
        }
        .into()
    }
}

/// Error returned when Blokli refuses a known HOPR action before broadcasting it.
///
/// Blokli decoded the transaction as a supported HOPR node-management operation and found a
/// precondition that the indexed chain state contradicts, so the transaction was never sent.
/// Resubmitting the same action will be refused again until the on-chain state changes.
#[derive(cynic::QueryFragment, Debug)]
pub struct HoprActionRejectedError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Stable Blokli error code.
    pub code: String,
    /// Human-readable error message from Blokli.
    pub message: String,
    /// The HOPR operation Blokli decoded, such as `finalize_channel_closure`.
    pub operation: String,
    /// Stable code for the precondition that failed.
    pub reason: String,
}

impl From<HoprActionRejectedError> for BlokliClientError {
    fn from(value: HoprActionRejectedError) -> Self {
        ErrorKind::HoprActionRejected {
            operation: value.operation,
            reason: value.reason,
            message: value.message,
        }
        .into()
    }
}

/// Error returned when Blokli temporarily suppresses a signer's repeated invalid actions.
///
/// Unlike [`HoprActionRejectedError`] this is transient: the same action may be submitted
/// again once `retry_after_seconds` has elapsed.
#[derive(cynic::QueryFragment, Debug)]
pub struct HoprActionThrottledError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Stable Blokli error code.
    pub code: String,
    /// Human-readable error message from Blokli.
    pub message: String,
    /// The HOPR operation Blokli decoded, such as `finalize_channel_closure`.
    pub operation: String,
    /// Stable code for the precondition that most recently failed.
    pub reason: String,
    /// Seconds until this signer may submit this operation again.
    #[cynic(rename = "retryAfterSeconds")]
    pub retry_after_seconds: i32,
}

impl From<HoprActionThrottledError> for BlokliClientError {
    fn from(value: HoprActionThrottledError) -> Self {
        ErrorKind::HoprActionThrottled {
            operation: value.operation,
            reason: value.reason,
            // Blokli reports whole seconds and never a negative delay; a malformed value
            // becomes "retry now" rather than a panic.
            retry_after: std::time::Duration::from_secs(value.retry_after_seconds.max(0) as u64),
            message: value.message,
        }
        .into()
    }
}

/// An equivalent logical HOPR action was already in flight, so nothing was broadcast.
///
/// Retries of a HOPR action are re-signed with a new nonce, so they are distinct raw
/// transactions expressing one intent. Blokli hands back the transaction already tracking
/// that intent, which callers follow exactly as if they had submitted it themselves.
#[derive(cynic::QueryFragment, Debug)]
pub struct DeduplicatedTransaction {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// The transaction already tracking this logical action.
    pub transaction: Transaction,
    /// The HOPR operation Blokli decoded.
    pub operation: String,
}

/// Outcome of a successful asynchronous submission.
///
/// Both variants carry the id to track. `Deduplicated` means nothing was broadcast for this call because an
/// equivalent logical action was already tracked under that id.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TrackedSubmission {
    /// The transaction was accepted as a new submission.
    New(TxId),
    /// An equivalent action was already in flight; this is the id already tracking it.
    Deduplicated(TxId),
}

impl TrackedSubmission {
    /// The id to track, whichever way the submission was accepted.
    pub fn tx_id(&self) -> &TxId {
        match self {
            Self::New(id) | Self::Deduplicated(id) => id,
        }
    }

    /// Consumes the outcome and returns the id to track.
    pub fn into_tx_id(self) -> TxId {
        match self {
            Self::New(id) | Self::Deduplicated(id) => id,
        }
    }

    /// Whether the submission joined an already tracked action.
    pub fn is_deduplicated(&self) -> bool {
        matches!(self, Self::Deduplicated(_))
    }
}

#[derive(cynic::InlineFragments, Debug)]
pub enum SendTransactionAsyncResult {
    Transaction(Transaction),
    ContractNotAllowedError(ContractNotAllowedError),
    FunctionNotAllowedError(FunctionNotAllowedError),
    RpcError(RpcError),
    OverloadedError(OverloadedError),
    HoprActionRejectedError(HoprActionRejectedError),
    HoprActionThrottledError(HoprActionThrottledError),
    DeduplicatedTransaction(DeduplicatedTransaction),
    #[cynic(fallback)]
    Unknown,
}

impl SendTransactionAsyncResult {
    /// Converts the result, keeping a deduplicated submission distinct from a new one.
    pub fn into_tracked_submission(self) -> Result<TrackedSubmission, BlokliClientError> {
        match self {
            Self::DeduplicatedTransaction(d) => Ok(TrackedSubmission::Deduplicated(d.transaction.id.into_inner())),
            other => {
                Result::<Transaction, BlokliClientError>::from(other).map(|t| TrackedSubmission::New(t.id.into_inner()))
            }
        }
    }
}

impl From<SendTransactionAsyncResult> for Result<Transaction, BlokliClientError> {
    fn from(value: SendTransactionAsyncResult) -> Self {
        match value {
            SendTransactionAsyncResult::Transaction(t) => Ok(t),
            SendTransactionAsyncResult::ContractNotAllowedError(e) => Err(e.into()),
            SendTransactionAsyncResult::FunctionNotAllowedError(e) => Err(e.into()),
            SendTransactionAsyncResult::RpcError(e) => Err(e.into()),
            SendTransactionAsyncResult::OverloadedError(e) => Err(e.into()),
            SendTransactionAsyncResult::HoprActionRejectedError(e) => Err(e.into()),
            SendTransactionAsyncResult::HoprActionThrottledError(e) => Err(e.into()),
            // Nothing was broadcast, but the intent is being tracked by the returned
            // transaction: the caller follows it exactly as it would its own submission.
            SendTransactionAsyncResult::DeduplicatedTransaction(d) => Ok(d.transaction),
            SendTransactionAsyncResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

#[derive(cynic::InlineFragments, Debug)]
pub enum SendTransactionResult {
    SendTransactionSuccess(SendTransactionSuccess),
    ContractNotAllowedError(ContractNotAllowedError),
    FunctionNotAllowedError(FunctionNotAllowedError),
    RpcError(RpcError),
    HoprActionRejectedError(HoprActionRejectedError),
    HoprActionThrottledError(HoprActionThrottledError),
    #[cynic(fallback)]
    Unknown,
}

impl From<SendTransactionResult> for Result<TxReceipt, BlokliClientError> {
    fn from(value: SendTransactionResult) -> Self {
        match value {
            SendTransactionResult::SendTransactionSuccess(t) => {
                let hash = t.transaction_hash.0.to_lowercase();
                Ok(hex::decode(hash.trim_start_matches("0x"))
                    .map_err(|_| ErrorKind::ParseError)?
                    .try_into()
                    .map_err(|_| ErrorKind::ParseError)?)
            }
            SendTransactionResult::ContractNotAllowedError(e) => Err(e.into()),
            SendTransactionResult::FunctionNotAllowedError(e) => Err(e.into()),
            SendTransactionResult::RpcError(e) => Err(e.into()),
            SendTransactionResult::HoprActionRejectedError(e) => Err(e.into()),
            SendTransactionResult::HoprActionThrottledError(e) => Err(e.into()),
            SendTransactionResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

#[derive(cynic::InlineFragments, Debug)]
pub enum SendTransactionSyncResult {
    Transaction(Transaction),
    ContractNotAllowedError(ContractNotAllowedError),
    FunctionNotAllowedError(FunctionNotAllowedError),
    RpcError(RpcError),
    TimeoutError(TimeoutError),
    HoprActionRejectedError(HoprActionRejectedError),
    HoprActionThrottledError(HoprActionThrottledError),
    #[cynic(fallback)]
    Unknown,
}

impl From<SendTransactionSyncResult> for Result<Transaction, BlokliClientError> {
    fn from(value: SendTransactionSyncResult) -> Self {
        match value {
            SendTransactionSyncResult::Transaction(t) => Ok(t),
            SendTransactionSyncResult::ContractNotAllowedError(e) => Err(e.into()),
            SendTransactionSyncResult::FunctionNotAllowedError(e) => Err(e.into()),
            SendTransactionSyncResult::RpcError(e) => Err(e.into()),
            SendTransactionSyncResult::TimeoutError(e) => Err(e.into()),
            SendTransactionSyncResult::HoprActionRejectedError(e) => Err(e.into()),
            SendTransactionSyncResult::HoprActionThrottledError(e) => Err(e.into()),
            SendTransactionSyncResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

#[cfg(test)]
mod diagnosis_tests {
    use super::{RawTransactionDiagnosis, TransactionDiagnosis, TransactionFees, UnminedReason};
    use crate::errors::ErrorKind;

    fn raw(value: serde_json::Value) -> RawTransactionDiagnosis {
        serde_json::from_value(value).expect("valid wire form")
    }

    #[test]
    fn reasons_round_trip_and_keep_unseen_values() {
        for (wire, reason) in [
            ("UNDERPRICED", UnminedReason::Underpriced),
            ("QUEUED", UnminedReason::Queued),
            ("NONCE_GAP", UnminedReason::NonceGap),
            ("DROPPED", UnminedReason::Dropped),
            ("REPLACED", UnminedReason::Replaced),
            ("UNKNOWN", UnminedReason::Undetermined),
        ] {
            let parsed = UnminedReason::from(wire);
            assert_eq!(parsed, reason);
            assert_eq!(parsed.as_str(), wire);
        }
        assert_eq!(
            "FROM_THE_FUTURE".parse::<UnminedReason>().expect("infallible"),
            UnminedReason::Unknown("FROM_THE_FUTURE".into())
        );
    }

    #[test]
    fn empty_or_malformed_fee_strings_map_to_none() {
        let diagnosis = TransactionDiagnosis::try_from(raw(serde_json::json!({
            "reason": "QUEUED",
            "signedFees": { "maxFeePerGas": "", "maxPriorityFeePerGas": "" },
            "recommendedFees": { "maxFeePerGas": "5", "maxPriorityFeePerGas": "" },
            "minReplacementFees": { "maxFeePerGas": "7", "maxPriorityFeePerGas": "3" },
            "retryAfterSeconds": 30,
        })))
        .expect("conversion succeeds");

        assert_eq!(diagnosis.signed_fees, None);
        assert_eq!(diagnosis.recommended_fees, None);
        assert_eq!(
            diagnosis.min_replacement_fees,
            Some(TransactionFees {
                max_fee_per_gas: 7,
                max_priority_fee_per_gas: 3
            })
        );
        assert_eq!(diagnosis.retry_after_seconds, 30);
    }

    #[test]
    fn a_negative_retry_delay_is_a_parse_error() {
        let error = TransactionDiagnosis::try_from(raw(serde_json::json!({
            "reason": "DROPPED",
            "signedFees": null,
            "recommendedFees": null,
            "minReplacementFees": null,
            "retryAfterSeconds": -1,
        })))
        .expect_err("negative delay");

        assert!(matches!(error.kind(), ErrorKind::ParseError));
    }
}
