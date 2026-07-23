use super::{DateTime, Hex32, schema};
use crate::{
    api::TxReceipt,
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

/// Transaction lifecycle state reported by Blokli.
#[derive(cynic::Enum, Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(cynic::InlineFragments, Debug)]
pub enum SendTransactionAsyncResult {
    Transaction(Transaction),
    ContractNotAllowedError(ContractNotAllowedError),
    FunctionNotAllowedError(FunctionNotAllowedError),
    RpcError(RpcError),
    #[cynic(fallback)]
    Unknown,
}

impl From<SendTransactionAsyncResult> for Result<Transaction, BlokliClientError> {
    fn from(value: SendTransactionAsyncResult) -> Self {
        match value {
            SendTransactionAsyncResult::Transaction(t) => Ok(t),
            SendTransactionAsyncResult::ContractNotAllowedError(e) => Err(e.into()),
            SendTransactionAsyncResult::FunctionNotAllowedError(e) => Err(e.into()),
            SendTransactionAsyncResult::RpcError(e) => Err(e.into()),
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
            SendTransactionSyncResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}
