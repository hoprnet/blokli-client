//! Schema-facing GraphQL fragments and scalar wrappers.
//!
//! The public client traits return selected structs and enums from these modules through
//! [`crate::api::types`]. Operation builders and GraphQL variables are implementation details used by
//! [`crate::BlokliClient`].

use crate::errors::ErrorKind;

pub mod accounts;
pub mod balances;
pub mod channels;
pub mod graph;
pub mod info;
pub mod safe;
pub mod services;
pub mod tickets;
pub mod txs;

#[cynic::schema("blokli")]
pub(crate) mod schema {}

// https://generator.cynic-rs.dev/

/// Token kind accepted by balance queries.
///
/// Maps the Blokli GraphQL `Token` enum onto Rust variants. Note that the
/// GraphQL `HOPR` symbol refers to the wrapped HOPR token (wxHOPR).
#[derive(cynic::Enum, Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::upper_case_acronyms)]
pub enum Token {
    /// Wrapped HOPR token (wxHOPR); the GraphQL `HOPR` symbol.
    #[cynic(rename = "HOPR")]
    WxHOPR,
    /// Native HOPR token (xHOPR).
    #[cynic(rename = "XHOPR")]
    XHOPR,
    /// Native chain token (xDai).
    #[cynic(rename = "NATIVE")]
    Native,
}

/// Channel lifecycle state reported by Blokli.
#[derive(cynic::Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelStatus {
    /// Channel is open and can carry traffic.
    #[cynic(rename = "OPEN")]
    Open,
    /// Channel close has been initiated but the closure grace period has not elapsed.
    #[cynic(rename = "PENDINGTOCLOSE")]
    PendingToClose,
    /// Channel is closed.
    #[cynic(rename = "CLOSED")]
    Closed,
}

/// Readiness state for a Blokli instance.
#[derive(cynic::Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadinessState {
    /// Blokli reports that it is ready to serve requests.
    #[cynic(rename = "READY")]
    Ready,
    /// Blokli reports that it is not ready.
    #[cynic(rename = "NOT_READY")]
    NotReady,
}

/// Date-time value as returned by the GraphQL API.
#[derive(cynic::Scalar, Debug, Clone, PartialEq, Eq)]
pub struct DateTime(pub String);

/// Decimal token amount encoded as a string by the GraphQL API.
#[derive(cynic::Scalar, Debug, Clone, PartialEq, Eq)]
pub struct TokenValueString(pub String);

/// Unsigned 64-bit integer encoded as a string by the GraphQL API.
#[derive(cynic::Scalar, Debug, Clone, PartialEq, Eq)]
#[cynic(graphql_type = "UInt64")]
pub struct Uint64(pub String);

/// 32-byte hex value as returned by the GraphQL API.
#[derive(cynic::Scalar, Debug, Clone, PartialEq, Eq)]
pub struct Hex32(pub String);

#[derive(cynic::InlineFragments, Debug)]
pub enum CountResult {
    Count(Count),
    MissingFilterError(MissingFilterError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<CountResult> for Result<u32, crate::errors::BlokliClientError> {
    fn from(value: CountResult) -> Self {
        match value {
            CountResult::Count(count) => Ok(count.count as u32),
            CountResult::MissingFilterError(e) => Err(e.into()),
            CountResult::QueryFailedError(e) => Err(e.into()),
            CountResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

/// Shared count payload used by several GraphQL count queries.
#[derive(cynic::QueryFragment, Debug)]
pub struct Count {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Number of matching records.
    pub count: i32,
}

/// Generic Blokli query failure returned by GraphQL union fields.
#[derive(cynic::QueryFragment, Debug)]
pub struct QueryFailedError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Human-readable error message from Blokli.
    pub message: String,
    /// Stable Blokli error code.
    pub code: String,
}

impl From<QueryFailedError> for crate::errors::BlokliClientError {
    fn from(value: QueryFailedError) -> Self {
        ErrorKind::BlokliError {
            kind: "query failed",
            code: value.code,
            message: value.message,
        }
        .into()
    }
}

/// Error returned when a query requires at least one filter.
#[derive(cynic::QueryFragment, Debug)]
pub struct MissingFilterError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Stable Blokli error code.
    pub code: String,
    /// Human-readable error message from Blokli.
    pub message: String,
}

impl From<MissingFilterError> for crate::errors::BlokliClientError {
    fn from(value: MissingFilterError) -> Self {
        ErrorKind::BlokliError {
            kind: "missing filter",
            code: value.code,
            message: value.message,
        }
        .into()
    }
}

/// Error returned when Blokli rejects an address argument.
#[derive(cynic::QueryFragment, Debug)]
pub struct InvalidAddressError {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Stable Blokli error code.
    pub code: String,
    /// Human-readable error message from Blokli.
    pub message: String,
}

impl From<InvalidAddressError> for crate::errors::BlokliClientError {
    fn from(value: InvalidAddressError) -> Self {
        ErrorKind::BlokliError {
            kind: "invalid address",
            code: value.code,
            message: value.message,
        }
        .into()
    }
}
