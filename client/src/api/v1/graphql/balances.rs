use super::{InvalidAddressError, MissingFilterError, QueryFailedError, Token, Uint64, schema};
use crate::{api::types::TokenValueString, errors::BlokliClientError};

#[derive(cynic::QueryVariables)]
pub struct BalanceVariables {
    pub address: String,
    pub token: Option<Token>,
}

#[derive(cynic::InputObject, Debug, Default, Clone)]
#[cynic(graphql_type = "RedeemedStatsFilter")]
pub struct RedeemedStatsFilter {
    #[cynic(rename = "safeAddress")]
    pub safe_address: Option<String>,
    #[cynic(rename = "nodeAddress")]
    pub node_address: Option<String>,
}

#[derive(cynic::QueryVariables, Default)]
pub struct RedeemedStatsVariables {
    pub filter: RedeemedStatsFilter,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "BalanceVariables")]
pub struct QueryHoprBalance {
    #[arguments(address: $address, token: $token)]
    pub hopr_balance: HoprBalanceResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "BalanceVariables")]
pub struct QueryNativeBalance {
    #[arguments(address: $address)]
    pub native_balance: NativeBalanceResult,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum HoprBalanceResult {
    HoprBalance(HoprBalance),
    InvalidAddressError(InvalidAddressError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<HoprBalanceResult> for Result<HoprBalance, BlokliClientError> {
    fn from(value: HoprBalanceResult) -> Self {
        match value {
            HoprBalanceResult::HoprBalance(balance) => Ok(balance),
            HoprBalanceResult::InvalidAddressError(e) => Err(e.into()),
            HoprBalanceResult::QueryFailedError(e) => Err(e.into()),
            HoprBalanceResult::Unknown => Err(crate::errors::ErrorKind::NoData.into()),
        }
    }
}

/// HOPR token balance for an address.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct HoprBalance {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Token balance encoded as a decimal string.
    pub balance: TokenValueString,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum NativeBalanceResult {
    NativeBalance(NativeBalance),
    InvalidAddressError(InvalidAddressError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<NativeBalanceResult> for Result<NativeBalance, BlokliClientError> {
    fn from(value: NativeBalanceResult) -> Self {
        match value {
            NativeBalanceResult::NativeBalance(balance) => Ok(balance),
            NativeBalanceResult::InvalidAddressError(e) => Err(e.into()),
            NativeBalanceResult::QueryFailedError(e) => Err(e.into()),
            NativeBalanceResult::Unknown => Err(crate::errors::ErrorKind::NoData.into()),
        }
    }
}

/// Native-chain balance for an address.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct NativeBalance {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Native balance encoded as a decimal string.
    pub balance: TokenValueString,
}

/// HOPR token allowance configured for a safe.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SafeHoprAllowance {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Allowance encoded as a decimal string.
    pub allowance: TokenValueString,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "BalanceVariables")]
pub struct QuerySafeAllowance {
    #[arguments(address: $address)]
    pub safe_hopr_allowance: SafeHoprAllowanceResult,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum SafeHoprAllowanceResult {
    SafeHoprAllowance(SafeHoprAllowance),
    InvalidAddressError(InvalidAddressError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<SafeHoprAllowanceResult> for Result<SafeHoprAllowance, BlokliClientError> {
    fn from(value: SafeHoprAllowanceResult) -> Self {
        match value {
            SafeHoprAllowanceResult::SafeHoprAllowance(allowance) => Ok(allowance),
            SafeHoprAllowanceResult::InvalidAddressError(e) => Err(e.into()),
            SafeHoprAllowanceResult::QueryFailedError(e) => Err(e.into()),
            SafeHoprAllowanceResult::Unknown => Err(crate::errors::ErrorKind::NoData.into()),
        }
    }
}

/// Variables of the `safeHoprApproval` subscription.
#[derive(cynic::QueryVariables, Debug)]
pub struct SafeHoprApprovalVariables {
    /// Safe address encoded as a hex string.
    pub address: String,
}

/// Absolute wxHOPR allowance that a Safe grants to the configured Channels contract.
///
/// Emitted by the `safeHoprApproval` subscription: first as a snapshot of the current allowance,
/// then once per matching `Approval` event of the configured wxHOPR token.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SafeHoprApproval {
    /// Safe address (token owner) encoded as a hex string.
    pub owner: String,
    /// Channels contract address (spender) encoded as a hex string.
    pub spender: String,
    /// Absolute allowance after the approval, not a delta.
    pub allowance: TokenValueString,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "SafeHoprApprovalVariables")]
pub struct SubscribeSafeHoprApproval {
    #[arguments(address: $address)]
    pub safe_hopr_approval: SafeHoprApproval,
}

/// Aggregate ticket redemption statistics.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct RedeemedStats {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Sum of accepted redemption amounts.
    pub redeemed_amount: TokenValueString,
    /// Count of accepted redemptions.
    pub redemption_count: Uint64,
    /// Sum of rejected redemption amounts.
    pub rejected_amount: TokenValueString,
    /// Count of rejected redemptions.
    pub rejection_count: Uint64,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "RedeemedStatsVariables")]
pub struct QueryRedeemedStats {
    #[arguments(filter: $filter)]
    #[cynic(rename = "ticketRedemptionStats")]
    pub ticket_redemption_stats: RedeemedStatsResult,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum RedeemedStatsResult {
    RedeemedStats(RedeemedStats),
    MissingFilterError(MissingFilterError),
    InvalidAddressError(InvalidAddressError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<RedeemedStatsResult> for Result<RedeemedStats, BlokliClientError> {
    fn from(value: RedeemedStatsResult) -> Self {
        match value {
            RedeemedStatsResult::RedeemedStats(stats) => Ok(stats),
            RedeemedStatsResult::MissingFilterError(e) => Err(e.into()),
            RedeemedStatsResult::InvalidAddressError(e) => Err(e.into()),
            RedeemedStatsResult::QueryFailedError(e) => Err(e.into()),
            RedeemedStatsResult::Unknown => Err(crate::errors::ErrorKind::NoData.into()),
        }
    }
}
