use hex::ToHex;

use super::{CountResult, InvalidAddressError, MissingFilterError, QueryFailedError, Uint64, schema};
use crate::{
    api::v1::AccountSelector,
    errors::{BlokliClientError, ErrorKind},
};

#[derive(cynic::QueryVariables, Default)]
pub struct AccountVariables {
    pub keyid: Option<i32>,
    pub packet_key: Option<String>,
    pub chain_key: Option<String>,
}

impl From<AccountSelector> for AccountVariables {
    fn from(value: AccountSelector) -> Self {
        match value {
            AccountSelector::KeyId(keyid) => AccountVariables {
                keyid: Some(keyid as i32),
                ..Default::default()
            },
            AccountSelector::Address(address) => AccountVariables {
                chain_key: Some(address.encode_hex()),
                ..Default::default()
            },
            AccountSelector::PacketKey(address) => AccountVariables {
                packet_key: Some(address.encode_hex()),
                ..Default::default()
            },
            AccountSelector::Any => AccountVariables::default(),
        }
    }
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "AccountVariables")]
pub struct QueryAccounts {
    #[arguments(keyid: $keyid, packetKey: $packet_key, chainKey: $chain_key)]
    pub accounts: AccountsResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "AccountVariables")]
pub struct SubscribeAccounts {
    #[arguments(keyid: $keyid, packetKey: $packet_key, chainKey: $chain_key)]
    pub account_updated: Account,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "AccountVariables")]
pub struct QueryAccountCount {
    #[arguments(keyid: $keyid, packetKey: $packet_key, chainKey: $chain_key)]
    pub account_count: CountResult,
}

/// List of accounts returned by an account query.
#[derive(cynic::QueryFragment, Debug)]
pub struct AccountsList {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Matching accounts.
    pub accounts: Vec<Account>,
}

/// Account indexed by Blokli.
///
/// Accounts connect a Blokli key id to the chain key, packet key, advertised multiaddresses, and optional safe
/// address associated with a node.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Account {
    /// On-chain address for this account, encoded as a hex string.
    pub chain_key: String,
    /// Numeric Blokli key id.
    pub keyid: i32,
    /// Advertised libp2p multiaddresses associated with the account.
    pub multi_addresses: Vec<String>,
    /// Packet key encoded as a hex string.
    pub packet_key: String,
    /// Safe contract address associated with this account, when one is known.
    pub safe_address: Option<String>,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum AccountsResult {
    AccountsList(AccountsList),
    MissingFilterError(MissingFilterError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<AccountsResult> for Result<Vec<Account>, BlokliClientError> {
    fn from(value: AccountsResult) -> Self {
        match value {
            AccountsResult::AccountsList(list) => Ok(list.accounts),
            AccountsResult::MissingFilterError(e) => Err(e.into()),
            AccountsResult::QueryFailedError(e) => Err(e.into()),
            AccountsResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

#[derive(cynic::QueryVariables, Debug)]
pub struct TxCountVariables {
    pub address: String,
}

/// Transaction count payload for an address.
#[derive(cynic::QueryFragment, Debug)]
pub struct TransactionCount {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Number of transactions encoded as a GraphQL `UInt64` string.
    pub count: Uint64,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "TxCountVariables")]
pub struct QueryTxCount {
    #[arguments(address: $address)]
    pub transaction_count: TransactionCountResult,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum TransactionCountResult {
    TransactionCount(TransactionCount),
    InvalidAddressError(InvalidAddressError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<TransactionCountResult> for Result<u64, BlokliClientError> {
    fn from(value: TransactionCountResult) -> Self {
        match value {
            TransactionCountResult::TransactionCount(count) => {
                Ok(count.count.0.parse().map_err(|_| ErrorKind::ParseError)?)
            }
            TransactionCountResult::InvalidAddressError(e) => Err(e.into()),
            TransactionCountResult::QueryFailedError(e) => Err(e.into()),
            TransactionCountResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}
