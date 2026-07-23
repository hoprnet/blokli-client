use hex::ToHex;

use super::{
    ChannelStatus, CountResult, DateTime, InvalidAddressError, MissingFilterError, QueryFailedError, TokenValueString,
    Uint64, schema,
};
use crate::api::v1::{ChainAddress, ChannelFilter, ChannelSelector};

#[derive(cynic::QueryVariables, Default)]
pub struct ChannelsVariables {
    pub concrete_channel_id: Option<String>,
    pub destination_key_id: Option<i32>,
    pub source_key_id: Option<i32>,
    pub safe_address: Option<String>,
    pub status: Option<ChannelStatus>,
}

impl From<ChannelSelector> for ChannelsVariables {
    fn from(value: ChannelSelector) -> Self {
        let safe_address = value.safe_address.map(|a: ChainAddress| hex::encode(a));
        match value.filter {
            Some(ChannelFilter::ChannelId(id)) => ChannelsVariables {
                concrete_channel_id: Some(id.encode_hex()),
                safe_address,
                status: value.status,
                ..Default::default()
            },
            Some(ChannelFilter::DestinationKeyId(dst)) => ChannelsVariables {
                destination_key_id: Some(dst as i32),
                safe_address,
                status: value.status,
                ..Default::default()
            },
            Some(ChannelFilter::SourceKeyId(src)) => ChannelsVariables {
                source_key_id: Some(src as i32),
                safe_address,
                status: value.status,
                ..Default::default()
            },
            Some(ChannelFilter::SourceAndDestinationKeyIds(src, dst)) => ChannelsVariables {
                destination_key_id: Some(dst as i32),
                source_key_id: Some(src as i32),
                safe_address,
                status: value.status,
                ..Default::default()
            },
            None => ChannelsVariables {
                safe_address,
                status: value.status,
                ..Default::default()
            },
        }
    }
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "ChannelsVariables")]
pub struct QueryChannels {
    #[arguments(concreteChannelId: $concrete_channel_id, destinationKeyId: $destination_key_id, sourceKeyId: $source_key_id, safeAddress: $safe_address, status: $status)]
    pub channels: ChannelsResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "ChannelsVariables")]
pub struct SubscribeChannels {
    #[arguments(concreteChannelId: $concrete_channel_id, destinationKeyId: $destination_key_id, sourceKeyId: $source_key_id, status: $status)]
    pub channel_updated: Channel,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "ChannelsVariables")]
pub struct QueryChannelCount {
    #[arguments(concreteChannelId: $concrete_channel_id, destinationKeyId: $destination_key_id, sourceKeyId: $source_key_id, safeAddress: $safe_address, status: $status)]
    pub channel_count: CountResult,
}

#[derive(cynic::QueryVariables, Default)]
pub struct ChannelStatsVariables {
    pub concrete_channel_id: Option<String>,
    pub destination_key_id: Option<i32>,
    pub safe_address: Option<String>,
    pub source_key_id: Option<i32>,
    pub status: Option<ChannelStatus>,
}

impl From<ChannelSelector> for ChannelStatsVariables {
    fn from(value: ChannelSelector) -> Self {
        let safe_address = value.safe_address.map(|a: ChainAddress| hex::encode(a));
        match value.filter {
            Some(ChannelFilter::ChannelId(id)) => ChannelStatsVariables {
                concrete_channel_id: Some(id.encode_hex()),
                safe_address,
                status: value.status,
                ..Default::default()
            },
            Some(ChannelFilter::DestinationKeyId(dst)) => ChannelStatsVariables {
                destination_key_id: Some(dst as i32),
                safe_address,
                status: value.status,
                ..Default::default()
            },
            Some(ChannelFilter::SourceKeyId(src)) => ChannelStatsVariables {
                source_key_id: Some(src as i32),
                safe_address,
                status: value.status,
                ..Default::default()
            },
            Some(ChannelFilter::SourceAndDestinationKeyIds(src, dst)) => ChannelStatsVariables {
                destination_key_id: Some(dst as i32),
                source_key_id: Some(src as i32),
                safe_address,
                status: value.status,
                ..Default::default()
            },
            None => ChannelStatsVariables {
                safe_address,
                status: value.status,
                ..Default::default()
            },
        }
    }
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "ChannelStatsVariables")]
pub struct QueryChannelStats {
    #[arguments(concreteChannelId: $concrete_channel_id, destinationKeyId: $destination_key_id, safeAddress: $safe_address, sourceKeyId: $source_key_id, status: $status)]
    pub channel_stats: ChannelStatsResult,
}

/// Aggregate count and balance for a channel selector.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cynic(graphql_type = "ChannelStats")]
pub struct ChannelStats {
    /// Number of matching channels.
    pub count: i32,
    /// Total wxHOPR balance across the matching channels.
    pub balance: TokenValueString,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum ChannelStatsResult {
    ChannelStats(ChannelStats),
    InvalidAddressError(InvalidAddressError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<ChannelStatsResult> for Result<ChannelStats, crate::errors::BlokliClientError> {
    fn from(value: ChannelStatsResult) -> Self {
        match value {
            ChannelStatsResult::ChannelStats(stats) => Ok(stats),
            ChannelStatsResult::InvalidAddressError(e) => Err(e.into()),
            ChannelStatsResult::QueryFailedError(e) => Err(e.into()),
            ChannelStatsResult::Unknown => Err(crate::errors::ErrorKind::NoData.into()),
        }
    }
}

/// List of channels returned by a channel query.
#[derive(cynic::QueryFragment, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ChannelsList {
    /// GraphQL concrete type name.
    pub __typename: String,
    /// Matching channels.
    pub channels: Vec<Channel>,
}

/// Payment channel indexed by Blokli.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Channel {
    /// Current channel balance.
    pub balance: TokenValueString,
    /// Time at which the channel may close, when closure has been initiated.
    pub closure_time: Option<DateTime>,
    /// Concrete channel id encoded as a hex string.
    pub concrete_channel_id: String,
    /// Destination account key id.
    pub destination: i32,
    /// Current channel epoch.
    pub epoch: i32,
    /// Source account key id.
    pub source: i32,
    /// Current channel lifecycle state.
    pub status: ChannelStatus,
    /// Current ticket index for the channel.
    pub ticket_index: Uint64,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum ChannelsResult {
    ChannelsList(ChannelsList),
    MissingFilterError(MissingFilterError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<ChannelsResult> for Result<ChannelsList, crate::errors::BlokliClientError> {
    fn from(value: ChannelsResult) -> Self {
        match value {
            ChannelsResult::ChannelsList(list) => Ok(ChannelsList {
                __typename: list.__typename,
                channels: list.channels,
            }),
            ChannelsResult::MissingFilterError(e) => Err(e.into()),
            ChannelsResult::QueryFailedError(e) => Err(e.into()),
            ChannelsResult::Unknown => Err(crate::errors::ErrorKind::NoData.into()),
        }
    }
}

#[derive(cynic::QueryVariables, Default)]
pub struct SafesBalanceVariables {
    pub owner_address: Option<String>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "SafesBalanceVariables")]
pub struct QuerySafesBalance {
    #[arguments(ownerAddress: $owner_address)]
    pub safes_balance: SafesBalanceResult,
}

/// Aggregate balance held across indexed safes.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SafesBalance {
    /// Total wxHOPR balance across matching safes.
    pub balance: TokenValueString,
    /// Number of safes included in the aggregate.
    pub count: i32,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum SafesBalanceResult {
    InvalidAddressError(InvalidAddressError),
    QueryFailedError(QueryFailedError),
    SafesBalance(SafesBalance),
    #[cynic(fallback)]
    Unknown,
}

impl From<SafesBalanceResult> for Result<SafesBalance, crate::errors::BlokliClientError> {
    fn from(value: SafesBalanceResult) -> Self {
        match value {
            SafesBalanceResult::SafesBalance(balance) => Ok(balance),
            SafesBalanceResult::InvalidAddressError(e) => Err(e.into()),
            SafesBalanceResult::QueryFailedError(e) => Err(e.into()),
            SafesBalanceResult::Unknown => Err(crate::errors::ErrorKind::NoData.into()),
        }
    }
}
