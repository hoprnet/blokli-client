use super::{Uint64, accounts::Account, channels::Channel, schema};

/// Variables of the channel graph subscription.
#[derive(cynic::QueryVariables, Clone, Debug, Default)]
pub struct GraphVariables {
    /// Resume after this cursor instead of starting with the snapshot of open channels.
    pub after: Option<ChannelStateCursor>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "GraphVariables")]
pub struct SubscribeGraph {
    #[arguments(after: $after)]
    pub opened_channel_graph_updated: OpenedChannelsGraphEntry,
}

/// Position of a channel state change in the indexed chain: the log that produced it.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ChannelStatePosition {
    /// Block number containing the log.
    pub block: Uint64,
    /// Zero-based transaction index inside the block.
    pub transaction_index: Uint64,
    /// Zero-based log index inside the transaction receipt.
    pub log_index: Uint64,
}

/// Exclusive resume cursor of the channel graph subscription.
#[derive(cynic::InputObject, Clone, Debug, PartialEq, Eq)]
pub struct ChannelStateCursor {
    /// Block number containing the log.
    pub block: Uint64,
    /// Zero-based transaction index inside the block.
    pub transaction_index: Uint64,
    /// Zero-based log index inside the transaction receipt.
    pub log_index: Uint64,
}

impl From<&ChannelStatePosition> for ChannelStateCursor {
    fn from(position: &ChannelStatePosition) -> Self {
        Self {
            block: position.block.clone(),
            transaction_index: position.transaction_index.clone(),
            log_index: position.log_index.clone(),
        }
    }
}

/// One edge in the open-channel graph.
///
/// The graph subscription emits an initial set of open-channel entries and then later channel changes. Consumers
/// should merge updates by [`Channel::concrete_channel_id`](super::channels::Channel::concrete_channel_id).
#[derive(cynic::QueryFragment, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct OpenedChannelsGraphEntry {
    /// Channel represented by this graph edge.
    pub channel: Channel,
    /// Destination account for the channel.
    pub destination: Account,
    /// Source account for the channel.
    pub source: Account,
    /// Where the subscription resumes after this entry. Blokli sets it on live updates, on resumed entries, and on
    /// the last snapshot entry; the client uses it to reconnect without losing changes.
    pub cursor: Option<ChannelStatePosition>,
}
