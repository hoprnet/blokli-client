use super::{accounts::Account, channels::Channel, schema};

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot")]
pub struct SubscribeGraph {
    pub opened_channel_graph_updated: OpenedChannelsGraphEntry,
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
}
