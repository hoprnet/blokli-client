use super::{QueryFailedError, ReadinessState, TokenValueString, Uint64, schema};

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot")]
pub struct QueryChainInfo {
    pub chain_info: ChainInfoResult,
}

/// Chain, contract, fee, and ticket parameters reported by Blokli.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ChainInfo {
    /// Channel closure grace period in chain units reported by Blokli.
    pub channel_closure_grace_period: Uint64,
    /// Deployed channel contract address, when available.
    pub channel_dst: Option<String>,
    /// Latest block number indexed or observed by Blokli.
    pub block_number: i32,
    /// Numeric chain id.
    pub chain_id: i32,
    /// Legacy gas price value, when supplied by the chain.
    pub gas_price: Option<String>,
    /// Deployed ledger contract address, when available.
    pub ledger_dst: Option<String>,
    /// Current EIP-1559 max fee per gas, when supplied by the chain.
    pub max_fee_per_gas: Option<String>,
    /// Current EIP-1559 max priority fee per gas, when supplied by the chain.
    pub max_priority_fee_per_gas: Option<String>,
    /// Minimum winning probability accepted for tickets.
    pub min_ticket_winning_probability: f64,
    /// Fee required for key binding.
    pub key_binding_fee: TokenValueString,
    /// Deployed safe registry contract address, when available.
    pub safe_registry_dst: Option<String>,
    /// Current ticket price.
    pub ticket_price: TokenValueString,
    /// Human-readable network name.
    pub network: String,
    /// Map of known contract addresses encoded by the GraphQL API.
    pub contract_addresses: ContractAddressMap,
    /// Expected block time in chain units reported by Blokli.
    pub expected_block_time: Uint64,
    /// Finality depth in blocks reported by Blokli.
    pub finality: Uint64,
}

/// Serialized map of contract names to addresses.
#[derive(cynic::Scalar, Debug, Clone, PartialEq, Eq)]
pub struct ContractAddressMap(pub String);

#[derive(cynic::InlineFragments, Debug)]
pub enum ChainInfoResult {
    ChainInfo(Box<ChainInfo>),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<ChainInfoResult> for Result<ChainInfo, crate::errors::BlokliClientError> {
    fn from(value: ChainInfoResult) -> Self {
        match value {
            ChainInfoResult::ChainInfo(info) => Ok(*info),
            ChainInfoResult::QueryFailedError(e) => Err(e.into()),
            ChainInfoResult::Unknown => Err(crate::errors::ErrorKind::NoData.into()),
        }
    }
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot")]
pub struct SubscribeTicketParams {
    pub ticket_parameters_updated: TicketParameters,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot")]
pub struct SubscribeHealth {
    pub health: ReadinessState,
}

/// Ticket parameter values emitted by Blokli.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TicketParameters {
    /// Minimum winning probability accepted for tickets.
    pub min_ticket_winning_probability: f64,
    /// Current ticket price.
    pub ticket_price: TokenValueString,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot")]
pub struct QueryVersion {
    pub version: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot")]
pub struct QueryHealth {
    pub health: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot")]
pub struct QueryCompatibility {
    pub compatibility: Compatibility,
}

/// Compatibility information reported by Blokli.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Compatibility {
    /// API version reported by the server.
    pub api_version: String,
    /// Client version range accepted by the server.
    pub supported_client_versions: String,
    /// Feature flags or capability names reported by the server.
    pub features: Vec<String>,
}
