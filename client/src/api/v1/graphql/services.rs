use super::{CountResult, MissingFilterError, QueryFailedError, Uint64, schema};
use crate::{
    api::v1::{ChainAddress, ServiceSelector, ServiceTypeId},
    errors::{BlokliClientError, ErrorKind},
};

/// Renders a service type id in the `0x`-prefixed hexadecimal form accepted by the API.
///
/// The API also accepts the ASCII name of a type, but the client holds the raw 32-byte id and the
/// hexadecimal form is accepted for every id, including the ones that do not follow the ASCII
/// convention.
pub(crate) fn service_type_to_filter(service_type: ServiceTypeId) -> String {
    format!("0x{}", hex::encode(service_type))
}

/// Decodes a service type id into the ASCII name Blokli renders it as, when the id follows the
/// right-padded printable-ASCII convention of the registry.
///
/// Returns `None` for any id that does not follow it, which Blokli renders as `0x`-prefixed hex
/// instead. `hopr_types::internal::service::ServiceType` is the source of truth for this
/// convention; it is reimplemented here because the client deliberately models a service type as a
/// plain [`ServiceTypeId`] and does not enable the `internal` feature of `hopr-types`.
///
/// Only the in-memory test client needs to reverse the rendering, so this is not compiled into a
/// plain library build.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn service_type_name(service_type: &ServiceTypeId) -> Option<&str> {
    let len = service_type.iter().rposition(|byte| *byte != 0)? + 1;
    let name = std::str::from_utf8(&service_type[..len]).ok()?;

    name.bytes().all(|byte| byte.is_ascii_graphic()).then_some(name)
}

/// Renders a node address for an API filter.
///
/// Unprefixed, matching every other address filter this client sends. The service type filter keeps its `0x`
/// prefix instead, because that is the form the API documents for an id that is not a printable ASCII name.
fn node_to_filter(node: ChainAddress) -> String {
    hex::encode(node)
}

#[derive(cynic::QueryVariables, Debug, Default)]
pub struct ServiceVariables {
    pub service_type: Option<String>,
    pub node: Option<String>,
}

#[derive(cynic::QueryVariables, Debug)]
pub struct ServicePageVariables {
    pub service_type: Option<String>,
    pub node: Option<String>,
    pub first: i32,
    pub after: Option<Uint64>,
    pub watermark: Option<Uint64>,
    pub live_only: bool,
}

impl ServicePageVariables {
    pub fn new(selector: ServiceSelector, after: Option<Uint64>, watermark: Option<Uint64>, live_only: bool) -> Self {
        let filters = ServiceVariables::from(selector);
        Self {
            service_type: filters.service_type,
            node: filters.node,
            first: 1000,
            after,
            watermark,
            live_only,
        }
    }
}

impl From<ServiceSelector> for ServiceVariables {
    fn from(value: ServiceSelector) -> Self {
        match value {
            ServiceSelector::ServiceType(service_type) => ServiceVariables {
                service_type: Some(service_type_to_filter(service_type)),
                node: None,
            },
            ServiceSelector::Node(node) => ServiceVariables {
                service_type: None,
                node: Some(node_to_filter(node)),
            },
            ServiceSelector::ServiceTypeAndNode { service_type, node } => ServiceVariables {
                service_type: Some(service_type_to_filter(service_type)),
                node: Some(node_to_filter(node)),
            },
            ServiceSelector::Any => ServiceVariables::default(),
        }
    }
}

#[derive(cynic::QueryVariables, Debug, Default)]
pub struct ServiceTypeVariables {
    pub service_type: Option<String>,
}

impl From<Option<ServiceTypeId>> for ServiceTypeVariables {
    fn from(value: Option<ServiceTypeId>) -> Self {
        ServiceTypeVariables {
            service_type: value.map(service_type_to_filter),
        }
    }
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "ServicePageVariables")]
pub struct QueryServices {
    #[arguments(serviceType: $service_type, node: $node, first: $first, after: $after, watermark: $watermark, liveOnly: $live_only)]
    pub services: ServicesResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "ServiceVariables")]
pub struct QueryServiceCount {
    #[arguments(serviceType: $service_type, node: $node)]
    pub service_count: CountResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "ServiceTypeVariables")]
pub struct QueryServiceTypes {
    #[arguments(serviceType: $service_type)]
    pub service_types: ServiceTypesResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot")]
pub struct QueryServiceRegistryConfig {
    pub service_registry_config: ServiceRegistryConfigResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "ServiceVariables")]
pub struct SubscribeServices {
    #[arguments(serviceType: $service_type, node: $node)]
    pub service_updated: ServiceUpdate,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "ServiceTypeVariables")]
pub struct SubscribeServiceTypes {
    #[arguments(serviceType: $service_type)]
    pub service_type_updated: ServiceTypeUpdate,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot")]
pub struct SubscribeServiceRegistryConfig {
    pub service_registry_config_updated: ServiceRegistryConfig,
}

/// Single entry of the on-chain service registry: one node offering one service type.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ServiceEntry {
    /// Service type identifier: the ASCII name, or `0x`-prefixed hex when the id is not printable
    /// ASCII.
    pub service_type: String,
    /// Chain address of the node offering the service, encoded as a hex string.
    pub node: String,
    /// Safe that performed the last write to this entry, encoded as a hex string.
    pub safe: String,
    /// Opaque metadata as `0x`-prefixed hex; the schema belongs to the service type.
    pub metadata: String,
    /// Unix timestamp in seconds at which the entry was registered.
    pub registered_at: Uint64,
    /// Unix timestamp in seconds at which the entry was last updated.
    pub updated_at: Uint64,
}

/// Configuration of a single service type.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ServiceTypeInfo {
    /// Service type identifier: the ASCII name, or `0x`-prefixed hex.
    pub service_type: String,
    /// Owner of the type; `None` once the type has been abandoned, which is one-way.
    pub owner: Option<String>,
    /// Requirement contract gating registration; `None` for an open type.
    pub requirement: Option<String>,
    /// wxHOPR burned on self-registration, as a decimal string in wei.
    pub registration_burn: String,
    /// wxHOPR burned on self-update, as a decimal string in wei.
    pub update_burn: String,
}

/// Registry-wide configuration, shared by every service type.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ServiceRegistryConfig {
    /// wxHOPR burned to register a new service type, as a decimal string in wei.
    pub type_registration_fee: String,
    /// Node-safe registry the service registry resolves node bindings against, as a hex string.
    pub node_safe_registry: String,
}

/// Kind of change reported for a single registry entry.
#[derive(cynic::Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceUpdateKind {
    /// The entry was created.
    #[cynic(rename = "REGISTERED")]
    Registered,
    /// An existing entry changed.
    #[cynic(rename = "UPDATED")]
    Updated,
    /// The entry was removed.
    #[cynic(rename = "DEREGISTERED")]
    Deregistered,
}

/// Kind of change reported for service-type or registry-wide configuration.
#[derive(cynic::Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceTypeUpdateKind {
    /// A new service type was registered.
    #[cynic(rename = "REGISTERED")]
    Registered,
    /// The owner of a service type changed, or the type was abandoned.
    #[cynic(rename = "OWNER_CHANGED")]
    OwnerChanged,
    /// The requirement contract of a service type changed.
    #[cynic(rename = "REQUIREMENT_CHANGED")]
    RequirementChanged,
    /// The self-registration burn of a service type changed.
    #[cynic(rename = "REGISTRATION_BURN_CHANGED")]
    RegistrationBurnChanged,
    /// The self-update burn of a service type changed.
    #[cynic(rename = "UPDATE_BURN_CHANGED")]
    UpdateBurnChanged,
    /// The registry-wide type registration fee changed.
    #[cynic(rename = "REGISTRATION_FEE_CHANGED")]
    RegistrationFeeChanged,
    /// The node-safe registry the service registry points at changed.
    #[cynic(rename = "REGISTRY_POINTER_CHANGED")]
    RegistryPointerChanged,
}

/// Change to one registry entry.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ServiceUpdate {
    /// What happened to the entry.
    pub kind: ServiceUpdateKind,
    /// Service type the entry belongs to.
    pub service_type: String,
    /// Node the entry belongs to, encoded as a hex string.
    pub node: String,
    /// Entry state after the change; `None` for
    /// [`Deregistered`](ServiceUpdateKind::Deregistered), where the entry no longer exists.
    pub entry: Option<ServiceEntry>,
}

/// Change to service-type or registry-wide configuration.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ServiceTypeUpdate {
    /// What changed.
    pub kind: ServiceTypeUpdateKind,
    /// Service type affected; `None` for the two registry-wide kinds.
    pub service_type: Option<String>,
    /// Type configuration after the change; `None` for the two registry-wide kinds.
    pub config: Option<ServiceTypeInfo>,
    /// Registry-wide configuration after the change; `None` for the five per-type kinds.
    pub registry_config: Option<ServiceRegistryConfig>,
}

/// List of registry entries returned by a service query.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ServicesList {
    /// Matching registry entries.
    pub services: Vec<ServiceEntry>,
    /// Block watermark shared by every page of this enumeration.
    pub watermark: Uint64,
    /// Cursor for the next page.
    pub next_cursor: Option<Uint64>,
}

#[derive(Debug)]
pub struct ServicePage {
    pub services: Vec<ServiceEntry>,
    pub watermark: Uint64,
    pub next_cursor: Option<Uint64>,
}

/// List of service types returned by a service type query.
#[derive(cynic::QueryFragment, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ServiceTypesList {
    /// Matching service types.
    pub service_types: Vec<ServiceTypeInfo>,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum ServicesResult {
    ServicesList(ServicesList),
    MissingFilterError(MissingFilterError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<ServicesResult> for Result<ServicePage, BlokliClientError> {
    fn from(value: ServicesResult) -> Self {
        match value {
            ServicesResult::ServicesList(list) => Ok(ServicePage {
                services: list.services,
                watermark: list.watermark,
                next_cursor: list.next_cursor,
            }),
            ServicesResult::MissingFilterError(e) => Err(e.into()),
            ServicesResult::QueryFailedError(e) => Err(e.into()),
            ServicesResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

#[derive(cynic::InlineFragments, Debug)]
pub enum ServiceTypesResult {
    ServiceTypesList(ServiceTypesList),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum ServiceRegistryConfigResult {
    ServiceRegistryConfig(ServiceRegistryConfig),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<ServiceRegistryConfigResult> for Result<ServiceRegistryConfig, BlokliClientError> {
    fn from(value: ServiceRegistryConfigResult) -> Self {
        match value {
            ServiceRegistryConfigResult::ServiceRegistryConfig(config) => Ok(config),
            ServiceRegistryConfigResult::QueryFailedError(e) => Err(e.into()),
            ServiceRegistryConfigResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

impl From<ServiceTypesResult> for Result<Vec<ServiceTypeInfo>, BlokliClientError> {
    fn from(value: ServiceTypesResult) -> Self {
        match value {
            ServiceTypesResult::ServiceTypesList(list) => Ok(list.service_types),
            ServiceTypesResult::QueryFailedError(e) => Err(e.into()),
            ServiceTypesResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ServiceVariables, service_type_name, service_type_to_filter};
    use crate::api::v1::ServiceSelector;

    const GVPN_EXIT: [u8; 32] = [
        0x67, 0x76, 0x70, 0x6e, 0x3a, 0x65, 0x78, 0x69, 0x74, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ];

    #[test]
    fn service_type_filter_is_zero_padded_prefixed_hex() {
        assert_eq!(
            service_type_to_filter(GVPN_EXIT),
            "0x6776706e3a657869740000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn ascii_name_is_decoded_from_a_right_padded_id() {
        assert_eq!(service_type_name(&GVPN_EXIT), Some("gvpn:exit"));
    }

    #[test]
    fn ascii_name_rejects_ids_outside_the_convention() {
        // An interior NUL byte, so the trailing bytes are not padding.
        let mut interior_nul = GVPN_EXIT;
        interior_nul[31] = b'x';
        assert_eq!(service_type_name(&interior_nul), None);

        // Non-graphic ASCII, which `FromStr` on the foundation type also rejects.
        let mut with_space = [0u8; 32];
        with_space[..3].copy_from_slice(b"a b");
        assert_eq!(service_type_name(&with_space), None);

        // The all-zero id, which the registry contract itself rejects.
        assert_eq!(service_type_name(&[0u8; 32]), None);
    }

    #[test]
    fn any_selector_sends_no_filters() {
        let variables = ServiceVariables::from(ServiceSelector::Any);

        assert!(variables.service_type.is_none());
        assert!(variables.node.is_none());
    }

    #[test]
    fn combined_selector_sends_both_filters() {
        let variables = ServiceVariables::from(ServiceSelector::ServiceTypeAndNode {
            service_type: GVPN_EXIT,
            node: [0x11; 20],
        });

        assert_eq!(
            variables.node.as_deref(),
            Some("1111111111111111111111111111111111111111")
        );
        assert_eq!(
            variables.service_type.as_deref(),
            Some("0x6776706e3a657869740000000000000000000000000000000000000000000000")
        );
    }
}
