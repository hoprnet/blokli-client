mod queries;
mod subscriptions;
mod table;

use std::{str::FromStr, time::Duration};

use blokli_client::{
    BlokliClient,
    api::{
        AccountSelector, BlokliTransactionClient, ChainAddress, ChannelFilter, ChannelSelector, RedeemedStatsSelector,
        ServiceSelector, ServiceTypeId,
        types::{Account, ChannelStatus},
    },
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::{StreamExt, TryFuture, TryFutureExt, future::Either, pin_mut};
use hopr_types::{
    crypto::types::OffchainPublicKey,
    primitive::prelude::{Address, ToHex},
};
use queries::QueryTarget;
use tokio::io::AsyncReadExt;
use tracing_subscriber::{EnvFilter, fmt};

use crate::subscriptions::{ChannelAllowedStates, SubscriptionTarget};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// URL of Blokli instance connect to (e.g., http://localhost:8080).
    #[arg(short, long, env = "BLOKLI_URL", value_parser = clap::value_parser!(url::Url))]
    url: url::Url,
    /// Output format.
    #[arg(short, long, env, value_enum, default_value = "json")]
    format: Formats,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Formats {
    /// Output in JSON format.
    Json,
    /// Output in YAML format.
    Yaml,
    /// Output in human-readable tables.
    Table,
}

impl Formats {
    pub fn serialize<T: serde::Serialize>(&self, value: T) -> anyhow::Result<String> {
        match self {
            Formats::Json => Ok(serde_json::to_string_pretty(&value)?),
            Formats::Yaml => Ok(serde_yaml::to_string(&value)?),
            Formats::Table => table::serialize(value),
        }
    }
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Perform a query to Blokli.
    #[clap(visible_alias = "q")]
    Query {
        #[clap(subcommand)]
        target: QueryTarget,
    },
    /// Subscribe to events from Blokli.
    #[clap(visible_alias = "sub")]
    Subscribe {
        #[clap(subcommand)]
        target: SubscriptionTarget,
    },
    /// Submit an on-chain transaction using Blokli.
    #[clap(visible_alias = "tx")]
    Transaction {
        /// Hex-encoded transaction payload.
        ///
        /// If not specified, reads the payload from the standard input as raw bytes.
        #[arg(short, long)]
        payload: Option<String>,
        /// Number of blocks to wait for confirmation.
        #[arg(short, long, group = "tx")]
        wait_for_confirmation: Option<usize>,
        /// Indicates whether to track the transaction status instead of waiting for confirmations.
        #[arg(short, long, group = "tx")]
        track: bool,
    },
}

#[derive(Debug, Clone, Args)]
pub(crate) struct RedemptionsArgs {
    /// Optional safe address filter.
    #[arg(short, long, value_parser = clap::value_parser!(String))]
    safe_address: Option<String>,
    /// Optional destination node address filter.
    #[arg(short, long, value_parser = clap::value_parser!(String))]
    node_address: Option<String>,
}

impl TryFrom<RedemptionsArgs> for RedeemedStatsSelector {
    type Error = anyhow::Error;

    fn try_from(value: RedemptionsArgs) -> Result<Self, Self::Error> {
        let RedemptionsArgs {
            safe_address,
            node_address,
        } = value;
        match (safe_address, node_address) {
            (Some(safe), None) => Ok(RedeemedStatsSelector::SafeAddress(ChainAddress::from(
                safe.parse::<Address>()?,
            ))),
            (None, Some(node)) => Ok(RedeemedStatsSelector::NodeAddress(ChainAddress::from(
                node.parse::<Address>()?,
            ))),
            (Some(safe), Some(node)) => Ok(RedeemedStatsSelector::SafeAndNodeAddress {
                safe_address: ChainAddress::from(safe.parse::<Address>()?),
                node_address: ChainAddress::from(node.parse::<Address>()?),
            }),
            (None, None) => Err(anyhow::anyhow!(
                "At least one of --safe-address or --node-address must be specified."
            )),
        }
    }
}

/// Parses a service type from the command line, either as its ASCII name or as a hexadecimal id.
///
/// The registry stores the id as a raw `bytes32` and by convention holds right-padded printable ASCII, so
/// `gvpn:exit` and `0x6776706e3a657869740000000000000000000000000000000000000000000000` name the same type.
///
/// The `0x` prefix decides which form applies, so a name is never mistaken for a truncated id. A value that starts
/// with `0x` must therefore spell all 32 bytes, and every other value is read as a name.
pub(crate) fn parse_service_type(value: &str) -> anyhow::Result<ServiceTypeId> {
    let id = match value.strip_prefix("0x") {
        Some(digits) => hex::decode(digits).map_err(anyhow::Error::from).and_then(|bytes| {
            <ServiceTypeId>::try_from(bytes)
                .map_err(|_| anyhow::anyhow!("a 0x-prefixed service type id must spell all 32 bytes"))
        })?,
        None => {
            if value.is_empty() || value.len() > size_of::<ServiceTypeId>() {
                return Err(anyhow::anyhow!("service type name must be 1 to 32 bytes long"));
            }
            // Space is excluded along with the control characters: it is indistinguishable from the padding to the
            // eye, which makes it a poor character for an identifier.
            if !value.bytes().all(|byte| byte.is_ascii_graphic()) {
                return Err(anyhow::anyhow!("service type name must be printable non-space ASCII"));
            }

            let mut id = ServiceTypeId::default();
            id[..value.len()].copy_from_slice(value.as_bytes());
            id
        }
    };

    if id == ServiceTypeId::default() {
        // The registry contract rejects this id with `ZeroServiceType`.
        return Err(anyhow::anyhow!("the zero service type id is never registered"));
    }

    Ok(id)
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ServiceArgs {
    /// Service type, either as its ASCII name such as `gvpn:exit`, or as a 0x-prefixed 32-byte hex id.
    #[arg(short, long)]
    service_type: Option<String>,
    /// Chain address of the node offering the service.
    #[arg(short, long, value_parser = clap::value_parser!(Address))]
    node: Option<Address>,
}

impl TryFrom<ServiceArgs> for ServiceSelector {
    type Error = anyhow::Error;

    fn try_from(value: ServiceArgs) -> Result<Self, Self::Error> {
        let ServiceArgs { service_type, node } = value;
        Ok(match (service_type, node) {
            (Some(service_type), None) => ServiceSelector::ServiceType(parse_service_type(&service_type)?),
            (None, Some(node)) => ServiceSelector::Node(node.into()),
            (Some(service_type), Some(node)) => ServiceSelector::ServiceTypeAndNode {
                service_type: parse_service_type(&service_type)?,
                node: node.into(),
            },
            (None, None) => ServiceSelector::Any,
        })
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ServiceTypeArgs {
    /// Service type, either as its ASCII name such as `gvpn:exit`, or as a 0x-prefixed 32-byte hex id.
    ///
    /// Omit to address every registered type.
    #[arg(short, long)]
    service_type: Option<String>,
}

impl TryFrom<ServiceTypeArgs> for Option<ServiceTypeId> {
    type Error = anyhow::Error;

    fn try_from(value: ServiceTypeArgs) -> Result<Self, Self::Error> {
        value.service_type.as_deref().map(parse_service_type).transpose()
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ChannelArgs {
    /// Channels with the given source key id.
    #[arg(short, long)]
    src_key_id: Option<u32>,
    /// Channels with the given destination key id.
    #[arg(short, long)]
    dst_key_id: Option<u32>,
    /// Channels with the given status.
    #[arg(short, long, value_enum)]
    allowed_states: Option<ChannelAllowedStates>,
    /// Channel with the given ID.
    #[arg(short, long)]
    channel_id: Option<String>,
    /// Restrict to channels belonging to the given safe address.
    #[arg(long, value_parser = clap::value_parser!(Address))]
    safe_address: Option<Address>,
}

impl TryFrom<ChannelArgs> for ChannelSelector {
    type Error = anyhow::Error;

    fn try_from(value: ChannelArgs) -> Result<Self, Self::Error> {
        let ChannelArgs {
            src_key_id,
            dst_key_id,
            channel_id,
            allowed_states,
            safe_address,
        } = value;
        Ok(ChannelSelector {
            filter: match (src_key_id, dst_key_id, channel_id) {
                (Some(src), None, None) => Some(ChannelFilter::SourceKeyId(src)),
                (None, Some(dst), None) => Some(ChannelFilter::DestinationKeyId(dst)),
                (Some(src), Some(dst), None) => Some(ChannelFilter::SourceAndDestinationKeyIds(src, dst)),
                (None, None, Some(channel_id)) => {
                    let channel_id = channel_id.to_lowercase();
                    Some(ChannelFilter::ChannelId(
                        hex::decode(channel_id.trim_start_matches("0x"))?
                            .try_into()
                            .map_err(|_| anyhow::anyhow!("invalid channel ID"))?,
                    ))
                }
                (None, None, None) => None,
                _ => {
                    return Err(anyhow::anyhow!(
                        "Invalid combination of --src-key-id, --dst-key-id and --channel-id given."
                    ));
                }
            },
            status: allowed_states.map(|s| match s {
                ChannelAllowedStates::Open => ChannelStatus::Open,
                ChannelAllowedStates::PendingToClose => ChannelStatus::PendingToClose,
                ChannelAllowedStates::Closed => ChannelStatus::Closed,
            }),
            safe_address: safe_address.map(ChainAddress::from),
        })
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct AccountArgs {
    /// Account chain address.
    #[arg(short, long, value_parser = clap::value_parser!(Address), group = "selector")]
    address: Option<Address>,
    /// Account packet key (either in hex or as a Peer ID).
    #[arg(short, long, group = "selector")]
    packet_key: Option<String>,
    /// Account key id.
    #[arg(short, long, group = "selector")]
    key_id: Option<u32>,
    /// Show peer IDs for accounts.
    #[arg(short, long)]
    show_peer_ids: bool,
}

fn parse_packet_key(value: &str) -> anyhow::Result<OffchainPublicKey> {
    if let Ok(key) = OffchainPublicKey::from_hex(value) {
        Ok(key)
    } else if let Ok(peer_id) = value.parse() {
        Ok(OffchainPublicKey::from_peerid(&peer_id)?)
    } else {
        Err(anyhow::anyhow!("Cannot parse packet key or Peer ID: {value}"))
    }
}

impl TryFrom<AccountArgs> for AccountSelector {
    type Error = anyhow::Error;

    fn try_from(value: AccountArgs) -> Result<Self, Self::Error> {
        let AccountArgs {
            address,
            key_id,
            packet_key,
            ..
        } = value;
        match (address, key_id, packet_key) {
            (Some(address), None, None) => Ok(AccountSelector::Address(address.into())),
            (None, Some(key_id), None) => Ok(AccountSelector::KeyId(key_id)),
            (None, None, Some(packet_key)) => {
                if let Ok(key) = OffchainPublicKey::from_hex(&packet_key) {
                    eprintln!("Corresponding PeerId: {}", key.to_peerid_str());
                    Ok(AccountSelector::PacketKey(key.into()))
                } else if let Ok(key) = OffchainPublicKey::from_peerid(&packet_key.parse()?) {
                    eprintln!("Corresponding packet key: {}", key.to_hex());
                    Ok(AccountSelector::PacketKey(key.into()))
                } else {
                    Err(anyhow::anyhow!("Cannot parse packet key: {packet_key}"))
                }
            }
            (None, None, None) => Ok(AccountSelector::Any),
            _ => Err(anyhow::anyhow!("Cannot specify both --address and --key-id.")),
        }
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct NodeOverviewArgs {
    /// Node chain key, hex packet key, or Peer ID.
    node: String,
}

impl TryFrom<NodeOverviewArgs> for AccountSelector {
    type Error = anyhow::Error;

    fn try_from(value: NodeOverviewArgs) -> Result<Self, Self::Error> {
        if let Ok(address) = value.node.parse::<Address>() {
            Ok(AccountSelector::Address(address.into()))
        } else {
            Ok(AccountSelector::PacketKey(parse_packet_key(&value.node)?.into()))
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AltAccount {
    pub chain_key: String,
    pub keyid: i32,
    pub multi_addresses: Vec<String>,
    pub packet_key: String,
    pub peer_id: String,
    pub safe_address: Option<String>,
}

impl TryFrom<Account> for AltAccount {
    type Error = anyhow::Error;

    fn try_from(value: Account) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_key: value.chain_key,
            keyid: value.keyid,
            multi_addresses: value.multi_addresses,
            peer_id: OffchainPublicKey::from_str(&value.packet_key)?.to_peerid_str(),
            packet_key: value.packet_key,
            safe_address: value.safe_address,
        })
    }
}

fn either_err<A, B>(either: Either<(<A as TryFuture>::Error, B), (<B as TryFuture>::Error, A)>) -> anyhow::Error
where
    A: TryFuture,
    B: TryFuture,
    A::Error: Into<anyhow::Error>,
    B::Error: Into<anyhow::Error>,
{
    match either {
        Either::Left((e, _)) => e.into(),
        Either::Right((e, _)) => e.into(),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_level(true)
        .compact()
        .init();

    let cli = Cli::parse();
    let blokli_client = BlokliClient::new(cli.url, Default::default());

    let exit_fut = tokio::signal::ctrl_c().inspect_ok(|_| {
        eprintln!("\nInterrupted.");
    });
    pin_mut!(exit_fut);

    match cli.command {
        Commands::Query { target } => {
            let exec_fut = target.execute(&blokli_client, cli.format);
            pin_mut!(exec_fut);

            if let Either::Right((value, _)) = futures::future::try_select(exit_fut, exec_fut)
                .map_err(either_err)
                .await?
            {
                println!("{value}");
            }
        }
        Commands::Subscribe { target } => {
            let stream_fut = target.execute(&blokli_client, cli.format)?.for_each(|v| {
                println!("{v}");
                futures::future::ready(())
            });
            pin_mut!(stream_fut);

            futures::future::select(exit_fut, stream_fut).await;
        }
        Commands::Transaction {
            payload,
            wait_for_confirmation,
            track,
        } => {
            let payload = if let Some(payload) = payload {
                let payload = payload.to_lowercase();
                hex::decode(payload.trim_start_matches("0x"))?
            } else {
                eprintln!("Waiting for transaction payload from stdin...");
                let mut payload = Vec::new();
                tokio::io::stdin().read_to_end(&mut payload).await?;
                payload
            };

            if let Some(confirmations) = wait_for_confirmation {
                let tx_fut = blokli_client.submit_and_confirm_transaction(&payload, confirmations);
                pin_mut!(tx_fut);

                if let Either::Right((receipt, _)) = futures::future::try_select(exit_fut, tx_fut)
                    .map_err(either_err)
                    .await?
                {
                    println!("{}", hex::encode(receipt));
                }
            } else if track {
                let track_tx_fut = tokio::time::timeout(
                    Duration::from_secs(10),
                    blokli_client.submit_and_track_transaction(&payload),
                )
                .map_err(anyhow::Error::from)
                .and_then(|res| futures::future::ready(res.map_err(anyhow::Error::from)))
                .inspect_ok(|tx_id| eprintln!("{tx_id}"))
                .and_then(|tracking_id| {
                    blokli_client
                        .track_transaction(tracking_id, Duration::from_secs(60))
                        .map_err(anyhow::Error::from)
                });
                pin_mut!(track_tx_fut);

                if let Either::Right((transaction, _)) = futures::future::try_select(exit_fut, track_tx_fut)
                    .map_err(either_err)
                    .await?
                {
                    println!("{}", cli.format.serialize(transaction)?)
                }
            } else {
                let tx_fut = blokli_client.submit_transaction(&payload);
                pin_mut!(tx_fut);

                if let Either::Right((receipt, _)) = futures::future::try_select(exit_fut, tx_fut)
                    .map_err(either_err)
                    .await?
                {
                    println!("{}", hex::encode(receipt));
                }
            }
        }
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use blokli_client::api::{AccountSelector, ServiceSelector, ServiceTypeId};
    use hopr_types::{crypto::types::OffchainPublicKey, primitive::prelude::ToHex};

    use super::{NodeOverviewArgs, ServiceArgs, ServiceTypeArgs, parse_service_type};

    /// `bytes32("gvpn:exit")`, the canonical id of the GnosisVPN exit-node service.
    const GVPN_EXIT: ServiceTypeId = [
        0x67, 0x76, 0x70, 0x6e, 0x3a, 0x65, 0x78, 0x69, 0x74, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ];

    #[test]
    fn service_type_parses_from_an_ascii_name_and_from_hex() -> anyhow::Result<()> {
        assert_eq!(parse_service_type("gvpn:exit")?, GVPN_EXIT);
        assert_eq!(
            parse_service_type("0x6776706e3a657869740000000000000000000000000000000000000000000000")?,
            GVPN_EXIT
        );
        Ok(())
    }

    /// A prefixed value is always an id, never a name, so a short one is an error rather than a type literally
    /// called `0x1234`.
    #[test]
    fn service_type_rejects_a_prefixed_value_that_is_not_a_full_id() {
        assert!(parse_service_type("0x1234").is_err());
        assert!(parse_service_type("0xnothex").is_err());
    }

    #[test]
    fn service_type_rejects_names_outside_the_convention() {
        for invalid in ["", "gvpn exit", &"a".repeat(33)] {
            assert!(
                parse_service_type(invalid).is_err(),
                "'{invalid}' should not parse as a service type"
            );
        }
    }

    #[test]
    fn service_type_rejects_the_zero_id() {
        assert!(parse_service_type(&format!("0x{}", "00".repeat(32))).is_err());
    }

    #[test]
    fn service_args_combine_both_filters() -> anyhow::Result<()> {
        let selector = ServiceSelector::try_from(ServiceArgs {
            service_type: Some("gvpn:exit".to_string()),
            node: Some("0x1111111111111111111111111111111111111111".parse()?),
        })?;

        assert!(matches!(
            selector,
            ServiceSelector::ServiceTypeAndNode { service_type, node }
                if service_type == GVPN_EXIT && node == [0x11; 20]
        ));
        Ok(())
    }

    #[test]
    fn service_args_without_filters_select_everything() -> anyhow::Result<()> {
        let selector = ServiceSelector::try_from(ServiceArgs {
            service_type: None,
            node: None,
        })?;

        assert!(matches!(selector, ServiceSelector::Any));
        Ok(())
    }

    #[test]
    fn service_type_args_are_optional() -> anyhow::Result<()> {
        assert_eq!(
            Option::<ServiceTypeId>::try_from(ServiceTypeArgs { service_type: None })?,
            None
        );
        assert_eq!(
            Option::<ServiceTypeId>::try_from(ServiceTypeArgs {
                service_type: Some("gvpn:exit".to_string())
            })?,
            Some(GVPN_EXIT)
        );
        Ok(())
    }

    #[test]
    fn node_overview_selector_accepts_chain_key() -> anyhow::Result<()> {
        let selector = AccountSelector::try_from(NodeOverviewArgs {
            node: "0x1111111111111111111111111111111111111111".to_string(),
        })?;

        assert!(matches!(selector, AccountSelector::Address(address) if address == [0x11; 20]));
        Ok(())
    }

    #[test]
    fn node_overview_selector_accepts_packet_key_and_peer_id() -> anyhow::Result<()> {
        let packet_key =
            OffchainPublicKey::from_hex("30dc46df1f429b9c0d1d6d81198420f3af92348e7fe97b003717108b22f8d985")?;
        let expected = packet_key.to_hex();
        let peer_id = packet_key.to_peerid_str();

        for node in [expected, peer_id] {
            let selector = AccountSelector::try_from(NodeOverviewArgs { node })?;
            assert!(matches!(selector, AccountSelector::PacketKey(_)));
        }
        Ok(())
    }

    #[test]
    fn node_overview_selector_rejects_invalid_value() {
        let result = AccountSelector::try_from(NodeOverviewArgs {
            node: "not-a-node".to_string(),
        });

        assert!(result.is_err());
    }
}
