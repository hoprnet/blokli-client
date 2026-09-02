use std::{net::IpAddr, time::Duration};

use anyhow::Result;
use blokli_client::{
    BlokliClient, BlokliClientConfig, BlokliDnsOverride,
    api::{
        BlokliSubscriptionClient, BlokliTransactionClient, ServiceSelector, TransactionTrackingOutcome,
        types::{ChannelStatus, ReadinessState, ServiceTypeUpdateKind, ServiceUpdateKind},
    },
};
use futures::StreamExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use url::Url;

/// `bytes32("gvpn:exit")`, the canonical id of the GnosisVPN exit-node service.
const GVPN_EXIT: [u8; 32] = [
    0x67, 0x76, 0x70, 0x6e, 0x3a, 0x65, 0x78, 0x69, 0x74, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0,
];

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
struct TicketParams {
    ticket_price: String,
    min_ticket_winning_probability: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReadinessEvent(ReadinessState);

impl ReadinessEvent {
    fn format_event(&self) -> String {
        let payload = serde_json::json!({
            "data": {
                "health": match self.0 {
                    ReadinessState::Ready => "READY",
                    ReadinessState::NotReady => "NOT_READY",
                }
            }
        });
        format!("event: next\ndata: {payload}\n\n")
    }
}

impl TicketParams {
    fn format_event(&self) -> String {
        let payload = serde_json::json!({
            "data": {
                "ticketParametersUpdated": {
                    "minTicketWinningProbability": self.min_ticket_winning_probability,
                    "ticketPrice": self.ticket_price,
                }
            }
        });
        format!("event: next\ndata: {payload}\n\n")
    }
}

#[tokio::test]
async fn track_transaction_timeout_preserves_tracking_id() -> Result<()> {
    let tx_id = "018f3f6a-9521-7c5d-8ed6-4a789f936f31";
    let (base_url, server) = spawn_pending_transaction_server(tx_id).await?;
    let client = BlokliClient::new(
        base_url,
        BlokliClientConfig {
            subscription_read_timeout: None,
            subscription_stream_restart_delay: None,
            ..BlokliClientConfig::default()
        },
    );

    let outcome = client
        .track_transaction(tx_id.to_owned(), Duration::from_millis(100))
        .await?;

    assert_eq!(
        outcome,
        TransactionTrackingOutcome::StatusUnknown {
            tx_id: tx_id.to_owned()
        }
    );

    server.await??;
    Ok(())
}

#[tokio::test]
async fn subscribe_ticket_params_recreates_stream_without_loss_or_duplication() -> Result<()> {
    let expected_ticket_params = vec![
        TicketParams {
            ticket_price: "0.0010 wxHOPR".to_string(),
            min_ticket_winning_probability: 0.25,
        },
        TicketParams {
            ticket_price: "0.0020 wxHOPR".to_string(),
            min_ticket_winning_probability: 0.5,
        },
        TicketParams {
            ticket_price: "0.0030 wxHOPR".to_string(),
            min_ticket_winning_probability: 0.75,
        },
    ];
    let stream_batches = vec![
        expected_ticket_params[..2].to_vec(),
        expected_ticket_params[2..].to_vec(),
    ];
    let (base_url, server) = spawn_reconnecting_server(stream_batches).await?;
    let client = BlokliClient::new(
        base_url,
        BlokliClientConfig {
            dns_override: None,
            timeout: Duration::from_secs(2),
            stream_reconnect_timeout: Duration::from_secs(2),
            subscription_read_timeout: Some(Duration::from_secs(2)),
            subscription_tcp_keepalive: Duration::from_secs(15),
            subscription_stream_restart_delay: Some(Duration::from_millis(100)),
        },
    );

    let stream = client.subscribe_ticket_params()?;
    let updates = futures::stream::unfold(stream, |mut stream| async move {
        let update_result = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
        match update_result {
            Ok(Some(Ok(update))) => Some((
                Ok(TicketParams {
                    ticket_price: update.ticket_price.0,
                    min_ticket_winning_probability: update.min_ticket_winning_probability,
                }),
                stream,
            )),
            Ok(Some(Err(error))) => Some((Err(error.into()), stream)),
            Ok(None) | Err(_) => None,
        }
    })
    .take(10)
    .collect::<Vec<Result<TicketParams>>>()
    .await
    .into_iter()
    .collect::<Result<Vec<TicketParams>>>()?;

    assert_eq!(updates, expected_ticket_params);

    server.await??;
    Ok(())
}

#[tokio::test]
async fn subscribe_ticket_params_stays_open_beyond_non_streaming_timeout() -> Result<()> {
    let expected_ticket_params = TicketParams {
        ticket_price: "0.0010 wxHOPR".to_string(),
        min_ticket_winning_probability: 0.25,
    };
    let (base_url, server) =
        spawn_delayed_streaming_server(vec![expected_ticket_params.clone()], Duration::from_millis(250)).await?;
    let client = BlokliClient::new(
        base_url,
        BlokliClientConfig {
            dns_override: None,
            timeout: Duration::from_millis(100),
            stream_reconnect_timeout: Duration::from_secs(2),
            subscription_read_timeout: Some(Duration::from_secs(2)),
            subscription_tcp_keepalive: Duration::from_secs(15),
            subscription_stream_restart_delay: Some(Duration::from_millis(100)),
        },
    );

    let mut stream = client.subscribe_ticket_params()?;
    let update = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("subscription ended before delivering an event"))??;

    assert_eq!(update.ticket_price.0, expected_ticket_params.ticket_price);
    assert_eq!(
        update.min_ticket_winning_probability,
        expected_ticket_params.min_ticket_winning_probability
    );

    server.await??;
    Ok(())
}

#[tokio::test]
async fn subscribe_ticket_params_reconnects_after_read_timeout() -> Result<()> {
    let expected_ticket_params = TicketParams {
        ticket_price: "0.0010 wxHOPR".to_string(),
        min_ticket_winning_probability: 0.25,
    };
    let (base_url, server) = spawn_timed_out_then_reconnecting_server(expected_ticket_params.clone()).await?;
    let client = BlokliClient::new(
        base_url,
        BlokliClientConfig {
            dns_override: None,
            timeout: Duration::from_secs(2),
            stream_reconnect_timeout: Duration::from_millis(250),
            subscription_read_timeout: Some(Duration::from_millis(100)),
            subscription_tcp_keepalive: Duration::from_secs(15),
            subscription_stream_restart_delay: Some(Duration::from_millis(100)),
        },
    );

    let mut stream = client.subscribe_ticket_params()?;
    let update = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("subscription ended before reconnecting"))??;

    assert_eq!(update.ticket_price.0, expected_ticket_params.ticket_price);
    assert_eq!(
        update.min_ticket_winning_probability,
        expected_ticket_params.min_ticket_winning_probability
    );

    server.await??;
    Ok(())
}

#[tokio::test]
async fn subscribe_health_streams_state_updates() -> Result<()> {
    let events = [
        ReadinessEvent(ReadinessState::NotReady),
        ReadinessEvent(ReadinessState::Ready),
    ];
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base_url = Url::parse(&format!("http://{}", listener.local_addr()?))?;

    let server = tokio::spawn(async move {
        let body = events.iter().map(ReadinessEvent::format_event).collect::<String>();
        let response = format_sse_response(&body);
        let (mut conn, _) = listener.accept().await?;
        conn.write_all(response.as_bytes()).await?;
        conn.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    });

    let client = BlokliClient::new(base_url, BlokliClientConfig::default());
    let updates = client
        .subscribe_health()?
        .take(2)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(updates, events.iter().map(|event| event.0).collect::<Vec<_>>());

    server.await??;
    Ok(())
}

#[tokio::test]
async fn subscribe_ticket_params_uses_dns_override() -> Result<()> {
    let expected_ticket_params = TicketParams {
        ticket_price: "0.0010 wxHOPR".to_string(),
        min_ticket_winning_probability: 0.25,
    };
    let (base_url, server) = spawn_dns_override_streaming_server(vec![expected_ticket_params.clone()]).await?;
    let expected_host = format!(
        "{}:{}",
        base_url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("missing base URL host"))?,
        base_url
            .port_or_known_default()
            .ok_or_else(|| anyhow::anyhow!("missing base URL port"))?,
    );
    let client = BlokliClient::new(
        base_url,
        BlokliClientConfig {
            dns_override: Some(BlokliDnsOverride {
                ip: IpAddr::from([127, 0, 0, 1]),
                port: None,
            }),
            timeout: Duration::from_secs(2),
            stream_reconnect_timeout: Duration::from_secs(2),
            subscription_read_timeout: Some(Duration::from_secs(2)),
            subscription_tcp_keepalive: Duration::from_secs(15),
            subscription_stream_restart_delay: Some(Duration::from_millis(100)),
        },
    );

    let mut stream = client.subscribe_ticket_params()?;
    let update = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("subscription ended before delivering an event"))??;

    assert_eq!(update.ticket_price.0, expected_ticket_params.ticket_price);
    assert_eq!(
        update.min_ticket_winning_probability,
        expected_ticket_params.min_ticket_winning_probability
    );

    let request = server.await??;
    assert!(request.contains(&format!("\r\nhost: {expected_host}\r\n")));

    Ok(())
}

#[tokio::test]
async fn subscribe_graph_forwards_closed_channel_entries() -> Result<()> {
    let channel_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let (base_url, server) = spawn_single_streaming_server(format_graph_event(channel_id, "CLOSED")).await?;
    let client = BlokliClient::new(
        base_url,
        BlokliClientConfig {
            dns_override: None,
            timeout: Duration::from_secs(2),
            stream_reconnect_timeout: Duration::from_secs(2),
            subscription_read_timeout: Some(Duration::from_secs(2)),
            subscription_tcp_keepalive: Duration::from_secs(15),
            subscription_stream_restart_delay: Some(Duration::from_millis(100)),
        },
    );

    let mut stream = client.subscribe_graph()?;
    let entry = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("subscription ended before delivering graph event"))??;

    assert_eq!(entry.channel.concrete_channel_id, channel_id);
    assert_eq!(entry.channel.status, ChannelStatus::Closed);
    assert_eq!(entry.source.keyid, 1);
    assert_eq!(entry.destination.keyid, 2);

    server.await??;
    Ok(())
}

#[cfg(feature = "curvy")]
#[tokio::test]
async fn subscribe_curvy_pending_notes_preserves_sdk_scanning_fields() -> Result<()> {
    let body = format_curvy_event(
        "curvyPendingNote",
        serde_json::json!({
            "noteId": "0x000000000000000000000000000000000000000000000000000000000000002a",
            "ephemeralKey": ["11", "12"],
            "viewTag": 7,
            "tokenId": "1",
            "amount": "1000",
            "isPlaintext": false,
            "position": curvy_position(10, 2, 7, 3),
        }),
    );
    let (base_url, server) = spawn_single_streaming_server(body).await?;
    let client = BlokliClient::new(
        base_url,
        BlokliClientConfig {
            subscription_stream_restart_delay: None,
            ..BlokliClientConfig::default()
        },
    );

    let note = tokio::time::timeout(
        Duration::from_secs(2),
        client.subscribe_curvy_pending_notes(Some(9))?.next(),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("candidate stream ended"))??;
    assert_eq!(
        note.note_id.0,
        "0x000000000000000000000000000000000000000000000000000000000000002a"
    );
    assert_eq!(note.ephemeral_key[0].0, "11");
    assert_eq!(note.ephemeral_key[1].0, "12");
    assert_eq!(note.view_tag, 7);
    assert_eq!(note.token_id.0, "1");
    assert_eq!(note.amount.0, "1000");
    assert!(!note.is_plaintext);
    assert_eq!(note.position.event_item_index.0, "3");

    server.await??;
    Ok(())
}

#[tokio::test]
async fn subscribe_services_forwards_a_registration() -> Result<()> {
    let (base_url, server) = spawn_single_streaming_server(format_service_event(
        "REGISTERED",
        Some(serde_json::json!({
            "serviceType": "gvpn:exit",
            "node": "0x1111111111111111111111111111111111111111",
            "safe": "0x3333333333333333333333333333333333333333",
            "metadata": "0xdeadbeef",
            "registeredAt": "1700000000",
            "updatedAt": "1700000000",
        })),
    ))
    .await?;
    let client = BlokliClient::new(base_url, BlokliClientConfig::default());

    let mut stream = client.subscribe_services(ServiceSelector::ServiceType(GVPN_EXIT))?;
    let update = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("subscription ended before delivering a service event"))??;

    assert_eq!(update.kind, ServiceUpdateKind::Registered);
    assert_eq!(update.service_type, "gvpn:exit");
    assert_eq!(update.node, "0x1111111111111111111111111111111111111111");
    assert_eq!(
        update.entry.as_ref().map(|entry| entry.metadata.as_str()),
        Some("0xdeadbeef")
    );

    server.await??;
    Ok(())
}

/// A deregistration carries no entry, which is the reason the payload is discriminated at all: a bare
/// `ServiceEntry` cannot express the removal of an entry.
#[tokio::test]
async fn subscribe_services_forwards_a_deregistration_without_an_entry() -> Result<()> {
    let (base_url, server) = spawn_single_streaming_server(format_service_event("DEREGISTERED", None)).await?;
    let client = BlokliClient::new(base_url, BlokliClientConfig::default());

    let mut stream = client.subscribe_services(ServiceSelector::Node([0x11; 20]))?;
    let update = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("subscription ended before delivering a service event"))??;

    assert_eq!(update.kind, ServiceUpdateKind::Deregistered);
    assert!(update.entry.is_none());

    server.await??;
    Ok(())
}

#[cfg(feature = "curvy")]
#[tokio::test]
async fn subscribe_curvy_committed_notes_preserves_correlation_fields() -> Result<()> {
    let body = format_curvy_event(
        "curvyCommittedNote",
        serde_json::json!({
            "noteId": "0x000000000000000000000000000000000000000000000000000000000000002a",
            "batchIndex": "0x0000000000000000000000000000000000000000000000000000000000000009",
            "leafIndex": "41",
            "position": curvy_position(11, 0, 1, 0),
        }),
    );
    let (base_url, server) = spawn_single_streaming_server(body).await?;
    let client = BlokliClient::new(
        base_url,
        BlokliClientConfig {
            subscription_stream_restart_delay: None,
            ..BlokliClientConfig::default()
        },
    );

    let note = tokio::time::timeout(
        Duration::from_secs(2),
        client.subscribe_curvy_committed_notes(Some(9))?.next(),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("completion stream ended"))??;
    assert_eq!(
        note.note_id.0,
        "0x000000000000000000000000000000000000000000000000000000000000002a"
    );
    assert_eq!(
        note.batch_index.0,
        "0x0000000000000000000000000000000000000000000000000000000000000009"
    );
    assert_eq!(note.leaf_index.0, "41");

    server.await??;
    Ok(())
}

#[tokio::test]
async fn subscribe_service_types_forwards_a_registry_wide_change() -> Result<()> {
    let payload = serde_json::json!({
        "data": {
            "serviceTypeUpdated": {
                "kind": "REGISTRY_POINTER_CHANGED",
                "serviceType": null,
                "config": null,
                "registryConfig": {
                    "typeRegistrationFee": "1 wxHOPR",
                    "nodeSafeRegistry": "0x5555555555555555555555555555555555555555",
                },
            },
        },
    });
    let (base_url, server) = spawn_single_streaming_server(format!("event: next\ndata: {payload}\n\n")).await?;
    let client = BlokliClient::new(base_url, BlokliClientConfig::default());

    let mut stream = client.subscribe_service_types(None)?;
    let update = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("subscription ended before delivering a service type event"))??;

    assert_eq!(update.kind, ServiceTypeUpdateKind::RegistryPointerChanged);
    assert!(update.service_type.is_none());
    assert!(update.config.is_none());
    assert_eq!(
        update
            .registry_config
            .as_ref()
            .map(|config| config.node_safe_registry.as_str()),
        Some("0x5555555555555555555555555555555555555555")
    );

    server.await??;
    Ok(())
}

#[tokio::test]
async fn subscribe_service_registry_config_forwards_complete_state() -> Result<()> {
    let payload = serde_json::json!({
        "data": {
            "serviceRegistryConfigUpdated": {
                "typeRegistrationFee": "1 wxHOPR",
                "nodeSafeRegistry": "0x5555555555555555555555555555555555555555",
            },
        },
    });
    let (base_url, server) = spawn_single_streaming_server(format!("event: next\ndata: {payload}\n\n")).await?;
    let client = BlokliClient::new(base_url, BlokliClientConfig::default());

    let mut stream = client.subscribe_service_registry_config()?;
    let config = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("subscription ended before delivering registry configuration"))??;

    assert_eq!(config.type_registration_fee, "1 wxHOPR");
    assert_eq!(config.node_safe_registry, "0x5555555555555555555555555555555555555555");

    server.await??;
    Ok(())
}

fn format_service_event(kind: &str, entry: Option<serde_json::Value>) -> String {
    let payload = serde_json::json!({
        "data": {
            "serviceUpdated": {
                "kind": kind,
                "serviceType": "gvpn:exit",
                "node": "0x1111111111111111111111111111111111111111",
                "entry": entry,
            },
        },
    });
    format!("event: next\ndata: {payload}\n\n")
}

async fn spawn_reconnecting_server(
    event_batches: Vec<Vec<TicketParams>>,
) -> Result<(Url, tokio::task::JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base_url = Url::parse(&format!("http://{}", listener.local_addr()?))?;

    let server = tokio::spawn(async move {
        let responses = event_batches
            .iter()
            .map(|events| format_sse_response(&format_ticket_params_events(events)));

        for response in responses {
            let (mut conn, _) = listener.accept().await?;
            conn.write_all(response.as_bytes()).await?;
            conn.shutdown().await?;
        }

        Ok(())
    });

    Ok((base_url, server))
}

async fn spawn_dns_override_streaming_server(
    events: Vec<TicketParams>,
) -> Result<(Url, tokio::task::JoinHandle<Result<String>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base_url = Url::parse(&format!(
        "http://blokli-stream.invalid:{}",
        listener.local_addr()?.port()
    ))?;

    let server = tokio::spawn(async move {
        let body = format_ticket_params_events(&events);
        let (mut conn, _) = listener.accept().await?;
        let request = read_http_request_head(&mut conn).await?;
        conn.write_all(format_sse_response(&body).as_bytes()).await?;
        conn.shutdown().await?;
        Ok(request)
    });

    Ok((base_url, server))
}

async fn read_http_request_head(conn: &mut tokio::net::TcpStream) -> Result<String> {
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0_u8; 1024];
        let n = conn.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    Ok(String::from_utf8_lossy(&buf).to_ascii_lowercase())
}

async fn spawn_delayed_streaming_server(
    events: Vec<TicketParams>,
    initial_delay: Duration,
) -> Result<(Url, tokio::task::JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base_url = Url::parse(&format!("http://{}", listener.local_addr()?))?;

    let server = tokio::spawn(async move {
        let response_headers = concat!(
            "HTTP/1.1 200 OK\r\n",
            "content-type: text/event-stream\r\n",
            "cache-control: no-cache\r\n",
            "connection: close\r\n",
            "\r\n",
        )
        .to_string();
        let body = format_ticket_params_events(&events);
        let (mut conn, _) = listener.accept().await?;
        conn.write_all(response_headers.as_bytes()).await?;
        tokio::time::sleep(initial_delay).await;
        conn.write_all(body.as_bytes()).await?;
        conn.shutdown().await?;
        Ok(())
    });

    Ok((base_url, server))
}

async fn spawn_timed_out_then_reconnecting_server(
    event: TicketParams,
) -> Result<(Url, tokio::task::JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base_url = Url::parse(&format!("http://{}", listener.local_addr()?))?;

    let server = tokio::spawn(async move {
        let response_headers = concat!(
            "HTTP/1.1 200 OK\r\n",
            "content-type: text/event-stream\r\n",
            "cache-control: no-cache\r\n",
            "connection: close\r\n",
            "\r\n",
        )
        .to_string();

        let (mut first_conn, _) = listener.accept().await?;
        first_conn.write_all(response_headers.as_bytes()).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        first_conn.shutdown().await?;

        let (mut second_conn, _) = listener.accept().await?;
        let body = format_ticket_params_events(&[event]);
        second_conn
            .write_all(format!("{response_headers}{body}").as_bytes())
            .await?;
        second_conn.shutdown().await?;

        Ok(())
    });

    Ok((base_url, server))
}

async fn spawn_single_streaming_server(body: String) -> Result<(Url, tokio::task::JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base_url = Url::parse(&format!("http://{}", listener.local_addr()?))?;

    let server = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await?;
        conn.write_all(format_sse_response(&body).as_bytes()).await?;
        conn.shutdown().await?;
        Ok(())
    });

    Ok((base_url, server))
}

async fn spawn_pending_transaction_server(tx_id: &str) -> Result<(Url, tokio::task::JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base_url = Url::parse(&format!("http://{}", listener.local_addr()?))?;
    let payload = serde_json::json!({
        "data": {
            "transactionUpdated": {
                "id": tx_id,
                "status": "SUBMITTED",
                "submittedAt": "2026-09-01T07:07:03Z",
                "transactionHash": "0x0101010101010101010101010101010101010101010101010101010101010101",
                "safeExecution": null,
            },
        },
    });

    let server = tokio::spawn(async move {
        let response_headers = concat!(
            "HTTP/1.1 200 OK\r\n",
            "content-type: text/event-stream\r\n",
            "cache-control: no-cache\r\n",
            "connection: close\r\n",
            "\r\n",
        );
        let body = format!("event: next\ndata: {payload}\n\n");
        let (mut conn, _) = listener.accept().await?;
        conn.write_all(format!("{response_headers}{body}").as_bytes()).await?;
        tokio::time::sleep(Duration::from_millis(250)).await;
        conn.shutdown().await?;
        Ok(())
    });

    Ok((base_url, server))
}

fn format_graph_event(channel_id: &str, status: &str) -> String {
    let payload = serde_json::json!({
        "data": {
            "openedChannelGraphUpdated": {
                "channel": {
                    "balance": "0 wxHOPR",
                    "closureTime": null,
                    "concreteChannelId": channel_id,
                    "destination": 2,
                    "epoch": 1,
                    "source": 1,
                    "status": status,
                    "ticketIndex": "0",
                },
                "destination": {
                    "chainKey": "0x2222222222222222222222222222222222222222",
                    "keyid": 2,
                    "multiAddresses": [],
                    "packetKey": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "safeAddress": null,
                },
                "source": {
                    "chainKey": "0x1111111111111111111111111111111111111111",
                    "keyid": 1,
                    "multiAddresses": [],
                    "packetKey": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "safeAddress": null,
                },
            },
        },
    });
    format!("event: next\ndata: {payload}\n\n")
}

#[cfg(feature = "curvy")]
fn format_curvy_event(field: &str, note: serde_json::Value) -> String {
    let payload = serde_json::json!({
        "data": {
            (field): note,
        },
    });
    format!("event: next\ndata: {payload}\n\n")
}

#[cfg(feature = "curvy")]
fn curvy_position(block: u64, transaction_index: u64, log_index: u64, event_item_index: u64) -> serde_json::Value {
    serde_json::json!({
        "transactionHash": format!("0x{block:064x}"),
        "blockHash": format!("0x{:064x}", block + 1),
        "block": block.to_string(),
        "transactionIndex": transaction_index.to_string(),
        "logIndex": log_index.to_string(),
        "eventItemIndex": event_item_index.to_string(),
    })
}

fn format_ticket_params_events(events: &[TicketParams]) -> String {
    events
        .iter()
        .map(|event| event.format_event())
        .collect::<Vec<String>>()
        .join("")
}

fn format_sse_response(body: &str) -> String {
    format!(
        concat!(
            "HTTP/1.1 200 OK\r\n",
            "content-type: text/event-stream\r\n",
            "cache-control: no-cache\r\n",
            "connection: close\r\n",
            "content-length: {}\r\n",
            "\r\n",
            "{}",
        ),
        body.len(),
        body,
    )
}
