use std::net::IpAddr;

use blokli_client::{
    BlokliClient, BlokliClientConfig, BlokliDnsOverride,
    api::{
        BlokliQueryClient, SafeSelector, ServiceSelector,
        types::{Token, Uint64},
    },
};
use mockito::Matcher;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::common::RequestRecorder;

mod common;

#[tokio::test]
async fn query_native_balance() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;

    let cli = BlokliClient::new(server.url().parse()?, Default::default());

    let recorder = RequestRecorder::default();

    let balance_mock = server
        .mock("POST", "/graphql")
        .match_body(Matcher::Regex("QueryNativeBalance".into()))
        .with_status(200)
        .match_request(recorder.as_matcher())
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
              "data": {
                "nativeBalance": {
                  "__typename": "NativeBalance",
                  "balance": "1234567890"
                }
              }
            }
        "#,
        )
        .create_async()
        .await;

    let balance = cli.query_native_balance(&[1u8; 20]).await?;
    assert_eq!("1234567890", balance.balance.0);

    balance_mock.assert_async().await;

    insta::assert_yaml_snapshot!(recorder.requests());

    Ok(())
}

#[tokio::test]
async fn query_token_balance() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;

    let cli = BlokliClient::new(server.url().parse()?, Default::default());

    let recorder = RequestRecorder::default();

    let balance_mock = server
        .mock("POST", "/graphql")
        .match_body(Matcher::Regex("QueryHoprBalance".into()))
        .with_status(200)
        .match_request(recorder.as_matcher())
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
              "data": {
                "hoprBalance": {
                  "__typename": "HoprBalance",
                  "balance": "1234567890"
                }
              }
            }
        "#,
        )
        .create_async()
        .await;

    let balance = cli.query_token_balance(&[1u8; 20], Token::WxHOPR).await?;
    assert_eq!("1234567890", balance.balance.0);

    balance_mock.assert_async().await;

    insta::assert_yaml_snapshot!(recorder.requests());

    Ok(())
}

#[tokio::test]
async fn query_safe_returns_safes_list() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let cli = BlokliClient::new(server.url().parse()?, Default::default());

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
              "data": {
                "safeBy": {
                  "__typename": "SafesList",
                  "safes": [
                    {
                      "address": "0x1111111111111111111111111111111111111111",
                      "chainKey": "0x3333333333333333333333333333333333333333",
                      "owners": ["0x7777777777777777777777777777777777777777"],
                      "moduleAddress": "0x2222222222222222222222222222222222222222",
                      "registeredNodes": ["0x8888888888888888888888888888888888888888"],
                      "threshold": "2"
                    }
                  ]
                }
              }
            }"#,
        )
        .create_async()
        .await;

    let safes = cli.query_safe(SafeSelector::Owner([0x77; 20])).await?;
    assert_eq!(safes.len(), 1);
    assert_eq!(safes[0].address, "0x1111111111111111111111111111111111111111");
    assert_eq!(safes[0].owners, vec!["0x7777777777777777777777777777777777777777"]);

    mock.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn query_safe_returns_empty_vec_when_safe_by_is_null() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let cli = BlokliClient::new(server.url().parse()?, Default::default());

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
              "data": {
                "safeBy": null
              }
            }"#,
        )
        .create_async()
        .await;

    let safes = cli.query_safe(SafeSelector::Owner([0x77; 20])).await?;
    assert!(safes.is_empty());

    mock.assert_async().await;
    Ok(())
}

/// `bytes32("gvpn:exit")`, the canonical id of the GnosisVPN exit-node service.
const GVPN_EXIT: [u8; 32] = [
    0x67, 0x76, 0x70, 0x6e, 0x3a, 0x65, 0x78, 0x69, 0x74, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0,
];

#[tokio::test]
async fn query_services_returns_registry_entries() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let cli = BlokliClient::new(server.url().parse()?, Default::default());
    let recorder = RequestRecorder::default();

    let mock = server
        .mock("POST", "/graphql")
        .match_body(Matcher::Regex("QueryServices".into()))
        .match_request(recorder.as_matcher())
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
              "data": {
                "services": {
                  "__typename": "ServicesList",
                  "services": [
                    {
                      "serviceType": "gvpn:exit",
                      "node": "0x1111111111111111111111111111111111111111",
                      "safe": "0x3333333333333333333333333333333333333333",
                      "metadata": "0xdeadbeef",
                      "registeredAt": "1700000000",
                      "updatedAt": "1700000100"
                    }
                  ],
                  "watermark": "1234",
                  "nextCursor": null
                }
              }
            }"#,
        )
        .create_async()
        .await;

    let entries = cli.query_services(ServiceSelector::ServiceType(GVPN_EXIT)).await?;

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].service_type, "gvpn:exit");
    assert_eq!(entries[0].node, "0x1111111111111111111111111111111111111111");
    assert_eq!(entries[0].metadata, "0xdeadbeef");
    assert_eq!(entries[0].registered_at, Uint64("1700000000".into()));
    assert_eq!(entries[0].updated_at, Uint64("1700000100".into()));

    mock.assert_async().await;
    insta::assert_yaml_snapshot!(recorder.requests());

    Ok(())
}

#[tokio::test]
async fn query_services_surfaces_the_missing_filter_error() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let cli = BlokliClient::new(server.url().parse()?, Default::default());

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
              "data": {
                "services": {
                  "__typename": "MissingFilterError",
                  "code": "MISSING_REQUIRED_FILTER",
                  "message": "at least one of serviceType or node must be provided"
                }
              }
            }"#,
        )
        .create_async()
        .await;

    let error = cli
        .query_services(ServiceSelector::Node([0x11; 20]))
        .await
        .expect_err("a MissingFilterError payload must not decode as a success");

    assert!(error.to_string().contains("MISSING_REQUIRED_FILTER"));

    mock.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn query_services_accepts_an_unfiltered_selector() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let cli = BlokliClient::new(server.url().parse()?, Default::default());

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{"data":{"services":{"__typename":"ServicesList","services":[],"watermark":"1234","nextCursor":null}}}"#,
        )
        .create_async()
        .await;

    assert!(cli.query_services(ServiceSelector::Any).await?.is_empty());

    mock.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn count_services_accepts_an_unfiltered_selector() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let cli = BlokliClient::new(server.url().parse()?, Default::default());
    let recorder = RequestRecorder::default();

    let mock = server
        .mock("POST", "/graphql")
        .match_body(Matcher::Regex("QueryServiceCount".into()))
        .match_request(recorder.as_matcher())
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"data":{"serviceCount":{"__typename":"Count","count":7}}}"#)
        .create_async()
        .await;

    assert_eq!(cli.count_services(ServiceSelector::Any).await?, 7);

    mock.assert_async().await;
    insta::assert_yaml_snapshot!(recorder.requests());

    Ok(())
}

#[tokio::test]
async fn query_service_types_returns_type_configuration() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let cli = BlokliClient::new(server.url().parse()?, Default::default());
    let recorder = RequestRecorder::default();

    let mock = server
        .mock("POST", "/graphql")
        .match_body(Matcher::Regex("QueryServiceTypes".into()))
        .match_request(recorder.as_matcher())
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
              "data": {
                "serviceTypes": {
                  "__typename": "ServiceTypesList",
                  "serviceTypes": [
                    {
                      "serviceType": "gvpn:exit",
                      "owner": "0x4444444444444444444444444444444444444444",
                      "requirement": null,
                      "registrationBurn": "1 wxHOPR",
                      "updateBurn": "0 wxHOPR"
                    }
                  ]
                }
              }
            }"#,
        )
        .create_async()
        .await;

    let types = cli.query_service_types(Some(GVPN_EXIT)).await?;

    assert_eq!(types.len(), 1);
    assert_eq!(types[0].service_type, "gvpn:exit");
    assert_eq!(
        types[0].owner.as_deref(),
        Some("0x4444444444444444444444444444444444444444")
    );
    assert!(types[0].requirement.is_none());
    assert_eq!(types[0].registration_burn, "1 wxHOPR");

    mock.assert_async().await;
    insta::assert_yaml_snapshot!(recorder.requests());

    Ok(())
}

#[tokio::test]
async fn query_service_registry_config_returns_current_configuration() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let cli = BlokliClient::new(server.url().parse()?, Default::default());
    let recorder = RequestRecorder::default();

    let mock = server
        .mock("POST", "/graphql")
        .match_body(Matcher::Regex("QueryServiceRegistryConfig".into()))
        .match_request(recorder.as_matcher())
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{
              "data": {
                "serviceRegistryConfig": {
                  "__typename": "ServiceRegistryConfig",
                  "typeRegistrationFee": "1000 wxHOPR",
                  "nodeSafeRegistry": "0x4444444444444444444444444444444444444444"
                }
              }
            }"#,
        )
        .create_async()
        .await;

    let config = cli.query_service_registry_config().await?;

    assert_eq!(config.type_registration_fee, "1000 wxHOPR");
    assert_eq!(config.node_safe_registry, "0x4444444444444444444444444444444444444444");

    mock.assert_async().await;
    insta::assert_yaml_snapshot!(recorder.requests());

    Ok(())
}

#[test]
fn default_config_uses_system_dns() {
    assert_eq!(BlokliClientConfig::default().dns_override, None);
}

#[tokio::test]
async fn query_uses_dns_override_without_rewriting_host() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let base_url = format!("http://blokli.invalid:{port}").parse()?;
    let client = BlokliClient::new(
        base_url,
        BlokliClientConfig {
            dns_override: Some(BlokliDnsOverride {
                ip: IpAddr::from([127, 0, 0, 1]),
                port: None,
            }),
            ..Default::default()
        },
    );

    let server = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await?;
        let request = read_http_request_head(&mut conn).await?;
        conn.write_all(format_json_response(r#"{"data":{"version":"0.19.1"}}"#).as_bytes())
            .await?;
        conn.shutdown().await?;
        anyhow::Ok(request)
    });

    let version = client.query_version().await?;
    let request = server.await??;

    assert_eq!(version, "0.19.1");
    assert!(request.starts_with("post /graphql http/1.1\r\n"));
    assert!(request.contains(&format!("\r\nhost: blokli.invalid:{port}\r\n")));

    Ok(())
}

#[tokio::test]
async fn query_uses_dns_override_with_explicit_port() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listener_port = listener.local_addr()?.port();
    let base_url = "http://blokli.invalid".parse()?;
    let client = BlokliClient::new(
        base_url,
        BlokliClientConfig {
            dns_override: Some(BlokliDnsOverride {
                ip: IpAddr::from([127, 0, 0, 1]),
                port: Some(listener_port),
            }),
            ..Default::default()
        },
    );

    let server = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await?;
        let request = read_http_request_head(&mut conn).await?;
        conn.write_all(format_json_response(r#"{"data":{"version":"0.19.1"}}"#).as_bytes())
            .await?;
        conn.shutdown().await?;
        anyhow::Ok(request)
    });

    let version = client.query_version().await?;
    let request = server.await??;

    assert_eq!(version, "0.19.1");
    assert!(request.starts_with("post /graphql http/1.1\r\n"));
    assert!(request.contains("\r\nhost: blokli.invalid\r\n"));

    Ok(())
}

async fn read_http_request_head(conn: &mut tokio::net::TcpStream) -> anyhow::Result<String> {
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

fn format_json_response(body: &str) -> String {
    format!(
        concat!(
            "HTTP/1.1 200 OK\r\n",
            "content-type: application/json\r\n",
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
