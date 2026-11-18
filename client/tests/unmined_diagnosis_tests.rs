//! Coverage for the unmined diagnosis Blokli attaches to timed out transactions.
//!
//! One small HTTP server answers every operation of a scenario, so tracking over the dedicated SSE connection and
//! the follow-up queries can be checked against the same client. The server records the operations it served.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use blokli_client::{
    BlokliClient, BlokliClientConfig, ErrorKind, TrackingErrorKind,
    api::{BlokliTransactionClient, types::UnminedReason},
    types::TrackedSubmission,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const TX_ID: &str = "018f3f6a-9521-7c5d-8ed6-4a789f936f31";
const SIGNED_TX: &[u8] = &[0x02, 0xF8, 0x6B, 0x01];

/// What the server answers to a `Version` query; `None` is an HTTP 500.
type VersionAnswer = Option<&'static str>;

struct Scenario {
    versions: Mutex<VecDeque<VersionAnswer>>,
    /// Body of the `UnminedDiagnosis` answer; `None` is an HTTP 500.
    diagnosis: Option<serde_json::Value>,
    /// Status of the transaction in a deduplicated submission.
    dedup_status: &'static str,
    requests: Mutex<Vec<String>>,
}

impl Scenario {
    fn new(versions: Vec<VersionAnswer>, diagnosis: Option<serde_json::Value>) -> Arc<Self> {
        Arc::new(Self {
            versions: Mutex::new(versions.into()),
            diagnosis,
            dedup_status: "TIMEOUT",
            requests: Mutex::default(),
        })
    }

    fn count(&self, operation: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|body| body.contains(operation))
            .count()
    }

    fn bodies(&self, operation: &str) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|body| body.contains(operation))
            .cloned()
            .collect()
    }
}

fn transaction(status: &str) -> serde_json::Value {
    serde_json::json!({
        "__typename": "Transaction",
        "id": TX_ID,
        "status": status,
        "submittedAt": "2026-09-01T07:07:03Z",
        "transactionHash": "0x0101010101010101010101010101010101010101010101010101010101010101",
        "safeExecution": null,
    })
}

fn diagnosis_answer(diagnosis: serde_json::Value) -> serde_json::Value {
    serde_json::json!({ "data": { "transaction": { "__typename": "Transaction", "diagnosis": diagnosis } } })
}

fn underpriced() -> serde_json::Value {
    diagnosis_answer(serde_json::json!({
        "reason": "UNDERPRICED",
        "signedFees": { "maxFeePerGas": "1000000000", "maxPriorityFeePerGas": "1000" },
        "recommendedFees": { "maxFeePerGas": "3000000000", "maxPriorityFeePerGas": "2000" },
        "minReplacementFees": { "maxFeePerGas": "", "maxPriorityFeePerGas": "" },
        "retryAfterSeconds": 12,
    }))
}

async fn spawn_server(scenario: Arc<Scenario>) -> Result<url::Url> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base_url = url::Url::parse(&format!("http://{}", listener.local_addr()?))?;

    tokio::spawn(async move {
        loop {
            let Ok((conn, _)) = listener.accept().await else { return };
            let scenario = scenario.clone();
            tokio::spawn(async move {
                let _ = serve(conn, scenario).await;
            });
        }
    });

    Ok(base_url)
}

async fn serve(mut conn: TcpStream, scenario: Arc<Scenario>) -> Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = conn.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        let text = String::from_utf8_lossy(&buf);
        if let Some((head, body)) = text.split_once("\r\n\r\n") {
            let length = head
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|v| v.trim().parse::<usize>())
                })
                .transpose()?
                .unwrap_or(0);
            if body.len() >= length {
                break;
            }
        }
    }
    let request = String::from_utf8_lossy(&buf).into_owned();
    scenario.requests.lock().unwrap().push(request.clone());

    if request.contains("transactionUpdated") {
        let payload = serde_json::json!({ "data": { "s0": transaction("TIMEOUT") } });
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\nconnection: \
             close\r\n\r\nevent: next\ndata: {payload}\n\n"
        );
        conn.write_all(response.as_bytes()).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        conn.shutdown().await?;
        return Ok(());
    }

    let body: Option<serde_json::Value> = if request.contains("QueryVersion") {
        let mut versions = scenario.versions.lock().unwrap();
        let answer = if versions.len() > 1 {
            versions.pop_front().flatten()
        } else {
            versions.front().copied().flatten()
        };
        answer.map(|v| serde_json::json!({ "data": { "version": v } }))
    } else if request.contains("QueryUnminedDiagnosis") {
        scenario.diagnosis.clone()
    } else if request.contains("MutateTrackTransaction") {
        let result = match scenario.dedup_status {
            "NEW" => transaction("SUBMITTED"),
            status => serde_json::json!({
                "__typename": "DeduplicatedTransaction",
                "transaction": transaction(status),
                "operation": "finalize_channel_closure",
            }),
        };
        Some(serde_json::json!({ "data": { "sendTransactionAsync": result } }))
    } else {
        None
    };

    let (status, payload) = match body {
        Some(body) => ("200 OK", body.to_string()),
        None => ("500 Internal Server Error", String::new()),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: \
         close\r\n\r\n{payload}",
        payload.len()
    );
    conn.write_all(response.as_bytes()).await?;
    conn.shutdown().await?;
    Ok(())
}

fn client(base_url: url::Url) -> BlokliClient {
    BlokliClient::new(
        base_url,
        BlokliClientConfig {
            subscription_read_timeout: None,
            subscription_stream_restart_delay: None,
            ..BlokliClientConfig::default()
        },
    )
}

async fn track(client: &BlokliClient) -> blokli_client::BlokliClientError {
    client
        .track_transaction(TX_ID.to_owned(), Duration::from_secs(5))
        .await
        .expect_err("a server timeout is a tracking error")
}

#[tokio::test]
async fn a_timeout_carries_the_diagnosis_when_the_server_supports_it() -> Result<()> {
    let scenario = Scenario::new(vec![Some("0.15.0")], Some(underpriced()));
    let cli = client(spawn_server(scenario.clone()).await?);

    let error = track(&cli).await;

    assert!(matches!(
        error.kind(),
        ErrorKind::TrackingError(TrackingErrorKind::Timeout)
    ));
    let diagnosis = error.unmined_diagnosis().expect("diagnosis is attached");
    assert_eq!(diagnosis.reason, UnminedReason::Underpriced);
    assert_eq!(diagnosis.retry_after_seconds, 12);
    let signed = diagnosis.signed_fees.expect("signed fees");
    assert_eq!(
        (signed.max_fee_per_gas, signed.max_priority_fee_per_gas),
        (1_000_000_000, 1000)
    );
    let recommended = diagnosis.recommended_fees.expect("recommended fees");
    assert_eq!(recommended.max_fee_per_gas, 3_000_000_000);
    // Empty strings mean the fees are unavailable
    assert_eq!(diagnosis.min_replacement_fees, None);

    // The tracking document itself never selects `diagnosis`
    assert!(
        scenario
            .bodies("transactionUpdated")
            .iter()
            .all(|b| !b.contains("diagnosis"))
    );
    Ok(())
}

#[tokio::test]
async fn an_older_server_gets_no_follow_up_query() -> Result<()> {
    let scenario = Scenario::new(vec![Some("0.14.6")], Some(underpriced()));
    let cli = client(spawn_server(scenario.clone()).await?);

    let error = track(&cli).await;

    assert!(matches!(
        error.kind(),
        ErrorKind::TrackingError(TrackingErrorKind::Timeout)
    ));
    assert!(error.unmined_diagnosis().is_none());
    assert_eq!(scenario.count("QueryVersion"), 1);
    assert_eq!(scenario.count("QueryUnminedDiagnosis"), 0);
    Ok(())
}

#[tokio::test]
async fn a_pre_release_of_the_first_supporting_version_counts_as_supported() -> Result<()> {
    let scenario = Scenario::new(vec![Some("0.15.0-rc.2")], Some(underpriced()));
    let cli = client(spawn_server(scenario.clone()).await?);

    assert!(track(&cli).await.unmined_diagnosis().is_some());
    Ok(())
}

#[tokio::test]
async fn a_failed_version_query_is_not_cached() -> Result<()> {
    // First version query fails, later ones answer 0.15.0
    let scenario = Scenario::new(vec![None, Some("0.15.0")], Some(underpriced()));
    let cli = client(spawn_server(scenario.clone()).await?);

    let first = track(&cli).await;
    assert!(matches!(
        first.kind(),
        ErrorKind::TrackingError(TrackingErrorKind::Timeout)
    ));
    assert!(first.unmined_diagnosis().is_none());
    assert_eq!(scenario.count("QueryUnminedDiagnosis"), 0);

    // Retried on the next timeout
    assert!(track(&cli).await.unmined_diagnosis().is_some());
    assert_eq!(scenario.count("QueryVersion"), 2);

    // A successful answer is cached, also for clones
    assert!(track(&cli.clone()).await.unmined_diagnosis().is_some());
    assert_eq!(scenario.count("QueryVersion"), 2);
    Ok(())
}

#[tokio::test]
async fn a_failed_diagnosis_query_keeps_the_original_timeout() -> Result<()> {
    let scenario = Scenario::new(vec![Some("0.15.0")], None);
    let cli = client(spawn_server(scenario.clone()).await?);

    let error = track(&cli).await;

    assert!(matches!(
        error.kind(),
        ErrorKind::TrackingError(TrackingErrorKind::Timeout)
    ));
    assert!(error.unmined_diagnosis().is_none());
    assert_eq!(scenario.count("QueryUnminedDiagnosis"), 1);
    Ok(())
}

#[tokio::test]
async fn an_unseen_reason_decodes_to_unknown() -> Result<()> {
    let answer = diagnosis_answer(serde_json::json!({
        "reason": "SOMETHING_NEW",
        "signedFees": null,
        "recommendedFees": null,
        "minReplacementFees": null,
        "retryAfterSeconds": 0,
    }));
    let scenario = Scenario::new(vec![Some("0.15.0")], Some(answer));
    let cli = client(spawn_server(scenario).await?);

    let error = track(&cli).await;

    let diagnosis = error.unmined_diagnosis().expect("diagnosis is attached");
    assert_eq!(diagnosis.reason, UnminedReason::Unknown("SOMETHING_NEW".into()));
    assert_eq!(diagnosis.reason.as_str(), "SOMETHING_NEW");
    assert_eq!(diagnosis.signed_fees, None);
    Ok(())
}

#[tokio::test]
async fn a_timed_out_deduplicated_submission_carries_the_diagnosis() -> Result<()> {
    let scenario = Scenario::new(vec![Some("0.15.1")], Some(underpriced()));
    let cli = client(spawn_server(scenario.clone()).await?);

    let (submission, diagnosis) = cli.submit_and_track_transaction_detailed(SIGNED_TX).await?;

    assert_eq!(submission, TrackedSubmission::Deduplicated(TX_ID.into()));
    assert_eq!(diagnosis.expect("diagnosis").reason, UnminedReason::Underpriced);
    // The submit document is unchanged
    assert!(
        scenario
            .bodies("MutateTrackTransaction")
            .iter()
            .all(|b| !b.contains("diagnosis"))
    );
    Ok(())
}

#[tokio::test]
async fn a_deduplicated_submission_still_in_flight_sends_no_extra_request() -> Result<()> {
    let mut scenario = Scenario::new(vec![Some("0.15.0")], Some(underpriced()));
    Arc::get_mut(&mut scenario).expect("sole owner").dedup_status = "SUBMITTED";
    let cli = client(spawn_server(scenario.clone()).await?);

    let (submission, diagnosis) = cli.submit_and_track_transaction_detailed(SIGNED_TX).await?;

    assert!(submission.is_deduplicated());
    assert!(diagnosis.is_none());
    assert_eq!(scenario.count("QueryVersion"), 0);
    Ok(())
}

#[tokio::test]
async fn a_new_submission_sends_no_extra_request() -> Result<()> {
    let mut scenario = Scenario::new(vec![Some("0.15.0")], Some(underpriced()));
    Arc::get_mut(&mut scenario).expect("sole owner").dedup_status = "NEW";
    let cli = client(spawn_server(scenario.clone()).await?);

    let (submission, diagnosis) = cli.submit_and_track_transaction_detailed(SIGNED_TX).await?;

    assert_eq!(submission, TrackedSubmission::New(TX_ID.into()));
    assert!(diagnosis.is_none());
    assert_eq!(scenario.count("QueryVersion"), 0);
    assert_eq!(scenario.count("QueryUnminedDiagnosis"), 0);
    Ok(())
}
