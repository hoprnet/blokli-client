//! Coverage for the outcomes Blokli's HOPR-aware transaction policy can return.
//!
//! These outcomes all mean the transaction was **not** broadcast, so what matters to a caller
//! is telling them apart: a rejection stands until the chain state changes, a throttle clears
//! on its own, and a deduplicated submission is already being carried by another transaction.
//! Each test serves one union member and asserts the client surfaces that distinction.

use blokli_client::{
    BlokliClient,
    api::BlokliTransactionClient,
    errors::{BlokliClientError, ErrorKind},
};
use mockito::Matcher;

const SIGNED_TX: &[u8] = &[0x02, 0xF8, 0x6B, 0x01];

/// Serve one GraphQL response for the mutation matching `operation`.
async fn serve(server: &mut mockito::ServerGuard, operation: &str, body: &str) -> mockito::Mock {
    server
        .mock("POST", "/graphql")
        .match_body(Matcher::Regex(operation.into()))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(body)
        .create_async()
        .await
}

#[tokio::test]
async fn a_rejected_action_reports_its_operation_and_reason() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let mock = serve(
        &mut server,
        "MutateTrackTransaction",
        r#"{
          "data": {
            "sendTransactionAsync": {
              "__typename": "HoprActionRejectedError",
              "code": "HOPR_ACTION_REJECTED",
              "message": "an outgoing channel closure can only be finalized while the channel is pending to close",
              "operation": "finalize_channel_closure",
              "reason": "CHANNEL_NOT_PENDING_TO_CLOSE"
            }
          }
        }"#,
    )
    .await;

    let cli = BlokliClient::new(server.url().parse()?, Default::default());
    let error = cli
        .submit_and_track_transaction(SIGNED_TX)
        .await
        .expect_err("a rejected action must not look like a successful submission");

    match error.kind() {
        ErrorKind::HoprActionRejected { operation, reason, .. } => {
            assert_eq!(operation, "finalize_channel_closure");
            assert_eq!(reason, "CHANNEL_NOT_PENDING_TO_CLOSE");
        }
        other => panic!("expected a HOPR action rejection, got {other:?}"),
    }

    mock.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn a_throttled_action_carries_a_machine_readable_backoff() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let mock = serve(
        &mut server,
        "MutateTrackTransaction",
        r#"{
          "data": {
            "sendTransactionAsync": {
              "__typename": "HoprActionThrottledError",
              "code": "HOPR_ACTION_THROTTLED",
              "message": "temporarily suppressed after repeated invalid submissions",
              "operation": "finalize_channel_closure",
              "reason": "CHANNEL_NOT_PENDING_TO_CLOSE",
              "retryAfterSeconds": 42
            }
          }
        }"#,
    )
    .await;

    let cli = BlokliClient::new(server.url().parse()?, Default::default());
    let error = cli
        .submit_and_track_transaction(SIGNED_TX)
        .await
        .expect_err("a throttled action must not look like a successful submission");

    match error.kind() {
        ErrorKind::HoprActionThrottled {
            operation, retry_after, ..
        } => {
            assert_eq!(operation, "finalize_channel_closure");
            // The backoff must survive as a value, not only inside the message text, so a
            // caller can wait on it instead of parsing prose.
            assert_eq!(retry_after.as_secs(), 42);
        }
        other => panic!("expected a HOPR action throttle, got {other:?}"),
    }

    mock.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn a_deduplicated_submission_yields_the_transaction_already_tracking_it() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let mock = serve(
        &mut server,
        "MutateTrackTransaction",
        r#"{
          "data": {
            "sendTransactionAsync": {
              "__typename": "DeduplicatedTransaction",
              "operation": "fund_channel",
              "transaction": {
                "__typename": "Transaction",
                "id": "5a6b7c8d-0000-0000-0000-000000000000",
                "status": "SUBMITTED",
                "submittedAt": "2026-09-23T10:00:00Z",
                "transactionHash": "0xaabbccddeeff00112233445566778899aabbccddeeff001122334455667788990",
                "safeExecution": null
              }
            }
          }
        }"#,
    )
    .await;

    let cli = BlokliClient::new(server.url().parse()?, Default::default());

    // Nothing was broadcast, but the intent is live under the returned id, so the caller
    // follows it exactly as it would its own submission rather than treating it as a failure.
    let tx_id = cli
        .submit_and_track_transaction(SIGNED_TX)
        .await
        .expect("a deduplicated submission is not a failure");
    assert_eq!(tx_id, "5a6b7c8d-0000-0000-0000-000000000000");

    mock.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn an_overloaded_blokli_is_reported_as_such() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let mock = serve(
        &mut server,
        "MutateTrackTransaction",
        r#"{
          "data": {
            "sendTransactionAsync": {
              "__typename": "OverloadedError",
              "code": "SUBMISSION_CAPACITY_EXCEEDED",
              "message": "transaction submission capacity is exhausted"
            }
          }
        }"#,
    )
    .await;

    let cli = BlokliClient::new(server.url().parse()?, Default::default());
    let error = cli
        .submit_and_track_transaction(SIGNED_TX)
        .await
        .expect_err("an overloaded blokli must not look like a successful submission");

    // Before this variant existed the response fell through to the unknown-member fallback
    // and surfaced as the far less useful `NoData`.
    match error.kind() {
        ErrorKind::BlokliError { kind, code, .. } => {
            assert_eq!(*kind, "overloaded");
            assert_eq!(code, "SUBMISSION_CAPACITY_EXCEEDED");
        }
        other => panic!("expected an overload error, got {other:?}"),
    }

    mock.assert_async().await;
    Ok(())
}

#[tokio::test]
async fn policy_outcomes_reach_the_synchronous_and_fire_and_forget_paths_too() -> anyhow::Result<()> {
    for (operation, body) in [
        (
            "MutateSendTransaction",
            r#"{"data":{"sendTransaction":{"__typename":"HoprActionRejectedError","code":"HOPR_ACTION_REJECTED","message":"m","operation":"fund_channel","reason":"CHANNEL_ALREADY_CLOSING"}}}"#,
        ),
        (
            "MutateConfirmTransaction",
            r#"{"data":{"sendTransactionSync":{"__typename":"HoprActionRejectedError","code":"HOPR_ACTION_REJECTED","message":"m","operation":"fund_channel","reason":"CHANNEL_ALREADY_CLOSING"}}}"#,
        ),
    ] {
        let mut server = mockito::Server::new_async().await;
        let mock = serve(&mut server, operation, body).await;
        let cli = BlokliClient::new(server.url().parse()?, Default::default());

        let error: BlokliClientError = if operation == "MutateSendTransaction" {
            cli.submit_transaction(SIGNED_TX).await.expect_err("must be refused")
        } else {
            cli.submit_and_confirm_transaction(SIGNED_TX, 1)
                .await
                .expect_err("must be refused")
        };

        assert!(
            matches!(error.kind(), ErrorKind::HoprActionRejected { reason, .. } if reason == "CHANNEL_ALREADY_CLOSING"),
            "{operation} did not surface the rejection: {error:?}"
        );
        mock.assert_async().await;
    }

    Ok(())
}

/// The test client must be able to replay these outcomes, so downstream consumers can cover
/// their own handling of them without standing up a real Blokli.
#[cfg(feature = "testing")]
#[tokio::test]
async fn the_test_client_can_replay_a_policy_refusal() -> anyhow::Result<()> {
    use std::time::Duration;

    use blokli_client::{BlokliTestClient, NopStateMutator, SimulatedPolicyOutcome};

    let cli = BlokliTestClient::new(Default::default(), NopStateMutator).with_policy_outcome(Some(
        SimulatedPolicyOutcome::Throttled {
            operation: "fund_channel".into(),
            reason: "CHANNEL_ALREADY_CLOSING".into(),
            retry_after: Duration::from_secs(30),
        },
    ));

    let error = cli
        .submit_and_track_transaction(SIGNED_TX)
        .await
        .expect_err("a simulated throttle must refuse the submission");

    assert!(matches!(
        error.kind(),
        ErrorKind::HoprActionThrottled { retry_after, .. } if retry_after.as_secs() == 30
    ));

    Ok(())
}
