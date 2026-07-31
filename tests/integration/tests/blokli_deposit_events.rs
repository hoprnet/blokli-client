use anyhow::{Context, Result, anyhow};
use blokli_client::{
    BlokliClient,
    api::{
        BlokliSubscriptionClient,
        types::{DepositCompletion, DepositDetectionCandidate, DepositEvent, DepositEventFilter},
    },
};
use blokli_integration_tests::{
    constants::subscription_timeout,
    fixtures::{IntegrationFixture, integration_fixture as fixture},
};
use curvy_bindings::exports::alloy::primitives::U256;
use futures::StreamExt;
use futures_time::future::FutureExt as FutureTimeoutExt;
use rstest::rstest;
use serial_test::serial;
use tokio::sync::oneshot;

async fn watch_deposit_lifecycle(
    client: BlokliClient,
    expected_count: usize,
    candidates_tx: oneshot::Sender<Vec<DepositDetectionCandidate>>,
) -> Result<DepositCompletion> {
    // BLOKLI CLIENT: opens the single server subscription and translates raw PendingNotes/CommittedNotes into the
    // connector-facing DetectionCandidate/Completed vocabulary. It does not retain wallet-specific state.
    let mut stream = client.subscribe_deposit_events(None, DepositEventFilter::lifecycle())?;

    // HOPR CONNECTOR: consumes the client stream and inspects each candidate for local BJJ ownership. The test uses
    // the first synthetic candidate as the locally owned one instead of performing the real cryptographic check.
    let first_candidate = loop {
        let event = stream
            .next()
            .timeout(subscription_timeout())
            .await
            .context("timed out waiting for the first deposit detection candidate")?
            .ok_or_else(|| anyhow!("deposit lifecycle subscription ended"))??;
        if let DepositEvent::DetectionCandidate(candidate) = event {
            break candidate;
        }
    };

    // HOPR CONNECTOR: persists the owned note ID and latest processed cursor. On connection loss it asks blokli-client
    // to reopen the same lifecycle subscription exclusively after that cursor.
    let resume_cursor = first_candidate.cursor.clone();
    let owned_note_id = first_candidate.deposit_note_id.clone();
    drop(stream);
    let mut stream = client.subscribe_deposit_events(Some(resume_cursor), DepositEventFilter::lifecycle())?;
    let mut candidates = Vec::with_capacity(expected_count);
    candidates.push(first_candidate);

    while candidates.len() < expected_count {
        let event = stream
            .next()
            .timeout(subscription_timeout())
            .await
            .context("timed out waiting for a deposit detection candidate")?
            .ok_or_else(|| anyhow!("deposit lifecycle subscription ended"))??;
        if let DepositEvent::DetectionCandidate(candidate) = event {
            candidates.push(candidate);
        }
    }

    // TEST HARNESS: gives the note IDs to the fake Curvy operator below so it can commit this circuit-sized batch.
    candidates_tx
        .send(candidates)
        .map_err(|_| anyhow!("deposit candidate receiver was dropped"))?;

    // HOPR CONNECTOR: discards commitments for other wallets, correlates the locally owned note ID, and returns the
    // matching completion. In production it translates this result into DepositCompleted for the PIX strategy.
    loop {
        let event = stream
            .next()
            .timeout(subscription_timeout())
            .await
            .context("timed out waiting for deposit completion")?
            .ok_or_else(|| anyhow!("deposit lifecycle subscription ended"))??;
        if let DepositEvent::Completed(completion) = event {
            if completion.deposit_note_id == owned_note_id {
                return Ok(completion);
            }
        }
    }
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
#[ignore = "requires the CI-produced bloklid-anvil-curvy image"]
async fn deposit_subscription_detects_resumes_and_completes(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    // TEST HARNESS SETUP — performed by neither PIX, the connector, nor blokli-client.
    // It creates privileged contract access used only to drive the combined Anvil+Curvy test chain.
    let test_chain = fixture.curvy_test_chain().await?;

    // PIX STRATEGY + HOPR CONNECTOR + BLOKLI CLIENT — subscription and deposit submission.
    // PIX receives NewDepositAddress. The connector starts lifecycle observation, constructs/signs the Curvy portal
    // transaction, and gives the signed bytes to blokli-client. The test helper replaces transaction construction and
    // generic submission with direct contract calls, but the subscription below uses the real blokli-client path.
    let client = fixture.client().clone();
    let (candidates_tx, candidates_rx) = oneshot::channel();
    let lifecycle_handle = tokio::spawn(watch_deposit_lifecycle(
        client,
        test_chain.commitment_batch_size(),
        candidates_tx,
    ));
    test_chain.submit_deposit_batch().await?;

    // HOPR CONNECTOR — local ownership, correlation, and lifecycle state.
    // The connector, not blokli-client or PIX, owns the BJJ check, matching note IDs, and resumable cursor. This test
    // receives those synthetic candidates only to tell the fake Curvy operator which batch it must commit.
    let candidates = candidates_rx
        .await
        .context("deposit lifecycle task ended before detecting candidates")?;
    let candidate = candidates.first().context("no deposit detection candidates received")?;
    let cursor = candidates
        .last()
        .context("no deposit detection candidates received")?
        .cursor
        .clone();
    let note_ids = candidates
        .iter()
        .map(|candidate| candidate.deposit_note_id.parse::<U256>())
        .collect::<Result<Vec<_>, _>>()?;

    // TEST HARNESS CHAIN ADVANCEMENT — performed by none of PIX, the connector, or blokli-client.
    // Normally an external Curvy operator commits pending notes. Since the image has no prover, the helper installs a
    // test-only accept-all verifier and commits the complete circuit-sized batch.
    test_chain.commit_deposit_batch(&fixture, note_ids).await?;

    // HOPR CONNECTOR -> PIX STRATEGY — deliver DepositCompleted.
    // Blokli-client reports a committed note; the connector correlates it and emits the strategy-level signal.
    let completion = lifecycle_handle.await??;
    assert_eq!(completion.deposit_note_id, candidate.deposit_note_id);
    assert!(completion.cursor.components() > cursor.components());

    // TEST HARNESS TEARDOWN — performed by neither PIX, the connector, nor blokli-client.
    // IntegrationFixture captures the container logs and stops the combined environment when this test process exits.
    Ok(())
}
