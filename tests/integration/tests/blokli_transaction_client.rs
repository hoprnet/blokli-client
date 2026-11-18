use std::{future::Future, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail};
use blokli_client::{
    ErrorKind, TrackingErrorKind,
    api::{
        BlokliQueryClient, BlokliTransactionClient, TransactionTrackingOutcome,
        types::{TrackedSubmission, Transaction, TransactionDiagnosis, TransactionStatus, UnminedReason},
    },
};
use blokli_integration_tests::{
    constants::parsed_safe_balance,
    fixtures::{Eip1559GasParameters, IntegrationFixture, integration_fixture as fixture, poll_until},
};
use hex::FromHex;
use hopr_bindings::exports::alloy::primitives::U256;
use hopr_types::{
    chain::{
        payload::{PayloadGenerator, SafePayloadGenerator},
        prelude::SignableTransaction,
    },
    primitive::prelude::Address as HoprAddress,
};
use rstest::*;
use serial_test::serial;

const TX_VALUE: u128 = 1_000_000; // 0.000000000001 ETH

fn confirmed_transaction(outcome: TransactionTrackingOutcome) -> Result<Transaction> {
    match outcome {
        TransactionTrackingOutcome::Confirmed(transaction) => Ok(transaction),
        TransactionTrackingOutcome::StatusUnknown { tx_id } => {
            bail!("transaction {tx_id} did not reach a terminal status before the test deadline")
        }
    }
}

enum ClientType {
    Rpc,
    Blokli,
}

#[rstest]
#[case(ClientType::Rpc)]
#[case(ClientType::Blokli)]
#[test_log::test(tokio::test)]
#[serial]
/// Test that submitting a transaction with a valid payload, via blokli and via direct RPC, goes through.
/// The payload is a simple token transfer to not test any HOPR-specific logic, but rather only the
/// transaction submission flow.
async fn submit_transaction(#[future(awt)] fixture: IntegrationFixture, #[case] client_type: ClientType) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let tx_value = U256::from(TX_VALUE);
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;

    let raw_tx = fixture.build_raw_tx(tx_value, sender, recipient, nonce).await?;
    let signed_bytes =
        Vec::from_hex(raw_tx.trim_start_matches("0x")).context("failed to decode raw transaction payload")?;

    let initial_balance = fixture.rpc().get_balance(&recipient.address).await?;

    match client_type {
        ClientType::Rpc => {
            fixture.rpc().execute_transaction(&raw_tx).await?;
        }
        ClientType::Blokli => {
            fixture.submit_tx(&signed_bytes).await?;
        }
    }

    // Wait for the balance to update after the fire-and-forget submission
    let expected_balance = initial_balance + tx_value;
    poll_until(
        "recipient balance updated",
        Duration::from_secs(30),
        Duration::from_millis(500),
        || {
            let rpc = fixture.rpc();
            let addr = &recipient.address;
            async move {
                let balance = rpc.get_balance(addr).await?;
                Ok(if balance >= expected_balance {
                    Some(balance)
                } else {
                    None
                })
            }
        },
    )
    .await?;

    let final_balance = fixture.rpc().get_balance(&recipient.address).await?;
    let delta = final_balance
        .checked_sub(initial_balance)
        .context("recipient balance decreased unexpectedly")?;

    assert_eq!(delta, tx_value);

    Ok(())
}

#[rstest]
#[case(ClientType::Rpc)]
#[case(ClientType::Blokli)]
#[test_log::test(tokio::test)]
#[serial]
/// Test that submitting a transaction with an incorrect payload, via blokli and via direct RPC, fails.
/// The payload is generated as a valid payload but then tampered to make it invalid.
async fn submit_transaction_with_incorrect_payload(
    #[future(awt)] fixture: IntegrationFixture,
    #[case] client_type: ClientType,
) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let tx_value = U256::from(TX_VALUE);
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;

    let mut raw_tx = fixture.build_raw_tx(tx_value, sender, recipient, nonce).await?;
    raw_tx.replace_range(10..14, "dead");
    let signed_bytes =
        Vec::from_hex(raw_tx.trim_start_matches("0x")).context("failed to decode raw transaction payload")?;

    let res = match client_type {
        ClientType::Rpc => fixture.rpc().execute_transaction(&raw_tx).await,
        ClientType::Blokli => fixture.submit_tx(&signed_bytes).await,
    };
    assert!(res.is_err(), "transaction with incorrect payload should fail");

    Ok(())
}

#[rstest]
#[case(ClientType::Rpc)]
#[case(ClientType::Blokli)]
#[test_log::test(tokio::test)]
#[serial]
/// Test that submitting a transaction with too much value, via blokli and via direct RPC, fails.
/// The payload used is per say valid, except that the value to transfer exceeds the source's balance.
async fn submit_transaction_with_too_much_value(
    #[future(awt)] fixture: IntegrationFixture,
    #[case] client_type: ClientType,
) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let tx_value = U256::MAX; // definitely too much value
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;

    let raw_tx = fixture.build_raw_tx(tx_value, sender, recipient, nonce).await?;
    let signed_bytes =
        Vec::from_hex(raw_tx.trim_start_matches("0x")).context("failed to decode raw transaction payload")?;

    let res = match client_type {
        ClientType::Rpc => fixture.rpc().execute_transaction(&raw_tx).await,
        ClientType::Blokli => fixture.submit_tx(&signed_bytes).await,
    };
    assert!(res.is_err(), "transaction with incorrect payload should fail");

    Ok(())
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
/// Test that submitting a transaction with a valid payload via blokli goes through, and the tracking
/// id provided by blokli can be used to track the transaction status until confirmation.
async fn submit_and_track_transaction(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let tx_value = U256::from(TX_VALUE);
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;

    let raw_tx = fixture.build_raw_tx(tx_value, sender, recipient, nonce).await?;
    let initial_balance = fixture.rpc().get_balance(&recipient.address).await?;
    let signed_bytes =
        Vec::from_hex(raw_tx.trim_start_matches("0x")).context("failed to decode raw transaction payload")?;

    let txid = fixture.submit_and_track_tx(&signed_bytes).await?;

    let res = confirmed_transaction(
        fixture
            .client()
            .track_transaction(txid.clone(), Duration::from_secs(30))
            .await?,
    )?;
    assert_eq!(res.status, TransactionStatus::Confirmed);

    let final_balance = fixture.rpc().get_balance(&recipient.address).await?;
    let delta = final_balance
        .checked_sub(initial_balance)
        .context("recipient balance decreased unexpectedly")?;

    assert_eq!(delta, tx_value);

    Ok(())
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
/// Test that submitting and confirming a transaction (1 block finality by default) with a valid
/// payload via blokli goes through.
async fn submit_and_confirm_transaction(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let tx_value = U256::from(TX_VALUE);
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;
    let confirmations = fixture.config().tx_confirmations;

    let raw_tx = fixture.build_raw_tx(tx_value, sender, recipient, nonce).await?;
    let signed_bytes =
        Vec::from_hex(raw_tx.trim_start_matches("0x")).context("failed to decode raw transaction payload")?;
    let initial_balance = fixture.rpc().get_balance(&recipient.address).await?;

    let block_number = fixture.client().query_chain_info().await?.block_number;
    fixture.submit_and_confirm_tx(&signed_bytes, confirmations).await?;

    // +1 to account for the acceptabled indexer lag. As the chain_info endpoint might be slightly lagging behind the
    // latest block.
    assert!(fixture.client().query_chain_info().await?.block_number + 1 >= block_number + (confirmations as i32));

    let final_balance = fixture.rpc().get_balance(&recipient.address).await?;
    let delta = final_balance
        .checked_sub(initial_balance)
        .context("recipient balance decreased unexpectedly")?;
    assert_eq!(delta, tx_value);
    Ok(())
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
/// Test that a Safe module transaction (via execTransactionFromModule) that succeeds internally
/// is correctly detected and enriched with safe_execution data.
///
/// Uses `fund_channel` which calls `execTransactionFromModule` on the Safe module,
/// triggering an `ExecutionFromModuleSuccess` event from the Safe contract.
async fn test_safe_module_transaction_execution_success(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [owner, counterparty] = fixture.sample_accounts::<2>();
    let safe = fixture.deploy_safe_and_announce(owner, parsed_safe_balance()).await?;
    fixture
        .deploy_safe_and_announce(counterparty, parsed_safe_balance())
        .await?;

    // Approve the channels contract to spend tokens on behalf of the Safe (setup).
    // This is a direct token call, not a module transaction.
    fixture
        .approve(
            owner,
            "1 wxHOPR".parse().expect("failed to parse amount"),
            &safe.module_address,
        )
        .await?;

    // Build a fund_channel transaction via the Safe module.
    // This goes through execTransactionFromModule → Safe → channels.fundChannel,
    // emitting ExecutionFromModuleSuccess from the Safe.
    let nonce = fixture.rpc().transaction_count(&owner.address).await?;
    let payload_generator = SafePayloadGenerator::new(
        &owner.keypair,
        *fixture.contract_addresses(),
        HoprAddress::from_str(&safe.module_address)?,
    );
    let amount = "0.01 wxHOPR".parse().expect("failed to parse amount");
    let payload = payload_generator.fund_channel(counterparty.address, amount)?;
    let payload_bytes = payload
        .sign_and_encode_to_eip2718(nonce, fixture.rpc().chain_id().await?, None, &owner.keypair)
        .await?;

    let txid = fixture.submit_and_track_tx(&payload_bytes).await?;

    let res = confirmed_transaction(
        fixture
            .client()
            .track_transaction(txid, Duration::from_secs(60))
            .await?,
    )?;
    insta::assert_yaml_snapshot!(res, {
        ".id" => "[uuid]",
        ".submitted_at" => "[timestamp]",
        ".transaction_hash" => "[tx_hash]",
    });

    Ok(())
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
/// Test that a Safe module transaction that fails internally (inner revert) is correctly
/// detected with safe_execution.success = false while the outer transaction still confirms.
async fn test_safe_module_transaction_execution_failure(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [owner, counterparty] = fixture.sample_accounts::<2>();
    let safe = fixture.deploy_safe_and_announce(owner, parsed_safe_balance()).await?;

    // Build a channel closure transaction for a non-existent channel.
    // The inner call will revert because there's no open channel between these parties.
    let nonce = fixture.rpc().transaction_count(&owner.address).await?;

    let payload_generator = SafePayloadGenerator::new(
        &owner.keypair,
        *fixture.contract_addresses(),
        HoprAddress::from_str(&safe.module_address)?,
    );
    let payload = payload_generator.initiate_outgoing_channel_closure(counterparty.address)?;
    let payload_bytes = payload
        .sign_and_encode_to_eip2718(nonce, fixture.rpc().chain_id().await?, None, &owner.keypair)
        .await?;

    let txid = fixture.submit_and_track_tx(&payload_bytes).await?;

    let res = confirmed_transaction(
        fixture
            .client()
            .track_transaction(txid, Duration::from_secs(60))
            .await?,
    )?;
    insta::assert_yaml_snapshot!(res, {
        ".id" => "[uuid]",
        ".submitted_at" => "[timestamp]",
        ".transaction_hash" => "[tx_hash]",
        ".safe_execution.revert_reason" => "[revert_data]",
    });

    Ok(())
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
/// Test that a plain ETH transfer (not targeting a Safe or module) has no safe_execution
/// enrichment.
async fn test_plain_transaction_no_safe_enrichment(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let tx_value = U256::from(TX_VALUE);
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;

    let raw_tx = fixture.build_raw_tx(tx_value, sender, recipient, nonce).await?;
    let signed_bytes =
        Vec::from_hex(raw_tx.trim_start_matches("0x")).context("failed to decode raw transaction payload")?;

    let txid = fixture.submit_and_track_tx(&signed_bytes).await?;

    let res = confirmed_transaction(
        fixture
            .client()
            .track_transaction(txid, Duration::from_secs(30))
            .await?,
    )?;
    insta::assert_yaml_snapshot!(res, {
        ".id" => "[uuid]",
        ".submitted_at" => "[timestamp]",
        ".transaction_hash" => "[tx_hash]",
        ".safe_execution.revert_reason" => "[revert_data]",
    });

    Ok(())
}

/// Env var overriding how long the tests below wait for Blokli to time a transaction out, in seconds.
///
/// Blokli's transaction monitor timeout defaults to 300 seconds. A stack started with a shorter monitor timeout
/// lets this deadline shrink accordingly.
const UNMINED_TRACK_TIMEOUT_ENV: &str = "BLOKLI_TEST_UNMINED_TIMEOUT_SECS";
const UNMINED_TRACK_TIMEOUT_DEFAULT_SECS: u64 = 8 * 60;

fn unmined_track_timeout() -> Duration {
    let secs = std::env::var(UNMINED_TRACK_TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(UNMINED_TRACK_TIMEOUT_DEFAULT_SECS);
    Duration::from_secs(secs)
}

/// Logs which image and Blokli version a result belongs to, so it is never read against the wrong server.
async fn log_server_under_test(fixture: &IntegrationFixture) -> Result<()> {
    let image = std::env::var("BLOKLI_TEST_IMAGE").unwrap_or_else(|_| "bloklid-anvil:integration-test".to_owned());
    let inspected = std::process::Command::new("docker")
        .args(["image", "inspect", "--format", "{{.Id}} {{.Created}}", &image])
        .output()
        .ok()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .unwrap_or_default();
    let version = fixture.client().query_version().await?;
    tracing::info!(%image, image_id_and_created = %inspected, %version, "server under test");
    Ok(())
}

/// Whether the Blokli under test serves the unmined diagnosis, judged by version the way the client does.
async fn server_supports_diagnosis(fixture: &IntegrationFixture) -> Result<bool> {
    let version = fixture.client().query_version().await?;
    let parsed = semver::Version::parse(version.trim().trim_start_matches('v'))
        .with_context(|| format!("blokli reported an unparseable version {version:?}"))?;
    // The client ignores a pre-release suffix when gating on the version
    Ok(semver::Version::new(parsed.major, parsed.minor, parsed.patch) >= semver::Version::new(0, 15, 0))
}

/// Submits `raw_tx` for tracking and waits until Blokli times it out.
///
/// Returns the tracking id and the tracking error.
async fn time_out_unmined(
    fixture: &IntegrationFixture,
    raw_tx: &str,
) -> Result<(String, blokli_client::BlokliClientError)> {
    log_server_under_test(fixture).await?;
    let signed_bytes =
        Vec::from_hex(raw_tx.trim_start_matches("0x")).context("failed to decode raw transaction payload")?;

    let tx_id = fixture.submit_and_track_tx(&signed_bytes).await?;
    let error = track_to_timeout(fixture, &tx_id).await?;

    Ok((tx_id, error))
}

/// Tracks an already submitted transaction until Blokli times it out.
async fn track_to_timeout(fixture: &IntegrationFixture, tx_id: &str) -> Result<blokli_client::BlokliClientError> {
    let error = fixture
        .client()
        .track_transaction(tx_id.to_owned(), unmined_track_timeout())
        .await
        .expect_err("an unmined transaction must time out");
    assert!(
        matches!(error.kind(), ErrorKind::TrackingError(TrackingErrorKind::Timeout)),
        "unexpected error: {error}"
    );

    Ok(error)
}

/// Resubmits the bytes of a timed out transaction the node still holds.
///
/// Blokli returns the tracked transaction and resumes tracking it, which clears the diagnosis, so the resubmission
/// is deduplicated but carries no diagnosis. A dropped or replaced transaction is no longer held: its resubmission
/// is a fresh one, so this is not called for those.
async fn assert_resubmission_deduplicated(fixture: &IntegrationFixture, raw_tx: &str, tx_id: String) -> Result<()> {
    let signed_bytes =
        Vec::from_hex(raw_tx.trim_start_matches("0x")).context("failed to decode raw transaction payload")?;
    let (submission, _) = fixture
        .client()
        .submit_and_track_transaction_detailed(&signed_bytes)
        .await?;
    assert_eq!(submission, TrackedSubmission::Deduplicated(tx_id));
    Ok(())
}

/// Asserts the fee and retry fields every diagnosis must carry, and returns it.
fn assert_complete_diagnosis(diagnosis: &TransactionDiagnosis, reason: &UnminedReason) {
    assert_eq!(&diagnosis.reason, reason, "{diagnosis:?}");
    let signed = diagnosis.signed_fees.expect("signed fees");
    let recommended = diagnosis.recommended_fees.expect("recommended fees");
    let replacement = diagnosis.min_replacement_fees.expect("min replacement fees");
    assert!(recommended.max_fee_per_gas > 0, "{diagnosis:?}");
    // A replacement must outbid the signed fees by at least 12.5%
    assert!(
        replacement.max_fee_per_gas * 8 >= signed.max_fee_per_gas * 9,
        "{diagnosis:?}"
    );
    assert!(
        replacement.max_priority_fee_per_gas * 8 >= signed.max_priority_fee_per_gas * 9,
        "{diagnosis:?}"
    );
    assert!(diagnosis.retry_after_seconds > 0, "{diagnosis:?}");
}

/// Checks the outcome of [`time_out_unmined`] against what the server version promises.
///
/// Servers older than 0.15.0 must keep returning the plain timeout, with no diagnosis.
async fn assert_outcome(
    fixture: &IntegrationFixture,
    error: &blokli_client::BlokliClientError,
    expected: impl Fn(&UnminedReason) -> bool,
    expected_name: &str,
) -> Result<()> {
    if server_supports_diagnosis(fixture).await? {
        let diagnosis = error
            .unmined_diagnosis()
            .context("blokli must report a diagnosis for a timed out transaction")?;
        assert!(
            expected(&diagnosis.reason),
            "expected {expected_name}, got {diagnosis:?}"
        );
        assert_complete_diagnosis(diagnosis, &diagnosis.reason);
    } else {
        assert!(error.unmined_diagnosis().is_none());
    }
    Ok(())
}

/// Block interval of the integration image's Anvil (`ANVIL_BLOCK_TIME`), restored after manual mining.
const ANVIL_BLOCK_TIME_SECS: u64 = 1;

/// Runs `body` with Anvil mining only on demand, and restores mining afterwards whatever the outcome.
///
/// The image mines a block every second on top of automining, so both have to be switched off for a transaction
/// to stay in the pool.
async fn with_manual_mining<T>(fixture: &IntegrationFixture, body: impl Future<Output = Result<T>>) -> Result<T> {
    fixture.rpc().set_automine(false).await?;
    fixture.rpc().set_interval_mining(0).await?;
    let outcome = body.await;
    let restored = async {
        // Mine what the pool still holds now, so it cannot confirm in the middle of the next test
        fixture.rpc().mine(1).await?;
        fixture.rpc().set_automine(true).await?;
        fixture.rpc().set_interval_mining(ANVIL_BLOCK_TIME_SECS).await
    }
    .await;
    let outcome = outcome?;
    restored?;
    Ok(outcome)
}

/// The fees Blokli currently recommends, as `(max fee, max priority fee)`.
async fn recommended_fees(fixture: &IntegrationFixture) -> Result<(u128, u128)> {
    let info = fixture.client().query_chain_info().await?;
    let parse = |value: Option<String>, name: &str| {
        value
            .and_then(|v| v.parse::<u128>().ok())
            .with_context(|| format!("chainInfo has no usable {name}"))
    };
    Ok((
        parse(info.max_fee_per_gas, "maxFeePerGas")?,
        parse(info.max_priority_fee_per_gas, "maxPriorityFeePerGas")?,
    ))
}

fn gas(max_fee_per_gas: u128, max_priority_fee_per_gas: u128) -> Eip1559GasParameters {
    Eip1559GasParameters {
        max_fee_per_gas,
        max_priority_fee_per_gas,
        ..Eip1559GasParameters::default()
    }
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
#[ignore = "needs a configurable monitor timeout, the default 300 s makes it a 5 minute test"]
/// A transaction signed with a nonce ahead of the sender's waits behind the gap.
async fn unmined_nonce_gap_is_diagnosed(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let nonce = fixture.rpc().transaction_count(&sender.address).await? + 3;
    let raw_tx = fixture
        .build_raw_tx(U256::from(TX_VALUE), sender, recipient, nonce)
        .await?;

    let (tx_id, error) = time_out_unmined(&fixture, &raw_tx).await?;

    assert_outcome(&fixture, &error, |r| *r == UnminedReason::NonceGap, "NONCE_GAP").await?;
    assert_resubmission_deduplicated(&fixture, &raw_tx, tx_id).await
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
#[ignore = "needs a configurable monitor timeout, the default 300 s makes it a 5 minute test"]
/// A transaction behind an earlier pending nonce of the same signer is queued.
async fn unmined_transaction_behind_a_pending_nonce_is_queued(
    #[future(awt)] fixture: IntegrationFixture,
) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;
    let (max_fee, max_priority) = recommended_fees(&fixture).await?;

    with_manual_mining(&fixture, async {
        let first = fixture
            .build_raw_tx(U256::from(TX_VALUE), sender, recipient, nonce)
            .await?;
        fixture.rpc().execute_transaction(&first).await?;
        let second = fixture
            .build_raw_tx_with_gas(
                U256::from(TX_VALUE),
                sender,
                recipient,
                nonce + 1,
                Some(gas(max_fee * 4, max_priority * 4)),
            )
            .await?;

        let (tx_id, error) = time_out_unmined(&fixture, &second).await?;
        assert_outcome(&fixture, &error, |r| *r == UnminedReason::Queued, "QUEUED").await?;
        assert_resubmission_deduplicated(&fixture, &second, tx_id).await
    })
    .await
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
#[ignore = "needs a configurable monitor timeout, the default 300 s makes it a 5 minute test"]
/// A transaction held by the node but signed below the recommended fee is underpriced.
async fn unmined_low_fee_transaction_is_underpriced(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;
    let (max_fee, max_priority) = recommended_fees(&fixture).await?;

    with_manual_mining(&fixture, async {
        let raw_tx = fixture
            .build_raw_tx_with_gas(
                U256::from(TX_VALUE),
                sender,
                recipient,
                nonce,
                Some(gas((max_fee / 4).max(1), (max_priority / 4).max(1))),
            )
            .await?;

        let (tx_id, error) = time_out_unmined(&fixture, &raw_tx).await?;
        assert_outcome(&fixture, &error, |r| *r == UnminedReason::Underpriced, "UNDERPRICED").await?;
        assert_resubmission_deduplicated(&fixture, &raw_tx, tx_id).await
    })
    .await
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
#[ignore = "needs a configurable monitor timeout, the default 300 s makes it a 5 minute test"]
/// Control for the underpriced case: a well-priced transaction the node holds is not diagnosed as underpriced.
async fn unmined_well_priced_transaction_is_not_underpriced(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;
    let (max_fee, max_priority) = recommended_fees(&fixture).await?;

    with_manual_mining(&fixture, async {
        let raw_tx = fixture
            .build_raw_tx_with_gas(
                U256::from(TX_VALUE),
                sender,
                recipient,
                nonce,
                Some(gas(max_fee * 4, max_priority * 4)),
            )
            .await?;

        let (_, error) = time_out_unmined(&fixture, &raw_tx).await?;
        assert_outcome(
            &fixture,
            &error,
            |r| *r != UnminedReason::Underpriced,
            "anything but UNDERPRICED",
        )
        .await
    })
    .await
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
#[ignore = "needs a configurable monitor timeout, the default 300 s makes it a 5 minute test"]
/// A transaction the node forgot, whose nonce is still unused, is dropped.
async fn unmined_forgotten_transaction_is_dropped(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;

    with_manual_mining(&fixture, async {
        let raw_tx = fixture
            .build_raw_tx(U256::from(TX_VALUE), sender, recipient, nonce)
            .await?;
        let signed_bytes =
            Vec::from_hex(raw_tx.trim_start_matches("0x")).context("failed to decode raw transaction payload")?;

        // Submit, then make the node forget the transaction before the monitor times out
        let tx_id = fixture.submit_and_track_tx(&signed_bytes).await?;
        let hash = fixture
            .client()
            .query_transaction_status(tx_id.clone())
            .await?
            .transaction_hash
            .0;
        fixture.rpc().drop_transaction(&hash).await?;

        log_server_under_test(&fixture).await?;
        let error = track_to_timeout(&fixture, &tx_id).await?;
        assert_outcome(&fixture, &error, |r| *r == UnminedReason::Dropped, "DROPPED").await
    })
    .await
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
#[ignore = "needs a configurable monitor timeout, the default 300 s makes it a 5 minute test"]
/// A transaction whose nonce was consumed by another transaction of the signer is replaced.
async fn unmined_replaced_transaction_is_diagnosed(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [sender, recipient] = fixture.sample_accounts::<2>();
    let nonce = fixture.rpc().transaction_count(&sender.address).await?;
    let (max_fee, max_priority) = recommended_fees(&fixture).await?;

    with_manual_mining(&fixture, async {
        let original = fixture
            .build_raw_tx(U256::from(TX_VALUE), sender, recipient, nonce)
            .await?;
        let signed_bytes =
            Vec::from_hex(original.trim_start_matches("0x")).context("failed to decode raw transaction payload")?;
        let tx_id = fixture.submit_and_track_tx(&signed_bytes).await?;

        // Replace it at the same nonce with higher fees straight on the node, then mine the replacement
        let replacement = fixture
            .build_raw_tx_with_gas(
                U256::from(TX_VALUE + 1),
                sender,
                recipient,
                nonce,
                Some(gas(max_fee * 4, max_priority * 4)),
            )
            .await?;
        fixture.rpc().execute_transaction(&replacement).await?;
        fixture.rpc().mine(1).await?;

        log_server_under_test(&fixture).await?;
        let error = track_to_timeout(&fixture, &tx_id).await?;
        assert_outcome(&fixture, &error, |r| *r == UnminedReason::Replaced, "REPLACED").await
    })
    .await
}
