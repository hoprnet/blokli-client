use std::str::FromStr;

use anyhow::{Context, Result, anyhow};
use blokli_client::api::{
    BlokliQueryClient, BlokliSubscriptionClient,
    types::{DepositEvent, DepositEventFilter},
};
use blokli_integration_tests::{
    constants::subscription_timeout,
    fixtures::{IntegrationFixture, integration_fixture as fixture},
};
use curvy_bindings::{
    curvy_aggregator_alpha_v2::{CurvyAggregatorAlphaV2::CurvyAggregatorAlphaV2Instance, CurvyTypes},
    exports::alloy::{
        primitives::{Address, U256},
        providers::ProviderBuilder,
        signers::local::PrivateKeySigner,
    },
};
use futures::StreamExt;
use futures_time::future::FutureExt as FutureTimeoutExt;
use hopr_types::crypto::keypairs::Keypair;
use rstest::rstest;
use serial_test::serial;

fn curvy_aggregator_address(raw_map: &str) -> Result<Address> {
    let addresses: serde_json::Value = serde_json::from_str(raw_map)?;
    let address = addresses
        .get("curvy_aggregator")
        .and_then(serde_json::Value::as_str)
        .context("chainInfo does not contain curvy_aggregator")?;
    Address::from_str(address).context("invalid Curvy aggregator address")
}

#[rstest]
#[test_log::test(tokio::test)]
#[serial]
#[ignore = "requires the CI-produced bloklid-anvil-curvy image"]
async fn deposit_subscription_detects_resumes_and_completes(#[future(awt)] fixture: IntegrationFixture) -> Result<()> {
    let [account] = fixture.sample_accounts::<1>();
    let chain_info = fixture.client().query_chain_info().await?;
    let aggregator_address = curvy_aggregator_address(&chain_info.contract_addresses.0)?;
    let signer = PrivateKeySigner::from_slice(account.keypair.secret().as_ref())?;
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(fixture.config().rpc_url().clone());
    let aggregator = CurvyAggregatorAlphaV2Instance::new(aggregator_address, provider);

    let client = fixture.client().clone();
    let candidate_handle = tokio::spawn(async move {
        let candidate_stream = client
            .subscribe_deposit_events(None, DepositEventFilter::detection_candidates())
            .expect("failed to create deposit detection subscription");
        let mut candidates = candidate_stream
            .filter_map(|result| async move {
                match result {
                    Ok(DepositEvent::DetectionCandidate(candidate)) => Some(Ok(candidate)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .boxed();
        candidates.next().timeout(subscription_timeout()).await
    });

    aggregator
        .autoShield(CurvyTypes::Note {
            ownerHash: U256::from(101),
            token: U256::ZERO,
            amount: U256::from(1),
            ephemeralKey: [U256::from(201), U256::from(202)],
            viewTag: 7,
        })
        .value(U256::from(1))
        .send()
        .await?
        .watch()
        .await?;

    let candidate = candidate_handle
        .await??
        .ok_or_else(|| anyhow!("deposit detection subscription ended"))??;
    let cursor = candidate.cursor.clone();
    let note_id = U256::from_str(&candidate.deposit_note_id)?;

    let client = fixture.client().clone();
    let resume_cursor = cursor.clone();
    let matching_note_id = candidate.deposit_note_id.clone();
    let completion_handle = tokio::spawn(async move {
        let completion_stream = client
            .subscribe_deposit_events(
                Some(resume_cursor),
                DepositEventFilter::completions(vec![matching_note_id]),
            )
            .expect("failed to create deposit completion subscription");
        let mut completions = completion_stream
            .filter_map(|result| async move {
                match result {
                    Ok(DepositEvent::Completed(completion)) => Some(Ok(completion)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .boxed();
        completions.next().timeout(subscription_timeout()).await
    });

    aggregator
        .commitPendingNotes(
            U256::from(1),
            vec![note_id],
            U256::from(301),
            [U256::ZERO; 2],
            [[U256::ZERO; 2]; 2],
            [U256::ZERO; 2],
        )
        .send()
        .await?
        .watch()
        .await?;

    let completion = completion_handle
        .await??
        .ok_or_else(|| anyhow!("deposit completion subscription ended"))??;
    assert_eq!(completion.deposit_note_id, candidate.deposit_note_id);
    assert!(completion.cursor.components() > cursor.components());
    Ok(())
}
