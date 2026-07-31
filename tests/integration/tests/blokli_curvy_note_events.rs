use std::str::FromStr;

use anyhow::{Context, Result, anyhow};
use blokli_client::api::{
    BlokliQueryClient, BlokliSubscriptionClient,
    types::{CurvyNoteEvent, CurvyNoteEventFilter, CurvyNoteEventKind},
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
async fn curvy_note_subscription_filters_resumes_and_correlates_raw_events(
    #[future(awt)] fixture: IntegrationFixture,
) -> Result<()> {
    let [account] = fixture.sample_accounts::<1>();
    let chain_info = fixture.client().query_chain_info().await?;
    let aggregator_address = curvy_aggregator_address(&chain_info.contract_addresses.0)?;
    let signer = PrivateKeySigner::from_slice(account.keypair.secret().as_ref())?;
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(fixture.config().rpc_url().clone());
    let aggregator = CurvyAggregatorAlphaV2Instance::new(aggregator_address, provider);

    let client = fixture.client().clone();
    let pending_handle = tokio::spawn(async move {
        let pending_stream = client
            .subscribe_curvy_note_events(
                None,
                Some(CurvyNoteEventFilter {
                    kinds: Some(vec![CurvyNoteEventKind::Pending]),
                    note_ids: None,
                }),
            )
            .expect("failed to create pending Curvy note subscription");
        let mut notes = pending_stream
            .filter_map(|result| async move {
                match result {
                    Ok(CurvyNoteEvent::CurvyPendingNote(note)) => Some(Ok(note)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .boxed();
        notes.next().timeout(subscription_timeout()).await
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

    let pending = pending_handle
        .await??
        .ok_or_else(|| anyhow!("pending note subscription ended"))??;
    let cursor = pending.cursor.clone();
    let note_id = U256::from_str(&pending.note_id)?;

    let client = fixture.client().clone();
    let resume_cursor = cursor.clone();
    let matching_note_id = pending.note_id.clone();
    let committed_handle = tokio::spawn(async move {
        let committed_stream = client
            .subscribe_curvy_note_events(
                Some(resume_cursor),
                Some(CurvyNoteEventFilter {
                    kinds: Some(vec![CurvyNoteEventKind::Committed]),
                    note_ids: Some(vec![matching_note_id]),
                }),
            )
            .expect("failed to create committed Curvy note subscription");
        let mut notes = committed_stream
            .filter_map(|result| async move {
                match result {
                    Ok(CurvyNoteEvent::CurvyCommittedNote(note)) => Some(Ok(note)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .boxed();
        notes.next().timeout(subscription_timeout()).await
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

    let committed = committed_handle
        .await??
        .ok_or_else(|| anyhow!("committed note subscription ended"))??;
    assert_eq!(committed.note_id, pending.note_id);
    assert!(committed.cursor.components() > cursor.components());
    Ok(())
}
