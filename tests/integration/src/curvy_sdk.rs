//! Curvy SDK fixtures and adapters shared by integration tests.

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use blokli_client::api::types::{CurvyEventPosition, CurvyPendingNote, Hex32, Uint64, Uint256};
use curvy_chain_api::{
    BalanceReader, ChainError, FeeConfigSource, NoteIndexSource, PortalDirectory, RootAnchor, TxSubmitter,
};
use curvy_sdk::{Account, CurvyClient, Discovered, OwnedNote, curvy_core, send::seal_note};
use curvy_types::{
    Addr, AggregatorState, CommittedNotesEvent, CommittedNullifiersEvent, Dec, FeeConfig, NotesTreeSnapshot,
    PendingNotesEvent, RawTx, TxOutcome,
};
use hopr_bindings::exports::alloy::primitives::U256;

const AGGREGATOR_ADDRESS: &str = "0x0000000000000000000000000000000000000001";
const PORTAL_FACTORY_ADDRESS: &str = "0x0000000000000000000000000000000000000002";

#[derive(Clone)]
struct ScanBackend {
    pending_notes: Vec<PendingNotesEvent>,
    head_block: u64,
}

impl ScanBackend {
    fn unsupported<T>(capability: &str) -> curvy_chain_api::Result<T> {
        Err(ChainError::Unsupported(format!(
            "{capability} is outside this ownership-scanning test"
        )))
    }
}

#[async_trait]
impl NoteIndexSource for ScanBackend {
    async fn pending_notes(&self, from_block: u64, to_block: u64) -> curvy_chain_api::Result<Vec<PendingNotesEvent>> {
        Ok(self
            .pending_notes
            .iter()
            .filter(|event| event.block_number >= from_block && event.block_number <= to_block)
            .cloned()
            .collect())
    }

    async fn committed_notes(
        &self,
        _from_block: u64,
        _to_block: u64,
    ) -> curvy_chain_api::Result<Vec<CommittedNotesEvent>> {
        Ok(Vec::new())
    }

    async fn committed_nullifiers(
        &self,
        _from_block: u64,
        _to_block: u64,
    ) -> curvy_chain_api::Result<Vec<CommittedNullifiersEvent>> {
        Ok(Vec::new())
    }

    async fn head_block(&self) -> curvy_chain_api::Result<u64> {
        Ok(self.head_block)
    }

    async fn notes_tree_snapshot(&self) -> curvy_chain_api::Result<Option<NotesTreeSnapshot>> {
        Ok(None)
    }
}

#[async_trait]
impl TxSubmitter for ScanBackend {
    async fn submit(&self, _raw: &RawTx) -> curvy_chain_api::Result<TxOutcome> {
        Self::unsupported("transaction submission")
    }

    fn backend(&self) -> &'static str {
        "scan-test"
    }
}

#[async_trait]
impl RootAnchor for ScanBackend {
    async fn state(&self) -> curvy_chain_api::Result<AggregatorState> {
        Self::unsupported("root anchoring")
    }

    async fn is_valid_notes_root(&self, _root: &Dec) -> curvy_chain_api::Result<bool> {
        Self::unsupported("root validation")
    }

    async fn note_status(&self, _note_id: &Dec) -> curvy_chain_api::Result<u8> {
        Self::unsupported("note status")
    }
}

#[async_trait]
impl FeeConfigSource for ScanBackend {
    async fn fees(&self) -> curvy_chain_api::Result<FeeConfig> {
        Self::unsupported("fee lookup")
    }
}

#[async_trait]
impl BalanceReader for ScanBackend {
    async fn eth_balance(&self, _address: &Addr) -> curvy_chain_api::Result<Dec> {
        Self::unsupported("ETH balance lookup")
    }

    async fn vault_balance(&self, _owner: &Addr, _token_id: &Dec) -> curvy_chain_api::Result<Dec> {
        Self::unsupported("vault balance lookup")
    }

    async fn tx_count(&self, _address: &Addr) -> curvy_chain_api::Result<u64> {
        Self::unsupported("transaction count lookup")
    }

    async fn gas_price(&self) -> curvy_chain_api::Result<u128> {
        Self::unsupported("gas price lookup")
    }

    async fn chain_id(&self) -> curvy_chain_api::Result<u64> {
        Self::unsupported("chain ID lookup")
    }
}

#[async_trait]
impl PortalDirectory for ScanBackend {
    async fn entry_portal_address(&self, _owner_hash: &Dec, _recovery: &Addr) -> curvy_chain_api::Result<Addr> {
        Self::unsupported("portal derivation")
    }

    async fn portal_is_registered(&self, _portal: &Addr) -> curvy_chain_api::Result<bool> {
        Self::unsupported("portal registration lookup")
    }
}

fn hex32_to_decimal(value: &Hex32) -> Result<String> {
    value
        .0
        .parse::<U256>()
        .map(|value| value.to_string())
        .with_context(|| format!("invalid Blokli Hex32 value {}", value.0))
}

fn pending_event_from_blokli(notes: &[CurvyPendingNote]) -> Result<PendingNotesEvent> {
    ensure!(!notes.is_empty(), "at least one pending note is required");
    let first = &notes[0];
    let block_number = first.position.block.0.parse::<u64>()?;
    let mut event = PendingNotesEvent {
        block_number,
        tx_hash: first.position.transaction_hash.0.clone(),
        ..PendingNotesEvent::default()
    };

    for note in notes {
        ensure!(note.position.block.0.parse::<u64>()? == block_number);
        ensure!(note.position.transaction_hash == first.position.transaction_hash);
        ensure!(
            note.ephemeral_key.len() == 2,
            "ephemeral key must contain two coordinates"
        );
        event.note_ids.push(hex32_to_decimal(&note.note_id)?);
        event.ephemeral_keys[0].push(note.ephemeral_key[0].0.clone());
        event.ephemeral_keys[1].push(note.ephemeral_key[1].0.clone());
        event.view_tags.push(u64::try_from(note.view_tag)?);
        event.tokens.push(note.token_id.0.clone());
        event.amounts.push(note.amount.0.clone());
        event.is_plaintext.push(note.is_plaintext);
    }
    Ok(event)
}

/// Seals an encrypted SDK note and exposes it using Blokli's pending-note DTO.
pub fn encrypted_pending_note(
    account: &Account,
    amount: u64,
    token: u64,
    event_item_index: u64,
) -> Result<(OwnedNote, CurvyPendingNote)> {
    let note = seal_note(
        &account.identity(),
        curvy_core::field::Fr::from(amount),
        curvy_core::field::Fr::from(token),
    )?;
    let note_id_hex = format!("0x{:064x}", curvy_core::field::fr_to_biguint(&note.note_id()));
    let encrypted = curvy_core::cipher::encrypt_amount_token(
        note.amount,
        note.token,
        &curvy_core::field::fr_to_biguint(&note.shared_secret),
        (
            &curvy_core::field::fr_to_biguint(&note.ephemeral_key.0),
            &curvy_core::field::fr_to_biguint(&note.ephemeral_key.1),
        ),
    );
    let pending = CurvyPendingNote {
        note_id: Hex32(note_id_hex),
        ephemeral_key: vec![
            Uint256(curvy_core::field::fr_to_dec(&note.ephemeral_key.0)),
            Uint256(curvy_core::field::fr_to_dec(&note.ephemeral_key.1)),
        ],
        view_tag: i32::from(note.view_tag),
        token_id: Uint256(curvy_core::field::fr_to_dec(&encrypted.encrypted_token)),
        amount: Uint256(curvy_core::field::fr_to_dec(&encrypted.encrypted_amount)),
        is_plaintext: false,
        position: CurvyEventPosition {
            transaction_hash: Hex32(format!("0x{:064x}", 10)),
            block_hash: Hex32(format!("0x{:064x}", 11)),
            block: Uint64("10".to_string()),
            transaction_index: Uint64("2".to_string()),
            log_index: Uint64("7".to_string()),
            event_item_index: Uint64(event_item_index.to_string()),
        },
    };
    Ok((note, pending))
}

/// Scans one Blokli pending-note DTO using the Curvy SDK's ownership and integrity checks.
pub async fn scan_pending_note(note: &CurvyPendingNote, account: &Account) -> Result<Option<Discovered>> {
    let event = pending_event_from_blokli(std::slice::from_ref(note))?;
    let backend = Arc::new(ScanBackend {
        head_block: event.block_number,
        pending_notes: vec![event],
    });
    let client = CurvyClient::new(
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend,
        AGGREGATOR_ADDRESS.to_string(),
        PORTAL_FACTORY_ADDRESS.to_string(),
        31_337,
    );
    let mut discovered = client.scan(account).await?;
    ensure!(discovered.len() <= 1, "a single pending note produced multiple matches");
    Ok(discovered.pop())
}
