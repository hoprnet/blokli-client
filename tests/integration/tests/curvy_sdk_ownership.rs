//! Compatibility test for the Blokli Curvy note DTO and Curvy SDK ownership scanner.

use anyhow::Result;
use blokli_integration_tests::curvy_sdk::{encrypted_pending_note, scan_pending_note};
use curvy_sdk::Account;

#[tokio::test]
async fn blokli_pending_notes_are_filtered_by_the_curvy_sdk_owner_scan() -> Result<()> {
    let exit = Account::from_signature_components("1", "2")?;
    let foreign_exit = Account::from_signature_components("3", "4")?;
    let (owned_note, owned_pending) = encrypted_pending_note(&exit, 1_000, 1, 0)?;
    let (foreign_note, foreign_pending) = encrypted_pending_note(&foreign_exit, 2_000, 1, 1)?;
    let discovered = scan_pending_note(&owned_pending, &exit)
        .await?
        .expect("the Exit must discover its own note");
    let rejected = scan_pending_note(&foreign_pending, &exit).await?;
    let foreign_discovered = scan_pending_note(&foreign_pending, &foreign_exit)
        .await?
        .expect("the foreign Exit must discover its own note");

    assert_eq!(discovered.note_id, owned_note.note_id());
    assert_eq!(discovered.amount, owned_note.amount);
    assert_eq!(discovered.token, owned_note.token);
    assert!(!discovered.is_plaintext);
    assert!(rejected.is_none());
    assert_eq!(foreign_discovered.note_id, foreign_note.note_id());
    Ok(())
}
