//! Cynic types for Curvy indexing, synchronization, contract reads, and subscriptions.
//!
//! Ownership is deliberately not decided by Blokli. Consumers pass pending-note
//! metadata to the Curvy SDK scanner and retain only note IDs owned by the node.

use super::{Hex32, InvalidAddressError, QueryFailedError, Uint64, Uint256, schema};
use crate::errors::{BlokliClientError, ErrorKind};

/// Exclusive chain-position cursor used by paginated Curvy event queries.
#[derive(cynic::InputObject, Clone, Debug, Eq, PartialEq)]
pub struct CurvyEventCursor {
    pub block: Uint64,
    pub transaction_index: Uint64,
    pub log_index: Uint64,
    pub event_item_index: Uint64,
    pub block_hash: Option<Hex32>,
}

impl CurvyEventCursor {
    /// Creates an unanchored exclusive cursor from chain-position components.
    pub fn new(block: u64, transaction_index: u64, log_index: u64, event_item_index: u64) -> Self {
        Self {
            block: Uint64(block.to_string()),
            transaction_index: Uint64(transaction_index.to_string()),
            log_index: Uint64(log_index.to_string()),
            event_item_index: Uint64(event_item_index.to_string()),
            block_hash: None,
        }
    }
}

/// Canonical chain position and transaction identity for a Curvy event item.
#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyEventPosition {
    pub transaction_hash: Hex32,
    pub block_hash: Hex32,
    pub block: Uint64,
    pub transaction_index: Uint64,
    pub log_index: Uint64,
    pub event_item_index: Uint64,
}

impl From<&CurvyEventPosition> for CurvyEventCursor {
    fn from(position: &CurvyEventPosition) -> Self {
        Self {
            block: position.block.clone(),
            transaction_index: position.transaction_index.clone(),
            log_index: position.log_index.clone(),
            event_item_index: position.event_item_index.clone(),
            block_hash: Some(position.block_hash.clone()),
        }
    }
}

/// One pending-note announcement. Feed this metadata to the Curvy SDK ownership scanner.
#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyPendingNote {
    pub note_id: Hex32,
    pub ephemeral_key: Vec<Uint256>,
    pub view_tag: i32,
    pub token_id: Uint256,
    pub amount: Uint256,
    pub is_plaintext: bool,
    pub position: CurvyEventPosition,
}

/// One committed note.
#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyCommittedNote {
    pub batch_index: Hex32,
    pub note_id: Hex32,
    pub leaf_index: Uint64,
    pub position: CurvyEventPosition,
}

/// One committed nullifier.
#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyCommittedNullifier {
    pub batch_index: Hex32,
    pub nullifier: Hex32,
    pub nullifier_index: Uint64,
    pub position: CurvyEventPosition,
}

#[derive(cynic::QueryVariables, Clone, Debug, Default)]
pub struct CurvyEventPageVariables {
    pub from_block: Option<Uint64>,
    pub after: Option<CurvyEventCursor>,
    pub first: Option<i32>,
}

#[derive(cynic::QueryVariables, Clone, Debug, Default)]
pub struct CurvyEventSubscriptionVariables {
    pub from_block: Option<Uint64>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyEventPageVariables")]
pub struct QueryCurvyPendingNotes {
    #[arguments(fromBlock: $from_block, after: $after, first: $first)]
    pub curvy_pending_notes: CurvyPendingNotesResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyEventPageVariables")]
pub struct QueryCurvyCommittedNotes {
    #[arguments(fromBlock: $from_block, after: $after, first: $first)]
    pub curvy_committed_notes: CurvyCommittedNotesResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyEventPageVariables")]
pub struct QueryCurvyCommittedNullifiers {
    #[arguments(fromBlock: $from_block, after: $after, first: $first)]
    pub curvy_committed_nullifiers: CurvyCommittedNullifiersResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "CurvyEventSubscriptionVariables")]
pub struct SubscribeCurvyPendingNote {
    #[arguments(fromBlock: $from_block)]
    pub curvy_pending_note: CurvyPendingNote,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "CurvyEventSubscriptionVariables")]
pub struct SubscribeCurvyCommittedNote {
    #[arguments(fromBlock: $from_block)]
    pub curvy_committed_note: CurvyCommittedNote,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "SubscriptionRoot", variables = "CurvyEventSubscriptionVariables")]
pub struct SubscribeCurvyCommittedNullifier {
    #[arguments(fromBlock: $from_block)]
    pub curvy_committed_nullifier: CurvyCommittedNullifier,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyPendingNotes {
    pub notes: Vec<CurvyPendingNote>,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum CurvyPendingNotesResult {
    CurvyPendingNotes(CurvyPendingNotes),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyCommittedNotes {
    pub notes: Vec<CurvyCommittedNote>,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum CurvyCommittedNotesResult {
    CurvyCommittedNotes(CurvyCommittedNotes),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyCommittedNullifiers {
    pub nullifiers: Vec<CurvyCommittedNullifier>,
}

#[derive(cynic::InlineFragments, Debug)]
pub enum CurvyCommittedNullifiersResult {
    CurvyCommittedNullifiers(CurvyCommittedNullifiers),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

/// Finalized Curvy synchronization checkpoint.
#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvySyncCheckpoint {
    pub block_number: Uint64,
    pub block_hash: Hex32,
    pub aggregator_address: String,
    pub tree_version: i32,
    pub tree_depth: i32,
    pub shard_height: i32,
    pub shard_size: Uint64,
    pub note_count: Uint64,
    pub nullifier_count: Uint64,
    pub shard_count: Uint64,
    pub notes_root: Hex32,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvySyncNote {
    pub leaf_index: Uint64,
    pub note_id: Hex32,
    pub batch_index: Hex32,
    pub announcement: Option<CurvyPendingNote>,
    pub commit_position: CurvyEventPosition,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvySyncNotePage {
    pub checkpoint: Hex32,
    pub notes: Vec<CurvySyncNote>,
    pub next_index: Uint64,
    pub total: Uint64,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvySyncNullifierPage {
    pub checkpoint: Hex32,
    pub nullifiers: Vec<CurvyCommittedNullifier>,
    pub next_index: Uint64,
    pub total: Uint64,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyShardRoot {
    pub shard_index: Uint64,
    pub root: Hex32,
    pub completion_position: CurvyEventPosition,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyShardRootPage {
    pub checkpoint: Hex32,
    pub shard_roots: Vec<CurvyShardRoot>,
    pub next_index: Uint64,
    pub total: Uint64,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyAggregatorState {
    pub notes_tree_root: Hex32,
    pub notes_batch_index: Uint256,
    pub nullifiers_batch_index: Uint256,
    pub note_index: Uint256,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyNoteStatus {
    pub status: i32,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyBooleanValue {
    pub value: bool,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyVaultFees {
    pub deposit_fee: Uint256,
    pub withdrawal_fee: Uint256,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyAggregatorFees {
    pub protocol_fee_per_thousand: Uint256,
    pub commitment_fee_root: Hex32,
    pub fee_note_public_key: Vec<Uint256>,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyGasFees {
    pub token_id: Uint256,
    pub portal_deployment: Uint256,
    pub pending_note_commitment: Uint256,
    pub withdrawal: Uint256,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyVaultToken {
    pub token_address: String,
    pub gas_fees: CurvyGasFees,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyVaultTokenCount {
    pub count: Uint256,
}

#[derive(cynic::QueryFragment, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CurvyAddress {
    pub address: String,
}

#[derive(cynic::QueryVariables, Clone, Debug, Default)]
pub struct CurvyCheckpointVariables {
    pub block_hash: Option<Hex32>,
}

#[derive(cynic::QueryVariables, Clone, Debug)]
pub struct CurvySyncPageVariables {
    pub checkpoint: Hex32,
    pub from_index: Option<Uint64>,
    pub first: Option<i32>,
}

#[derive(cynic::QueryVariables, Clone, Debug)]
pub struct CurvyNoteIdVariables {
    pub note_id: Hex32,
}

#[derive(cynic::QueryVariables, Clone, Debug)]
pub struct CurvyRootVariables {
    pub root: Hex32,
}

#[derive(cynic::QueryVariables, Clone, Debug)]
pub struct CurvyNullifierVariables {
    pub nullifier: Hex32,
}

#[derive(cynic::QueryVariables, Clone, Debug)]
pub struct CurvyVaultTokenVariables {
    pub token_id: Uint256,
}

#[derive(cynic::QueryVariables, Clone, Debug)]
pub struct CurvyEntryPortalVariables {
    pub owner_hash: Uint256,
    pub recovery: String,
}

#[derive(cynic::QueryVariables, Clone, Debug)]
pub struct CurvyExitPortalVariables {
    pub exit_address: String,
    pub exit_chain_id: Uint256,
    pub recovery: String,
}

#[derive(cynic::QueryVariables, Clone, Debug)]
pub struct CurvyPortalVariables {
    pub portal_address: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyCheckpointVariables")]
pub struct QueryCurvySyncCheckpoint {
    #[arguments(blockHash: $block_hash)]
    pub curvy_sync_checkpoint: CurvySyncCheckpointResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvySyncPageVariables")]
pub struct QueryCurvySyncNotes {
    #[arguments(checkpoint: $checkpoint, fromIndex: $from_index, first: $first)]
    pub curvy_sync_notes: CurvySyncNotesResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvySyncPageVariables")]
pub struct QueryCurvySyncNullifiers {
    #[arguments(checkpoint: $checkpoint, fromIndex: $from_index, first: $first)]
    pub curvy_sync_nullifiers: CurvySyncNullifiersResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvySyncPageVariables")]
pub struct QueryCurvyShardRoots {
    #[arguments(checkpoint: $checkpoint, fromIndex: $from_index, first: $first)]
    pub curvy_shard_roots: CurvyShardRootsResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot")]
pub struct QueryCurvyAggregatorState {
    pub curvy_aggregator_state: CurvyAggregatorStateResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyNoteIdVariables")]
pub struct QueryCurvyNoteStatus {
    #[arguments(noteId: $note_id)]
    pub curvy_note_status: CurvyNoteStatusResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyRootVariables")]
pub struct QueryCurvyValidNotesRoot {
    #[arguments(root: $root)]
    pub curvy_valid_notes_root: CurvyValidNotesRootResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyNullifierVariables")]
pub struct QueryCurvyNullifierSpent {
    #[arguments(nullifier: $nullifier)]
    pub curvy_nullifier_spent: CurvyNullifierSpentResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot")]
pub struct QueryCurvyVaultFees {
    pub curvy_vault_fees: CurvyVaultFeesResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot")]
pub struct QueryCurvyAggregatorFees {
    pub curvy_aggregator_fees: CurvyAggregatorFeesResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot")]
pub struct QueryCurvyVaultTokenCount {
    pub curvy_vault_token_count: CurvyVaultTokenCountResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyVaultTokenVariables")]
pub struct QueryCurvyVaultToken {
    #[arguments(tokenId: $token_id)]
    pub curvy_vault_token: CurvyVaultTokenResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyEntryPortalVariables")]
pub struct QueryCurvyEntryPortalAddress {
    #[arguments(ownerHash: $owner_hash, recovery: $recovery)]
    pub curvy_entry_portal_address: CurvyEntryPortalAddressResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyExitPortalVariables")]
pub struct QueryCurvyExitPortalAddress {
    #[arguments(exitAddress: $exit_address, exitChainId: $exit_chain_id, recovery: $recovery)]
    pub curvy_exit_portal_address: CurvyExitPortalAddressResult,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(graphql_type = "QueryRoot", variables = "CurvyPortalVariables")]
pub struct QueryCurvyPortalRegistered {
    #[arguments(portalAddress: $portal_address)]
    pub curvy_portal_registered: CurvyPortalRegisteredResult,
}

macro_rules! simple_union {
    ($name:ident, $success:ident, $value:ty) => {
        #[derive(cynic::InlineFragments, Debug)]
        pub enum $name {
            $success($value),
            QueryFailedError(QueryFailedError),
            #[cynic(fallback)]
            Unknown,
        }

        impl From<$name> for Result<$value, BlokliClientError> {
            fn from(value: $name) -> Self {
                match value {
                    $name::$success(value) => Ok(value),
                    $name::QueryFailedError(error) => Err(error.into()),
                    $name::Unknown => Err(ErrorKind::NoData.into()),
                }
            }
        }
    };
}

simple_union!(CurvySyncCheckpointResult, CurvySyncCheckpoint, CurvySyncCheckpoint);
simple_union!(CurvySyncNotesResult, CurvySyncNotePage, CurvySyncNotePage);
simple_union!(
    CurvySyncNullifiersResult,
    CurvySyncNullifierPage,
    CurvySyncNullifierPage
);
simple_union!(CurvyShardRootsResult, CurvyShardRootPage, CurvyShardRootPage);
simple_union!(CurvyAggregatorStateResult, CurvyAggregatorState, CurvyAggregatorState);
simple_union!(CurvyNoteStatusResult, CurvyNoteStatus, CurvyNoteStatus);
simple_union!(CurvyValidNotesRootResult, CurvyBooleanValue, CurvyBooleanValue);
simple_union!(CurvyNullifierSpentResult, CurvyBooleanValue, CurvyBooleanValue);
simple_union!(CurvyVaultFeesResult, CurvyVaultFees, CurvyVaultFees);
simple_union!(CurvyAggregatorFeesResult, CurvyAggregatorFees, CurvyAggregatorFees);
simple_union!(CurvyVaultTokenCountResult, CurvyVaultTokenCount, CurvyVaultTokenCount);
simple_union!(CurvyVaultTokenResult, CurvyVaultToken, CurvyVaultToken);

macro_rules! address_union {
    ($name:ident) => {
        #[derive(cynic::InlineFragments, Debug)]
        pub enum $name {
            CurvyAddress(CurvyAddress),
            InvalidAddressError(InvalidAddressError),
            QueryFailedError(QueryFailedError),
            #[cynic(fallback)]
            Unknown,
        }

        impl From<$name> for Result<CurvyAddress, BlokliClientError> {
            fn from(value: $name) -> Self {
                match value {
                    $name::CurvyAddress(value) => Ok(value),
                    $name::InvalidAddressError(error) => Err(error.into()),
                    $name::QueryFailedError(error) => Err(error.into()),
                    $name::Unknown => Err(ErrorKind::NoData.into()),
                }
            }
        }
    };
}

address_union!(CurvyEntryPortalAddressResult);
address_union!(CurvyExitPortalAddressResult);

#[derive(cynic::InlineFragments, Debug)]
pub enum CurvyPortalRegisteredResult {
    CurvyBooleanValue(CurvyBooleanValue),
    InvalidAddressError(InvalidAddressError),
    QueryFailedError(QueryFailedError),
    #[cynic(fallback)]
    Unknown,
}

impl From<CurvyPortalRegisteredResult> for Result<CurvyBooleanValue, BlokliClientError> {
    fn from(value: CurvyPortalRegisteredResult) -> Self {
        match value {
            CurvyPortalRegisteredResult::CurvyBooleanValue(value) => Ok(value),
            CurvyPortalRegisteredResult::InvalidAddressError(error) => Err(error.into()),
            CurvyPortalRegisteredResult::QueryFailedError(error) => Err(error.into()),
            CurvyPortalRegisteredResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

impl From<CurvyPendingNotesResult> for Result<CurvyPendingNotes, BlokliClientError> {
    fn from(value: CurvyPendingNotesResult) -> Self {
        match value {
            CurvyPendingNotesResult::CurvyPendingNotes(notes) => Ok(notes),
            CurvyPendingNotesResult::QueryFailedError(error) => Err(error.into()),
            CurvyPendingNotesResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

impl From<CurvyCommittedNotesResult> for Result<CurvyCommittedNotes, BlokliClientError> {
    fn from(value: CurvyCommittedNotesResult) -> Self {
        match value {
            CurvyCommittedNotesResult::CurvyCommittedNotes(notes) => Ok(notes),
            CurvyCommittedNotesResult::QueryFailedError(error) => Err(error.into()),
            CurvyCommittedNotesResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

impl From<CurvyCommittedNullifiersResult> for Result<CurvyCommittedNullifiers, BlokliClientError> {
    fn from(value: CurvyCommittedNullifiersResult) -> Self {
        match value {
            CurvyCommittedNullifiersResult::CurvyCommittedNullifiers(nullifiers) => Ok(nullifiers),
            CurvyCommittedNullifiersResult::QueryFailedError(error) => Err(error.into()),
            CurvyCommittedNullifiersResult::Unknown => Err(ErrorKind::NoData.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::{
        CurvyAddress, CurvyBooleanValue, CurvyCommittedNotes, CurvyCommittedNotesResult, CurvyCommittedNullifiers,
        CurvyCommittedNullifiersResult, CurvyEntryPortalAddressResult, CurvyEventCursor, CurvyEventPosition,
        CurvyNoteStatus, CurvyNoteStatusResult, CurvyPendingNotes, CurvyPendingNotesResult,
        CurvyPortalRegisteredResult, Hex32, InvalidAddressError, QueryFailedError, Uint64,
    };
    use crate::errors::{BlokliClientError, ErrorKind};

    fn query_failed_error() -> QueryFailedError {
        QueryFailedError {
            __typename: "QueryFailedError".to_owned(),
            message: "query failed".to_owned(),
            code: "QUERY_FAILED".to_owned(),
        }
    }

    fn invalid_address_error() -> InvalidAddressError {
        InvalidAddressError {
            __typename: "InvalidAddressError".to_owned(),
            message: "invalid address".to_owned(),
            code: "INVALID_ADDRESS".to_owned(),
        }
    }

    fn assert_no_data<T: Debug>(result: Result<T, BlokliClientError>) {
        assert!(matches!(
            result.expect_err("conversion should fail").kind(),
            ErrorKind::NoData
        ));
    }

    #[test]
    fn event_position_converts_to_anchored_cursor() {
        let position = CurvyEventPosition {
            transaction_hash: Hex32("0xtx".to_owned()),
            block_hash: Hex32("0xblock".to_owned()),
            block: Uint64("10".to_owned()),
            transaction_index: Uint64("2".to_owned()),
            log_index: Uint64("3".to_owned()),
            event_item_index: Uint64("4".to_owned()),
        };

        let cursor = CurvyEventCursor::from(&position);

        assert_eq!(
            cursor,
            CurvyEventCursor {
                block: Uint64("10".to_owned()),
                transaction_index: Uint64("2".to_owned()),
                log_index: Uint64("3".to_owned()),
                event_item_index: Uint64("4".to_owned()),
                block_hash: Some(Hex32("0xblock".to_owned())),
            }
        );
        assert_eq!(CurvyEventCursor::new(10, 2, 3, 4).block_hash, None);
    }

    #[test]
    fn simple_union_converts_success_and_errors() {
        let status: Result<CurvyNoteStatus, BlokliClientError> =
            CurvyNoteStatusResult::CurvyNoteStatus(CurvyNoteStatus { status: 2 }).into();
        assert_eq!(status.expect("status should convert").status, 2);

        let error: Result<CurvyNoteStatus, BlokliClientError> =
            CurvyNoteStatusResult::QueryFailedError(query_failed_error()).into();
        assert!(matches!(
            error.expect_err("query failure should convert").kind(),
            ErrorKind::BlokliError {
                kind: "query failed",
                ..
            }
        ));

        assert_no_data(Result::<CurvyNoteStatus, BlokliClientError>::from(
            CurvyNoteStatusResult::Unknown,
        ));
    }

    #[test]
    fn address_union_converts_every_variant() {
        let address: Result<CurvyAddress, BlokliClientError> =
            CurvyEntryPortalAddressResult::CurvyAddress(CurvyAddress {
                address: "0x1234".to_owned(),
            })
            .into();
        assert_eq!(address.expect("address should convert").address, "0x1234");

        let invalid: Result<CurvyAddress, BlokliClientError> =
            CurvyEntryPortalAddressResult::InvalidAddressError(invalid_address_error()).into();
        assert!(matches!(
            invalid.expect_err("invalid address should convert").kind(),
            ErrorKind::BlokliError {
                kind: "invalid address",
                ..
            }
        ));

        let failed: Result<CurvyAddress, BlokliClientError> =
            CurvyEntryPortalAddressResult::QueryFailedError(query_failed_error()).into();
        assert!(matches!(
            failed.expect_err("query failure should convert").kind(),
            ErrorKind::BlokliError {
                kind: "query failed",
                ..
            }
        ));

        assert_no_data(Result::<CurvyAddress, BlokliClientError>::from(
            CurvyEntryPortalAddressResult::Unknown,
        ));
    }

    #[test]
    fn portal_registered_union_converts_every_variant() {
        let registered: Result<CurvyBooleanValue, BlokliClientError> =
            CurvyPortalRegisteredResult::CurvyBooleanValue(CurvyBooleanValue { value: true }).into();
        assert!(registered.expect("boolean should convert").value);

        let invalid: Result<CurvyBooleanValue, BlokliClientError> =
            CurvyPortalRegisteredResult::InvalidAddressError(invalid_address_error()).into();
        assert!(matches!(
            invalid.expect_err("invalid address should convert").kind(),
            ErrorKind::BlokliError {
                kind: "invalid address",
                ..
            }
        ));

        let failed: Result<CurvyBooleanValue, BlokliClientError> =
            CurvyPortalRegisteredResult::QueryFailedError(query_failed_error()).into();
        assert!(matches!(
            failed.expect_err("query failure should convert").kind(),
            ErrorKind::BlokliError {
                kind: "query failed",
                ..
            }
        ));

        assert_no_data(Result::<CurvyBooleanValue, BlokliClientError>::from(
            CurvyPortalRegisteredResult::Unknown,
        ));
    }

    #[test]
    fn event_page_unions_convert_success_and_errors() {
        let pending: Result<CurvyPendingNotes, BlokliClientError> =
            CurvyPendingNotesResult::CurvyPendingNotes(CurvyPendingNotes { notes: Vec::new() }).into();
        assert!(pending.expect("pending notes should convert").notes.is_empty());
        let committed: Result<CurvyCommittedNotes, BlokliClientError> =
            CurvyCommittedNotesResult::CurvyCommittedNotes(CurvyCommittedNotes { notes: Vec::new() }).into();
        assert!(committed.expect("committed notes should convert").notes.is_empty());
        let nullifiers: Result<CurvyCommittedNullifiers, BlokliClientError> =
            CurvyCommittedNullifiersResult::CurvyCommittedNullifiers(CurvyCommittedNullifiers {
                nullifiers: Vec::new(),
            })
            .into();
        assert!(nullifiers.expect("nullifiers should convert").nullifiers.is_empty());

        let pending_error: Result<CurvyPendingNotes, BlokliClientError> =
            CurvyPendingNotesResult::QueryFailedError(query_failed_error()).into();
        assert!(matches!(
            pending_error.expect_err("query failure should convert").kind(),
            ErrorKind::BlokliError { .. }
        ));
        let committed_error: Result<CurvyCommittedNotes, BlokliClientError> =
            CurvyCommittedNotesResult::QueryFailedError(query_failed_error()).into();
        assert!(matches!(
            committed_error.expect_err("query failure should convert").kind(),
            ErrorKind::BlokliError { .. }
        ));
        let nullifier_error: Result<CurvyCommittedNullifiers, BlokliClientError> =
            CurvyCommittedNullifiersResult::QueryFailedError(query_failed_error()).into();
        assert!(matches!(
            nullifier_error.expect_err("query failure should convert").kind(),
            ErrorKind::BlokliError { .. }
        ));

        assert_no_data(Result::<CurvyPendingNotes, BlokliClientError>::from(
            CurvyPendingNotesResult::Unknown,
        ));
        assert_no_data(Result::<CurvyCommittedNotes, BlokliClientError>::from(
            CurvyCommittedNotesResult::Unknown,
        ));
        assert_no_data(Result::<CurvyCommittedNullifiers, BlokliClientError>::from(
            CurvyCommittedNullifiersResult::Unknown,
        ));
    }
}
