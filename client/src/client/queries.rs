use std::collections::HashSet;

use cynic::QueryBuilder;
use hex::ToHex;

use super::{BlokliClient, GraphQlQueries, response_to_data};
use crate::{
    api::{internal::*, types::*, *},
    errors::{BlokliClientError, ErrorKind},
};

fn parse_chain_address_hex(value: &str) -> Result<ChainAddress> {
    let bytes = hex::decode(value.trim_start_matches("0x")).map_err(|_| ErrorKind::ParseError)?;
    bytes.try_into().map_err(|_| ErrorKind::ParseError.into())
}

impl BlokliClient {
    async fn source_key_ids_for_safe(&self, safe_address: ChainAddress) -> Result<HashSet<i32>> {
        let safe_response = self
            .build_query(GraphQlQueries::query_safe_by(SafeSelectorInput::Address, &safe_address))?
            .await?;

        let safe: Option<Safe> = match response_to_data(safe_response)?.safe_by {
            Some(safe_result) => {
                let parsed_safes: Result<Vec<Safe>> = safe_result.into();
                parsed_safes?.into_iter().next()
            }
            None => None,
        };

        let Some(safe) = safe else {
            return Ok(HashSet::new());
        };

        let mut source_key_ids = HashSet::new();
        for registered_node in safe.registered_nodes {
            let node_address = parse_chain_address_hex(&registered_node)?;
            let accounts_response = self
                .build_query(GraphQlQueries::query_accounts(AccountSelector::Address(node_address)))?
                .await?;
            let accounts_result = response_to_data(accounts_response)?.accounts;
            let accounts: Vec<Account> = {
                let parsed_accounts: Result<Vec<Account>> = accounts_result.into();
                parsed_accounts?
            };
            for account in accounts {
                source_key_ids.insert(account.keyid);
            }
        }

        Ok(source_key_ids)
    }

    async fn filter_channels_by_safe(
        &self,
        channels: ChannelsList,
        safe_address: ChainAddress,
    ) -> Result<ChannelsList> {
        let source_key_ids = self.source_key_ids_for_safe(safe_address).await?;
        let filtered_channels: Vec<Channel> = channels
            .channels
            .into_iter()
            .filter(|channel| source_key_ids.contains(&channel.source))
            .collect();

        Ok(ChannelsList {
            __typename: channels.__typename,
            channels: filtered_channels,
        })
    }
}

impl GraphQlQueries {
    fn curvy_page_size(first: u32) -> Result<i32> {
        let first = i32::try_from(first).map_err(|_| ErrorKind::InvalidInput("Curvy page size exceeds i32"))?;
        if !(1..=1_000).contains(&first) {
            return Err(ErrorKind::InvalidInput("Curvy page size must be between 1 and 1000").into());
        }
        Ok(first)
    }

    fn curvy_event_page_variables(
        from_block: Option<u64>,
        after: Option<CurvyEventCursor>,
        first: u32,
    ) -> Result<CurvyEventPageVariables> {
        let first = Self::curvy_page_size(first)?;
        Ok(CurvyEventPageVariables {
            from_block: from_block.map(|block| Uint64(block.to_string())),
            after,
            first: Some(first),
        })
    }

    /// Paginated pending Curvy note query.
    pub fn query_curvy_pending_notes(
        from_block: Option<u64>,
        after: Option<CurvyEventCursor>,
        first: u32,
    ) -> Result<cynic::Operation<QueryCurvyPendingNotes, CurvyEventPageVariables>> {
        Ok(QueryCurvyPendingNotes::build(Self::curvy_event_page_variables(
            from_block, after, first,
        )?))
    }

    /// Paginated committed Curvy note query.
    pub fn query_curvy_committed_notes(
        from_block: Option<u64>,
        after: Option<CurvyEventCursor>,
        first: u32,
    ) -> Result<cynic::Operation<QueryCurvyCommittedNotes, CurvyEventPageVariables>> {
        Ok(QueryCurvyCommittedNotes::build(Self::curvy_event_page_variables(
            from_block, after, first,
        )?))
    }

    /// Paginated committed Curvy nullifier query.
    pub fn query_curvy_committed_nullifiers(
        from_block: Option<u64>,
        after: Option<CurvyEventCursor>,
        first: u32,
    ) -> Result<cynic::Operation<QueryCurvyCommittedNullifiers, CurvyEventPageVariables>> {
        Ok(QueryCurvyCommittedNullifiers::build(Self::curvy_event_page_variables(
            from_block, after, first,
        )?))
    }

    pub fn query_curvy_sync_checkpoint(
        block_hash: Option<String>,
    ) -> cynic::Operation<QueryCurvySyncCheckpoint, CurvyCheckpointVariables> {
        QueryCurvySyncCheckpoint::build(CurvyCheckpointVariables {
            block_hash: block_hash.map(Hex32),
        })
    }

    fn curvy_sync_page_variables(
        checkpoint: String,
        from_index: Option<u64>,
        first: u32,
    ) -> Result<CurvySyncPageVariables> {
        Ok(CurvySyncPageVariables {
            checkpoint: Hex32(checkpoint),
            from_index: from_index.map(|index| Uint64(index.to_string())),
            first: Some(Self::curvy_page_size(first)?),
        })
    }

    pub fn query_curvy_sync_notes(
        checkpoint: String,
        from_index: Option<u64>,
        first: u32,
    ) -> Result<cynic::Operation<QueryCurvySyncNotes, CurvySyncPageVariables>> {
        Ok(QueryCurvySyncNotes::build(Self::curvy_sync_page_variables(
            checkpoint, from_index, first,
        )?))
    }

    pub fn query_curvy_sync_nullifiers(
        checkpoint: String,
        from_index: Option<u64>,
        first: u32,
    ) -> Result<cynic::Operation<QueryCurvySyncNullifiers, CurvySyncPageVariables>> {
        Ok(QueryCurvySyncNullifiers::build(Self::curvy_sync_page_variables(
            checkpoint, from_index, first,
        )?))
    }

    pub fn query_curvy_shard_roots(
        checkpoint: String,
        from_index: Option<u64>,
        first: u32,
    ) -> Result<cynic::Operation<QueryCurvyShardRoots, CurvySyncPageVariables>> {
        Ok(QueryCurvyShardRoots::build(Self::curvy_sync_page_variables(
            checkpoint, from_index, first,
        )?))
    }

    pub fn query_curvy_aggregator_state() -> cynic::Operation<QueryCurvyAggregatorState, ()> {
        QueryCurvyAggregatorState::build(())
    }

    pub fn query_curvy_note_status(note_id: String) -> cynic::Operation<QueryCurvyNoteStatus, CurvyNoteIdVariables> {
        QueryCurvyNoteStatus::build(CurvyNoteIdVariables {
            note_id: Hex32(note_id),
        })
    }

    pub fn query_curvy_valid_notes_root(
        root: String,
    ) -> cynic::Operation<QueryCurvyValidNotesRoot, CurvyRootVariables> {
        QueryCurvyValidNotesRoot::build(CurvyRootVariables { root: Hex32(root) })
    }

    pub fn query_curvy_nullifier_spent(
        nullifier: String,
    ) -> cynic::Operation<QueryCurvyNullifierSpent, CurvyNullifierVariables> {
        QueryCurvyNullifierSpent::build(CurvyNullifierVariables {
            nullifier: Hex32(nullifier),
        })
    }

    pub fn query_curvy_vault_fees() -> cynic::Operation<QueryCurvyVaultFees, ()> {
        QueryCurvyVaultFees::build(())
    }

    pub fn query_curvy_aggregator_fees() -> cynic::Operation<QueryCurvyAggregatorFees, ()> {
        QueryCurvyAggregatorFees::build(())
    }

    pub fn query_curvy_vault_token_count() -> cynic::Operation<QueryCurvyVaultTokenCount, ()> {
        QueryCurvyVaultTokenCount::build(())
    }

    pub fn query_curvy_vault_token(
        token_id: String,
    ) -> cynic::Operation<QueryCurvyVaultToken, CurvyVaultTokenVariables> {
        QueryCurvyVaultToken::build(CurvyVaultTokenVariables {
            token_id: Uint256(token_id),
        })
    }

    pub fn query_curvy_entry_portal_address(
        owner_hash: String,
        recovery: String,
    ) -> cynic::Operation<QueryCurvyEntryPortalAddress, CurvyEntryPortalVariables> {
        QueryCurvyEntryPortalAddress::build(CurvyEntryPortalVariables {
            owner_hash: Uint256(owner_hash),
            recovery,
        })
    }

    pub fn query_curvy_exit_portal_address(
        exit_address: String,
        exit_chain_id: String,
        recovery: String,
    ) -> cynic::Operation<QueryCurvyExitPortalAddress, CurvyExitPortalVariables> {
        QueryCurvyExitPortalAddress::build(CurvyExitPortalVariables {
            exit_address,
            exit_chain_id: Uint256(exit_chain_id),
            recovery,
        })
    }

    pub fn query_curvy_portal_registered(
        portal_address: String,
    ) -> cynic::Operation<QueryCurvyPortalRegistered, CurvyPortalVariables> {
        QueryCurvyPortalRegistered::build(CurvyPortalVariables { portal_address })
    }

    /// `AccountCount` GraphQL query.
    pub fn count_accounts(selector: AccountSelector) -> cynic::Operation<QueryAccountCount, AccountVariables> {
        QueryAccountCount::build(AccountVariables::from(selector))
    }

    /// `Accounts` GraphQL query.
    pub fn query_accounts(selector: AccountSelector) -> cynic::Operation<QueryAccounts, AccountVariables> {
        QueryAccounts::build(AccountVariables::from(selector))
    }

    /// `NativeBalance` GraphQL query.
    pub fn query_native_balance(address: &ChainAddress) -> cynic::Operation<QueryNativeBalance, BalanceVariables> {
        QueryNativeBalance::build(BalanceVariables {
            address: address.encode_hex(),
            token: None,
        })
    }

    /// `TokenBalance` GraphQL query.
    pub fn query_token_balance(
        address: &ChainAddress,
        token: Token,
    ) -> cynic::Operation<QueryHoprBalance, BalanceVariables> {
        QueryHoprBalance::build(BalanceVariables {
            address: address.encode_hex(),
            token: Some(token),
        })
    }

    /// `TransactionCount` GraphQL query.
    pub fn query_transaction_count(address: &ChainAddress) -> cynic::Operation<QueryTxCount, TxCountVariables> {
        QueryTxCount::build(TxCountVariables {
            address: address.encode_hex(),
        })
    }

    /// `SafeAllowance` GraphQL query.
    pub fn query_safe_allowance(address: &ChainAddress) -> cynic::Operation<QuerySafeAllowance, BalanceVariables> {
        QuerySafeAllowance::build(BalanceVariables {
            address: address.encode_hex(),
            token: None,
        })
    }

    /// `TicketRedemptionStats` GraphQL query.
    pub fn query_redeemed_stats(
        selector: RedeemedStatsSelector,
    ) -> cynic::Operation<QueryRedeemedStats, RedeemedStatsVariables> {
        QueryRedeemedStats::build(RedeemedStatsVariables {
            filter: match selector {
                RedeemedStatsSelector::SafeAddress(safe) => RedeemedStatsFilter {
                    safe_address: Some(safe.encode_hex()),
                    node_address: None,
                },
                RedeemedStatsSelector::NodeAddress(node) => RedeemedStatsFilter {
                    safe_address: None,
                    node_address: Some(node.encode_hex()),
                },
                RedeemedStatsSelector::SafeAndNodeAddress {
                    safe_address,
                    node_address,
                } => RedeemedStatsFilter {
                    safe_address: Some(safe_address.encode_hex()),
                    node_address: Some(node_address.encode_hex()),
                },
            },
        })
    }

    /// `safeBy` GraphQL query.
    pub fn query_safe_by(
        selector: SafeSelectorInput,
        address: &ChainAddress,
    ) -> cynic::Operation<QuerySafeBy, SafeByVariables> {
        QuerySafeBy::build(SafeByVariables {
            selector,
            address: address.encode_hex(),
        })
    }

    /// `ModuleAddressPrediction` GraphQL query.
    pub fn query_module_address_prediction(
        input: ModulePredictionInput,
    ) -> cynic::Operation<QueryModuleAddress, ModuleAddressVariables> {
        QueryModuleAddress::build(ModuleAddressVariables {
            nonce: Uint64(input.nonce.to_string()),
            owner: input.owner.encode_hex(),
            safe_address: input.safe_address.encode_hex(),
        })
    }

    /// `ChannelCount` GraphQL query.
    #[deprecated(note = "Use query_channel_stats instead, which returns both count and total wxHOPR balance.")]
    pub fn query_channel_count(selector: ChannelSelector) -> cynic::Operation<QueryChannelCount, ChannelsVariables> {
        QueryChannelCount::build(ChannelsVariables::from(selector))
    }

    /// `ChannelStats` GraphQL query.
    pub fn query_channel_stats(
        selector: ChannelSelector,
    ) -> cynic::Operation<QueryChannelStats, ChannelStatsVariables> {
        QueryChannelStats::build(ChannelStatsVariables::from(selector))
    }

    /// `Channels` GraphQL query.
    pub fn query_channels(selector: ChannelSelector) -> cynic::Operation<QueryChannels, ChannelsVariables> {
        QueryChannels::build(ChannelsVariables::from(selector))
    }

    /// `SafesBalance` GraphQL query.
    pub fn query_safes_balance(
        owner_address: Option<ChainAddress>,
    ) -> cynic::Operation<QuerySafesBalance, SafesBalanceVariables> {
        QuerySafesBalance::build(SafesBalanceVariables {
            owner_address: owner_address.map(hex::encode),
        })
    }

    /// `Transaction` GraphQL query.
    pub fn query_transaction(id: TxId) -> cynic::Operation<QueryTransaction, TransactionsVariables> {
        QueryTransaction::build(TransactionsVariables { id: id.into() })
    }

    /// `ChainInfo` GraphQL query.
    pub fn query_chain_info() -> cynic::Operation<QueryChainInfo, ()> {
        QueryChainInfo::build(())
    }

    /// `Version` GraphQL query.
    pub fn query_version() -> cynic::Operation<QueryVersion, ()> {
        QueryVersion::build(())
    }

    /// `Health` GraphQL query.
    pub fn query_health() -> cynic::Operation<QueryHealth, ()> {
        QueryHealth::build(())
    }

    /// `Compatibility` GraphQL query.
    pub fn query_compatibility() -> cynic::Operation<QueryCompatibility, ()> {
        QueryCompatibility::build(())
    }
}

#[async_trait::async_trait]
impl BlokliQueryClient for BlokliClient {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_pending_notes(
        &self,
        from_block: Option<u64>,
        after: Option<CurvyEventCursor>,
        first: u32,
    ) -> Result<CurvyPendingNotes> {
        let operation = GraphQlQueries::query_curvy_pending_notes(from_block, after, first)?;
        let response = self.build_query(operation)?.await?;
        response_to_data(response)?.curvy_pending_notes.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_committed_notes(
        &self,
        from_block: Option<u64>,
        after: Option<CurvyEventCursor>,
        first: u32,
    ) -> Result<CurvyCommittedNotes> {
        let operation = GraphQlQueries::query_curvy_committed_notes(from_block, after, first)?;
        let response = self.build_query(operation)?.await?;
        response_to_data(response)?.curvy_committed_notes.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_committed_nullifiers(
        &self,
        from_block: Option<u64>,
        after: Option<CurvyEventCursor>,
        first: u32,
    ) -> Result<CurvyCommittedNullifiers> {
        let operation = GraphQlQueries::query_curvy_committed_nullifiers(from_block, after, first)?;
        let response = self.build_query(operation)?.await?;
        response_to_data(response)?.curvy_committed_nullifiers.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_sync_checkpoint(&self, block_hash: Option<String>) -> Result<CurvySyncCheckpoint> {
        let response = self
            .build_query(GraphQlQueries::query_curvy_sync_checkpoint(block_hash))?
            .await?;
        response_to_data(response)?.curvy_sync_checkpoint.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_sync_notes(
        &self,
        checkpoint: String,
        from_index: Option<u64>,
        first: u32,
    ) -> Result<CurvySyncNotePage> {
        let operation = GraphQlQueries::query_curvy_sync_notes(checkpoint, from_index, first)?;
        let response = self.build_query(operation)?.await?;
        response_to_data(response)?.curvy_sync_notes.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_sync_nullifiers(
        &self,
        checkpoint: String,
        from_index: Option<u64>,
        first: u32,
    ) -> Result<CurvySyncNullifierPage> {
        let operation = GraphQlQueries::query_curvy_sync_nullifiers(checkpoint, from_index, first)?;
        let response = self.build_query(operation)?.await?;
        response_to_data(response)?.curvy_sync_nullifiers.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_shard_roots(
        &self,
        checkpoint: String,
        from_index: Option<u64>,
        first: u32,
    ) -> Result<CurvyShardRootPage> {
        let operation = GraphQlQueries::query_curvy_shard_roots(checkpoint, from_index, first)?;
        let response = self.build_query(operation)?.await?;
        response_to_data(response)?.curvy_shard_roots.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_aggregator_state(&self) -> Result<CurvyAggregatorState> {
        let response = self
            .build_query(GraphQlQueries::query_curvy_aggregator_state())?
            .await?;
        response_to_data(response)?.curvy_aggregator_state.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_note_status(&self, note_id: String) -> Result<CurvyNoteStatus> {
        let response = self
            .build_query(GraphQlQueries::query_curvy_note_status(note_id))?
            .await?;
        response_to_data(response)?.curvy_note_status.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_valid_notes_root(&self, root: String) -> Result<bool> {
        let response = self
            .build_query(GraphQlQueries::query_curvy_valid_notes_root(root))?
            .await?;
        let value: Result<CurvyBooleanValue> = response_to_data(response)?.curvy_valid_notes_root.into();
        Ok(value?.value)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_nullifier_spent(&self, nullifier: String) -> Result<bool> {
        let response = self
            .build_query(GraphQlQueries::query_curvy_nullifier_spent(nullifier))?
            .await?;
        let value: Result<CurvyBooleanValue> = response_to_data(response)?.curvy_nullifier_spent.into();
        Ok(value?.value)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_vault_fees(&self) -> Result<CurvyVaultFees> {
        let response = self.build_query(GraphQlQueries::query_curvy_vault_fees())?.await?;
        response_to_data(response)?.curvy_vault_fees.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_aggregator_fees(&self) -> Result<CurvyAggregatorFees> {
        let response = self.build_query(GraphQlQueries::query_curvy_aggregator_fees())?.await?;
        response_to_data(response)?.curvy_aggregator_fees.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_vault_token_count(&self) -> Result<CurvyVaultTokenCount> {
        let response = self
            .build_query(GraphQlQueries::query_curvy_vault_token_count())?
            .await?;
        response_to_data(response)?.curvy_vault_token_count.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_vault_token(&self, token_id: String) -> Result<CurvyVaultToken> {
        let response = self
            .build_query(GraphQlQueries::query_curvy_vault_token(token_id))?
            .await?;
        response_to_data(response)?.curvy_vault_token.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_entry_portal_address(&self, owner_hash: String, recovery: String) -> Result<String> {
        let response = self
            .build_query(GraphQlQueries::query_curvy_entry_portal_address(owner_hash, recovery))?
            .await?;
        let value: Result<CurvyAddress> = response_to_data(response)?.curvy_entry_portal_address.into();
        Ok(value?.address)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_exit_portal_address(
        &self,
        exit_address: String,
        exit_chain_id: String,
        recovery: String,
    ) -> Result<String> {
        let response = self
            .build_query(GraphQlQueries::query_curvy_exit_portal_address(
                exit_address,
                exit_chain_id,
                recovery,
            ))?
            .await?;
        let value: Result<CurvyAddress> = response_to_data(response)?.curvy_exit_portal_address.into();
        Ok(value?.address)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_curvy_portal_registered(&self, portal_address: String) -> Result<bool> {
        let response = self
            .build_query(GraphQlQueries::query_curvy_portal_registered(portal_address))?
            .await?;
        let value: Result<CurvyBooleanValue> = response_to_data(response)?.curvy_portal_registered.into();
        Ok(value?.value)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    async fn count_accounts(&self, selector: AccountSelector) -> Result<u32> {
        let resp = self.build_query(GraphQlQueries::count_accounts(selector))?.await?;

        response_to_data(resp)?.account_count.into()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    async fn query_accounts(&self, selector: AccountSelector) -> Result<Vec<Account>> {
        if matches!(selector, AccountSelector::Any) {
            return Err(ErrorKind::InvalidInput("filter must be specified on account query").into());
        }

        let resp = self.build_query(GraphQlQueries::query_accounts(selector))?.await?;

        response_to_data(resp)?.accounts.into()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(address = hex::encode(address)))]
    async fn query_native_balance(&self, address: &ChainAddress) -> Result<NativeBalance> {
        let resp = self.build_query(GraphQlQueries::query_native_balance(address))?.await?;

        response_to_data(resp)?.native_balance.into()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(address = hex::encode(address)))]
    async fn query_token_balance(&self, address: &ChainAddress, token: Token) -> Result<HoprBalance> {
        let resp = self
            .build_query(GraphQlQueries::query_token_balance(address, token))?
            .await?;

        response_to_data(resp)?.hopr_balance.into()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(address = hex::encode(address)))]
    async fn query_transaction_count(&self, address: &ChainAddress) -> Result<u64> {
        let resp = self
            .build_query(GraphQlQueries::query_transaction_count(address))?
            .await?;

        response_to_data(resp)?.transaction_count.into()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(address = hex::encode(address)))]
    async fn query_safe_allowance(&self, address: &ChainAddress) -> Result<SafeHoprAllowance> {
        let resp = self.build_query(GraphQlQueries::query_safe_allowance(address))?.await?;

        response_to_data(resp)?.safe_hopr_allowance.into()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    async fn query_redeemed_stats(&self, selector: RedeemedStatsSelector) -> Result<RedeemedStats> {
        let resp = self
            .build_query(GraphQlQueries::query_redeemed_stats(selector))?
            .await?;

        response_to_data(resp)?.ticket_redemption_stats.into()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    async fn query_safe(&self, selector: SafeSelector) -> Result<Vec<Safe>> {
        let (gql_selector, addr) = match selector {
            SafeSelector::SafeAddress(addr) => (SafeSelectorInput::Address, addr),
            SafeSelector::Owner(addr) => (SafeSelectorInput::Owner, addr),
            SafeSelector::ChainKey(addr) => (SafeSelectorInput::ChainKey, addr),
            SafeSelector::RegisteredNode(addr) => (SafeSelectorInput::RegisteredNode, addr),
        };

        let res = self
            .build_query(GraphQlQueries::query_safe_by(gql_selector, &addr))?
            .await?;

        match response_to_data(res)?.safe_by {
            Some(result) => result.into(),
            None => Ok(Vec::new()),
        }
    }

    async fn query_module_address_prediction(&self, input: ModulePredictionInput) -> Result<ChainAddress> {
        let resp = self
            .build_query(GraphQlQueries::query_module_address_prediction(input))?
            .await?;

        response_to_data(resp)?.calculate_module_address.into()
    }

    #[allow(deprecated)]
    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    async fn count_channels(&self, selector: ChannelSelector) -> Result<u32> {
        if selector.safe_address.is_some() {
            let channels = self.query_channels(selector).await?;
            return u32::try_from(channels.channels.len()).map_err(|_| ErrorKind::ParseError.into());
        }

        let resp = self.build_query(GraphQlQueries::query_channel_count(selector))?.await?;

        response_to_data(resp)?.channel_count.into()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    async fn query_channel_stats(&self, selector: ChannelSelector) -> Result<ChannelStats> {
        let resp = self.build_query(GraphQlQueries::query_channel_stats(selector))?.await?;

        response_to_data(resp)?.channel_stats.into()
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?selector))]
    async fn query_channels(&self, selector: ChannelSelector) -> Result<ChannelsList> {
        if selector.filter.is_none() && selector.safe_address.is_none() {
            return Err(ErrorKind::InvalidInput("at least one filter must be specified on channel query").into());
        }

        let safe_address = selector.safe_address;
        let resp = self.build_query(GraphQlQueries::query_channels(selector))?.await?;
        let channels_result = response_to_data(resp)?.channels;
        let channels: ChannelsList = {
            let parsed_channels: Result<ChannelsList> = channels_result.into();
            parsed_channels?
        };

        if let Some(safe_address) = safe_address {
            return self.filter_channels_by_safe(channels, safe_address).await;
        }

        Ok(channels)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_transaction_status(&self, tx_id: TxId) -> Result<Transaction> {
        let resp = self.build_query(GraphQlQueries::query_transaction(tx_id))?.await?;

        response_to_data(resp)?
            .transaction
            .ok_or::<BlokliClientError>(ErrorKind::NoData.into())?
            .into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_chain_info(&self) -> Result<ChainInfo> {
        let resp = self.build_query(GraphQlQueries::query_chain_info())?.await?;

        response_to_data(resp)?.chain_info.into()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_version(&self) -> Result<String> {
        let resp = self.build_query(GraphQlQueries::query_version())?.await?;

        response_to_data(resp).map(|data| data.version)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_health(&self) -> Result<String> {
        let resp = self.build_query(GraphQlQueries::query_health())?.await?;

        response_to_data(resp).map(|data| data.health)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn query_compatibility(&self) -> Result<Compatibility> {
        let resp = self.build_query(GraphQlQueries::query_compatibility())?.await?;

        response_to_data(resp).map(|data| data.compatibility)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(?owner_address))]
    async fn query_safes_balance(&self, owner_address: Option<ChainAddress>) -> Result<SafesBalance> {
        let resp = self
            .build_query(GraphQlQueries::query_safes_balance(owner_address))?
            .await?;

        response_to_data(resp)?.safes_balance.into()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::GraphQlQueries;
    use crate::api::types::CurvyEventCursor;

    #[test]
    fn curvy_pending_query_serializes_structured_exclusive_cursor() {
        let operation =
            GraphQlQueries::query_curvy_pending_notes(Some(10), Some(CurvyEventCursor::new(11, 2, 3, 4)), 1000)
                .expect("valid Curvy page");
        let serialized = serde_json::to_value(operation).expect("operation should serialize");

        assert_eq!(
            serialized["variables"],
            json!({
                "fromBlock": "10",
                "after": {
                    "block": "11",
                    "transactionIndex": "2",
                    "logIndex": "3",
                    "eventItemIndex": "4",
                    "blockHash": null,
                },
                "first": 1000,
            })
        );
    }

    #[test]
    fn curvy_queries_reject_invalid_page_sizes() {
        assert!(GraphQlQueries::query_curvy_pending_notes(None, None, 0).is_err());
        assert!(GraphQlQueries::query_curvy_pending_notes(None, None, 1001).is_err());
    }
}
