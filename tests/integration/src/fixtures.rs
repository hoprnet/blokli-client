use std::{
    future::Future,
    str::FromStr,
    sync::{Arc, Mutex, Once},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use blokli_client::{
    BlokliClient, BlokliClientConfig,
    api::{
        AccountSelector, BlokliQueryClient, BlokliSubscriptionClient, BlokliTransactionClient, SafeSelector,
        types::{ReadinessState, Safe},
    },
};
use curvy_bindings::{
    curvy_aggregator_alpha_v2::CurvyAggregatorAlphaV2::CurvyAggregatorAlphaV2Instance,
    portal_factory::{CurvyTypes, PortalFactory::PortalFactoryInstance},
};
use futures::StreamExt;
use futures_time::future::FutureExt as FutureTimeoutExt;
use hopli_lib::methods::transfer_or_mint_tokens;
use hopr_bindings::{
    erc677_mock::ERC677Mock::transferCall,
    exports::alloy::{
        primitives::{Address, U256, keccak256},
        providers::{
            Provider, ProviderBuilder,
            fillers::{BlobGasFiller, CachedNonceManager, ChainIdFiller, GasFiller, NonceFiller},
        },
        rpc::types::TransactionRequest,
        signers::local::PrivateKeySigner,
        sol_types::SolCall,
    },
    hopr_token::HoprToken::HoprTokenInstance,
};
use hopr_types::{
    chain::{
        ContractAddresses,
        payload::{BasicPayloadGenerator, PayloadGenerator, SafePayloadGenerator},
        prelude::SignableTransaction,
    },
    crypto::{
        keypairs::{ChainKeypair, Keypair},
        types::{HalfKey, Hash, Response},
    },
    internal::{Multiaddr, announcement::AnnouncementData, tickets::TicketBuilder},
    primitive::{
        prelude::{Address as HoprAddress, HoprBalance, XHoprBalance},
        traits::IntoEndian,
    },
};
use libc::atexit;
use rand::seq::IndexedRandom;
use rstest::fixture;
use tokio::sync::OnceCell;
use tracing::{debug, info, warn};

use crate::{
    anvil::AnvilAccount,
    config::TestConfig,
    constants::{STACK_STARTUP_WAIT, subscription_timeout},
    docker::DockerEnvironment,
    rpc::RpcClient,
    transaction::TransactionBuilder as TestTransactionBuilder,
};

#[derive(Clone)]
pub struct IntegrationFixture {
    inner: Arc<IntegrationFixtureInner>,
}

struct IntegrationFixtureInner {
    config: Arc<TestConfig>,
    accounts: Vec<AnvilAccount>,
    client: BlokliClient,
    rpc: RpcClient,
    docker: Mutex<Option<DockerEnvironment>>,
    contract_addrs: ContractAddresses,
}

const DEFAULT_MAX_FEE_PER_GAS: u128 = 2_000_000_000;
const DEFAULT_MAX_PRIORITY_FEE_PER_GAS: u128 = 1_000_000_000;
const DEFAULT_GAS_LIMIT: u64 = 10_000_000;
const READY_POLL_INTERVAL: Duration = Duration::from_millis(250);
const CURVY_COMMITMENT_BATCH_SIZE: usize = 5;
/// Curvy's local development configuration registers native ETH as token ID 1.
const CURVY_TEST_NATIVE_TOKEN_ID: u64 = 1;
const CURVY_TEST_DEPOSIT_AMOUNT: u128 = 1_000_000_000_000_000_000;
/// Minimal EVM runtime returning ABI-encoded `true` for any verifier call.
const ACCEPT_ALL_VERIFIER_CODE: [u8; 10] = [0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3];

/// Test-only direct Curvy access replacing connector transaction construction and external operator actions.
pub struct CurvyTestChain<P> {
    account: Address,
    aggregator: CurvyAggregatorAlphaV2Instance<P>,
    portal_factory: PortalFactoryInstance<P>,
    provider: P,
}

impl<P> CurvyTestChain<P>
where
    P: Provider + Clone,
{
    pub const fn commitment_batch_size(&self) -> usize {
        CURVY_COMMITMENT_BATCH_SIZE
    }

    /// Drives deposits directly on Anvil.
    ///
    /// In production, PIX supplies the deposit intent, `hopr-chain-connector` constructs and signs the portal
    /// transaction, and `blokli-client` submits the signed bytes. None of those components calls this helper.
    pub async fn submit_deposit_batch(&self) -> Result<()> {
        let token = U256::from(CURVY_TEST_NATIVE_TOKEN_ID);
        let amount = U256::from(CURVY_TEST_DEPOSIT_AMOUNT);

        for index in 0..CURVY_COMMITMENT_BATCH_SIZE {
            let index = U256::from(index);
            let note = CurvyTypes::Note {
                ownerHash: U256::from(101) + index,
                token,
                amount,
                ephemeralKey: [U256::from(201) + index, U256::from(202) + index],
                viewTag: 7,
            };
            let portal_address = self
                .portal_factory
                .getEntryPortalAddress(note.ownerHash, self.account)
                .call()
                .await?;

            self.provider
                .send_transaction(TransactionRequest::default().to(portal_address).value(amount))
                .await?
                .watch()
                .timeout(subscription_timeout())
                .await
                .context("timed out waiting for Curvy portal funding transaction")??;
            self.portal_factory
                .deployShieldPortal(note, self.account)
                .send()
                .await?
                .watch()
                .timeout(subscription_timeout())
                .await
                .context("timed out waiting for Curvy shield portal deployment")??;
        }

        Ok(())
    }

    /// Simulates the external Curvy operator committing a batch.
    ///
    /// This is not performed by the PIX strategy, `hopr-chain-connector`, or `blokli-client` in production. The
    /// dedicated deposit-test stack is discarded after this test binary, so the test-only verifier code is not
    /// restored.
    pub async fn commit_deposit_batch(&self, rpc: &RpcClient, note_ids: Vec<U256>) -> Result<()> {
        ensure!(
            note_ids.len() == CURVY_COMMITMENT_BATCH_SIZE,
            "commitPendingNotes requires exactly {CURVY_COMMITMENT_BATCH_SIZE} note IDs, received {}",
            note_ids.len()
        );
        let batch_size = U256::from(CURVY_COMMITMENT_BATCH_SIZE);
        let verifier = self
            .aggregator
            .getPendingNotesCommitmentVerifier(batch_size)
            .call()
            .await?;
        rpc.set_anvil_code(&verifier, &ACCEPT_ALL_VERIFIER_CODE).await?;

        self.aggregator
            .commitPendingNotes(
                batch_size,
                note_ids,
                U256::from(301),
                [U256::ZERO; 2],
                [[U256::ZERO; 2]; 2],
                [U256::ZERO; 2],
            )
            .send()
            .await?
            .watch()
            .timeout(subscription_timeout())
            .await
            .context("timed out waiting for Curvy deposit commitment")??;

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Eip1559GasParameters {
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
    pub gas_limit: u64,
}

impl Default for Eip1559GasParameters {
    fn default() -> Self {
        Self {
            max_fee_per_gas: DEFAULT_MAX_FEE_PER_GAS,
            max_priority_fee_per_gas: DEFAULT_MAX_PRIORITY_FEE_PER_GAS,
            gas_limit: DEFAULT_GAS_LIMIT,
        }
    }
}

// Accessor methods
impl IntegrationFixture {
    pub fn config(&self) -> &TestConfig {
        &self.inner.config
    }

    pub fn accounts(&self) -> &[AnvilAccount] {
        &self.inner.accounts
    }

    pub fn sample_accounts<const N: usize>(&self) -> [&AnvilAccount; N] {
        assert!(self.inner.accounts.len() >= N, "not enough accounts available");

        let selected = self.inner.accounts.sample(&mut rand::rng(), N);
        let mut iter = selected.into_iter();
        let result: [&AnvilAccount; N] = std::array::from_fn(|_| iter.next().unwrap());
        result
    }

    pub fn client(&self) -> &BlokliClient {
        &self.inner.client
    }

    pub fn rpc(&self) -> &RpcClient {
        &self.inner.rpc
    }

    pub fn contract_addresses(&self) -> &ContractAddresses {
        &self.inner.contract_addrs
    }

    /// Creates test-only Curvy contract access for producing indexed note events.
    pub async fn curvy_test_chain(&self) -> Result<CurvyTestChain<impl Provider + Clone>> {
        let account = &self.accounts()[0];
        let chain_info = self.client().query_chain_info().await?;
        let addresses: serde_json::Value = serde_json::from_str(&chain_info.contract_addresses.0)
            .context("failed to parse contract addresses reported by bloklid")?;
        let aggregator_address = addresses
            .get("curvy_aggregator")
            .and_then(serde_json::Value::as_str)
            .context("chainInfo does not contain curvy_aggregator")?
            .parse()
            .context("invalid Curvy aggregator address")?;
        let signer = PrivateKeySigner::from_slice(account.keypair.secret().as_ref())
            .context("failed to build a signer from the first Anvil account")?;
        let provider = ProviderBuilder::new()
            .wallet(signer)
            .connect_http(self.config().rpc_url().clone());
        let aggregator = CurvyAggregatorAlphaV2Instance::new(aggregator_address, provider.clone());
        let portal_factory_address = aggregator.portalFactory().call().await?;
        let portal_factory = PortalFactoryInstance::new(portal_factory_address, provider.clone());

        Ok(CurvyTestChain {
            account: account.to_alloy_address(),
            aggregator,
            portal_factory,
            provider,
        })
    }
}

// Transaction related helpers
impl IntegrationFixture {
    /// Builds a raw EIP-1559 transaction hex string.
    pub async fn build_raw_tx(
        &self,
        value: U256,
        sender: &AnvilAccount,
        recipient: &AnvilAccount,
        nonce: u64,
    ) -> Result<String> {
        self.build_raw_tx_with_gas(value, sender, recipient, nonce, None).await
    }

    pub async fn build_raw_tx_with_gas(
        &self,
        value: U256,
        sender: &AnvilAccount,
        recipient: &AnvilAccount,
        nonce: u64,
        gas: Option<Eip1559GasParameters>,
    ) -> Result<String> {
        let gas = self.resolve_eip1559_gas_parameters(gas).await;
        let tx_builder = TestTransactionBuilder::new(&sender.keypair)?;
        tx_builder
            .build_eip1559_transaction_hex(
                self.rpc().chain_id().await?,
                nonce,
                &recipient.address.to_string(),
                value,
                gas.max_fee_per_gas,
                gas.max_priority_fee_per_gas,
                gas.gas_limit,
            )
            .await
    }

    async fn resolve_eip1559_gas_parameters(&self, gas: Option<Eip1559GasParameters>) -> Eip1559GasParameters {
        if let Some(gas) = gas {
            return gas;
        }

        let chain_info = match self.client().query_chain_info().await {
            Ok(chain_info) => chain_info,
            Err(error) => {
                warn!(
                    %error,
                    "failed to fetch chainInfo gas parameters, using integration defaults"
                );
                return Eip1559GasParameters::default();
            }
        };

        let max_fee_per_gas = chain_info
            .max_fee_per_gas
            .as_deref()
            .and_then(|value| value.parse::<u128>().ok());
        let max_priority_fee_per_gas = chain_info
            .max_priority_fee_per_gas
            .as_deref()
            .and_then(|value| value.parse::<u128>().ok());

        match (max_fee_per_gas, max_priority_fee_per_gas) {
            (Some(max_fee_per_gas), Some(max_priority_fee_per_gas)) => Eip1559GasParameters {
                max_fee_per_gas,
                max_priority_fee_per_gas,
                ..Eip1559GasParameters::default()
            },
            _ => {
                warn!("chainInfo gas parameters unavailable, using integration defaults");
                Eip1559GasParameters::default()
            }
        }
    }

    /// Submits the signed transaction blindly.
    pub async fn submit_tx(&self, signed_bytes: &[u8]) -> Result<[u8; 32]> {
        let receipt = self
            .client()
            .submit_transaction(signed_bytes)
            .await
            .context("blokli client failed to submit transaction")?;
        Ok(receipt)
    }

    /// Submits the signed transaction and returns the tracking id.
    pub async fn submit_and_track_tx(&self, signed_bytes: &[u8]) -> Result<String> {
        let tx_id = self
            .client()
            .submit_and_track_transaction(signed_bytes)
            .await
            .context("blokli client failed to submit transaction")?;
        Ok(tx_id)
    }

    /// Submits the signed transaction and waits for the specified number of confirmations.
    pub async fn submit_and_confirm_tx(&self, signed_bytes: &[u8], confirmations: usize) -> Result<[u8; 32]> {
        let receipt = self
            .client()
            .submit_and_confirm_transaction(signed_bytes, confirmations)
            .await
            .context("blokli client failed to submit transaction")?;
        Ok(receipt)
    }
}

// Safe related helpers
impl IntegrationFixture {
    /// Deploys a safe for the specified owner.
    async fn deploy_safe(&self, owner: &AnvilAccount, amount: HoprBalance) -> Result<[u8; 32]> {
        let nonce = self.rpc().transaction_count(&owner.address).await?;

        let contract_addresses = self.contract_addresses();
        let payload = hopli_lib::payloads::edge_node_deploy_safe_module_and_maybe_include_node(
            contract_addresses.node_stake_factory,
            contract_addresses.token,
            contract_addresses.channels,
            U256::from(nonce),
            U256::from_be_bytes(amount.to_be_bytes()),
            vec![owner.to_alloy_address()],
            true,
        )?;

        let payload_bytes = payload
            .sign_and_encode_to_eip2718(nonce, self.rpc().chain_id().await?, None, &owner.keypair)
            .await?;

        self.submit_and_confirm_tx(&payload_bytes, self.config().tx_confirmations)
            .await
    }

    /// Deploys and returns a safe for the specified owner if not already deployed, otherwise retrieves the existing
    /// safe.
    pub async fn deploy_or_get_safe(&self, owner: &AnvilAccount, amount: HoprBalance) -> Result<Safe> {
        let maybe_safe = self
            .client()
            .query_safe(SafeSelector::ChainKey(owner.to_alloy_address().into()))
            .await?
            .into_iter()
            .next();

        match maybe_safe {
            Some(s) => Ok(s),
            None => {
                self.deploy_safe(owner, amount).await?;

                let selector = SafeSelector::ChainKey(owner.to_alloy_address().into());
                let client = self.client().clone();
                let safe = poll_until(
                    "safe indexing",
                    Duration::from_secs(30),
                    Duration::from_millis(500),
                    || {
                        let client = client.clone();
                        let selector = selector.clone();
                        async move { Ok(client.query_safe(selector).await?.into_iter().next()) }
                    },
                )
                .await?;

                self.register_safe(owner, &safe.address).await?;

                Ok(safe)
            }
        }
    }

    pub async fn register_safe(&self, owner: &AnvilAccount, safe_address: &str) -> Result<[u8; 32]> {
        let nonce = self.rpc().transaction_count(&owner.address).await?;

        let payload_generator = BasicPayloadGenerator::new(owner.address, *self.contract_addresses());

        let payload = payload_generator.register_safe_by_node(HoprAddress::from_str(safe_address)?)?;

        let payload_bytes = payload
            .sign_and_encode_to_eip2718(nonce, self.rpc().chain_id().await?, None, &owner.keypair)
            .await?;

        self.submit_and_confirm_tx(&payload_bytes, self.config().tx_confirmations)
            .await
    }
}

// Account related helpers
impl IntegrationFixture {
    /// Announces the account using the specified safe module.
    pub async fn announce_account(&self, account: &AnvilAccount, module: &str) -> Result<[u8; 32]> {
        let nonce = self.rpc().transaction_count(&account.address).await?;

        let payload_generator = SafePayloadGenerator::new(
            &account.keypair,
            *self.contract_addresses(),
            HoprAddress::from_str(module)?,
        );
        let multiaddress: Multiaddr = "/ip4/127.0.0.1/udp/3001".parse().expect("multiaddress parsing failed");
        let binding_fee = "0.01 wxHOPR".parse().expect("failed parsing the binding fee");

        let payload = payload_generator.announce(
            AnnouncementData::new(account.keybinding(), Some(multiaddress))?,
            binding_fee,
        )?;

        let payload_bytes = payload
            .sign_and_encode_to_eip2718(nonce, self.rpc().chain_id().await?, None, &account.keypair)
            .await?;

        self.submit_and_confirm_tx(&payload_bytes, self.config().tx_confirmations)
            .await
    }

    /// Announces the account if not announced yet. If already announced, does nothing.
    pub async fn announce_or_get_account(&self, account: &AnvilAccount, module: &str) -> Result<()> {
        let maybe_account = self
            .client()
            .query_accounts(AccountSelector::Address(account.to_alloy_address().into()))
            .await?;

        match maybe_account.first() {
            Some(_) => Ok(()),
            None => {
                debug!("account not found, proceeding to announce");
                self.announce_account(account, module).await?;

                let selector = AccountSelector::Address(account.to_alloy_address().into());
                let client = self.client().clone();
                poll_until(
                    "account indexing after announcement",
                    Duration::from_secs(30),
                    Duration::from_millis(500),
                    || {
                        let client = client.clone();
                        let selector = selector.clone();
                        async move {
                            let accounts = client.query_accounts(selector).await?;
                            if accounts.is_empty() { Ok(None) } else { Ok(Some(())) }
                        }
                    },
                )
                .await?;
                Ok(())
            }
        }
    }
}

// Ticket related helpers
impl IntegrationFixture {
    /// Updates the ticket's winning probability to `new_win_prob`.
    pub async fn update_winning_probability(
        &self,
        owner: &AnvilAccount,
        contract_address: Address,
        new_win_prob: f64,
    ) -> Result<[u8; 32]> {
        let nonce = self.rpc().transaction_count(&owner.address).await?;

        let payload = hopli_lib::payloads::set_winning_probability(contract_address, new_win_prob)?;

        let payload_bytes = payload
            .sign_and_encode_to_eip2718(nonce, self.rpc().chain_id().await?, None, &owner.keypair)
            .await?;

        self.submit_and_confirm_tx(&payload_bytes, self.config().tx_confirmations)
            .await
    }

    /// Generates a redeemable ticket and submits the redemption transaction.
    pub async fn redeem_ticket(
        &self,
        issuer: &AnvilAccount,
        redeemer: &AnvilAccount,
        amount: HoprBalance,
        module: &str,
        ticket_index: u64,
        channel_epoch: u32,
    ) -> Result<[u8; 32]> {
        let nonce = self.rpc().transaction_count(&redeemer.address).await?;

        let domain_separator = self
            .client()
            .query_chain_info()
            .await?
            .channel_dst
            .context("missing channel domain separator in chain info")?;
        let domain_separator =
            Hash::from_str(&domain_separator).context("failed to parse channel domain separator from chain info")?;

        let issuer_half_key =
            HalfKey::try_from(issuer.keypair.secret().as_ref()).context("failed to derive issuer half key")?;
        let redeemer_half_key =
            HalfKey::try_from(redeemer.keypair.secret().as_ref()).context("failed to derive redeemer half key")?;
        let response = Response::from_half_keys(&issuer_half_key, &redeemer_half_key)?;

        let ticket = TicketBuilder::default()
            .counterparty(redeemer.address)
            .balance(amount)
            .index(ticket_index)
            .channel_epoch(channel_epoch)
            .challenge(response.to_challenge()?)
            .build_signed(&issuer.keypair, &domain_separator)?
            .into_acknowledged(response)
            .into_redeemable(&redeemer.keypair, &domain_separator)?;

        let payload_generator = SafePayloadGenerator::new(
            &redeemer.keypair,
            *self.contract_addresses(),
            HoprAddress::from_str(module)?,
        );

        let payload = payload_generator.redeem_ticket(ticket)?;

        let payload_bytes = payload
            .sign_and_encode_to_eip2718(nonce, self.rpc().chain_id().await?, None, &redeemer.keypair)
            .await?;

        self.submit_and_confirm_tx(&payload_bytes, self.config().tx_confirmations)
            .await
    }
}

// Token related helpers
impl IntegrationFixture {
    /// Approves the safe module to spend `amount` of wxHOPR on behalf of `owner`.
    pub async fn approve(&self, owner: &AnvilAccount, amount: HoprBalance, module: &str) -> Result<[u8; 32]> {
        let nonce = self.rpc().transaction_count(&owner.address).await?;
        let spender = HoprAddress::from_str(&self.contract_addresses().channels.to_string())
            .expect("Invalid spender address hex");

        let payload_generator = SafePayloadGenerator::new(
            &owner.keypair,
            *self.contract_addresses(),
            HoprAddress::from_str(module)?,
        );

        let payload = payload_generator.approve(spender, amount)?;

        let payload_bytes = payload
            .sign_and_encode_to_eip2718(nonce, self.rpc().chain_id().await?, None, &owner.keypair)
            .await?;

        self.submit_and_confirm_tx(&payload_bytes, self.config().tx_confirmations)
            .await
    }

    /// Transfers `amount` of xHOPR (the `ERC677Mock` token) from `from` to `to` by submitting a
    /// signed `transfer(to, amount)` call to the xHOPR contract through blokli.
    pub async fn transfer_xhopr(
        &self,
        from: &AnvilAccount,
        to: &AnvilAccount,
        amount: XHoprBalance,
    ) -> Result<[u8; 32]> {
        let nonce = self.rpc().transaction_count(&from.address).await?;
        let gas = self.resolve_eip1559_gas_parameters(None).await;

        let calldata = transferCall {
            _to: to.to_alloy_address(),
            _value: U256::from_be_bytes(amount.to_be_bytes()),
        }
        .abi_encode();

        let tx_builder = TestTransactionBuilder::new(&from.keypair)?;
        let signed_hex = tx_builder
            .build_eip1559_call_hex(
                self.rpc().chain_id().await?,
                nonce,
                self.contract_addresses().xhopr_token,
                U256::ZERO,
                calldata,
                gas.max_fee_per_gas,
                gas.max_priority_fee_per_gas,
                gas.gas_limit,
            )
            .await?;

        let signed_bytes =
            hex::decode(signed_hex.trim_start_matches("0x")).context("failed to decode xHOPR transfer tx")?;

        self.submit_and_confirm_tx(&signed_bytes, self.config().tx_confirmations)
            .await
    }
}

impl IntegrationFixture {
    pub async fn deploy_safe_and_announce(&self, owner: &AnvilAccount, amount: HoprBalance) -> Result<Safe> {
        let safe = self.deploy_or_get_safe(owner, amount).await?;
        self.announce_or_get_account(owner, &safe.module_address).await?;
        Ok(safe)
    }
}

// Channel related helpers
impl IntegrationFixture {
    /// Opens a channel from `from` to `to` with the specified `amount`.
    pub async fn open_channel(
        &self,
        from: &AnvilAccount,
        to: &AnvilAccount,
        amount: HoprBalance,
        module: &str,
        nonce: Option<u64>,
    ) -> Result<[u8; 32]> {
        let nonce = self
            .rpc()
            .transaction_count(&from.address)
            .await?
            .max(nonce.unwrap_or(0));

        let payload_generator = SafePayloadGenerator::new(
            &from.keypair,
            *self.contract_addresses(),
            HoprAddress::from_str(module)?,
        );

        let payload = payload_generator.fund_channel(to.address, amount)?;

        let payload_bytes = payload
            .sign_and_encode_to_eip2718(nonce, self.rpc().chain_id().await?, None, &from.keypair)
            .await?;

        self.submit_and_confirm_tx(&payload_bytes, self.config().tx_confirmations)
            .await
    }

    /// Opens a channel using fire-and-forget submission (no confirmation wait).
    ///
    /// Returns the transaction hash immediately after submission.
    /// The caller is responsible for polling the indexer to confirm the channel was created.
    pub async fn open_channel_fire_and_forget(
        &self,
        from: &AnvilAccount,
        to: &AnvilAccount,
        amount: HoprBalance,
        module: &str,
        nonce: Option<u64>,
    ) -> Result<[u8; 32]> {
        let nonce = self
            .rpc()
            .transaction_count(&from.address)
            .await?
            .max(nonce.unwrap_or(0));

        let payload_generator = SafePayloadGenerator::new(
            &from.keypair,
            *self.contract_addresses(),
            HoprAddress::from_str(module)?,
        );

        let payload = payload_generator.fund_channel(to.address, amount)?;

        let payload_bytes = payload
            .sign_and_encode_to_eip2718(nonce, self.rpc().chain_id().await?, None, &from.keypair)
            .await?;

        self.submit_tx(&payload_bytes).await
    }

    /// Starts closing an outgoing channel from `from` to `to`.
    pub async fn initiate_outgoing_channel_closure(
        &self,
        from: &AnvilAccount,
        to: &AnvilAccount,
        module: &str,
    ) -> Result<[u8; 32]> {
        let nonce = self.rpc().transaction_count(&from.address).await?;

        let payload_generator = SafePayloadGenerator::new(
            &from.keypair,
            *self.contract_addresses(),
            HoprAddress::from_str(module)?,
        );

        let payload = payload_generator.initiate_outgoing_channel_closure(to.address)?;

        let payload_bytes = payload
            .sign_and_encode_to_eip2718(nonce, self.rpc().chain_id().await?, None, &from.keypair)
            .await?;

        self.submit_and_confirm_tx(&payload_bytes, self.config().tx_confirmations)
            .await
    }

    /// Finalizes closure of an outgoing channel from `from` to `to`.
    /// Must be called after `initiate_outgoing_channel_closure` and after the notice period has elapsed.
    pub async fn finalize_outgoing_channel_closure(
        &self,
        from: &AnvilAccount,
        to: &AnvilAccount,
        module: &str,
    ) -> Result<[u8; 32]> {
        let nonce = self.rpc().transaction_count(&from.address).await?;

        let payload_generator = SafePayloadGenerator::new(
            &from.keypair,
            *self.contract_addresses(),
            HoprAddress::from_str(module)?,
        );

        let payload = payload_generator.finalize_outgoing_channel_closure(to.address)?;

        let payload_bytes = payload
            .sign_and_encode_to_eip2718(nonce, self.rpc().chain_id().await?, None, &from.keypair)
            .await?;

        self.submit_and_confirm_tx(&payload_bytes, self.config().tx_confirmations)
            .await
    }

    /// Fully closes an outgoing channel from `from` to `to` (initiates then finalizes).
    /// Notice period in test contracts is 1 second; the confirmation wait between the two
    /// transactions is enough for it to elapse.
    pub async fn close_outgoing_channel(&self, from: &AnvilAccount, to: &AnvilAccount, module: &str) -> Result<()> {
        self.initiate_outgoing_channel_closure(from, to, module).await?;
        self.finalize_outgoing_channel_closure(from, to, module).await?;
        Ok(())
    }

    fn teardown(&self) {
        self.inner.teardown();
    }
}

impl IntegrationFixtureInner {
    fn teardown(&self) {
        let mut docker_guard = self
            .docker
            .lock()
            .expect("integration docker environment mutex poisoned");

        if docker_guard.is_some() {
            info!("tearing down docker stack for integration tests");
        }

        docker_guard.take();
    }
}

async fn wait_for_blokli_ready(client: &BlokliClient) -> Result<()> {
    let mut health_stream = client
        .subscribe_health()
        .context("failed to subscribe to blokli health readiness stream")?;
    let deadline = Instant::now() + STACK_STARTUP_WAIT;
    let mut last_observation = "no readiness event received".to_string();

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(anyhow::anyhow!(
                "timed out waiting for blokli readiness via health subscription after {}s (last observation: {})",
                STACK_STARTUP_WAIT.as_secs(),
                last_observation
            ));
        }

        match tokio::time::timeout(remaining, health_stream.next()).await {
            Ok(Some(Ok(ReadinessState::Ready))) => {
                info!(base_url = %client.base_url(), "integration stack is ready");
                return Ok(());
            }
            Ok(Some(Ok(ReadinessState::NotReady))) => {
                last_observation = "received NOT_READY".to_string();
            }
            Ok(Some(Err(error))) => {
                last_observation = format!("subscription error: {error}");
            }
            Ok(None) => {
                return Err(anyhow::anyhow!(
                    "blokli health subscription closed before READY (last observation: {})",
                    last_observation
                ));
            }
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "timed out waiting for blokli readiness via health subscription after {}s (last observation: {})",
                    STACK_STARTUP_WAIT.as_secs(),
                    last_observation
                ));
            }
        }
    }
}

async fn wait_for_blokli_indexed_block(client: &BlokliClient, rpc: &RpcClient) -> Result<()> {
    let target_block = rpc
        .block_number()
        .await
        .context("failed to query RPC block number after integration bootstrap")?;
    let deadline = Instant::now() + STACK_STARTUP_WAIT;

    info!(
        target_block,
        seconds = STACK_STARTUP_WAIT.as_secs(),
        "waiting for blokli to index bootstrap transactions"
    );

    loop {
        let last_observation = match client.query_chain_info().await {
            Ok(chain_info) => {
                let indexed_block = u64::try_from(chain_info.block_number).map_err(|_| {
                    anyhow::anyhow!(
                        "blokli reported a negative indexed block during bootstrap: {}",
                        chain_info.block_number
                    )
                })?;

                if indexed_block >= target_block {
                    info!(indexed_block, target_block, "bootstrap transactions indexed by blokli");
                    return Ok(());
                }

                format!("indexed block {} < target block {}", indexed_block, target_block)
            }
            Err(error) => error.to_string(),
        };

        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "timed out waiting for blokli to index bootstrap transactions after {}s (last observation: {})",
                STACK_STARTUP_WAIT.as_secs(),
                last_observation
            ));
        }

        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
}

/// Polls a check function until it returns `Some(T)`, with timeout.
pub async fn poll_until<F, Fut, T>(description: &str, timeout: Duration, interval: Duration, mut check: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let start = Instant::now();
    let mut last_error: Option<anyhow::Error> = None;
    loop {
        match check().await {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => {}
            Err(e) => last_error = Some(e),
        }
        if start.elapsed() > timeout {
            if let Some(e) = last_error {
                return Err(e.context(format!("{description} did not complete within {timeout:?}")));
            }
            anyhow::bail!("{description} did not complete within {timeout:?}");
        }
        tokio::time::sleep(interval).await;
    }
}

pub async fn build_integration_fixture() -> Result<IntegrationFixture> {
    let config: Arc<TestConfig> = Arc::new(TestConfig::load()?);
    let mut docker = DockerEnvironment::new(config.clone());

    if !config.external_stack {
        docker.ensure_image_available()?;
        docker.compose_up()?;
    }
    let client = BlokliClient::new(config.bloklid_url().clone(), BlokliClientConfig::default());
    info!(
        seconds = STACK_STARTUP_WAIT.as_secs(),
        base_url = %client.base_url(),
        "waiting for integration stack readiness"
    );
    wait_for_blokli_ready(&client).await?;
    let accounts = docker.fetch_anvil_accounts()?;

    let rpc = RpcClient::new(config.rpc_url().as_str(), config.http_timeout)?;

    let deployer: ChainKeypair = accounts[0].keypair.clone();
    let deployer_address = accounts[0].to_alloy_address();
    let wallet = PrivateKeySigner::from_slice(deployer.secret().as_ref()).expect("failed to construct wallet");

    // Build default JSON RPC provider
    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .filler(ChainIdFiller::default())
        .filler(NonceFiller::new(CachedNonceManager::default()))
        .filler(GasFiller::default())
        .filler(BlobGasFiller::default())
        .wallet(wallet)
        .connect_http(config.rpc_url().clone());

    let chain_info = client.query_chain_info().await?;
    let contract_addresses: ContractAddresses = serde_json::from_str(&chain_info.contract_addresses.0)
        .context("failed to parse contract addresses reported by bloklid")?;

    info!(contract_addresses = ?contract_addresses, "using contract addresses deployed by the integration image");

    let hopr_token = HoprTokenInstance::new(contract_addresses.token, Arc::new(provider));

    let encoded_minter_role = keccak256(b"MINTER_ROLE");
    hopr_token
        .grantRole(encoded_minter_role, deployer_address)
        .send()
        .await?
        .watch()
        .timeout(subscription_timeout())
        .await
        .context("timed out granting the integration deployer HOPR token minter access")??;

    let external_addresses: Vec<Address> = accounts.iter().skip(1).map(AnvilAccount::to_alloy_address).collect();
    let amounts = vec![U256::from(1_000_000_000_000_000_000_000u128); external_addresses.len()];

    let total_transferred_amount = transfer_or_mint_tokens(hopr_token, external_addresses, amounts).await?;

    info!(
        total=?total_transferred_amount,
        "minted and distributed HOPR tokens to test accounts",
    );

    let fixture = IntegrationFixture {
        inner: Arc::new(IntegrationFixtureInner {
            config,
            accounts,
            client,
            rpc,
            docker: Mutex::new(Some(docker)),
            contract_addrs: contract_addresses,
        }),
    };

    wait_for_blokli_indexed_block(fixture.client(), fixture.rpc()).await?;

    register_shutdown_hook();

    Ok(fixture)
}

static SHARED_FIXTURE: OnceCell<IntegrationFixture> = OnceCell::const_new();
static SHUTDOWN_HOOK: Once = Once::new();

extern "C" fn teardown_shared_fixture() {
    if let Some(fixture) = SHARED_FIXTURE.get() {
        fixture.teardown();
    }
}

fn register_shutdown_hook() {
    SHUTDOWN_HOOK.call_once(|| unsafe {
        let result = atexit(teardown_shared_fixture);
        if result != 0 {
            panic!("failed to register integration fixture teardown hook");
        }
    });
}

#[fixture]
pub async fn integration_fixture() -> IntegrationFixture {
    SHARED_FIXTURE
        .get_or_try_init(|| async { build_integration_fixture().await })
        .await
        .expect("failed to set up integration fixture")
        .clone()
}
