use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use hopr_bindings::exports::alloy::primitives::{Address as AlloyAddress, U256};
use hopr_types::primitive::prelude::Address;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub struct RpcClient {
    http: Client,
    url: String,
}

impl RpcClient {
    pub fn new(url: &str, timeout: Duration) -> Result<Self> {
        let http = Client::builder()
            .timeout(timeout)
            .build()
            .context("Failed to build RPC client")?;
        Ok(Self {
            http,
            url: url.to_string(),
        })
    }

    pub async fn block_number(&self) -> Result<u64> {
        let value = self
            .call_raw("eth_blockNumber", Vec::new())
            .await?
            .context("eth_blockNumber returned no result")?;
        parse_hex_quantity(value.as_str().context("eth_blockNumber returned non-string result")?)
    }

    pub async fn chain_id(&self) -> Result<u64> {
        let value = self
            .call_raw("eth_chainId", Vec::new())
            .await?
            .context("eth_chainId returned no result")?;
        parse_hex_quantity(value.as_str().context("eth_chainId returned non-string result")?)
    }

    pub async fn transaction_count(&self, address: &Address) -> Result<u64> {
        let value = self
            .call_raw(
                "eth_getTransactionCount",
                vec![json!(address.to_string()), json!("latest")],
            )
            .await?
            .context("eth_getTransactionCount returned no result")?;
        parse_hex_quantity(
            value
                .as_str()
                .context("eth_getTransactionCount returned non-string result")?,
        )
    }

    pub async fn get_balance(&self, address: &Address) -> Result<U256> {
        let value = self
            .call_raw("eth_getBalance", vec![json!(address.to_string()), json!("latest")])
            .await?
            .context("eth_getBalance returned no result")?;
        parse_u256(value.as_str().context("eth_getBalance returned non-string result")?)
    }

    /// Replaces an account's bytecode through Anvil's test-only RPC API.
    pub async fn set_anvil_code(&self, address: &AlloyAddress, code: &[u8]) -> Result<()> {
        self.call_raw(
            "anvil_setCode",
            vec![json!(address.to_string()), json!(format!("0x{}", hex::encode(code)))],
        )
        .await?;
        Ok(())
    }

    /// Performs an `eth_call` against `to` with the given calldata and returns the raw return data.
    pub async fn call(&self, to: &str, calldata: &[u8]) -> Result<Vec<u8>> {
        let value = self
            .call_raw(
                "eth_call",
                vec![
                    json!({"to": to, "input": format!("0x{}", hex::encode(calldata))}),
                    json!("latest"),
                ],
            )
            .await?
            .context("eth_call returned no result")?;

        let encoded = value.as_str().context("eth_call returned non-string result")?;
        hex::decode(encoded.trim_start_matches("0x")).context("failed to decode eth_call return data")
    }

    /// Makes Anvil accept unsigned transactions sent from `address`.
    ///
    /// This is the cheapest way for a test to act as a contract, such as a Safe, that has no
    /// private key.
    pub async fn impersonate_account(&self, address: &str) -> Result<()> {
        self.call_raw("anvil_impersonateAccount", vec![json!(address)]).await?;
        Ok(())
    }

    /// Reverts [`impersonate_account`](RpcClient::impersonate_account) for `address`.
    pub async fn stop_impersonating_account(&self, address: &str) -> Result<()> {
        self.call_raw("anvil_stopImpersonatingAccount", vec![json!(address)])
            .await?;
        Ok(())
    }

    /// Sets the native balance of `address`, so an impersonated account can pay for gas.
    pub async fn set_balance(&self, address: &str, balance: U256) -> Result<()> {
        self.call_raw("anvil_setBalance", vec![json!(address), json!(balance.to_string())])
            .await?;
        Ok(())
    }

    /// Sends an unsigned transaction from `from`, which Anvil must be impersonating.
    pub async fn send_transaction_from(&self, from: &str, to: &str, calldata: &[u8]) -> Result<[u8; 32]> {
        let value = self
            .call_raw(
                "eth_sendTransaction",
                vec![json!({
                    "from": from,
                    "to": to,
                    "input": format!("0x{}", hex::encode(calldata)),
                })],
            )
            .await?
            .context("eth_sendTransaction returned no result")?;

        let tx_hash_str = value
            .as_str()
            .context("eth_sendTransaction returned non-string result")?;
        let tx_hash_bytes =
            hex::decode(tx_hash_str.trim_start_matches("0x")).context("failed to decode transaction hash from hex")?;

        tx_hash_bytes
            .try_into()
            .map_err(|_| anyhow!("eth_sendTransaction returned a transaction hash that is not 32 bytes"))
    }

    async fn call_raw(&self, method: &str, params: Vec<Value>) -> Result<Option<Value>> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            method,
            params,
            id: 1,
        };

        let response = self
            .http
            .post(&self.url)
            .json(&request)
            .send()
            .await
            .context("Failed to call JSON-RPC")?;

        let payload: JsonRpcResponse = response
            .error_for_status()
            .context("JSON-RPC request returned error status")?
            .json()
            .await
            .context("Failed to parse JSON-RPC response")?;

        if let Some(error) = payload.error {
            return Err(anyhow!(
                "JSON-RPC call {} failed (code {}): {}",
                method,
                error.code,
                error.message
            ));
        }

        Ok(payload.result)
    }

    pub async fn execute_transaction(&self, raw_tx: &str) -> Result<[u8; 32]> {
        let value = self
            .call_raw("eth_sendRawTransaction", vec![json!(raw_tx)])
            .await?
            .context("eth_sendRawTransaction returned no result")?;

        let tx_hash_str = value
            .as_str()
            .context("eth_sendRawTransaction returned non-string result")?;

        let tx_hash_bytes =
            hex::decode(tx_hash_str.trim_start_matches("0x")).context("Failed to decode transaction hash from hex")?;

        let mut tx_hash = [0u8; 32];
        tx_hash.copy_from_slice(&tx_hash_bytes);
        Ok(tx_hash)
    }
}

#[derive(Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'static str,
    method: &'a str,
    params: Vec<Value>,
    id: u64,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    result: Option<Value>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

fn parse_hex_quantity(value: &str) -> Result<u64> {
    let trimmed = value.trim_start_matches("0x");
    if trimmed.is_empty() {
        return Ok(0);
    }
    u64::from_str_radix(trimmed, 16).map_err(|e| anyhow!("Failed to parse hex quantity {value}: {e}"))
}

fn parse_u256(value: &str) -> Result<U256> {
    let trimmed = value.trim_start_matches("0x");
    if trimmed.is_empty() {
        return Ok(U256::ZERO);
    }

    let padded = if trimmed.len().is_multiple_of(2) {
        trimmed.to_string()
    } else {
        format!("0{trimmed}")
    };
    let bytes = hex::decode(padded)?;
    Ok(U256::from_be_slice(&bytes))
}
