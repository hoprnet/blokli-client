use std::time::Duration;

use cynic::{MutationBuilder, QueryBuilder, SubscriptionBuilder};
use futures::TryStreamExt;
use futures_time::{future::FutureExt, time::Duration as FuturesTimeDuration};
use hex::ToHex;

use super::{BlokliClient, GraphQlQueries, response_to_data};
use crate::{
    api::{
        BlokliQueryClient, BlokliTransactionClient, Result, TransactionTrackingOutcome, TxId, TxReceipt,
        internal::{
            ConfirmTransactionVariables, MutateConfirmTransaction, MutateSendTransaction, MutateTrackTransaction,
            QueryUnminedDiagnosis, SendTransactionAsyncResult, SendTransactionVariables, SubscribeTransaction,
            TransactionsVariables,
        },
        types::{TrackedSubmission, Transaction, TransactionDiagnosis, TransactionStatus},
    },
    errors::{ErrorKind, TrackingErrorKind},
};

impl GraphQlQueries {
    /// `SendTransaction` GraphQL mutation.
    pub fn mutate_submit_transaction(
        signed_tx: &[u8],
    ) -> cynic::Operation<MutateSendTransaction, SendTransactionVariables> {
        MutateSendTransaction::build(SendTransactionVariables {
            raw_transaction: signed_tx.encode_hex(),
        })
    }

    /// `TrackTransaction` GraphQL mutation.
    pub fn mutate_submit_and_track_transaction(
        signed_tx: &[u8],
    ) -> cynic::Operation<MutateTrackTransaction, SendTransactionVariables> {
        MutateTrackTransaction::build(SendTransactionVariables {
            raw_transaction: signed_tx.encode_hex(),
        })
    }

    /// `ConfirmTransaction` GraphQL mutation.
    pub fn mutate_submit_and_confirm_transaction(
        signed_tx: &[u8],
        confirmations: usize,
    ) -> cynic::Operation<MutateConfirmTransaction, ConfirmTransactionVariables> {
        MutateConfirmTransaction::build(ConfirmTransactionVariables {
            raw_transaction: signed_tx.encode_hex(),
            confirmations: confirmations.min(128) as i32,
        })
    }

    /// `TrackTransaction` GraphQL subscription.
    pub fn subscribe_track_transaction(
        tx_id: TxId,
    ) -> cynic::StreamingOperation<SubscribeTransaction, TransactionsVariables> {
        SubscribeTransaction::build(TransactionsVariables { id: tx_id.into() })
    }

    /// `UnminedDiagnosis` GraphQL query.
    ///
    /// Selects `Transaction.diagnosis`, which Blokli versions before [`MIN_DIAGNOSIS_VERSION`] reject. The client
    /// sends it only after checking the server version.
    pub fn query_unmined_diagnosis(tx_id: TxId) -> cynic::Operation<QueryUnminedDiagnosis, TransactionsVariables> {
        QueryUnminedDiagnosis::build(TransactionsVariables { id: tx_id.into() })
    }
}

/// First Blokli version that serves `Transaction.diagnosis`.
const MIN_DIAGNOSIS_VERSION: (u64, u64, u64) = (0, 15, 0);

/// Whether the Blokli `version` string denotes a release serving `Transaction.diagnosis`.
///
/// Compares `major.minor.patch` and ignores a leading `v` and any pre-release or build suffix, so `0.15.0-rc.1`
/// counts as supported. An unparseable version is unsupported.
fn version_supports_diagnosis(version: &str) -> bool {
    let core = version.trim().trim_start_matches('v');
    let core = core.split(['-', '+']).next().unwrap_or_default();
    let mut parts = core.split('.').map(|part| part.parse::<u64>());
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(Ok(major)), Some(Ok(minor)), Some(Ok(patch)), None) => (major, minor, patch) >= MIN_DIAGNOSIS_VERSION,
        _ => false,
    }
}

impl BlokliClient {
    /// Whether the server serves `Transaction.diagnosis`, judged by its version.
    ///
    /// Read once, on the first timeout, and cached for all clones of this client. A failed version query is not
    /// cached: it counts as unsupported for this call and is retried on the next timeout.
    async fn supports_unmined_diagnosis(&self) -> bool {
        let supported = self
            .unmined_diagnosis_supported
            .get_or_try_init(|| async { self.query_version().await.map(|v| version_supports_diagnosis(&v)) })
            .await;

        match supported {
            Ok(supported) => *supported,
            Err(error) => {
                tracing::warn!(%error, "could not read the blokli version, skipping the unmined diagnosis");
                false
            }
        }
    }

    /// Reads the diagnosis of a transaction that Blokli reported as timed out.
    ///
    /// Returns `None` when the server does not support diagnoses or the lookup fails, so the caller keeps the
    /// timeout it already has.
    async fn fetch_unmined_diagnosis(&self, tx_id: &TxId) -> Option<TransactionDiagnosis> {
        if !self.supports_unmined_diagnosis().await {
            return None;
        }

        let result: Result<Option<TransactionDiagnosis>> = async {
            let resp = self
                .build_query(GraphQlQueries::query_unmined_diagnosis(tx_id.clone()))?
                .await?;

            response_to_data(resp)?
                .transaction
                .ok_or::<crate::errors::BlokliClientError>(ErrorKind::NoData.into())?
                .into_diagnosis()
        }
        .await;

        result.unwrap_or_else(|error| {
            tracing::warn!(%error, tx_id, "could not read the unmined diagnosis");
            None
        })
    }

    /// Submits a signed transaction for tracking and reports whether it joined an already tracked action.
    ///
    /// Behaves like [`BlokliTransactionClient::submit_and_track_transaction`], but a deduplicated submission is
    /// returned as [`TrackedSubmission::Deduplicated`] instead of being indistinguishable from a new one.
    pub async fn submit_and_track_transaction_outcome(&self, signed_tx: &[u8]) -> Result<TrackedSubmission> {
        let resp = self
            .build_query(GraphQlQueries::mutate_submit_and_track_transaction(signed_tx))?
            .await?;

        response_to_data(resp)?.send_transaction_async.into_tracked_submission()
    }

    /// Like [`Self::submit_and_track_transaction_outcome`], and also returns why the tracked transaction timed
    /// out unmined when a deduplicated submission already reports a timeout.
    ///
    /// The diagnosis is `None` for every other outcome, when the Blokli server is older than 0.15.0, or when the
    /// lookup fails.
    pub async fn submit_and_track_transaction_detailed(
        &self,
        signed_tx: &[u8],
    ) -> Result<(TrackedSubmission, Option<TransactionDiagnosis>)> {
        let resp = self
            .build_query(GraphQlQueries::mutate_submit_and_track_transaction(signed_tx))?
            .await?;

        let result = response_to_data(resp)?.send_transaction_async;
        let timed_out = matches!(
            &result,
            SendTransactionAsyncResult::DeduplicatedTransaction(d) if d.transaction.status == TransactionStatus::Timeout
        );
        let submission = result.into_tracked_submission()?;

        let diagnosis = if timed_out {
            self.fetch_unmined_diagnosis(submission.tx_id()).await
        } else {
            None
        };

        Ok((submission, diagnosis))
    }
}

#[async_trait::async_trait]
impl BlokliTransactionClient for BlokliClient {
    async fn submit_transaction(&self, signed_tx: &[u8]) -> Result<TxReceipt> {
        let resp = self
            .build_query(GraphQlQueries::mutate_submit_transaction(signed_tx))?
            .await?;

        response_to_data(resp)?.send_transaction.into()
    }

    async fn submit_and_track_transaction(&self, signed_tx: &[u8]) -> Result<TxId> {
        let resp = self
            .build_query(GraphQlQueries::mutate_submit_and_track_transaction(signed_tx))?
            .await?;

        let tx: Result<Transaction> = response_to_data(resp)?.send_transaction_async.into();

        Ok(tx?.id.into_inner())
    }

    async fn submit_and_confirm_transaction(&self, signed_tx: &[u8], num_confirmations: usize) -> Result<TxReceipt> {
        let resp = self
            .build_query(GraphQlQueries::mutate_submit_and_confirm_transaction(
                signed_tx,
                num_confirmations,
            ))?
            .await?;

        let tx: Result<Transaction> = response_to_data(resp)?.send_transaction_sync.into();

        let hash = tx?.transaction_hash.0.to_lowercase();
        Ok(hex::decode(hash.trim_start_matches("0x"))
            .map_err(|_| ErrorKind::ParseError)
            .and_then(|d| d.try_into().map_err(|_| ErrorKind::ParseError))?)
    }

    async fn track_transaction(&self, tx_id: TxId, client_timeout: Duration) -> Result<TransactionTrackingOutcome> {
        let transaction = self
            .build_dedicated_subscription_stream(
                GraphQlQueries::subscribe_track_transaction(tx_id.clone()),
                "track_transaction",
            )?
            .0
            .try_filter_map(|item| {
                futures::future::ready(match &item.transaction_updated.status {
                    TransactionStatus::Confirmed => Ok(Some(item.transaction_updated)),
                    TransactionStatus::Pending | TransactionStatus::Submitted => Ok(None),
                    TransactionStatus::Reverted => Err(ErrorKind::TrackingError(TrackingErrorKind::Reverted).into()),
                    TransactionStatus::SubmissionFailed => {
                        Err(ErrorKind::TrackingError(TrackingErrorKind::SubmissionFailed).into())
                    }
                    TransactionStatus::Timeout => Err(ErrorKind::TrackingError(TrackingErrorKind::Timeout).into()),
                    TransactionStatus::ValidationFailed => {
                        Err(ErrorKind::TrackingError(TrackingErrorKind::ValidationFailed).into())
                    }
                    // A status added by a newer Blokli. Whether it is final is unknown, so keep
                    // waiting: a later known status ends tracking, or the client timeout does.
                    TransactionStatus::Unknown => {
                        tracing::warn!(
                            tx_id = item.transaction_updated.id.inner(),
                            "unknown transaction status from blokli, still waiting"
                        );
                        Ok(None)
                    }
                })
            })
            .try_next()
            .timeout(FuturesTimeDuration::from(client_timeout))
            .await;

        match transaction {
            Ok(Ok(Some(transaction))) => Ok(TransactionTrackingOutcome::Confirmed(transaction)),
            Ok(Ok(None)) | Err(_) => Ok(TransactionTrackingOutcome::StatusUnknown { tx_id }),
            Ok(Err(error)) if matches!(error.kind(), ErrorKind::TrackingError(TrackingErrorKind::Timeout)) => {
                match self.fetch_unmined_diagnosis(&tx_id).await {
                    Some(diagnosis) => Err(error.with_unmined_diagnosis(diagnosis)),
                    None => Err(error),
                }
            }
            Ok(Err(error)) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::version_supports_diagnosis;

    #[test]
    fn version_gate_compares_major_minor_patch() {
        for supported in [
            "0.15.0",
            "0.15.1",
            "0.16.0",
            "1.0.0",
            "v0.15.0",
            " 0.15.0 ",
            "0.15.0-rc.1",
            "0.15.0+build.5",
        ] {
            assert!(version_supports_diagnosis(supported), "{supported} must be supported");
        }
        for unsupported in [
            "0.14.6",
            "0.14.99",
            "0.9.0",
            "0.15",
            "0.15.0.1",
            "abc",
            "",
            "0.15.x",
            "0.14.9-rc.1",
        ] {
            assert!(
                !version_supports_diagnosis(unsupported),
                "{unsupported} must be unsupported"
            );
        }
    }
}
