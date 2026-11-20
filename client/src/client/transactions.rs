use std::time::{Duration, Instant};

use cynic::{MutationBuilder, QueryBuilder, SubscriptionBuilder};
use futures::TryStreamExt;
use futures_time::{future::FutureExt, time::Duration as FuturesTimeDuration};
use hex::ToHex;

use super::{BlokliClient, GraphQlQueries, response_to_data};
use crate::{
    api::{
        BlokliTransactionClient, Result, TransactionTrackingOutcome, TxId, TxReceipt,
        internal::{
            ConfirmTransactionVariables, MutateConfirmTransaction, MutateSendTransaction, MutateTrackTransaction,
            QueryUnminedDiagnosis, SendTransactionAsyncResult, SendTransactionVariables, SubscribeTransaction,
            TransactionsVariables,
        },
        types::{TrackedSubmission, Transaction, TransactionDiagnosis, TransactionStatus},
    },
    errors::{BlokliClientError, ErrorKind, TrackingErrorKind},
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
    /// Selects `Transaction.diagnosis`, which Blokli servers without that field reject as an unknown field. The client
    /// sends it only after a server timeout, and stops sending it for a while once a server rejected it.
    pub fn query_unmined_diagnosis(tx_id: TxId) -> cynic::Operation<QueryUnminedDiagnosis, TransactionsVariables> {
        QueryUnminedDiagnosis::build(TransactionsVariables { id: tx_id.into() })
    }
}

/// How long the client stops asking for a diagnosis after a server rejected the `diagnosis` field.
///
/// Servers that do not know the field fail the query at validation, so asking again on every timeout is pointless.
/// The pause is bounded so that a Blokli upgrade is picked up without restarting the client.
const UNMINED_DIAGNOSIS_UNSUPPORTED_TTL: Duration = Duration::from_secs(12 * 60 * 60);

/// The validation message of a server that does not know `Transaction.diagnosis`.
///
/// async-graphql attaches no error code or extensions to validation errors, so the message is the only marker.
const UNKNOWN_DIAGNOSIS_FIELD_MESSAGE: &str = "Unknown field \"diagnosis\" on type \"Transaction\"";

/// Remembers, per client and its clones, until when the server is assumed unable to diagnose.
#[derive(Debug)]
pub(super) struct UnminedDiagnosisBackoff {
    ttl: Duration,
    unsupported_until: std::sync::Mutex<Option<Instant>>,
}

impl UnminedDiagnosisBackoff {
    pub(super) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            unsupported_until: std::sync::Mutex::new(None),
        }
    }

    fn is_unsupported(&self) -> bool {
        let guard = self.unsupported_until.lock().unwrap_or_else(|e| e.into_inner());
        guard.is_some_and(|until| Instant::now() < until)
    }

    fn mark_unsupported(&self) {
        let mut guard = self.unsupported_until.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Instant::now().checked_add(self.ttl);
    }
}

impl Default for UnminedDiagnosisBackoff {
    fn default() -> Self {
        Self::new(UNMINED_DIAGNOSIS_UNSUPPORTED_TTL)
    }
}

/// Whether the error is a server rejecting the `diagnosis` field as unknown.
fn is_unknown_diagnosis_field(error: &BlokliClientError) -> bool {
    matches!(error.kind(), ErrorKind::GraphQLError(e) if e.message.contains(UNKNOWN_DIAGNOSIS_FIELD_MESSAGE))
}

impl BlokliClient {
    /// Reads the diagnosis of a transaction that Blokli reported as timed out.
    ///
    /// Returns `None` when the lookup fails, so the caller keeps the timeout it already has. A server that rejects
    /// the `diagnosis` field as unknown is not asked again for [`UNMINED_DIAGNOSIS_UNSUPPORTED_TTL`]; no other
    /// failure is remembered.
    async fn fetch_unmined_diagnosis(&self, tx_id: &TxId) -> Option<TransactionDiagnosis> {
        if self.unmined_diagnosis_backoff.is_unsupported() {
            return None;
        }

        let result: Result<Option<TransactionDiagnosis>> = async {
            let resp = self
                .build_query(GraphQlQueries::query_unmined_diagnosis(tx_id.clone()))?
                .await?;

            response_to_data(resp)?
                .transaction
                .ok_or::<BlokliClientError>(ErrorKind::NoData.into())?
                .into_diagnosis()
        }
        .await;

        result.unwrap_or_else(|error| {
            if is_unknown_diagnosis_field(&error) {
                tracing::info!(%error, "blokli does not serve the unmined diagnosis, not asking again for a while");
                self.unmined_diagnosis_backoff.mark_unsupported();
            } else {
                tracing::warn!(%error, tx_id, "could not read the unmined diagnosis");
            }
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
    /// The diagnosis is `None` for every other outcome, when the Blokli server does not know the `diagnosis` field, or
    /// when the lookup fails.
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
                Err(error.with_unmined_diagnosis(self.fetch_unmined_diagnosis(&tx_id).await))
            }
            Ok(Err(error)) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mockito::Matcher;

    use super::*;
    use crate::BlokliClientConfig;

    const UNKNOWN_FIELD_BODY: &str = r#"{"errors":[{"message":"Unknown field \"diagnosis\" on type \"Transaction\".","locations":[{"line":3,"column":5}]}]}"#;
    const DIAGNOSIS_BODY: &str = r#"{"data":{"transaction":{"__typename":"Transaction","diagnosis":{"reason":"QUEUED","signedFees":null,"recommendedFees":null,"minReplacementFees":null,"retryAfterSeconds":3}}}}"#;

    async fn serve(server: &mut mockito::ServerGuard, status: usize, body: &str) -> mockito::Mock {
        server
            .mock("POST", "/graphql")
            .match_body(Matcher::Regex("QueryUnminedDiagnosis".into()))
            .with_status(status)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await
    }

    fn client(server: &mockito::ServerGuard, ttl: Duration) -> BlokliClient {
        let mut client = BlokliClient::new(server.url().parse().expect("valid URL"), BlokliClientConfig::default());
        client.unmined_diagnosis_backoff = Arc::new(UnminedDiagnosisBackoff::new(ttl));
        client
    }

    #[test]
    fn the_unsupported_pause_lasts_twelve_hours() {
        assert_eq!(UNMINED_DIAGNOSIS_UNSUPPORTED_TTL, Duration::from_secs(12 * 3600));
    }

    #[tokio::test]
    async fn a_supporting_server_returns_the_diagnosis() {
        let mut server = mockito::Server::new_async().await;
        let mock = serve(&mut server, 200, DIAGNOSIS_BODY).await;
        let client = client(&server, UNMINED_DIAGNOSIS_UNSUPPORTED_TTL);

        let diagnosis = client
            .fetch_unmined_diagnosis(&"tx".to_owned())
            .await
            .expect("diagnosis");

        assert_eq!(diagnosis.reason, crate::api::types::UnminedReason::Queued);
        assert_eq!(diagnosis.retry_after_seconds, 3);
        // Success needs no bookkeeping: the next timeout asks again
        client
            .fetch_unmined_diagnosis(&"tx".to_owned())
            .await
            .expect("diagnosis");
        mock.expect(2).assert_async().await;
    }

    #[tokio::test]
    async fn an_unknown_field_error_is_remembered_for_the_ttl_and_shared_by_clones() {
        let mut server = mockito::Server::new_async().await;
        let mock = serve(&mut server, 200, UNKNOWN_FIELD_BODY).await;
        let client = client(&server, Duration::from_millis(200));
        let clone = client.clone();

        assert!(client.fetch_unmined_diagnosis(&"tx".to_owned()).await.is_none());
        // Within the pause, neither this client nor its clone asks again
        assert!(client.fetch_unmined_diagnosis(&"tx".to_owned()).await.is_none());
        assert!(clone.fetch_unmined_diagnosis(&"tx".to_owned()).await.is_none());
        mock.expect(1).assert_async().await;

        // After the pause the query is sent again, and an upgraded server answers it
        tokio::time::sleep(Duration::from_secs(1)).await;
        let upgraded = serve(&mut server, 200, DIAGNOSIS_BODY).await;
        assert!(clone.fetch_unmined_diagnosis(&"tx".to_owned()).await.is_some());
        upgraded.assert_async().await;
    }

    #[tokio::test]
    async fn other_failures_are_not_remembered() {
        let other_graphql_error = r#"{"errors":[{"message":"internal error"}]}"#;
        for (status, body) in [
            (500, ""),
            (200, other_graphql_error),
            (200, r#"{"data":{"transaction":null}}"#),
        ] {
            let mut server = mockito::Server::new_async().await;
            let mock = serve(&mut server, status, body).await;
            let client = client(&server, UNMINED_DIAGNOSIS_UNSUPPORTED_TTL);

            // None of these may start a pause: every call reaches the server
            assert!(client.fetch_unmined_diagnosis(&"tx".to_owned()).await.is_none());
            assert!(client.fetch_unmined_diagnosis(&"tx".to_owned()).await.is_none());
            mock.expect(2).assert_async().await;
        }
    }

    #[tokio::test]
    async fn an_unrelated_unknown_field_is_not_taken_for_missing_diagnosis_support() {
        let mut server = mockito::Server::new_async().await;
        let body = r#"{"errors":[{"message":"Unknown field \"transaction\" on type \"QueryRoot\"."}]}"#;
        let mock = serve(&mut server, 200, body).await;
        let client = client(&server, UNMINED_DIAGNOSIS_UNSUPPORTED_TTL);

        assert!(client.fetch_unmined_diagnosis(&"tx".to_owned()).await.is_none());
        assert!(client.fetch_unmined_diagnosis(&"tx".to_owned()).await.is_none());
        mock.expect(2).assert_async().await;
    }
}
