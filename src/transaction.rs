use crate::qldb_hash::QldbHash;
use crate::QldbError;
use crate::{
    api::{QldbSessionApi, TransactionId},
    execution_stats::ExecutionStats,
};
use crate::{ion_compat::ion_hash, pool::QldbHttp1Connection};
use anyhow::Result;
use bytes::Bytes;
use ion_c_sys::reader::IonCReaderHandle;
use ion_c_sys::result::IonCError;
use std::convert::TryFrom;

/// The results of executing a statement.
///
/// A statement may return may pages of results. This type represents pulling
/// all of those pages into memory. As such, this type represents reading all
/// results (it will never be constructed with partial results).
///
/// [`cumulative_timing_information`] and [`cumulative_io_usage`] represent the
/// sum of server reported timing and IO usage across all pages that were
/// fetched.
pub struct StatementResults {
    values: Vec<Bytes>,
    execution_stats: ExecutionStats,
}

impl StatementResults {
    fn new(values: Vec<Bytes>, execution_stats: ExecutionStats) -> StatementResults {
        StatementResults {
            values,
            execution_stats,
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn readers(&self) -> impl Iterator<Item = Result<IonCReaderHandle, IonCError>> {
        self.values
            .iter()
            .map(|bytes| IonCReaderHandle::try_from(&bytes[..]))
    }

    pub fn execution_stats(&self) -> &ExecutionStats {
        &self.execution_stats
    }
}

pub enum TransactionDisposition {
    Commit,
    Abort,
}

pub struct TransactionOutcome<R> {
    pub(crate) disposition: TransactionDisposition,
    pub(crate) response: Result<R, QldbError>,
}

pub struct TransactionAttempt<C: QldbSessionApi + Send> {
    client: C,
    pooled_session: QldbHttp1Connection,
    pub id: TransactionId,
    commit_digest: QldbHash,
    execution_stats: ExecutionStats,
}

impl<C> TransactionAttempt<C>
where
    C: QldbSessionApi + Send,
{
    pub(crate) async fn start(
        client: C,
        pooled_session: QldbHttp1Connection,
    ) -> Result<TransactionAttempt<C>, QldbError> {
        let mut execution_stats = ExecutionStats::default();
        let start_result = client
            .start_transaction(&pooled_session.session_token())
            .await?;
        execution_stats.accumulate(&start_result);
        let id = start_result
            .transaction_id
            .ok_or(QldbError::UnexpectedResponse(
                "StartTransaction should always return a transaction_id".into(),
            ))?;

        let seed_hash = ion_hash(&id);
        let commit_digest = QldbHash::from_bytes(seed_hash).unwrap();
        let transaction = TransactionAttempt {
            client,
            pooled_session,
            id,
            commit_digest,
            execution_stats,
        };
        Ok(transaction)
    }

    // FIXME: params, result, IonHash
    pub async fn execute_statement<S>(
        &mut self,
        statement: S,
    ) -> Result<StatementResults, QldbError>
    where
        S: Into<String>,
    {
        let statement = statement.into();

        let mut execution_stats = ExecutionStats::default();
        let execute_result = self
            .client
            .execute_statement(
                &self.pooled_session.session_token(),
                &self.id,
                statement.clone(),
            )
            .await?;
        execution_stats.accumulate(&execute_result);

        let statement_hash = QldbHash::from_bytes(ion_hash(&statement)).unwrap();
        self.commit_digest = self.commit_digest.dot(&statement_hash);

        let mut values = vec![];
        let mut current = execute_result.first_page;
        loop {
            let page = match &current {
                Some(_) => current.take().unwrap(),
                None => break,
            };

            if let Some(holders) = page.values {
                for holder in holders {
                    let bytes = match (holder.ion_text, holder.ion_binary) {
                        (None, Some(bytes)) => bytes,
                        (Some(_txt), None) => unimplemented!(), // TextIonCursor::new(txt),
                        _ => Err(QldbError::UnexpectedResponse(
                            "expected only one of ion binary or text".to_string(),
                        ))?,
                    };
                    values.push(bytes);
                }

                if let Some(next_page_token) = page.next_page_token {
                    let fetch_page_result = self
                        .client
                        .fetch_page(
                            &self.pooled_session.session_token(),
                            &self.id,
                            next_page_token,
                        )
                        .await?;

                    execution_stats.accumulate(&fetch_page_result);

                    if let Some(p) = fetch_page_result.page {
                        current.replace(p);
                    }
                }
            }
        }

        self.execution_stats.accumulate(&execution_stats);
        Ok(StatementResults::new(values, execution_stats))
    }

    #[deprecated(note = "Please use commit instead")]
    pub async fn ok<R>(self, user_data: R) -> Result<TransactionOutcome<R>> {
        self.commit(user_data).await
    }

    pub async fn commit<R>(mut self, user_data: R) -> Result<TransactionOutcome<R>> {
        debug!("transaction {} will be committed", self.id);
        let res = self
            .client
            .commit_transaction(
                &self.pooled_session.session_token(),
                self.id.clone(),
                self.commit_digest.bytes(),
            )
            .await;

        // If we get a successful commit, check some invariants. Otherwise, the
        // error must be handled by the caller. In most cases, this should be
        // the driver which may retry the transaction.
        if let Ok(ref api) = res {
            if let Some(ref id) = api.transaction_id {
                if id != &self.id {
                    Err(QldbError::IllegalState(format!(
                        "transaction {} response returned a different id: {:#?}",
                        self.id, api,
                    )))?
                }
            }

            if let Some(ref bytes) = api.commit_digest {
                if bytes != &self.commit_digest.bytes() {
                    Err(QldbError::IllegalState(format!(
                        "transaction {} response returned a different commit digest: {:#?}",
                        self.id, api,
                    )))?
                }
            }

            self.execution_stats.accumulate(api);
        }

        let response = res.map(|_| user_data);

        Ok(TransactionOutcome {
            disposition: TransactionDisposition::Commit,
            response,
        })
    }

    // FIXME: there are two issues with this API
    // 1. we really don't want to encourage returning data that didn't pass OCC
    // 2. it requires the user to match the R type of their commit
    pub async fn abort<R>(mut self, user_data: R) -> Result<TransactionOutcome<R>> {
        debug!("transaction {} will be aborted", self.id);
        match self
            .client
            .abort_transaction(&self.pooled_session.session_token())
            .await
        {
            Ok(r) => self.execution_stats.accumulate(&r),
            Err(e) => {
                debug!("ignoring failure to abort tx {}: {}", self.id, e);
                self.pooled_session.notify_invalid();
            }
        };

        Ok(TransactionOutcome {
            disposition: TransactionDisposition::Abort,
            response: Ok(user_data),
        })
    }
}
