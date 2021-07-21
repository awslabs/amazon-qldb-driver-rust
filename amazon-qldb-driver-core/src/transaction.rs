use crate::api::QldbSession;
use crate::error::{QldbError, QldbResult};
use crate::execution_stats::HasExecutionStats;
use crate::qldb_hash::QldbHash;
use crate::{
    api::{QldbSessionApi, TransactionId},
    execution_stats::ExecutionStats,
};
use crate::{ion_compat::ion_hash, pool::QldbHttp1Connection};
use anyhow::Result;
use aws_sdk_qldbsession::model::{FetchPageResult, Page, ValueHolder};
use bytes::Bytes;
use futures::{FutureExt, Stream};
use ion_c_sys::reader::IonCReaderHandle;
use ion_c_sys::result::IonCError;
use std::convert::TryFrom;
use std::pin::Pin;
use std::task::{self, Poll};
use tracing::debug;

/// The results of executing a statement.
///
/// A statement may return may pages of results. This type represents pulling
/// all of those pages into memory. As such, this type represents reading all
/// results (it will never be constructed with partial results).
///
/// [`cumulative_timing_information`] and [`cumulative_io_usage`] represent the
/// sum of server reported timing and IO usage across all pages that were
/// fetched.
pub struct ResultStream<'tx, C>
where
    C: QldbSession + Send + Sync + Clone,
{
    attempt: &'tx mut TransactionAttempt<C>,
    current_page: Option<(Page, usize)>,
    execution_stats: ExecutionStats,
}

impl<'tx, C> ResultStream<'tx, C>
where
    C: QldbSession + Send + Sync + Clone,
{
    fn new(attempt: &'tx mut TransactionAttempt<C>) -> ResultStream<'tx, C> {
        ResultStream {
            attempt,
            current_page: None,
            execution_stats: ExecutionStats::default(),
        }
    }

    fn on_next_page<S: HasExecutionStats>(&mut self, page: Option<Page>, stats: &S) {
        self.execution_stats.accumulate(stats);
        self.current_page = match page {
            Some(p) => Some((p, 0)),
            None => None,
        };
    }

    // FIXME: Don't clone
    async fn next_value(&mut self) -> QldbResult<Option<ValueHolder>> {
        // Fast path: already have results in memory
        if let Some((ref page, ref mut index)) = self.current_page {
            if let Some(ref values) = page.values {
                if values.len() < *index {
                    *index += 1;
                    return Ok(Some(values[*index - 1].clone()));
                }
            }
        }

        // If we didn't return, that means we need to fetch more results if
        // there is a `next_page_token`.
        if let Some(Some(next_page_token)) = self
            .current_page
            .map(|(ref p, _)| p.next_page_token.clone())
        {
            let fetch = self.attempt.fetch_page_internal(next_page_token).await?;
            self.on_next_page(fetch.page, &fetch);
            return self.next_value().await;
        }

        // Otherwise, we're done.
        Ok(None)
    }
}

pub struct StatementResults {
    values: Vec<Bytes>,
    execution_stats: ExecutionStats,
}

impl StatementResults {
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn raw_values(&self) -> impl Iterator<Item = &[u8]> {
        self.values.iter().map(|bytes| bytes.as_ref())
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

pub enum TransactionAttemptResult<R> {
    Committed {
        commit_execution_stats: ExecutionStats,
        user_data: R,
    },
    Aborted,
}

pub struct TransactionAttempt<C>
where
    C: QldbSession + Send + Sync + Clone,
{
    pooled_session: QldbHttp1Connection<C>,
    pub id: TransactionId,
    commit_digest: QldbHash,
    /// Accumulates stats for this transaction attempt. Repeated calls of this
    /// method may return different results if additional API calls were made.
    /// The stats will include the timing and IO usage for the start and commit
    /// API calls too.
    ///
    /// If you call this method at the start of a transaction, that will include
    /// the timing information of the start transaction call!
    pub accumulated_execution_stats: ExecutionStats,
}

impl<C> TransactionAttempt<C>
where
    C: QldbSession + Send + Sync + Clone,
{
    pub(crate) async fn start(
        pooled_session: QldbHttp1Connection<C>,
    ) -> Result<TransactionAttempt<C>, QldbError> {
        let mut accumulated_execution_stats = ExecutionStats::default();
        let start_result = pooled_session
            .start_transaction(&pooled_session.session_token())
            .await?;
        accumulated_execution_stats.accumulate(&start_result);
        let id = start_result
            .transaction_id
            .ok_or(QldbError::UnexpectedResponse(
                "StartTransaction should always return a transaction_id".into(),
            ))?;

        let seed_hash = ion_hash(&id);
        let commit_digest = QldbHash::from_bytes(seed_hash).unwrap();
        let transaction = TransactionAttempt {
            pooled_session,
            id,
            commit_digest,
            accumulated_execution_stats,
        };
        Ok(transaction)
    }

    pub fn statement<S>(&mut self, statement: S) -> StatementBuilder<'_, C>
    where
        S: Into<String>,
    {
        StatementBuilder::new(self, statement.into())
    }

    /// Send a statement without any parameters. For example, this could be used
    /// to create a table where the name is already sanitized.
    pub async fn execute_statement<S>(&mut self, partiql: S) -> Result<ResultStream, QldbError>
    where
        S: Into<String>,
    {
        self.statement(partiql).execute().await
    }

    // FIXME: don't buffer all results
    async fn execute_statement_internal(
        &mut self,
        statement: Statement,
    ) -> QldbResult<ResultStream> {
        let execute_result = self
            .pooled_session
            .execute_statement(
                &self.pooled_session.session_token(),
                &self.id,
                statement.partiql.clone(),
            )
            .await?;

        let statement_hash = QldbHash::from_bytes(ion_hash(&statement.partiql)).unwrap();
        self.commit_digest = self.commit_digest.dot(&statement_hash);

        let mut results = ResultStream::new(self);
        results.on_next_page(execute_result.first_page, &execute_result);

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
                    values.push(Bytes::from(bytes.into_inner()));
                }

                if let Some(next_page_token) = page.next_page_token {
                    let fetch_page_result = self
                        .pooled_session
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

        self.accumulated_execution_stats
            .accumulate(&execution_stats);
        Ok(ResultStream::new(values, execution_stats))
    }

    async fn fetch_page_internal(
        &mut self,
        next_page_token: String,
    ) -> QldbResult<FetchPageResult> {
        todo!()
    }

    pub async fn commit<R>(mut self, user_data: R) -> Result<TransactionAttemptResult<R>> {
        debug!(id = &self.id[..], "transaction will be committed");
        let res = self
            .pooled_session
            .commit_transaction(
                &self.pooled_session.session_token(),
                self.id.clone(),
                self.commit_digest.bytes(),
            )
            .await?;

        // If we get a successful commit, check some invariants. Otherwise, the
        // error must be handled by the caller. In most cases, this should be
        // the driver which may retry the transaction.
        if let Some(ref id) = res.transaction_id {
            if id != &self.id {
                Err(QldbError::IllegalState(format!(
                    "transaction {} response returned a different id: {:#?}",
                    self.id, res,
                )))?
            }
        }

        if let Some(ref bytes) = res.commit_digest {
            if bytes.as_ref() != &self.commit_digest.bytes()[..] {
                Err(QldbError::IllegalState(format!(
                    "transaction {} response returned a different commit digest: {:#?}",
                    self.id, res,
                )))?
            }
        }

        self.accumulated_execution_stats.accumulate(&res);

        Ok(TransactionAttemptResult::Committed {
            commit_execution_stats: ExecutionStats::from_api(res),
            user_data,
        })
    }

    // Always returns `Ok` even though the signature says `Result`. This is to
    // keep the type consistent with `commit`.
    pub async fn abort<R>(mut self) -> Result<TransactionAttemptResult<R>> {
        debug!(id = &self.id[..], "transaction will be aborted");
        match self
            .pooled_session
            .abort_transaction(&self.pooled_session.session_token())
            .await
        {
            Ok(r) => self.accumulated_execution_stats.accumulate(&r),
            Err(e) => {
                debug!(
                    error = %e,
                    id = &self.id[..],
                    "ignoring failure to abort tx"
                );
                self.pooled_session.notify_invalid();
            }
        };

        Ok(TransactionAttemptResult::Aborted)
    }
}

pub struct StatementBuilder<'tx, C>
where
    C: QldbSession + Send + Sync + Clone,
{
    attempt: &'tx mut TransactionAttempt<C>,
    statement: Statement,
}

impl<'tx, C> StatementBuilder<'tx, C>
where
    C: QldbSession + Send + Sync + Clone,
{
    fn new(attempt: &'tx mut TransactionAttempt<C>, partiql: String) -> StatementBuilder<'tx, C> {
        StatementBuilder {
            attempt,
            statement: Statement {
                partiql: partiql.into(),
                params: vec![],
            },
        }
    }

    // TODO: This currently takes anything as bytes, which is wrong in two ways:
    // 1. need an IonElement so we can hash it. in the future, we hope to remove this as a requirement
    // 2. perhaps we want an in-crate trait for coherency reasons
    // TODO: make public when ready
    fn param<B>(mut self, param: B) -> StatementBuilder<'tx, C>
    where
        B: Into<Bytes>,
    {
        self.statement.params.push(param.into());
        self
    }

    async fn execute(self) -> QldbResult<ResultStream> {
        let StatementBuilder { attempt, statement } = self;
        attempt.execute_statement_internal(statement).await
    }
}

struct Statement {
    partiql: String,
    params: Vec<Bytes>,
}
