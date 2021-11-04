use aws_sdk_qldbsessionv2::model::{
    AbortTransactionRequest, CommandStream, CommitTransactionRequest, ExecuteStatementRequest,
    FetchPageRequest, ResultStream, StartTransactionRequest,
};
use bb8::PooledConnection;
use bytes::Bytes;
use ion_c_sys::reader::IonCReaderHandle;
use ion_c_sys::result::IonCError;
use std::convert::TryFrom;
use std::marker::PhantomData;
use tracing::debug;

use crate::error;
use crate::pool::QldbSessionV2Manager;
use crate::{error::TransactError, execution_stats::ExecutionStats};

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

pub enum TransactionDisposition<R> {
    Committed {
        commit_execution_stats: ExecutionStats,
        user_data: R,
    },
    Aborted,
}

/// QLDB uses Optimistic Concurrency Control. Transactions are speculative until
/// committed (may be rejected due to interference). This "attempt" at a
/// transaction may be an actual transaction if [`TransactionAttempt::commit`]
/// succeeds!
///
/// `E` represents any custom error variant the user may throw.
pub struct TransactionAttempt<E> {
    /// A pooled connection that we'll send our commands down.
    connection: PooledConnection<'static, QldbSessionV2Manager>,

    /// The id of this transaction attempt. This is a speculative transaction
    /// id. That is, if the transaction commits, then this id is the id of the
    /// transaction. However, if the transaction does not commit then it is not
    /// a valid QLDB transaction id. If the transaction is retired (another
    /// attempt is made), then a new id will be assigned.
    pub id: String,

    /// Accumulates stats for this transaction attempt. Repeated calls of this
    /// method may return different results if additional API calls were made.
    /// The stats will include the timing and IO usage for the start and commit
    /// API calls too.
    ///
    /// If you call this method at the start of a transaction, that will include
    /// the timing information of the start transaction call!
    pub accumulated_execution_stats: ExecutionStats,

    /// Preserves any custom error variants so that the method signatures line
    /// up.
    err: PhantomData<E>,
}

impl<E> TransactionAttempt<E>
where
    E: std::error::Error + 'static,
{
    pub(crate) async fn start(
        mut connection: PooledConnection<'static, QldbSessionV2Manager>,
    ) -> Result<TransactionAttempt<E>, TransactError<E>> {
        let mut accumulated_execution_stats = ExecutionStats::default();
        let resp = connection
            .send_streaming_command(CommandStream::StartTransaction(
                StartTransactionRequest::builder().build(),
            ))
            .await
            .map_err(error::transport_err)?;
        let start_result = match resp {
            ResultStream::StartTransaction(it) => it,
            it => Err(error::unexpected_response("StartTransaction", it))?,
        };
        accumulated_execution_stats.accumulate(&start_result);

        let id = start_result
            .transaction_id
            .ok_or(error::malformed_response(
                "StartTransaction did not return a transaction_id",
            ))?;

        Ok(TransactionAttempt {
            connection,
            id,
            accumulated_execution_stats,
            err: PhantomData,
        })
    }

    pub fn statement<S>(&mut self, statement: S) -> StatementBuilder<'_, E>
    where
        S: Into<String>,
    {
        StatementBuilder::new(self, statement.into())
    }

    /// Send a statement without any parameters. For example, this could be used
    /// to create a table where the name is already sanitized.
    #[must_use]
    pub async fn execute_statement<S>(
        &mut self,
        partiql: S,
    ) -> Result<StatementResults, TransactError<E>>
    where
        S: Into<String>,
    {
        self.statement(partiql).execute().await
    }

    // FIXME: don't buffer all results
    async fn execute_statement_internal(
        &mut self,
        statement: Statement,
    ) -> Result<StatementResults, TransactError<E>> {
        let mut execution_stats = ExecutionStats::default();
        let resp = self
            .connection
            .send_streaming_command(CommandStream::ExecuteStatement(
                ExecuteStatementRequest::builder()
                    .transaction_id(&self.id)
                    .statement(&statement.partiql)
                    .build(),
            ))
            .await
            .map_err(error::transport_err)?;

        let execute_result = match resp {
            ResultStream::ExecuteStatement(it) => it,
            it => Err(error::unexpected_response("ExecuteStatement", it))?,
        };
        execution_stats.accumulate(&execute_result);

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
                        _ => Err(error::malformed_response(
                            "expected only one of ion binary or text",
                        ))?,
                    };
                    values.push(Bytes::from(bytes.into_inner()));
                }

                if let Some(next_page_token) = page.next_page_token {
                    let resp = self
                        .connection
                        .send_streaming_command(CommandStream::FetchPage(
                            FetchPageRequest::builder()
                                .transaction_id(&self.id)
                                .next_page_token(&next_page_token)
                                .build(),
                        ))
                        .await
                        .map_err(error::transport_err)?;

                    let fetch_page_result = match resp {
                        ResultStream::FetchPage(it) => it,
                        it => Err(error::unexpected_response("FetchPage", it))?,
                    };

                    execution_stats.accumulate(&fetch_page_result);

                    if let Some(p) = fetch_page_result.page {
                        current.replace(p);
                    }
                }
            }
        }

        self.accumulated_execution_stats
            .accumulate(&execution_stats);
        Ok(StatementResults::new(values, execution_stats))
    }

    /// Attempt to commit this transaction. If the commit succeeds, `user_data`
    /// will be returned.
    pub async fn commit<R>(
        mut self,
        user_data: R,
    ) -> Result<TransactionDisposition<R>, TransactError<E>> {
        debug!(id = &self.id[..], "transaction will be committed");
        let resp = self
            .connection
            .send_streaming_command(CommandStream::CommitTransaction(
                CommitTransactionRequest::builder()
                    .transaction_id(&self.id)
                    .build(),
            ))
            .await
            .map_err(error::transport_err)?;

        let commit_result = match resp {
            ResultStream::CommitTransaction(it) => it,
            it => Err(error::unexpected_response("CommitTransaction", it))?,
        };

        // If we get a successful commit, check some invariants. Otherwise, the
        // error must be handled by the caller. In most cases, this should be
        // the driver which may retry the transaction.
        if let Some(ref id) = commit_result.transaction_id {
            if id != &self.id {
                Err(error::malformed_response(format!(
                    "transaction {} response returned a different id: {:#?}",
                    self.id, id,
                )))?
            }
        }

        self.accumulated_execution_stats.accumulate(&commit_result);

        Ok(TransactionDisposition::Committed {
            commit_execution_stats: ExecutionStats::from_api(commit_result),
            user_data,
        })
    }

    /// Attempts to abort this transaction.
    // TODO: Abort failures should close the connection.
    pub async fn abort<R>(mut self) -> Result<TransactionDisposition<R>, TransactError<E>> {
        debug!(id = &self.id[..], "transaction will be aborted");
        let resp = self
            .connection
            .send_streaming_command(CommandStream::AbortTransaction(
                AbortTransactionRequest::builder().build(),
            ))
            .await
            .map_err(error::transport_err)?;

        let abort_result = match resp {
            ResultStream::AbortTransaction(it) => it,
            it => Err(error::unexpected_response("AbortTransaction", it))?,
        };

        // TODO: Should we ignore abort failures?
        self.accumulated_execution_stats.accumulate(&abort_result);

        Ok(TransactionDisposition::Aborted)
    }
}

pub struct StatementBuilder<'tx, E> {
    attempt: &'tx mut TransactionAttempt<E>,
    statement: Statement,
}

impl<'tx, E> StatementBuilder<'tx, E>
where
    E: std::error::Error + 'static,
{
    fn new(attempt: &'tx mut TransactionAttempt<E>, partiql: String) -> StatementBuilder<'tx, E> {
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
    pub fn param<B>(mut self, param: B) -> StatementBuilder<'tx, E>
    where
        B: Into<Bytes>,
    {
        self.statement.params.push(param.into());
        self
    }

    pub async fn execute(self) -> Result<StatementResults, TransactError<E>> {
        let StatementBuilder { attempt, statement } = self;
        attempt.execute_statement_internal(statement).await
    }
}

struct Statement {
    partiql: String,
    params: Vec<Bytes>,
}
