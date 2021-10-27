use crate::error::{self, QldbError, QldbResult};
use crate::execution_stats::ExecutionStats;
use crate::pool::QldbHttp2Connection;
use anyhow::Result;
use aws_sdk_qldbsessionv2::model::{
    AbortTransactionRequest, CommandStream, CommitTransactionRequest, ExecuteStatementRequest,
    FetchPageRequest, ResultStream, StartTransactionRequest,
};
use bytes::Bytes;
use ion_c_sys::reader::IonCReaderHandle;
use ion_c_sys::result::IonCError;
use std::convert::TryFrom;
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

pub enum TransactionAttemptResult<R> {
    Committed {
        commit_execution_stats: ExecutionStats,
        user_data: R,
    },
    Aborted,
}

pub struct TransactionAttempt<'pool> {
    connection: &'pool mut QldbHttp2Connection,
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
}

impl<'pool> TransactionAttempt<'pool> {
    pub(crate) async fn start(
        connection: &'pool mut QldbHttp2Connection,
    ) -> Result<TransactionAttempt<'pool>, QldbError> {
        let mut accumulated_execution_stats = ExecutionStats::default();
        let resp = connection
            .send_streaming_command(CommandStream::StartTransaction(
                StartTransactionRequest::builder().build(),
            ))
            .await
            .map_err(|_| error::todo_stable_error_api())?;
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
        })
    }

    pub fn statement<S>(&mut self, statement: S) -> StatementBuilder<'pool, '_>
    where
        S: Into<String>,
    {
        StatementBuilder::new(self, statement.into())
    }

    /// Send a statement without any parameters. For example, this could be used
    /// to create a table where the name is already sanitized.
    pub async fn execute_statement<S>(&mut self, partiql: S) -> Result<StatementResults, QldbError>
    where
        S: Into<String>,
    {
        self.statement(partiql).execute().await
    }

    // FIXME: don't buffer all results
    async fn execute_statement_internal(
        &mut self,
        statement: Statement,
    ) -> QldbResult<StatementResults> {
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
            .map_err(|_| error::todo_stable_error_api())?;

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
                        .map_err(|_| error::todo_stable_error_api())?;

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

    pub async fn commit<R>(mut self, user_data: R) -> Result<TransactionAttemptResult<R>> {
        debug!(id = &self.id[..], "transaction will be committed");
        let resp = self
            .connection
            .send_streaming_command(CommandStream::CommitTransaction(
                CommitTransactionRequest::builder()
                    .transaction_id(&self.id)
                    .build(),
            ))
            .await
            .map_err(|_| error::todo_stable_error_api())?;

        let commit_result = match resp {
            ResultStream::CommitTransaction(it) => it,
            it => Err(error::unexpected_response("CommitTransaction", it))?,
        };

        // If we get a successful commit, check some invariants. Otherwise, the
        // error must be handled by the caller. In most cases, this should be
        // the driver which may retry the transaction.
        if let Some(ref id) = commit_result.transaction_id {
            if id != &self.id {
                Err(error::illegal_state(format!(
                    "transaction {} response returned a different id: {:#?}",
                    self.id, id,
                )))?
            }
        }

        self.accumulated_execution_stats.accumulate(&commit_result);

        Ok(TransactionAttemptResult::Committed {
            commit_execution_stats: ExecutionStats::from_api(commit_result),
            user_data,
        })
    }

    // Always returns `Ok` even though the signature says `Result`. This is to
    // keep the type consistent with `commit`.
    pub async fn abort<R>(mut self) -> Result<TransactionAttemptResult<R>> {
        debug!(id = &self.id[..], "transaction will be aborted");
        let resp = self
            .connection
            .send_streaming_command(CommandStream::AbortTransaction(
                AbortTransactionRequest::builder().build(),
            ))
            .await
            .map_err(|_| error::todo_stable_error_api())?;

        let abort_result = match resp {
            ResultStream::AbortTransaction(it) => it,
            it => Err(error::unexpected_response("AbortTransaction", it))?,
        };

        // TODO: Should we ignore abort failures?
        self.accumulated_execution_stats.accumulate(&abort_result);

        Ok(TransactionAttemptResult::Aborted)
    }
}

pub struct StatementBuilder<'pool, 'tx> {
    attempt: &'tx mut TransactionAttempt<'pool>,
    statement: Statement,
}

impl<'pool, 'tx> StatementBuilder<'pool, 'tx> {
    fn new(
        attempt: &'tx mut TransactionAttempt<'pool>,
        partiql: String,
    ) -> StatementBuilder<'pool, 'tx> {
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
    fn param<B>(mut self, param: B) -> StatementBuilder<'pool, 'tx>
    where
        B: Into<Bytes>,
    {
        self.statement.params.push(param.into());
        self
    }

    async fn execute(self) -> QldbResult<StatementResults> {
        let StatementBuilder { attempt, statement } = self;
        attempt.execute_statement_internal(statement).await
    }
}

struct Statement {
    partiql: String,
    params: Vec<Bytes>,
}
