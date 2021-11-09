use async_stream::try_stream;
use aws_sdk_qldbsessionv2::model::{
    CommandStream, ExecuteStatementRequest, FetchPageRequest, ResultStream,
};
use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;
use ion_c_sys::reader::IonCReaderHandle;
use ion_c_sys::result::IonCError;
use std::convert::TryFrom;
use std::pin::Pin;

use crate::error::{self, TransactError};
use crate::execution_stats::{ExecutionStats, HasExecutionStats};
use crate::transaction::Statement;
use crate::TransactionAttempt;

// Implements the client side behavior of statement execution.
//
// A statement has a PartiQL string and a list of bind params, e.g.:
//
// ```txt
// PartiQL = select * from users where id = ?
// params  = [IonInt(1)]
// ```
//
// The statement and params are sent off to the server, which will return either
// an error or an intial page of results. Error conditions include:
//
// 1. The transaction has failed in some way
// 2. The PartiQL is invalid in some way
// 3. One or more of the parameters is not valid
// 4. IO errors
//
// If the statement executes correctly, the service will return paginated
// results and some statistics. The "execution stats" include server-side timing
// information as well as billing information (such as how many read IOs were
// consumed).
//
// Pagination works by return pages of results and a token that can be used to
// fetch more results. Each call to fetch page will return more statistics.
// These statistics are accumulated in memory, and can be accessed via the
// `execution_stats` method. Note that the observed values will change as more
// results are fetched (but remain constant until a page boundary is hit).
//
// [`StatementResults`] implements the [`Stream`] trait (an asynchronous
// iterator). For small result sets, it may be simpler to use the [`buffered`]
// API to drag all the results into memory.
//
// Note that usually, in Rust, both iterators and futures do nothing unless
// polled. Consider this usage:
//
// ```
// let results = execute_statement_paginated(transaction, statement /* fetches loads of results /*).await?;
// results.next().await?;
// ```
//
// The first line executes the statement and returns the intial page from the
// server. This should be intuitively obvious: the future was polled, and thus
// any side-effects of the statement must have run. What's less obvious (and not
// idiomatic in Rust) is that the intial page is already fetch. That is, the
// first N calls to `results.next()` won't do any IO (or, put another way, will
// return `Poll::Ready` immediately). This is simply the way the service API
// works (reduces a roundtrip to fetch the first page).
pub(crate) async fn execute_statement_paginated<E>(
    transaction: &mut TransactionAttempt<E>,
    statement: Statement,
) -> Result<StatementResults<'_, E>, TransactError<E>>
where
    E: std::error::Error + 'static,
{
    let mut execution_stats = ExecutionStats::default();
    let resp = transaction
        .send_streaming_command(CommandStream::ExecuteStatement(
            ExecuteStatementRequest::builder()
                .transaction_id(&transaction.id)
                .statement(&statement.partiql)
                .build(),
        ))
        .await?;

    let execute_result = match resp {
        ResultStream::ExecuteStatement(it) => it,
        it => Err(error::unexpected_response("ExecuteStatement", it))?,
    };
    execution_stats.accumulate(&execute_result);

    let stream = try_stream! {
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
                    yield YieldNext::Value(Bytes::from(bytes.into_inner()));
                }

                if let Some(next_page_token) = page.next_page_token {
                    let resp = transaction
                        .send_streaming_command(CommandStream::FetchPage(
                            FetchPageRequest::builder()
                                .transaction_id(&transaction.id)
                                .next_page_token(&next_page_token)
                                .build(),
                        ))
                        .await?;

                    let fetch_page_result = match resp {
                        ResultStream::FetchPage(it) => it,
                        it => Err(error::unexpected_response("FetchPage", it))?,
                    };

                    yield YieldNext::Stats(fetch_page_result.extract_owned());

                    if let Some(p) = fetch_page_result.page {
                        current.replace(p);
                    }
                }
            }
        }
    };

    // TODO: Need to do this
    // self.accumulated_execution_stats
    //     .accumulate(&execution_stats);

    Ok(StatementResults::new(stream, execution_stats))
}

enum YieldNext {
    Value(Bytes),
    Stats(ExecutionStats),
}

/// The results of executing a statement.
///
/// A statement may return may pages of results. This type represents pulling
/// all of those pages into memory. As such, this type represents reading all
/// results (it will never be constructed with partial results).
///
/// [`cumulative_timing_information`] and [`cumulative_io_usage`] represent the
/// sum of server reported timing and IO usage across all pages that were
/// fetched.
pub struct StatementResults<'tx, E>
where
    E: std::error::Error + 'static,
{
    stream: Pin<Box<dyn Stream<Item = Result<YieldNext, TransactError<E>>> + 'tx>>,
    execution_stats: ExecutionStats,
}

impl<'tx, E> StatementResults<'tx, E>
where
    E: std::error::Error + 'static,
{
    fn new(
        stream: impl Stream<Item = Result<YieldNext, TransactError<E>>> + 'tx,
        execution_stats: ExecutionStats,
    ) -> StatementResults<'tx, E> {
        StatementResults {
            stream: Box::pin(stream),
            execution_stats,
        }
    }

    pub fn execution_stats(&self) -> &ExecutionStats {
        &self.execution_stats
    }

    pub async fn buffered(mut self) -> Result<BufferedStatementResults, TransactError<E>> {
        let mut values = vec![];
        while let Some(it) = self.next().await {
            values.push(it?)
        }

        Ok(BufferedStatementResults { values })
    }
}

/// Asynchronously stream results from QLDB. The base usage looks like this:
///
/// ```no_run
/// while let Some(result) = results.next().await {
///     let value = result?;
///     // process the raw bytes
/// }
/// ```
///
/// Higher level abstractions or combinators can be used to make this more
/// ergonomic, such as deserializing all values into structs.
impl<'tx, E> Stream for StatementResults<'tx, E>
where
    E: std::error::Error + 'static,
{
    type Item = Result<Bytes, TransactError<E>>;

    // This implementation does two things. First, it delegates `poll_next` to
    // the underlying stream of values. That's the easy part! If the stream is
    // not ready, or returns an error or is complete, then so too are we.
    //
    // However, not all iterations return values: some return statistics. When
    // we encounter these, we accumulate the stats and redrive the call to
    // `next` under the covers.
    //
    // QLDB returns a page of results, accompanying stats and an optional next
    // page token. So, in theory, we should only ever have to call `next` one or
    // two times (the latter being the case where we got stats). However, a
    // correct pagination implementation must handle empty pages of results (0
    // values, some stats, some next page). Thus, we implement the inner polling
    // as a loop. The only branch that doesn't break (return) is the stats one.
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let pointer = unsafe { Pin::get_unchecked_mut(self) };
        loop {
            match pointer.stream.poll_next_unpin(cx) {
                std::task::Poll::Ready(ready) => match ready {
                    Some(next) => match next {
                        Ok(ok) => match ok {
                            YieldNext::Value(bytes) => {
                                return std::task::Poll::Ready(Some(Ok(bytes)))
                            }
                            YieldNext::Stats(stats) => {
                                pointer.execution_stats.accumulate(&stats);
                            }
                        },
                        Err(e) => return std::task::Poll::Ready(Some(Err(e))),
                    },
                    None => return std::task::Poll::Ready(None),
                },
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}

pub struct BufferedStatementResults {
    values: Vec<Bytes>,
}

impl BufferedStatementResults {
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.values.iter().map(|bytes| bytes.as_ref())
    }

    pub fn readers(&self) -> impl Iterator<Item = Result<IonCReaderHandle, IonCError>> {
        self.values
            .iter()
            .map(|bytes| IonCReaderHandle::try_from(&bytes[..]))
    }
}
