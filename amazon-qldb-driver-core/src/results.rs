use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;
use ion_c_sys::reader::IonCReaderHandle;
use ion_c_sys::result::IonCError;
use std::convert::TryFrom;
use std::pin::Pin;

use crate::error::TransactError;
use crate::execution_stats::ExecutionStats;

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
    stream: Pin<Box<dyn Stream<Item = Result<Bytes, TransactError<E>>> + 'tx>>,
    execution_stats: ExecutionStats,
}

impl<'tx, E> StatementResults<'tx, E>
where
    E: std::error::Error + 'static,
{
    pub(crate) fn new(
        stream: impl Stream<Item = Result<Bytes, TransactError<E>>> + 'tx,
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

impl<'tx, E> Stream for StatementResults<'tx, E>
where
    E: std::error::Error + 'static,
{
    type Item = Result<Bytes, TransactError<E>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // TODO: consume execution stats
        let stream = unsafe { self.map_unchecked_mut(|s| &mut s.stream) };
        stream.poll_next(cx)
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
