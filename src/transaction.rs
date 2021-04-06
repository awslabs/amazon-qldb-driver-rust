use crate::ion_compat::ion_hash;
use crate::qldb_hash::QldbHash;
use crate::QldbError;
use crate::{
    api::{QldbSessionApi, SessionToken, TransactionId},
    execution_stats::ExecutionStats,
};
use anyhow::Result;
use bytes::Bytes;
use ion_c_sys::reader::IonCReaderHandle;
use ion_c_sys::result::IonCError;
use std::convert::TryFrom;
use tokio::sync::mpsc::{self, Receiver, Sender};

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
    pub(crate) tx_id: TransactionId,
    pub(crate) disposition: TransactionDisposition,
    pub(crate) execution_stats: ExecutionStats,
    pub(crate) user_data: R,
}

pub struct TransactionAttempt<C: QldbSessionApi + Send> {
    client: C,
    session_token: SessionToken,
    pub id: TransactionId,
    commit_digest: QldbHash,
    channel: Sender<QldbHash>,
    execution_stats: ExecutionStats,
}

impl<C> TransactionAttempt<C>
where
    C: QldbSessionApi + Send,
{
    pub(crate) async fn start(
        client: C,
        session_token: SessionToken,
    ) -> Result<(TransactionAttempt<C>, Receiver<QldbHash>), QldbError> {
        let mut execution_stats = ExecutionStats::default();
        let start_result = client.start_transaction(&session_token).await?;
        execution_stats.accumulate(&start_result);
        let id = start_result
            .transaction_id
            .ok_or(QldbError::UnexpectedResponse(
                "StartTransaction should always return a transaction_id".into(),
            ))?;

        let (sender, receiver) = mpsc::channel(1);
        let seed_hash = ion_hash(&id);
        let commit_digest = QldbHash::from_bytes(seed_hash).unwrap();
        let transaction = TransactionAttempt {
            client,
            session_token,
            id,
            commit_digest,
            channel: sender,
            execution_stats,
        };
        Ok((transaction, receiver))
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
            .execute_statement(&self.session_token, &self.id, statement.clone())
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
                        .fetch_page(&self.session_token, &self.id, next_page_token)
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

    pub async fn commit<R>(self, user_data: R) -> Result<TransactionOutcome<R>> {
        self.channel.send(self.commit_digest).await?;

        Ok(TransactionOutcome {
            tx_id: self.id,
            disposition: TransactionDisposition::Commit,
            execution_stats: self.execution_stats,
            user_data,
        })
    }

    pub async fn abort<R>(self, user_data: R) -> Result<TransactionOutcome<R>> {
        Ok(TransactionOutcome {
            tx_id: self.id,
            disposition: TransactionDisposition::Abort,
            execution_stats: self.execution_stats,
            user_data,
        })
    }
}
