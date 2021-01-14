use crate::api::{QldbSessionApi, SessionToken, TransactionId};
use crate::ion_compat::ion_hash;
use crate::qldb_hash::QldbHash;
use crate::QldbError;
use bytes::Bytes;
use ion_c_sys::reader::IonCReaderHandle;
use ion_c_sys::result::IonCError;
use std::convert::TryFrom;
use std::error::Error as StdError;
use tokio::sync::mpsc::Sender;

pub struct StatementResults {
    values: Vec<Bytes>,
}

impl StatementResults {
    fn new(values: Vec<Bytes>) -> StatementResults {
        StatementResults { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn readers(&self) -> impl Iterator<Item = Result<IonCReaderHandle, IonCError>> {
        self.values
            .iter()
            .map(|bytes| IonCReaderHandle::try_from(&bytes[..]))
    }
}

pub enum TransactionDisposition {
    Commit,
    Abort,
}

pub struct TransactionAttempt<R> {
    pub(crate) tx_id: TransactionId,
    pub(crate) disposition: TransactionDisposition,
    pub(crate) user_data: R,
}

pub struct Transaction {
    client: Box<dyn QldbSessionApi>,
    session_token: SessionToken,
    pub tx_id: TransactionId,
    commit_digest: QldbHash,
    channel: Sender<QldbHash>,
}

impl Transaction {
    pub(crate) fn new(
        client: Box<dyn QldbSessionApi>,
        session_token: SessionToken,
        tx_id: TransactionId,
        channel: Sender<QldbHash>,
    ) -> Transaction {
        let seed_hash = ion_hash(&tx_id);
        let commit_digest = QldbHash::from_bytes(seed_hash).unwrap();
        Transaction {
            client,
            session_token,
            tx_id,
            commit_digest,
            channel,
        }
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

        let first_page = self
            .client
            .execute_statement(&self.session_token, &self.tx_id, statement.clone())
            .await?;

        let statement_hash = QldbHash::from_bytes(ion_hash(&statement)).unwrap();
        self.commit_digest = self.commit_digest.dot(&statement_hash);

        let mut values = vec![];
        let mut current = first_page;
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
                    let page = self
                        .client
                        .fetch_page(&self.session_token, &self.tx_id, next_page_token)
                        .await?;

                    if let Some(p) = page {
                        current.replace(p);
                    }
                }
            }
        }

        Ok(StatementResults::new(values))
    }

    pub async fn ok<R>(self, user_data: R) -> Result<TransactionAttempt<R>, Box<dyn StdError>> {
        self.channel.send(self.commit_digest).await?;

        Ok(TransactionAttempt {
            tx_id: self.tx_id,
            disposition: TransactionDisposition::Commit,
            user_data: user_data,
        })
    }

    pub async fn abort<R>(self, user_data: R) -> Result<TransactionAttempt<R>, Box<dyn StdError>> {
        Ok(TransactionAttempt {
            tx_id: self.tx_id,
            disposition: TransactionDisposition::Abort,
            user_data: user_data,
        })
    }
}
