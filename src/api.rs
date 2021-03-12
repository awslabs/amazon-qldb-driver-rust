use crate::QldbError;
use async_trait::async_trait;
use bytes::Bytes;
use rusoto_qldb_session::*;

pub type SessionToken = String;
pub type TransactionId = String;

/// An abstraction over the QLDBSession API, for the driver to use. It
/// is not expected that end-users use this trait. Rather, they should
/// use the abstractions provided by the [`QldbDriver`].
///
/// There are two reasons for this abstraction. First,
/// decoupling. There is no reason to tightly couple to any particular
/// SDK (currently we only support Rusoto). Rust offers zero-cost
/// abstractions, so indirection costs very little.
///
/// Second, usability. The session API has the notion of commands that
/// are sent back and forth, 1 command per request. This is done to
/// simplify permissions [1] at the wire level, but requires more
/// careful use by application code: e.g. only 1 command variant can
/// be sent per request. This abstraction exposes APIs that correctly
/// use the underlying transport.
///
/// [1] QLDB uses PartiQL statements to read/mutate the database
/// (e.g. a SELECT or INSERT statement). Thus, there is very little
/// value in having fine-grained Actions (such as a
/// StartTransactionRequest), because the only meaningful set of
/// permissions is "all actions". Consider being able to start a
/// session but not start a transaction - not very useful!
#[async_trait]
pub trait QldbSessionApi {
    async fn abort_transaction(
        &self,
        session_token: &SessionToken,
    ) -> Result<AbortTransactionResult, QldbError>;

    async fn commit_transaction(
        &self,
        session_token: &SessionToken,
        transaction_id: TransactionId,
        commit_digest: bytes::Bytes,
    ) -> Result<CommitTransactionResult, QldbError>;
    async fn end_session(&self, session_token: SessionToken) -> Result<(), QldbError>;

    // FIXME: params, result
    async fn execute_statement(
        &self,
        session_token: &SessionToken,
        transaction_id: &TransactionId,
        statement: String,
    ) -> Result<ExecuteStatementResult, QldbError>;

    async fn fetch_page(
        &self,
        session_token: &SessionToken,
        transaction_id: &TransactionId,
        next_page_token: String,
    ) -> Result<FetchPageResult, QldbError>;

    async fn start_session(&self, ledger_name: String) -> Result<SessionToken, QldbError>;

    async fn start_transaction(
        &self,
        session_token: &SessionToken,
    ) -> Result<StartTransactionResult, QldbError>;
}

#[async_trait]
impl<C> QldbSessionApi for C
where
    C: QldbSession + Send + Sync,
{
    async fn abort_transaction(
        &self,
        session_token: &SessionToken,
    ) -> Result<AbortTransactionResult, QldbError> {
        let request = SendCommandRequest {
            session_token: Some(session_token.clone()),
            abort_transaction: Some(AbortTransactionRequest {}),
            ..Default::default()
        };

        debug!("request: abort_transaction {:?}", request);
        let response = self.send_command(request).await?;

        Ok(response
            .abort_transaction
            .ok_or(QldbError::UnexpectedResponse(
                "AbortTransaction requests should return AbortTransaction responses".into(),
            ))?)
    }

    async fn commit_transaction(
        &self,
        session_token: &SessionToken,
        transaction_id: TransactionId,
        commit_digest: Bytes,
    ) -> Result<CommitTransactionResult, QldbError> {
        let request = SendCommandRequest {
            session_token: Some(session_token.clone()),
            commit_transaction: Some(CommitTransactionRequest {
                transaction_id: transaction_id.clone(),
                commit_digest: commit_digest.clone(),
            }),
            ..Default::default()
        };

        debug!("request: commit_transaction {:?}", request);
        let response = self.send_command(request).await?;

        let committed = response
            .commit_transaction
            .ok_or(QldbError::UnexpectedResponse(
                "CommitTransaction requests should return CommitTransaction responses".into(),
            ))?;

        let remote_transaction_id =
            committed
                .transaction_id
                .as_ref()
                .ok_or(QldbError::UnexpectedResponse(
                    "CommitTransaction should always return a transaction_id".into(),
                ))?;

        if &transaction_id != remote_transaction_id {
            return Err(QldbError::UnexpectedResponse(format!(
                r#"The committed transaction id did not match our transaction id, this should never happen.
 The transaction we we committed was {:?}, the server responded with {:?}."#,
                transaction_id, remote_transaction_id
            )));
        }

        let remote_commit_digest =
            committed
                .commit_digest
                .as_ref()
                .ok_or(QldbError::UnexpectedResponse(
                    "CommitTransaction should always return a commit_digest".into(),
                ))?;

        if commit_digest != remote_commit_digest {
            return Err(QldbError::UnexpectedResponse(format!(
                r#"The commit digest we sent did not match the response sent by the server, this should never happen.
 The digest we sent was {:?}, the server responded with {:?}."#,
                commit_digest, remote_commit_digest
            )));
        }

        Ok(committed)
    }

    async fn end_session(&self, session_token: SessionToken) -> Result<(), QldbError> {
        let request = SendCommandRequest {
            session_token: Some(session_token.clone()),
            end_session: Some(EndSessionRequest {}),
            ..Default::default()
        };

        debug!("request: end_session {:?}", request);
        let response = self.send_command(request).await?;

        response.end_session.ok_or(QldbError::UnexpectedResponse(
            "EndSession requests should return EndSession responses".into(),
        ))?;

        Ok(())
    }

    // FIXME: params, result
    async fn execute_statement(
        &self,
        session_token: &SessionToken,
        transaction_id: &TransactionId,
        statement: String,
    ) -> Result<ExecuteStatementResult, QldbError> {
        let request = SendCommandRequest {
            session_token: Some(session_token.clone()),
            execute_statement: Some(ExecuteStatementRequest {
                transaction_id: transaction_id.clone(),
                statement: statement,
                parameters: None,
            }),
            ..Default::default()
        };

        debug!("request: execute_statement {:?}", request);
        let response = self.send_command(request).await?.execute_statement.ok_or(
            QldbError::UnexpectedResponse(
                "ExecuteTransaction requests should return ExecuteTransaction responses".into(),
            ),
        )?;

        Ok(response)
    }

    // FIXME: dont eat the page
    async fn fetch_page(
        &self,
        session_token: &SessionToken,
        transaction_id: &TransactionId,
        next_page_token: String,
    ) -> Result<FetchPageResult, QldbError> {
        let request = SendCommandRequest {
            session_token: Some(session_token.clone()),
            fetch_page: Some(FetchPageRequest {
                next_page_token: next_page_token,
                transaction_id: transaction_id.clone(),
            }),
            ..Default::default()
        };

        debug!("request: fetch_page {:?}", request);
        let response =
            self.send_command(request)
                .await?
                .fetch_page
                .ok_or(QldbError::UnexpectedResponse(
                    "FetchPage requests should return FetchPage responses".into(),
                ))?;

        Ok(response)
    }

    async fn start_session(&self, ledger_name: String) -> Result<SessionToken, QldbError> {
        let request = SendCommandRequest {
            start_session: Some(StartSessionRequest {
                ledger_name: ledger_name,
            }),
            ..Default::default()
        };

        debug!("request: start_session {:?}", request);
        let response = self.send_command(request).await?;

        response
            .start_session
            .ok_or(QldbError::UnexpectedResponse(
                "StartSession requests should return StartSession responses".into(),
            ))?
            .session_token
            .ok_or(QldbError::UnexpectedResponse(
                "StartSession should always return a session_token".into(),
            ))
    }

    async fn start_transaction(
        &self,
        session_token: &SessionToken,
    ) -> Result<StartTransactionResult, QldbError> {
        let request = SendCommandRequest {
            session_token: Some(session_token.clone()),
            start_transaction: Some(StartTransactionRequest {}),
            ..Default::default()
        };

        debug!("request: start_transaction {:?}", request);
        let response = self.send_command(request).await?;

        Ok(response
            .start_transaction
            .ok_or(QldbError::UnexpectedResponse(
                "StartTransaction requests should return StartTransaction responses".into(),
            ))?)
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use rusoto_core::RusotoError;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    #[derive(Clone)]
    pub struct TestQldbSessionClient {
        inner: Arc<TestQldbSessionClientInner>,
    }

    // This is not a very good mock, but it'll do. Each request variant has a
    // queue of responses.
    //
    // We use a Mutex for interior mutability, since `send_command` takes
    // `&self`.
    struct TestQldbSessionClientInner {
        queue:
            Mutex<HashMap<String, Vec<Result<SendCommandResult, RusotoError<SendCommandError>>>>>,
    }

    impl TestQldbSessionClient {
        pub fn new() -> TestQldbSessionClient {
            TestQldbSessionClient {
                inner: Arc::new(TestQldbSessionClientInner {
                    queue: Mutex::new(HashMap::new()),
                }),
            }
        }

        pub fn respond<S: Into<String>>(
            &mut self,
            variant: S,
            with: Result<SendCommandResult, RusotoError<SendCommandError>>,
        ) {
            let mut queue = self.inner.queue.lock().unwrap();
            let entry = queue.entry(variant.into()).or_insert(vec![]);
            entry.insert(0, with);
        }
    }

    impl Default for TestQldbSessionClient {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl QldbSession for TestQldbSessionClient {
        async fn send_command(
            &self,
            input: SendCommandRequest,
        ) -> Result<SendCommandResult, RusotoError<SendCommandError>> {
            let mut queue = self.inner.queue.lock().unwrap();
            let variant = match input {
                SendCommandRequest {
                    start_session: Some(_),
                    ..
                } => "StartSession",
                SendCommandRequest {
                    start_transaction: Some(_),
                    ..
                } => "StartTransaction",
                SendCommandRequest {
                    commit_transaction: Some(_),
                    ..
                } => "CommitTransaction",
                _ => todo!(),
            };

            match queue.get_mut(variant).map(|v| v.pop()) {
                Some(Some(response)) => response,
                _ => panic!("Invalid usage of TestQldbSessionClient (no response was prepared). Request follows:\n{:#?}", input),
            }
        }
    }
}
