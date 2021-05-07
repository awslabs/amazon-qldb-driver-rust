use crate::QldbError;
use async_trait::async_trait;
use bytes::Bytes;
use rusoto_qldb_session::*;
use tracing::debug;

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

        debug!(?request, command = "abort_transaction", "request");
        let response = self.send_command(request).await?;
        debug!(?response);

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

        debug!(?request, command = "commit_transaction", "request");
        let response = self.send_command(request).await?;
        debug!(?response);

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

        debug!(?request, command = "end_session", "request");
        let response = self.send_command(request).await?;
        debug!(?response);

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
                statement,
                parameters: None,
            }),
            ..Default::default()
        };

        debug!(?request, command = "execute_statement", "request");
        let response = self.send_command(request).await?;
        debug!(?response);

        let inner = response
            .execute_statement
            .ok_or(QldbError::UnexpectedResponse(
                "ExecuteTransaction requests should return ExecuteTransaction responses".into(),
            ))?;

        Ok(inner)
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
                next_page_token,
                transaction_id: transaction_id.clone(),
            }),
            ..Default::default()
        };

        debug!(?request, command = "fetch_page", "request");
        let response = self.send_command(request).await?;
        debug!(?response);

        let inner = response.fetch_page.ok_or(QldbError::UnexpectedResponse(
            "FetchPage requests should return FetchPage responses".into(),
        ))?;

        Ok(inner)
    }

    async fn start_session(&self, ledger_name: String) -> Result<SessionToken, QldbError> {
        let request = SendCommandRequest {
            start_session: Some(StartSessionRequest { ledger_name }),
            ..Default::default()
        };

        debug!(?request, command = "start_session", "request");
        let response = self.send_command(request).await?;
        debug!(?response);

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

        debug!(?request, command = "start_transaction", "request");
        let response = self.send_command(request).await?;
        debug!(?response);

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
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    /// A mock implementation of [`QldbSession`]. The client can be cloned to
    /// retain access to the testing functionality, even after the instance is
    /// handed to a [`QldbDriver`].
    ///
    /// The client generates default responses (literally using the [`Default`]
    /// trait), but provides a facility to override the response per request.
    ///
    /// Creating the request matchers and response types is a little tedious, so
    /// we also provide some factory methods to make things easier.
    #[derive(Clone)]
    pub struct TestQldbSessionClient {
        inner: Arc<TestQldbSessionClientInner>,
    }

    /// This typealias gives a name to something that looks a little scary.
    /// Essentially, this holds overrides to the default responses.
    ///
    /// Probably want a map in the future so it's easy to update responses, but
    /// that requires dealing with the fact that the rusoto types don't
    /// implement Hash.
    type RequestResponseStore = Vec<(
        SendCommandRequest,
        Result<SendCommandResult, RusotoError<SendCommandError>>,
    )>;

    pub fn build_response(
        req: SendCommandRequest,
    ) -> Result<SendCommandResult, RusotoError<SendCommandError>> {
        match req {
            SendCommandRequest {
                abort_transaction: Some(abort),
                ..
            } => Ok(build_abort_transaction_response(abort)),
            SendCommandRequest {
                commit_transaction: Some(commit),
                ..
            } => Ok(build_commit_transaction_response(commit)),
            SendCommandRequest {
                end_session: Some(end),
                ..
            } => Ok(build_end_transaction_response(end)),
            SendCommandRequest {
                execute_statement: Some(execute),
                ..
            } => Ok(build_execute_statement_response(execute)),
            SendCommandRequest {
                fetch_page: Some(fetch_page),
                ..
            } => Ok(build_fetch_page_response(fetch_page)),
            SendCommandRequest {
                start_session: Some(start_session),
                ..
            } => Ok(build_start_session_response(start_session)),
            SendCommandRequest {
                start_transaction: Some(start_transaction),
                ..
            } => Ok(build_start_transaction_response(start_transaction)),
            _ => unreachable!(),
        }
    }

    pub fn build_abort_transaction_response(_req: AbortTransactionRequest) -> SendCommandResult {
        SendCommandResult {
            abort_transaction: Some(AbortTransactionResult {
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn build_commit_transaction_response(req: CommitTransactionRequest) -> SendCommandResult {
        SendCommandResult {
            commit_transaction: Some(CommitTransactionResult {
                transaction_id: Some(req.transaction_id),
                commit_digest: Some(req.commit_digest),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn build_end_transaction_response(_req: EndSessionRequest) -> SendCommandResult {
        SendCommandResult {
            end_session: Some(EndSessionResult {
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn build_execute_statement_response(_req: ExecuteStatementRequest) -> SendCommandResult {
        SendCommandResult {
            execute_statement: Some(ExecuteStatementResult {
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn build_fetch_page_response(_req: FetchPageRequest) -> SendCommandResult {
        SendCommandResult {
            fetch_page: Some(FetchPageResult {
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn build_start_session_response(req: StartSessionRequest) -> SendCommandResult {
        let now = SystemTime::now();

        SendCommandResult {
            start_session: Some(StartSessionResult {
                session_token: Some(format!(
                    "session-{}-{}",
                    req.ledger_name,
                    now.duration_since(UNIX_EPOCH).unwrap().as_millis()
                )),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn build_start_transaction_response(_req: StartTransactionRequest) -> SendCommandResult {
        let now = SystemTime::now();

        SendCommandResult {
            start_transaction: Some(StartTransactionResult {
                transaction_id: Some(format!(
                    "transaction-{}",
                    now.duration_since(UNIX_EPOCH).unwrap().as_millis()
                )),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    // This is not a very good mock, but it'll do. Each request variant has a
    // queue of responses.
    //
    // We use a Mutex for interior mutability, since `send_command` takes
    // `&self`.
    struct TestQldbSessionClientInner {
        queue: Mutex<RequestResponseStore>,
    }

    impl TestQldbSessionClient {
        pub fn new() -> TestQldbSessionClient {
            TestQldbSessionClient {
                inner: Arc::new(TestQldbSessionClientInner {
                    queue: Mutex::new(Vec::new()),
                }),
            }
        }

        pub fn respond(
            &mut self,
            req: SendCommandRequest,
            with: Result<SendCommandResult, RusotoError<SendCommandError>>,
        ) {
            let mut queue = self.inner.queue.lock().unwrap();
            queue.push((req, with));
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
            req: SendCommandRequest,
        ) -> Result<SendCommandResult, RusotoError<SendCommandError>> {
            let mut queue = self.inner.queue.lock().unwrap();
            match queue.iter().position(|it| it.0 == req) {
                Some(i) => {
                    let res = queue.remove(i);
                    res.1
                }
                None => build_response(req),
            }
        }
    }
}
