use std::sync::Arc;

use async_trait::async_trait;
use aws_hyper::{Client, DynConnector, SmithyConnector};
use aws_sdk_qldbsession::{
    error::SendCommandError, input::SendCommandInput, model::*, output::SendCommandOutput, Blob,
    Config, SdkError,
};
use bytes::Bytes;
use tracing::debug;

use crate::error::{QldbError, QldbResult};

/// The AWS SDK for Rust does not provide a trait abstraction over the API, so
/// we provide one here.
#[async_trait]
pub trait QldbSession {
    async fn send_command(
        &self,
        input: SendCommandInput,
    ) -> Result<SendCommandOutput, SdkError<SendCommandError>>;
}

#[derive(Clone)]
pub struct QldbSessionSdk<C = DynConnector> {
    inner: Arc<QldbSessionSdkInner<C>>,
}

struct QldbSessionSdkInner<C = DynConnector> {
    client: Client<C>,
    conf: Config,
}

impl<C> QldbSessionSdk<C> {
    pub(crate) fn new(client: Client<C>, conf: Config) -> QldbSessionSdk<C> {
        let inner = QldbSessionSdkInner { client, conf };
        QldbSessionSdk {
            inner: Arc::new(inner),
        }
    }
}

#[async_trait]
impl<C> QldbSession for QldbSessionSdk<C>
where
    C: SmithyConnector,
{
    async fn send_command(
        &self,
        input: SendCommandInput,
    ) -> Result<SendCommandOutput, SdkError<SendCommandError>> {
        let op = input
            .make_operation(&self.inner.conf)
            .await
            .map_err(|err| SdkError::ConstructionFailure(err.into()))?;
        self.inner.client.call(op).await
    }
}

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
    ) -> QldbResult<AbortTransactionResult>;

    async fn commit_transaction(
        &self,
        session_token: &SessionToken,
        transaction_id: TransactionId,
        commit_digest: bytes::Bytes,
    ) -> QldbResult<CommitTransactionResult>;

    async fn end_session(&self, session_token: SessionToken) -> QldbResult<()>;

    // FIXME: params, result
    async fn execute_statement(
        &self,
        session_token: &SessionToken,
        transaction_id: &TransactionId,
        statement: String,
    ) -> QldbResult<ExecuteStatementResult>;

    async fn fetch_page(
        &self,
        session_token: &SessionToken,
        transaction_id: &TransactionId,
        next_page_token: String,
    ) -> QldbResult<FetchPageResult>;

    async fn start_session(&self, ledger_name: String) -> QldbResult<SessionToken>;

    async fn start_transaction(
        &self,
        session_token: &SessionToken,
    ) -> QldbResult<StartTransactionResult>;
}

#[async_trait]
impl<C> QldbSessionApi for C
where
    C: QldbSession + Send + Sync,
{
    async fn abort_transaction(
        &self,
        session_token: &SessionToken,
    ) -> QldbResult<AbortTransactionResult> {
        let request = SendCommandInput::builder()
            .session_token(session_token.clone())
            .abort_transaction(AbortTransactionRequest::builder().build())
            .build()?;

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
    ) -> QldbResult<CommitTransactionResult> {
        let request = SendCommandInput::builder()
            .session_token(session_token.clone())
            .commit_transaction(
                CommitTransactionRequest::builder()
                    .transaction_id(transaction_id.clone())
                    .commit_digest(Blob::new(&commit_digest[..]))
                    .build(),
            )
            .build()?;

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

        if &commit_digest[..] != remote_commit_digest.as_ref() {
            return Err(QldbError::UnexpectedResponse(format!(
                r#"The commit digest we sent did not match the response sent by the server, this should never happen.
 The digest we sent was {:?}, the server responded with {:?}."#,
                commit_digest, remote_commit_digest
            )));
        }

        Ok(committed)
    }

    async fn end_session(&self, session_token: SessionToken) -> QldbResult<()> {
        let request = SendCommandInput::builder()
            .session_token(session_token.clone())
            .end_session(EndSessionRequest::builder().build())
            .build()?;

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
    ) -> QldbResult<ExecuteStatementResult> {
        let request = SendCommandInput::builder()
            .session_token(session_token.clone())
            .execute_statement(
                ExecuteStatementRequest::builder()
                    .transaction_id(transaction_id.clone())
                    .statement(statement)
                    .build(),
            )
            .build()?;

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
    ) -> QldbResult<FetchPageResult> {
        let request = SendCommandInput::builder()
            .session_token(session_token.clone())
            .fetch_page(
                FetchPageRequest::builder()
                    .next_page_token(next_page_token)
                    .transaction_id(transaction_id.clone())
                    .build(),
            )
            .build()?;

        debug!(?request, command = "fetch_page", "request");
        let response = self.send_command(request).await?;
        debug!(?response);

        let inner = response.fetch_page.ok_or(QldbError::UnexpectedResponse(
            "FetchPage requests should return FetchPage responses".into(),
        ))?;

        Ok(inner)
    }

    async fn start_session(&self, ledger_name: String) -> QldbResult<SessionToken> {
        let request = SendCommandInput::builder()
            .start_session(
                StartSessionRequest::builder()
                    .ledger_name(ledger_name)
                    .build(),
            )
            .build()?;

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
    ) -> QldbResult<StartTransactionResult> {
        let request = SendCommandInput::builder()
            .session_token(session_token.clone())
            .start_transaction(StartTransactionRequest::builder().build())
            .build()?;

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
