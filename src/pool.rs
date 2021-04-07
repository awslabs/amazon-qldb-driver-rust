use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use crate::api::{QldbSessionApi, SessionToken, TransactionId};
use crate::QldbError;
use async_trait::async_trait;
use bb8::{ErrorSink, ManageConnection};
use bytes::Bytes;
use futures::Future;
use rusoto_core::RusotoError;
use rusoto_qldb_session::*;

pub struct QldbSessionManager<C>
where
    C: QldbSession + Send + Sync + Clone,
{
    pub client: C,
    pub ledger_name: String,
}

#[derive(Debug, Copy, Clone)]
pub struct QldbErrorLoggingErrorSink;

impl QldbErrorLoggingErrorSink {
    pub fn new() -> QldbErrorLoggingErrorSink {
        QldbErrorLoggingErrorSink {}
    }
}

impl ErrorSink<QldbError> for QldbErrorLoggingErrorSink {
    fn sink(&self, error: QldbError) {
        debug!("error in connection pool: {}", error);
    }

    fn boxed_clone(&self) -> Box<dyn ErrorSink<QldbError>> {
        Box::new(*self)
    }
}

/// A HTTP/1 based connection to QLDB. There is no one physical connection.
/// Rather, a connection is represented by a "session token" (a unique, opaque
/// string) that is passed as a parameter over an HTTP/1 request.
///
/// [`discard`] should be set to true if an API response ever indicates the
/// session is broken.
#[derive(Clone)]
pub struct QldbHttp1Connection<C>
where
    C: QldbSession + Send + Sync + Clone,
{
    pub(crate) inner: Arc<Mutex<QldbHttp1ConnectionInner<C>>>,
}

pub(crate) struct QldbHttp1ConnectionInner<C>
where
    C: QldbSession + Send + Sync + Clone,
{
    client: C,
    token: SessionToken,
    discard: bool,
}

impl<C> QldbHttp1Connection<C>
where
    C: QldbSession + Send + Sync + Clone,
{
    pub fn session_token(&self) -> SessionToken {
        if let Ok(g) = self.inner.lock() {
            g.token.clone()
        } else {
            // there is no code that can panic while holding the lock, so it
            // seems unreasonable to force the caller of this code to deal with
            // that case.
            unreachable!()
        }
    }

    pub(crate) fn notify_invalid(&self) {
        if let Ok(mut g) = self.inner.lock() {
            g.discard = true;
        }
    }

    /// Call the function `F(QldbSession)` ensuring that:
    ///
    /// 1. The call won't be made if the session is marked invalid
    /// 2. If the response says the session is invalid, it will be marked as such
    ///
    /// This is meant for internal use, see the impl block that wraps the API.
    async fn handle<T, F, Fut>(&self, f: F) -> Result<T, QldbError>
    where
        F: FnOnce(C) -> Fut,
        Fut: Future<Output = Result<T, QldbError>>,
    {
        let client = if let Ok(g) = self.inner.lock() {
            if g.discard {
                Err(QldbError::IllegalState(format!(
                    "session {} should have been discarded",
                    g.token
                )))?
            }

            // Note: the call to the function creates a future which must be
            // `Send`, which a `MutexGuard` isn't. So, we clone the underlying
            // client and move that into the closure. Rusoto clients have an
            // inner Arc, so this clone is really cheap.
            g.client.clone()
        } else {
            unreachable!()
        };

        let res = f(client).await;
        if let Err(QldbError::Rusoto(RusotoError::Service(SendCommandError::InvalidSession(_)))) =
            res
        {
            self.notify_invalid();
        }
        res
    }
}

#[async_trait]
impl<C> ManageConnection for QldbSessionManager<C>
where
    C: QldbSession + Send + Sync + Clone + 'static,
{
    type Connection = QldbHttp1Connection<C>;
    type Error = QldbError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let token = self.client.start_session(self.ledger_name.clone()).await?;
        Ok(QldbHttp1Connection {
            inner: Arc::new(Mutex::new(QldbHttp1ConnectionInner {
                client: self.client.clone(),
                token,
                discard: false,
            })),
        })
    }

    async fn is_valid(
        &self,
        _conn: &mut bb8::PooledConnection<'_, Self>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        if let Ok(g) = conn.inner.lock() {
            g.discard
        } else {
            true
        }
    }
}

/// Wraps each call to the [`QldbSessionApi`] with a call to [`self.handle`].
/// This ensures we call `self.notify_invalid` if we ever get an
/// `InvalidSession` response.
///
/// `StartSession` has additional checks.
#[async_trait]
impl<C> QldbSessionApi for QldbHttp1Connection<C>
where
    C: QldbSession + Send + Sync + Clone,
{
    async fn abort_transaction(
        &self,
        session_token: &SessionToken,
    ) -> Result<AbortTransactionResult, QldbError> {
        self.handle(|client| async move { client.abort_transaction(session_token).await })
            .await
    }

    async fn commit_transaction(
        &self,
        session_token: &SessionToken,
        transaction_id: TransactionId,
        commit_digest: Bytes,
    ) -> Result<CommitTransactionResult, QldbError> {
        self.handle(|client| async move {
            client
                .commit_transaction(session_token, transaction_id, commit_digest)
                .await
        })
        .await
    }

    async fn end_session(&self, session_token: SessionToken) -> Result<(), QldbError> {
        self.handle(|client| async move { client.end_session(session_token).await })
            .await
    }

    async fn execute_statement(
        &self,
        session_token: &SessionToken,
        transaction_id: &TransactionId,
        statement: String,
    ) -> Result<ExecuteStatementResult, QldbError> {
        self.handle(|client| async move {
            client
                .execute_statement(session_token, transaction_id, statement)
                .await
        })
        .await
    }

    async fn fetch_page(
        &self,
        session_token: &SessionToken,
        transaction_id: &TransactionId,
        next_page_token: String,
    ) -> Result<FetchPageResult, QldbError> {
        self.handle(|client| async move {
            client
                .fetch_page(session_token, transaction_id, next_page_token)
                .await
        })
        .await
    }

    async fn start_session(&self, ledger_name: String) -> Result<SessionToken, QldbError> {
        self.handle(|client| async move { client.start_session(ledger_name).await })
            .await
    }

    async fn start_transaction(
        &self,
        session_token: &SessionToken,
    ) -> Result<StartTransactionResult, QldbError> {
        let res = self
            .handle(|client| async move { client.start_transaction(session_token).await })
            .await;

        if let Err(QldbError::Rusoto(RusotoError::Service(SendCommandError::BadRequest(
            ref message,
        )))) = res
        {
            debug!(
                "unable to start a transaction on session {} (will be discarded): {}",
                self.session_token(),
                message
            );
            self.notify_invalid();
        }

        res
    }
}
