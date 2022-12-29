use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use crate::api::{QldbSession, QldbSessionApi, SessionToken};
use crate::error::QldbError;
use async_trait::async_trait;
use aws_sdk_qldbsession::{
    error::{SendCommandError, SendCommandErrorKind},
    input::SendCommandInput,
    output::SendCommandOutput,
    types::SdkError,
};
use bb8::{ErrorSink, ManageConnection};
use tracing::debug;

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
        debug!(error = %error, "error in connection pool");
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

    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
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
impl<C> QldbSession for QldbHttp1Connection<C>
where
    C: QldbSession + Send + Sync + Clone,
{
    async fn send_command(
        &self,
        input: SendCommandInput,
    ) -> Result<SendCommandOutput, SdkError<SendCommandError>> {
        let client = if let Ok(g) = self.inner.lock() {
            if g.discard {
                panic!("session {} should have been discarded", g.token)
            }

            // Note: the delegated call to `send_command` creates a future which
            // must be `Send`, which a `MutexGuard` isn't. So, we clone the
            // underlying client and move that into the closure. Rusoto clients
            // have an inner Arc, so this clone is really cheap.
            g.client.clone()
        } else {
            unreachable!()
        };

        let is_start_session = input.start_session.is_some();
        let res = client.send_command(input).await;

        if let Err(SdkError::ServiceError(service_error)) = &res {
            let kind = &service_error.err().kind;
            if let SendCommandErrorKind::InvalidSessionException(_) = kind {
                self.notify_invalid();
            }

            if is_start_session {
                if let SendCommandErrorKind::BadRequestException(ref message) = kind {
                    debug!(
                        session_token = %self.session_token(),
                        %message, "unable to start a transaction on session (will be discarded)"
                    );
                    self.notify_invalid();
                }
            }
        }

        res
    }
}
