use std::fmt::Debug;

use crate::api::{QldbSessionApi, SessionToken};
use crate::QldbError;
use async_trait::async_trait;
use bb8::{ErrorSink, ManageConnection};
use rusoto_qldb_session::*;

pub struct QldbSessionManager<C>
where
    C: QldbSession + Send + Sync,
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
pub struct QldbHttp1Connection {
    pub token: SessionToken,
    discard: bool,
}

impl QldbHttp1Connection {
    pub fn notify_invalid(&mut self) {
        self.discard = true;
    }
}

#[async_trait]
impl<C> ManageConnection for QldbSessionManager<C>
where
    C: QldbSession + Send + Sync + 'static,
{
    type Connection = QldbHttp1Connection;
    type Error = QldbError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let token = self.client.start_session(self.ledger_name.clone()).await?;
        Ok(QldbHttp1Connection {
            token,
            discard: false,
        })
    }

    async fn is_valid(
        &self,
        _conn: &mut bb8::PooledConnection<'_, Self>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.discard
    }
}
