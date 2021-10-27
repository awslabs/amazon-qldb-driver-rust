use async_trait::async_trait;
use aws_hyper::SmithyConnector;
use aws_sdk_qldbsessionv2::{
    error::SendCommandError,
    model::{CommandStream, ResultStream},
    output::SendCommandOutput,
    Client,
};
use aws_smithy_http::event_stream::BoxError;
use bb8::{ErrorSink, ManageConnection};
use futures::{
    channel::mpsc::{channel, Sender},
    SinkExt,
};
use tracing::debug;

use crate::error;

type ConnectionError = aws_smithy_http::result::SdkError<SendCommandError>;

#[derive(Debug, Copy, Clone)]
pub struct QldbErrorLoggingErrorSink;

impl QldbErrorLoggingErrorSink {
    pub fn new() -> QldbErrorLoggingErrorSink {
        QldbErrorLoggingErrorSink {}
    }
}

impl ErrorSink<ConnectionError> for QldbErrorLoggingErrorSink {
    fn sink(&self, error: ConnectionError) {
        debug!(error = %error, "error in connection pool");
    }

    fn boxed_clone(&self) -> Box<dyn ErrorSink<ConnectionError>> {
        Box::new(*self)
    }
}

// FIXME: This is not generic over the client (it uses the rust awssdk
// directly), which means we cannot change the sdk (e.g. to support javascript).
pub struct QldbSessionV2Manager<C>
where
    C: SmithyConnector,
{
    client: Client<C>,
    pub ledger_name: String,
}

impl<C> QldbSessionV2Manager<C>
where
    C: SmithyConnector,
{
    pub(crate) fn new(
        client: Client<C>,
        ledger_name: impl Into<String>,
    ) -> QldbSessionV2Manager<C> {
        QldbSessionV2Manager {
            client,
            ledger_name: ledger_name.into(),
        }
    }
}

/// A HTTP/2 based connection to QLDB. There is one physical connection per
/// session, and each session can have at most one open transaction.
///
/// Sessions are broken upon exception. An exception is an error that is sent as
/// an exception (as opposed to as data).
pub struct QldbHttp2Connection {
    pub ledger_name: String,
    sender: Sender<Result<CommandStream, BoxError>>,
    output: SendCommandOutput,
}

#[async_trait]
impl<C> ManageConnection for QldbSessionV2Manager<C>
where
    C: SmithyConnector,
{
    type Connection = QldbHttp2Connection;
    type Error = ConnectionError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        // Event streams support full-duplex communication. However, Qldb does
        // not yet support this. That is, only 1 request-response pair may be
        // inflight at any given time.
        let (sender, receiver) = channel(1);

        let output = self
            .client
            .send_command()
            .ledger_name(&self.ledger_name[..])
            .command_stream(receiver.into())
            .send()
            .await?;

        // TODO: Is there an initial-response?
        // let initial = output.result_stream.try_recv_initial().await?;

        Ok(QldbHttp2Connection {
            ledger_name: self.ledger_name.clone(),
            sender,
            output,
        })
    }

    // FIXME: these methods are probably wrong. Usually you have to do some work
    // (i.e. read from the connection) to learn that it's broken. Figure out
    // what to do here and inline bb8 documentation (i.e. when are these called,
    // etc.).

    async fn is_valid(
        &self,
        _conn: &mut bb8::PooledConnection<'_, Self>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        match (
            conn.sender.is_closed(),
            false, /* conn.output.result_stream.is_closed() FIXME?? */
        ) {
            (false, false) => true,
            _ => false,
        }
    }
}

impl QldbHttp2Connection {
    pub(crate) async fn send_streaming_command(
        &mut self,
        command: CommandStream,
    ) -> Result<ResultStream, SendCommandError> {
        self.sender.send(Ok(command)).await.map_err(|_| todo!())?;
        let resp = match self
            .output
            .result_stream
            .recv()
            .await
            .map_err(|_| todo!())?
        {
            Some(msg) => msg,
            None => Err(SendCommandError::unhandled(error::illegal_state(
                "attempted to send a message on a closed channel",
            )))?,
        };

        Ok(resp)
    }
}
