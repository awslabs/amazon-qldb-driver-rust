use async_trait::async_trait;
use aws_sdk_qldbsession::{input::SendCommandInput, model::*, output::SendCommandOutput, Blob};
use bytes::Bytes;
use tracing::debug;

use crate::{QldbError, QldbResult};

/// The AWS SDK for Rust does not provide a trait abstraction over the API, so
/// we provide one here.
#[async_trait]
pub trait QldbSession {
    async fn send_command(&self, input: SendCommandInput) -> QldbResult<SendCommandOutput>;
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

pub mod rusoto {
    use crate::api::rusoto::convert::ConvertInto;

    use super::*;
    use async_trait::async_trait;

    mod convert {
        use aws_sdk_qldbsession::input::SendCommandInput;
        use aws_sdk_qldbsession::model::*;
        use aws_sdk_qldbsession::output::SendCommandOutput;
        use aws_sdk_qldbsession::Blob;
        use bytes::Bytes;
        use rusoto_qldb_session;

        /// In-crate implementations of the stdlib From/Into traits. These are
        /// required because the Smithy and Rusoto shapes are both out-of-crate,
        /// and thus coherency rules prevent using the stdlib traits.

        pub trait ConvertFrom<T>: Sized {
            fn convert_from(_: T) -> Self;
        }

        pub trait ConvertInto<T>: Sized {
            fn convert_into(self) -> T;
        }

        impl<T, U> ConvertInto<U> for T
        where
            U: ConvertFrom<T>,
        {
            fn convert_into(self) -> U {
                U::convert_from(self)
            }
        }

        impl ConvertFrom<SendCommandInput> for rusoto_qldb_session::SendCommandRequest {
            fn convert_from(model: SendCommandInput) -> Self {
                Self {
                    abort_transaction: model.abort_transaction.map(|v| v.convert_into()),
                    commit_transaction: model.commit_transaction.map(|v| v.convert_into()),
                    end_session: model.end_session.map(|v| v.convert_into()),
                    execute_statement: model.execute_statement.map(|v| v.convert_into()),
                    fetch_page: model.fetch_page.map(|v| v.convert_into()),
                    session_token: model.session_token,
                    start_session: model.start_session.map(|v| v.convert_into()),
                    start_transaction: model.start_transaction.map(|v| v.convert_into()),
                }
            }
        }

        impl ConvertFrom<AbortTransactionRequest> for rusoto_qldb_session::AbortTransactionRequest {
            fn convert_from(_: AbortTransactionRequest) -> Self {
                Self {}
            }
        }

        impl ConvertFrom<CommitTransactionRequest> for rusoto_qldb_session::CommitTransactionRequest {
            fn convert_from(model: CommitTransactionRequest) -> Self {
                Self {
                    commit_digest: model
                        .commit_digest
                        .map(|b| Bytes::from(b.as_ref()))
                        .expect("commit digest is always present"),
                    transaction_id: model
                        .transaction_id
                        .expect("transaction_id is always present"),
                }
            }
        }

        impl ConvertFrom<EndSessionRequest> for rusoto_qldb_session::EndSessionRequest {
            fn convert_from(_: EndSessionRequest) -> Self {
                Self {}
            }
        }

        impl ConvertFrom<ExecuteStatementRequest> for rusoto_qldb_session::ExecuteStatementRequest {
            fn convert_from(model: ExecuteStatementRequest) -> Self {
                Self {
                    parameters: model.parameters.map(|p| {
                        p.iter()
                            .map(|h| rusoto_qldb_session::ValueHolder {
                                ion_binary: h.ion_binary.map(|b| Bytes::from(b.as_ref())),
                                ion_text: h.ion_text,
                            })
                            .collect()
                    }),
                    statement: model.statement.expect("statement is always present"),
                    transaction_id: model
                        .transaction_id
                        .expect("transaction_id is always present"),
                }
            }
        }

        impl ConvertFrom<FetchPageRequest> for rusoto_qldb_session::FetchPageRequest {
            fn convert_from(model: FetchPageRequest) -> Self {
                Self {
                    next_page_token: model
                        .next_page_token
                        .expect("next_page_token is always present"),
                    transaction_id: model
                        .transaction_id
                        .expect("transaction_id is always present"),
                }
            }
        }

        impl ConvertFrom<StartSessionRequest> for rusoto_qldb_session::StartSessionRequest {
            fn convert_from(model: StartSessionRequest) -> Self {
                Self {
                    ledger_name: model.ledger_name.expect("ledger_name is always present"),
                }
            }
        }

        impl ConvertFrom<StartTransactionRequest> for rusoto_qldb_session::StartTransactionRequest {
            fn convert_from(_: StartTransactionRequest) -> Self {
                Self {}
            }
        }

        impl ConvertFrom<rusoto_qldb_session::SendCommandResult> for SendCommandOutput {
            fn convert_from(rusoto: rusoto_qldb_session::SendCommandResult) -> Self {
                let builder = SendCommandOutput::builder();
                let builder = match rusoto.start_session {
                    Some(v) => builder.start_session(v.convert_into()),
                    None => builder,
                };
                let builder = match rusoto.start_transaction {
                    Some(v) => builder.start_transaction(v.convert_into()),
                    None => builder,
                };
                let builder = match rusoto.end_session {
                    Some(v) => builder.end_session(v.convert_into()),
                    None => builder,
                };
                let builder = match rusoto.commit_transaction {
                    Some(v) => builder.commit_transaction(v.convert_into()),
                    None => builder,
                };
                let builder = match rusoto.abort_transaction {
                    Some(v) => builder.abort_transaction(v.convert_into()),
                    None => builder,
                };
                let builder = match rusoto.execute_statement {
                    Some(v) => builder.execute_statement(v.convert_into()),
                    None => builder,
                };
                let builder = match rusoto.fetch_page {
                    Some(v) => builder.fetch_page(v.convert_into()),
                    None => builder,
                };
                builder.build()
            }
        }

        impl ConvertFrom<rusoto_qldb_session::StartSessionResult> for StartSessionResult {
            fn convert_from(rusoto: rusoto_qldb_session::StartSessionResult) -> Self {
                let builder = StartSessionResult::builder();
                let builder = match rusoto.session_token {
                    Some(v) => builder.session_token(v),
                    None => builder,
                };
                let builder = match rusoto.timing_information {
                    Some(v) => builder.timing_information(v.convert_into()),
                    None => builder,
                };
                builder.build()
            }
        }

        impl ConvertFrom<rusoto_qldb_session::StartTransactionResult> for StartTransactionResult {
            fn convert_from(rusoto: rusoto_qldb_session::StartTransactionResult) -> Self {
                let builder = StartTransactionResult::builder();
                let builder = match rusoto.transaction_id {
                    Some(v) => builder.transaction_id(v),
                    None => builder,
                };
                let builder = match rusoto.timing_information {
                    Some(v) => builder.timing_information(v.convert_into()),
                    None => builder,
                };
                builder.build()
            }
        }

        impl ConvertFrom<rusoto_qldb_session::EndSessionResult> for EndSessionResult {
            fn convert_from(rusoto: rusoto_qldb_session::EndSessionResult) -> Self {
                let builder = EndSessionResult::builder();
                let builder = match rusoto.timing_information {
                    Some(v) => builder.timing_information(v.convert_into()),
                    None => builder,
                };
                builder.build()
            }
        }
        impl ConvertFrom<rusoto_qldb_session::CommitTransactionResult> for CommitTransactionResult {
            fn convert_from(rusoto: rusoto_qldb_session::CommitTransactionResult) -> Self {
                //Self { transaction_id: (), commit_digest: (), timing_information: (), consumed_i_os: () }
                let builder = CommitTransactionResult::builder();
                let builder = match rusoto.transaction_id {
                    Some(v) => builder.transaction_id(v),
                    None => builder,
                };
                let builder = match rusoto.commit_digest {
                    Some(v) => builder.commit_digest(Blob::new(&v[..])),
                    None => builder,
                };
                let builder = match rusoto.timing_information {
                    Some(v) => builder.timing_information(v.convert_into()),
                    None => builder,
                };
                let builder = match rusoto.consumed_i_os {
                    Some(v) => builder.consumed_i_os(v.convert_into()),
                    None => builder,
                };
                builder.build()
            }
        }
        impl ConvertFrom<rusoto_qldb_session::AbortTransactionResult> for AbortTransactionResult {
            fn convert_from(rusoto: rusoto_qldb_session::AbortTransactionResult) -> Self {
                let builder = AbortTransactionResult::builder();
                let builder = match rusoto.timing_information {
                    Some(v) => builder.timing_information(v.convert_into()),
                    None => builder,
                };
                builder.build()
            }
        }

        impl ConvertFrom<rusoto_qldb_session::ExecuteStatementResult> for ExecuteStatementResult {
            fn convert_from(rusoto: rusoto_qldb_session::ExecuteStatementResult) -> Self {
                let builder = ExecuteStatementResult::builder();
                let builder = match rusoto.first_page {
                    Some(v) => builder.first_page(v.convert_into()),
                    None => builder,
                };
                let builder = match rusoto.timing_information {
                    Some(v) => builder.timing_information(v.convert_into()),
                    None => builder,
                };
                let builder = match rusoto.consumed_i_os {
                    Some(v) => builder.consumed_i_os(v.convert_into()),
                    None => builder,
                };
                builder.build()
            }
        }
        impl ConvertFrom<rusoto_qldb_session::FetchPageResult> for FetchPageResult {
            fn convert_from(rusoto: rusoto_qldb_session::FetchPageResult) -> Self {
                let builder = FetchPageResult::builder();
                let builder = match rusoto.page {
                    Some(v) => builder.page(v.convert_into()),
                    None => builder,
                };

                let builder = match rusoto.timing_information {
                    Some(v) => builder.timing_information(v.convert_into()),
                    None => builder,
                };
                let builder = match rusoto.consumed_i_os {
                    Some(v) => builder.consumed_i_os(v.convert_into()),
                    None => builder,
                };
                builder.build()
            }
        }

        impl ConvertFrom<rusoto_qldb_session::TimingInformation> for TimingInformation {
            fn convert_from(rusoto: rusoto_qldb_session::TimingInformation) -> Self {
                TimingInformation::builder()
                    .processing_time_milliseconds(rusoto.processing_time_milliseconds.unwrap_or(0))
                    .build()
            }
        }

        impl ConvertFrom<rusoto_qldb_session::IOUsage> for IOUsage {
            fn convert_from(rusoto: rusoto_qldb_session::IOUsage) -> Self {
                IOUsage::builder()
                    .read_i_os(rusoto.read_i_os.unwrap_or(0))
                    .write_i_os(rusoto.write_i_os.unwrap_or(0))
                    .build()
            }
        }

        impl ConvertFrom<rusoto_qldb_session::Page> for Page {
            fn convert_from(rusoto: rusoto_qldb_session::Page) -> Self {
                let builder = Page::builder();
                let builder = match rusoto.values {
                    Some(v) => {
                        let it = v.into_iter().map(|h| h.convert_into()).collect();
                        builder.set_values(Some(it))
                    }
                    None => builder,
                };
                let builder = match rusoto.next_page_token {
                    Some(v) => builder.next_page_token(v),
                    None => builder,
                };
                builder.build()
            }
        }

        impl ConvertFrom<rusoto_qldb_session::ValueHolder> for ValueHolder {
            fn convert_from(rusoto: rusoto_qldb_session::ValueHolder) -> Self {
                let builder = ValueHolder::builder();
                let builder = match rusoto.ion_binary {
                    Some(v) => builder.ion_binary(Blob::new(&v[..])),
                    None => builder,
                };
                let builder = match rusoto.ion_text {
                    Some(v) => builder.ion_text(v),
                    None => builder,
                };
                builder.build()
            }
        }
    }

    #[async_trait]
    impl<R> super::QldbSession for R
    where
        R: rusoto_qldb_session::QldbSession + Send + Sync,
    {
        async fn send_command(&self, input: SendCommandInput) -> QldbResult<SendCommandOutput> {
            let req = input.convert_into();
            let res = self.send_command(req).await?;
            res.convert_into()
        }
    }

    #[cfg(test)]
    pub mod testing {
        use async_trait::async_trait;
        use rusoto_core::RusotoError;
        use rusoto_qldb_session::*;
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

        pub fn build_abort_transaction_response(
            _req: AbortTransactionRequest,
        ) -> SendCommandResult {
            SendCommandResult {
                abort_transaction: Some(AbortTransactionResult {
                    ..Default::default()
                }),
                ..Default::default()
            }
        }

        pub fn build_commit_transaction_response(
            req: CommitTransactionRequest,
        ) -> SendCommandResult {
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

        pub fn build_execute_statement_response(
            _req: ExecuteStatementRequest,
        ) -> SendCommandResult {
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

        pub fn build_start_transaction_response(
            _req: StartTransactionRequest,
        ) -> SendCommandResult {
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
}
