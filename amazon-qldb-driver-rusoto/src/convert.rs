use amazon_qldb_driver_core::api::QldbSession;
use async_trait::async_trait;

use aws_sdk_qldbsession::error::{
    BadRequestException, CapacityExceededException, InvalidSessionException,
    LimitExceededException, OccConflictException, RateExceededException, SendCommandError,
    SendCommandErrorKind,
};
use aws_sdk_qldbsession::input::SendCommandInput;
use aws_sdk_qldbsession::output::SendCommandOutput;
use aws_sdk_qldbsession::Blob;
use aws_sdk_qldbsession::{model::*, SdkError};
use bytes::Bytes;
use rusoto_core::RusotoError;
use rusoto_qldb_session::{self, QldbSessionClient};
use smithy_http::body::SdkBody;
use smithy_http::operation;

use amazon_qldb_driver_core::error::{StringError, UnitError};

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
                .map(|b| Bytes::from(b.into_inner()))
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
                p.into_iter()
                    .map(|h| rusoto_qldb_session::ValueHolder {
                        ion_binary: h.ion_binary.map(|b| Bytes::from(b.into_inner())),
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

// Maps the response type (one `Result` to another). The type signature
// is awful, but don't be alarmed. This is just more Rusoto -> smithy
// mapping.
impl
    ConvertFrom<
        Result<
            rusoto_qldb_session::SendCommandResult,
            RusotoError<rusoto_qldb_session::SendCommandError>,
        >,
    > for Result<SendCommandOutput, SdkError<SendCommandError>>
{
    fn convert_from(
        rusoto: Result<
            rusoto_qldb_session::SendCommandResult,
            RusotoError<rusoto_qldb_session::SendCommandError>,
        >,
    ) -> Self {
        match rusoto {
            Ok(value) => Ok(value.convert_into()),
            Err(err) => Err(err.convert_into()),
        }
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

impl ConvertFrom<rusoto_qldb_session::IOUsage> for IoUsage {
    fn convert_from(rusoto: rusoto_qldb_session::IOUsage) -> Self {
        IoUsage::builder()
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

impl ConvertFrom<RusotoError<rusoto_qldb_session::SendCommandError>>
    for SdkError<SendCommandError>
{
    // Note: all the HTTP body stuff is set to empty. At the time of
    // writing, it was too annoying to convert these over and the driver
    // didn't actually care.
    fn convert_from(rusoto: RusotoError<rusoto_qldb_session::SendCommandError>) -> Self {
        match rusoto {
            RusotoError::Service(err) => match err {
                rusoto_qldb_session::SendCommandError::BadRequest(message) => {
                    SdkError::ServiceError {
                        err: SendCommandError::new(
                            SendCommandErrorKind::BadRequestException(
                                BadRequestException::builder().message(message).build(),
                            ),
                            Default::default(),
                        ),
                        raw: operation::Response::new(
                            http::Response::builder().body(SdkBody::empty()).unwrap(),
                        ),
                    }
                }
                rusoto_qldb_session::SendCommandError::InvalidSession(message) => {
                    SdkError::ServiceError {
                        err: SendCommandError::new(
                            SendCommandErrorKind::InvalidSessionException(
                                InvalidSessionException::builder().message(message).build(),
                            ),
                            Default::default(),
                        ),
                        raw: operation::Response::new(
                            http::Response::builder().body(SdkBody::empty()).unwrap(),
                        ),
                    }
                }
                rusoto_qldb_session::SendCommandError::LimitExceeded(message) => {
                    SdkError::ServiceError {
                        err: SendCommandError::new(
                            SendCommandErrorKind::LimitExceededException(
                                LimitExceededException::builder().message(message).build(),
                            ),
                            Default::default(),
                        ),
                        raw: operation::Response::new(
                            http::Response::builder().body(SdkBody::empty()).unwrap(),
                        ),
                    }
                }
                rusoto_qldb_session::SendCommandError::OccConflict(message) => {
                    SdkError::ServiceError {
                        err: SendCommandError::new(
                            SendCommandErrorKind::OccConflictException(
                                OccConflictException::builder().message(message).build(),
                            ),
                            Default::default(),
                        ),
                        raw: operation::Response::new(
                            http::Response::builder().body(SdkBody::empty()).unwrap(),
                        ),
                    }
                }
                rusoto_qldb_session::SendCommandError::RateExceeded(message) => {
                    SdkError::ServiceError {
                        err: SendCommandError::new(
                            SendCommandErrorKind::RateExceededException(
                                RateExceededException::builder().message(message).build(),
                            ),
                            Default::default(),
                        ),
                        raw: operation::Response::new(
                            http::Response::builder().body(SdkBody::empty()).unwrap(),
                        ),
                    }
                }
                rusoto_qldb_session::SendCommandError::CapacityExceeded(message) => {
                    SdkError::ServiceError {
                        err: SendCommandError::new(
                            SendCommandErrorKind::CapacityExceededException(
                                CapacityExceededException::builder()
                                    .message(message)
                                    .build(),
                            ),
                            Default::default(),
                        ),
                        raw: operation::Response::new(
                            http::Response::builder().body(SdkBody::empty()).unwrap(),
                        ),
                    }
                }
            },
            RusotoError::HttpDispatch(err) => SdkError::DispatchFailure(Box::new(err)),
            RusotoError::Credentials(err) => SdkError::ConstructionFailure(Box::new(err)),
            RusotoError::Validation(message) => {
                SdkError::ConstructionFailure(Box::new(StringError(message)))
            }
            RusotoError::ParseError(message) => SdkError::ResponseError {
                raw: operation::Response::new(
                    http::Response::builder().body(SdkBody::empty()).unwrap(),
                ),
                err: Box::new(StringError(message)),
            },
            RusotoError::Unknown(_http) => SdkError::ResponseError {
                raw: operation::Response::new(
                    http::Response::builder().body(SdkBody::empty()).unwrap(),
                ),
                err: Box::new(UnitError),
            },
            RusotoError::Blocking => {
                SdkError::DispatchFailure(Box::new(StringError("blocking".to_string())))
            }
        }
    }
}

/// Wrapper so we can implement `QldbSession` (defined in the core trait) for
/// the rusoto client (not defined in this crate either).
#[derive(Clone)]
pub struct RusotoQldbSessionClient(pub QldbSessionClient);

#[async_trait]
impl QldbSession for RusotoQldbSessionClient {
    async fn send_command(
        &self,
        input: SendCommandInput,
    ) -> Result<SendCommandOutput, SdkError<SendCommandError>> {
        let req = input.convert_into();
        // Fully qualifying the rusoto trait here because it's too confusing
        // otherwise! Lots of things using the same name.
        let res = rusoto_qldb_session::QldbSession::send_command(&self.0, req).await;
        res.convert_into()
    }
}
