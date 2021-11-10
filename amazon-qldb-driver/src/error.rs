use core::fmt;

use aws_sdk_qldbsessionv2::model::ResultStream;
use thiserror::Error;

use crate::pool::SendStreamingCommandError;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// An error building the driver.
#[derive(Debug, Error)]
pub enum BuilderError {
    /// Did not attempt to build a driver because the builder was incorrectly
    /// used, such as omitting a field.
    #[error("usage error: {0}")]
    UsageError(String),

    /// The builder was used correctly, but some configuration or environmental
    /// problem prevents the driver being built.
    #[error("unable to build driver: {source}")]
    BuildError {
        #[source]
        source: BoxError,
    },
}

pub(crate) fn build_err<E>(source: E) -> BuilderError
where
    E: std::error::Error + Send + Sync + 'static,
{
    BuilderError::BuildError {
        source: Box::new(source),
    }
}

// In-crate stable errors variants over the result stream error variants.
// TODO: WIP

#[derive(Debug)]
pub struct GenericError {
    code: Option<String>,
    message: Option<String>,
}

impl fmt::Display for GenericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message.as_deref().unwrap_or(""))
    }
}

impl From<aws_sdk_qldbsessionv2::model::StatementError> for GenericError {
    fn from(awssdk: aws_sdk_qldbsessionv2::model::StatementError) -> Self {
        GenericError {
            code: awssdk.code,
            message: awssdk.message,
        }
    }
}

impl From<aws_sdk_qldbsessionv2::model::TransactionError> for GenericError {
    fn from(awssdk: aws_sdk_qldbsessionv2::model::TransactionError) -> Self {
        GenericError {
            code: awssdk.code,
            message: awssdk.message,
        }
    }
}

/// Represents a failure in a call to [`QldbDriver.transact`]. Failures could be
/// due to user code (e.g. deserializing a QLDB document into a local type), or
/// from interacting with the QLDB service.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransactError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    /// Any error thrown by user code. The driver does not retry these.
    // This is the only variant that uses the From infrastructure to keep
    // user-code pleasant to write.
    #[error("{0}")]
    User(#[from] E),

    /// A database command failed with a non-terminal user-error. The driver
    /// does not retry these.
    #[error("{0}")]
    GenericError(GenericError),

    /// A database command failed with a service or network error. See [`retry`]
    /// for policy on how these are handled.
    // The underlying error is erased to discourage handling in user code.
    #[error("communication error")]
    CommunicationError {
        #[source]
        source: BoxError,
    },

    #[error("transaction was aborted by user code")]
    Aborted,

    #[error("transaction failed after {attempts} attempts, last error: {last_err}")]
    MaxAttempts { last_err: BoxError, attempts: u32 },

    #[error("illegal state: {source}")]
    IllegalState {
        #[source]
        source: BoxError,
    },
}

/// Various ways client-server interaction can fail. This error is ultimately
/// used by the [`retry`] module.
#[derive(Debug, Error)]
pub enum CommunicationError {
    #[error("{0}")]
    Transport(#[from] SendStreamingCommandError),

    #[error("unexpected response: expected {expected}, got {actual:?}")]
    UnexpectedResponse {
        expected: String,
        actual: ResultStream,
    },

    #[error("malformed response: {message}")]
    MalformedResponse { message: String },
}

pub(crate) fn transport_err<E>(err: SendStreamingCommandError) -> TransactError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    TransactError::CommunicationError {
        source: Box::new(CommunicationError::Transport(err)),
    }
}

pub(crate) fn unexpected_response<E, S>(expected: S, actual: ResultStream) -> TransactError<E>
where
    E: std::error::Error + Send + Sync + 'static,
    S: Into<String>,
{
    TransactError::CommunicationError {
        source: Box::new(CommunicationError::UnexpectedResponse {
            expected: expected.into(),
            actual,
        }),
    }
}

pub(crate) fn malformed_response<E, S>(message: S) -> TransactError<E>
where
    E: std::error::Error + Send + Sync + 'static,
    S: Into<String>,
{
    TransactError::CommunicationError {
        source: Box::new(CommunicationError::MalformedResponse {
            message: message.into(),
        }),
    }
}
