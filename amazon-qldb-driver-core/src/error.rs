use aws_smithy_http::operation::BuildError;
use thiserror::Error;

pub type QldbResult<T> = std::result::Result<T, QldbError>;
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Error, Debug)]
pub enum QldbError {
    /// This error variant is used in place of a panic. It represents a codepath
    /// that should not be taken. Panicing in libraries is bad!
    #[error("illegal state: {0}")]
    IllegalState(String),
    #[error("usage error: {0}")]
    UsageError(String),
    #[error("todo")]
    TodoStableErrorApi,
}

// Smithy-generated builders could return an error on build. In most cases, this
// isn't actually possible (i.e. the method is infallible).
//
// In cases where an error could be returned, doing so would be a bug in the
// driver (as it should have provided the requisite fields). The driver, being a
// library, should never panic as crashing a user application is bad.
impl From<BuildError> for QldbError {
    fn from(smithy: BuildError) -> Self {
        QldbError::IllegalState(format!("{}", smithy))
    }
}

pub(crate) fn usage_error<S>(message: S) -> QldbError
where
    S: Into<String>,
{
    QldbError::UsageError(message.into())
}

pub(crate) fn illegal_state<S>(message: S) -> QldbError
where
    S: Into<String>,
{
    QldbError::IllegalState(message.into())
}

pub(crate) fn todo_stable_error_api() -> QldbError {
    QldbError::TodoStableErrorApi
}
