use aws_sdk_qldbsession::{error, types::SdkError};

use aws_smithy_http::operation::BuildError;
use core::fmt;
use thiserror::Error;

pub type QldbResult<T> = std::result::Result<T, QldbError>;

#[derive(Error, Debug)]
pub enum QldbError {
    /// This error variant is used in place of a panic. It represents a codepath
    /// that should not be taken. Panicing in libraries is bad!
    #[error("illegal state: {0}")]
    IllegalState(String),
    #[error("usage error: {0}")]
    UsageError(String),
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),
    #[error("communication failure: {0}")]
    SdkError(#[from] SdkError<error::SendCommandError>),
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

#[derive(Debug)]
pub struct UnitError;

impl fmt::Display for UnitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no further informaation")
    }
}

impl std::error::Error for UnitError {}

#[derive(Debug)]
pub struct StringError(pub String);

impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StringError {}
