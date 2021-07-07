use core::fmt;

pub use aws_sdk_qldbsession::{error, input, model, output, SdkError};

use anyhow::Result;
use smithy_http::operation::BuildError;
use thiserror::Error;

pub mod api;
pub mod driver;
pub mod execution_stats;
pub mod ion_compat;
pub mod pool;
pub mod qldb_hash;
pub mod retry;
pub mod rusoto_ext;
pub mod transaction;

pub use crate::driver::{QldbDriver, QldbDriverBuilder};
pub use crate::transaction::{TransactionAttempt, TransactionAttemptResult};

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[inline(always)]
pub fn version() -> &'static str {
    VERSION
}

/// An alias for the type returned by calls to [`QldbDriver::transact`]. The
/// outer `Result` signifies whether the block of code succeeded or not (i.e.
/// did a `?` cause early return). The inner type is the outcome of the
/// transaction: whether or not it was committed or aborted.
pub type TransactionResult<R> = Result<TransactionAttemptResult<R>>;

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
pub struct StringError(String);

impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StringError {}
