use rusoto_core::RusotoError;
use rusoto_qldb_session::*;

use anyhow::Result;
use thiserror::Error;

#[macro_use]
extern crate log;

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

#[derive(Error, Debug)]
pub enum QldbError {
    #[error("illegal state: {0}")]
    IllegalState(String),
    #[error("usage error: {0}")]
    UsageError(String),
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),
    #[error("communication failure: {0}")]
    Rusoto(#[from] RusotoError<SendCommandError>),
}
