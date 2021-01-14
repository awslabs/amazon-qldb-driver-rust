use rusoto_core::RusotoError;
use rusoto_qldb_session::*;

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

pub use crate::driver::{BlockingQldbDriver, QldbDriver, QldbDriverBuilder};

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
