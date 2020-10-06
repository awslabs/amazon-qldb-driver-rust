use rusoto_core::RusotoError;
use rusoto_qldb_session::*;

use thiserror::Error;

#[macro_use]
extern crate log;

pub mod api;
pub mod ion_compat;
pub mod qldb_hash;

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
