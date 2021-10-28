pub mod driver;
pub mod error;
pub mod execution_stats;
pub mod ion_compat;
pub mod pool;
pub mod retry;
pub mod transaction;

pub use crate::driver::{QldbDriver, QldbDriverBuilder};
pub use crate::transaction::{TransactionAttempt, TransactionAttemptResult};

use anyhow::Result;

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
