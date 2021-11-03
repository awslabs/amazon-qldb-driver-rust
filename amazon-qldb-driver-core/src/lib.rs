pub mod driver;
pub mod error;
pub mod execution_stats;
pub mod ion_compat;
pub mod pool;
pub mod retry;
pub mod transaction;

pub use crate::driver::{QldbDriver, QldbDriverBuilder};
pub use crate::transaction::TransactionAttempt;
pub use aws_sdk_qldbsessionv2 as awssdk;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[inline(always)]
pub fn version() -> &'static str {
    VERSION
}
