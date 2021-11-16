//! The Amazon QLDB driver for Rust.
//!
//! Usage example:
//!
//! ```no_run
//! use amazon_qldb_driver::awssdk::Config;
//! use amazon_qldb_driver::{QldbDriverBuilder, TransactError};
//! use tokio;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let aws_config = aws_config::load_from_env().await;
//!
//!     let driver = QldbDriverBuilder::new()
//!         .ledger_name("sample-ledger")
//!         .sdk_config(&aws_config)
//!         .await?;
//!
//!     driver.transact(|mut tx| async {
//!         tx.execute_statement("create table my_table").await?;
//!         tx.commit(()).await
//!     }).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod driver;
pub mod error;
pub mod execution_stats;
pub mod ion_compat;
pub mod pool;
pub mod results;
pub mod retry;
pub mod transaction;

pub use crate::driver::{QldbDriver, QldbDriverBuilder};
pub use crate::error::{BoxError, BuilderError, TransactError};
pub use crate::results::StatementResults;
pub use crate::transaction::{TransactionAttempt, TransactionDisposition};
pub use aws_sdk_qldbsessionv2 as awssdk;

pub type TransactionResult<R, E> = Result<TransactionDisposition<R>, TransactError<E>>;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[inline(always)]
pub fn version() -> &'static str {
    VERSION
}
