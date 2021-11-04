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
//!     // FIXME: I put a random error type in here
//!     let r: Result<(), TransactError<std::io::Error>> = driver.transact(|mut tx| async {
//!         tx.execute_statement("create table my_table").await?;
//!         tx.commit(()).await
//!     }).await;
//!     r?;
//!
//!     Ok(())
//! }
//! ```

pub use amazon_qldb_driver_core::awssdk;
pub use amazon_qldb_driver_core::error::{BoxError, BuilderError, TransactError};
pub use amazon_qldb_driver_core::ion_compat;
pub use amazon_qldb_driver_core::transaction::StatementResults;
pub use amazon_qldb_driver_core::{
    retry, version, QldbDriver, QldbDriverBuilder, TransactionAttempt,
};
