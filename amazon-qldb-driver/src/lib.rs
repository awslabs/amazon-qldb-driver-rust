//! The Amazon QLDB driver for Rust.
//!
//! Usage example:
//!
//! ```no_run
//! use std::convert::Infallible;
//! use amazon_qldb_driver::awssdk::Config;
//! use amazon_qldb_driver::{QldbDriverBuilder, TransactionAttempt, TransactError};
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
//!     driver.transact(|mut tx: TransactionAttempt<Infallible>| async {
//!         tx.execute_statement("create table my_table").await?;
//!         tx.commit(()).await
//!     }).await?;
//!
//!     Ok(())
//! }
//! ```

pub use amazon_qldb_driver_core::awssdk;
pub use amazon_qldb_driver_core::error::{BoxError, BuilderError, TransactError};
pub use amazon_qldb_driver_core::ion_compat;
pub use amazon_qldb_driver_core::transaction::{StatementResults, TransactionDisposition};
pub use amazon_qldb_driver_core::{
    retry, version, QldbDriver, QldbDriverBuilder, TransactionAttempt,
};
pub type TransactionResult<R, E> = Result<TransactionDisposition<R>, TransactError<E>>;
