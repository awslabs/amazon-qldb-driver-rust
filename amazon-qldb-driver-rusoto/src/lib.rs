//! Here is some example code:
//!
//! ```no_run
//! use tokio;
//! use amazon_qldb_driver_core::QldbDriverBuilder;
//! use amazon_qldb_driver_rusoto::QldbDriverBuilderExt;
//! use rusoto_core::region::Region;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let driver = QldbDriverBuilder::new()
//!         .ledger_name("sample-ledger")
//!         .via_rusoto()
//!         .build()
//!         .await?;
//!     let (a, b) = driver.transact(|mut tx| async {
//!         let a = tx.execute_statement("SELECT 1").await?;
//!         let b = tx.execute_statement("SELECT 2").await?;
//!         tx.commit((a, b)).await
//!     }).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! Note that the `transaction` argument is `Fn` not `FnOnce` or `FnMut`.
//! This is to support retries (of the entire transaction) and ensure that
//! your function is idempotent (cannot mutate the environment it captures).
//! For example the following will not compile:
//!
//! ```compile_fail
//! # use tokio;
//! # use amazon_qldb_driver::QldbDriverBuilder;
//! # use rusoto_core::region::Region;
//! #
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! #   let driver = QldbDriverBuilder::new().ledger_name("sample-ledger").build()?;
//!     let mut string = String::new();
//!     driver.transact(|tx| async {
//!        string.push('1');
//!        tx.commit(()).await
//!     }).await?;
//! #
//! #   Ok(())
//! # }
//! ```
//!
//! Again, this is because `transaction` is `Fn` not `FnMut` and thus is not
//! allowed to mutate `string`.

pub mod convert;
pub mod driver_ext;
pub mod rusoto_ext;
pub mod testing;

pub use driver_ext::QldbDriverBuilderExt;
