use std::convert::Infallible;

use amazon_qldb_driver_core::TransactionAttempt;
use amazon_qldb_driver_core::{QldbDriverBuilder, TransactionResult};
use anyhow::Result;
use ion_c_sys::reader::IonCReader;
use tokio;

/// This example shows how you can use normal Rust functions as QLDB `transact`
/// arguments.
///
/// Lambdas are really nice short in-line transactions but sometimes a function
/// is a better fit.
#[tokio::main]
async fn main() -> Result<()> {
    // Run me with `export RUST_LOG=debug` for more output!
    tracing_subscriber::fmt::init();

    let aws_config = aws_config::load_from_env().await;

    let driver = QldbDriverBuilder::new()
        .ledger_name("sample-ledger")
        .sdk_config(&aws_config)
        .await?;

    let table_names = driver.transact(list_table_names).await?;
    for name in table_names {
        println!("- {}", name);
    }

    Ok(())
}

/// Your function receives a `TransactionAttempt` as an argument. You can use
/// this argument to call functions against the transaction, such as
/// `execute_statement`.
///
/// The function returns a special result type. See the documentation on
/// [`TransactionResult`] for more information.
async fn list_table_names(
    mut tx: TransactionAttempt<Infallible>,
) -> TransactionResult<Vec<String>, Infallible> {
    let results = tx
        .execute_statement("select value name from information_schema.user_tables")
        .await?
        .buffered()
        .await?;

    let names: Vec<String> = results
        .readers()
        .map(|reader| {
            let mut reader = reader?;
            let _ = reader.next()?;
            let s = reader.read_string()?;
            Ok(s.as_str().to_string())
        })
        .filter_map(|it: Result<String>| it.ok())
        .collect();
    tx.commit(names).await
}
