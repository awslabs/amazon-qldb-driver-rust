use amazon_qldb_driver::{QldbDriverBuilder, QldbDriverBuilderExt};
use amazon_qldb_driver::{TransactionAttempt, TransactionResult};
use amazon_qldb_driver_core::api::QldbSession;
use anyhow::Result;
use ion_c_sys::reader::IonCReader;
use rusoto_core::Region;
use tokio;

/// This example shows how you can use normal Rust functions as QLDB `transact`
/// arguments.
///
/// Lambdas are really nice short in-line transactions but sometimes a function
/// is a better fit.
#[tokio::main]
async fn main() -> Result<()> {
    let driver = QldbDriverBuilder::new()
        .ledger_name("sample-ledger")
        .via_rusoto()
        .region(Region::UsWest2)
        .build()
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
async fn list_table_names<C>(mut tx: TransactionAttempt<C>) -> TransactionResult<Vec<String>>
where
    C: QldbSession + Send + Sync + Clone,
{
    let results = tx
        .execute_statement("select value name from information_schema.user_tables")
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
