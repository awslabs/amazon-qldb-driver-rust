use amazon_qldb_driver::ion_compat;
use amazon_qldb_driver::QldbDriverBuilder;
use rusoto_core::Region;
use tokio;
#[macro_use]
extern crate log;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    info!("Creating a QLDB driver");
    let driver = QldbDriverBuilder::new()
        .ledger_name("sample-ledger")
        .region(Region::UsWest2)
        .build()?;

    // Usage example 1: Here we use a closure that returns a `Result<R, QldbError>`. The closure is wrapped in ceremony to appease the type system.
    info!("Transaction example 1 now running");
    let results = driver
        .transact(|mut tx| async {
            let results = tx
                .execute_statement("select value 42 from information_schema.user_tables")
                .await?;

            tx.ok(results).await
        })
        .await?;
    info!("Tx 1 returned {} result(s):", results.len());
    for reader in results.readers() {
        let pretty = ion_compat::to_string_pretty(reader?)?;
        info!("{}", pretty);
    }

    // No support for Ion yet!
    // assert_eq!(42, value);

    info!(
        "Statement executed in {}ms and used {} read IOs",
        results
            .execution_stats()
            .timing_information
            .processing_time_milliseconds,
        results.execution_stats().io_usage.read_ios
    );

    info!("Goodbye!");

    Ok(())
}
