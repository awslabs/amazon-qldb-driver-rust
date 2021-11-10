use std::convert::Infallible;

use amazon_qldb_driver_core::{QldbDriver, QldbDriverBuilder, TransactionAttempt};
use anyhow::Result;
use tokio::{self, spawn};

/// This example shows how you can implement a unique constraint in QLDB. QLDB's
/// concurrency control makes this easy!
///
/// First, let's pretend there is no concurrency. To implement uniqueness, all
/// you need to do is:
///
/// 1. Check if the value already exists
/// 2. If not, insert it
///
/// The presence of concurrency makes this no harder. QLDB uses optimistic
/// concurrency control (OCC) to automatically detect if another transaction
/// interfered with yours. If so, the driver will automatically try again.
///
/// For example, consider two transactions "racing" to insert a value. We can
/// visualize this as a table, with transaction 1 on the left and transaction 2
/// on the right:
///
/// | # | transaction 1    | transaction 2    |
/// |---|:-----------------|:-----------------|
/// | 1 | start            | start            |
/// | 2 | exists? -> false | exists? -> false |
/// | 3 | insert           | insert           |
/// | 4 | commit -> true   | commit -> OCC    |
///
/// In the above example, transaction 1 won the race. However, transaction 2
/// didn't know it was going to lose until the call to commit (step 4) failed!
// by default, this is a multi-threaded runtime and uses as many threads as you have CPUs
#[tokio::main]
async fn main() -> Result<()> {
    // Run me with `export RUST_LOG=debug` for more output!
    tracing_subscriber::fmt::init();

    // First we initialize a QLDB driver. We're going to connect to a specific
    // ledger named 'unique-example' in us-west-2. Feel free to change the
    // ledger name or region as you see fit!
    //
    // The ledger must exist prior to running this example and should have a
    // single table named 'example' with an index on 'id'. You can create a
    // ledger easily with the AWS CLI:
    //
    // ```sh
    // aws --region us-west-2 qldb create-ledger --name unique-example --permissions ALLOW_ALL
    // ```
    //
    // And you can create the expected schema with the QLDB shell:
    //
    // ```sh
    // $ qldb --ledger unique-example --region us-west-2
    //
    // qldb> create table example
    // qldb> create index on example (id)
    // ```
    //
    // If you run this example multiple times, make sure you empty or re-create
    // the table in between attempts:
    //
    // ```sh
    // qldb> delete from example
    // ```
    let aws_config = aws_config::load_from_env().await;

    let driver = QldbDriverBuilder::new()
        .ledger_name("unique-example")
        .sdk_config(&aws_config)
        .await?;

    // Next up, we'll run 100 concurrent transactions. Each of them will run the
    // following statements:
    //
    // 1. Check if an item with `id=1` exists in the table
    // 2. If so, insert it and return true (this signifies the race is won!)
    // 3. Otherwise, return false
    //
    // Only one transaction should win the race. Play around with the
    // concurrency settings by either changing the number of concurrent
    // transactions or explicitly control the async runtime (see the
    // `tokio::main` annotation).
    let handles: Vec<_> = (0..100)
        .map(|i| {
            // Each task may run on its own thread. The QLDB driver is
            // clone-friendly, which allows each thread to appear to have full
            // access to the driver after appearing to get its own copy.
            let driver = driver.clone();

            // If you're reading this top-down, you should now jump to the
            // function `example_transaction` to see what happens inside this
            // task.
            spawn(async move { example_transaction(driver, i) })
        })
        .collect();

    // Figure out which task won the race. There should never be more than 1
    // winner, but it's OK to have 0 winners if the table wasn't empty to begin
    // with!
    //
    // What you may find when running this example is that task 0 wins more
    // often than not. The reason for that is it gets a head-start! It's the
    // first to get a thread, get a QLDB session, and so on. Try play around
    // with adding delay to prove to yourself that another task *could* win.

    let mut race_won = false;
    let mut races_lost: usize = 0;

    for handle in handles {
        let (winner, task_id) = handle.await?.await?;
        if winner {
            if race_won {
                panic!("only 1 transaction should ever win the race");
            } else {
                race_won = true;
                println!("task {} won the race!", task_id);
            }
        } else {
            races_lost = races_lost + 1;
        }
    }

    println!(
        "{} other transactions attempted but were beaten to the punch",
        races_lost
    );

    Ok(())
}

/// Run our sample transaction. We're given our own copy of the driver and a
/// task id and we must return that same task id as well as a boolean indicating
/// whether or not we won the race. We win the race if we're the transaction
/// that inserts the example document.
///
/// Take special note of the fact that we *always* call `tx.commit`. In QLDB, it
/// is really important to call `commit` because that's when optimistic
/// concurrency control is done. Any data you've seen before `commit` is
/// speculative.
///
/// In our example, the "state machine" of the example document transitions from
/// "DOES NOT EXIST" to "EXISTS" and that's it. So, you may be tempted to think
/// that if `check.len() != 0` then the document exists and there is no need to
/// do any futher work (including calling abort).
///
/// However, consider that our state machine actually has a loop! We *might*
/// delete the record and run the example again. In that case, the check
/// statement might return 1 if it still observes the existence of the previous
/// example run (i.e. that the delete did not yet reflect).
///
/// How can this happen? QLDB is a Journal-first database. Transactions are
/// first written to the Journal and then "indexed storage" is updated to
/// reflect those changes. Writes go to the Journal first, queries go from
/// indexed storage. So, when we run our check query, we're actually querying
/// the indexed storage which may or may not reflect the results of all writes.
///
/// In practice, the QLDB indexed storage is usually instantly up to date. It is
/// highly unlikely you can trigger this behavior by running this example
/// quickly and emptying the table in between!
///
/// It is a best practice in QLDB to **always call commit**. In QLDB, there is
/// no reason to take a lock or check rows updated with a conditional.
/// Concurrency in QLDB is simple. Write your code as if you're the only actor
/// in the system **and then call commit**.
async fn example_transaction(driver: QldbDriver, task_id: u32) -> Result<(bool, u32)> {
    let winner = driver
        .transact(|mut tx: TransactionAttempt<Infallible>| async {
            let check = tx
                .execute_statement("select * from example where id = 1")
                .await?
                .buffered()
                .await?;
            if check.len() == 0 {
                let _ = tx
                    .execute_statement("insert into example `{id: 1}`")
                    .await?;
                tx.commit(true).await
            } else {
                // Note that we always call commit, even though we're not
                // inserting the example document here. The `false` argument is
                // not sent to QLDB. Rather, it becomes the return value of the
                // `transact` method (bound to the variable `winner`) **only
                // after the commit succeeds**. In this way, if `winner` is
                // assigned to, you are guaranteed that your transaction was
                // serialized with other changes to the ledger.
                tx.commit(false).await
            }
        })
        .await?;

    Ok((winner, task_id))
}
