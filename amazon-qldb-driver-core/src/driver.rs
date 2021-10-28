use anyhow::Result;
use aws_hyper::{DynConnector, SmithyConnector};
use aws_sdk_qldbsessionv2::{Client, Config};
use bb8::Pool;
use std::sync::Arc;
use std::{future::Future, time::Duration};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::debug;

use crate::error::{self, QldbError, QldbResult};
use crate::transaction::TransactionAttempt;
use crate::{pool::QldbErrorLoggingErrorSink, transaction::TransactionAttemptResult};
use crate::{
    pool::QldbSessionV2Manager, retry::default_retry_policy, retry::TransactionRetryPolicy,
};

pub struct QldbDriverBuilder {
    ledger_name: Option<String>,
    transaction_retry_policy: Box<dyn TransactionRetryPolicy + Send + Sync>,
    max_concurrent_transactions: u32,
}

impl Default for QldbDriverBuilder {
    fn default() -> Self {
        QldbDriverBuilder {
            ledger_name: None,
            transaction_retry_policy: Box::new(default_retry_policy()),
            max_concurrent_transactions: 1500,
        }
    }
}

impl QldbDriverBuilder {
    pub fn new() -> Self {
        QldbDriverBuilder::default()
    }

    pub fn ledger_name<S>(mut self, ledger_name: S) -> Self
    where
        S: Into<String>,
    {
        self.ledger_name = Some(ledger_name.into());
        self
    }

    pub fn transaction_retry_policy<P>(mut self, transaction_retry_policy: P) -> Self
    where
        P: TransactionRetryPolicy + Send + Sync + 'static,
    {
        self.transaction_retry_policy = Box::new(transaction_retry_policy);
        self
    }

    pub fn max_sessions(mut self, max_sessions: u32) -> Self {
        self.max_concurrent_transactions = max_sessions;
        self
    }

    /// Build a `QldbDriver` using the AWS SDK for Rust.
    ///
    /// `sdk_config` will be used to create a configured instance of the SDK.
    /// Note that this configuration uses the "sdk config" as opposed to the
    /// QLDB specific config.
    pub async fn sdk_config(
        self,
        sdk_config: &aws_types::config::Config,
    ) -> QldbResult<QldbDriver<DynConnector>> {
        let client = Client::new(sdk_config);
        self.build_with_client(client).await
    }

    /// Builds a `QldbDriver` using the AWS SDK for Rust.
    ///
    /// Note that `config` is the service-specific (QldbSession) config. For
    /// shared config, see [`sdk_config`].
    pub async fn config(self, config: Config) -> QldbResult<QldbDriver<DynConnector>> {
        let client = Client::from_conf(config);
        self.build_with_client(client).await
    }

    pub async fn build_with_client<C>(self, client: Client<C>) -> QldbResult<QldbDriver<C>>
    where
        C: SmithyConnector,
    {
        let ledger_name = self
            .ledger_name
            .ok_or(error::usage_error("ledger_name must be initialized"))?;

        let transaction_retry_policy = Arc::new(Mutex::new(self.transaction_retry_policy));

        let session_pool = Pool::builder()
            .test_on_check_out(false)
            .max_lifetime(None)
            .max_size(self.max_concurrent_transactions)
            .connection_timeout(Duration::from_secs(10))
            .error_sink(Box::new(QldbErrorLoggingErrorSink::new()))
            .build(QldbSessionV2Manager::new(client, ledger_name.clone()))
            .await
            .map_err(|_| error::todo_stable_error_api())?;

        Ok(QldbDriver {
            ledger_name: Arc::new(ledger_name.clone()),
            session_pool: Arc::new(session_pool),
            transaction_retry_policy,
        })
    }
}

/// ## Concurrency
///
/// End users of the driver should call `clone` and drive concurrency off the
/// clones. Under the hood, no actual copy is made. The driver will pool
/// sessions across clones.
///
/// The work the QLDB driver does is:
///
/// 1. Running user transaction logic
/// 2. Communicating with the QLDB service
///
/// Running user code requires true concurrency, as the code could do anything
/// (by design). For example, it might be perfectly reasonable to start a
/// transaction and do some CPU bound work for a few seconds before proceeding
/// to update some value. To scale this out, you need real OS threads.
///
/// However, communicating with the service is almost entirely IO bound and sits
/// on top of an async based stack.
///
/// The intersection is a little tricky. A transaction needs a session and if it
/// encounters an error it needs to know if it should retry. If we have true
/// concurrency of transactions, then does that imply our session pool and retry
/// policies also need to be `Send + Sync`?
///
/// The session pool is internal to the driver. It is a requirement that the
/// same pool be used across threads, and thus the pool must be `Send + Sync`.
/// You can read the documentation on [`SessionPool`] to see how we achieve
/// that.
///
/// Retry policies are not internal to the driver (even though there are default
/// policies). An important design question is whether or not a retry policy
/// should be able to make a decision based on all in-flight (or past)
/// transactions. If the answer is 'no', then we could make the decision that
/// every thread using the driver gets a clone of the policy. This would enable
/// common use-cases like backoff-jitter, but prevent more complicated use-cases
/// such as circuit breakers. Both of these answers suck, so instead of picking
/// we wrap all policies in a Mutex. Performance of a Mutex is unlikely to be
/// the limiting factor in a QLDB application.
#[derive(Clone)]
pub struct QldbDriver<C>
where
    C: SmithyConnector,
{
    pub ledger_name: Arc<String>,
    session_pool: Arc<Pool<QldbSessionV2Manager<C>>>,
    transaction_retry_policy: Arc<Mutex<Box<dyn TransactionRetryPolicy + Send + Sync>>>,
}

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("transaction was aborted")]
    Aborted,

    #[error("transaction failed after {attempts} attempts, last error: {last_err}")]
    MaxAttempts { last_err: QldbError, attempts: u32 },
}

impl<C> QldbDriver<C>
where
    C: SmithyConnector,
{
    pub fn ledger_name(&self) -> String {
        (*self.ledger_name).clone()
    }

    /// Execute a transaction against QLDB, retrying as necessary.
    ///
    /// This function is the primary way you should interact with QLDB. The
    /// driver will acquire a connection and open a [`Transaction`], handing it
    /// to your code (the closure you pass in). The driver will run your
    /// transaction and attempt to commit it. While executing the transaction,
    /// failures may occur. The driver will retry (acquire a new transaction and
    /// run your code again). This means your code *must be idempotent*.
    ///
    /// If you wish to get results out of your transaction, you must return them
    /// from your closure. Do not attempt to write to variables outside of your
    /// closure! You should consider code running inside the closure as seeing
    /// speculative results that are only confirmed once the transaction
    /// commits.
    pub async fn transact<F, Fut, R>(&self, transaction: F) -> Result<R>
    where
        Fut: Future<Output = Result<TransactionAttemptResult<R>>>,
        F: Fn(TransactionAttempt) -> Fut,
    {
        let mut attempt_number = 0u32;

        loop {
            attempt_number += 1;

            // Note that `PooledConnection` uses the `Drop` trait to hand the
            // connection back to the pool. It'd be really nice to hand the
            // owned connection to the attempt such that the attempt naturally
            // handed the connection back. However, this requires plumbing the
            // manager's generics down, which is ugly, especially since
            // `TransactionAttempt` is customer-facing. Instead, we manually
            // discard the connection asap. This isn't a correctness risk (since
            // Rust will guarantee we do it at some point), but putting the
            // connection back in the pool before sleeping will probably be
            // beneficial.
            let mut pooled_session = self.session_pool.get().await?;

            let tx = match TransactionAttempt::start(&mut pooled_session).await {
                Ok(tx) => tx,
                Err(e) => {
                    // FIXME: Include some sort of sleep and attempt cap.
                    debug!(error = %e, "unable to start a session, will retry");
                    continue;
                }
            };
            let tx_id = tx.id.clone();

            // Run the user's transaction. They can run methods on
            // [`Transaction`] such as [`execute_statement`]. When this future
            // completes, one of 4 things could have happened:
            //
            // 1. The user code ended with `tx.commit(R)`. This means we get
            // instructions back to attempt a commit. If the commit succeeds,
            // the user gets their data and we're done.
            //
            // 2. The user code ended with `tx.abort(R)`. We're done, regardless
            // of whether the API request to `abort` succeeded.
            //
            // 3. The user code returned an `Err`:
            //    a. If it is a QldbError that is retriable (e.g. consider a
            //    communications failure), then the transaction is retried.
            //    b. Otherwise, we return the error to the user. Arbitrary
            //    application types are *not* retried.
            let qldb_err = match transaction(tx).await {
                Ok(TransactionAttemptResult::Committed { user_data, .. }) => return Ok(user_data),
                Ok(TransactionAttemptResult::Aborted) => Err(TransactionError::Aborted)?,
                Err(e) => {
                    debug!(
                        error = %e,
                        id = &tx_id[..],
                        "transaction failed with error"
                    );

                    match e.downcast::<QldbError>() {
                        Ok(qldb_err) => qldb_err,
                        // This branch means the transaction block failed with a
                        // non-QldbError. This means something unrelated to QLDB
                        // went wrong. We don't retry on arbitrary user errors.
                        Err(other) => {
                            return Err(other);
                        }
                    }
                }
            };

            // See above note. We put the connection back in the pool before
            // adding any delay.
            drop(pooled_session);

            let retry_ins = {
                let policy = self.transaction_retry_policy.lock().await;
                policy.on_err(&qldb_err, attempt_number)
            };

            if retry_ins.should_retry {
                debug!(
                    error = %qldb_err,
                    attempt_number, "Error comitting, will retry"
                );
                if let Some(duration) = retry_ins.delay {
                    debug!(
                        delay = duration.as_millis().to_string().as_str(),
                        "Retry will be delayed"
                    );
                    sleep(duration).await;
                }
            } else {
                debug!(
                    "Not retrying after {} attempts due to error: {}",
                    attempt_number, qldb_err
                );

                return Err(TransactionError::MaxAttempts {
                    last_err: qldb_err,
                    attempts: attempt_number,
                })?;
            }

            // Here we retry. We're in a loop, remember!
        }
    }
}

// FIXME: Move to correct crate (not sure where)
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::api::rusoto::testing::TestQldbSessionClient;
//     use anyhow::Result;
//     use tokio::spawn;

//     // This test shows how to clone the driver for use in a multi-threaded tokio
//     // runtime. It's really just a compile test in that no actual transactions
//     // are run with expected results.
//     #[tokio::test(flavor = "multi_thread")]
//     async fn multi_thread_example() -> Result<()> {
//         // this driver will be used concurrently.
//         let mock = TestQldbSessionClient::new();
//         let driver = QldbDriverBuilder::new()
//             .ledger_name("multi_thread_example")
//             .build_with_client(mock.clone())
//             .await?;

//         let fut_1 = spawn({
//             let driver = driver.clone();
//             async move { driver.transact(|tx| async { tx.commit(()).await }).await }
//         });

//         let fut_2 = spawn({
//             let driver = driver.clone();
//             async move { driver.transact(|tx| async { tx.commit(()).await }).await }
//         });

//         fut_1.await??;
//         fut_2.await??;

//         Ok(())
//     }

//     // This test shows how to call abort. There are very few reasons to call
//     // abort in QLDB as nearly all transactions *should* go through a `commit`
//     // to ensure the returned data was not concurrently modified.
//     #[tokio::test]
//     async fn abort_usage() -> Result<()> {
//         // this driver will be used concurrently.
//         let mock = TestQldbSessionClient::new();
//         let driver = QldbDriverBuilder::new()
//             .ledger_name("abort_usage")
//             .build_with_client(mock.clone())
//             .await?;

//         let result = driver
//             .transact(|tx| async {
//                 if 1 > 2 {
//                     tx.commit(42).await
//                 } else {
//                     tx.abort().await
//                 }
//             })
//             .await;

//         let err = result.unwrap_err();
//         if let Ok(TransactionError::Aborted) = err.downcast::<TransactionError>() {
//             // expected
//         } else {
//             panic!("expected an aborted error");
//         }

//         Ok(())
//     }
// }
