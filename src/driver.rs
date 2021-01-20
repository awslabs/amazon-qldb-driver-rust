use crate::api::QldbSessionApi;
use crate::rusoto_ext::*;
use crate::transaction::{TransactionAttempt, TransactionDisposition, TransactionOutcome};
use crate::{
    pool::SessionPool, retry::default_retry_policy, retry::TransactionRetryPolicy, QldbError,
};
use runtime::Runtime;
use rusoto_core::{
    credential::{DefaultCredentialsProvider, ProvideAwsCredentials},
    Client, HttpClient, Region, RusotoError,
};
use rusoto_qldb_session::*;
use std::error::Error as StdError;
use std::{cell::RefCell, future::Future};
use tokio::runtime;

/// A builder to help you customize a [`QldbDriver`].
///
/// In many cases, it is sufficient to use [`QldbDriver::new`] to build a driver out of a Rusoto client for a particular QLDB ledger. However, if you wish to customize the driver beyond the
/// defaults, this builder is what you want.
///
/// Note that the following setters _must_ be called, else [`build`] will return an `Err`:
/// - `ledger_name`
/// - `client`
///
/// Usage example:
/// ```no_run
/// # use amazon_qldb_driver::QldbDriverBuilder;
/// # use rusoto_core::region::Region;
/// # use rusoto_qldb_session::QldbSessionClient;
/// #
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = QldbSessionClient::new(Region::UsWest2);
/// let driver = QldbDriverBuilder::new()
///     .ledger_name("sample-ledger")
///     .region(Region::UsEast1)
///     .build()?;
/// # Ok(())
/// # }
/// ```
pub struct QldbDriverBuilder {
    ledger_name: Option<String>,
    credentials_provider: BoxedCredentialsProvider,
    region: Option<Region>,
    transaction_retry_policy: Box<dyn TransactionRetryPolicy>,
    max_concurrent_transactions: usize,
}

impl Default for QldbDriverBuilder {
    fn default() -> Self {
        QldbDriverBuilder {
            ledger_name: None,
            credentials_provider: into_boxed(
                DefaultCredentialsProvider::new().expect("failed to create credentials provider"),
            ),
            region: None,
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

    pub fn credentials_provider<P>(mut self, credentials_provider: P) -> Self
    where
        P: ProvideAwsCredentials + Send + Sync + 'static,
    {
        self.credentials_provider = into_boxed(credentials_provider);
        self
    }

    pub fn region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }

    pub fn transaction_retry_policy<P>(mut self, transaction_retry_policy: P) -> Self
    where
        P: TransactionRetryPolicy + Send + Sync + 'static,
    {
        self.transaction_retry_policy = Box::new(transaction_retry_policy);
        self
    }

    pub fn max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_concurrent_transactions = max_sessions;
        self
    }

    pub fn build(self) -> Result<QldbDriver, Box<dyn StdError>> {
        let ledger_name = self.ledger_name.ok_or("ledger_name must be initialized")?;
        let client = Client::new_with(self.credentials_provider, default_dispatcher()?);
        let region = self.region.unwrap_or(Region::default());
        let qldb_client = QldbSessionClient::new_with_client(client, region);

        Ok(QldbDriver {
            ledger_name: ledger_name.clone(),
            client: qldb_client.clone(),
            session_pool: SessionPool::new(
                qldb_client.clone(),
                ledger_name.clone(),
                self.max_concurrent_transactions,
            ),
            transaction_retry_policy: self.transaction_retry_policy,
        })
    }
}

fn default_dispatcher() -> Result<HttpClient, Box<dyn StdError>> {
    let mut client = HttpClient::new()?;
    client.local_agent(format!(
        "QLDB Driver for Rust v{}",
        env!("CARGO_PKG_VERSION")
    ));
    Ok(client)
}

// FIXME: Make trait, make Clone/Sync?
pub struct QldbDriver {
    ledger_name: String,
    client: QldbSessionClient,
    session_pool: SessionPool,
    transaction_retry_policy: Box<dyn TransactionRetryPolicy>,
}

impl QldbDriver {
    /// Discovery shortcut: use the builder!
    pub fn builder() -> QldbDriverBuilder {
        QldbDriverBuilder::new()
    }

    pub fn ledger_name(&self) -> String {
        self.ledger_name.clone()
    }

    /// Execute a transaction against QLDB, retrying as necessary.
    ///
    /// This function is the primary way you should interact with QLDB. The driver will acquire a connection and open a [`Transaction`], handing it to your code (the closure you pass
    /// in). The driver will run your transaction and attempt to commit it. While executing the transaction, failures may occur. The driver will retry (acquire a new transaction and run your
    /// code again). This means your code *must be idempotent*.
    ///
    /// If you wish to get results out of your transaction, you must return them from your closure. Do not attempt to write to variables outside of your closure! You should consider code
    /// running inside the closure as seeing speculative results that are only confirmed once the transaction commits.
    ///
    /// Here is some example code:
    ///
    /// ```no_run
    /// use tokio;
    /// use amazon_qldb_driver::QldbDriver;
    /// use rusoto_core::region::Region;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let driver = QldbDriver::builder().ledger_name("sample-ledger").build()?;
    ///     let (a, b) = driver.transact(|mut tx| async {
    ///         let a = tx.execute_statement("SELECT 1").await?;
    ///         let b = tx.execute_statement("SELECT 2").await?;
    ///         tx.ok((a, b)).await
    ///     }).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// Note that the `transaction` argument is `Fn` not `FnOnce` or `FnMut`. This is to support retries (of the entire transaction) and ensure that your function is idempotent (cannot
    /// mutate the environment it captures). For example the following will not compile:
    ///
    /// ```compile_fail
    /// # use tokio;
    /// # use amazon_qldb_driver::QldbDriver;
    /// # use rusoto_core::region::Region;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #   let driver = QldbDriver::builder().ledger_name("sample-ledger").build()?;
    ///     let mut string = String::new();
    ///     driver.transact(|tx| async {
    ///        string.push('1');
    ///        tx.ok(()).await
    ///     }).await?;
    /// #
    /// #   Ok(())
    /// # }
    /// ```
    ///
    /// Again, this is because `transaction` is `Fn` not `FnMut` and thus is not allowed to mutate `string`.
    pub async fn transact<F, Fut, R>(&self, transaction: F) -> Result<R, Box<dyn StdError>>
    where
        Fut: Future<Output = Result<TransactionOutcome<R>, Box<dyn StdError>>>,
        F: Fn(TransactionAttempt) -> Fut,
    {
        let mut attempt_number = 0u32;

        loop {
            attempt_number += 1;

            let session_handle = self.session_pool.next().await?;
            let session_token = &session_handle.session_token;

            let (tx, mut receiver) = match TransactionAttempt::start(
                self.client.clone(),
                session_handle.session_token.clone(),
            )
            .await
            {
                Ok(tx) => tx,
                Err(qldb_err) => {
                    if let QldbError::Rusoto(RusotoError::Service(SendCommandError::BadRequest(
                        message,
                    ))) = qldb_err
                    {
                        debug!(
                            "unable to start a transaction on session {} (will be discarded): {}",
                            session_token, message
                        );
                        session_handle.notify_invalid();
                    }

                    // FIXME: Include some sort of sleep and attempt cap.
                    continue;
                }
            };
            let tx_id = tx.id.clone();

            // Run the user's transaction. They can run methods on [`Transaction`] such as [`execute_statement`]. When this future completes, one of 4 things could have happened:
            //
            // 1. The user code ended with `tx.ok(R)`. This means we get instructions back to attempt a commit. If the commit succeeds, the user gets their data.
            // 2. The user code ended with `tx.abort(R)`. Similar story, but we attempt to abort. Even if the abort fails, the user gets their data back.
            // 3. The user code returned an `Err`:
            //    a. If it is a QldbError that is retriable (e.g. consider a communications failure), then the transaction is retried.
            //    b. Otherwise, we return the error to the user.
            let result = match transaction(tx).await {
                Ok(a) => {
                    let mut execution_stats = a.execution_stats.clone();

                    // This should never happen, but because we move the Transaction into the closure, we need to assert the user returns a `TransactionAttempt` for that same transaction!
                    if a.tx_id != tx_id {
                        return Err(QldbError::UsageError(format!("the call to transact passed your code a Transaction with id {} but you returned instructions for Transaction with id {}", tx_id, a.tx_id)))?;
                    }

                    match a.disposition {
                        TransactionDisposition::Abort => {
                            debug!("transaction {} will be aborted", a.tx_id);

                            // If a user calls abort and the API request fails, simply ignore the failure. There are a couple of reasons for this choice.
                            //
                            // First off, failure is inevitable. There may be communication failure (e.g. WiFi is spotty), API failure (e.g. request throttling) or usage failure
                            // (e.g. session or transaction has timed out). It may be that the transaction is already aborted, or eventually will be due to server-side timeouts.
                            //
                            // Next, safety. It is possible that a failure to abort the transaction here may lead to a problem with the next transaction. For example, consider a network
                            // error such that the server doesn't receive the abort (transaction is still open server-side). When the session is re-used, the next StartTransaction request
                            // might fail ("transaction already open"). This is not a safety issue, but it will cause user-level failures as BadRequests are typically not retried.
                            //
                            // So, we take the pragmatic approach and mark the session as invalid. This prevents it being returned to the pool.
                            match self.client.abort_transaction(session_token).await {
                                Ok(r) => {
                                    execution_stats.accumulate(&r);
                                }
                                Err(e) => {
                                    debug!("ignoring failure to abort tx {}: {}", a.tx_id, e);
                                    session_handle.notify_invalid();
                                }
                            };

                            // If a user calls abort, they will get `Ok(user_data)` returned. This does not allow a disambiguation between data that was committed (verified by OCC) versus
                            // data that was captured in an aborted transaction. Consider an alternative API where the Result Ok variant is an Enum with either `Committed(data)` or
                            // `Uncommitted(data)`. More clear, but also much more annoying to work with. Because this API is generic over `R`, we leave the commit/abort wrapping up to
                            // users.
                            return Ok(a.user_data); // TODO: return stats
                        }
                        TransactionDisposition::Commit => {
                            debug!("transaction {} will be committed", a.tx_id);
                            Ok(a.user_data)
                        }
                    }
                }
                Err(e) => {
                    debug!("transaction {} failed with error: {}", tx_id, e);

                    match e.downcast::<QldbError>() {
                        Ok(qldb_err) => {
                            // This error will flow through the next match statement to the error handling block at the bottom. It would be cleaner to extract and call and error handler
                            // here, but that function lands up capturing so many variables it's not worth it.
                            Err(*qldb_err)
                        }
                        // This branch means the transaction block failed with a non-QldbError. This means something unrelated to QLDB went wrong.
                        Err(other) => {
                            return Err(other);
                        }
                    }
                }
            };

            // There are two possibilities at this point in the code. Either the block of code `transaction` ended with [`Transaction.ok`] or a [`QldbError`] was encountered. The other cases
            // are already handled:
            //
            // 1. [`Transaction.abort`] will have returned the data
            // 2. Another disposition will be compile-fail
            // 3. Another error will return the `Err` to the user
            //
            // So all that's left to do is commit (for the `ok` case) and then go into error handling (for the QldbError case OR if the commit fails).
            let attempt = match result {
                Ok(user_data) => {
                    // TODO: Assert the digest is for this transaction. Maybe that the channel is closed and also send the tx id with the digest?
                    match receiver.recv().await {
                        Some(commit_digest) => self
                            .client
                            .commit_transaction(session_token, tx_id.clone(), commit_digest.bytes())
                            .await
                            .and_then(|_| Ok(user_data)),
                        None => {
                            return Err(QldbError::IllegalState(format!("Attempting to commit transaction {} but the channel holding the commit digest was closed", tx_id)))?;
                        }
                    }
                }
                _ => result,
            };

            // At this point, `attempt` is either `Ok(R)` (in which case we return it to the user - this is a successful commit), or it is an error. The error may have come before or after
            // the attempt to commit. Either way, we need to figure out if the session should be marked invalid and/or if we should retry from the top.
            match attempt {
                Ok(user_data) => return Ok(user_data),
                Err(e) => {
                    // Note: This catch block is after the attempt to commit. We also have to call `notify_valid` before this attempt for the case where one of the commands send during
                    // execution of the user code ran into the ISE. Also note that we should not even attempt to commit if the previous error was an ISE! Currently this is taken care of by
                    // not retrying at all (when user code fails; we only retry on OCC during commit).
                    if let QldbError::Rusoto(RusotoError::Service(
                        SendCommandError::InvalidSession(_),
                    )) = e
                    {
                        session_handle.notify_invalid();
                    }

                    let should_retry = self
                        .transaction_retry_policy
                        .on_err(&e, attempt_number)
                        .await;
                    if should_retry {
                        debug!(
                            "Error comitting ({}) on attempt {}, will retry",
                            e, attempt_number
                        );
                    } else {
                        debug!(
                            "Not retrying after {} attempts due to error: {}",
                            attempt_number, e
                        );

                        return Err(e)?;
                    }
                }
            }

            // Here we retry. We're in a loop, remember!
        }
    }

    pub fn into_blocking(self) -> Result<BlockingQldbDriver, Box<dyn std::error::Error>> {
        BlockingQldbDriver::new(self)
    }
}

pub struct BlockingQldbDriver {
    async_driver: QldbDriver,
    runtime: RefCell<Runtime>,
}

impl BlockingQldbDriver {
    fn new(async_driver: QldbDriver) -> Result<BlockingQldbDriver, Box<dyn std::error::Error>> {
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(BlockingQldbDriver {
            async_driver: async_driver,
            runtime: RefCell::new(runtime),
        })
    }

    pub fn transact<F, Fut, R>(&self, transaction: F) -> Result<R, Box<dyn StdError>>
    where
        Fut: Future<Output = Result<TransactionOutcome<R>, Box<dyn StdError>>>,
        F: Fn(TransactionAttempt) -> Fut,
    {
        let runtime = self.runtime.borrow();
        let fun = &transaction;
        runtime.block_on(async move { self.async_driver.transact(fun).await })
    }
}
