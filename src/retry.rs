use crate::QldbError;
use async_trait::async_trait;
use rand::thread_rng;
use rand::Rng;
use rusoto_core::RusotoError;
use rusoto_qldb_session::*;
use std::{cmp::min, time::Duration};
use tokio::time::sleep;

pub fn default_retry_policy() -> impl TransactionRetryPolicy {
    ExponentialBackoffJitterTransactionRetryPolicy::default()
}

pub fn never() -> impl TransactionRetryPolicy {
    NeverRetryPolicy {}
}

/// A retry policy receives an `error` and the `attempt_number` and returns true/false based on whether the driver should retry or not. The function [`on_err`] is marked `async` to allow
/// implementations to model features such as backoff-jitter without blocking the runtime.
#[async_trait]
pub trait TransactionRetryPolicy {
    async fn on_err(&self, error: &QldbError, attempt_number: u32) -> bool;
}

/// Don't try this at home.
pub struct NeverRetryPolicy {}

#[async_trait]
impl TransactionRetryPolicy for NeverRetryPolicy {
    async fn on_err(&self, _error: &QldbError, _attempt_number: u32) -> bool {
        false
    }
}

/// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
pub struct ExponentialBackoffJitterTransactionRetryPolicy {
    base: u32,
    cap: u32,
    max_attempts: u32,
}

impl ExponentialBackoffJitterTransactionRetryPolicy {
    pub fn new(
        base: u32,
        cap: u32,
        max_attempts: u32,
    ) -> ExponentialBackoffJitterTransactionRetryPolicy {
        ExponentialBackoffJitterTransactionRetryPolicy {
            base,
            cap,
            max_attempts,
        }
    }
}

impl Default for ExponentialBackoffJitterTransactionRetryPolicy {
    fn default() -> ExponentialBackoffJitterTransactionRetryPolicy {
        ExponentialBackoffJitterTransactionRetryPolicy::new(10, 5000, 4)
    }
}

#[async_trait]
impl TransactionRetryPolicy for ExponentialBackoffJitterTransactionRetryPolicy {
    async fn on_err(&self, error: &QldbError, attempt_number: u32) -> bool {
        match error {
            QldbError::Rusoto(e) => {
                let should_retry = match &e {
                    RusotoError::Service(service) => match &service {
                        SendCommandError::BadRequest(_) => false,
                        SendCommandError::InvalidSession(_) => true,
                        SendCommandError::LimitExceeded(_) => false,
                        SendCommandError::OccConflict(_) => true,
                        SendCommandError::RateExceeded(_) => false,
                    },
                    RusotoError::HttpDispatch(_) => true,
                    RusotoError::Credentials(_) => false,
                    RusotoError::Validation(_) => false,
                    RusotoError::ParseError(_) => false,
                    RusotoError::Unknown(r) => match r.status.as_u16() {
                        500 | 503 => true,
                        _ => false,
                    },
                    RusotoError::Blocking => false,
                };

                if !should_retry || attempt_number > self.max_attempts {
                    return false;
                } else {
                    let delay =
                        exponential_backoff_with_jitter(self.base, self.cap, attempt_number);
                    sleep(Duration::from_millis(delay as u64)).await;

                    return true;
                }
            }
            _ => return false,
        }
    }
}

fn exponential_backoff_with_jitter(base: u32, cap: u32, attempt_number: u32) -> u32 {
    let max = min(cap, base.pow(attempt_number));
    thread_rng().gen_range(0..max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_with_jitter_seq() {
        let ExponentialBackoffJitterTransactionRetryPolicy {
            base,
            cap,
            max_attempts,
        } = ExponentialBackoffJitterTransactionRetryPolicy::default();

        let mut seq = vec![0..10, 0..100, 0..1000, 0..5000]; // not super tight
        seq.reverse();

        for attempt_number in 1..=max_attempts {
            let sleep = exponential_backoff_with_jitter(base, cap, attempt_number);
            let expected = seq.pop().unwrap();
            assert!(
                expected.contains(&(sleep as u32)),
                "on attempt {} we should be sleeping in the range {:?} but we calculated {}",
                attempt_number,
                expected,
                sleep
            );
        }

        assert!(seq.is_empty());
    }
}
