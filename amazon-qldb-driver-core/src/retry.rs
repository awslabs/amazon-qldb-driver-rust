use crate::error::QldbError;
use aws_sdk_qldbsession::error::SendCommandError;
use aws_sdk_qldbsession::error::SendCommandErrorKind;
use aws_sdk_qldbsession::SdkError;
use rand::thread_rng;
use rand::Rng;
use std::{cmp::min, time::Duration};

pub fn default_retry_policy() -> impl TransactionRetryPolicy {
    ExponentialBackoffJitterTransactionRetryPolicy::default()
}

pub fn never() -> impl TransactionRetryPolicy {
    NeverRetryPolicy {}
}

pub struct RetryInstructions {
    pub should_retry: bool,
    pub delay: Option<Duration>,
}

impl RetryInstructions {
    fn dont() -> RetryInstructions {
        RetryInstructions {
            should_retry: false,
            delay: None,
        }
    }

    fn after(delay: Duration) -> RetryInstructions {
        RetryInstructions {
            should_retry: true,
            delay: Some(delay),
        }
    }
}

/// A retry policy receives an `error` and the `attempt_number` and returns true/false based on whether the driver should retry or not. The function [`on_err`] is marked `async` to allow
/// implementations to model features such as backoff-jitter without blocking the runtime.
pub trait TransactionRetryPolicy {
    fn on_err(&self, error: &QldbError, attempt_number: u32) -> RetryInstructions;
}

/// Don't try this at home.
pub struct NeverRetryPolicy {}

impl TransactionRetryPolicy for NeverRetryPolicy {
    fn on_err(&self, _error: &QldbError, _attempt_number: u32) -> RetryInstructions {
        RetryInstructions::dont()
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

impl TransactionRetryPolicy for ExponentialBackoffJitterTransactionRetryPolicy {
    fn on_err(&self, error: &QldbError, attempt_number: u32) -> RetryInstructions {
        match error {
            QldbError::SdkError(e) => {
                let should_retry = match &e {
                    SdkError::ServiceError {
                        err: SendCommandError { kind, .. },
                        ..
                    } => match kind {
                        SendCommandErrorKind::BadRequestException(_) => false,
                        SendCommandErrorKind::InvalidSessionException(_) => true,
                        SendCommandErrorKind::LimitExceededException(_) => false,
                        SendCommandErrorKind::OccConflictException(_) => true,
                        SendCommandErrorKind::RateExceededException(_) => false,
                        SendCommandErrorKind::CapacityExceededException(_) => true,
                        SendCommandErrorKind::Unhandled(_) => false,
                        _ => false,
                    },
                    // Construction failures mean that the sdk has rejected the
                    // request shape (e.g. violating client-side constraints).
                    // There is no point retrying, since the sdk will simply
                    // reject again!
                    SdkError::ConstructionFailure(_) => false,
                    // We retry dispatch and timeout failures even though the request *may*
                    // have been sent. In QLDB, the commit digest protects
                    // against a duplicate statement being sent.
                    SdkError::DispatchFailure(_) | SdkError::TimeoutError(_) => true,
                    SdkError::ResponseError { raw, .. } => match raw.http().status().as_u16() {
                        500 | 503 => true,
                        _ => false,
                    },
                };

                if !should_retry || attempt_number > self.max_attempts {
                    return RetryInstructions::dont();
                } else {
                    let delay =
                        exponential_backoff_with_jitter(self.base, self.cap, attempt_number);
                    return RetryInstructions::after(Duration::from_millis(delay as u64));
                }
            }
            _ => return RetryInstructions::dont(),
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

    #[test]
    fn policies_are_send_and_sync() {
        fn is_send_and_sync<T: Send + Sync>() {}

        is_send_and_sync::<NeverRetryPolicy>();
        is_send_and_sync::<ExponentialBackoffJitterTransactionRetryPolicy>();
    }
}
