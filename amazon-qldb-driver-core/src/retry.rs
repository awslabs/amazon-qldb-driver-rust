use rand::thread_rng;
use rand::Rng;
use std::{cmp::min, time::Duration};
use tracing::debug;
use tracing::error;

use aws_sdk_qldbsessionv2::error::SendCommandError;
use aws_sdk_qldbsessionv2::error::SendCommandErrorKind;
use aws_sdk_qldbsessionv2::SdkError;

use crate::pool::SendStreamingCommandError;

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
    fn on_err(&self, error: &SendStreamingCommandError, attempt_number: u32) -> RetryInstructions;
}

/// Don't try this at home.
pub struct NeverRetryPolicy {}

impl TransactionRetryPolicy for NeverRetryPolicy {
    fn on_err(
        &self,
        _error: &SendStreamingCommandError,
        _attempt_number: u32,
    ) -> RetryInstructions {
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
    fn on_err(&self, error: &SendStreamingCommandError, attempt_number: u32) -> RetryInstructions {
        let should_retry = match error {
            SendStreamingCommandError::SendError(err) => {
                if err.is_full() {
                    // At the time of writing, eventstreaming is being used in
                    // request-response style, i.e. the underlying channel is
                    // bounded to a single inflight request. As a result, the
                    // send channel being full represents a bug.
                    // FIXME: close the connection
                    error!("send channel is full, this should never happen");
                    return RetryInstructions::dont();
                }

                err.is_disconnected()
            }
            SendStreamingCommandError::RecvError(err) => match err {
                SdkError::ServiceError {
                    err: SendCommandError { kind, .. },
                    ..
                } => match kind {
                    SendCommandErrorKind::BadRequestException(_) => false,
                    SendCommandErrorKind::LimitExceededException(_) => true,
                    SendCommandErrorKind::RateExceededException(_) => true,
                    SendCommandErrorKind::CapacityExceededException(_) => true,
                    SendCommandErrorKind::Unhandled(_) => false,
                    _ => false,
                },
                // Construction failures mean that the sdk has rejected the
                // request shape (e.g. violating client-side constraints).
                // There is no point retrying, since the sdk will simply
                // reject again!
                SdkError::ConstructionFailure(_) => false,
                // We retry dispatch failures even though the request *may*
                // have been sent. In QLDB, the commit digest protects
                // against a duplicate statement being sent.
                SdkError::DispatchFailure(_) => true,
                SdkError::ResponseError { raw, .. } => match raw {
                    aws_smithy_http::event_stream::RawMessage::Decoded(decoded) => {
                        // FIXME: Do 500s come back?
                        // match decoded.headers().status().as_u16() {
                        //     500 | 503 => true,
                        //     _ => false,
                        // }
                        true
                    }
                    aws_smithy_http::event_stream::RawMessage::Invalid(_) => {
                        debug!("invalid message, will not retry");
                        false
                    }
                    _ => {
                        debug!("unexpected response, will not retry");
                        false
                    }
                },
            },
            SendStreamingCommandError::ChannelClosed => true,
        };

        if !should_retry || attempt_number > self.max_attempts {
            RetryInstructions::dont()
        } else {
            let delay = exponential_backoff_with_jitter(self.base, self.cap, attempt_number);
            RetryInstructions::after(Duration::from_millis(delay as u64))
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
