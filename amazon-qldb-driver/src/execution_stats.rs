use aws_sdk_qldbsessionv2::model;

// public (stable) types for execution stats.
//
// First, we don't want to leak our implementation details (that we use Rusoto)
// to customers. Doing so would make changing the underlying SDK in the future
// harder. Second, while the API models all values as optional, in reality QLDB
// always returns execution stats.

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionStats {
    pub timing_information: model::TimingInformation,
    pub io_usage: model::IoUsage,
}

impl ExecutionStats {
    pub fn accumulate<R>(&mut self, other: &R)
    where
        R: HasExecutionStats,
    {
        let timing = other.timing_information();
        self.timing_information.processing_time_milliseconds += timing.processing_time_milliseconds;

        let io = other.io_usage();
        self.io_usage.read_i_os += io.read_i_os;
        self.io_usage.write_i_os += io.write_i_os;
    }

    // Can't use From here because it conflicts with the stdlib.
    pub(crate) fn from_api<S>(api: S) -> ExecutionStats
    where
        S: HasExecutionStats,
    {
        ExecutionStats {
            timing_information: api.timing_information(),
            io_usage: api.io_usage(),
        }
    }
}

impl Default for ExecutionStats {
    fn default() -> Self {
        ExecutionStats {
            timing_information: model::TimingInformation::builder().build(),
            io_usage: model::IoUsage::builder().build(),
        }
    }
}
pub trait HasExecutionStats {
    fn timing_information(&self) -> model::TimingInformation;
    fn io_usage(&self) -> model::IoUsage;

    fn extract_owned(&self) -> ExecutionStats
    where
        Self: Sized,
    {
        let mut new = ExecutionStats::default();
        new.accumulate(self);
        new
    }
}

impl HasExecutionStats for ExecutionStats {
    fn timing_information(&self) -> model::TimingInformation {
        self.timing_information.clone()
    }

    fn io_usage(&self) -> model::IoUsage {
        self.io_usage.clone()
    }
}

/// Implements the execution stats API for types that only have timing
/// information.
macro_rules! impl_execution_stats_1 {
        ($($t:ty),*) => ($(
            impl HasExecutionStats for $t {
                fn timing_information(&self) -> model::TimingInformation {
                    self.timing_information.clone().unwrap_or(model::TimingInformation::builder().build())
                }

                fn io_usage(&self) -> model::IoUsage {
                    model::IoUsage::builder().build()
                }
            }
        )*)
    }

impl_execution_stats_1!(model::StartTransactionResult, model::AbortTransactionResult);

/// Implements for types that also have IO usage.
macro_rules! impl_execution_stats_2 {
        ($($t:ty),*) => ($(
            impl HasExecutionStats for $t {
                fn timing_information(&self) -> model::TimingInformation {
                    self.timing_information.clone().unwrap_or(model::TimingInformation::builder().build())
                }

                fn io_usage(&self) -> model::IoUsage {
                    self.consumed_i_os.clone().unwrap_or(model::IoUsage::builder().build())
                }
            }
        )*)
    }

impl_execution_stats_2!(
    model::ExecuteStatementResult,
    model::FetchPageResult,
    model::CommitTransactionResult
);
