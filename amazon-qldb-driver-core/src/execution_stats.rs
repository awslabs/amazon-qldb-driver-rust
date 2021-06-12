use aws_sdk_qldbsession::model;

// public (stable) types for execution stats.
//
// First, we don't want to leak our implementation details (that we use Rusoto)
// to customers. Doing so would make changing the underlying SDK in the future
// harder. Second, while the API models all values as optional, in reality QLDB
// always returns execution stats.

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionStats {
    pub timing_information: model::TimingInformation,
    pub io_usage: model::IOUsage,
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
}

impl Default for ExecutionStats {
    fn default() -> Self {
        ExecutionStats {
            timing_information: model::TimingInformation::builder().build(),
            io_usage: model::IOUsage::builder().build(),
        }
    }
}
pub trait HasExecutionStats {
    fn timing_information(&self) -> model::TimingInformation;
    fn io_usage(&self) -> model::IOUsage;
}

impl ExecutionStats {
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

impl HasExecutionStats for ExecutionStats {
    fn timing_information(&self) -> model::TimingInformation {
        self.timing_information.clone()
    }

    fn io_usage(&self) -> model::IOUsage {
        self.io_usage.clone()
    }
}

pub(crate) mod rusoto_support {
    use super::*;
    use rusoto_qldb_session::IOUsage as RusotoIOUsage;
    use rusoto_qldb_session::TimingInformation as RusotoTimingInformation;
    use rusoto_qldb_session::*;

    fn map_rusoto_timing_information(
        rusoto: &Option<RusotoTimingInformation>,
    ) -> model::TimingInformation {
        rusoto
            .as_ref()
            .map(|info| {
                model::TimingInformation::builder()
                    .processing_time_milliseconds(
                        info.processing_time_milliseconds.unwrap_or_default(),
                    )
                    .build()
            })
            .unwrap_or(model::TimingInformation::builder().build())
    }

    fn map_rusoto_io_usage(rusoto: &Option<RusotoIOUsage>) -> model::IOUsage {
        rusoto
            .as_ref()
            .map(|io| {
                model::IOUsage::builder()
                    .read_i_os(io.read_i_os.unwrap_or_default())
                    .write_i_os(io.write_i_os.unwrap_or_default())
                    .build()
            })
            .unwrap_or(model::IOUsage::builder().build())
    }

    /// Implements the execution stats API for types that only have timing
    /// information.
    macro_rules! impl_execution_stats_for_rusoto_1 {
    ($($t:ty),*) => ($(
        impl HasExecutionStats for $t {
            fn timing_information(&self) -> model::TimingInformation {
                map_rusoto_timing_information(&self.timing_information)
            }

            fn io_usage(&self) -> model::IOUsage {
                model::IOUsage::builder().build()
            }
        }
    )*)
}

    impl_execution_stats_for_rusoto_1!(StartTransactionResult, AbortTransactionResult);

    /// Implements for types that also have IO usage.
    macro_rules! impl_execution_stats_for_rusoto_2 {
    ($($t:ty),*) => ($(
        impl HasExecutionStats for $t {
            fn timing_information(&self) -> model::TimingInformation {
                map_rusoto_timing_information(&self.timing_information)
            }

            fn io_usage(&self) -> model::IOUsage {
                map_rusoto_io_usage(&self.consumed_i_os)
            }
        }
    )*)
}

    impl_execution_stats_for_rusoto_2!(
        ExecuteStatementResult,
        FetchPageResult,
        CommitTransactionResult
    );
}
