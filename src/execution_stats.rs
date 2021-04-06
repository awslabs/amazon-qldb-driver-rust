use rusoto_qldb_session::StartTransactionResult;
use rusoto_qldb_session::TimingInformation as RusotoTimingInformation;
use rusoto_qldb_session::{AbortTransactionResult, CommitTransactionResult, FetchPageResult};
use rusoto_qldb_session::{ExecuteStatementResult, IOUsage as RusotoIOUsage};

// public (stable) types for execution stats.
//
// First, we don't want to leak our implementation details (that we use Rusoto)
// to customers. Doing so would make changing the underlying SDK in the future
// harder. Second, while the API models all values as optional, in reality QLDB
// always returns execution stats.

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct ExecutionStats {
    pub timing_information: TimingInformation,
    pub io_usage: IOUsage,
}

impl ExecutionStats {
    pub fn accumulate<R>(&mut self, other: &R)
    where
        R: HasExecutionStats,
    {
        self.timing_information.accumulate(other);
        self.io_usage.accumulate(other);
    }
}

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct TimingInformation {
    pub processing_time_milliseconds: i64,
}

impl TimingInformation {
    pub fn accumulate<R>(&mut self, other: &R)
    where
        R: HasExecutionStats,
    {
        self.processing_time_milliseconds +=
            other.timing_information().processing_time_milliseconds;
    }
}

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct IOUsage {
    pub read_ios: i64,
    pub write_ios: i64,
}

impl IOUsage {
    pub fn accumulate<R>(&mut self, other: &R)
    where
        R: HasExecutionStats,
    {
        if let Some(IOUsage {
            read_ios,
            write_ios,
        }) = other.io_usage()
        {
            self.read_ios += read_ios;
            self.write_ios += write_ios;
        }
    }
}

impl From<(Option<RusotoTimingInformation>, Option<RusotoIOUsage>)> for ExecutionStats {
    fn from(rusoto: (Option<RusotoTimingInformation>, Option<RusotoIOUsage>)) -> Self {
        match rusoto {
            (Some(t), Some(u)) => (t, u).into(),
            _ => {
                // NOTE: We don't bother with partial Some/None combinations. We
                // expect to get both back, always. This branch could reasonably
                // be replaced with `unreachable!`.
                trace!("it is expected that QLDB always return timing and IO usage information, but did not");
                ExecutionStats::default()
            }
        }
    }
}

impl From<(RusotoTimingInformation, RusotoIOUsage)> for ExecutionStats {
    fn from(rusoto: (RusotoTimingInformation, RusotoIOUsage)) -> Self {
        ExecutionStats {
            timing_information: rusoto.0.into(),
            io_usage: rusoto.1.into(),
        }
    }
}

impl From<Option<RusotoTimingInformation>> for TimingInformation {
    fn from(rusoto: Option<RusotoTimingInformation>) -> Self {
        rusoto
            .map(|info| info.into())
            .unwrap_or(TimingInformation::default())
    }
}

impl From<RusotoTimingInformation> for TimingInformation {
    fn from(rusoto: RusotoTimingInformation) -> Self {
        TimingInformation {
            processing_time_milliseconds: rusoto.processing_time_milliseconds.unwrap_or(0),
        }
    }
}

impl From<RusotoIOUsage> for IOUsage {
    fn from(rusoto: RusotoIOUsage) -> Self {
        IOUsage {
            read_ios: rusoto.read_i_os.unwrap_or(0),
            write_ios: rusoto.write_i_os.unwrap_or(0),
        }
    }
}

pub trait HasExecutionStats {
    fn timing_information(&self) -> TimingInformation;
    fn io_usage(&self) -> Option<IOUsage>;
}

impl ExecutionStats {
    // Can't use From here because it conflicts with the stdlib.
    pub(crate) fn from_api<S>(s: S) -> ExecutionStats
    where
        S: HasExecutionStats,
    {
        ExecutionStats {
            timing_information: s.timing_information(),
            io_usage: s.io_usage().unwrap_or(IOUsage::default()),
        }
    }
}

impl HasExecutionStats for ExecutionStats {
    fn timing_information(&self) -> TimingInformation {
        self.timing_information.clone()
    }

    fn io_usage(&self) -> Option<IOUsage> {
        Some(self.io_usage.clone())
    }
}

impl HasExecutionStats for StartTransactionResult {
    fn timing_information(&self) -> TimingInformation {
        self.timing_information.clone().into()
    }

    fn io_usage(&self) -> Option<IOUsage> {
        None
    }
}

impl HasExecutionStats for AbortTransactionResult {
    fn timing_information(&self) -> TimingInformation {
        self.timing_information.clone().into()
    }

    fn io_usage(&self) -> Option<IOUsage> {
        None
    }
}

impl HasExecutionStats for ExecuteStatementResult {
    fn timing_information(&self) -> TimingInformation {
        self.timing_information.clone().into()
    }

    fn io_usage(&self) -> Option<IOUsage> {
        self.consumed_i_os.clone().map(|usage| usage.into())
    }
}

impl HasExecutionStats for FetchPageResult {
    fn timing_information(&self) -> TimingInformation {
        self.timing_information.clone().into()
    }

    fn io_usage(&self) -> Option<IOUsage> {
        self.consumed_i_os.clone().map(|usage| usage.into())
    }
}

impl HasExecutionStats for CommitTransactionResult {
    fn timing_information(&self) -> TimingInformation {
        self.timing_information.clone().into()
    }

    fn io_usage(&self) -> Option<IOUsage> {
        self.consumed_i_os.clone().map(|usage| usage.into())
    }
}
