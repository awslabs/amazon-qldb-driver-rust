use rusoto_qldb_session::IOUsage as RusotoIOUsage;
use rusoto_qldb_session::TimingInformation as RusotoTimingInformation;

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

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct TimingInformation {
    pub processing_time_milliseconds: i64,
}

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct IOUsage {
    pub read_ios: i64,
    pub write_ios: i64,
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

// Maintenance of execution stats.
//
// The API models TimingInformation as optional, which can make it a little
// tricky to accumulate processing time (consider fetching pages of results,
// summing total time). The traits here serve to organize this concern out of
// the main body of code.

pub trait RusotoTimingInformationExt {
    fn accumulate(&mut self, other: &Option<RusotoTimingInformation>);
}

impl<T> RusotoTimingInformationExt for Option<T>
where
    T: RusotoTimingInformationExt,
{
    fn accumulate(&mut self, other: &Option<RusotoTimingInformation>) {
        if let Some(t) = self {
            t.accumulate(other);
        }
    }
}

impl RusotoTimingInformationExt for RusotoTimingInformation {
    fn accumulate(&mut self, other: &Option<RusotoTimingInformation>) {
        if let Some(o) = other {
            if let (Some(i), Some(j)) = (
                self.processing_time_milliseconds,
                o.processing_time_milliseconds,
            ) {
                self.processing_time_milliseconds = Some(i + j)
            }
        }
    }
}

pub trait RusotoIOUsageExt {
    fn accumulate(&mut self, other: &Option<RusotoIOUsage>);
}

impl<T> RusotoIOUsageExt for Option<T>
where
    T: RusotoIOUsageExt,
{
    fn accumulate(&mut self, other: &Option<RusotoIOUsage>) {
        if let Some(t) = self {
            t.accumulate(other);
        }
    }
}

impl RusotoIOUsageExt for RusotoIOUsage {
    fn accumulate(&mut self, other: &Option<RusotoIOUsage>) {
        if let Some(o) = other {
            if let (Some(i), Some(j)) = (self.read_i_os, o.read_i_os) {
                self.read_i_os = Some(i + j)
            }

            if let (Some(i), Some(j)) = (self.write_i_os, o.write_i_os) {
                self.write_i_os = Some(i + j)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulating_stats() {
        let mut stats = (
            RusotoTimingInformation {
                processing_time_milliseconds: Some(1),
            },
            RusotoIOUsage {
                read_i_os: Some(2),
                write_i_os: Some(3),
            },
        );

        stats.0.accumulate(&Some(RusotoTimingInformation {
            processing_time_milliseconds: Some(4),
        }));

        stats.1.accumulate(&Some(RusotoIOUsage {
            read_i_os: Some(5),
            write_i_os: Some(6),
        }));

        assert_eq!(
            RusotoTimingInformation {
                processing_time_milliseconds: Some(5)
            },
            stats.0
        );

        assert_eq!(
            RusotoIOUsage {
                read_i_os: Some(7),
                write_i_os: Some(9)
            },
            stats.1
        );
    }
}
