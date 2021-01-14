///! Maintenance of execution stats.
///!
///! The API models TimingInformation as optional, which can make it a little
///! tricky to accumulate processing time (consider fetching pages of results,
///! summing total time). The traits here serve to organize this concern out of
///! the main body of code.
use rusoto_qldb_session::{IOUsage, TimingInformation};

pub trait TimingInformationExt {
    fn accumulate(&mut self, other: &Option<TimingInformation>);
}

impl<T> TimingInformationExt for Option<T>
where
    T: TimingInformationExt,
{
    fn accumulate(&mut self, other: &Option<TimingInformation>) {
        if let Some(t) = self {
            t.accumulate(other);
        }
    }
}

impl TimingInformationExt for TimingInformation {
    fn accumulate(&mut self, other: &Option<TimingInformation>) {
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

pub trait IOUsageExt {
    fn accumulate(&mut self, other: &Option<IOUsage>);
}

impl<T> IOUsageExt for Option<T>
where
    T: IOUsageExt,
{
    fn accumulate(&mut self, other: &Option<IOUsage>) {
        if let Some(t) = self {
            t.accumulate(other);
        }
    }
}

impl IOUsageExt for IOUsage {
    fn accumulate(&mut self, other: &Option<IOUsage>) {
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
            TimingInformation {
                processing_time_milliseconds: Some(1),
            },
            IOUsage {
                read_i_os: Some(1),
                write_i_os: Some(1),
            },
        );

        stats.0.accumulate(&Some(TimingInformation {
            processing_time_milliseconds: Some(2),
        }));

        stats.1.accumulate(&Some(IOUsage {
            read_i_os: Some(2),
            write_i_os: Some(2),
        }));

        assert_eq!(
            TimingInformation {
                processing_time_milliseconds: Some(3)
            },
            stats.0
        );
    }
}
