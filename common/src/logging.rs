use crate::Result;
use flexi_logger::{DeferredNow, Duplicate, Logger, ReconfigurationHandle};
use log::Record;

fn log_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> std::result::Result<(), std::io::Error> {
    write!(
        w,
        "[{}] {} [{}] {}:{}: {}",
        now.now().format("%Y-%m-%d %H:%M:%S%.6f %:z"),
        record.level(),
        record.metadata().target(),
        //record.module_path().unwrap_or("<unnamed>"),
        record.file().unwrap_or("<unnamed>"),
        record.line().unwrap_or(0),
        &record.args()
    )
}

pub fn set_up_logging(log_to_file: bool) -> Result<ReconfigurationHandle> {
    let mut logger = Logger::with_env_or_str("debug").format(log_format);
    if log_to_file {
        logger = logger
            //.log_to_file()
            //.directory("logs")
            .duplicate_to_stderr(Duplicate::All)
        /*.rotate(
            Criterion::Size(100_000_000),
            Naming::Timestamps,
            Cleanup::KeepLogFiles(10),
        )*/;
    }

    let handle = logger.start()?;

    Ok(handle)
}
