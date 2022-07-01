use crate::Result;
use failure::ResultExt;
use flexi_logger::{DeferredNow, Logger, LoggerHandle, TS_DASHES_BLANK_COLONS_DOT_BLANK};
use log::Record;

fn log_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> std::result::Result<(), std::io::Error> {
    write!(
        w,
        "[{}] {} [{}] {}:{}: {}",
        now.format(TS_DASHES_BLANK_COLONS_DOT_BLANK),
        record.level(),
        record.metadata().target(),
        //record.module_path().unwrap_or("<unnamed>"),
        record.file().unwrap_or("<unnamed>"),
        record.line().unwrap_or(0),
        &record.args()
    )
}

pub fn set_up_logging() -> Result<LoggerHandle> {
    let logger = Logger::try_with_env_or_str("info")
        .context("unable to set up logging")?
        .use_utc()
        .format(log_format);

    let handle = logger.start()?;

    Ok(handle)
}
