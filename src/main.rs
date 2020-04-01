use tokio_postgres::{NoTls};
use failure::Error;
use std::process;
use flexi_logger::{Logger, ReconfigurationHandle, Duplicate, Criterion, Naming, Cleanup, DeferredNow};
use log::Record;
use tokio::task;

#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
#[macro_use]
extern crate diesel;

type Result<T> = std::result::Result<T,Error>;

#[tokio::main]
async fn main() -> Result<()> {
    debug!("coneecting to DB...");
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=leo dbname=ipfs", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    task::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
            process::exit(1);
        }
    });

    // Now we can execute a simple statement that just returns its parameter.
    let rows = client
        .query("SELECT $1::TEXT", &[&"hello world"])
        .await?;

    // And then check that we got back the same string we sent over.
    let value: &str = rows[0].get(0);
    assert_eq!(value, "hello world");

    Ok(())
}


pub(crate) fn log_format(
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

pub(crate) fn set_up_logging(log_to_file: bool) -> Result<ReconfigurationHandle> {
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