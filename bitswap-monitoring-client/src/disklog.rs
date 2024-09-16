use crate::Result;
use async_compression::tokio::write::GzipEncoder;
use failure::{Error, Fail, ResultExt};
use ipfs_monitoring_plugin_client::monitoring::PushedEvent;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

/// An async to-disk logger for Bitswap messages.
/// Messages are processed concurrently, pipelined.
/// Output is written to a separate subdirectory per monitor, to gzipped JSON files.
/// The output file is rotated regularly.
#[derive(Debug)]
pub(crate) struct ToDiskLogger {
    output: Sender<PushedEvent>,
    last_error: Arc<Mutex<Option<Error>>>,
    writer_handle: JoinHandle<()>,
}

impl ToDiskLogger {
    /// Creates a new to-disk logger for Bitswap messages.
    /// A subdirectory with the monitor name will be created under the provided path,
    /// if it does not exist already.
    pub(crate) async fn new_for_monitor(base_directory: &str, monitor_name: &str) -> Result<Self> {
        // Create subdirectory for monitor, if not exists.
        let base_dir = PathBuf::from_str(base_directory).context("invalid path")?;
        let target_dir = base_dir.join(monitor_name);
        fs::create_dir_all(&target_dir).context("unable to create logging directory")?;

        // Create initial output file
        let out_file = Self::rotate_file(None, &target_dir)
            .await
            .context("unable to create output file")?;

        // Set up some plumbing
        let (send_msg, recv_msg) = mpsc::channel(1);
        let (send_json, recv_json) = mpsc::channel(1);
        let last_error = Arc::new(Mutex::new(None));

        // Spawn tasks
        tokio::spawn(Self::json_encode_task(
            monitor_name.to_string(),
            recv_msg,
            send_json,
            last_error.clone(),
        ));
        let writer_handle = tokio::spawn(Self::write_to_file(
            monitor_name.to_string(),
            target_dir.clone(),
            last_error.clone(),
            recv_json,
            out_file,
        ));

        let logger = ToDiskLogger {
            output: send_msg,
            last_error,
            writer_handle,
        };

        Ok(logger)
    }

    /// Logs the given message to file.
    /// An error is returned if logging of previous messages failed.
    /// After an error has been returned, subsequent calls will panic.
    pub(crate) async fn log_message(&self, msg: PushedEvent) -> Result<()> {
        if let Err(_) = self.output.send(msg).await {
            // Receiver closed, this is an error.
            if let Some(err) = self.last_error.lock().await.take() {
                return Err(err);
            }
            unreachable!()
        }

        Ok(())
    }

    async fn json_encode_task(
        monitor_name: String,
        mut input: Receiver<PushedEvent>,
        output: Sender<Vec<u8>>,
        error_storage: Arc<Mutex<Option<Error>>>,
    ) {
        while let Some(msg) = input.recv().await {
            let serialized = match serde_json::to_vec(&msg) {
                Ok(s) => s,
                Err(e) => {
                    let mut last_err = error_storage.lock().await;
                    *last_err = Some(e.context("unable to serialize to JSON").into());
                    break;
                }
            };
            if let Err(_) = output.send(serialized).await {
                // Receiver closed, this is an error.
                // However, the receiver is responsible for storing that error, so we just quit.
                debug!(
                    "{} json encode: unable to send, receiver closed",
                    monitor_name
                );
                break;
            }
        }

        debug!("{} json encode: exiting", monitor_name);
    }

    async fn finalize_file(mut f: BufWriter<GzipEncoder<File>>) -> Result<()> {
        f.flush().await.context("unable to flush buffer")?;
        f.shutdown().await.context("unable to write trailer")?;
        Ok(())
    }

    pub(crate) async fn close(self) -> Result<()> {
        let ToDiskLogger {
            output,
            last_error,
            writer_handle,
        } = self;

        // Shut down by dropping the sender, everything else should follow.
        drop(output);

        // Wait for the writer to finish
        writer_handle
            .await
            .context("write did not shut down cleanly")?;

        // Check if we maybe had an error somewhere
        if let Some(err) = last_error.lock().await.take() {
            return Err(err);
        }

        Ok(())
    }

    async fn rotate_file(
        old_file: Option<BufWriter<GzipEncoder<File>>>,
        target_dir: &PathBuf,
    ) -> Result<BufWriter<GzipEncoder<File>>> {
        if let Some(f) = old_file {
            Self::finalize_file(f)
                .await
                .context("unable to finalize previous file")?;
        }

        let fp = loop {
            let ts = format!("{}", chrono::Utc::now().format("%Y-%m-%d_%H-%M-%S_%Z"));
            let fp = target_dir.join(format!("{}.json.gz", ts));
            debug!("checking if new log file {:?} exists...", fp);
            if !tokio::fs::try_exists(&fp)
                .await
                .context("unable to check if file exists")?
            {
                debug!("log file {:?} does not exist", fp);
                break fp;
            }
            debug!("log file {:?} exists already, sleeping one second...", fp);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        };

        debug!("creating new log file at {:?}", fp);
        let out_file = File::create(fp)
            .await
            .context("unable to bitswap logging file")?;

        Ok(BufWriter::new(GzipEncoder::new(out_file)))
    }

    async fn write_to_file(
        monitor_name: String,
        target_dir: PathBuf,
        error_storage: Arc<Mutex<Option<Error>>>,
        mut input: Receiver<Vec<u8>>,
        file: BufWriter<GzipEncoder<File>>,
    ) {
        let mut current_file = file;
        let newline = "\n".as_bytes();

        // Create a ticker to rotate the output file every hour.
        let mut rotation_ticker = tokio::time::interval(Duration::from_secs(60 * 60));
        // First tick comes for free.
        rotation_ticker.tick().await;

        loop {
            select! {
                msg = input.recv() => {
                    match msg {
                        None => {
                            // Sender closed, we're shutting down (or something went wrong).
                            debug!("{} write: sender closed, exiting",monitor_name);
                            // Don't forget to flush and finalize.
                            if let Err(e) = Self::finalize_file(current_file).await {
                                error!("{} write: unable to finalize output file: {:?}",monitor_name,e);
                                let mut last_err = error_storage.lock().await;
                                *last_err = Some(e.context("unable to finalize output file").into());
                            }
                            break
                        }
                        Some(msg) => {
                            // Write to file
                            let res = current_file.write_all(&msg).await;
                            if let Err(e) = res {
                                error!("{} write: unable to write: {:?}",monitor_name,e);
                                let mut last_err = error_storage.lock().await;
                                *last_err = Some(e.context("unable to write").into());
                                break
                            }
                            let res = current_file.write_all(newline).await;
                            if let Err(e) = res {
                                error!("{} write: unable to write: {:?}",monitor_name,e);
                                let mut last_err = error_storage.lock().await;
                                *last_err = Some(e.context("unable to write").into());
                                break
                            }
                        }
                    }
                },
                _ = rotation_ticker.tick() => {
                    // Rotate the file
                    debug!("{} write: rotating output file",monitor_name);
                    current_file = match Self::rotate_file(Some(current_file),&target_dir).await {
                        Ok(f) => f,
                        Err(e) => {
                            error!("{} write: unable to rotate bitswap log file: {:?}",monitor_name,e);
                            let mut last_err =  error_storage.lock().await;
                            *last_err = Some(e.context("unable to rotate output file").into());
                            break
                        }
                    }
                }
            }
        }

        debug!("{} write: exiting", monitor_name);
    }
}
