use failure::{err_msg, ResultExt};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio::sync::RwLock;

use crate::Result;

pub(crate) fn set_up_signal_handling(
    path: String,
    known_gateways: Arc<RwLock<HashSet<String>>>,
) -> Result<()> {
    let mut stream =
        signal(SignalKind::user_defined1()).context("failed to set up handler for SIGUSR1")?;
    tokio::spawn(async move {
        signal_handler_update_gateways(&mut stream, &path, &known_gateways).await
    });

    Ok(())
}

async fn signal_handler_update_gateways(
    signal_stream: &mut Signal,
    gateway_data_path: &String,
    known_gateways: &Arc<RwLock<HashSet<String>>>,
) {
    while let Some(_) = signal_stream.recv().await {
        info!("received SIGUSR1, reloading gateway IDs");
        match update_known_gateways(gateway_data_path, known_gateways).await {
            Ok(_) => {
                info!("updated known gateways successfully");
            }
            Err(err) => {
                // We should let the user know this failed, but we will try again next time.
                error!(
                    "unable to update known gateways from file {}: {:?}",
                    gateway_data_path, err
                );
            }
        }
        // Let's chill a bit before we take new update-requests.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    info!("SIGUSR1 stream closed, exiting signal handler");
}

pub(crate) async fn update_known_gateways(
    gateway_data_path: &String,
    known_gateways: &Arc<RwLock<HashSet<String>>>,
) -> Result<()> {
    let file = File::open(gateway_data_path)
        .await
        .context("unable to open gateway ID file")?;

    let mut new_gateways = HashSet::new();
    let mut lines = BufReader::new(file).lines();

    while let Some(line) = lines.next_line().await.context("unable to read")? {
        if !line.starts_with("Qm") && !line.starts_with("12D3KooW") && line.len() < 46 {
            return Err(err_msg(format!("invalid gateway ID {}", line)));
        }

        new_gateways.insert(line);
    }

    let mut known_gateways = known_gateways.write().await;
    known_gateways.extend(new_gateways);

    Ok(())
}
