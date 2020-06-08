#[macro_use]
extern crate log;

use failure::ResultExt;
use ipfs_api::TryFromUri;
use ipfs_resolver_common::wantlist::JSONMessage;
use ipfs_resolver_common::{logging, Result};
use rand::{Rng, SeedableRng};
use reqwest::Url;
use std::collections::HashMap;
use std::env;
use std::io::Cursor;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use wantlist_client_lib::net::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    logging::set_up_logging(false)?;
    dotenv::dotenv().ok();

    do_stuff().await?;

    Ok(())
}

#[derive(Clone, Debug, Default)]
struct ProbingState {
    data: Option<Vec<u8>>,
    cid_v1: Option<String>,
    cid_v0: Option<String>,

    http_request_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    http_requests_sent: u32,
    http_request_remote: Option<SocketAddr>,
    http_success_timestamp: Option<chrono::DateTime<chrono::Utc>>,

    wantlist_message: Option<JSONMessage>,
}

async fn do_stuff() -> Result<()> {
    let wantlist_logging_address =
        env::var("WANTLIST_LOGGING_TCP_ADDRESS").expect("WANTLIST_LOGGING_TCP_ADDRESS must be set");
    info!(
        "Using wantlist logging address {}",
        wantlist_logging_address
    );

    let monitoring_node_port = env::var("IPFS_MONITOR_PORT")
        .expect("IPFS_MONITOR_PORT must be set")
        .parse::<u16>()
        .context("invalid IPFS_MONITOR_PORT")?;
    info!(
        "Using monitoring node at localhost:{}",
        monitoring_node_port
    );

    // Get list of gateways.
    info!("getting list of gateways...");
    let gateway_list_url = Url::parse(
        "https://raw.githubusercontent.com/ipfs/public-gateway-checker/master/gateways.json",
    )?;
    let client = reqwest::blocking::Client::builder()
        .timeout(Some(Duration::from_secs(30)))
        .build()?;

    let gateway_list_resp = client.get(gateway_list_url).send()?;
    let gateway_list: Vec<String> = gateway_list_resp.json()?;
    info!("got {} gateways: {:?}", gateway_list.len(), gateway_list);

    // Initialize our states.
    let gateway_states: Arc<HashMap<String, Mutex<ProbingState>>> = Arc::new(
        gateway_list
            .into_iter()
            .map(|e| (e, Default::default()))
            .collect(),
    );

    // Generate random data to add to IPFS.
    info!("generating random data...");
    for (_, state) in gateway_states.iter() {
        let rng = rand::rngs::StdRng::from_entropy();
        let bytes: Vec<u8> = rng
            .sample_iter(rand::distributions::Standard)
            .take(1024)
            .collect();

        let mut state = state.lock().await;
        state.data = Some(bytes);
    }

    // Add to IPFS.
    info!("adding data to IPFS...");
    for (_, state) in gateway_states.iter() {
        let mut state = state.lock().await;
        let data = state.data.as_ref().unwrap().clone();

        let client = ipfs_api::IpfsClient::from_host_and_port(
            http::uri::Scheme::HTTP,
            "localhost",
            monitoring_node_port,
        )?;
        let d = Cursor::new(data);

        let add_resp = client.add(d).await.context("unable to add data to IPFS")?;

        //let add_resp = ipfs::query_ipfs_add_block(&ipfs_api_base, data)
        //    .context("unable to add data to IPFS")?;
        let c = cid::Cid::from_str(&add_resp.hash)?;
        let cid_v1 = cid::Cid::new_v1(c.codec(), c.hash().to_owned());
        let cid_v1_encoded = multibase::encode(multibase::Base::Base32Lower, cid_v1.to_bytes());
        info!("added CID {} = {}", add_resp.hash, cid_v1_encoded);
        state.cid_v0 = Some(add_resp.hash.clone());
        state.cid_v1 = Some(cid_v1_encoded);
    }

    // Wait for DHT propagation...
    info!("waiting for DHT propagation..");
    tokio::time::delay_for(Duration::from_secs(30)).await;

    // Collect a list of all CIDs for easier searching.
    let mut cids = Vec::new();
    for (_, state) in gateway_states.iter() {
        let state = state.lock().await;
        cids.push(state.cid_v1.as_ref().unwrap().clone());
    }

    // Start listening for wantlist messages
    info!(
        "connecting to wantlist server at {}...",
        wantlist_logging_address
    );
    let conn = TcpStream::connect(wantlist_logging_address.as_str()).await?;

    let client = Connection::new(conn).await?;
    let remote = client.remote;
    let mut messages_in = client.messages_in;

    let task_gateway_states = gateway_states.clone();
    tokio::task::spawn(async move {
        info!("receiving wantlists from {}...", remote);
        while let Some(wl) = messages_in.recv().await {
            if let Some(entries) = wl.received_entries.as_ref() {
                for entry in entries {
                    if cids.contains(&entry.cid.path) {
                        debug!("received interesting CID {}", entry.cid.path);
                        for (gw, state) in task_gateway_states.iter() {
                            let mut state = state.lock().await;
                            if state.wantlist_message.is_none() {
                                if entry.cid.path.eq(state.cid_v1.as_ref().unwrap()) {
                                    info!(
                                        "got wantlist CID {} from peer {}, which is gateway {}",
                                        entry.cid.path, wl.peer, gw
                                    );
                                    state.wantlist_message = Some(wl.clone());
                                    break;
                                }
                            }
                        }

                        // We remove this from our interesting CID list because we only need it once.

                        break;
                    }
                }
            }
        }
    });

    info!("waiting for wantlist collection to be warmed up...");
    tokio::time::delay_for(Duration::from_secs(3)).await;

    // Send one CID to each gateway
    info!("probing gateways...");
    let (tx, mut done_rx) = tokio::sync::mpsc::channel(gateway_states.len());
    for (gateway, state) in gateway_states.iter() {
        let task_state = gateway_states.clone();
        let task_gateway = gateway.clone();
        let task_cid = {
            let state = state.lock().await;
            state.cid_v1.as_ref().unwrap().clone()
        };
        let mut task_done = tx.clone();
        tokio::task::spawn(async move {
            let res = probe_gateway(task_state, &task_gateway, &task_cid).await;

            match res {
                Ok(()) => {}
                Err(err) => error!("HTTP failed for {}: {:?}", task_gateway, err),
            }

            task_done.send(()).await.unwrap();
        });
    }

    // Wait...
    info!("waiting for responses...");
    for _i in 0..gateway_states.len() {
        done_rx.recv().await.unwrap()
    }

    info!(
        "all HTTP workers are done or timed out, waiting some more time for wantlist messages..."
    );
    tokio::time::delay_for(Duration::from_secs(120)).await;

    // Remove data from IPFS.
    info!("removing data from local IPFS node...");
    for (_, state) in gateway_states.iter() {
        let state = state.lock().await;
        let cid = state.cid_v0.as_ref().unwrap();
        debug!("cleaning up CID {}", cid);

        let client = ipfs_api::IpfsClient::from_host_and_port(
            http::uri::Scheme::HTTP,
            "localhost",
            monitoring_node_port,
        )?;
        client.pin_rm(cid.as_str(), true).await?;

        //ipfs::query_ipfs_rm_pin(&ipfs_api_base, &cid).context("unable to remove data from IPFS")?;
        info!("cleaned up CID {}", cid);
    }

    // Match them up, write down address, ID, maybe latency, ...
    info!("printing results..");
    println!("gateway,gateway_url,cid,http_request_ts,http_request_ts_subsec_millis,http_success_ts,http_success_ts_subsec_millis,http_requests_sent,http_remote,first_wl_ts,first_wl_ts_subsec_millis,first_wl_peer,first_wl_address");
    for (gateway, state) in gateway_states.iter() {
        let state = state.lock().await;
        let gw = gateway.replace(":hash.", "");
        let gw_url = Url::parse(&gw)?;
        print!("{},{}", gw_url.host_str().unwrap(), gateway);
        if let Some(cid) = state.cid_v1.as_ref() {
            print!(",{}", cid);
            print!(
                ",{},{}",
                state.http_request_timestamp.as_ref().unwrap().timestamp(),
                state
                    .http_request_timestamp
                    .as_ref()
                    .unwrap()
                    .timestamp_subsec_millis()
            );
            if let Some(http_ts) = state.http_success_timestamp.as_ref() {
                print!(
                    ",{},{}",
                    http_ts.timestamp(),
                    http_ts.timestamp_subsec_millis()
                );
            } else {
                print!(",,");
            }
            print!(",{}", state.http_requests_sent);
            if let Some(remote) = state.http_request_remote.as_ref() {
                print!(",{}", remote)
            } else {
                print!(",");
            }
            if let Some(msg) = state.wantlist_message.as_ref() {
                print!(
                    ",{},{},{},{}",
                    msg.timestamp.timestamp(),
                    msg.timestamp.timestamp_subsec_millis(),
                    msg.peer,
                    if let Some(addr) = msg.address.as_ref() {
                        format!("{}", addr)
                    } else {
                        "".to_string()
                    }
                );
            } else {
                print!(",,,,");
            }
        }
        println!();
    }

    Ok(())
}

async fn probe_gateway(
    gateway_state: Arc<HashMap<String, Mutex<ProbingState>>>,
    gateway_url: &str,
    cid: &str,
) -> Result<()> {
    let mut url = Url::parse(gateway_url.replace(":hash", &cid).as_str())?;
    url.set_fragment(Some("x-ipfs-companion-no-redirect"));

    {
        let mut state = gateway_state.get(gateway_url).unwrap().lock().await;
        state.http_request_timestamp = Some(chrono::Utc::now());
    }

    for i in 0..10 {
        debug!("requesting {}...", url);
        let resp = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?
            .get(url.clone())
            .send()
            .await;
        match resp {
            Ok(resp) => {
                let remote = resp.remote_addr().clone();
                let body = resp.bytes().await?;

                {
                    let mut state = gateway_state.get(gateway_url).unwrap().lock().await;
                    if state.http_request_remote.is_none() {
                        state.http_request_remote = remote
                    }
                    state.http_requests_sent = i + 1;

                    if !body.eq(state.data.as_ref().unwrap()) {
                        debug!("data mismatch for gateway {}, got {} bytes, expected {} (and maybe different bytes)", gateway_url, body.len(), state.data.as_ref().unwrap().len());
                        tokio::time::delay_for(Duration::from_secs(3)).await;
                        continue;
                    }

                    info!("got correct response from gateway {}", gateway_url);
                    state.http_success_timestamp = Some(chrono::Utc::now());
                    return Ok(());
                }
            }
            Err(err) => info!("no response from {}: {:?}", gateway_url, err),
        }
    }
    info!(
        "did not get a correct response from gateway {}",
        gateway_url
    );

    Ok(())
}
