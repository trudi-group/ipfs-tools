use failure::{err_msg, ResultExt};
use ipfs_resolver_common::wantlist::JsonCID;
use ipfs_resolver_common::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

const API_BASE_PATH: &str = "/metric_plugin/v1";
const API_PATH_PING: &str = "/ping";
const API_PATH_BROADCAST_WANT: &str = "/broadcast_want";
const API_PATH_BROADCAST_CANCEL: &str = "/broadcast_cancel";
const API_PATH_BROADCAST_WANT_CANCEL: &str = "/broadcast_want_cancel";

#[derive(Debug)]
pub struct APIClient {
    base_url: reqwest::Url,
    client: reqwest::Client,
}

impl APIClient {
    pub fn new(base_url: &str) -> Result<APIClient> {
        let u = reqwest::Url::parse(base_url).context("unable to parse base URL")?;

        Ok(APIClient {
            base_url: u,
            client: reqwest::Client::new(),
        })
    }

    fn build_address(&self, path: &str) -> reqwest::Url {
        let mut u = self.base_url.clone();
        u.set_path(format!("{}{}", API_BASE_PATH, path).as_str());
        debug!("computed URL {}", u);

        u
    }

    pub async fn ping(&self) -> Result<()> {
        let _ = self
            .client
            .get(self.build_address(API_PATH_PING))
            .send()
            .await
            .context("unable to query API")?
            .json::<JSONResponse<PingResponse>>()
            .await
            .context("unable to decode JSON")?
            .into_result()?;

        Ok(())
    }

    pub async fn monitoring_addresses(&self) -> Result<Vec<String>> {
        let resp = self
            .client
            .get(self.build_address(API_PATH_PING))
            .send()
            .await
            .context("unable to query API")?
            .json::<JSONResponse<MonitoringAddressesResponse>>()
            .await
            .context("unable to decode JSON")?
            .into_result()?;

        Ok(resp.addresses)
    }

    pub async fn broadcast_bitswap_want(
        &self,
        cids: Vec<String>,
    ) -> Result<Vec<BroadcastBitswapWantEntry>> {
        let resp = self
            .client
            .post(self.build_address(API_PATH_BROADCAST_WANT))
            .json(&BroadcastBitswapWantRequest {
                cids: cids.into_iter().map(|c| JsonCID { path: c }).collect(),
            })
            .send()
            .await
            .context("unable to query API")?
            .json::<JSONResponse<BroadcastBitswapWantResponse>>()
            .await
            .context("unable to decode JSON")?
            .into_result()?;

        Ok(resp.peers)
    }

    pub async fn broadcast_bitswap_cancel(
        &self,
        cids: Vec<String>,
    ) -> Result<Vec<BroadcastBitswapCancelEntry>> {
        let resp = self
            .client
            .post(self.build_address(API_PATH_BROADCAST_CANCEL))
            .json(&BroadcastBitswapCancelRequest {
                cids: cids.into_iter().map(|c| JsonCID { path: c }).collect(),
            })
            .send()
            .await
            .context("unable to query API")?
            .json::<JSONResponse<BroadcastBitswapCancelResponse>>()
            .await
            .context("unable to decode JSON")?
            .into_result()?;

        Ok(resp.peers)
    }

    pub async fn broadcast_bitswap_want_cancel(
        &self,
        cids: Vec<String>,
        seconds_before_cancel: u32,
    ) -> Result<Vec<BroadcastBitswapWantCancelEntry>> {
        let resp = self
            .client
            .post(self.build_address(API_PATH_BROADCAST_WANT_CANCEL))
            .json(&BroadcastBitswapWantCancelRequest {
                cids: cids.into_iter().map(|c| JsonCID { path: c }).collect(),
                seconds_before_cancel,
            })
            .send()
            .await
            .context("unable to query API")?
            .json::<JSONResponse<BroadcastBitswapWantCancelResponse>>()
            .await
            .context("unable to decode JSON")?
            .into_result()?;

        Ok(resp.peers)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapWantRequest {
    cids: Vec<JsonCID>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapCancelRequest {
    cids: Vec<JsonCID>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapWantCancelRequest {
    cids: Vec<JsonCID>,
    seconds_before_cancel: u32,
}

#[derive(Clone, Deserialize, Debug)]
pub struct JSONResponse<T> {
    pub status: i32,
    pub result: Option<T>,
    pub error: Option<String>,
}

impl<T> JSONResponse<T> {
    fn into_result(self) -> Result<T> {
        if let Some(err) = self.error {
            return Err(err_msg(format!("remote returned error: {}", err)));
        }
        if let Some(resp) = self.result {
            return Ok(resp);
        }

        return Err(err_msg("remote returned neither a response nor an error"));
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PingResponse {}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MonitoringAddressesResponse {
    addresses: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapWantResponse {
    peers: Vec<BroadcastBitswapWantEntry>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapCancelResponse {
    peers: Vec<BroadcastBitswapCancelEntry>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapWantCancelResponse {
    peers: Vec<BroadcastBitswapWantCancelEntry>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapWantEntry {
    pub peer: String,
    pub timestamp_before_send: chrono::DateTime<chrono::Utc>,
    pub send_duration_millis: i64,
    pub error: Option<String>,
    pub request_type_sent: Option<i32>,
}

pub const TCP_BITSWAP_REQUEST_TYPE_BLOCK: i32 = 0;
pub const TCP_BITSWAP_REQUEST_TYPE_HAVE: i32 = 1;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapCancelEntry {
    pub peer: String,
    pub timestamp_before_send: chrono::DateTime<chrono::Utc>,
    pub send_duration_millis: i64,
    pub error: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapWantCancelEntry {
    pub peer: String,
    pub want_status: BroadcastBitswapWantCancelWantEntry,
    pub cancel_status: BroadcastBitswapWantCancelCancelEntry,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapWantCancelWantEntry {
    pub timestamp_before_send: chrono::DateTime<chrono::Utc>,
    pub send_duration_millis: i64,
    pub error: Option<String>,
    pub request_type_sent: Option<i32>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BroadcastBitswapWantCancelCancelEntry {
    pub timestamp_before_send: chrono::DateTime<chrono::Utc>,
    pub send_duration_millis: i64,
    pub error: Option<String>,
}
