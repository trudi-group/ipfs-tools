use crate::Result;
use failure::err_msg;
use failure::ResultExt;
use ipfs_api::response;
use reqwest::Url;

pub(crate) async fn query_ipfs_for_cat(cid_string: &str, length: u64) -> Result<Vec<u8>> {
    let base = "http://localhost:5002/api/v0";
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    let cat_url = match length {
        0 => Url::parse(&format!(
            "{}/cat?arg={}&timeout=30s",
            base, ipfs_prefixed_cid
        )),
        _ => Url::parse(&format!(
            "{}/cat?arg={}&timeout=30s&length={}",
            base, ipfs_prefixed_cid, length
        )),
    }
    .expect("invalid URL...");

    query_ipfs_api_raw(cat_url).await
}

pub(crate) async fn query_ipfs_for_block_get(cid_string: &str) -> Result<Vec<u8>> {
    let base = "http://localhost:5002/api/v0";
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    let block_get_url = Url::parse(&format!(
        "{}/block/get?arg={}&timeout=30s",
        base, ipfs_prefixed_cid
    ))
    .expect("invalid URL...");

    query_ipfs_api_raw(block_get_url).await
}

pub(crate) async fn query_ipfs_for_object_data(cid_string: &str) -> Result<Vec<u8>> {
    let base = "http://localhost:5002/api/v0";
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    let object_data_url = Url::parse(&format!(
        "{}/object/data?arg={}&timeout=30s",
        base, ipfs_prefixed_cid
    ))
    .expect("invalid URL...");

    query_ipfs_api_raw(object_data_url).await
}

pub(crate) async fn query_ipfs_for_metadata(
    cid_string: &str,
) -> Result<(
    response::BlockStatResponse,
    response::FilesStatResponse,
    response::ObjectStatResponse,
    response::ObjectLinksResponse,
)> {
    let base = "http://localhost:5002/api/v0";
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    let block_stat_url = Url::parse(&format!(
        "{}/block/stat?arg={}&timeout=30s",
        base, cid_string
    ))
    .expect("invalid URL...");
    let files_stat_url = Url::parse(&format!(
        "{}/files/stat?arg={}&timeout=30s",
        base, ipfs_prefixed_cid
    ))
    .expect("invalid URL...");
    let object_stat_url = Url::parse(&format!(
        "{}/object/stat?arg={}&timeout=30s",
        base, ipfs_prefixed_cid
    ))
    .expect("invalid URL...");
    let object_links_url = Url::parse(&format!(
        "{}/object/links?arg={}&timeout=30s",
        base, ipfs_prefixed_cid
    ))
    .expect("invalid URL...");

    let block_stat: response::BlockStatResponse = query_ipfs_api(block_stat_url)
        .await
        .context("unable to query IPFS API /block/stat")?;
    let files_stat: response::FilesStatResponse = query_ipfs_api(files_stat_url)
        .await
        .context("unable to query IPFS API /files/stat")?;
    let object_stat: response::ObjectStatResponse = query_ipfs_api(object_stat_url)
        .await
        .context("unable to query IPFS API /object/stat")?;

    // The IPFS HTTP API leaves out the "Links" field if there are no refs, which in turn causes
    // JSON parsing to fail. So if we have no links, just return a dummy response...
    if object_stat.num_links == 0 {
        return Ok((
            block_stat,
            files_stat,
            object_stat,
            response::ObjectLinksResponse {
                hash: cid_string.to_string(),
                links: vec![],
            },
        ));
    }
    let refs: response::ObjectLinksResponse = query_ipfs_api(object_links_url)
        .await
        .context("unable to query IPFS API /object/links")?;

    Ok((block_stat, files_stat, object_stat, refs))
}

async fn query_ipfs_api_raw(url: Url) -> Result<Vec<u8>> {
    let resp = reqwest::get(url)
        .await
        .context("unable to query IPFS API")?;

    match resp.status() {
        hyper::StatusCode::OK => {
            // parse as T
            let body = resp.bytes().await.context("unable to read body")?;
            Ok(body.to_vec())
        }
        _ => {
            // try to parse as IPFS error...
            let body = resp.bytes().await.context("unable to read body")?;
            let err = serde_json::from_slice::<ipfs_api::response::ApiError>(&body);
            match err {
                Ok(err) => Err(ipfs_api::response::Error::Api(err).into()),
                Err(_) => {
                    // just return the body I guess...
                    let err_text =
                        String::from_utf8(body.to_vec()).context("response is invalid UTF8")?;
                    Err(err_msg(format!(
                        "unable to parse IPFS API response: {}",
                        err_text
                    )))
                }
            }
        }
    }
}

async fn query_ipfs_api<Res>(url: Url) -> Result<Res>
where
    for<'de> Res: 'static + serde::Deserialize<'de>,
{
    let body = query_ipfs_api_raw(url).await?;

    let parsed = serde_json::from_slice::<Res>(&body);
    match parsed {
        Ok(parsed) => Ok(parsed),
        Err(_) => {
            // try to parse as IPFS error instead...
            let err = serde_json::from_slice::<ipfs_api::response::ApiError>(&body);
            match err {
                Ok(err) => Err(ipfs_api::response::Error::Api(err).into()),
                Err(_) => {
                    // just return the body I guess...
                    let err_text =
                        String::from_utf8(body.to_vec()).context("response is invalid UTF8")?;
                    Err(err_msg(format!(
                        "unable to parse IPFS API response: {}",
                        err_text
                    )))
                }
            }
        }
    }
}
