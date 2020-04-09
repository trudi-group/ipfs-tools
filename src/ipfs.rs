use crate::Result;
use failure::err_msg;
use failure::ResultExt;
use ipfs_api::response;
use reqwest::Url;

pub(crate) async fn query_ipfs_for_cat(
    ipfs_base: &Url,
    resolve_timeout: u16,
    cid_string: &str,
    length: u64,
) -> Result<Vec<u8>> {
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    let mut url = ipfs_base.clone();
    url.set_path("api/v0/cat");

    let query = match length {
        0 => format!(
            "arg={}&timeout={}s",
            &percent_encoding::utf8_percent_encode(
                &ipfs_prefixed_cid,
                percent_encoding::NON_ALPHANUMERIC
            ),
            resolve_timeout
        ),
        _ => format!(
            "arg={}&timeout={}s&length={}",
            &percent_encoding::utf8_percent_encode(
                &ipfs_prefixed_cid,
                percent_encoding::NON_ALPHANUMERIC
            ),
            resolve_timeout,
            length
        ),
    };

    url.set_query(Some(&query));

    query_ipfs_api_raw(url).await
}

pub(crate) async fn query_ipfs_for_block_get(
    ipfs_base: &Url,
    resolve_timeout: u16,
    cid_string: &str,
) -> Result<Vec<u8>> {
    let mut url = ipfs_base.clone();
    url.set_path("api/v0/block/get");
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    url.set_query(Some(&format!(
        "arg={}&timeout={}s",
        &percent_encoding::utf8_percent_encode(
            &ipfs_prefixed_cid,
            percent_encoding::NON_ALPHANUMERIC,
        ),
        resolve_timeout
    )));

    query_ipfs_api_raw(url).await
}

pub(crate) async fn query_ipfs_for_object_data(
    ipfs_base: &Url,
    resolve_timeout: u16,
    cid_string: &str,
) -> Result<Vec<u8>> {
    let mut url = ipfs_base.clone();
    url.set_path("api/v0/object/data");
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);
    url.set_query(Some(&format!(
        "arg={}&timeout={}s",
        &percent_encoding::utf8_percent_encode(
            &ipfs_prefixed_cid,
            percent_encoding::NON_ALPHANUMERIC,
        ),
        resolve_timeout
    )));

    query_ipfs_api_raw(url).await
}

pub(crate) async fn query_ipfs_for_metadata(
    ipfs_base: &Url,
    resolve_timeout: u16,
    cid_string: &str,
) -> Result<(
    response::BlockStatResponse,
    response::FilesStatResponse,
    response::ObjectStatResponse,
    response::ObjectLinksResponse,
)> {
    let ipfs_prefixed_cid = format!("/ipfs/{}", cid_string);

    let mut block_stat_url = ipfs_base.clone();
    block_stat_url.set_path("api/v0/block/stat");
    block_stat_url.set_query(Some(&format!(
        "arg={}&timeout={}s",
        &percent_encoding::utf8_percent_encode(cid_string, percent_encoding::NON_ALPHANUMERIC),
        resolve_timeout
    )));

    let mut files_stat_url = ipfs_base.clone();
    files_stat_url.set_path("api/v0/files/stat");
    files_stat_url.set_query(Some(&format!(
        "arg={}&timeout={}s",
        &percent_encoding::utf8_percent_encode(
            &ipfs_prefixed_cid,
            percent_encoding::NON_ALPHANUMERIC,
        ),
        resolve_timeout
    )));

    let mut object_stat_url = ipfs_base.clone();
    object_stat_url.set_path("api/v0/object/stat");
    object_stat_url.set_query(Some(&format!(
        "arg={}&timeout={}s",
        &percent_encoding::utf8_percent_encode(
            &ipfs_prefixed_cid,
            percent_encoding::NON_ALPHANUMERIC,
        ),
        resolve_timeout
    )));

    let mut object_links_url = ipfs_base.clone();
    object_links_url.set_path("api/v0/object/links");
    object_links_url.set_query(Some(&format!(
        "arg={}&timeout={}s",
        &percent_encoding::utf8_percent_encode(
            &ipfs_prefixed_cid,
            percent_encoding::NON_ALPHANUMERIC,
        ),
        resolve_timeout
    )));

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
    let c = reqwest::Client::new();
    let resp = c
        .post(url)
        .send()
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
