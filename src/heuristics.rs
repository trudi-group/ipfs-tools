use crate::{ipfs, Result};
use chardetng::EncodingDetector;
use failure::ResultExt;

#[derive(Debug, Clone)]
pub(crate) struct FileHeuristics {
    pub chardet_heuristics: Option<ChardetHeuristics>,
    pub tree_mime_mime_type: Option<String>,
    pub chardetng_encoding: Option<String>,
    pub whatlang_heuristics: Option<whatlang::Info>,
}

#[derive(Debug, Clone)]
pub(crate) struct ChardetHeuristics {
    pub charset: String,
    pub language: String,
    pub confidence: f32,
}

pub(crate) async fn get_file_heuristics(cid_string: &str) -> Result<FileHeuristics> {
    let data = ipfs::query_ipfs_for_cat(cid_string, 10 * 1024)
        .await
        .context("unable to /cat file")?;

    let mut encoding_detector = EncodingDetector::new();
    encoding_detector.feed(&data, true);

    let mime_type = tree_magic::from_u8(&data);
    let (chardet_charset, chardet_confidence, charcet_language) = chardet::detect(&data);
    let chardetng_encoding = encoding_detector.guess(None, true);

    let mut decoded = String::with_capacity(256 * 1024);
    let (res, _, _) = chardetng_encoding
        .new_decoder()
        .decode_to_string(&data, &mut decoded, true);
    // TODO maybe use res?

    let whatlang_language = match decoded.len() {
        0 => None,
        _ => whatlang::detect(&decoded),
    };

    Ok(FileHeuristics {
        chardet_heuristics: Some(ChardetHeuristics {
            charset: chardet_charset,
            language: charcet_language,
            confidence: chardet_confidence,
        }),
        tree_mime_mime_type: Some(mime_type),
        chardetng_encoding: Some(chardetng_encoding.name().to_string()),
        whatlang_heuristics: whatlang_language,
    })
}
