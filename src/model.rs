use crate::schema::*;

#[derive(Identifiable, Queryable, PartialEq, Debug)]
#[table_name = "codecs"]
#[primary_key(id)]
pub struct Codec {
    pub id: i32,
    pub name: String,
}

lazy_static! {
    pub static ref CODEC_DAG_PB: Codec = Codec {
        id: 112,
        name: "dag-pb".to_string()
    };
    pub static ref CODEC_RAW: Codec = Codec {
        id: 85,
        name: "raw".to_string()
    };
}

#[derive(Identifiable, Queryable, PartialEq, Debug)]
#[table_name = "unixfs_types"]
#[primary_key(id)]
pub struct UnixFSType {
    pub id: i32,
    pub name: String,
}

lazy_static! {
    pub static ref UNIXFS_TYPE_RAW: UnixFSType = UnixFSType {
        id: 0,
        name: "raw".to_string()
    };
    pub static ref UNIXFS_TYPE_DIRECTORY: UnixFSType = UnixFSType {
        id: 1,
        name: "directory".to_string()
    };
    pub static ref UNIXFS_TYPE_FILE: UnixFSType = UnixFSType {
        id: 2,
        name: "file".to_string()
    };
    pub static ref UNIXFS_TYPE_METADATA: UnixFSType = UnixFSType {
        id: 3,
        name: "metadata".to_string()
    };
    pub static ref UNIXFS_TYPE_SYMLINK: UnixFSType = UnixFSType {
        id: 4,
        name: "symlink".to_string()
    };
    pub static ref UNIXFS_TYPE_HAMT_SHARD: UnixFSType = UnixFSType {
        id: 5,
        name: "HAMTShard".to_string()
    };
}

#[derive(Identifiable, Queryable, PartialEq, Debug)]
#[table_name = "errors"]
#[primary_key(id)]
pub struct BlockError {
    pub id: i32,
    pub name: String,
}

lazy_static! {
    pub static ref BLOCK_ERROR_FAILED_TO_GET_BLOCK_DEADLINE_EXCEEDED: BlockError = BlockError {
        id: 1,
        name: "failed to get block: deadline exceeded".to_string()
    };
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[table_name = "blocks"]
#[belongs_to(Codec)]
#[primary_key(id)]
pub struct Block {
    pub id: i32,
    pub base32_cidv1: String,
    pub codec_id: i32,
    pub block_size: i32,
    pub first_bytes: Vec<u8>,
}

#[derive(Insertable)]
#[table_name = "blocks"]
pub struct NewBlock<'a> {
    pub base32_cidv1: &'a str,
    pub codec_id: &'a i32,
    pub block_size: &'a i32,
    pub first_bytes: &'a Vec<u8>,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[table_name = "failed_blocks"]
#[belongs_to(Block, foreign_key = "block_id")]
#[belongs_to(BlockError, foreign_key = "error_id")]
#[primary_key(block_id)]
pub struct FailedBlock {
    pub block_id: i32,
    pub error_id: i32,
}

#[derive(Insertable)]
#[table_name = "failed_blocks"]
pub struct NewFailedBlock<'a> {
    pub block_id: &'a i32,
    pub error_id: &'a i32,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[table_name = "unixfs_blocks"]
#[belongs_to(Block, foreign_key = "block_id")]
#[belongs_to(UnixFSType, foreign_key = "unixfs_type_id")]
#[primary_key(block_id)]
pub struct UnixFSBlock {
    pub block_id: i32,
    pub unixfs_type_id: i32,
    pub size: i64,
    pub cumulative_size: i64,
    pub blocks: i32,
    pub num_links: i32,
}

#[derive(Insertable)]
#[table_name = "unixfs_blocks"]
pub struct NewUnixFSBlock<'a> {
    pub block_id: &'a i32,
    pub unixfs_type_id: &'a i32,
    pub size: &'a i64,
    pub cumulative_size: &'a i64,
    pub blocks: &'a i32,
    pub num_links: &'a i32,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[table_name = "unixfs_links"]
#[belongs_to(UnixFSBlock, foreign_key = "parent_block_id")]
#[primary_key(parent_block_id, referenced_base32_cidv1, name)]
pub struct UnixFSLink {
    pub parent_block_id: i32,
    pub referenced_base32_cidv1: String,
    pub name: String,
    pub size: i64,
}

#[derive(Insertable)]
#[table_name = "unixfs_links"]
pub struct NewUnixFSLink<'a> {
    pub parent_block_id: &'a i32,
    pub referenced_base32_cidv1: &'a str,
    pub name: &'a str,
    pub size: &'a i64,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[table_name = "unixfs_file_heuristics"]
#[belongs_to(UnixFSBlock, foreign_key = "block_id")]
#[primary_key(block_id)]
pub struct UnixFSFileHeuristics {
    pub block_id: i32,
    pub tree_mime_mime_type: Option<String>,
    pub chardet_encoding: Option<String>,
    pub chardet_language: Option<String>,
    pub chardet_confidence: Option<f32>,
    pub chardetng_encoding: Option<String>,
    pub whatlang_language: Option<String>,
    pub whatlang_script: Option<String>,
    pub whatlang_confidence: Option<f64>,
}

#[derive(Insertable)]
#[table_name = "unixfs_file_heuristics"]
pub struct NewUnixFSFileHeuristics<'a> {
    pub block_id: &'a i32,
    pub tree_mime_mime_type: Option<&'a str>,
    pub chardet_encoding: Option<&'a str>,
    pub chardet_language: Option<&'a str>,
    pub chardet_confidence: Option<&'a f32>,
    pub chardetng_encoding: Option<&'a str>,
    pub whatlang_language: Option<&'a str>,
    pub whatlang_script: Option<&'a str>,
    pub whatlang_confidence: Option<&'a f64>,
}
