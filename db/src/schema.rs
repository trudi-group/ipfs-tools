table! {
    block_stats (block_id) {
        block_id -> Int4,
        block_size -> Int4,
        first_bytes -> Bytea,
    }
}

table! {
    blocks (id) {
        id -> Int4,
        codec_id -> Int4,
        cidv1 -> Bytea,
    }
}

table! {
    codecs (id) {
        id -> Int4,
        name -> Text,
    }
}

table! {
    errors (id) {
        id -> Int4,
        name -> Text,
    }
}

table! {
    failed_resolves (id) {
        block_id -> Int4,
        error_id -> Int4,
        ts -> Timestamp,
        id -> Int4,
    }
}

table! {
    successful_resolves (id) {
        block_id -> Int4,
        ts -> Timestamp,
        id -> Int4,
    }
}

table! {
    unixfs_blocks (block_id) {
        block_id -> Int4,
        unixfs_type_id -> Int4,
        size -> Int8,
        cumulative_size -> Int8,
        blocks -> Int4,
        num_links -> Int4,
    }
}

table! {
    unixfs_file_heuristics (block_id) {
        block_id -> Int4,
        tree_mime_mime_type -> Nullable<Text>,
        chardet_encoding -> Nullable<Text>,
        chardet_language -> Nullable<Text>,
        chardet_confidence -> Nullable<Float4>,
        chardetng_encoding -> Nullable<Text>,
        whatlang_language -> Nullable<Text>,
        whatlang_script -> Nullable<Text>,
        whatlang_confidence -> Nullable<Float8>,
    }
}

table! {
    unixfs_links (parent_block_id, name, referenced_cidv1) {
        parent_block_id -> Int4,
        name -> Text,
        size -> Int8,
        referenced_cidv1 -> Bytea,
    }
}

table! {
    unixfs_types (id) {
        id -> Int4,
        name -> Text,
    }
}

joinable!(block_stats -> blocks (block_id));
joinable!(blocks -> codecs (codec_id));
joinable!(failed_resolves -> blocks (block_id));
joinable!(failed_resolves -> errors (error_id));
joinable!(successful_resolves -> blocks (block_id));
joinable!(unixfs_blocks -> block_stats (block_id));
joinable!(unixfs_blocks -> unixfs_types (unixfs_type_id));
joinable!(unixfs_file_heuristics -> unixfs_blocks (block_id));
joinable!(unixfs_links -> unixfs_blocks (parent_block_id));

allow_tables_to_appear_in_same_query!(
    block_stats,
    blocks,
    codecs,
    errors,
    failed_resolves,
    successful_resolves,
    unixfs_blocks,
    unixfs_file_heuristics,
    unixfs_links,
    unixfs_types,
);
