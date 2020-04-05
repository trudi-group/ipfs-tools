table! {
    blocks (id) {
        id -> Int4,
        base32_cidv1 -> Text,
        codec_id -> Int4,
        block_size -> Int4,
        first_bytes -> Bytea,
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
    failed_blocks (block_id) {
        block_id -> Int4,
        error_id -> Int4,
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
    unixfs_links (parent_block_id, referenced_base32_cidv1, name) {
        parent_block_id -> Int4,
        referenced_base32_cidv1 -> Text,
        name -> Text,
        size -> Int8,
    }
}

table! {
    unixfs_types (id) {
        id -> Int4,
        name -> Text,
    }
}

joinable!(blocks -> codecs (codec_id));
joinable!(failed_blocks -> blocks (block_id));
joinable!(failed_blocks -> errors (error_id));
joinable!(unixfs_blocks -> blocks (block_id));
joinable!(unixfs_blocks -> unixfs_types (unixfs_type_id));
joinable!(unixfs_file_heuristics -> unixfs_blocks (block_id));
joinable!(unixfs_links -> unixfs_blocks (parent_block_id));

allow_tables_to_appear_in_same_query!(
    blocks,
    codecs,
    errors,
    failed_blocks,
    unixfs_blocks,
    unixfs_file_heuristics,
    unixfs_links,
    unixfs_types,
);
