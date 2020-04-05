-- Your SQL goes here

ALTER TABLE unixfs_file_heuristics RENAME encoding TO chardet_encoding;
ALTER TABLE unixfs_file_heuristics RENAME language TO chardet_language;
ALTER TABLE unixfs_file_heuristics RENAME mime_type TO tree_mime_mime_type;
ALTER TABLE unixfs_file_heuristics ADD chardetng_encoding TEXT;
ALTER TABLE unixfs_file_heuristics ADD whatlang_language TEXT;
ALTER TABLE unixfs_file_heuristics ADD whatlang_script TEXT;
ALTER TABLE unixfs_file_heuristics ADD whatlang_confidence FLOAT;
