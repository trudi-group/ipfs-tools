-- This file should undo anything in `up.sql`


ALTER TABLE unixfs_file_heuristics DROP chardetng_encoding;
ALTER TABLE unixfs_file_heuristics DROP whatlang_language;
ALTER TABLE unixfs_file_heuristics DROP whatlang_script;
ALTER TABLE unixfs_file_heuristics DROP whatlang_confidence;

ALTER TABLE unixfs_file_heuristics RENAME chardet_encoding TO encoding;
ALTER TABLE unixfs_file_heuristics RENAME chardet_language TO language;
ALTER TABLE unixfs_file_heuristics RENAME tree_mime_mime_type TO mime_type;
