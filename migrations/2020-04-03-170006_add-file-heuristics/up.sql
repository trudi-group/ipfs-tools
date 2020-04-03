-- Your SQL goes here

CREATE TABLE unixfs_file_heuristics(
	block_id INT NOT NULL PRIMARY KEY REFERENCES unixfs_blocks(block_id),
	mime_type TEXT,
	encoding TEXT,
	language TEXT,
	chardet_confidence REAL
);

