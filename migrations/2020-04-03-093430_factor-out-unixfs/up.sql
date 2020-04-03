-- Your SQL goes here

-- Haha, "migration"... Mistakes were made.
DROP INDEX IF EXISTS refs_referenced_cid;
DROP INDEX IF EXISTS refs_based_cid_id;
DROP TABLE IF EXISTS refs;
DROP TABLE IF EXISTS resolved_cids;
DROP TABLE IF EXISTS types;

CREATE TABLE codecs(
	id INT NOT NULL PRIMARY KEY,
	name TEXT NOT NULL
);

INSERT INTO codecs(id,name) VALUES
	(85,'raw'),
	(112,'dag-pb');

CREATE TABLE blocks(
	id SERIAL NOT NULL PRIMARY KEY,
	base32_cidv1 TEXT NOT NULL UNIQUE,
	codec_id INT NOT NULL REFERENCES codecs(id),
	block_size INT NOT NULL,
	first_bytes BYTEA NOT NULL -- up to first 32 bytes
);

CREATE TABLE errors(
	id INT NOT NULL PRIMARY KEY,
	name TEXT NOT NULL
);

INSERT INTO errors(id,name) VALUES (1,'failed to get block: deadline exceeded');

CREATE TABLE failed_blocks(
	block_id INT NOT NULL PRIMARY KEY REFERENCES blocks(id),
	error_id INT NOT NULL REFERENCES errors(id)
);

CREATE TABLE unixfs_types(
	id INT NOT NULL PRIMARY KEY,
	name TEXT NOT NULL
);

INSERT INTO unixfs_types(id,name) VALUES 
	(0,'raw'),
	(1,'directory'),
	(2,'file'),
	(3,'metadata'),
	(4,'symlink'),
	(5,'HAMTShard');

CREATE TABLE unixfs_blocks(
	block_id INT NOT NULL PRIMARY KEY REFERENCES blocks(id),
	unixfs_type_id INT NOT NULL REFERENCES unixfs_types(id),
	size BIGINT NOT NULL,
	cumulative_size BIGINT NOT NULL,
	blocks INT NOT NULL,
	num_links INT NOT NULL
);

CREATE TABLE unixfs_links(
	parent_block_id INT NOT NULL REFERENCES unixfs_blocks(block_id),
	referenced_base32_cidv1 TEXT NOT NULL,
	name TEXT NOT NULL,
	size BIGINT NOT NULL,
	PRIMARY KEY(parent_block_id, referenced_base32_cidv1, name)
);
CREATE INDEX unixfs_links_parent_block_id ON unixfs_links(parent_block_id);
CREATE INDEX unixfs_links_referenced_base32_cidv1 ON unixfs_links(referenced_base32_cidv1);

