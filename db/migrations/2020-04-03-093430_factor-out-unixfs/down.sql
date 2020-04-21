-- This file should undo anything in `up.sql`

DROP INDEX IF EXISTS unixfs_links_parent_block_id;
DROP INDEX IF EXISTS unixfs_links_referenced_base32_cidv1;
DROP TABLE IF EXISTS unixfs_links;
DROP TABLE IF EXISTS unixfs_blocks;
DROP TABLE IF EXISTS unixfs_types;
DROP TABLE IF EXISTS failed_blocks;
DROP TABLE IF EXISTS errors;
DROP TABLE IF EXISTS blocks;
DROP TABLE IF EXISTS codecs;

CREATE TABLE types(
	type_id SERIAL NOT NULL PRIMARY KEY,
	name TEXT NOT NULL
);

CREATE TABLE resolved_cids(
	id SERIAL NOT NULL PRIMARY KEY,
	base32_cidv1 TEXT NOT NULL UNIQUE,
	type_id INT NOT NULL REFERENCES types(type_id),
	cumulative_size BIGINT NOT NULL,
	block_size BIGINT NOT NULL,
	links_size BIGINT NOT NULL,
	data_size BIGINT NOT NULL,
	num_links INT NOT NULL,
	blocks INT NOT NULL
);

CREATE TABLE refs(
	base_cid_id INT NOT NULL REFERENCES resolved_cids(id),
	referenced_base32_cidv1 TEXT NOT NULL,
	name TEXT NOT NULL,
	size BIGINT NOT NULL,
	PRIMARY KEY(base_cid_id, referenced_base32_cidv1, name)
);
CREATE INDEX refs_referenced_cid ON refs(referenced_base32_cidv1);
CREATE INDEX refs_based_cid_id ON refs(base_cid_id);

INSERT INTO types (type_id, name) VALUES (1, 'directory');
INSERT INTO types (type_id, name) VALUES (2, 'file');
