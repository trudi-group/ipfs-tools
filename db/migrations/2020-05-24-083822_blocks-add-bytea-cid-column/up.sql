-- Your SQL goes here

ALTER TABLE blocks DROP CONSTRAINT IF EXISTS blocks_base32_cidv1_key;
ALTER TABLE blocks ADD cidv1 BYTEA UNIQUE;

DROP INDEX IF EXISTS unixfs_links_referenced_base32_cidv1_key;
ALTER TABLE unixfs_links ADD referenced_cidv1 BYTEA;
CREATE INDEX unixfs_links_referenced_cidv1_key ON unixfs_links USING hash (referenced_cidv1);
