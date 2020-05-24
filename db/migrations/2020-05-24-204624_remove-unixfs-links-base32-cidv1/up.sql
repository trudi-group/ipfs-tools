-- Your SQL goes here

ALTER TABLE unixfs_links ALTER COLUMN referenced_cidv1 SET NOT NULL;
ALTER TABLE unixfs_links DROP CONSTRAINT unixfs_links_pkey;
ALTER TABLE unixfs_links DROP IF EXISTS referenced_base32_cidv1;
ALTER TABLE unixfs_links ADD PRIMARY KEY (parent_block_id, name, referenced_cidv1);

