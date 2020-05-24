-- This file should undo anything in `up.sql`

ALTER TABLE unixfs_links ADD referenced_base32_cidv1 TEXT;
ALTER TABLE unixfs_links DROP CONSTRAINT unixfs_links_pkey;
ALTER TABLE unixfs_links ADD PRIMARY KEY (parent_block_id, name, referenced_base32_cidv1);
ALTER TABLE unixfs_links ALTER COLUMN referenced_cidv1 DROP NOT NULL;
