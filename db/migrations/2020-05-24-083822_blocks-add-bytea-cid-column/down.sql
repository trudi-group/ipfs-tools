-- This file should undo anything in `up.sql`

DROP INDEX blocks_cidv1_key;
ALTER TABLE blocks DROP cidv1;
ALTER TABLE blocks ADD CONSTRAINT blocks_base32_cidv1_key UNIQUE (base32_cidv1);

DROP INDEX unixfs_links_referenced_cidv1_key;
ALTER TABLE unixfs_links DROP referenced_cidv1;
CREATE INDEX unixfs_links_referenced_base32_cidv1 ON unixfs_links(referenced_base32_cidv1);
