-- This file should undo anything in `up.sql`

-- Change the UnixFS foreign key back to the blocks table.
ALTER TABLE unixfs_blocks DROP CONSTRAINT unixfs_blocks_block_id_fkey;
ALTER TABLE unixfs_blocks ADD CONSTRAINT unixfs_blocks_block_id_fkey FOREIGN KEY (block_id) REFERENCES blocks (id);

-- Remove everything from failed_resolves and blocks for which we don't have an entry in block_stats, because we need to merge block_stats with blocks.
DELETE FROM failed_resolves f WHERE NOT EXISTS (SELECT * FROM block_stats s WHERE s.block_id = f.block_id);
DELETE FROM blocks b WHERE NOT EXISTS (SELECT * FROM block_stats s WHERE s.block_id = b.id);

-- Merge block_stats into blocks.
ALTER TABLE blocks ADD block_size INT;
ALTER TABLE blocks ADD first_bytes bytea;

UPDATE blocks b SET block_size = (SELECT block_size FROM block_stats s WHERE s.block_id = b.id);
UPDATE blocks b SET first_bytes = (SELECT first_bytes FROM block_stats s WHERE s.block_id = b.id);

ALTER TABLE blocks ALTER COLUMN block_size SET NOT NULL;
ALTER TABLE blocks ALTER COLUMN first_bytes SET NOT NULL;

-- Remove block_stats
DROP TABLE block_stats;

-- Rename/alter these back to the useless form they were before.
DROP TABLE successful_resolves;
ALTER TABLE failed_resolves DROP ts;
ALTER TABLE failed_resolves RENAME TO failed_blocks;
