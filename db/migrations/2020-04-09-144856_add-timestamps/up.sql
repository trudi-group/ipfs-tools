-- Your SQL goes here

-- Create/rename tables to track when a block was successfully resolved/failed to resolve.
CREATE TABLE successful_resolves (
	block_id INT NOT NULL PRIMARY KEY REFERENCES blocks(id),
	ts TIMESTAMP NOT NULL
);

ALTER TABLE failed_blocks RENAME TO failed_resolves;

ALTER TABLE failed_resolves ADD ts TIMESTAMP NOT NULL;

-- Factor out basic block stats from the blocks table, as we don't have that info for failed blocks.
CREATE TABLE block_stats (
	block_id INT NOT NULL PRIMARY KEY REFERENCES blocks(id),
	block_size INT NOT NULL,
	first_bytes BYTEA NOT NULL
);

-- Shovel current (=all successful) blocks' stats into the new table.
INSERT INTO block_stats SELECT id, block_size, first_bytes FROM blocks;

-- Remove stats from blocks table.
ALTER TABLE blocks DROP block_size;
ALTER TABLE blocks DROP first_bytes;

-- Change the UnixFS foreign key to point to block_stats so we can guarantee to have block stats for every UnixFS block.
ALTER TABLE unixfs_blocks DROP CONSTRAINT unixfs_blocks_block_id_fkey;
ALTER TABLE unixfs_blocks ADD CONSTRAINT unixfs_blocks_block_id_fkey FOREIGN KEY (block_id) REFERENCES block_stats (block_id);
