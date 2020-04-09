-- This file should undo anything in `up.sql`

-- we have to delete duplicates...
DELETE FROM successful_resolves s1 USING successful_resolves s2 WHERE s1.ctid < s2.ctid AND s1.block_id = s2.block_id;
DELETE FROM failed_resolves f1 USING failed_resolves f2 WHERE f1.ctid < f2.ctid AND f1.block_id = f2.block_id;

-- and then change the keys back...
ALTER TABLE successful_resolves DROP CONSTRAINT successful_resolves_pkey;
ALTER TABLE successful_resolves DROP id;
DROP INDEX successful_resolves_block_id;
CREATE UNIQUE INDEX successful_resolves_block_id ON successful_resolves(block_id);
ALTER TABLE successful_resolves ADD PRIMARY KEY USING INDEX successful_resolves_block_id;
ALTER INDEX successful_resolves_block_id RENAME TO successful_resolves_pkey;

ALTER TABLE failed_resolves DROP CONSTRAINT failed_resolves_pkey;
ALTER TABLE failed_resolves DROP id;
DROP INDEX failed_resolves_block_id;
CREATE UNIQUE INDEX failed_resolves_block_id ON failed_resolves(block_id);
ALTER TABLE failed_resolves ADD PRIMARY KEY USING INDEX failed_resolves_block_id;
ALTER INDEX failed_resolves_block_id RENAME TO failed_blocks_pkey;

