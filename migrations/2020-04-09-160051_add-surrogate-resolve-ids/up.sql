-- Your SQL goes here

ALTER TABLE successful_resolves DROP CONSTRAINT successful_resolves_pkey;
ALTER TABLE successful_resolves ADD id SERIAL NOT NULL PRIMARY KEY;
CREATE INDEX successful_resolves_block_id ON successful_resolves(block_id);

ALTER TABLE failed_resolves DROP CONSTRAINT failed_blocks_pkey;
ALTER TABLE failed_resolves ADD id SERIAL NOT NULL PRIMARY KEY;
CREATE INDEX failed_resolves_block_id ON failed_resolves(block_id);
