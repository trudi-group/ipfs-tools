-- This file was automatically created by Diesel to setup helper functions
-- and other internal bookkeeping. This file is safe to edit, any future
-- changes will be added to existing projects as new migrations.

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


-- Sets up a trigger for the given table to automatically set a column called
-- `updated_at` whenever the row is modified (unless `updated_at` was included
-- in the modified columns)
--
-- # Example
--
-- ```sql
-- CREATE TABLE users (id SERIAL PRIMARY KEY, updated_at TIMESTAMP NOT NULL DEFAULT NOW());
--
-- SELECT diesel_manage_updated_at('users');
-- ```
CREATE OR REPLACE FUNCTION diesel_manage_updated_at(_tbl regclass) RETURNS VOID AS $$
BEGIN
    EXECUTE format('CREATE TRIGGER set_updated_at BEFORE UPDATE ON %s
                    FOR EACH ROW EXECUTE PROCEDURE diesel_set_updated_at()', _tbl);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION diesel_set_updated_at() RETURNS trigger AS $$
BEGIN
    IF (
        NEW IS DISTINCT FROM OLD AND
        NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at
    ) THEN
        NEW.updated_at := current_timestamp;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
