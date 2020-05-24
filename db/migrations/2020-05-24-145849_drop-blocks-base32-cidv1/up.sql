-- Your SQL goes here

ALTER TABLE blocks DROP IF EXISTS base32_cidv1;
ALTER TABLE blocks ALTER COLUMN cidv1 SET NOT NULL;
