-- This file should undo anything in `up.sql`

ALTER TABLE blocks ALTER COLUMN cidv1 DROP NOT NULL;
ALTER TABLE blocks ADD base32_cidv1 TEXT;

