-- This file was automatically created by Diesel to setup helper functions
-- and other internal bookkeeping. This file is safe to edit, any future
-- changes will be added to existing projects as new migrations.

DROP TABLE IF EXISTS refs;
DROP TABLE IF EXISTS resolved_cids;
DROP TABLE IF EXISTS types;

DROP FUNCTION IF EXISTS diesel_manage_updated_at(_tbl regclass);
DROP FUNCTION IF EXISTS diesel_set_updated_at();
