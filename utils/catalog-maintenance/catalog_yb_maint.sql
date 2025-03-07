-- catalog_yb_maint.sql
--
-- VACUUM FULL "yellowbrick" database catalog tables typically needing it:
-- . Shared sys.* catalog tables
-- . Shared pg_catalog.* tables
-- . Non-shared pg_catalog.* tables
--
-- This is meant only to be run against the database yellowbrick.
--
-- 2024-08-30

\c yellowbrick
\set prc 0
\set maint_cmd 'VACUUM FULL'
\set command 'set application_name = ''syscat-maint-' :prc ''';'
:command

-- 2000000 is 2000000kB e.g. 1.9 GB
SET maintenance_work_mem TO 2000000;

\qecho 'Beginning shared (global) table maintenance in database yellowbrick'

:maint_cmd sys.shardstore;
:maint_cmd sys.kri;
:maint_cmd sys.yb_query_plan;
:maint_cmd sys.rowunique;
:maint_cmd sys.yb_query_execution_analyze;
:maint_cmd sys.yb_query_execution_usage;
:maint_cmd sys.aa_tables;
:maint_cmd sys.aalog;
:maint_cmd pg_catalog.pg_shdepend;
:maint_cmd pg_catalog.yb_deletes_pg_shdepend;
:maint_cmd pg_catalog.pg_auth_members;

\qecho 'Beginning non-shared table maintenance in database yellowbrick'
\i catalog_db_maint.sql