-- catalog_db_maint.sql
--
-- VACUUM FULL only the non-shared PG tables that typically have any meaningful
-- growth and so might need it in the current database.
--
-- This methodology does not require locks on shared objects as VACUUM FULL of
-- the entire database does. So this does not wait on blocked or create blocking
-- locks (unless there is a write transaction in flight in the db). 
--
-- 2024-05-16

SELECT CURRENT_DATABASE() AS cdb
\gset
SELECT date_trunc('second', now()) AS ts
\gset
\qecho [:prc] [:ts] Beginning maintenance for database :cdb
\set command 'set application_name = ''syscat-maint-' :prc ''';'
:command
SET maintenance_work_mem TO 2000000;

VACUUM FULL pg_catalog.pg_attrdef;
VACUUM FULL pg_catalog.pg_attribute;
VACUUM FULL pg_catalog.pg_class;
VACUUM FULL pg_catalog.pg_constraint;
VACUUM FULL pg_catalog.pg_default_acl;
VACUUM FULL pg_catalog.pg_depend;
VACUUM FULL pg_catalog.pg_description;
VACUUM FULL pg_catalog.pg_rewrite;
VACUUM FULL pg_catalog.pg_statistic;
VACUUM FULL pg_catalog.pg_type;