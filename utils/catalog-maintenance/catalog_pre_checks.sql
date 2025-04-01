-- catalog_pre_checks.sql
--
-- Check critical system states prior to starting catalog maintenance.i.e.
-- . Check for long running sessions.
-- . Check for old backup chains.
-- . Clear obsolete sys.aalog rows.
--
-- Typically you will want to run this an hour ahead of time.
--
-- 2024-10-04 - Send backup chain summary to console, full output to file.
-- 2024-09-03

\pset pager off
\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

\echo == Check for potentially obsolete backup_chains
\i get_old_backup_chains.sql

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo == Check for long running sessions (older than 60 seconds)
\i get_long_running_txns.sql

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo == System catalog total size summary
\i get_system_catalog_total_size_summary.sql

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo == Active replicas
SELECT r.name AS "replica", r.backup_chain_id AS backup_chain, r.status
	, date_trunc('second', r.last_replication_time) AS last_replicated, now() - last_replicated AS age
	, d.datname AS "database", r.alias AS remote_database, s.name AS remote_server_name, s.host AS remote_server_host
FROM sys.REPLICA AS r
	JOIN pg_database AS d ON d.oid = r.database_id
	JOIN sys.remote_server AS s USING (remote_server_id)
ORDER BY d.datname;

\echo ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\echo == The End