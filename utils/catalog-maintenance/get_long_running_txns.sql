-- get_long_running_txns.sql
--
-- Show transactions active for more than 60 seconds.
--
-- 2024-04-12

SELECT
	  pid
	, session_id
	, datname
	, usename
	, application_name
	, client_addr
	, state
	, date_trunc('seconds', backend_start) AS backend_start
	, date_trunc('seconds', now() - backend_start)  AS backend_age
	, date_trunc('seconds', now() - xact_start)     AS txn_age
	, date_trunc('seconds', now() - state_change)   AS state_changed
	, date_trunc('seconds', now() - last_statement) AS last_stmt_age
FROM pg_stat_activity
WHERE xact_start < now() - interval '1 MINUTE';