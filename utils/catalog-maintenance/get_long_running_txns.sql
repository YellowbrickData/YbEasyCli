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
	, date_trunc('seconds', state_change) AS state_change
	, date_trunc('seconds', last_statement) AS last_statement
	, date_trunc('seconds', backend_start) AS backend_start
	, now() - backend_start AS backend_age
FROM pg_stat_activity
WHERE state != 'idle'
	AND state_change < now() - interval '1 MINUTE'