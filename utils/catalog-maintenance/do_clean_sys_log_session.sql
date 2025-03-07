-- do_clean_sys_log_session.sql
-- 
-- Remove rows from sys.log_session older than 90 days.
-- Uses end_time instead of start_time as sessions can span multiple days.
-- sys.log_session is a view on top of sys._log_session.
--
-- 2024-10-04

SET work_mem TO 2000000; 

\echo Copying rows to be deleted to log_session.csv
\COPY (SELECT * FROM sys._log_session WHERE end_time < (CURRENT_DATE - 90)) TO 'log_session.csv' WITH (FORMAT CSV);

\echo DELETING rows for sessions older than 90 days
DELETE
FROM sys._log_session
WHERE end_time < (CURRENT_DATE - 90);

\echo DONE
\echo