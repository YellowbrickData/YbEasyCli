-- do_clean_sys_log_authentication.sql
-- 
-- Remove rows from sys.log_authentication older than 90 days.
-- Does a COPY of data to be removed to a CSV file before DELETE.
-- You should VACUUM FULL the table after DELETE.
-- sys.log_authentication is a view on top of sys._log_authentication
--
-- 2024-10-04

SET work_mem TO 2000000; 

\echo Copying rows to be deleted to log_authentication.csv
\COPY (SELECT * FROM sys._log_authentication WHERE end_time < (CURRENT_DATE - 90)) TO 'log_authentication.csv' WITH (FORMAT CSV);

\echo DELETING rows for sessions older than 90 days
DELETE
FROM sys._log_authentication
WHERE end_time < (CURRENT_DATE - 90);

\echo DONE
\echo