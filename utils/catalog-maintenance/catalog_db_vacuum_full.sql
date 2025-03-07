-- catalog_db_vacuum_full.sql
--
-- VACUUM FULL the entire database catalog. 
-- Typically run only against the database "yellowbrick".
--
-- 2024-09-19

-- 2000000 is 2000000kB e.g. 1.9 GB
SET maintenance_work_mem TO 2000000;

SELECT 'Begining VACUUM FULL in database ' || CURRENT_DATABASE() AS message;

VACUUM FULL;