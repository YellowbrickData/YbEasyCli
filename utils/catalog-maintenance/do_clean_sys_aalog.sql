-- do_clean_sys_aalog.sql
-- 
-- Remove orphan rows from sys.aalog. 
-- Only needs to be run once after upgrade to 5.2.17|5.4.13 or later.
--
-- 2024-05-22

SET work_mem TO 2000000; 

DELETE
FROM sys.aalog a
WHERE a.time < CURRENT_TIMESTAMP - interval '1 day'
AND NOT EXISTS
   (  SELECT
         t.table_id
      FROM sys.table t
      WHERE t.table_id = a.table_id
   )
;