-- db_txn_id_wraparound_check.sql
--
-- 2025.03.21

-- How close each db is to wraparound limit.
WITH freeze_max_age AS
(  SELECT
     current_setting('autovacuum_freeze_max_age')::INT AS autovacuum_freeze_max_age
)
SELECT
   datname                                                                  AS database_name
 , datfrozenxid                                                             AS datfrozenxid
 , age(datfrozenxid)                                                        AS xid_age
 , freeze_max_age.autovacuum_freeze_max_age                                 AS max_age
 , (age(datfrozenxid) > freeze_max_age.autovacuum_freeze_max_age + 50000)   AS near_wraparound
FROM pg_database
 , freeze_max_age
WHERE near_wraparound = 't'
ORDER BY xid_age DESC
 ;
