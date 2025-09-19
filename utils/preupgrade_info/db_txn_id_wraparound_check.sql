-- db_txn_id_wraparound_check.sql
--
-- 2025.03.21

-- How close the currnt db is to the wraparound limit.
WITH freeze_max_age AS
(  SELECT
     current_setting('autovacuum_freeze_max_age')::INT AS autovacuum_freeze_max_age
)
SELECT
   datname                                                                  AS database_name
 , datfrozenxid                                                             AS datfrozenxid
 , age(datfrozenxid)                                                        AS xid_age
 , freeze_max_age.autovacuum_freeze_max_age                                 AS max_age
 , (age(datfrozenxid) > freeze_max_age.autovacuum_freeze_max_age * 50000.0) AS near_wraparound
FROM pg_database
 , freeze_max_age
WHERE near_wraparound = 't'
ORDER BY xid_age DESC
 ;

WITH relfrozenxid AS
   (  SELECT MIN(relfrozenxid::varchar::bigint) AS relfrozenxid
      FROM pg_class
      WHERE relkind = 'r'
   )
 , datfrozenxid AS
   (  SELECT datfrozenxid
      FROM pg_database
      WHERE oid IN
         (  SELECT oid
            FROM pg_database
            WHERE datname = current_database()
         )
   )
 , db_names AS
   (  SELECT MAX(LENGTH(name)) AS pad_chars
      FROM sys.database
      LIMIT 1
   )
 , is_healthy AS
   (  SELECT current_database()                                              AS db
       , NOT(  datfrozenxid::varchar::bigint > relfrozenxid::bigint + 50000) AS is_healthy
       , datfrozenxid                                                        AS datfrozenxid
       , relfrozenxid                                                        AS relfrozenxid
       , pad_chars                                                           AS pad_chars
      FROM relfrozenxid
      CROSS JOIN datfrozenxid
      CROSS JOIN db_names
   )
SELECT 'DB IS ' || DECODE(true, is_healthy, '', 'NOT ') || 'healthy: ' 
        || rpad(db, pad_chars) 
        || ' datfrozenxid: ' || rpad(datfrozenxid::varchar, 10) 
        || ' relfrozenxid: ' || relfrozenxid 
        AS db_health
FROM is_healthy
 ;