-- db_tbl_txn_id_wraparound_check.sql
--
-- 2025.03.21
--
-- TODO: FIX so system tables are checked but shared ones only in yellowbrick database.

-- List all tables near wraparound (require a VACUUM FULL)
SELECT
   c.oid::regclass                                                                 AS table_name
 , c.relfrozenxid                                                                  AS relfrozenxid
 , age(c.relfrozenxid)                                                             AS xid_age
 , current_setting('autovacuum_freeze_max_age')::int                               AS autovacuum_freeze_max_age
 , (age(c.relfrozenxid) > current_setting('autovacuum_freeze_max_age')::int * 0.9) AS near_wraparound
FROM pg_class        AS c
   JOIN pg_namespace AS n ON c.relnamespace = n.oid
WHERE c.relkind   IN ('r', 't')                                  -- Only regular tables and TOAST tables
AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sys') -- Exclude system schemas
and near_wraparound = 't'
ORDER BY xid_age DESC
;
