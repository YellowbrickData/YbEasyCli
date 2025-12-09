-- orphan_snapshots.sql
-- 2025.12.09

WITH snapshots AS
(  SELECT
      date_trunc('secs', bs.creation_time)::timestamp AS snap_created
    , bs.backup_txid                                  AS backup_txid
    , bs.backup_txsnap                                AS backup_txsnap
    , bs.obsolete_for_rollback                        AS obsolete_for_rollback
    , bs.obsolete_for_backup                          AS obsolete_for_backup
    , bs.snapshot_name                                AS snapshot_name
    , bs.snapshot_name = bc.oldest_backup_point_id    AS oldest_backup
    , bs.snapshot_name = bc.last_backup_point_id      AS latest_backup
    , date_trunc('secs', date_trunc('second', CURRENT_TIMESTAMP - bs.creation_time))
                                                      AS snap_age
    , bs.database_id                                  AS snap_db_id
    , d.name                                          AS snap_dbname
    , '->'                                            AS dep
    , bd.database_id                                  AS dep_db_id
    , bd.chain_name                                   AS dep_chain_name
    , NOT(  d.name = 'yellowbrick'
         OR bs.backup_txsnap = '0:3:3:')
      AND bd.chain_name IS NULL                       AS orphan
    , '->'                                            AS chain
    , bc.*
   FROM sys.backup_snapshots AS bs
   JOIN sys.database AS d ON d.database_id = bs.database_id
   LEFT JOIN sys.backup_depends AS bd
   JOIN sys.backup_chains AS bc ON bc.database_id = bd.database_id
      AND bc.chain_name = bd.chain_name ON bs.oid = bd.ref_object
      AND bs.database_id = bd.database_id
      AND bd.ref_class::regclass = 'sys.backup_snapshots'::regclass
   WHERE
      true
   ORDER BY bs.database_id
    , bd.chain_name nulls last
    , bs.creation_time
)

SELECT
   snap_dbname
 , snap_db_id
 , snapshot_name
 , orphan
 , snap_created
 , snap_age
-- , for_replication
FROM snapshots
WHERE
   orphan = true
ORDER BY snap_dbname
 , snap_created DESC
;