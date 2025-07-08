-- aged_backup_chains_smry.sql
-- 
-- Backup & replication chains not used the last n days summary.
--
-- Prerequisites:
-- . min_horizon_age variable must be passed in.
-- . Assumes superuers or has system prvilege
--
-- 2025-02-28 - Inital version


-- \set min_snapshot_age 30
-- \set min_horizon_age  30

WITH curr_chains AS
     (SELECT  database_id       AS database_id
            , policy            AS policy
            , for_replication   AS for_replication
            , MAX(creation_time)AS creation_time
     FROM     sys.backup_chains
     GROUP BY database_id, policy, for_replication
     )
   , all_chains AS
     (SELECT   d.database_id::INT8                                                                           AS database_id
             , d.name::VARCHAR(128)                                                                          AS database_name
             , ac.chain_name::VARCHAR(256)                                                                   AS chain_name
             , DECODE( for_replication, 't','replication','backup')::VARCHAR(16)                             AS chain_type             
             , CASE WHEN ac.creation_time = cc.creation_time THEN 'current' ELSE 'previous' END::VARCHAR(16) AS history
             , date_trunc('secs', ac.creation_time)::TIMESTAMP                                               AS creation_time
             , ceil(extract(epoch FROM(CURRENT_TIMESTAMP - ac.creation_time)) /(60 * 60 * 24))::INT4         AS chain_days
             , date_trunc('secs', bseh.creation_time)::TIMESTAMP                                             AS event_horizon_time
             , ceil(extract(epoch FROM(CURRENT_TIMESTAMP - bseh.creation_time)) /(60 * 60 * 24))::INT4       AS event_horizon_days
             , date_trunc('secs', bsmr.creation_time)::TIMESTAMP                                             AS last_snapshot
             , ceil(extract(epoch FROM(CURRENT_TIMESTAMP - bsmr.creation_time)) /(60 * 60 * 24))::INT4       AS snapshot_days
             , TRANSLATE(policy, '{}','')::VARCHAR(60000)                                                    AS policy             
     FROM      sys.backup_chains ac
     LEFT JOIN curr_chains cc
        USING (database_id, policy, for_replication)
     LEFT JOIN sys.backup_snapshots bsmr /* the most recent backup snapshot */
        ON ac.last_backup_point_id = bsmr.snapshot_name
        AND cc.database_id         = bsmr.database_id
     LEFT JOIN sys.backup_snapshots bseh /* the event horizon snapshot */
        ON ac.last_backup_point_id = bseh.snapshot_name
        AND cc.database_id         = bseh.database_id
     JOIN      sys.database d
        ON cc.database_id = d.database_id
     )
SELECT
   chain_type                    AS chain_type
 , history                       AS history
 , COUNT(DISTINCT database_name) AS databases
 , COUNT(DISTINCT chain_name )   AS chains
 , MAX(chain_days)               AS mx_chn_days
 , MAX(event_horizon_days)       AS mx_hrzn_days
 , MAX(snapshot_days)            AS mx_snpsht_days
FROM all_chains
WHERE event_horizon_days > :min_horizon_age
group by 1, 2
ORDER BY 1, 2
;
