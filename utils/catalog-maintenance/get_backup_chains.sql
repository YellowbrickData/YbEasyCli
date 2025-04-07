-- 2023 Jul 27, Eugene Mindrov: initial version
-- 2024 Mar 28, Eugene Mindrov: added check for stray rollback snapshots and submitted to GitHub
SELECT d.oid                                                      AS db_id
    , d.datname                                                   AS db_name
    , d.dathotstandby                                             AS hotstandby
    -- NOTE: We don't care about c.last_backup_point_id here as it's also modified by incremental snapshots
    , Coalesce(bs.snapshot_name, c.oldest_backup_point_id)        AS last_bck_name
    , date_trunc('secs', bs.creation_time)::TIMESTAMP             AS last_bck_created
    , extract(DAY FROM date_trunc('secs',now()-bs.creation_time)) AS last_bck_age
    , bs.snapshot_name IS NULL                                    AS last_bck_not_found
    , Coalesce(rs.snapshot_name, c.oldest_rollback_point_id)      AS last_rlb_name
    , date_trunc('secs', rs.creation_time)::TIMESTAMP             AS last_rlb_created
    , extract(DAY FROM date_trunc('secs',now()-rs.creation_time)) AS last_rlb_age
    , rs.snapshot_name IS NULL
        AND c.oldest_rollback_point_id IS NOT NULL                AS last_rlb_not_found
    , c.inprogress_backup_point_id                                AS in_progress_bck_name
    , r.name                                                      AS repl_name
    , c.for_replication                                           AS chain_repl
    , c.chain_name                                                AS chain_name
    , date_trunc('secs',c.creation_time)::TIMESTAMP               AS chain_created
    -- NOTE: A chain age could be legitimately very old, examples:
    --       1) active replication created a long time ago
    --       2) a chain that gets repeatedly reused/"broken" by ybbackup (example: default)
    , extract(DAY FROM date_trunc('secs',now()-c.creation_time))  AS chain_age
    , IIF(c.lock IS NULL
        ,IIF(c.for_replication
            ,'replica-'
                ||IIF(d.dathotstandby
                    ,'target-valid'
                    ,IIF(r.name IS NULL
                        ,'target-retired'
                        ,'source'))
            ,IIF(c.last_backup_point_id IS NULL
                , 'restore-'
                ||IIF(d.dathotstandby
                    ,'active'
                    ,'retired')
                , 'backup')
            )
        ,'locked')::varchar(32)                                   AS chain_type
    , LEFT(c.lock,8)||'...'                                       AS chain_lock
FROM sys.backup_chains             AS c
    LEFT JOIN sys.backup_snapshots AS bs -- backup snapshots
        ON bs.snapshot_name = c.oldest_backup_point_id AND bs.database_id = c.database_id
    LEFT JOIN sys.backup_snapshots AS rs -- rollback snapshots
        ON rs.snapshot_name = c.oldest_rollback_point_id AND rs.database_id = c.database_id
    LEFT JOIN pg_database          AS d
        ON d.oid = c.database_id
    LEFT JOIN sys.replica          AS r
        ON r.backup_chain_id = c.chain_name AND r.database_id = c.database_id
WHERE TRUE --> Add your own filters, for example:
--  AND c.for_replication
ORDER BY d.datname
    , bs.creation_time NULLS LAST;
