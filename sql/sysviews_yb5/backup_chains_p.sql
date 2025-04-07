/* ****************************************************************************
** backup_chains_p()
**
** Existing backup chains with creating and snapshot info.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2018 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2023.06.05 - Cosmetic code updates.
** . 2023.05.15 - ybCliUtils inclusion.
** . 2024.05.20 - refactored backup chain query and returned columns
*/

/* ****************************************************************************
**  Example results:
**
** db_id  |db_nm              |is_ht_stndby|wrn |lst_bkup_nm                       |lst_bkup_crtd          |lst_bkup_age|lst_bkup_nt_fnd|lst_rlbk_nm                       |lst_rlbk_created       |lst_rlbk_age|lst_rlbk_nt_fnd|in_prgrs_bkup_nm|rplca_nm        |chn_rplctn|chn_nm          |chn_crtd               |chn_age|chn_typ             |chn_lck|plcy                              |
** -------+-------------------+------------+----+----------------------------------+-----------------------+------------+---------------+----------------------------------+-----------------------+------------+---------------+----------------+----------------+----------+----------------+-----------------------+-------+--------------------+-------+----------------------------------+
**  865953|dbadmin            |true        |    |dbadmin_replica_24_05_19_06_44_54 |2024-05-19 08:44:56.000|           1|false          |dbadmin_replica_24_05_19_06_44_54 |2024-05-19 08:44:56.000|           1|false          |                |                |true      |dbadmin_replica |2024-02-23 13:21:07.000|     87|replica-target-valid|       |                                  |
**   28763|eugene             |false       |    |default_23_04_04_15_41_33         |2023-04-04 15:41:33.000|         411|false          |                                  |                       |            |false          |                |                |false     |default         |2023-04-04 15:41:33.000|    411|backup              |       |                                  |
**   28763|eugene             |false       |    |chain5224_23_04_24_10_41_36       |2023-04-24 13:41:37.000|         392|false          |                                  |                       |            |false          |                |                |false     |chain5224       |2023-04-24 13:41:37.000|    392|backup              |       |                                  |
**   28763|eugene             |false       |    |zd21737_24_04_05_11_45_24         |2024-04-05 11:45:25.000|          45|false          |                                  |                       |            |false          |                |                |false     |zd21737         |2024-04-05 11:43:19.000|     45|backup              |       |                                  |
**   28763|eugene             |false       |    |backup_aws_24_04_05_13_25_44      |2024-04-05 13:25:46.000|          45|false          |                                  |                       |            |false          |                |                |false     |backup_aws      |2024-04-05 12:02:41.000|     45|backup              |       |                                  |
**   28763|eugene             |false       |    |zd22090_24_05_16_10_56_03         |2024-05-16 10:56:03.000|           4|false          |                                  |                       |            |false          |                |                |false     |zd22090         |2024-05-16 10:56:03.000|      4|backup              |       |                                  |
** 1059276|eugene01           |false       |    |dropme_24_04_05_13_47_36          |2024-04-05 13:47:39.000|          45|false          |                                  |                       |            |false          |                |                |false     |dropme          |2024-04-05 12:42:45.000|     45|backup              |       |                                  |
** 1059276|eugene01           |false       |    |snap01                            |2024-05-16 18:17:32.000|           3|false          |                                  |                       |            |false          |                |                |false     |chain01         |2024-05-16 18:17:32.000|      3|backup              |       |                                  |
** 1059276|eugene01           |false       |    |snap02                            |2024-05-16 18:25:38.000|           3|false          |                                  |                       |            |false          |                |                |false     |chain02         |2024-05-16 18:25:38.000|      3|backup              |       |                                  |
** 1059276|eugene01           |false       |    |eugene01_replica_24_05_20_18_40_09|2024-05-20 14:40:09.000|           0|false          |eugene01_replica_24_05_20_18_40_09|2024-05-20 14:40:09.000|           0|false          |                |eugene01_replica|true      |eugene01_replica|2024-05-03 12:02:32.000|     17|replica-source      |       |                                  |
** 1179570|eugene01_replicated|true        |    |eugene01_replica_24_05_20_18_40_09|2024-05-20 14:40:09.000|           0|false          |eugene01_replica_24_05_20_18_40_09|2024-05-20 14:40:09.000|           0|false          |                |                |true      |eugene01_replica|2024-05-03 12:02:33.000|     17|replica-target-valid|       |                                  |
** 1059277|eugene02           |false       |    |chain01_24_03_08_14_16_30         |2024-03-08 14:16:30.000|          72|false          |                                  |                       |            |false          |                |                |false     |chain01         |2024-03-08 14:16:30.000|     72|backup              |       |                                  |
** 1215109|eugene_r01         |true        |    |zd22090_24_05_16_10_56_03         |2024-05-16 10:56:24.000|           4|false          |                                  |                       |            |false          |                |                |false     |zd22090         |2024-05-16 10:56:23.000|      4|restore-active      |       |                                  |
**   28824|eugene_restored    |true        |    |default_23_04_04_15_41_33         |2023-04-04 15:48:42.000|         411|false          |                                  |                       |            |false          |                |                |false     |default         |2023-04-04 15:48:42.000|    411|restore-active      |       |                                  |
** 1114224|eugene_restored00  |true        |    |backup_aws_24_04_05_13_25_44      |2024-04-05 13:30:57.000|          45|false          |                                  |                       |            |false          |                |                |false     |backup_aws      |2024-04-05 13:28:05.000|     45|restore-active      |       |                                  |
** 1095992|eugene_zd21386     |false       |age |default_24_03_28_11_01_39         |2024-03-28 11:01:39.000|          53|false          |default_24_03_28_11_05_36         |2024-03-28 11:05:38.000|          53|false          |                |                |false     |default         |2024-03-28 11:01:39.000|     53|backup              |       |                                  |
** 1064211|eugene_zd_21386    |false       |    |chain00_24_03_14_14_32_53         |2024-03-14 14:32:58.000|          67|false          |                                  |                       |            |false          |                |                |false     |chain00         |2024-03-14 14:17:07.000|     67|backup              |       |                                  |
** 1174744|eugene_zd_21713_r0 |true        |    |cumu3                             |2024-05-01 15:49:36.000|          18|false          |                                  |                       |            |false          |                |                |false     |zd_21713        |2024-05-01 15:48:37.000|     18|restore-active      |       |"excludedSchemas":["excluded\\_1"]|
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS backup_chains_t CASCADE
;


CREATE TABLE backup_chains_t (
    db_id bigint
    , db_nm VARCHAR(256)
    , is_ht_stndby BOOLEAN
    , wrn VARCHAR(256)
    , lst_bkup_nm VARCHAR(256)
    , lst_bkup_crtd TIMESTAMP
    , lst_bkup_age INTEGER
    , lst_bkup_nt_fnd BOOLEAN
    , lst_rlbk_nm VARCHAR(256)
    , lst_rlbk_created TIMESTAMP
    , lst_rlbk_age INTEGER
    , lst_rlbk_nt_fnd BOOLEAN
    , in_prgrs_bkup_nm VARCHAR(256)
    , rplca_nm VARCHAR(256)
    , chn_rplctn BOOLEAN
    , chn_nm VARCHAR(256)
    , chn_crtd TIMESTAMP
    , chn_age INTEGER
    , chn_typ VARCHAR(256)
    , chn_lck VARCHAR(256)
    , plcy VARCHAR(10000)
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE backup_chains_p( 
      _trunc_policy   BOOLEAN DEFAULT 'FALSE'
    , _yb_util_filter VARCHAR DEFAULT 'TRUE' 
   )
   RETURNS SETOF backup_chains_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'backup_chains_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   -- Prefix ybd_query_tags with procedure name
   EXECUTE 'SET ybd_query_tags  TO ' || quote_literal( _tags ); 
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);   

   _sql := $$SELECT
    d.oid::BIGINT                                                         AS db_id
    , d.datname::VARCHAR(256)                                             AS db_nm
    , d.dathotstandby                                                     AS ht_stndby
    , (IIF((NOW() - rs.creation_time) > INTERVAL '45 days', 'age ', '')
       || IIF( bs.snapshot_name IS NULL AND c.chain_name IS NOT NULL, 'orphan', '' ))::VARCHAR(256) AS wrn
   /* NOTE: We don't care about c.last_backup_point_id here as it's
   ** also modified by incremental snapshots
   */
    , COALESCE(bs.snapshot_name, c.oldest_backup_point_id)::VARCHAR(256)   AS lst_bkup_nm
    , DATE_TRUNC('SECS', bs.creation_time)::TIMESTAMP                      AS lst_bkup_crtd
    , EXTRACT(DAY FROM DATE_TRUNC('SECS', NOW() - bs.creation_time))::INT  AS lst_bkup_age
    , bs.snapshot_name IS NULL                                             AS lst_bkup_nt_fnd
    , COALESCE(rs.snapshot_name, c.oldest_rollback_point_id)::VARCHAR(256) AS lst_rlbk_nm
    , DATE_TRUNC('SECS', rs.creation_time)::TIMESTAMP                      AS lst_rlbk_created
    , EXTRACT(DAY FROM DATE_TRUNC('SECS', NOW() - rs.creation_time))::INT  AS lst_rlbk_age
    , rs.snapshot_name IS NULL AND c.oldest_rollback_point_id IS NOT NULL  AS lst_rlbk_nt_fnd
    , c.inprogress_backup_point_id::VARCHAR(256)                           AS in_prgrs_bkup_nm
    , r.name::VARCHAR(256)                                                 AS rplca_nm
    , c.for_replication::BOOLEAN                                           AS chn_rplctn
    , c.chain_name::VARCHAR(256)                                           AS chn_nm
    , DATE_TRUNC('SECS', c.creation_time)::TIMESTAMP                       AS chn_crtd
   /* NOTE: A chain age could be legitimately very old, examples:
   **       1) active replication created a long time ago
   **       2) a chain that gets repeatedly reused/"broken" by ybbackup (example: default)
   **/
    , EXTRACT(DAY FROM DATE_TRUNC('SECS', NOW() - c.creation_time))::INT   AS chn_age
    , IIF(
       c.lock IS NULL
       , IIF(
           c.for_replication
           , 'replica-' || IIF(
                               d.dathotstandby
                               , 'target-valid'
                               , IIF(r.name IS NULL, 'target-retired', 'source'))
           , IIF(
               c.last_backup_point_id IS NULL
               , 'restore-' || IIF(d.dathotstandby, 'active', 'retired')
               , 'backup'))
       , 'locked')::VARCHAR(256)                                          AS chn_typ
    , (LEFT(c.lock, 8)||'...')::VARCHAR(256)                              AS chn_lck
    , (CASE $$ || quote_literal(_trunc_policy) || $$::BOOLEAN 
        WHEN 'F' THEN TRANSLATE(policy,'{}','')
        ELSE       SUBSTR(TRANSLATE(policy,'{}',''), 1, 6)
    END)::VARCHAR(10000)                                                  AS plcy
FROM sys.backup_chains            AS c
   /* backup snapshots */
   LEFT JOIN sys.backup_snapshots AS bs
       ON bs.snapshot_name  = c.oldest_backup_point_id
       AND bs.database_id   = c.database_id
      /* rollback snapshots */
   LEFT JOIN sys.backup_snapshots AS rs
       ON rs.snapshot_name  = c.oldest_rollback_point_id
       AND rs.database_id   = c.database_id
   LEFT JOIN pg_database          AS d
       ON d.oid             = c.database_id
   LEFT JOIN sys.replica          AS r
       ON r.backup_chain_id = c.chain_name
       AND r.database_id    = c.database_id
WHERE $$ || _yb_util_filter || $$
ORDER BY d.datname, bs.creation_time NULLS LAST$$;

   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   
END;   
$proc$
;

   
COMMENT ON FUNCTION backup_chains_p( BOOLEAN, VARCHAR ) IS 
$cmnt$Description:
Existing backup chains with creating and snapshot info. 

Useful in finding unnecessary existing backup chains for deletion.
  
Examples:
  SELECT * FROM backup_chains_p();
  SELECT * FROM backup_chains_p()    WHERE chain_type != 'replication';
  SELECT * FROM backup_chains_p('f') WHERE chain_days > 45;  
  
Arguments:
. _trunc_policy   BOOLEAN - (optl ) Truncate the chain policy desc at 6 chars.
                            The policy is the schema exclude list if used.  
                            DEFAULT 'FALSE'
. _yb_util_filter VARCHAR - (intrn) Used by YbEasyCli.
                            DEFAULT 'TRUE' 
Version:
. 2023.06.05 - Yellowbrick Technical Support
$cmnt$
;
