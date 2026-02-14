/* yrs_file_smry.sql
** 
** Summary of yrs storage usge by type.
** 
** Example output
**
**  unflshd_gb | flshd_gb | cmt_gb | dlt_gb | ttl_gb | free_gb | est_dbs | est_tbls |  min_txid   |  max_txid   |  crnt_txid
** ------------+----------+--------+--------+--------+---------+---------+----------+-------------+-------------+-------------
**         0.1 |      0.1 |    0.0 |    0.0 |    0.3 |   759.7 |       1 |        3 | 81607732729 | 81643700721 | 81643700725
**                 
**
** yrs functions: 
** . sys.yb_yrs_rowstore_status() - Returns state of yrs. Should be NORMAL.
** . sys.yrs_flush                - Record of committed flush operations per table. Drop also counts as a flushes.
**                                   The difference is that the rows don't make it to the backend.
** . yb_yrs_data_files            - 
** . yb_yrs_commit_files()        - Committed files waiting to be freed.
**                                  A large size implies a problem; i.e a backup event horizon 
**                                  and/or tables continually written to so housekeeping can never clear them out.
** . yb_yrs_tables()              - Per table info for tables that have data in the rowstore 
** . yb_yrs_delete_files          - YRS files whose rows have been flushed but files not deleted.
**                                  The files should be deleted on next YB restart or yb_yrs_delete_unused_files();
** Additional Notes:
** . free_gb - yrs rowstore usable size is 80% of ttl_gb. For appliances that is 80% of 950 GB. e.g.
**             ROUND((950*.8) - ttl_gb, 1)                                                
** 
** Revision History
** . 2026.02.08 (rek) - Add dlta_txid & dlta_crnt
** . 2024.11.25 (rek) - Inital version
*/

\o delete.me
SET work_mem TO 1000000;
\o 

WITH unflushed_data_files AS
   (  SELECT
         0                           AS sort_key
       , 'unflushed_data_files'      AS type
       , COUNT(*)                    AS files
       , ceil((files * 30) / 1024.0) AS gb
       , COUNT(DISTINCT database_id) AS databases
       , COUNT(DISTINCT table_id )   AS tables
       , MIN( lowest_txid )          AS min_txid
       , MAX( highest_txid )         AS max_txid
      FROM yb_yrs_data_files()       AS yrsdf
      GROUP BY 1, 2
   )
, flushed_data_files AS
   (  SELECT
         1                           AS sort_key
       , 'flushed_data_files'        AS type
       , COUNT(*)                    AS files
       , ceil((files * 30) / 1024.0) AS gb
       , COUNT(DISTINCT database_id) AS databases
       , COUNT(DISTINCT table_id )   AS tables
       , MIN( lowest_txid )          AS min_txid
       , MAX( highest_txid )         AS max_txid
      FROM yb_yrs_data_files()       AS yrsdf
      GROUP BY 1, 2
   )   
 , commit_files AS
   (  SELECT
         2                             AS sort_key
       , 'commit_files'                AS type
       , COUNT(DISTINCT file_id)       AS files
       , ceil((files * 30 ) / 1024.0 ) AS gb
       , NULL                          AS databases
       , NULL                          AS tables
       , MIN(block_highest_txid)       AS min_txid
       , MAX(block_highest_txid)       AS max_txid
      FROM yb_yrs_commit_files()       AS yrscf
      GROUP BY 1, 2
   )
 , delete_files AS
   (  SELECT
         3                                  AS sort_key
       , 'delete_files'                     AS type
       , COUNT(DISTINCT yrsdf.data_file_id) AS files
       , ceil((files * 30) / 1024.0)        AS gb
       , COUNT(DISTINCT st.database_id)     AS databases
       , COUNT(DISTINCT yrsdf.table_id)     AS tables
       , MIN(yrsdf.txid)                    AS min_txid
       , MAX(yrsdf.txid)                    AS max_txid
      FROM yb_yrs_delete_files()            AS yrsdf
      LEFT JOIN sys.table                   AS st ON yrsdf.table_id = st.table_id
      GROUP BY 1, 2
   )
, all_files AS 
(   SELECT * FROM unflushed_data_files
    UNION ALL
    SELECT * FROM flushed_data_files
    UNION ALL
    SELECT * FROM commit_files
    UNION ALL
    SELECT * FROM delete_files
)
SELECT
   ROUND(SUM(DECODE(type, 'unflushed_data_files', files, 0)) * 30 / 1024.0, 1) AS unflshd_gb
 , ROUND(SUM(DECODE(type, 'flushed_data_files ' , files, 0)) * 30 / 1024.0, 1) AS flshd_gb
 , ROUND(SUM(DECODE(type, 'commit_files'        , files, 0)) * 30 / 1024.0, 1) AS cmt_gb
 , ROUND(SUM(DECODE(type, 'delete_files'        , files, 0)) * 30 / 1024.0, 1) AS dlt_gb
 , ROUND(SUM(files)                                          * 30 / 1024.0, 1) AS ttl_gb
 , ROUND((950*.8) - ttl_gb, 1)                                                 AS free_gb
 , MAX(databases)                                                              AS est_dbs
 , MAX(tables)                                                                 AS est_tbls
 , MIN(min_txid)                                                               AS min_txid
 , MAX(max_txid)                                                               AS max_txid
 , TXID_CURRENT()                                                              AS crnt_txid
 , MAX(max_txid) - MIN(min_txid)                                               AS dlta_txid
 , TXID_CURRENT()  - MIN(min_txid)                                             AS dlta_crnt
FROM all_files
;
